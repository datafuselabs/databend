// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Copyright (c) 2018 by the tantivy project authors
// (https://github.com/quickwit-oss/tantivy), as listed in the AUTHORS file.
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the "Software"),
// to deal in the Software without restriction, including without limitation
// the rights to use, copy, modify, merge, publish, distribute, sublicense,
// and/or sell copies of the Software, and to permit persons to whom the Software
// is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included
// in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
// THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::io;
use std::io::BufWriter;
use std::io::Cursor;
use std::io::Read;
use std::io::Write;
use std::marker::PhantomData;
use std::ops::BitAndAssign;
use std::ops::BitOrAssign;
use std::ops::SubAssign;
use std::path::Path;
use std::path::PathBuf;
use std::result;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_storages_common_table_meta::meta::testify_version;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use databend_storages_common_table_meta::meta::Versioned;
use levenshtein_automata::Distance;
use levenshtein_automata::LevenshteinAutomatonBuilder;
use levenshtein_automata::DFA;
use log::warn;
use roaring::RoaringTreemap;
use tantivy::directory::error::DeleteError;
use tantivy::directory::error::OpenReadError;
use tantivy::directory::error::OpenWriteError;
use tantivy::directory::AntiCallToken;
use tantivy::directory::FileHandle;
use tantivy::directory::FileSlice;
use tantivy::directory::OwnedBytes;
use tantivy::directory::TerminatingWrite;
use tantivy::directory::WatchCallback;
use tantivy::directory::WatchHandle;
use tantivy::directory::WritePtr;
use tantivy::positions::PositionReader;
use tantivy::postings::BlockSegmentPostings;
use tantivy::postings::TermInfo;
use tantivy::query::AllQuery;
use tantivy::query::BooleanQuery;
use tantivy::query::BoostQuery;
use tantivy::query::ConstScoreQuery;
use tantivy::query::EmptyQuery;
use tantivy::query::FuzzyTermQuery;
use tantivy::query::Occur;
use tantivy::query::PhrasePrefixQuery;
use tantivy::query::PhraseQuery;
use tantivy::query::Query;
use tantivy::query::QueryClone;
use tantivy::query::TermQuery;
use tantivy::schema::Field;
use tantivy::Directory;
use tantivy::Term;
use tantivy_common::BinarySerializable;
use tantivy_common::HasLen;
use tantivy_common::VInt;
use tantivy_fst::Automaton;
use tantivy_fst::IntoStreamer;
use tantivy_fst::Regex;
use tantivy_fst::Streamer;

fn extract_footer(data: FileSlice) -> Result<(Vec<FileAddr>, Vec<usize>)> {
    // The following code is copied from tantivy `CompositeFile::open` function.
    // extract field number and offsets of each fields.
    let end = data.len();
    let footer_len_data = data.slice_from(end - 4).read_bytes()?;
    let footer_len = u32::deserialize(&mut footer_len_data.as_slice())? as usize;
    let footer_start = end - 4 - footer_len;
    let footer_data = data
        .slice(footer_start..footer_start + footer_len)
        .read_bytes()?;
    let mut footer_buffer = footer_data.as_slice();
    let num_fields = VInt::deserialize(&mut footer_buffer)?.0 as usize;

    let mut offset = 0;
    let mut offsets = Vec::with_capacity(num_fields);
    let mut file_addrs = Vec::with_capacity(num_fields);
    for _ in 0..num_fields {
        offset += VInt::deserialize(&mut footer_buffer)?.0 as usize;
        offsets.push(offset);
        let file_addr = FileAddr::deserialize(&mut footer_buffer)?;
        file_addrs.push(file_addr);
    }
    offsets.push(footer_start);

    Ok((file_addrs, offsets))
}

// Extract fsts and term dicts into separate columns.
pub fn extract_fsts(
    data: FileSlice,
    fields: &mut Vec<TableField>,
    values: &mut Vec<Scalar>,
) -> Result<()> {
    let (file_addrs, offsets) = extract_footer(data.clone())?;

    let mut term_dict_fields = Vec::with_capacity(file_addrs.len());
    let mut term_dict_values = Vec::with_capacity(file_addrs.len());
    for (i, file_addr) in file_addrs.iter().enumerate() {
        let field_id = file_addr.field.field_id();
        let start_offset = offsets[i];
        let end_offset = offsets[i + 1];

        let field_slice = data.slice(start_offset..end_offset);

        let (main_slice, footer_len_slice) = field_slice.split_from_end(16);
        let mut footer_len_bytes = footer_len_slice.read_bytes()?;
        let footer_size = u64::deserialize(&mut footer_len_bytes)?;

        let (fst_file_slice, term_dict_file_slice) =
            main_slice.split_from_end(footer_size as usize);

        let fst_field_name = format!("fst-{}", field_id);
        let fst_field = TableField::new(&fst_field_name, TableDataType::Binary);
        fields.push(fst_field);

        let fst_bytes = fst_file_slice.read_bytes()?;
        values.push(Scalar::Binary(fst_bytes.as_slice().to_vec()));

        let term_dict_field_name = format!("term-{}", field_id);
        let term_dict_field = TableField::new(&term_dict_field_name, TableDataType::Binary);
        term_dict_fields.push(term_dict_field);

        let term_dict_bytes = term_dict_file_slice.read_bytes()?;
        term_dict_values.push(Scalar::Binary(term_dict_bytes.as_slice().to_vec()));
    }

    fields.append(&mut term_dict_fields);
    values.append(&mut term_dict_values);

    Ok(())
}

// Extract component file into separate columns by fields.
pub fn extract_component_fields(
    name: &str,
    data: FileSlice,
    fields: &mut Vec<TableField>,
    values: &mut Vec<Scalar>,
) -> Result<()> {
    let (file_addrs, offsets) = extract_footer(data.clone())?;

    for (i, file_addr) in file_addrs.iter().enumerate() {
        let field_id = file_addr.field.field_id();
        let start_offset = offsets[i];
        let end_offset = offsets[i + 1];

        let field_name = format!("{}-{}", name, field_id);
        let field = TableField::new(&field_name, TableDataType::Binary);
        fields.push(field);

        let field_slice = data.slice(start_offset..end_offset);
        let field_bytes = field_slice.read_bytes()?;
        values.push(Scalar::Binary(field_bytes.as_slice().to_vec()));
    }

    Ok(())
}

#[derive(Eq, PartialEq, Hash, Copy, Ord, PartialOrd, Clone, Debug)]
struct FileAddr {
    field: Field,
    idx: usize,
}

impl BinarySerializable for FileAddr {
    fn serialize<W: Write + ?Sized>(&self, writer: &mut W) -> io::Result<()> {
        self.field.serialize(writer)?;
        VInt(self.idx as u64).serialize(writer)?;
        Ok(())
    }

    fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
        let field = Field::deserialize(reader)?;
        let idx = VInt::deserialize(reader)?.0 as usize;
        Ok(FileAddr { field, idx })
    }
}

struct DfaWrapper(pub DFA);

impl Automaton for DfaWrapper {
    type State = u32;

    fn start(&self) -> Self::State {
        self.0.initial_state()
    }

    fn is_match(&self, state: &Self::State) -> bool {
        match self.0.distance(*state) {
            Distance::Exact(_) => true,
            Distance::AtLeast(_) => false,
        }
    }

    fn can_match(&self, state: &u32) -> bool {
        *state != levenshtein_automata::SINK_STATE
    }

    fn accept(&self, state: &Self::State, byte: u8) -> Self::State {
        self.0.transition(*state, byte)
    }
}

// Collect matched `doc_ids` for a Query.
#[derive(Clone)]
pub struct DocIdsCollector {
    row_count: u64,
    need_position: bool,
    // key is `term`, value is `term_id`,
    // These terms are in `fst`, means that those terms exist in the block,
    // we need to use related term infos to determine the matched `doc_ids`.
    // Use `term_id` as key to get related information and avoid copy `term`.
    term_map: HashMap<Term, u64>,
    // key is `term_id`, value is `field_id` and `term_info`.
    term_infos: HashMap<u64, (u32, TermInfo)>,
    // key is `term_id`, value is related `BlockSegmentPostings`,
    // used to read `doc_ids` and `term_freqs`.
    block_postings_map: HashMap<u64, BlockSegmentPostings>,
    // key is `term_id`, value is related `PositionReader`,
    // used to read `positions` in each docs.
    position_reader_map: HashMap<u64, PositionReader>,
    // key is `term_id`, value is related `doc_ids`.
    // `doc_ids` is lazy loaded when used.
    doc_ids: HashMap<u64, RoaringTreemap>,
    // key is `term_id`, value is related `term_freqs`.
    // `term_freqs` is lazy loaded when used.
    term_freqs: HashMap<u64, Vec<u32>>,
}

impl DocIdsCollector {
    pub fn create(
        row_count: u64,
        need_position: bool,
        terms: HashMap<Term, (u32, u64)>,
        term_infos: HashMap<u64, (u32, TermInfo)>,
        block_postings_map: HashMap<u64, BlockSegmentPostings>,
        position_reader_map: HashMap<u64, PositionReader>,
    ) -> Self {
        let term_len = terms.len();
        let term_map = terms
            .into_iter()
            .map(|(term, (_, term_id))| (term, term_id))
            .collect::<HashMap<Term, u64>>();

        Self {
            row_count,
            need_position,
            term_map,
            term_infos,
            block_postings_map,
            position_reader_map,
            doc_ids: HashMap::with_capacity(term_len),
            term_freqs: HashMap::with_capacity(term_len),
        }
    }

    fn check_term_match(
        fst_map: &tantivy_fst::Map<OwnedBytes>,
        field_id: u32,
        term: &Term,
        matched_terms: &mut HashMap<Term, (u32, u64)>,
    ) -> bool {
        if let Some(term_id) = fst_map.get(term.serialized_value_bytes()) {
            matched_terms.insert(term.clone(), (field_id, term_id));
            true
        } else {
            false
        }
    }

    fn get_fst_map(
        field_id: u32,
        fst_maps: &HashMap<u32, tantivy_fst::Map<OwnedBytes>>,
    ) -> Result<&tantivy_fst::Map<OwnedBytes>> {
        let fst_map = fst_maps.get(&field_id).ok_or_else(|| {
            ErrorCode::TantivyError(format!(
                "inverted index fst field `{}` does not exist",
                field_id
            ))
        })?;

        Ok(fst_map)
    }

    // Check if fst contains terms in query.
    // If not, we can skip read other parts of inverted index.
    pub fn check_term_fsts_match(
        query: Box<dyn Query>,
        fst_maps: &HashMap<u32, tantivy_fst::Map<OwnedBytes>>,
        fuzziness: &Option<u8>,
        matched_terms: &mut HashMap<Term, (u32, u64)>,
        prefix_terms: &mut HashMap<Term, Vec<u64>>,
        fuzziness_terms: &mut HashMap<Term, Vec<u64>>,
    ) -> Result<bool> {
        if let Some(term_query) = query.downcast_ref::<TermQuery>() {
            let term = term_query.term();
            let field = term.field();
            let field_id = field.field_id();
            let fst_map = Self::get_fst_map(field_id, fst_maps)?;
            let matched = Self::check_term_match(fst_map, field_id, term, matched_terms);
            Ok(matched)
        } else if let Some(bool_query) = query.downcast_ref::<BooleanQuery>() {
            let mut matched_any = false;
            for (occur, sub_query) in bool_query.clauses() {
                let matched = Self::check_term_fsts_match(
                    sub_query.box_clone(),
                    fst_maps,
                    fuzziness,
                    matched_terms,
                    prefix_terms,
                    fuzziness_terms,
                )?;
                match occur {
                    Occur::Should => {
                        if matched {
                            matched_any = true;
                        }
                    }
                    Occur::Must => {
                        if matched {
                            matched_any = true;
                        } else {
                            return Ok(false);
                        }
                    }
                    Occur::MustNot => {
                        // Matched means that the block contains the term,
                        // but we still need to filter out the `doc_ids`.
                        matched_any = true;
                    }
                }
            }
            Ok(matched_any)
        } else if let Some(phrase_query) = query.downcast_ref::<PhraseQuery>() {
            // PhraseQuery must match all terms.
            let field = phrase_query.field();
            let field_id = field.field_id();
            let fst_map = Self::get_fst_map(field_id, fst_maps)?;

            let mut matched_all = true;
            for term in phrase_query.phrase_terms() {
                let matched = Self::check_term_match(fst_map, field_id, &term, matched_terms);
                if !matched {
                    matched_all = false;
                    break;
                }
            }
            Ok(matched_all)
        } else if let Some(phrase_prefix_query) = query.downcast_ref::<PhrasePrefixQuery>() {
            // PhrasePrefixQuery must match all terms.
            let field = phrase_prefix_query.field();
            let field_id = field.field_id();
            let fst_map = Self::get_fst_map(field_id, fst_maps)?;

            let mut matched_all = true;
            for term in phrase_prefix_query.phrase_terms() {
                let matched = Self::check_term_match(fst_map, field_id, &term, matched_terms);
                if !matched {
                    matched_all = false;
                    break;
                }
            }
            if !matched_all {
                return Ok(false);
            }

            // using regex to check prefix term, get related term ids.
            let (_, prefix_term) = phrase_prefix_query.prefix_term_with_offset();
            let term_str = String::from_utf8_lossy(prefix_term.serialized_value_bytes());
            let key = format!("{}.*", term_str);
            let re = Regex::new(&key).map_err(|_| {
                ErrorCode::TantivyError(format!("inverted index create regex `{}` failed", key))
            })?;

            let mut prefix_term_ids = vec![];
            let mut stream = fst_map.search(&re).into_stream();
            while let Some((key, term_id)) = stream.next() {
                let key_str = unsafe { std::str::from_utf8_unchecked(key) };
                let prefix_term = Term::from_field_text(field, key_str);
                matched_terms.insert(prefix_term.clone(), (field_id, term_id));
                prefix_term_ids.push(term_id);
            }
            let matched = !prefix_term_ids.is_empty();
            if matched {
                prefix_terms.insert(prefix_term.clone(), prefix_term_ids);
            }
            Ok(matched)
        } else if let Some(fuzzy_term_query) = query.downcast_ref::<FuzzyTermQuery>() {
            // FuzzyTermQuery match terms by levenshtein distance.
            let fuzziness = fuzziness.unwrap();
            let term = fuzzy_term_query.term();
            let field = term.field();
            let field_id = field.field_id();
            let fst_map = Self::get_fst_map(field_id, fst_maps)?;

            // build levenshtein automaton to check fuzziness term, get related term ids.
            let lev_automaton_builder = LevenshteinAutomatonBuilder::new(fuzziness, true);
            let term_str = String::from_utf8_lossy(term.serialized_value_bytes());
            let automaton = DfaWrapper(lev_automaton_builder.build_dfa(&term_str));

            let mut fuzz_term_ids = vec![];
            let mut stream = fst_map.search(automaton).into_stream();
            while let Some((key, term_id)) = stream.next() {
                let key_str = unsafe { std::str::from_utf8_unchecked(key) };
                let fuzz_term = Term::from_field_text(field, key_str);
                matched_terms.insert(fuzz_term.clone(), (field_id, term_id));
                fuzz_term_ids.push(term_id);
            }
            let matched = !fuzz_term_ids.is_empty();
            if matched {
                fuzziness_terms.insert(term.clone(), fuzz_term_ids);
            }
            Ok(matched)
        } else if let Some(boost_query) = query.downcast_ref::<BoostQuery>() {
            Self::check_term_fsts_match(
                boost_query.query(),
                fst_maps,
                fuzziness,
                matched_terms,
                prefix_terms,
                fuzziness_terms,
            )
        } else if let Some(const_query) = query.downcast_ref::<ConstScoreQuery>() {
            Self::check_term_fsts_match(
                const_query.query(),
                fst_maps,
                fuzziness,
                matched_terms,
                prefix_terms,
                fuzziness_terms,
            )
        } else if let Some(_empty_query) = query.downcast_ref::<EmptyQuery>() {
            Ok(false)
        } else if let Some(_all_query) = query.downcast_ref::<AllQuery>() {
            Ok(true)
        } else {
            Err(ErrorCode::TantivyError(format!(
                "inverted index unsupported query `{:?}`",
                query
            )))
        }
    }

    // get `doc_ids` of a `term_id`,
    fn get_doc_ids(&mut self, term_id: u64) -> Result<&RoaringTreemap> {
        if let std::collections::hash_map::Entry::Vacant(doc_ids_entry) =
            self.doc_ids.entry(term_id)
        {
            // `doc_ids` are lazy loaded when used.
            let block_postings = self.block_postings_map.get_mut(&term_id).ok_or_else(|| {
                ErrorCode::TantivyError(format!(
                    "inverted index block postings `{}` does not exist",
                    term_id
                ))
            })?;

            let term_freqs_len = if self.need_position {
                let (_, term_info) = self.term_infos.get(&term_id).ok_or_else(|| {
                    ErrorCode::TantivyError(format!(
                        "inverted index term info `{}` does not exist",
                        term_id
                    ))
                })?;
                term_info.doc_freq as usize
            } else {
                0
            };
            let mut doc_ids = RoaringTreemap::new();
            let mut term_freqs = Vec::with_capacity(term_freqs_len);
            // `doc_ids` are stored in multiple blocks and need to be decode sequentially.
            // TODO: We can skip some blocks by checking related `doc_ids`.
            loop {
                let block_doc_ids = block_postings.docs();
                if block_doc_ids.is_empty() {
                    break;
                }
                doc_ids
                    .append(block_doc_ids.iter().map(|id| *id as u64))
                    .unwrap();

                // `term_freqs` is only used if the query need position.
                if self.need_position {
                    let block_term_freqs = block_postings.freqs();
                    term_freqs.extend_from_slice(block_term_freqs);
                }
                block_postings.advance();
            }
            doc_ids_entry.insert(doc_ids);
            self.term_freqs.insert(term_id, term_freqs);
        }

        let doc_ids = self.doc_ids.get(&term_id).unwrap();
        Ok(doc_ids)
    }

    // get the position `offsets` and `term_freqs` of a `term_id` in each `docs`,
    // which is used to read `positions`.
    fn get_position_offsets(
        &mut self,
        term_id: u64,
        all_doc_ids: &RoaringTreemap,
    ) -> Result<HashMap<u64, (u64, u32)>> {
        let doc_ids = self.doc_ids.get(&term_id).unwrap();
        let term_freqs = self.term_freqs.get(&term_id).unwrap();

        let mut doc_offset = 0;
        let mut offset_and_term_freqs = HashMap::with_capacity(all_doc_ids.len() as usize);
        for (doc_id, term_freq) in doc_ids.iter().zip(term_freqs.iter()) {
            if all_doc_ids.len() as usize == offset_and_term_freqs.len() {
                break;
            }
            if !all_doc_ids.contains(doc_id) {
                doc_offset += *term_freq as u64;
                continue;
            }

            offset_and_term_freqs.insert(doc_id, (doc_offset, *term_freq));
            doc_offset += *term_freq as u64;
        }

        Ok(offset_and_term_freqs)
    }

    // get `positions` of a `term_id` in a `doc`.
    fn get_positions(
        &mut self,
        term_id: u64,
        doc_offset: u64,
        term_freq: u32,
    ) -> Result<RoaringTreemap> {
        let position_reader = self.position_reader_map.get_mut(&term_id).ok_or_else(|| {
            ErrorCode::TantivyError(format!(
                "inverted index position reader `{}` does not exist",
                term_id
            ))
        })?;

        let mut positions = vec![0; term_freq as usize];
        position_reader.read(doc_offset, &mut positions[..]);
        for i in 1..positions.len() {
            positions[i] += positions[i - 1];
        }
        let term_poses =
            RoaringTreemap::from_sorted_iter(positions.into_iter().map(|i| i as u64)).unwrap();
        Ok(term_poses)
    }

    // The phrase query matches `doc_ids` as follows:
    //
    // 1. Collect the position for each term in the query.
    // 2. Collect the `doc_ids` of each term and take
    //    the intersection to get the candidate `doc_ids`.
    // 3. Iterate over the candidate `doc_ids` to check whether
    //    the position of terms matches the position of terms in query.
    // 4. Each position in the first term is a possible query phrase beginning.
    //    Verify that the beginning is valid by checking whether corresponding
    //    positions in other terms exist. If not, delete the possible position
    //    in the first term. After traversing all terms, determine if there are
    //    any positions left in the first term. If there are, then the `doc_id`
    //    is matched.
    //
    // If the query is a prefix phrase query, also check if any prefix terms
    // match the positions.
    pub fn collect_phrase_matched_rows(
        &mut self,
        phrase_terms: Vec<(usize, Term)>,
        prefix_term: Option<(usize, &Vec<u64>)>,
    ) -> Result<Option<RoaringTreemap>> {
        let mut query_term_poses = Vec::with_capacity(phrase_terms.len());
        for (term_pos, term) in &phrase_terms {
            // term not exist means this phrase in not matched.
            let Some(term_id) = self.term_map.get(term) else {
                return Ok(None);
            };
            query_term_poses.push((*term_pos, *term_id));
        }
        if query_term_poses.is_empty() {
            return Ok(None);
        }

        let first_term_pos = &query_term_poses[0].0;
        let first_term_id = &query_term_poses[0].1;

        let mut term_ids = HashSet::with_capacity(phrase_terms.len() + 1);
        term_ids.insert(*first_term_id);

        let first_doc_ids = self.get_doc_ids(*first_term_id)?;
        let mut candidate_doc_ids = RoaringTreemap::new();
        candidate_doc_ids.bitor_assign(first_doc_ids);

        // Collect the `doc_ids` of other terms in the query, and take the intersection,
        // obtains the candidate `doc_ids` containing all terms.
        let mut query_term_offsets = Vec::with_capacity(query_term_poses.len() - 1);
        for (term_pos, term_id) in query_term_poses.iter().skip(1) {
            if !term_ids.contains(term_id) {
                let doc_ids = self.get_doc_ids(*term_id)?;

                candidate_doc_ids.bitand_assign(doc_ids);
                if candidate_doc_ids.is_empty() {
                    break;
                }
                term_ids.insert(*term_id);
            }
            let term_pos_offset = (term_pos - first_term_pos) as u64;
            query_term_offsets.push((*term_id, term_pos_offset));
        }
        // If the candidate `doc_ids` is empty, all docs are not matched.
        if candidate_doc_ids.is_empty() {
            return Ok(None);
        }

        // If the query is a prefix phrase query,
        // also need to collect `doc_ids` of prefix terms.
        if let Some((_, prefix_term_ids)) = prefix_term {
            let mut all_prefix_doc_ids = RoaringTreemap::new();
            for prefix_term_id in prefix_term_ids {
                let prefix_doc_ids = self.get_doc_ids(*prefix_term_id)?;
                // If the `doc_ids` does not intersect at all, this prefix can be ignored.
                if candidate_doc_ids.is_disjoint(prefix_doc_ids) {
                    continue;
                }

                all_prefix_doc_ids.bitor_assign(prefix_doc_ids);
                term_ids.insert(*prefix_term_id);
            }
            // If there is no matched prefix `doc_ids`, the prefix term does not matched.
            if all_prefix_doc_ids.is_empty() {
                return Ok(None);
            }

            // Get the intersection of phrase `doc_ids` and prefix `doc_ids`
            candidate_doc_ids.bitand_assign(all_prefix_doc_ids);
        }

        // Collect the position `offset` and `term_freqs` for each terms,
        // which can be used to read positions.
        let mut offset_and_term_freqs_map = HashMap::new();
        for term_id in term_ids.into_iter() {
            let offset_and_term_freqs = self.get_position_offsets(term_id, &candidate_doc_ids)?;
            offset_and_term_freqs_map.insert(term_id, offset_and_term_freqs);
        }

        let first_offset_and_term_freqs = offset_and_term_freqs_map.get(first_term_id).unwrap();

        // Check every candidate `doc_ids` if the position of each terms match the query.
        let mut all_doc_ids = RoaringTreemap::new();
        let mut offset_poses = RoaringTreemap::new();
        let mut term_poses_map = HashMap::new();
        for (doc_id, (first_doc_offset, first_term_freq)) in first_offset_and_term_freqs.iter() {
            let mut first_term_poses =
                self.get_positions(*first_term_id, *first_doc_offset, *first_term_freq)?;

            term_poses_map.clear();
            term_poses_map.insert(first_term_id, first_term_poses.clone());

            for (term_id, term_pos_offset) in &query_term_offsets {
                if !term_poses_map.contains_key(term_id) {
                    let offset_and_term_freqs = offset_and_term_freqs_map.get(term_id).unwrap();
                    let (doc_offset, term_freq) = offset_and_term_freqs.get(doc_id).unwrap();

                    let term_poses = self.get_positions(*term_id, *doc_offset, *term_freq)?;
                    term_poses_map.insert(term_id, term_poses);
                }
                let term_poses = term_poses_map.get(term_id).unwrap();

                // Using the position of the first term and the offset of this term with the first term,
                // calculate all possible positions for this term.
                offset_poses.clear();
                offset_poses
                    .append(first_term_poses.iter().map(|pos| pos + term_pos_offset))
                    .unwrap();

                // Term possible positions subtract term actual positions,
                // remaining positions are not matched and need to be removed in the first term.
                offset_poses.sub_assign(term_poses);
                for offset_pos in &offset_poses {
                    first_term_poses.remove(offset_pos - term_pos_offset);
                }
                if first_term_poses.is_empty() {
                    break;
                }
            }

            // If the query is a prefix phrase query,
            // also need to check if any prefix term match.
            if let Some((prefix_term_pos, prefix_term_ids)) = prefix_term {
                if !first_term_poses.is_empty() {
                    let mut prefix_matched = false;
                    let prefix_term_pos_offset = (prefix_term_pos - first_term_pos) as u64;
                    for prefix_term_id in prefix_term_ids {
                        if !term_poses_map.contains_key(prefix_term_id) {
                            if let Some(offset_and_term_freqs) =
                                offset_and_term_freqs_map.get(prefix_term_id)
                            {
                                if let Some((doc_offset, term_freq)) =
                                    offset_and_term_freqs.get(doc_id)
                                {
                                    let term_poses = self.get_positions(
                                        *prefix_term_id,
                                        *doc_offset,
                                        *term_freq,
                                    )?;
                                    term_poses_map.insert(prefix_term_id, term_poses);
                                }
                            }
                        }
                        if let Some(term_poses) = term_poses_map.get(prefix_term_id) {
                            offset_poses.clear();
                            offset_poses
                                .append(
                                    first_term_poses
                                        .iter()
                                        .map(|pos| pos + prefix_term_pos_offset),
                                )
                                .unwrap();

                            offset_poses.bitand_assign(term_poses);
                            // If any of the possible prefix term positions exist,
                            // the prefix phrase query is matched.
                            if !offset_poses.is_empty() {
                                prefix_matched = true;
                                break;
                            }
                        }
                    }
                    if prefix_matched {
                        all_doc_ids.insert(*doc_id);
                    }
                }
            } else {
                let matched = !first_term_poses.is_empty();
                if matched {
                    all_doc_ids.insert(*doc_id);
                }
            }
        }

        if !all_doc_ids.is_empty() {
            Ok(Some(all_doc_ids))
        } else {
            Ok(None)
        }
    }

    // collect matched rows by term value
    pub fn collect_matched_rows(
        &mut self,
        query: Box<dyn Query>,
        prefix_terms: &HashMap<Term, Vec<u64>>,
        fuzziness_terms: &HashMap<Term, Vec<u64>>,
    ) -> Result<Option<RoaringTreemap>> {
        if let Some(term_query) = query.downcast_ref::<TermQuery>() {
            let term = term_query.term();
            if let Some(term_id) = self.term_map.get(term) {
                let doc_ids = self.get_doc_ids(*term_id)?;
                Ok(Some(doc_ids.clone()))
            } else {
                Ok(None)
            }
        } else if let Some(bool_query) = query.downcast_ref::<BooleanQuery>() {
            let mut should_doc_ids: Option<RoaringTreemap> = None;
            let mut must_doc_ids: Option<RoaringTreemap> = None;
            let mut must_not_doc_ids: Option<RoaringTreemap> = None;
            for (occur, sub_query) in bool_query.clauses() {
                let doc_ids = self.collect_matched_rows(
                    sub_query.box_clone(),
                    prefix_terms,
                    fuzziness_terms,
                )?;
                if doc_ids.is_none() {
                    match occur {
                        Occur::Should => {
                            continue;
                        }
                        Occur::Must => {
                            if must_doc_ids.is_none() {
                                must_doc_ids = Some(RoaringTreemap::new());
                            }
                            continue;
                        }
                        Occur::MustNot => {
                            if must_not_doc_ids.is_none() {
                                must_not_doc_ids = Some(RoaringTreemap::new());
                            }
                            continue;
                        }
                    }
                }
                let doc_ids = doc_ids.unwrap();
                match occur {
                    Occur::Should => {
                        if let Some(ref mut should_doc_ids) = should_doc_ids {
                            should_doc_ids.bitor_assign(&doc_ids);
                        } else {
                            should_doc_ids = Some(doc_ids);
                        }
                    }
                    Occur::Must => {
                        if let Some(ref mut must_doc_ids) = must_doc_ids {
                            must_doc_ids.bitand_assign(&doc_ids);
                        } else {
                            must_doc_ids = Some(doc_ids);
                        }
                    }
                    Occur::MustNot => {
                        if let Some(ref mut must_not_doc_ids) = must_not_doc_ids {
                            must_not_doc_ids.bitor_assign(&doc_ids);
                        } else {
                            must_not_doc_ids = Some(doc_ids);
                        }
                    }
                }
            }

            let all_doc_ids = match (should_doc_ids, must_doc_ids, must_not_doc_ids) {
                // only should
                (Some(should_doc_ids), None, None) => should_doc_ids,
                // only must
                (None, Some(must_doc_ids), None) => must_doc_ids,
                // only must not
                (None, None, Some(must_not_doc_ids)) => {
                    let mut all_doc_ids = RoaringTreemap::from_iter(0..self.row_count);
                    all_doc_ids.sub_assign(must_not_doc_ids);
                    all_doc_ids
                }
                // should and must
                (Some(mut should_doc_ids), Some(must_doc_ids), None) => {
                    should_doc_ids.bitor_assign(must_doc_ids);
                    should_doc_ids
                }
                // should and must not
                (Some(mut should_doc_ids), None, Some(must_not_doc_ids)) => {
                    should_doc_ids.sub_assign(must_not_doc_ids);
                    should_doc_ids
                }
                // must and must not
                (None, Some(mut must_doc_ids), Some(must_not_doc_ids)) => {
                    must_doc_ids.sub_assign(must_not_doc_ids);
                    must_doc_ids
                }
                // should, must and must not
                (Some(mut should_doc_ids), Some(must_doc_ids), Some(must_not_doc_ids)) => {
                    should_doc_ids.bitor_assign(must_doc_ids);
                    should_doc_ids.sub_assign(must_not_doc_ids);
                    should_doc_ids
                }
                (None, None, None) => {
                    return Ok(None);
                }
            };

            if !all_doc_ids.is_empty() {
                Ok(Some(all_doc_ids))
            } else {
                Ok(None)
            }
        } else if let Some(phrase_query) = query.downcast_ref::<PhraseQuery>() {
            let phrase_terms = phrase_query.phrase_terms_with_offsets();
            self.collect_phrase_matched_rows(phrase_terms, None)
        } else if let Some(phrase_prefix_query) = query.downcast_ref::<PhrasePrefixQuery>() {
            let phrase_terms = phrase_prefix_query.phrase_terms_with_offsets();
            let (prefix_term_pos, prefix_term) = phrase_prefix_query.prefix_term_with_offset();

            let Some(prefix_term_ids) = prefix_terms.get(&prefix_term) else {
                return Ok(None);
            };
            let prefix_term = Some((prefix_term_pos, prefix_term_ids));

            self.collect_phrase_matched_rows(phrase_terms, prefix_term)
        } else if let Some(fuzzy_term_query) = query.downcast_ref::<FuzzyTermQuery>() {
            let mut all_doc_ids = RoaringTreemap::new();
            let term = fuzzy_term_query.term();

            let Some(fuzz_term_ids) = fuzziness_terms.get(term) else {
                return Ok(None);
            };
            // collect related terms of the original term.
            for term_id in fuzz_term_ids {
                let doc_ids = self.get_doc_ids(*term_id)?;
                all_doc_ids.bitor_assign(doc_ids);
            }
            if !all_doc_ids.is_empty() {
                Ok(Some(all_doc_ids))
            } else {
                Ok(None)
            }
        } else if let Some(boost_query) = query.downcast_ref::<BoostQuery>() {
            self.collect_matched_rows(boost_query.query(), prefix_terms, fuzziness_terms)
        } else if let Some(const_query) = query.downcast_ref::<ConstScoreQuery>() {
            self.collect_matched_rows(const_query.query(), prefix_terms, fuzziness_terms)
        } else if let Some(_empty_query) = query.downcast_ref::<EmptyQuery>() {
            Ok(None)
        } else if let Some(_all_query) = query.downcast_ref::<AllQuery>() {
            let all_doc_ids = RoaringTreemap::from_iter(0..self.row_count);
            Ok(Some(all_doc_ids))
        } else {
            Err(ErrorCode::TantivyError(format!(
                "inverted index unsupported query `{:?}`",
                query
            )))
        }
    }
}

#[derive(Clone)]
pub struct InvertedIndexMeta {
    pub columns: Vec<(String, SingleColumnMeta)>,
}

#[derive(Clone, Debug)]
pub struct InvertedIndexFile {
    pub name: String,
    pub data: OwnedBytes,
}

impl InvertedIndexFile {
    pub fn create(name: String, data: Vec<u8>) -> Self {
        let data = OwnedBytes::new(data);
        Self { name, data }
    }
}

/// The Writer just writes a buffer.
struct VecWriter {
    path: PathBuf,
    data: Cursor<Vec<u8>>,
    is_flushed: bool,
}

impl VecWriter {
    fn new(path_buf: PathBuf) -> VecWriter {
        VecWriter {
            path: path_buf,
            data: Cursor::new(Vec::new()),
            is_flushed: true,
        }
    }
}

impl Drop for VecWriter {
    fn drop(&mut self) {
        if !self.is_flushed {
            warn!(
                "You forgot to flush {:?} before its writer got Drop. Do not rely on drop. This \
                 also occurs when the indexer crashed, so you may want to check the logs for the \
                 root cause.",
                self.path
            )
        }
    }
}

impl Write for VecWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.is_flushed = false;
        self.data.write_all(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.is_flushed = true;
        Ok(())
    }
}

impl TerminatingWrite for VecWriter {
    fn terminate_ref(&mut self, _: AntiCallToken) -> io::Result<()> {
        self.flush()
    }
}

// InvertedIndexDirectory holds all indexed data for tantivy queries to search for relevant data.
// The data is read-only, write and delete operations will be ignored and return success directly.
#[derive(Clone, Debug)]
pub struct InvertedIndexDirectory {
    meta_path: PathBuf,
    managed_path: PathBuf,

    fast_data: OwnedBytes,
    store_data: OwnedBytes,
    fieldnorm_data: OwnedBytes,
    pos_data: OwnedBytes,
    idx_data: OwnedBytes,
    term_data: OwnedBytes,
    meta_data: OwnedBytes,
    managed_data: OwnedBytes,
}

impl InvertedIndexDirectory {
    pub fn try_create(files: Vec<Arc<InvertedIndexFile>>) -> Result<Self> {
        let mut file_map = BTreeMap::<String, OwnedBytes>::new();

        for file in files.into_iter() {
            let file = Arc::unwrap_or_clone(file.clone());
            if file.data.is_empty() {
                continue;
            }
            file_map.insert(file.name, file.data);
        }

        let fast_data = file_map.remove("fast").unwrap();
        let store_data = file_map.remove("store").unwrap();
        let fieldnorm_data = file_map.remove("fieldnorm").unwrap();
        let pos_data = file_map.remove("pos").unwrap();
        let idx_data = file_map.remove("idx").unwrap();
        let term_data = file_map.remove("term").unwrap();
        let meta_data = file_map.remove("meta.json").unwrap();
        let managed_data = file_map.remove(".managed.json").unwrap();

        let meta_path = PathBuf::from("meta.json");
        let managed_path = PathBuf::from(".managed.json");

        Ok(Self {
            meta_path,
            managed_path,

            fast_data,
            store_data,
            fieldnorm_data,
            pos_data,
            idx_data,
            term_data,
            meta_data,
            managed_data,
        })
    }

    pub fn size(&self) -> usize {
        self.fast_data.len()
            + self.store_data.len()
            + self.fieldnorm_data.len()
            + self.pos_data.len()
            + self.idx_data.len()
            + self.term_data.len()
            + self.meta_data.len()
            + self.managed_data.len()
            + self.meta_path.capacity()
            + self.managed_path.capacity()
    }
}

impl Directory for InvertedIndexDirectory {
    fn get_file_handle(&self, path: &Path) -> result::Result<Arc<dyn FileHandle>, OpenReadError> {
        let file_slice = self.open_read(path)?;
        Ok(Arc::new(file_slice))
    }

    fn open_read(&self, path: &Path) -> result::Result<FileSlice, OpenReadError> {
        if path == self.meta_path.as_path() {
            return Ok(FileSlice::new(Arc::new(self.meta_data.clone())));
        } else if path == self.managed_path.as_path() {
            return Ok(FileSlice::new(Arc::new(self.managed_data.clone())));
        }

        if let Some(ext) = path.extension() {
            let bytes = match ext.to_str() {
                Some("term") => self.term_data.clone(),
                Some("idx") => self.idx_data.clone(),
                Some("pos") => self.pos_data.clone(),
                Some("fieldnorm") => self.fieldnorm_data.clone(),
                Some("store") => self.store_data.clone(),
                Some("fast") => self.fast_data.clone(),
                _ => {
                    return Err(OpenReadError::FileDoesNotExist(PathBuf::from(path)));
                }
            };
            Ok(FileSlice::new(Arc::new(bytes)))
        } else {
            Err(OpenReadError::FileDoesNotExist(PathBuf::from(path)))
        }
    }

    fn delete(&self, _path: &Path) -> result::Result<(), DeleteError> {
        Ok(())
    }

    fn exists(&self, path: &Path) -> result::Result<bool, OpenReadError> {
        if path == self.meta_path.as_path() || path == self.managed_path.as_path() {
            return Ok(true);
        }
        if let Some(ext) = path.extension() {
            match ext.to_str() {
                Some("term") => Ok(true),
                Some("idx") => Ok(true),
                Some("pos") => Ok(true),
                Some("fieldnorm") => Ok(true),
                Some("store") => Ok(true),
                Some("fast") => Ok(true),
                _ => Ok(false),
            }
        } else {
            Ok(false)
        }
    }

    fn open_write(&self, path: &Path) -> result::Result<WritePtr, OpenWriteError> {
        let path_buf = PathBuf::from(path);
        let vec_writer = VecWriter::new(path_buf);
        Ok(BufWriter::new(Box::new(vec_writer)))
    }

    fn atomic_read(&self, path: &Path) -> result::Result<Vec<u8>, OpenReadError> {
        let bytes =
            self.open_read(path)?
                .read_bytes()
                .map_err(|io_error| OpenReadError::IoError {
                    io_error: Arc::new(io_error),
                    filepath: path.to_path_buf(),
                })?;
        Ok(bytes.as_slice().to_owned())
    }

    fn atomic_write(&self, _path: &Path, _data: &[u8]) -> io::Result<()> {
        Ok(())
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        let watch_handle = WatchHandle::new(Arc::new(watch_callback));
        Ok(watch_handle)
    }

    fn sync_directory(&self) -> io::Result<()> {
        Ok(())
    }
}

impl Versioned<0> for InvertedIndexFile {}

pub enum InvertedIndexFileVersion {
    V0(PhantomData<InvertedIndexFile>),
}

impl TryFrom<u64> for InvertedIndexFileVersion {
    type Error = ErrorCode;
    fn try_from(value: u64) -> std::result::Result<Self, Self::Error> {
        match value {
            0 => Ok(InvertedIndexFileVersion::V0(testify_version::<_, 0>(
                PhantomData,
            ))),
            _ => Err(ErrorCode::Internal(format!(
                "unknown inverted index file version {value}, versions supported: 0"
            ))),
        }
    }
}
