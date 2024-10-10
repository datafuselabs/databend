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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::F32;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_search_milliseconds;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use opendal::Operator;
use tantivy::collector::DocSetCollector;
use tantivy::collector::TopDocs;
use tantivy::directory::FileSlice;
use tantivy::directory::OwnedBytes;
use tantivy::positions::PositionReader;
use tantivy::postings::BlockSegmentPostings;
use tantivy::query::Query;
use tantivy::query::QueryClone;
use tantivy::schema::IndexRecordOption;
use tantivy::termdict::TermInfoStore;
use tantivy::tokenizer::TokenizerManager;
use tantivy::Index;
use tantivy_fst::raw::Fst;

use crate::index::DocIdsCollector;
use crate::io::read::inverted_index::inverted_index_loader::cache_key_of_index_columns;
use crate::io::read::inverted_index::inverted_index_loader::load_inverted_index_directory;
use crate::io::read::inverted_index::inverted_index_loader::load_inverted_index_files;
use crate::io::read::inverted_index::inverted_index_loader::load_inverted_index_meta;

#[derive(Clone)]
pub struct InvertedIndexReader {
    dal: Operator,
}

impl InvertedIndexReader {
    pub fn create(dal: Operator) -> Self {
        Self { dal }
    }

    // Filter the rows and scores in the block that can match the query text,
    // if there is no row that can match, this block can be pruned.
    #[allow(clippy::type_complexity)]
    #[allow(clippy::too_many_arguments)]
    pub async fn do_filter(
        self,
        settings: &ReadSettings,
        need_position: bool,
        has_score: bool,
        query: Box<dyn Query>,
        field_ids: &HashSet<u32>,
        index_record: &IndexRecordOption,
        fuzziness: &Option<u8>,
        tokenizer_manager: TokenizerManager,
        row_count: u64,
        index_loc: &str,
    ) -> Result<Option<Vec<(usize, Option<F32>)>>> {
        let start = Instant::now();

        let matched_rows = self
            .search(
                settings,
                index_loc,
                query,
                field_ids,
                need_position,
                has_score,
                index_record,
                fuzziness,
                tokenizer_manager,
                row_count,
            )
            .await?;

        // Perf.
        {
            metrics_inc_block_inverted_index_search_milliseconds(start.elapsed().as_millis() as u64);
        }

        Ok(matched_rows)
    }

    // legacy query search function, using tantivy searcher.
    async fn legacy_search<'a>(
        &self,
        settings: &ReadSettings,
        index_path: &'a str,
        query: Box<dyn Query>,
        has_score: bool,
        tokenizer_manager: TokenizerManager,
        row_count: u64,
        inverted_index_meta_map: HashMap<String, SingleColumnMeta>,
    ) -> Result<Option<Vec<(usize, Option<F32>)>>> {
        let directory =
            load_inverted_index_directory(settings, index_path, &self.dal, inverted_index_meta_map)
                .await?;

        let mut index = Index::open(directory)?;
        index.set_tokenizers(tokenizer_manager);
        let reader = index.reader()?;
        let searcher = reader.searcher();

        let matched_rows = if has_score {
            let collector = TopDocs::with_limit(row_count as usize);
            let docs = searcher.search(&query, &collector)?;

            let mut matched_rows = Vec::with_capacity(docs.len());
            for (score, doc_addr) in docs {
                let doc_id = doc_addr.doc_id as usize;
                let score = F32::from(score);
                matched_rows.push((doc_id, Some(score)));
            }
            matched_rows
        } else {
            let collector = DocSetCollector;
            let docs = searcher.search(&query, &collector)?;

            let mut matched_rows = Vec::with_capacity(docs.len());
            for doc_addr in docs {
                let doc_id = doc_addr.doc_id as usize;
                matched_rows.push((doc_id, None));
            }
            matched_rows
        };
        if !matched_rows.is_empty() {
            Ok(Some(matched_rows))
        } else {
            Ok(None)
        }
    }

    // Follow the process below to perform the query search:
    //
    // 1. Read the `fst` first, check if the term in the query matches.
    //    If it matches, collect the terms that need to be checked
    //    for subsequent processing, otherwise, return directly
    //    and ignore the block.
    // 2. Read the `term_info` for each terms from the `term_dict`,
    //    which contains three parts:
    //    `doc_freq` is the number of docs containing the term.
    //    `postings_range` is used to read posting list in the postings (`.idx`) file.
    //    `positions_range` is used to read positions in the positions (`.pos`) file.
    // 3. Read `postings` data from postings (`.idx`) file by `postings_range`
    //    of each terms, and `positions` data from positions (`.pos`) file
    //    by `positions_range` of each terms.
    // 4. Open `BlockSegmentPostings` using `postings` data for each terms,
    //    which can be used to read `doc_ids` and `term_freqs`.
    // 5. If the query is a phrase query, Open `PositionReader` using
    //    `positions` data for each terms, which can be use to read
    //    term `positions` in each docs.
    // 6. Collect matched `doc_ids` of the query.
    //    If the query is a term query, the `doc_ids` can read from `BlockSegmentPostings`.
    //    If the query is a phrase query, in addition to `doc_ids`, also need to
    //    use `PositionReader` to read the positions for each terms and check whether
    //    the position of terms in doc is the same as the position of terms in query.
    //
    // If the term does not match, only the `fst` file needs to be read.
    // If the term matches, the `term_dict` and `postings`, `positions`
    // data of the related terms need to be read instead of all
    // the `postings` and `positions` data.
    #[allow(clippy::too_many_arguments)]
    async fn search<'a>(
        &self,
        settings: &ReadSettings,
        index_path: &'a str,
        query: Box<dyn Query>,
        field_ids: &HashSet<u32>,
        need_position: bool,
        has_score: bool,
        index_record: &IndexRecordOption,
        fuzziness: &Option<u8>,
        tokenizer_manager: TokenizerManager,
        row_count: u64,
    ) -> Result<Option<Vec<(usize, Option<F32>)>>> {
        // 1. read index meta.
        let inverted_index_meta = load_inverted_index_meta(self.dal.clone(), index_path).await?;

        let mut inverted_index_meta_map = inverted_index_meta
            .columns
            .clone()
            .into_iter()
            .collect::<HashMap<_, _>>();

        // if meta contains `meta.json` columns,
        // the index file is the first version implementation
        // use compatible search function to read.
        if inverted_index_meta_map.contains_key("meta.json") {
            return self
                .legacy_search(
                    settings,
                    index_path,
                    query,
                    has_score,
                    tokenizer_manager,
                    row_count,
                    inverted_index_meta_map,
                )
                .await;
        }

        // 2. read fst and term files.
        let mut columns = Vec::with_capacity(field_ids.len() * 2);
        for field_id in field_ids {
            let fst_col_name = format!("fst-{}", field_id);
            let term_col_name = format!("term-{}", field_id);

            if let Some(fst_col_meta) = inverted_index_meta_map.remove(&fst_col_name) {
                let fst_range = fst_col_meta.offset..(fst_col_meta.offset + fst_col_meta.len);
                columns.push((fst_col_name, fst_range));
            }
            if let Some(term_col_meta) = inverted_index_meta_map.remove(&term_col_name) {
                let term_range = term_col_meta.offset..(term_col_meta.offset + term_col_meta.len);
                columns.push((term_col_name, term_range));
            }
        }

        let column_files =
            load_inverted_index_files(settings, columns, index_path, &self.dal).await?;
        let mut column_files_map = column_files
            .into_iter()
            .map(|f| (f.name.clone(), f.data.clone()))
            .collect::<HashMap<_, _>>();

        let mut fst_maps = HashMap::with_capacity(field_ids.len());
        for field_id in field_ids {
            let fst_col_name = format!("fst-{}", field_id);
            let fst = if let Some(fst_data) = column_files_map.remove(&fst_col_name) {
                Fst::new(fst_data).map_err(|err| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Fst data is corrupted: {:?}", err),
                    )
                })?
            } else {
                // If the FST data does not exist, create an empty FST.
                // This means that the field does not have any valid terms.
                let builder = tantivy_fst::MapBuilder::memory();
                let bytes = builder.into_inner().unwrap();
                let fst_data = OwnedBytes::new(bytes);
                Fst::new(fst_data).unwrap()
            };
            let fst_map = tantivy_fst::Map::from(fst);
            fst_maps.insert(*field_id, fst_map);
        }

        // 3. check whether query is matched in the fsts.
        let mut matched_terms = HashMap::new();
        let mut prefix_terms = HashMap::new();
        let mut fuzziness_terms = HashMap::new();
        let matched = DocIdsCollector::check_term_fsts_match(
            query.box_clone(),
            &fst_maps,
            fuzziness,
            &mut matched_terms,
            &mut prefix_terms,
            &mut fuzziness_terms,
        )?;

        // if not matched, return without further check
        if !matched {
            return Ok(None);
        }

        // 4. collect term infos for each terms.
        let mut term_infos = HashMap::with_capacity(matched_terms.len());
        for field_id in field_ids {
            let term_col_name = format!("term-{}", field_id);
            if let Some(term_dict_data) = column_files_map.remove(&term_col_name) {
                let term_dict_file = FileSlice::new(Arc::new(term_dict_data));
                let term_info_store = TermInfoStore::open(term_dict_file)?;

                for (_, (term_field_id, term_id)) in matched_terms.iter() {
                    if field_id == term_field_id {
                        let term_info = term_info_store.get(*term_id);
                        term_infos.insert(*term_id, (*field_id, term_info));
                    }
                }
            }
        }

        // 5. read postings and optional positions.
        let term_slice_len = if need_position {
            term_infos.len() * 2
        } else {
            term_infos.len()
        };
        let mut slice_columns = Vec::with_capacity(term_slice_len);
        let mut slice_name_map = HashMap::with_capacity(term_slice_len);
        for (term_id, (field_id, term_info)) in term_infos.iter() {
            let idx_name = format!("idx-{}", field_id);
            let idx_meta = inverted_index_meta_map.get(&idx_name).ok_or_else(|| {
                ErrorCode::TantivyError(format!(
                    "inverted index column `{}` does not exist",
                    idx_name
                ))
            })?;
            // ignore 8 bytes total_num_tokens_slice
            let offset = idx_meta.offset + 8 + (term_info.postings_range.start as u64);
            let len = term_info.postings_range.len() as u64;
            let idx_slice_range = offset..(offset + len);

            let idx_slice_name = format!("{}-{}", idx_name, term_info.postings_range.start);
            slice_name_map.insert(idx_slice_name.clone(), *term_id);
            slice_columns.push((idx_slice_name, idx_slice_range));

            if need_position {
                let pos_name = format!("pos-{}", field_id);
                let pos_meta = inverted_index_meta_map.get(&pos_name).ok_or_else(|| {
                    ErrorCode::TantivyError(format!(
                        "inverted index column `{}` does not exist",
                        pos_name
                    ))
                })?;
                let offset = pos_meta.offset + (term_info.positions_range.start as u64);
                let len = term_info.positions_range.len() as u64;
                let pos_slice_range = offset..(offset + len);

                let pos_slice_name = format!("{}-{}", pos_name, term_info.positions_range.start);
                slice_name_map.insert(pos_slice_name.clone(), *term_id);
                slice_columns.push((pos_slice_name, pos_slice_range));
            }
        }

        let slice_column_files =
            load_inverted_index_files(settings, slice_columns, index_path, &self.dal).await?;
        let slice_column_files_map = slice_column_files
            .into_iter()
            .map(|f| (f.name.clone(), f.data.clone()))
            .collect::<HashMap<_, _>>();

        let mut block_postings_map = HashMap::with_capacity(term_infos.len());
        let mut position_reader_map = HashMap::with_capacity(term_infos.len());
        for (slice_name, slice_data) in slice_column_files_map.into_iter() {
            let term_id = slice_name_map.remove(&slice_name).unwrap();
            let (_, term_info) = term_infos.get(&term_id).unwrap();

            if slice_name.starts_with("idx") {
                let posting_file = FileSlice::new(Arc::new(slice_data));
                let block_postings = BlockSegmentPostings::open(
                    term_info.doc_freq,
                    posting_file,
                    *index_record,
                    *index_record,
                )?;

                block_postings_map.insert(term_id, block_postings);
            } else if slice_name.starts_with("pos") {
                let position_reader = PositionReader::open(slice_data)?;
                position_reader_map.insert(term_id, position_reader);
            }
        }

        // 6. collect matched doc ids.
        let mut collector = DocIdsCollector::create(
            row_count,
            need_position,
            matched_terms,
            term_infos,
            block_postings_map,
            position_reader_map,
        );
        let matched_doc_ids =
            collector.collect_matched_rows(query.box_clone(), &prefix_terms, &fuzziness_terms)?;

        if let Some(matched_doc_ids) = matched_doc_ids {
            if !matched_doc_ids.is_empty() {
                let mut matched_rows = Vec::with_capacity(matched_doc_ids.len() as usize);
                let doc_ids_iter = matched_doc_ids.iter();
                if has_score {
                    // TODO: add score
                    for doc_id in doc_ids_iter {
                        matched_rows.push((doc_id as usize, Some(F32::from(1.0))));
                    }
                } else {
                    for doc_id in doc_ids_iter {
                        matched_rows.push((doc_id as usize, None));
                    }
                }
                return Ok(Some(matched_rows));
            }
        }
        Ok(None)
    }

    // delegation of [inverted_index_loader::cache_key_of_index_columns]
    pub fn cache_key_of_index_columns(index_path: &str) -> Vec<String> {
        cache_key_of_index_columns(index_path)
    }
}
