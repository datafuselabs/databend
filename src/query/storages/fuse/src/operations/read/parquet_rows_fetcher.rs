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

use databend_common_base::runtime::execute_futures_in_parallel;
use databend_common_catalog::plan::block_idx_in_segment;
use databend_common_catalog::plan::split_prefix;
use databend_common_catalog::plan::split_row_id;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_storage::ColumnNodes;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::TableSnapshot;
use itertools::Itertools;

use super::fuse_rows_fetcher::RowsFetcher;
use crate::io::BlockReader;
use crate::io::CompactSegmentInfoReader;
use crate::io::MetaReaders;
use crate::BlockReadResult;
use crate::FuseBlockPartInfo;
use crate::FuseTable;

pub(super) struct ParquetRowsFetcher<const BLOCKING_IO: bool> {
    snapshot: Option<Arc<TableSnapshot>>,
    table: Arc<FuseTable>,
    segment_reader: CompactSegmentInfoReader,
    projection: Projection,
    schema: TableSchemaRef,

    settings: ReadSettings,
    reader: Arc<BlockReader>,
    part_map: HashMap<u64, PartInfoPtr>,
    segment_blocks_cache: HashMap<u64, Vec<Arc<BlockMeta>>>,

    // To control the parallelism of fetching blocks.
    max_threads: usize,
}

#[async_trait::async_trait]
impl<const BLOCKING_IO: bool> RowsFetcher for ParquetRowsFetcher<BLOCKING_IO> {
    #[async_backtrace::framed]
    async fn on_start(&mut self) -> Result<()> {
        self.snapshot = self.table.read_table_snapshot().await?;
        Ok(())
    }

    #[async_backtrace::framed]
    async fn fetch(&mut self, row_ids: &[u64]) -> Result<DataBlock> {
        self.prepare_part_map(row_ids).await?;

        let num_rows = row_ids.len();
        let mut part_set = HashSet::new();
        let mut row_set = Vec::with_capacity(num_rows);
        for row_id in row_ids {
            let (prefix, idx) = split_row_id(*row_id);
            part_set.insert(prefix);
            row_set.push((prefix, idx));
        }

        // Read blocks in `prefix` order.
        let part_set = part_set.into_iter().sorted().collect::<Vec<_>>();
        let idx_map = part_set
            .iter()
            .enumerate()
            .map(|(i, p)| (*p, i))
            .collect::<HashMap<_, _>>();
        // parts_per_thread = num_parts / max_threads
        // remain = num_parts % max_threads
        // task distribution:
        //   Part number of each task   |       Task number
        // ------------------------------------------------------
        //    parts_per_thread + 1      |         remain
        //      parts_per_thread        |   max_threads - remain
        let num_parts = part_set.len();
        let mut tasks = Vec::with_capacity(self.max_threads);
        // Fetch blocks in parallel.
        let part_size = num_parts / self.max_threads;
        let remainder = num_parts % self.max_threads;
        let mut begin = 0;
        for i in 0..self.max_threads {
            let end = if i < remainder {
                begin + part_size + 1
            } else {
                begin + part_size
            };
            if begin == end {
                break;
            }
            let parts = part_set[begin..end]
                .iter()
                .map(|idx| self.part_map[idx].clone())
                .collect::<Vec<_>>();
            tasks.push(Self::fetch_blocks(
                self.reader.clone(),
                parts,
                self.settings,
            ));
            begin = end;
        }

        let num_task = tasks.len();
        let blocks = execute_futures_in_parallel(
            tasks,
            num_task,
            num_task * 2,
            "parqeut rows fetch".to_string(),
        )
        .await?
        .into_iter()
        .collect::<Result<Vec<_>>>()?
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
        // Take result rows from blocks.
        let indices = row_set
            .iter()
            .map(|(prefix, row_idx)| {
                let block_idx = idx_map[prefix];
                (block_idx as u32, *row_idx as u32, 1_usize)
            })
            .collect::<Vec<_>>();

        Ok(DataBlock::take_blocks(&blocks, &indices, num_rows))
    }

    fn schema(&self) -> DataSchema {
        self.reader.data_schema()
    }
}

impl<const BLOCKING_IO: bool> ParquetRowsFetcher<BLOCKING_IO> {
    pub fn create(
        table: Arc<FuseTable>,
        projection: Projection,
        reader: Arc<BlockReader>,
        settings: ReadSettings,
        max_threads: usize,
    ) -> Self {
        let schema = table.schema();
        let segment_reader =
            MetaReaders::segment_info_reader(table.operator.clone(), schema.clone());
        Self {
            table,
            snapshot: None,
            segment_reader,
            projection,
            schema,
            reader,
            settings,
            part_map: HashMap::new(),
            segment_blocks_cache: HashMap::new(),
            max_threads,
        }
    }

    async fn prepare_part_map(&mut self, row_ids: &[u64]) -> Result<()> {
        let snapshot = self.snapshot.as_ref().unwrap();

        let arrow_schema = self.schema.as_ref().into();
        let column_nodes = ColumnNodes::new_from_schema(&arrow_schema, Some(&self.schema));

        for row_id in row_ids {
            let (prefix, _) = split_row_id(*row_id);

            if self.part_map.contains_key(&prefix) {
                continue;
            }

            let (segment, block) = split_prefix(prefix);
            if let std::collections::hash_map::Entry::Vacant(e) =
                self.segment_blocks_cache.entry(segment)
            {
                let (location, ver) = snapshot.segments[segment as usize].clone();
                let compact_segment_info = self
                    .segment_reader
                    .read(&LoadParams {
                        ver,
                        location,
                        len_hint: None,
                        put_cache: true,
                    })
                    .await?;

                let blocks = compact_segment_info.block_metas()?;
                e.insert(blocks);
            }

            let blocks = self.segment_blocks_cache.get(&segment).unwrap();
            let block_idx = block_idx_in_segment(blocks.len(), block as usize);
            let block_meta = &blocks[block_idx];
            let part_info = FuseTable::projection_part(
                block_meta,
                &None,
                &column_nodes,
                None,
                &self.projection,
            );

            self.part_map.insert(prefix, part_info);
        }

        Ok(())
    }

    #[async_backtrace::framed]
    async fn fetch_blocks(
        reader: Arc<BlockReader>,
        parts: Vec<PartInfoPtr>,
        settings: ReadSettings,
    ) -> Result<Vec<DataBlock>> {
        let mut chunks = Vec::with_capacity(parts.len());
        if BLOCKING_IO {
            for part in parts.iter() {
                let chunk = reader.sync_read_columns_data_by_merge_io(&settings, part, &None)?;
                chunks.push(chunk);
            }
        } else {
            for part in parts.iter() {
                let part = FuseBlockPartInfo::from_part(part)?;
                let chunk = reader
                    .read_columns_data_by_merge_io(
                        &settings,
                        &part.location,
                        &part.columns_meta,
                        &None,
                    )
                    .await?;
                chunks.push(chunk);
            }
        }
        let fetched_blocks = chunks
            .into_iter()
            .zip(parts.iter())
            .map(|(chunk, part)| Self::build_block(&reader, part, chunk))
            .collect::<Result<Vec<_>>>()?;

        Ok(fetched_blocks)
    }

    fn build_block(
        reader: &BlockReader,
        part: &PartInfoPtr,
        chunk: BlockReadResult,
    ) -> Result<DataBlock> {
        let columns_chunks = chunk.columns_chunks()?;
        let part = FuseBlockPartInfo::from_part(part)?;
        reader.deserialize_parquet_chunks(
            part.nums_rows,
            &part.columns_meta,
            columns_chunks,
            &part.compression,
            &part.location,
        )
    }
}
