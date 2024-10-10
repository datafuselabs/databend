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

use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_metrics::storage::*;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::TableDataCacheKey;
use databend_storages_common_io::MergeIOReader;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::ColumnMeta;
use opendal::Operator;

use crate::io::BlockReader;
use crate::BlockReadResult;

impl BlockReader {
    #[async_backtrace::framed]
    pub async fn read_columns_data_by_merge_io(
        &self,
        settings: &ReadSettings,
        location: &str,
        columns_meta: &HashMap<ColumnId, ColumnMeta>,
        ignore_column_ids: &Option<HashSet<ColumnId>>,
    ) -> Result<BlockReadResult> {
        // Perf
        {
            metrics_inc_remote_io_read_parts(1);
        }

        let mut ranges = vec![];
        // for async read, try using table data cache (if enabled in settings)
        let column_data_cache = CacheManager::instance().get_table_data_cache();
        let column_array_cache = CacheManager::instance().get_table_data_array_cache();
        let mut cached_column_data = vec![];
        let mut cached_column_array = vec![];
        for (_index, (column_id, ..)) in self.project_indices.iter() {
            if let Some(ignore_column_ids) = ignore_column_ids {
                if ignore_column_ids.contains(column_id) {
                    continue;
                }
            }

            if let Some(column_meta) = columns_meta.get(column_id) {
                let (offset, len) = column_meta.offset_length();

                let column_cache_key = TableDataCacheKey::new(location, *column_id, offset, len);

                // first, check in memory table data cache
                // column_array_cache
                if let Some(cache_array) = column_array_cache.get_sized(&column_cache_key, len) {
                    cached_column_array.push((*column_id, cache_array));
                    continue;
                }

                // and then, check on disk table data cache
                if let Some(cached_column_raw_data) =
                    column_data_cache.get_sized(&column_cache_key, len)
                {
                    cached_column_data.push((*column_id, cached_column_raw_data));
                    continue;
                }

                // if all caches missed, prepare the ranges to be read
                ranges.push((*column_id, offset..(offset + len)));

                // Perf
                {
                    metrics_inc_remote_io_seeks(1);
                    metrics_inc_remote_io_read_bytes(len);
                }
            }
        }

        let merge_io_result =
            MergeIOReader::merge_io_read(settings, self.operator.clone(), location, &ranges)
                .await?;

        if self.put_cache {
            let table_data_cache = CacheManager::instance().get_table_data_cache();
            // add raw data (compressed raw bytes) to column cache
            for (column_id, (chunk_idx, range)) in &merge_io_result.columns_chunk_offsets {
                let cache_key = TableDataCacheKey::new(
                    &merge_io_result.block_path,
                    *column_id,
                    range.start as u64,
                    (range.end - range.start) as u64,
                );
                let chunk_data = merge_io_result
                    .owner_memory
                    .get_chunk(*chunk_idx, &merge_io_result.block_path)?;
                let data = chunk_data.slice(range.clone());
                table_data_cache.insert(cache_key.as_ref().to_owned(), data);
            }
        }

        let block_read_res =
            BlockReadResult::create(merge_io_result, cached_column_data, cached_column_array);

        self.report_cache_metrics(&block_read_res, ranges.iter().map(|(_, r)| r));

        Ok(block_read_res)
    }

    #[inline]
    #[async_backtrace::framed]
    async fn read_range(
        op: Operator,
        path: &str,
        index: usize,
        start: u64,
        end: u64,
    ) -> Result<(usize, Vec<u8>)> {
        let chunk = op.read_with(path).range(start..end).await?;
        Ok((index, chunk.to_vec()))
    }
}
