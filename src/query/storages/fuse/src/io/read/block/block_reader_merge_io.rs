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
use std::sync::Arc;

use bytes::Bytes;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_storages_common_cache::SizedColumnArray;
use databend_storages_common_io::MergeIOReadResult;
use enum_as_inner::EnumAsInner;

type CachedColumnData = Vec<(ColumnId, Arc<Bytes>)>;
type CachedColumnArray = Vec<(ColumnId, Arc<SizedColumnArray>)>;

#[derive(EnumAsInner)]
pub enum DataItem<'a> {
    RawData(Bytes),
    ColumnArray(&'a Arc<SizedColumnArray>),
}

pub struct BlockReadResult {
    merge_io_result: MergeIOReadResult,
    pub(crate) cached_column_data: CachedColumnData,
    pub(crate) cached_column_array: CachedColumnArray,
}

impl BlockReadResult {
    pub fn create(
        merge_io_result: MergeIOReadResult,
        cached_column_data: CachedColumnData,
        cached_column_array: CachedColumnArray,
    ) -> BlockReadResult {
        BlockReadResult {
            merge_io_result,
            cached_column_data,
            cached_column_array,
        }
    }

    pub fn columns_chunks(&self) -> Result<HashMap<ColumnId, DataItem>> {
        let mut res = HashMap::with_capacity(self.merge_io_result.columns_chunk_offsets.len());

        // merge column data fetched from object storage
        for (column_id, (chunk_idx, range)) in &self.merge_io_result.columns_chunk_offsets {
            let chunk = self
                .merge_io_result
                .owner_memory
                .get_chunk(*chunk_idx, &self.merge_io_result.block_path)?;
            res.insert(*column_id, DataItem::RawData(chunk.slice(range.clone())));
        }

        // merge column data from cache
        for (column_id, data) in &self.cached_column_data {
            let data = data.as_ref();
            res.insert(*column_id, DataItem::RawData(data.clone()));
        }

        // merge column array from cache
        for (column_id, data) in &self.cached_column_array {
            res.insert(*column_id, DataItem::ColumnArray(data));
        }

        Ok(res)
    }

    pub fn column_buffers(&self) -> Result<HashMap<ColumnId, Bytes>> {
        let mut res = HashMap::with_capacity(self.merge_io_result.columns_chunk_offsets.len());

        // merge column data fetched from object storage
        for (column_id, (chunk_idx, range)) in &self.merge_io_result.columns_chunk_offsets {
            let chunk = self
                .merge_io_result
                .owner_memory
                .get_chunk(*chunk_idx, &self.merge_io_result.block_path)?;
            res.insert(*column_id, chunk.slice(range.clone()));
        }

        // merge column data from cache
        for (column_id, data) in &self.cached_column_data {
            let data = data.as_ref();
            res.insert(*column_id, data.clone());
        }

        Ok(res)
    }
}
