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
use std::ops::Range;

use bytes::Bytes;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;

type ChunkIndex = usize;
pub struct OwnerMemory {
    pub chunks: HashMap<ChunkIndex, Bytes>,
}

impl OwnerMemory {
    pub fn create(chunks: Vec<(ChunkIndex, Vec<u8>)>) -> OwnerMemory {
        let chunks = chunks
            .into_iter()
            .map(|(idx, chunk)| (idx, Bytes::from(chunk)))
            .collect();
        OwnerMemory { chunks }
    }

    pub fn get_chunk(&self, index: ChunkIndex, path: &str) -> Result<Bytes> {
        match self.chunks.get(&index) {
            Some(chunk) => Ok(chunk.clone()),
            None => Err(ErrorCode::Internal(format!(
                "It's a terrible bug, not found range data, merged_range_idx:{}, path:{}",
                index, path
            ))),
        }
    }
}

pub struct MergeIOReadResult {
    pub block_path: String,
    pub columns_chunk_offsets: HashMap<ColumnId, (ChunkIndex, Range<usize>)>,
    pub owner_memory: OwnerMemory,
}

impl MergeIOReadResult {
    pub fn create(
        owner_memory: OwnerMemory,
        columns_chunk_offsets: HashMap<ColumnId, (ChunkIndex, Range<usize>)>,
        path: String,
    ) -> MergeIOReadResult {
        Self {
            block_path: path,
            columns_chunk_offsets,
            owner_memory,
        }
    }
}
