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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;

// The spiller buffer will record each partition's unspilled data.
// When the buffer is full(>=8MB), it will pick the partition with the most unspilled data to spill.
#[derive(Clone)]
pub struct SpillBuffer {
    partition_data: Vec<Vec<DataBlock>>,
    buffer_size: usize,
}

impl SpillBuffer {
    pub fn create(num_partitions: usize) -> Self {
        SpillBuffer {
            partition_data: vec![Vec::new(); num_partitions],
            buffer_size: 0,
        }
    }

    // Add a partition's unspilled data to the SpillBuffer.
    pub fn add_partition_data(&mut self, partition_id: u8, data_block: DataBlock) {
        let data_size = data_block.memory_size();
        self.buffer_size += data_size;
        self.partition_data[partition_id as usize].push(data_block);
    }

    // Check if the buffer is full, if so, it will return the partition with the most unspilled data.
    pub fn pick_unspilled_partition_data(&mut self) -> Result<Option<(u8, DataBlock)>> {
        fn rows(data: &[DataBlock]) -> usize {
            data.iter().map(|block| block.num_rows()).sum()
        }
        if self.buffer_size >= 8 * 1024 * 1024 {
            // Pick the partition with the most unspilled data in `partition_data`
            let (partition_id, blocks) = self
                .partition_data
                .iter()
                .enumerate()
                .max_by_key(|(_, data_blocks)| rows(data_blocks))
                .map(|(partition_id, data_blocks)| (partition_id, data_blocks.clone()))
                .ok_or_else(|| ErrorCode::Internal("No unspilled data in the buffer"))?;
            debug_assert!(!blocks.is_empty());
            self.partition_data[partition_id].clear();
            let merged_block = DataBlock::concat(&blocks)?;
            self.buffer_size -= merged_block.memory_size();
            return Ok(Some((partition_id as u8, merged_block)));
        }
        Ok(None)
    }

    // Get partition data by partition id.
    pub fn read_partition_data(&mut self, partition_id: u8, pick: bool) -> Option<Vec<DataBlock>> {
        if !self.partition_data[partition_id as usize].is_empty() {
            let data_blocks = if pick {
                let data_blocks = self.partition_data[partition_id as usize].clone();
                self.partition_data[partition_id as usize] = vec![];
                data_blocks
            } else {
                self.partition_data[partition_id as usize].clone()
            };
            Some(data_blocks)
        } else {
            None
        }
    }

    pub fn buffered_partitions(&self) -> Vec<u8> {
        let mut partition_ids = vec![];
        for (partition_id, data) in self.partition_data.iter().enumerate() {
            if !data.is_empty() {
                partition_ids.push(partition_id as u8);
            }
        }
        partition_ids
    }

    pub fn empty_partition(&self, partition_id: u8) -> bool {
        self.partition_data[partition_id as usize].is_empty()
    }
}
