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

use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::HashMethodKind;
use common_functions::BUILTIN_FUNCTIONS;
use common_storage::DataOperator;
use parking_lot::RwLock;

use crate::pipelines::processors::transforms::group_by::KeysColumnIter;
use crate::pipelines::processors::transforms::group_by::PolymorphicKeysHelper;
use crate::pipelines::processors::transforms::hash_join::BuildSpillCoordinator;
use crate::pipelines::processors::transforms::hash_join::HashJoinBuildState;
use crate::sessions::QueryContext;
use crate::spiller::Spiller;
use crate::spiller::SpillerConfig;
use crate::spiller::SpillerType;

/// Define some states for hash join build spilling
pub struct BuildSpillState {
    /// Hash join build state
    pub(crate) build_state: Arc<HashJoinBuildState>,
    /// Spilling memory threshold
    pub(crate) spill_memory_threshold: usize,
    /// Hash join build spilling coordinator
    pub(crate) spill_coordinator: Arc<BuildSpillCoordinator>,
    /// Spiller, responsible for specific spill work
    pub(crate) spiller: Spiller,
}

impl BuildSpillState {
    pub fn create(
        _ctx: Arc<QueryContext>,
        spill_coordinator: Arc<BuildSpillCoordinator>,
        build_state: Arc<HashJoinBuildState>,
    ) -> Self {
        let spill_config = SpillerConfig::create("_hash_join_build_spill".to_string());
        let operator = DataOperator::instance().operator();
        let spiller = Spiller::create(operator, spill_config, SpillerType::HashJoin);
        Self {
            build_state,
            spill_memory_threshold: 1024,
            spill_coordinator,
            spiller,
        }
    }
}

/// Define some spill-related APIs for hash join build
impl BuildSpillState {
    #[async_backtrace::framed]
    // Start to spill `input_data`.
    // Todo: add unit tests for the method.
    pub(crate) async fn spill(&mut self) -> Result<()> {
        let mut rows_size = 0;
        self.spiller.input_data.iter().for_each(|block| {
            rows_size += block.num_rows();
        });
        // Hash values for all rows in input data.
        let mut hashes = Vec::with_capacity(rows_size);
        let block = DataBlock::concat(&self.spiller.input_data)?;
        self.get_hashes(&block, &mut hashes)?;
        // Ensure which partition each row belongs to. Currently we limit the maximum number of partitions to 8.
        // We read the first 3 bits of the hash value as the partition id.

        // partition_id -> row indexes
        let mut partition_map = HashMap::with_capacity(8);
        for (row_idx, hash) in hashes.iter().enumerate() {
            let partition_id = *hash as u8 & 0b0000_0111;
            partition_map
                .entry(partition_id)
                .and_modify(|v: &mut Vec<usize>| v.push(row_idx))
                .or_insert(vec![row_idx]);
        }

        // Take row indexes from block and put them to Spiller `partitions`
        for (partition_id, row_indexes) in partition_map.iter() {
            self.spiller.partition_set.push(*partition_id);
            let block_row_indexes = row_indexes
                .iter()
                .map(|idx| (0 as u32, *idx as u32, 1 as usize))
                .collect::<Vec<_>>();
            let partition_block =
                DataBlock::take_blocks(&[block.clone()], &block_row_indexes, row_indexes.len());
            self.spiller
                .partitions
                .push((*partition_id, partition_block));
        }

        self.spiller.spill().await
    }

    // Get all hashes for input data.
    fn get_hashes(&self, block: &DataBlock, hashes: &mut Vec<u64>) -> Result<()> {
        let func_ctx = self.build_state.ctx.get_function_context()?;
        let mut evaluator = Evaluator::new(block, &func_ctx, &BUILTIN_FUNCTIONS);
        // Use the first column as the key column to generate hash
        let first_build_key = &self.build_state.hash_join_state.hash_join_desc.build_keys[0];
        let build_key_column = evaluator.run(first_build_key)?;
        let build_key_column = build_key_column.as_column().unwrap();
        // Todo: simplify the following code
        match &*self.build_state.method {
            HashMethodKind::Serializer(method) => {
                let rows_iter = method.keys_iter_from_column(&build_key_column)?;
                for row in rows_iter.iter() {
                    hashes.push(method.get_hash(row));
                }
            }
            HashMethodKind::DictionarySerializer(method) => {
                let rows_iter = method.keys_iter_from_column(&build_key_column)?;
                for row in rows_iter.iter() {
                    hashes.push(method.get_hash(row));
                }
            }
            HashMethodKind::SingleString(method) => {
                let rows_iter = method.keys_iter_from_column(&build_key_column)?;
                for row in rows_iter.iter() {
                    hashes.push(method.get_hash(row));
                }
            }
            HashMethodKind::KeysU8(method) => {
                let rows_iter = method.keys_iter_from_column(&build_key_column)?;
                for row in rows_iter.iter() {
                    hashes.push(method.get_hash(row));
                }
            }
            HashMethodKind::KeysU16(method) => {
                let rows_iter = method.keys_iter_from_column(&build_key_column)?;
                for row in rows_iter.iter() {
                    hashes.push(method.get_hash(row));
                }
            }
            HashMethodKind::KeysU32(method) => {
                let rows_iter = method.keys_iter_from_column(&build_key_column)?;
                for row in rows_iter.iter() {
                    hashes.push(method.get_hash(row));
                }
            }
            HashMethodKind::KeysU64(method) => {
                let rows_iter = method.keys_iter_from_column(&build_key_column)?;
                for row in rows_iter.iter() {
                    hashes.push(method.get_hash(row));
                }
            }
            HashMethodKind::KeysU128(method) => {
                let rows_iter = method.keys_iter_from_column(&build_key_column)?;
                for row in rows_iter.iter() {
                    hashes.push(method.get_hash(row));
                }
            }
            HashMethodKind::KeysU256(method) => {
                let rows_iter = method.keys_iter_from_column(&build_key_column)?;
                for row in rows_iter.iter() {
                    hashes.push(method.get_hash(row));
                }
            }
        }
        Ok(())
    }

    // Check if need to spill.
    // Notes: even if input can fit into memory, but there exists one processor need to spill, then it needs to wait spill.
    pub(crate) fn check_need_spill(&self, input: &DataBlock) -> Result<bool> {
        let settings = self.build_state.ctx.get_settings();
        let enable_spill = settings.get_enable_join_spill()?;
        let spill_threshold = settings.get_join_spilling_threshold()?;
        if !enable_spill || self.spiller.is_all_spilled() {
            return Ok(false);
        }

        if self.spiller.input_data.is_empty() {
            return Ok(false);
        }

        let mut total_bytes = input.memory_size();

        for block in self
            .build_state
            .hash_join_state
            .row_space
            .buffer
            .read()
            .iter()
        {
            total_bytes += block.memory_size();
        }

        let chunks = unsafe { &*self.build_state.hash_join_state.chunks.get() };
        for block in chunks.iter() {
            total_bytes += block.memory_size();
        }

        if total_bytes > spill_threshold {
            return Ok(true);
        }

        Ok(false)
    }

    #[async_backtrace::framed]
    // Directly spill input data without buffering.
    // Return unspilled data.
    pub(crate) async fn spill_input(&mut self, data_block: DataBlock) -> Result<DataBlock> {
        // Save the row index which is not spilled.
        let mut unspilled_row_index = Vec::with_capacity(data_block.num_rows());
        // Compute the hash value for each row.
        let mut hashes = Vec::with_capacity(data_block.num_rows());
        self.get_hashes(&data_block, &mut hashes)?;
        // Key is partition, value is row indexes
        let mut partition_rows = HashMap::new();
        // Classify rows to spill or not spill.
        for (row_idx, hash) in hashes.iter().enumerate() {
            let partition_id = *hash as u8 & 0b0000_0111;
            if self.spiller.spilled_partition_set.contains(&partition_id) {
                // the row can be directly spilled to corresponding partition
                partition_rows
                    .entry(partition_id)
                    .and_modify(|v: &mut Vec<usize>| v.push(row_idx))
                    .or_insert(vec![row_idx]);
            } else {
                unspilled_row_index.push(row_idx);
            }
        }
        for (p_id, row_indexes) in partition_rows.iter() {
            let block_row_indexes = row_indexes
                .iter()
                .map(|idx| (0 as u32, *idx as u32, 1 as usize))
                .collect::<Vec<_>>();
            let block = DataBlock::take_blocks(
                &[data_block.clone()],
                &block_row_indexes,
                row_indexes.len(),
            );
            // Spill block with partition id
            self.spiller.spill_with_partition(p_id, &block).await?;
        }
        // Return unspilled data
        let unspilled_block_row_indexes = unspilled_row_index
            .iter()
            .map(|idx| (0 as u32, *idx as u32, 1 as usize))
            .collect::<Vec<_>>();
        Ok(DataBlock::take_blocks(
            &[data_block.clone()],
            &unspilled_block_row_indexes,
            unspilled_row_index.len(),
        ))
    }

    // Buffer the input data for the join build processor
    // It will be spilled when the memory usage exceeds threshold.
    // If data isn't in partition set, it'll be used to build hash table.
    pub(crate) fn buffer_data(&mut self, input: DataBlock) {
        self.spiller.input_data.push(input);
    }
}
