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

use std::cell::SyncUnsafeCell;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use databend_common_base::base::tokio::sync::watch;
use databend_common_base::base::tokio::sync::watch::Receiver;
use databend_common_base::base::tokio::sync::watch::Sender;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::HashMethodFixedKeys;
use databend_common_expression::HashMethodSerializer;
use databend_common_expression::HashMethodSingleBinary;
use databend_common_hashtable::BinaryHashJoinHashMap;
use databend_common_hashtable::HashJoinHashMap;
use databend_common_hashtable::HashtableKeyable;
use databend_common_sql::plans::JoinType;
use databend_common_sql::ColumnSet;
use ethnum::U256;
use parking_lot::RwLock;

use super::merge_into_hash_join_optimization::MergeIntoState;
use crate::pipelines::processors::transforms::hash_join::build_state::BuildState;
use crate::pipelines::processors::transforms::hash_join::row::RowSpace;
use crate::pipelines::processors::transforms::hash_join::transform_hash_join_build::HashTableType;
use crate::pipelines::processors::transforms::hash_join::util::build_schema_wrap_nullable;
use crate::pipelines::processors::HashJoinDesc;
use crate::sessions::QueryContext;
use crate::sql::IndexType;

pub struct SerializerHashJoinHashTable {
    pub(crate) hash_table: BinaryHashJoinHashMap,
    pub(crate) hash_method: HashMethodSerializer,
}

pub struct SingleBinaryHashJoinHashTable {
    pub(crate) hash_table: BinaryHashJoinHashMap,
    pub(crate) hash_method: HashMethodSingleBinary,
}

pub struct FixedKeyHashJoinHashTable<T: HashtableKeyable> {
    pub(crate) hash_table: HashJoinHashMap<T>,
    pub(crate) hash_method: HashMethodFixedKeys<T>,
}

pub enum HashJoinHashTable {
    Null,
    Serializer(SerializerHashJoinHashTable),
    SingleBinary(SingleBinaryHashJoinHashTable),
    KeysU8(FixedKeyHashJoinHashTable<u8>),
    KeysU16(FixedKeyHashJoinHashTable<u16>),
    KeysU32(FixedKeyHashJoinHashTable<u32>),
    KeysU64(FixedKeyHashJoinHashTable<u64>),
    KeysU128(FixedKeyHashJoinHashTable<u128>),
    KeysU256(FixedKeyHashJoinHashTable<U256>),
}

/// Define some shared states for hash join build and probe.
/// It will like a bridge to connect build and probe.
/// Such as build side will pass hash table to probe side by it
pub struct HashJoinState {
    /// A shared big hash table stores all the rows from build side
    pub(crate) hash_table: SyncUnsafeCell<HashJoinHashTable>,
    /// After HashTable is built, send message to notify all probe processors.
    /// There are three types of messages:
    /// 1. FirstRound: it is the first time the hash table is constructed.
    /// 2. Restored: the hash table is restored from the spilled data.
    /// 3. Empty: the hash table is empty.
    pub(crate) build_watcher: Sender<HashTableType>,
    /// A dummy receiver to make build done watcher channel open
    pub(crate) _build_done_dummy_receiver: Receiver<HashTableType>,
    /// Some description of hash join. Such as join type, join keys, etc.
    pub(crate) hash_join_desc: HashJoinDesc,
    /// Interrupt the build phase or probe phase.
    pub(crate) interrupt: AtomicBool,
    /// If there is no data in build side, maybe we can fast return.
    pub(crate) fast_return: AtomicBool,
    /// Use the column of probe side to construct build side column.
    /// (probe index, (is probe column nullable, is build column nullable))
    pub(crate) probe_to_build: Vec<(usize, (bool, bool))>,
    /// `RowSpace` contains all rows from build side.
    pub(crate) row_space: RowSpace,
    /// `BuildState` contains all data used in probe phase.
    pub(crate) build_state: SyncUnsafeCell<BuildState>,

    /// Spill related states.
    /// It record whether spill has happened.
    pub(crate) is_spill_happened: AtomicBool,
    /// Spilled partition set, it contains all spilled partition sets from all build processors.
    pub(crate) spilled_partitions: RwLock<HashSet<u8>>,
    /// Spill partition bits, it is used to calculate the number of partitions.
    pub(crate) spill_partition_bits: usize,
    /// The next partition id to be restored.
    pub(crate) partition_id: AtomicU8,
    /// Whether need next round, if it is true, restore data from spilled data and start next round.
    pub(crate) need_next_round: AtomicBool,
    /// Send message to notify all build processors to next round.
    /// Initial message is false, send true to wake up all build processors.
    pub(crate) continue_build_watcher: Sender<bool>,
    /// A dummy receiver to make continue build watcher channel open
    pub(crate) _continue_build_dummy_receiver: Receiver<bool>,
    pub(crate) _enable_spill: bool,

    pub(crate) merge_into_state: Option<SyncUnsafeCell<MergeIntoState>>,

    /// Build side cache info.
    /// A HashMap for mapping the column indexes to the BlockEntry indexes in DataBlock.
    pub(crate) column_map: HashMap<usize, usize>,
    // The index of the next cache block to be read.
    pub(crate) next_cache_block_index: AtomicUsize,
}

impl HashJoinState {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        mut build_schema: DataSchemaRef,
        build_projections: &ColumnSet,
        hash_join_desc: HashJoinDesc,
        probe_to_build: &[(usize, (bool, bool))],
        merge_into_is_distributed: bool,
        enable_merge_into_optimization: bool,
        build_side_cache_info: Option<(usize, HashMap<IndexType, usize>)>,
    ) -> Result<Arc<HashJoinState>> {
        if matches!(
            hash_join_desc.join_type,
            JoinType::Left | JoinType::LeftSingle | JoinType::Full
        ) {
            build_schema = build_schema_wrap_nullable(&build_schema);
        };
        let (build_watcher, _build_done_dummy_receiver) = watch::channel(HashTableType::UnFinished);
        let (continue_build_watcher, _continue_build_dummy_receiver) = watch::channel(false);
        let settings = ctx.get_settings();
        let mut _enable_spill = false;
        if settings.get_join_spilling_memory_ratio()? != 0 {
            _enable_spill = true;
        }
        let spill_partition_bits = settings.get_join_spilling_partition_bits()?;

        let column_map = if let Some((_, column_map)) = build_side_cache_info {
            column_map
        } else {
            HashMap::new()
        };
        Ok(Arc::new(HashJoinState {
            hash_table: SyncUnsafeCell::new(HashJoinHashTable::Null),
            build_watcher,
            _build_done_dummy_receiver,
            hash_join_desc,
            interrupt: AtomicBool::new(false),
            fast_return: Default::default(),
            probe_to_build: probe_to_build.to_vec(),
            row_space: RowSpace::new(ctx, build_schema, build_projections)?,
            build_state: SyncUnsafeCell::new(BuildState::new()),
            spilled_partitions: Default::default(),
            continue_build_watcher,
            _continue_build_dummy_receiver,
            partition_id: AtomicU8::new(0),
            need_next_round: AtomicBool::new(false),
            is_spill_happened: AtomicBool::new(false),
            _enable_spill,
            spill_partition_bits,
            merge_into_state: match enable_merge_into_optimization {
                false => None,
                true => Some(MergeIntoState::create_merge_into_state(
                    merge_into_is_distributed,
                )),
            },
            column_map,
            next_cache_block_index: AtomicUsize::new(0),
        }))
    }

    pub fn interrupt(&self) {
        self.interrupt.store(true, Ordering::Release);
    }

    /// Used by hash join probe processors, wait for build phase finished.
    #[async_backtrace::framed]
    pub async fn wait_build_notify(&self) -> Result<HashTableType> {
        let mut rx = self.build_watcher.subscribe();
        if *rx.borrow() != HashTableType::UnFinished {
            return Ok(*rx.borrow());
        }
        rx.changed()
            .await
            .map_err(|_| ErrorCode::TokioError("build_watcher's sender is dropped"))?;
        let hash_table_type = *rx.borrow();
        Ok(hash_table_type)
    }

    pub fn join_type(&self) -> JoinType {
        self.hash_join_desc.join_type.clone()
    }

    pub fn need_outer_scan(&self) -> bool {
        matches!(
            self.hash_join_desc.join_type,
            JoinType::Full
                | JoinType::Right
                | JoinType::RightSingle
                | JoinType::RightSemi
                | JoinType::RightAnti
        )
    }

    pub fn need_mark_scan(&self) -> bool {
        matches!(self.hash_join_desc.join_type, JoinType::LeftMark)
    }

    pub fn need_final_scan(&self) -> bool {
        self.need_outer_scan() || self.need_mark_scan()
    }

    pub fn can_probe_first_round(&self) -> bool {
        !matches!(
            self.hash_join_desc.join_type,
            JoinType::Left | JoinType::LeftSingle | JoinType::LeftAnti | JoinType::Full
        )
    }

    pub fn add_spilled_partitions(&self, partitions: &HashSet<u8>) {
        let mut spilled_partitions = self.spilled_partitions.write();
        spilled_partitions.extend(partitions);
    }

    #[async_backtrace::framed]
    pub(crate) async fn wait_probe_notify(&self) -> Result<()> {
        let mut rx = self.continue_build_watcher.subscribe();
        if *rx.borrow() {
            return Ok(());
        }
        rx.changed()
            .await
            .map_err(|_| ErrorCode::TokioError("continue_build_watcher's sender is dropped"))?;
        debug_assert!(*rx.borrow());
        Ok(())
    }

    // Reset the state for next round run.
    // It's only called when spill is enable.
    pub(crate) fn reset(&self) {
        self.row_space.reset();
        let build_state = unsafe { &mut *self.build_state.get() };
        build_state.generation_state.chunks.clear();
        build_state.generation_state.build_num_rows = 0;
        build_state.generation_state.build_columns.clear();
        build_state.generation_state.build_columns_data_type.clear();
        if self.need_outer_scan() {
            build_state.outer_scan_map.clear();
        }
        if self.need_mark_scan() {
            build_state.mark_scan_map.clear();
        }
        build_state.generation_state.is_build_projected = true;
    }

    pub fn num_build_chunks(&self) -> usize {
        let build_state = unsafe { &*self.build_state.get() };
        build_state.generation_state.chunks.len()
    }

    pub fn get_cached_columns(&self, column_index: usize) -> Vec<BlockEntry> {
        let index = self.column_map.get(&column_index).unwrap();
        let build_state = unsafe { &*self.build_state.get() };
        let columns = build_state
            .generation_state
            .chunks
            .iter()
            .map(|data_block| data_block.get_by_offset(*index).clone())
            .collect::<Vec<_>>();
        columns
    }

    pub fn get_cached_num_rows(&self) -> Vec<usize> {
        let build_state = unsafe { &*self.build_state.get() };
        let num_rows = build_state
            .generation_state
            .chunks
            .iter()
            .map(|data_block| data_block.num_rows())
            .collect::<Vec<_>>();
        num_rows
    }

    pub fn next_cache_block_index(&self) -> usize {
        self.next_cache_block_index.fetch_add(1, Ordering::Relaxed)
    }
}
