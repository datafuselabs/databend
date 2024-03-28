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

use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicU8;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_base::base::tokio::sync::Barrier;
use databend_common_catalog::merge_into_join::MergeIntoJoinType;
use databend_common_catalog::runtime_filter_info::RuntimeFilterInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::arrow::and_validities;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDomain;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnVec;
use databend_common_expression::DataBlock;
use databend_common_expression::Domain;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethod;
use databend_common_expression::HashMethodKind;
use databend_common_expression::HashMethodSerializer;
use databend_common_expression::HashMethodSingleBinary;
use databend_common_expression::KeysState;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_hashtable::BinaryHashJoinHashMap;
use databend_common_hashtable::HashJoinHashMap;
use databend_common_hashtable::RawEntry;
use databend_common_hashtable::RowPtr;
use databend_common_hashtable::StringRawEntry;
use databend_common_hashtable::STRING_EARLY_SIZE;
use databend_common_sql::plans::JoinType;
use databend_common_sql::ColumnSet;
use databend_common_storages_fuse::operations::can_merge_into_target_build_bloom_filter;
use databend_storages_common_index::BloomIndex;
use ethnum::U256;
use itertools::Itertools;
use log::info;
use parking_lot::Mutex;
use parking_lot::RwLock;
use xorf::BinaryFuse16;

use crate::pipelines::processors::transforms::hash_join::common::wrap_true_validity;
use crate::pipelines::processors::transforms::hash_join::desc::MARKER_KIND_FALSE;
use crate::pipelines::processors::transforms::hash_join::util::dedup_build_key_column;
use crate::pipelines::processors::transforms::hash_join::util::hash_by_method;
use crate::pipelines::processors::transforms::hash_join::util::inlist_filter;
use crate::pipelines::processors::transforms::hash_join::util::min_max_filter;
use crate::pipelines::processors::transforms::hash_join::FixedKeyHashJoinHashTable;
use crate::pipelines::processors::transforms::hash_join::HashJoinHashTable;
use crate::pipelines::processors::transforms::hash_join::SerializerHashJoinHashTable;
use crate::pipelines::processors::transforms::hash_join::SingleBinaryHashJoinHashTable;
use crate::pipelines::processors::HashJoinState;
use crate::sessions::QueryContext;

pub(crate) const INLIST_RUNTIME_FILTER_THRESHOLD: usize = 1024;

/// Define some shared states for all hash join build threads.
pub struct HashJoinBuildState {
    pub(crate) ctx: Arc<QueryContext>,
    pub(crate) func_ctx: FunctionContext,
    /// `hash_join_state` is shared by `HashJoinBuild` and `HashJoinProbe`
    pub(crate) hash_join_state: Arc<HashJoinState>,
    // When build side input data is coming, will put it into chunks.
    // To make the size of each chunk suitable, it's better to define a threshold to the size of each chunk.
    // Before putting the input data into `Chunk`, we will add them to buffer of `RowSpace`
    // After buffer's size hits the threshold, we will flush the buffer to `Chunk`.
    pub(crate) chunk_size_limit: usize,
    /// Wait util all processors finish row space build, then go to next phase.
    pub(crate) barrier: Barrier,
    /// It will be increased by 1 when a new hash join build processor is created.
    /// After the processor put its input data into `RowSpace`, it will be decreased by 1.
    /// The processor will wait other processors to finish their work before starting to build hash table.
    /// When the counter is 0, it means all hash join build processors have input their data to `RowSpace`.
    pub(crate) row_space_builders: AtomicUsize,
    /// Hash method for hash join keys.
    pub(crate) method: HashMethodKind,
    /// The size of each entry in HashTable.
    pub(crate) entry_size: AtomicUsize,
    pub(crate) raw_entry_spaces: Mutex<Vec<Vec<u8>>>,
    /// `build_projections` only contains the columns from upstream required columns
    /// and columns from other_condition which are in build schema.
    pub(crate) build_projections: ColumnSet,
    pub(crate) build_worker_num: AtomicU32,
    /// Tasks for building hash table.
    pub(crate) build_hash_table_tasks: RwLock<VecDeque<usize>>,
    pub(crate) mutex: Mutex<()>,

    /// Spill related states
    /// `send_val` is the message which will be sent into `build_done_watcher` channel.
    pub(crate) send_val: AtomicU8,
    /// Wait all processors finish read spilled data, then go to new round build
    pub(crate) restore_barrier: Barrier,
    // Max memory usage for join
    pub(crate) max_memory_usage: usize,
    // Spilling threshold for each processor
    pub(crate) spilling_threshold_per_proc: usize,
    /// Spilled partition set, it contains all spilled_partition_sets from all processors
    pub(crate) spilled_partition_set: RwLock<HashSet<u8>>,

    /// Runtime filter related states
    pub(crate) enable_inlist_runtime_filter: bool,
    pub(crate) enable_min_max_runtime_filter: bool,
    /// Need to open runtime filter setting.
    pub(crate) enable_bloom_runtime_filter: bool,
}

impl HashJoinBuildState {
    #[allow(clippy::too_many_arguments)]
    pub fn try_create(
        ctx: Arc<QueryContext>,
        func_ctx: FunctionContext,
        build_keys: &[RemoteExpr],
        build_projections: &ColumnSet,
        hash_join_state: Arc<HashJoinState>,
        num_threads: usize,
    ) -> Result<Arc<HashJoinBuildState>> {
        let hash_key_types = build_keys
            .iter()
            .map(|expr| {
                expr.as_expr(&BUILTIN_FUNCTIONS)
                    .data_type()
                    .clone()
                    .remove_nullable()
            })
            .collect::<Vec<_>>();
        let method = DataBlock::choose_hash_method_with_types(&hash_key_types, false)?;
        let mut enable_bloom_runtime_filter = false;
        let mut enable_inlist_runtime_filter = false;
        let mut enable_min_max_runtime_filter = false;
        if supported_join_type_for_runtime_filter(&hash_join_state.hash_join_desc.join_type) {
            let is_cluster = !ctx.get_cluster().is_empty();
            // For cluster, only support runtime filter for broadcast join.
            let is_broadcast_join = hash_join_state.hash_join_desc.broadcast;
            if !is_cluster || is_broadcast_join {
                enable_inlist_runtime_filter = true;
                enable_min_max_runtime_filter = true;
                enable_bloom_runtime_filter =
                    hash_join_state.hash_join_desc.enable_bloom_runtime_filter;
            }
        }
        let chunk_size_limit = ctx.get_settings().get_max_block_size()? as usize * 16;
        let (max_memory_usage, spilling_threshold_per_proc) =
            Self::max_memory_usage(ctx.clone(), num_threads)?;
        Ok(Arc::new(Self {
            ctx: ctx.clone(),
            func_ctx,
            hash_join_state,
            chunk_size_limit,
            barrier: Barrier::new(num_threads),
            restore_barrier: Barrier::new(num_threads),
            max_memory_usage,
            row_space_builders: Default::default(),
            method,
            entry_size: Default::default(),
            raw_entry_spaces: Default::default(),
            build_projections: build_projections.clone(),
            build_worker_num: Default::default(),
            build_hash_table_tasks: Default::default(),
            mutex: Default::default(),
            send_val: AtomicU8::new(1),
            enable_bloom_runtime_filter,
            enable_inlist_runtime_filter,
            enable_min_max_runtime_filter,
            spilling_threshold_per_proc,
            spilled_partition_set: Default::default(),
        }))
    }

    // Get max memory usage for settings
    fn max_memory_usage(ctx: Arc<QueryContext>, num_threads: usize) -> Result<(usize, usize)> {
        debug_assert!(num_threads != 0);
        let settings = ctx.get_settings();
        let spilling_threshold_per_proc = settings.get_join_spilling_bytes_threshold_per_proc()?;
        let mut memory_ratio = settings.get_join_spilling_memory_ratio()? as f64 / 100_f64;
        if memory_ratio > 1_f64 {
            memory_ratio = 1_f64;
        }
        let max_memory_usage = match settings.get_max_memory_usage()? {
            0 => usize::MAX,
            max_memory_usage => match memory_ratio {
                mr if mr == 0_f64 => usize::MAX,
                mr => (max_memory_usage as f64 * mr) as usize,
            },
        };

        let spilling_threshold_per_proc = match spilling_threshold_per_proc {
            0 => max_memory_usage / num_threads,
            bytes => bytes,
        };

        Ok((max_memory_usage, spilling_threshold_per_proc))
    }

    /// Add input `DataBlock` to `hash_join_state.row_space`.
    pub fn build(&self, input: DataBlock) -> Result<()> {
        let mut buffer = self.hash_join_state.row_space.buffer.write();

        let input_rows = input.num_rows();
        let old_size = self
            .hash_join_state
            .row_space
            .buffer_row_size
            .fetch_add(input_rows, Ordering::Relaxed);

        self.merge_into_try_build_block_info_index(input.clone(), old_size);
        buffer.push(input);

        if old_size + input_rows < self.chunk_size_limit {
            return Ok(());
        }

        let data_block = DataBlock::concat(buffer.as_slice())?;
        buffer.clear();
        self.hash_join_state
            .row_space
            .buffer_row_size
            .store(0, Ordering::Relaxed);
        drop(buffer);
        self.add_build_block(data_block)
    }

    // Add `data_block` for build table to `row_space`
    pub(crate) fn add_build_block(&self, data_block: DataBlock) -> Result<()> {
        let block_outer_scan_map = if self.hash_join_state.need_outer_scan()
            || matches!(
                self.hash_join_state.hash_join_desc.single_to_inner,
                Some(JoinType::RightSingle)
            ) {
            vec![false; data_block.num_rows()]
        } else {
            vec![]
        };

        let block_mark_scan_map = if self.hash_join_state.need_mark_scan() {
            vec![MARKER_KIND_FALSE; data_block.num_rows()]
        } else {
            vec![]
        };

        {
            // Acquire lock in current scope
            let _lock = self.mutex.lock();
            let build_state = unsafe { &mut *self.hash_join_state.build_state.get() };
            if self.hash_join_state.need_outer_scan()
                || matches!(
                    self.hash_join_state.hash_join_desc.single_to_inner,
                    Some(JoinType::RightSingle)
                )
            {
                build_state.outer_scan_map.push(block_outer_scan_map);
            }
            if self.hash_join_state.need_mark_scan() {
                build_state.mark_scan_map.push(block_mark_scan_map);
            }

            build_state.generation_state.build_num_rows += data_block.num_rows();
            build_state.generation_state.chunks.push(data_block);

            self.merge_into_try_add_chunk_offset(build_state);
        }
        Ok(())
    }

    /// Attach to state: `row_space_builders` and `hash_table_builders`.
    pub fn build_attach(&self) -> usize {
        let worker_id = self.row_space_builders.fetch_add(1, Ordering::Relaxed);
        self.hash_join_state
            .hash_table_builders
            .fetch_add(1, Ordering::Relaxed);
        self.build_worker_num.fetch_add(1, Ordering::Relaxed);
        worker_id
    }

    /// Detach to state: `row_space_builders`,
    /// create finalize task and initialize the hash table.
    pub(crate) fn row_space_build_done(&self) -> Result<()> {
        let old_count = self.row_space_builders.fetch_sub(1, Ordering::Relaxed);
        if old_count == 1 {
            {
                let mut buffer = self.hash_join_state.row_space.buffer.write();
                if !buffer.is_empty() {
                    let data_block = DataBlock::concat(&buffer)?;
                    self.add_build_block(data_block)?;
                    buffer.clear();
                }
            }

            // Get the number of rows of the build side.
            let build_num_rows = unsafe {
                (*self.hash_join_state.build_state.get())
                    .generation_state
                    .build_num_rows
            };

            let build_chunks = unsafe {
                (*self.hash_join_state.build_state.get())
                    .generation_state
                    .chunks
                    .clone()
            };

            // If spilling happened, skip adding runtime filter, because probe data is ready and spilled.
            if self.spilled_partition_set.read().is_empty() {
                self.add_runtime_filter(&build_chunks, build_num_rows)?;
            }

            if self.hash_join_state.hash_join_desc.join_type == JoinType::Cross {
                return Ok(());
            }

            // Divide the finalize phase into multiple tasks.
            self.generate_finalize_task()?;

            // Fast path for hash join
            if build_num_rows == 0
                && !matches!(
                    self.hash_join_state.hash_join_desc.join_type,
                    JoinType::LeftMark | JoinType::RightMark
                )
                && self.ctx.get_settings().get_join_spilling_memory_ratio()? == 0
            {
                self.hash_join_state
                    .fast_return
                    .store(true, Ordering::Relaxed);
                self.hash_join_state
                    .build_done_watcher
                    .send(self.send_val.load(Ordering::Acquire))
                    .map_err(|_| ErrorCode::TokioError("build_done_watcher channel is closed"))?;
                return Ok(());
            }

            // Create a fixed size hash table.
            let hashjoin_hashtable = match self.method.clone() {
                HashMethodKind::Serializer(_) => {
                    self.entry_size
                        .store(std::mem::size_of::<StringRawEntry>(), Ordering::SeqCst);
                    HashJoinHashTable::Serializer(SerializerHashJoinHashTable {
                        hash_table: BinaryHashJoinHashMap::with_build_row_num(build_num_rows),
                        hash_method: HashMethodSerializer::default(),
                    })
                }
                HashMethodKind::SingleBinary(_) => {
                    self.entry_size
                        .store(std::mem::size_of::<StringRawEntry>(), Ordering::SeqCst);
                    HashJoinHashTable::SingleBinary(SingleBinaryHashJoinHashTable {
                        hash_table: BinaryHashJoinHashMap::with_build_row_num(build_num_rows),
                        hash_method: HashMethodSingleBinary::default(),
                    })
                }
                HashMethodKind::KeysU8(hash_method) => {
                    self.entry_size
                        .store(std::mem::size_of::<RawEntry<u8>>(), Ordering::SeqCst);
                    HashJoinHashTable::KeysU8(FixedKeyHashJoinHashTable {
                        hash_table: HashJoinHashMap::<u8>::with_build_row_num(build_num_rows),
                        hash_method,
                    })
                }
                HashMethodKind::KeysU16(hash_method) => {
                    self.entry_size
                        .store(std::mem::size_of::<RawEntry<u16>>(), Ordering::SeqCst);
                    HashJoinHashTable::KeysU16(FixedKeyHashJoinHashTable {
                        hash_table: HashJoinHashMap::<u16>::with_build_row_num(build_num_rows),
                        hash_method,
                    })
                }
                HashMethodKind::KeysU32(hash_method) => {
                    self.entry_size
                        .store(std::mem::size_of::<RawEntry<u32>>(), Ordering::SeqCst);
                    HashJoinHashTable::KeysU32(FixedKeyHashJoinHashTable {
                        hash_table: HashJoinHashMap::<u32>::with_build_row_num(build_num_rows),
                        hash_method,
                    })
                }
                HashMethodKind::KeysU64(hash_method) => {
                    self.entry_size
                        .store(std::mem::size_of::<RawEntry<u64>>(), Ordering::SeqCst);
                    HashJoinHashTable::KeysU64(FixedKeyHashJoinHashTable {
                        hash_table: HashJoinHashMap::<u64>::with_build_row_num(build_num_rows),
                        hash_method,
                    })
                }
                HashMethodKind::KeysU128(hash_method) => {
                    self.entry_size
                        .store(std::mem::size_of::<RawEntry<u128>>(), Ordering::SeqCst);
                    HashJoinHashTable::KeysU128(FixedKeyHashJoinHashTable {
                        hash_table: HashJoinHashMap::<u128>::with_build_row_num(build_num_rows),
                        hash_method,
                    })
                }
                HashMethodKind::KeysU256(hash_method) => {
                    self.entry_size
                        .store(std::mem::size_of::<RawEntry<U256>>(), Ordering::SeqCst);
                    HashJoinHashTable::KeysU256(FixedKeyHashJoinHashTable {
                        hash_table: HashJoinHashMap::<U256>::with_build_row_num(build_num_rows),
                        hash_method,
                    })
                }
                HashMethodKind::DictionarySerializer(_) => unimplemented!(),
            };
            let hashtable = unsafe { &mut *self.hash_join_state.hash_table.get() };
            *hashtable = hashjoin_hashtable;
            self.merge_into_try_generate_matched_memory();
        }
        Ok(())
    }

    /// Divide the finalize phase into multiple tasks.
    pub fn generate_finalize_task(&self) -> Result<()> {
        let task_num = unsafe { &*self.hash_join_state.build_state.get() }
            .generation_state
            .chunks
            .len();
        if task_num == 0 {
            return Ok(());
        }
        let tasks = (0..task_num).collect_vec();
        *self.build_hash_table_tasks.write() = tasks.into();
        Ok(())
    }

    /// Get the finalize task and using the `chunks` in `hash_join_state.row_space` to build hash table in parallel.
    pub(crate) fn finalize(&self, task: usize) -> Result<()> {
        let entry_size = self.entry_size.load(Ordering::Relaxed);
        let mut local_raw_entry_spaces: Vec<Vec<u8>> = Vec::new();
        let hashtable = unsafe { &mut *self.hash_join_state.hash_table.get() };
        let build_state = unsafe { &mut *self.hash_join_state.build_state.get() };

        macro_rules! insert_key {
            ($table: expr, $method: expr, $chunk: expr, $build_keys: expr, $valids: expr, $chunk_index: expr, $entry_size: expr, $local_raw_entry_spaces: expr, $t: ty,) => {{
                let keys_state = $method.build_keys_state(&$build_keys, $chunk.num_rows())?;
                let build_keys_iter = $method.build_keys_iter(&keys_state)?;

                let valid_num = match &$valids {
                    Some(valids) => valids.len() - valids.unset_bits(),
                    None => $chunk.num_rows(),
                };
                let mut local_space: Vec<u8> = Vec::with_capacity(valid_num * entry_size);
                let mut raw_entry_ptr = unsafe {
                    std::mem::transmute::<*mut u8, *mut RawEntry<$t>>(local_space.as_mut_ptr())
                };

                match $valids {
                    Some(valids) => {
                        for (row_index, (key, valid)) in
                            build_keys_iter.zip(valids.iter()).enumerate()
                        {
                            if !valid {
                                continue;
                            }
                            let row_ptr = RowPtr {
                                chunk_index: $chunk_index,
                                row_index: row_index as u32,
                            };

                            // # Safety
                            // The memory address of `raw_entry_ptr` is valid.
                            unsafe {
                                *raw_entry_ptr = RawEntry {
                                    row_ptr,
                                    key: *key,
                                    next: 0,
                                }
                            }
                            $table.insert(*key, raw_entry_ptr);
                            raw_entry_ptr = unsafe { raw_entry_ptr.add(1) };
                        }
                    }
                    None => {
                        for (row_index, key) in build_keys_iter.enumerate() {
                            let row_ptr = RowPtr {
                                chunk_index: $chunk_index,
                                row_index: row_index as u32,
                            };

                            // # Safety
                            // The memory address of `raw_entry_ptr` is valid.
                            unsafe {
                                *raw_entry_ptr = RawEntry {
                                    row_ptr,
                                    key: *key,
                                    next: 0,
                                }
                            }
                            $table.insert(*key, raw_entry_ptr);
                            raw_entry_ptr = unsafe { raw_entry_ptr.add(1) };
                        }
                    }
                }

                local_raw_entry_spaces.push(local_space);
            }};
        }

        macro_rules! insert_binary_key {
            ($table: expr, $method: expr, $chunk: expr, $build_keys: expr, $valids: expr, $chunk_index: expr, $entry_size: expr, $local_raw_entry_spaces: expr, ) => {{
                let keys_state = $method.build_keys_state(&$build_keys, $chunk.num_rows())?;
                let build_keys_iter = $method.build_keys_iter(&keys_state)?;

                let space_size = match &keys_state {
                    // safe to unwrap(): offset.len() >= 1.
                    KeysState::Column(Column::Binary(col) | Column::Variant(col) | Column::Bitmap(col)) => col.offsets().last().unwrap(),
                    KeysState::Column(Column::String(col) ) => col.offsets().last().unwrap(),
                    // The function `build_keys_state` of both HashMethodSerializer and HashMethodSingleString
                    // must return `Column::Binary` | `Column::String` | `Column::Variant` | `Column::Bitmap`.
                    _ => unreachable!(),
                };
                let valid_num = match &$valids {
                    Some(valids) => valids.len() - valids.unset_bits(),
                    None => $chunk.num_rows(),
                };
                let mut entry_local_space: Vec<u8> =
                    Vec::with_capacity(valid_num * entry_size);
                let mut string_local_space: Vec<u8> =
                    Vec::with_capacity(*space_size as usize);
                let mut raw_entry_ptr = unsafe { std::mem::transmute::<*mut u8, *mut StringRawEntry>(entry_local_space.as_mut_ptr()) };
                let mut string_local_space_ptr = string_local_space.as_mut_ptr();

                match $valids {
                    Some(valids) => {
                        for (row_index, (key, valid)) in build_keys_iter.zip(valids.iter()).enumerate() {
                            if !valid {
                                continue;
                            }
                            let row_ptr = RowPtr {
                                chunk_index: $chunk_index,
                                row_index: row_index as u32,
                            };

                            // # Safety
                            // The memory address of `raw_entry_ptr` is valid.
                            // string_offset + key.len() <= space_size.
                            unsafe {
                                (*raw_entry_ptr).row_ptr = row_ptr;
                                (*raw_entry_ptr).length = key.len() as u32;
                                (*raw_entry_ptr).next = 0;
                                (*raw_entry_ptr).key = string_local_space_ptr;
                                // The size of `early` is 4.
                                std::ptr::copy_nonoverlapping(
                                    key.as_ptr(),
                                    (*raw_entry_ptr).early.as_mut_ptr(),
                                    std::cmp::min(STRING_EARLY_SIZE, key.len()),
                                );
                                std::ptr::copy_nonoverlapping(key.as_ptr(), string_local_space_ptr, key.len());
                                string_local_space_ptr = string_local_space_ptr.add(key.len());
                            }

                            $table.insert(key, raw_entry_ptr);
                            raw_entry_ptr = unsafe { raw_entry_ptr.add(1) };
                        }
                    }
                    None => {
                        for (row_index, key) in build_keys_iter.enumerate() {
                            let row_ptr = RowPtr {
                                chunk_index: $chunk_index,
                                row_index: row_index as u32,
                            };

                            // # Safety
                            // The memory address of `raw_entry_ptr` is valid.
                            // string_offset + key.len() <= space_size.
                            unsafe {
                                (*raw_entry_ptr).row_ptr = row_ptr;
                                (*raw_entry_ptr).length = key.len() as u32;
                                (*raw_entry_ptr).next = 0;
                                (*raw_entry_ptr).key = string_local_space_ptr;
                                // The size of `early` is 4.
                                std::ptr::copy_nonoverlapping(
                                    key.as_ptr(),
                                    (*raw_entry_ptr).early.as_mut_ptr(),
                                    std::cmp::min(STRING_EARLY_SIZE, key.len()),
                                );
                                std::ptr::copy_nonoverlapping(key.as_ptr(), string_local_space_ptr, key.len());
                                string_local_space_ptr = string_local_space_ptr.add(key.len());
                            }

                            $table.insert(key, raw_entry_ptr);
                            raw_entry_ptr = unsafe { raw_entry_ptr.add(1) };
                        }
                    }
                }

                local_raw_entry_spaces.push(entry_local_space);
                local_raw_entry_spaces.push(string_local_space);
            }};
        }

        if self.hash_join_state.interrupt.load(Ordering::Relaxed) {
            return Err(ErrorCode::AbortedQuery(
                "Aborted query, because the server is shutting down or the query was killed.",
            ));
        }

        let chunk_index = task;
        let chunk = &mut build_state.generation_state.chunks[chunk_index];

        let mut _has_null = false;
        let mut _nullable_chunk = None;
        let evaluator = if matches!(
            self.hash_join_state.hash_join_desc.join_type,
            JoinType::Left | JoinType::LeftSingle | JoinType::Full
        ) {
            let validity = Bitmap::new_constant(true, chunk.num_rows());
            let nullable_columns = chunk
                .columns()
                .iter()
                .map(|c| wrap_true_validity(c, chunk.num_rows(), &validity))
                .collect::<Vec<_>>();
            _nullable_chunk = Some(DataBlock::new(nullable_columns, chunk.num_rows()));
            Evaluator::new(
                _nullable_chunk.as_ref().unwrap(),
                &self.func_ctx,
                &BUILTIN_FUNCTIONS,
            )
        } else {
            Evaluator::new(chunk, &self.func_ctx, &BUILTIN_FUNCTIONS)
        };
        let mut build_keys: Vec<(Column, DataType)> = self
            .hash_join_state
            .hash_join_desc
            .build_keys
            .iter()
            .map(|expr| {
                let return_type = expr.data_type();
                Ok((
                    evaluator
                        .run(expr)?
                        .convert_to_full_column(return_type, chunk.num_rows()),
                    return_type.clone(),
                ))
            })
            .collect::<Result<_>>()?;

        let column_nums = chunk.num_columns();
        let mut block_entries = Vec::with_capacity(self.build_projections.len());
        for index in 0..column_nums {
            if !self.build_projections.contains(&index) {
                continue;
            }
            block_entries.push(chunk.get_by_offset(index).clone());
        }
        if block_entries.is_empty() {
            build_state.generation_state.is_build_projected = false;
        }
        *chunk = DataBlock::new(block_entries, chunk.num_rows());

        let mut valids = None;
        if build_keys
            .iter()
            .any(|(_, ty)| ty.is_nullable() || ty.is_null())
        {
            for (col, _) in build_keys.iter() {
                let (is_all_null, tmp_valids) = col.validity();
                if is_all_null {
                    valids = Some(Bitmap::new_constant(false, chunk.num_rows()));
                    break;
                } else {
                    valids = and_validities(valids, tmp_valids.cloned());
                }
            }
        }

        valids = match valids {
            Some(valids) => {
                if valids.unset_bits() == valids.len() {
                    return Ok(());
                } else if valids.unset_bits() == 0 {
                    None
                } else {
                    Some(valids)
                }
            }
            None => None,
        };

        match self.hash_join_state.hash_join_desc.join_type {
            JoinType::LeftMark => {
                let markers = &mut build_state.mark_scan_map[chunk_index];
                self.hash_join_state
                    .init_markers(&build_keys, chunk.num_rows(), markers);
            }
            JoinType::RightMark => {
                if !_has_null && !build_keys.is_empty() {
                    if let Some(validity) = build_keys[0].0.validity().1 {
                        if validity.unset_bits() > 0 {
                            _has_null = true;
                            let mut has_null_ref = self
                                .hash_join_state
                                .hash_join_desc
                                .marker_join_desc
                                .has_null
                                .write();
                            *has_null_ref = true;
                        }
                    }
                }
            }
            _ => {}
        };

        for (col, ty) in build_keys.iter_mut() {
            *col = col.remove_nullable();
            *ty = ty.remove_nullable();
        }

        match hashtable {
            HashJoinHashTable::Serializer(table) => insert_binary_key! {
              &mut table.hash_table, &table.hash_method, chunk, build_keys, valids, chunk_index as u32, entry_size, &mut local_raw_entry_spaces,
            },
            HashJoinHashTable::SingleBinary(table) => insert_binary_key! {
              &mut table.hash_table, &table.hash_method, chunk, build_keys, valids, chunk_index as u32, entry_size, &mut local_raw_entry_spaces,
            },
            HashJoinHashTable::KeysU8(table) => insert_key! {
              &mut table.hash_table, &table.hash_method, chunk, build_keys, valids, chunk_index as u32, entry_size, &mut local_raw_entry_spaces, u8,
            },
            HashJoinHashTable::KeysU16(table) => insert_key! {
              &mut table.hash_table, &table.hash_method, chunk, build_keys, valids, chunk_index as u32, entry_size, &mut local_raw_entry_spaces, u16,
            },
            HashJoinHashTable::KeysU32(table) => insert_key! {
              &mut table.hash_table, &table.hash_method, chunk, build_keys, valids, chunk_index as u32, entry_size, &mut local_raw_entry_spaces, u32,
            },
            HashJoinHashTable::KeysU64(table) => insert_key! {
              &mut table.hash_table, &table.hash_method, chunk, build_keys, valids, chunk_index as u32, entry_size, &mut local_raw_entry_spaces, u64,
            },
            HashJoinHashTable::KeysU128(table) => insert_key! {
              &mut table.hash_table, &table.hash_method, chunk, build_keys, valids, chunk_index as u32, entry_size, &mut local_raw_entry_spaces, u128,
            },
            HashJoinHashTable::KeysU256(table) => insert_key! {
              &mut table.hash_table, &table.hash_method, chunk, build_keys, valids, chunk_index as u32, entry_size, &mut local_raw_entry_spaces, U256,
            },
            HashJoinHashTable::Null => {
                return Err(ErrorCode::AbortedQuery(
                    "Aborted query, because the hash table is uninitialized.",
                ));
            }
        }

        {
            let mut raw_entry_spaces = self.raw_entry_spaces.lock();
            raw_entry_spaces.extend(local_raw_entry_spaces);
        }
        Ok(())
    }

    /// Get one build hash table task.
    pub fn finalize_task(&self) -> Option<usize> {
        let mut tasks = self.build_hash_table_tasks.write();
        tasks.pop_front()
    }

    /// Detach to state: `hash_table_builders`.
    pub(crate) fn build_done(&self) -> Result<()> {
        let old_count = self
            .hash_join_state
            .hash_table_builders
            .fetch_sub(1, Ordering::Relaxed);
        if old_count == 1 {
            self.hash_join_state
                .set_spilled_partition(&self.spilled_partition_set.read());

            let build_state = unsafe { &mut *self.hash_join_state.build_state.get() };
            let build_num_rows = build_state.generation_state.build_num_rows;
            info!("finish build hash table with {} rows", build_num_rows);

            let data_blocks = &mut build_state.generation_state.chunks;

            if !data_blocks.is_empty()
                && self.hash_join_state.hash_join_desc.join_type != JoinType::Cross
            {
                let num_columns = data_blocks[0].num_columns();
                let columns_data_type: Vec<DataType> = (0..num_columns)
                    .map(|index| data_blocks[0].get_by_offset(index).data_type.clone())
                    .collect();
                let columns: Vec<ColumnVec> = (0..num_columns)
                    .map(|index| {
                        let columns = data_blocks
                            .iter()
                            .map(|block| (block.get_by_offset(index), block.num_rows()))
                            .collect_vec();
                        let full_columns: Vec<Column> = columns
                            .iter()
                            .map(|(entry, rows)| match &entry.value {
                                Value::Scalar(s) => {
                                    let builder =
                                        ColumnBuilder::repeat(&s.as_ref(), *rows, &entry.data_type);
                                    builder.build()
                                }
                                Value::Column(c) => c.clone(),
                            })
                            .collect();
                        Column::take_downcast_column_vec(
                            &full_columns,
                            columns[0].0.data_type.clone(),
                        )
                    })
                    .collect();
                build_state.generation_state.build_columns_data_type = columns_data_type;
                build_state.generation_state.build_columns = columns;
            }
            self.hash_join_state
                .build_done_watcher
                .send(self.send_val.load(Ordering::Acquire))
                .map_err(|_| ErrorCode::TokioError("build_done_watcher channel is closed"))?;
        }
        Ok(())
    }

    fn add_runtime_filter(&self, build_chunks: &[DataBlock], build_num_rows: usize) -> Result<()> {
        for (build_key, probe_key, table_index) in self
            .hash_join_state
            .hash_join_desc
            .build_keys
            .iter()
            .zip(self.hash_join_state.hash_join_desc.probe_keys_rt.iter())
            .filter_map(|(b, p)| p.as_ref().map(|(p, index)| (b, p, index)))
        {
            let mut runtime_filter = RuntimeFilterInfo::default();
            if self.enable_inlist_runtime_filter && build_num_rows < INLIST_RUNTIME_FILTER_THRESHOLD
            {
                self.inlist_runtime_filter(
                    &mut runtime_filter,
                    build_chunks,
                    build_key,
                    probe_key,
                )?;
            }

            // add BloomIndex hash keys for merge into source build.
            if can_merge_into_target_build_bloom_filter(self.ctx.clone(), *table_index)? {
                self.build_merge_into_runtime_filter_siphashes(
                    build_chunks,
                    &mut runtime_filter,
                    build_key,
                    probe_key,
                )?;
            }

            if self.enable_bloom_runtime_filter {
                self.bloom_runtime_filter(build_chunks, &mut runtime_filter, build_key, probe_key)?;
            }

            if self.enable_min_max_runtime_filter {
                self.min_max_runtime_filter(
                    build_chunks,
                    &mut runtime_filter,
                    build_key,
                    probe_key,
                )?;
            }
            if !runtime_filter.is_empty() {
                self.ctx.set_runtime_filter((*table_index, runtime_filter));
            }
        }
        Ok(())
    }

    fn bloom_runtime_filter(
        &self,
        data_blocks: &[DataBlock],
        runtime_filter: &mut RuntimeFilterInfo,
        build_key: &Expr,
        probe_key: &Expr<String>,
    ) -> Result<()> {
        if !build_key.data_type().remove_nullable().is_numeric()
            && !build_key.data_type().remove_nullable().is_string()
        {
            return Ok(());
        }
        if let Expr::ColumnRef { id, .. } = probe_key {
            let mut columns = Vec::with_capacity(data_blocks.len());
            for block in data_blocks.iter() {
                if block.num_columns() == 0 {
                    continue;
                }
                let evaluator = Evaluator::new(block, &self.func_ctx, &BUILTIN_FUNCTIONS);
                let column = evaluator
                    .run(build_key)?
                    .convert_to_full_column(build_key.data_type(), block.num_rows());
                columns.push(column);
            }
            if columns.is_empty() {
                return Ok(());
            }
            let build_key_column = Column::concat_columns(columns.into_iter())?;
            // Generate bloom filter using build column
            let data_type = build_key.data_type().clone();
            let num_rows = build_key_column.len();
            let method = DataBlock::choose_hash_method_with_types(&[data_type.clone()], false)?;
            let mut hashes = HashSet::with_capacity(num_rows);
            hash_by_method(
                &method,
                &[(build_key_column, data_type)],
                num_rows,
                &mut hashes,
            )?;
            let mut hashes_vec = Vec::with_capacity(num_rows);
            hashes.into_iter().for_each(|hash| {
                hashes_vec.push(hash);
            });
            let filter = BinaryFuse16::try_from(&hashes_vec)?;
            runtime_filter.add_bloom((id.to_string(), filter));
        }
        Ok(())
    }

    //      for merge into source build cases, like below:
    // merge into `t1` using `t2` on xxx when matched xx when not matched xxx, if merge_into_optimizer
    // gives `t2` as source build side, we can build source join keys `siphashes`, that's because we use
    // siphash to build target table's bloom index block.
    // in this way, we can avoid current `runtime_filter()` func's performance cost, especially for large
    // target table case, the `runtime_filter()`'s cost is even higher than disable `runtime_filter()`.
    //      However, for `build_runtime_filter_siphashes()` usages, we currently just used for merge into,
    // we doesn't support join query, and it's only for `source build` cases. In fact, source build is the
    // main case in most time.
    fn build_merge_into_runtime_filter_siphashes(
        &self,
        data_blocks: &[DataBlock],
        runtime_filter: &mut RuntimeFilterInfo,
        build_key: &Expr,
        probe_key: &Expr<String>,
    ) -> Result<()> {
        // `calculate_nullable_column_digest`, `apply_bloom_pruning`
        let merge_type = self.ctx.get_merge_into_join();
        if matches!(merge_type.merge_into_join_type, MergeIntoJoinType::Right) {
            if let Expr::ColumnRef { id, .. } = probe_key {
                let mut columns = Vec::with_capacity(data_blocks.len());
                for block in data_blocks.iter() {
                    if block.num_columns() == 0 {
                        continue;
                    }
                    let evaluator = Evaluator::new(block, &self.func_ctx, &BUILTIN_FUNCTIONS);
                    let column = evaluator
                        .run(build_key)?
                        .convert_to_full_column(build_key.data_type(), block.num_rows());
                    columns.push(column);
                }
                if columns.is_empty() {
                    return Ok(());
                }
                let build_key_column = Column::concat_columns(columns.into_iter())?;
                // maybe there will be null values here, so we use nullable column, the null value will be treat as default
                // value for the specified type, like String -> "", int -> 0. so we need to remove the null hash values here.
                let (hashes, bitmap_op) = BloomIndex::calculate_nullable_column_digest(
                    &self.func_ctx,
                    &build_key_column,
                    &build_key_column.data_type(),
                )?;
                if let Some(bitmap) = bitmap_op {
                    // no null values
                    let digests = if bitmap.unset_bits() == 0 {
                        hashes.to_vec()
                    } else {
                        let mut new_hashes = Vec::with_capacity(bitmap.len());
                        assert_eq!(hashes.len(), bitmap.len());
                        for row_idx in 0..bitmap.len() {
                            if bitmap.get_bit(row_idx) {
                                new_hashes.push(hashes[row_idx])
                            }
                        }
                        new_hashes.to_vec()
                    };
                    // id is probe key name
                    runtime_filter
                        .add_merge_into_source_build_siphashkeys((id.to_string(), digests));
                } else {
                    // id is probe key name
                    runtime_filter
                        .add_merge_into_source_build_siphashkeys((id.to_string(), hashes.to_vec()));
                }
            }
        }
        Ok(())
    }

    fn inlist_runtime_filter(
        &self,
        runtime_filter: &mut RuntimeFilterInfo,
        data_blocks: &[DataBlock],
        build_key: &Expr,
        probe_key: &Expr<String>,
    ) -> Result<()> {
        if let Some(distinct_build_column) =
            dedup_build_key_column(&self.func_ctx, data_blocks, build_key)?
        {
            if let Some(filter) = inlist_filter(probe_key, distinct_build_column.clone())? {
                runtime_filter.add_inlist(filter);
            }
        }
        Ok(())
    }

    fn min_max_runtime_filter(
        &self,
        data_blocks: &[DataBlock],
        runtime_filter: &mut RuntimeFilterInfo,
        build_key: &Expr,
        probe_key: &Expr<String>,
    ) -> Result<()> {
        if !build_key.runtime_filter_supported_types() {
            return Ok(());
        }
        if let Expr::ColumnRef { .. } = probe_key {
            let mut columns = Vec::with_capacity(data_blocks.len());
            for block in data_blocks.iter() {
                if block.num_columns() == 0 {
                    continue;
                }
                let evaluator = Evaluator::new(block, &self.func_ctx, &BUILTIN_FUNCTIONS);
                let column = evaluator
                    .run(build_key)?
                    .convert_to_full_column(build_key.data_type(), block.num_rows());
                columns.push(column);
            }
            if columns.is_empty() {
                return Ok(());
            }
            let build_key_column = Column::concat_columns(columns.into_iter())?;
            if build_key_column.len() == 0 {
                return Ok(());
            }
            // Generate min max filter using build column
            let min_max = build_key_column.remove_nullable().domain();
            let min_max_filter = match min_max {
                Domain::Number(domain) => match domain {
                    NumberDomain::UInt8(simple_domain) => {
                        let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                        let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                        min_max_filter(min, max, probe_key)?
                    }
                    NumberDomain::UInt16(simple_domain) => {
                        let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                        let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                        min_max_filter(min, max, probe_key)?
                    }
                    NumberDomain::UInt32(simple_domain) => {
                        let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                        let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                        min_max_filter(min, max, probe_key)?
                    }
                    NumberDomain::UInt64(simple_domain) => {
                        let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                        let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                        min_max_filter(min, max, probe_key)?
                    }
                    NumberDomain::Int8(simple_domain) => {
                        let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                        let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                        min_max_filter(min, max, probe_key)?
                    }
                    NumberDomain::Int16(simple_domain) => {
                        let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                        let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                        min_max_filter(min, max, probe_key)?
                    }
                    NumberDomain::Int32(simple_domain) => {
                        let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                        let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                        min_max_filter(min, max, probe_key)?
                    }
                    NumberDomain::Int64(simple_domain) => {
                        let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                        let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                        min_max_filter(min, max, probe_key)?
                    }
                    NumberDomain::Float32(simple_domain) => {
                        let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                        let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                        min_max_filter(min, max, probe_key)?
                    }
                    NumberDomain::Float64(simple_domain) => {
                        let min = Scalar::Number(NumberScalar::from(simple_domain.min));
                        let max = Scalar::Number(NumberScalar::from(simple_domain.max));
                        min_max_filter(min, max, probe_key)?
                    }
                },
                Domain::String(domain) => {
                    let min = Scalar::String(domain.min);
                    let max = Scalar::String(domain.max.unwrap());
                    min_max_filter(min, max, probe_key)?
                }
                Domain::Date(date_domain) => {
                    let min = Scalar::Date(date_domain.min);
                    let max = Scalar::Date(date_domain.max);
                    min_max_filter(min, max, probe_key)?
                }
                _ => unreachable!(),
            };
            if let Some(min_max_filter) = min_max_filter {
                runtime_filter.add_min_max(min_max_filter);
            }
        }
        Ok(())
    }

    pub(crate) fn join_type(&self) -> JoinType {
        self.hash_join_state.hash_join_desc.join_type.clone()
    }

    pub fn get_enable_bloom_runtime_filter(&self) -> bool {
        self.enable_bloom_runtime_filter
    }

    pub fn get_enable_min_max_runtime_filter(&self) -> bool {
        self.enable_min_max_runtime_filter
    }
}

pub fn supported_join_type_for_runtime_filter(join_type: &JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Inner
            | JoinType::Right
            | JoinType::RightSemi
            | JoinType::RightAnti
            | JoinType::LeftMark
    )
}
