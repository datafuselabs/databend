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
use std::ops::ControlFlow;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::bitmap::MutableBitmap;
use databend_common_base::base::tokio::sync::Barrier;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::arrow::and_validities;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::with_join_hash_method;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Evaluator;
use databend_common_expression::FunctionContext;
use databend_common_expression::HashMethod;
use databend_common_expression::HashMethodKind;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_hashtable::HashJoinHashtableLike;
use databend_common_hashtable::Interval;
use databend_common_sql::ColumnSet;
use itertools::Itertools;
use log::info;
use parking_lot::Mutex;
use parking_lot::RwLock;

use super::ProbeState;
use crate::pipelines::processors::transforms::hash_join::common::wrap_true_validity;
use crate::pipelines::processors::transforms::hash_join::desc::MARKER_KIND_FALSE;
use crate::pipelines::processors::transforms::hash_join::desc::MARKER_KIND_NULL;
use crate::pipelines::processors::transforms::hash_join::desc::MARKER_KIND_TRUE;
use crate::pipelines::processors::transforms::hash_join::hash_join_state::HashJoinHashTable;
use crate::pipelines::processors::transforms::hash_join::util::probe_schema_wrap_nullable;
use crate::pipelines::processors::HashJoinState;
use crate::sessions::QueryContext;
use crate::sql::planner::plans::JoinType;

// ({(Interval,prefix),(Interval,repfix),...},chunk_idx)
// 1.The Interval is the partial unmodified interval offset in chunks.
// 2.Prefix is segment_idx_block_id
// 3.chunk_idx: the index of correlated chunk in chunks.
pub type MergeIntoChunkPartialUnmodified = (Vec<(Interval, u64)>, u64);
/// Define some shared states for all hash join probe threads.
pub struct HashJoinProbeState {
    pub(crate) ctx: Arc<QueryContext>,
    pub(crate) func_ctx: FunctionContext,
    /// `hash_join_state` is shared by `HashJoinBuild` and `HashJoinProbe`
    pub(crate) hash_join_state: Arc<HashJoinState>,
    /// Processors count
    pub(crate) processor_count: usize,
    /// It will be increased by 1 when a new hash join probe processor is created.
    /// After the processor finish probe hash table, it will be decreased by 1.
    /// (Note: it doesn't mean the processor has finished its work, it just means it has finished probe hash table.)
    /// When the counter is 0, processors will go to next phase's work
    pub(crate) probe_workers: AtomicUsize,
    /// Wait all `probe_workers` finish
    pub(crate) barrier: Barrier,
    pub(crate) barrier_count: AtomicUsize,
    /// The schema of probe side.
    pub(crate) probe_schema: DataSchemaRef,
    /// `probe_projections` only contains the columns from upstream required columns
    /// and columns from other_condition which are in probe schema.
    pub(crate) probe_projections: ColumnSet,
    /// Todo(xudong): add more detailed comments for the following fields.
    /// Final scan tasks
    pub(crate) final_scan_tasks: RwLock<VecDeque<usize>>,
    /// for merge into target as build side.
    pub(crate) merge_into_final_partial_unmodified_scan_tasks:
        RwLock<VecDeque<MergeIntoChunkPartialUnmodified>>,
    pub(crate) mark_scan_map_lock: Mutex<()>,
    /// Hash method
    pub(crate) hash_method: HashMethodKind,

    /// Spill related states
    /// Record spill workers
    pub(crate) spill_workers: AtomicUsize,
    /// Record final probe workers
    pub(crate) final_probe_workers: AtomicUsize,
    /// Probe spilled partitions set
    pub(crate) spill_partitions: RwLock<HashSet<u8>>,
    /// Wait all processors to restore spilled data, then go to new probe
    pub(crate) restore_barrier: Barrier,
}

impl HashJoinProbeState {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        ctx: Arc<QueryContext>,
        func_ctx: FunctionContext,
        hash_join_state: Arc<HashJoinState>,
        probe_projections: &ColumnSet,
        probe_keys: &[RemoteExpr],
        mut probe_schema: DataSchemaRef,
        join_type: &JoinType,
        processor_count: usize,
        barrier: Barrier,
        restore_barrier: Barrier,
    ) -> Result<Self> {
        if matches!(
            join_type,
            &JoinType::Right | &JoinType::RightSingle | &JoinType::Full
        ) {
            probe_schema = probe_schema_wrap_nullable(&probe_schema);
        }
        let hash_key_types = probe_keys
            .iter()
            .map(|expr| {
                expr.as_expr(&BUILTIN_FUNCTIONS)
                    .data_type()
                    .remove_nullable()
                    .clone()
            })
            .collect::<Vec<_>>();
        let method = DataBlock::choose_hash_method_with_types(&hash_key_types, false)?;
        Ok(HashJoinProbeState {
            ctx,
            func_ctx,
            hash_join_state,
            processor_count,
            probe_workers: AtomicUsize::new(0),
            spill_workers: AtomicUsize::new(0),
            final_probe_workers: Default::default(),
            barrier,
            restore_barrier,
            probe_schema,
            probe_projections: probe_projections.clone(),
            final_scan_tasks: RwLock::new(VecDeque::new()),
            merge_into_final_partial_unmodified_scan_tasks: RwLock::new(VecDeque::new()),
            mark_scan_map_lock: Mutex::new(()),
            hash_method: method,
            spill_partitions: Default::default(),
            barrier_count: AtomicUsize::new(0),
        })
    }

    /// Probe the hash table and retrieve matched rows as DataBlocks.
    pub fn probe(&self, input: DataBlock, probe_state: &mut ProbeState) -> Result<Vec<DataBlock>> {
        match self.hash_join_state.hash_join_desc.join_type {
            JoinType::Inner
            | JoinType::Asof
            | JoinType::LeftAsof
            | JoinType::RightAsof
            | JoinType::LeftSemi
            | JoinType::LeftAnti
            | JoinType::RightSemi
            | JoinType::RightAnti
            | JoinType::Left
            | JoinType::LeftMark
            | JoinType::RightMark
            | JoinType::LeftSingle
            | JoinType::RightSingle
            | JoinType::Right
            | JoinType::Full => self.probe_join(input, probe_state),
            JoinType::Cross => self.cross_join(input, probe_state),
        }
    }

    pub fn probe_join(
        &self,
        mut input: DataBlock,
        probe_state: &mut ProbeState,
    ) -> Result<Vec<DataBlock>> {
        let input_num_rows = input.num_rows();
        let mut _nullable_data_block = None;
        let evaluator = if matches!(
            self.hash_join_state.hash_join_desc.join_type,
            JoinType::Right | JoinType::RightSingle | JoinType::Full
        ) {
            let nullable_columns = input
                .columns()
                .iter()
                .map(|c| {
                    wrap_true_validity(
                        c,
                        input_num_rows,
                        &probe_state.generation_state.true_validity,
                    )
                })
                .collect::<Vec<_>>();
            _nullable_data_block = Some(DataBlock::new(nullable_columns, input_num_rows));
            Evaluator::new(
                _nullable_data_block.as_ref().unwrap(),
                &probe_state.func_ctx,
                &BUILTIN_FUNCTIONS,
            )
        } else {
            Evaluator::new(&input, &probe_state.func_ctx, &BUILTIN_FUNCTIONS)
        };
        let probe_keys = &self.hash_join_state.hash_join_desc.probe_keys;
        let mut keys_columns = probe_keys
            .iter()
            .map(|expr| {
                Ok(evaluator
                    .run(expr)?
                    .convert_to_full_column(expr.data_type(), input_num_rows))
            })
            .collect::<Result<Vec<_>>>()?;

        if self.hash_join_state.hash_join_desc.join_type == JoinType::RightMark
            && self
                .hash_join_state
                .hash_join_desc
                .other_predicate
                .is_none()
        {
            self.hash_join_state.init_markers(
                (&keys_columns).into(),
                input_num_rows,
                probe_state.markers.as_mut().unwrap(),
            );
        }

        let is_null_equal = &self.hash_join_state.hash_join_desc.is_null_equal;
        let valids = if !Self::check_for_eliminate_valids(
            self.hash_join_state.hash_join_desc.from_correlated_subquery,
            &self.hash_join_state.hash_join_desc.join_type,
        ) && probe_keys.iter().any(|expr| {
            let ty = expr.data_type();
            ty.is_nullable() || ty.is_null()
        }) {
            let valids = keys_columns
                .iter()
                .zip(is_null_equal.iter().copied())
                .filter(|(_, is_null_equal)| !is_null_equal)
                .map(|(col, _)| col.validity())
                .try_fold(None, |valids, (is_all_null, tmp_valids)| {
                    if is_all_null {
                        ControlFlow::Break(Some(Bitmap::new_constant(false, input_num_rows)))
                    } else {
                        ControlFlow::Continue(and_validities(valids, tmp_valids.cloned()))
                    }
                });
            match valids {
                ControlFlow::Continue(valids) | ControlFlow::Break(valids) => valids,
            }
        } else {
            None
        };

        keys_columns
            .iter_mut()
            .zip(is_null_equal.iter().copied())
            .filter(|(col, is_null_equal)| !is_null_equal && col.as_nullable().is_some())
            .for_each(|(col, _)| {
                *col = col.remove_nullable();
            });
        let probe_keys = (&keys_columns).into();

        if self.hash_join_state.hash_join_desc.join_type != JoinType::LeftMark {
            input = input.project(&self.probe_projections);
        }
        probe_state.generation_state.is_probe_projected = input.num_columns() > 0;

        if self.hash_join_state.fast_return.load(Ordering::Relaxed)
            && matches!(
                self.hash_join_state.hash_join_desc.join_type,
                JoinType::Left | JoinType::LeftSingle | JoinType::Full | JoinType::LeftAnti
            )
        {
            return self.left_fast_return(
                input,
                probe_state.generation_state.is_probe_projected,
                &probe_state.generation_state.true_validity,
            );
        }

        // Adaptive early filtering.
        // Thanks to the **adaptive** execution strategy of early filtering, we don't experience a performance decrease
        // when all keys have matches. This allows us to achieve the same performance as before.
        probe_state.num_keys += if let Some(valids) = &valids {
            (valids.len() - valids.unset_bits()) as u64
        } else {
            input_num_rows as u64
        };
        // We use the information from the probed data to predict the matching state of this probe.
        let prefer_early_filtering =
            (probe_state.num_keys_hash_matched as f64) / (probe_state.num_keys as f64) < 0.8;

        // Probe:
        // (1) INNER / RIGHT / RIGHT SINGLE / RIGHT SEMI / RIGHT ANTI / RIGHT MARK / LEFT SEMI / LEFT MARK
        //        prefer_early_filtering is true  => early_filtering_matched_probe
        //        prefer_early_filtering is false => probe
        // (2) LEFT / LEFT SINGLE / LEFT ANTI / FULL
        //        prefer_early_filtering is true  => early_filtering_probe
        //        prefer_early_filtering is false => probe
        let hash_table = unsafe { &*self.hash_join_state.hash_table.get() };
        with_join_hash_method!(|T| match hash_table {
            HashJoinHashTable::T(table) => {
                // Build `keys` and get the hashes of `keys`.
                let keys_state = table
                    .hash_method
                    .build_keys_state(probe_keys, input_num_rows)?;
                let keys = table
                    .hash_method
                    .build_keys_accessor_and_hashes(keys_state, &mut probe_state.hashes)?;

                // Perform a round of hash table probe.
                probe_state.probe_with_selection = prefer_early_filtering;
                probe_state.selection_count = if !Self::need_unmatched_selection(
                    &self.hash_join_state.hash_join_desc.join_type,
                    probe_state.with_conjunction,
                ) {
                    if prefer_early_filtering {
                        // Early filtering, use selection to get better performance.
                        table.hash_table.early_filtering_matched_probe(
                            &mut probe_state.hashes,
                            valids,
                            &mut probe_state.selection,
                        )
                    } else {
                        // If don't do early filtering, don't use selection.
                        table.hash_table.probe(&mut probe_state.hashes, valids)
                    }
                } else {
                    if prefer_early_filtering {
                        // Early filtering, use matched selection and unmatched selection to get better performance.
                        let unmatched_selection =
                            probe_state.probe_unmatched_indexes.as_mut().unwrap();
                        let (matched_count, unmatched_count) =
                            table.hash_table.early_filtering_probe(
                                &mut probe_state.hashes,
                                valids,
                                &mut probe_state.selection,
                                unmatched_selection,
                            );
                        probe_state.probe_unmatched_indexes_count = unmatched_count;
                        matched_count
                    } else {
                        // If don't do early filtering, don't use selection.
                        table.hash_table.probe(&mut probe_state.hashes, valids)
                    }
                };
                probe_state.num_keys_hash_matched += probe_state.selection_count as u64;

                // Continue to probe hash table and process data blocks.
                self.result_blocks(&input, keys, &table.hash_table, probe_state)
            }
            HashJoinHashTable::Null => Err(ErrorCode::AbortedQuery(
                "Aborted query, because the hash table is uninitialized.",
            )),
        })
    }

    /// Checks if a join type can eliminate valids.
    pub fn check_for_eliminate_valids(
        from_correlated_subquery: bool,
        join_type: &JoinType,
    ) -> bool {
        if !from_correlated_subquery {
            return false;
        }
        matches!(
            join_type,
            JoinType::Inner
                | JoinType::Full
                | JoinType::Left
                | JoinType::LeftSingle
                | JoinType::LeftAnti
                | JoinType::LeftSemi
                | JoinType::LeftMark
                | JoinType::RightMark
        )
    }

    /// Checks if the join type need to use unmatched selection.
    pub fn need_unmatched_selection(join_type: &JoinType, with_conjunction: bool) -> bool {
        matches!(
            join_type,
            JoinType::Left | JoinType::LeftSingle | JoinType::Full | JoinType::LeftAnti
        ) && !with_conjunction
    }

    pub fn probe_attach(&self) -> Result<usize> {
        let mut worker_id = 0;
        if self.hash_join_state.need_outer_scan()
            || self.hash_join_state.need_mark_scan()
            || self
                .hash_join_state
                .merge_into_need_target_partial_modified_scan()
        {
            worker_id = self.probe_workers.fetch_add(1, Ordering::Relaxed);
        }
        if self.hash_join_state.enable_spill {
            worker_id = self.final_probe_workers.fetch_add(1, Ordering::Relaxed);
            self.spill_workers.fetch_add(1, Ordering::Relaxed);
        }

        Ok(worker_id)
    }

    pub fn finish_final_probe(&self) -> Result<()> {
        let old_count = self.final_probe_workers.fetch_sub(1, Ordering::Relaxed);
        if old_count == 1 {
            if self.join_type() != JoinType::Cross {
                // If build side has spilled data, we need to wait build side to next round.
                // Set partition id to `HashJoinState`
                let mut spill_partitions = self.spill_partitions.write();
                if let Some(id) = spill_partitions.iter().next().cloned() {
                    spill_partitions.remove(&id);
                    self.hash_join_state
                        .partition_id
                        .store(id as i8, Ordering::Relaxed);
                } else {
                    self.hash_join_state
                        .partition_id
                        .store(-1, Ordering::Relaxed);
                }
                info!(
                    "next partition to read: {:?}, final probe done",
                    self.hash_join_state.partition_id.load(Ordering::Relaxed)
                );
            }
            self.hash_join_state
                .continue_build_watcher
                .send(true)
                .map_err(|_| ErrorCode::TokioError("continue_build_watcher channel is closed"))?;
        }
        Ok(())
    }

    pub fn probe_done(&self) -> Result<()> {
        let old_count = self.probe_workers.fetch_sub(1, Ordering::Relaxed);
        if old_count == 1 {
            // Divide the final scan phase into multiple tasks.
            self.generate_final_scan_task()?;
        }
        Ok(())
    }

    pub fn finish_spill(&self) -> Result<()> {
        self.final_probe_workers.fetch_sub(1, Ordering::Relaxed);
        let old_count = self.spill_workers.fetch_sub(1, Ordering::Relaxed);
        if old_count == 1 {
            if self.join_type() != JoinType::Cross {
                // Set partition id to `HashJoinState`
                let mut spill_partitions = self.spill_partitions.write();
                if let Some(id) = spill_partitions.iter().next().cloned() {
                    spill_partitions.remove(&id);
                    self.hash_join_state
                        .partition_id
                        .store(id as i8, Ordering::Relaxed);
                } else {
                    self.hash_join_state
                        .partition_id
                        .store(-1, Ordering::Relaxed);
                };
                info!(
                    "next partition to read: {:?}, probe spill done",
                    self.hash_join_state.partition_id.load(Ordering::Relaxed)
                );
            }
            self.hash_join_state
                .continue_build_watcher
                .send(true)
                .map_err(|_| ErrorCode::TokioError("continue_build_watcher channel is closed"))?;
        }
        Ok(())
    }

    pub fn generate_final_scan_task(&self) -> Result<()> {
        let task_num = unsafe { &*self.hash_join_state.build_state.get() }
            .generation_state
            .chunks
            .len();
        if task_num == 0 {
            return Ok(());
        }
        let tasks = (0..task_num).collect_vec();
        *self.final_scan_tasks.write() = tasks.into();
        Ok(())
    }

    pub fn final_scan_task(&self) -> Option<usize> {
        let mut tasks = self.final_scan_tasks.write();
        tasks.pop_front()
    }

    pub fn final_scan(&self, task: usize, state: &mut ProbeState) -> Result<Vec<DataBlock>> {
        match &self.hash_join_state.hash_join_desc.join_type {
            JoinType::Right | JoinType::RightSingle | JoinType::Full => {
                self.right_and_full_outer_scan(task, state)
            }
            JoinType::RightSemi => self.right_semi_outer_scan(task, state),
            JoinType::RightAnti => self.right_anti_outer_scan(task, state),
            JoinType::LeftMark => self.left_mark_scan(task, state),
            _ => unreachable!(),
        }
    }

    pub fn right_and_full_outer_scan(
        &self,
        task: usize,
        probe_state: &mut ProbeState,
    ) -> Result<Vec<DataBlock>> {
        if self.hash_join_state.interrupt.load(Ordering::Relaxed) {
            return Err(ErrorCode::AbortedQuery(
                "Aborted query, because the server is shutting down or the query was killed.",
            ));
        }

        // Probe states.
        let max_block_size = probe_state.max_block_size;
        let mutable_indexes = &mut probe_state.mutable_indexes;
        let build_indexes = &mut mutable_indexes.build_indexes;
        let mut projected_probe_fields = vec![];
        for (i, field) in self.probe_schema.fields().iter().enumerate() {
            if self.probe_projections.contains(&i) {
                projected_probe_fields.push(field.clone());
            }
        }

        // Build states.
        let build_state = unsafe { &*self.hash_join_state.build_state.get() };
        let generation_state = &build_state.generation_state;
        let outer_scan_map = &build_state.outer_scan_map;
        let chunk_index = task;
        let outer_map = &outer_scan_map[chunk_index];
        let outer_map_len = outer_map.len();
        let mut row_index = 0;

        // Results.
        let mut build_indexes_occupied = 0;
        let mut result_blocks = vec![];

        while row_index < outer_map_len {
            while row_index < outer_map_len && build_indexes_occupied < max_block_size {
                unsafe {
                    if !outer_map.get_unchecked(row_index) {
                        build_indexes
                            .get_unchecked_mut(build_indexes_occupied)
                            .chunk_index = chunk_index as u32;
                        build_indexes
                            .get_unchecked_mut(build_indexes_occupied)
                            .row_index = row_index as u32;
                        build_indexes_occupied += 1;
                    }
                }
                row_index += 1;
            }

            if self.hash_join_state.interrupt.load(Ordering::Relaxed) {
                return Err(ErrorCode::AbortedQuery(
                    "Aborted query, because the server is shutting down or the query was killed.",
                ));
            }

            let probe_block = if !projected_probe_fields.is_empty() {
                // Create null chunk for unmatched rows in probe side
                Some(DataBlock::new(
                    projected_probe_fields
                        .iter()
                        .map(|df| {
                            BlockEntry::new(df.data_type().clone(), Value::Scalar(Scalar::Null))
                        })
                        .collect(),
                    build_indexes_occupied,
                ))
            } else {
                None
            };
            let build_block = if build_state.generation_state.is_build_projected {
                let mut unmatched_build_block = self.hash_join_state.row_space.gather(
                    &build_indexes[0..build_indexes_occupied],
                    &generation_state.build_columns,
                    &generation_state.build_columns_data_type,
                    &generation_state.build_num_rows,
                    &mut probe_state.generation_state.string_items_buf,
                )?;

                if self.hash_join_state.hash_join_desc.join_type == JoinType::Full {
                    let num_rows = unmatched_build_block.num_rows();
                    let nullable_unmatched_build_columns = unmatched_build_block
                        .columns()
                        .iter()
                        .map(|c| {
                            wrap_true_validity(
                                c,
                                num_rows,
                                &probe_state.generation_state.true_validity,
                            )
                        })
                        .collect::<Vec<_>>();
                    unmatched_build_block =
                        DataBlock::new(nullable_unmatched_build_columns, num_rows);
                };
                Some(unmatched_build_block)
            } else {
                None
            };
            result_blocks.push(self.merge_eq_block(
                probe_block,
                build_block,
                build_indexes_occupied,
            ));

            build_indexes_occupied = 0;
        }
        Ok(result_blocks)
    }

    pub fn right_semi_outer_scan(
        &self,
        task: usize,
        probe_state: &mut ProbeState,
    ) -> Result<Vec<DataBlock>> {
        if self.hash_join_state.interrupt.load(Ordering::Relaxed) {
            return Err(ErrorCode::AbortedQuery(
                "Aborted query, because the server is shutting down or the query was killed.",
            ));
        }

        // Probe states.
        let max_block_size = probe_state.max_block_size;
        let mutable_indexes = &mut probe_state.mutable_indexes;
        let build_indexes = &mut mutable_indexes.build_indexes;

        // Build states.
        let build_state = unsafe { &*self.hash_join_state.build_state.get() };
        let generation_state = &build_state.generation_state;
        let outer_scan_map = &build_state.outer_scan_map;
        let chunk_index = task;
        let outer_map = &outer_scan_map[chunk_index];
        let outer_map_len = outer_map.len();
        let mut row_index = 0;

        // Results.
        let mut build_indexes_idx = 0;
        let mut result_blocks = vec![];

        while row_index < outer_map_len {
            while row_index < outer_map_len && build_indexes_idx < max_block_size {
                unsafe {
                    if *outer_map.get_unchecked(row_index) {
                        build_indexes
                            .get_unchecked_mut(build_indexes_idx)
                            .chunk_index = chunk_index as u32;
                        build_indexes.get_unchecked_mut(build_indexes_idx).row_index =
                            row_index as u32;
                        build_indexes_idx += 1;
                    }
                }
                row_index += 1;
            }

            if self.hash_join_state.interrupt.load(Ordering::Relaxed) {
                return Err(ErrorCode::AbortedQuery(
                    "Aborted query, because the server is shutting down or the query was killed.",
                ));
            }

            result_blocks.push(self.hash_join_state.row_space.gather(
                &build_indexes[0..build_indexes_idx],
                &generation_state.build_columns,
                &generation_state.build_columns_data_type,
                &generation_state.build_num_rows,
                &mut probe_state.generation_state.string_items_buf,
            )?);
            build_indexes_idx = 0;
        }
        Ok(result_blocks)
    }

    pub fn right_anti_outer_scan(
        &self,
        task: usize,
        probe_state: &mut ProbeState,
    ) -> Result<Vec<DataBlock>> {
        if self.hash_join_state.interrupt.load(Ordering::Relaxed) {
            return Err(ErrorCode::AbortedQuery(
                "Aborted query, because the server is shutting down or the query was killed.",
            ));
        }

        // Probe states.
        let max_block_size = probe_state.max_block_size;
        let mutable_indexes = &mut probe_state.mutable_indexes;
        let build_indexes = &mut mutable_indexes.build_indexes;

        // Build states.
        let build_state = unsafe { &*self.hash_join_state.build_state.get() };
        let generation_state = &build_state.generation_state;
        let outer_scan_map = &build_state.outer_scan_map;
        let chunk_index = task;
        let outer_map = &outer_scan_map[chunk_index];
        let outer_map_len = outer_map.len();
        let mut row_index = 0;

        // Results.
        let mut build_indexes_idx = 0;
        let mut result_blocks = vec![];

        while row_index < outer_map_len {
            while row_index < outer_map_len && build_indexes_idx < max_block_size {
                unsafe {
                    if !outer_map.get_unchecked(row_index) {
                        build_indexes
                            .get_unchecked_mut(build_indexes_idx)
                            .chunk_index = chunk_index as u32;
                        build_indexes.get_unchecked_mut(build_indexes_idx).row_index =
                            row_index as u32;
                        build_indexes_idx += 1;
                    }
                }
                row_index += 1;
            }

            if self.hash_join_state.interrupt.load(Ordering::Relaxed) {
                return Err(ErrorCode::AbortedQuery(
                    "Aborted query, because the server is shutting down or the query was killed.",
                ));
            }

            result_blocks.push(self.hash_join_state.row_space.gather(
                &build_indexes[0..build_indexes_idx],
                &generation_state.build_columns,
                &generation_state.build_columns_data_type,
                &generation_state.build_num_rows,
                &mut probe_state.generation_state.string_items_buf,
            )?);
            build_indexes_idx = 0;
        }
        Ok(result_blocks)
    }

    pub fn left_mark_scan(
        &self,
        task: usize,
        probe_state: &mut ProbeState,
    ) -> Result<Vec<DataBlock>> {
        if self.hash_join_state.interrupt.load(Ordering::Relaxed) {
            return Err(ErrorCode::AbortedQuery(
                "Aborted query, because the server is shutting down or the query was killed.",
            ));
        }

        // Probe states.
        let max_block_size = probe_state.max_block_size;
        let mutable_indexes = &mut probe_state.mutable_indexes;
        let build_indexes = &mut mutable_indexes.build_indexes;

        // Build states.
        let build_state = unsafe { &*self.hash_join_state.build_state.get() };
        let generation_state = &build_state.generation_state;
        let mark_scan_map = &build_state.mark_scan_map;
        let chunk_index = task;
        let markers = &mark_scan_map[chunk_index];
        let has_null = *self
            .hash_join_state
            .hash_join_desc
            .marker_join_desc
            .has_null
            .read();
        let markers_len = markers.len();
        let mut row_index = 0;

        // Results.
        let mut build_indexes_idx = 0;
        let mut result_blocks = vec![];

        while row_index < markers_len {
            let block_size = std::cmp::min(markers_len - row_index, max_block_size);
            let mut validity = MutableBitmap::with_capacity(block_size);
            let mut boolean_bit_map = MutableBitmap::with_capacity(block_size);
            while build_indexes_idx < block_size {
                let marker = if unsafe { *markers.get_unchecked(row_index) } == MARKER_KIND_FALSE
                    && has_null
                {
                    MARKER_KIND_NULL
                } else {
                    unsafe { *markers.get_unchecked(row_index) }
                };
                if marker == MARKER_KIND_NULL {
                    validity.push(false);
                } else {
                    validity.push(true);
                }
                if marker == MARKER_KIND_TRUE {
                    boolean_bit_map.push(true);
                } else {
                    boolean_bit_map.push(false);
                }
                unsafe {
                    build_indexes
                        .get_unchecked_mut(build_indexes_idx)
                        .chunk_index = chunk_index as u32;
                    build_indexes.get_unchecked_mut(build_indexes_idx).row_index = row_index as u32;
                }
                build_indexes_idx += 1;
                row_index += 1;
            }

            if self.hash_join_state.interrupt.load(Ordering::Relaxed) {
                return Err(ErrorCode::AbortedQuery(
                    "Aborted query, because the server is shutting down or the query was killed.",
                ));
            }

            let boolean_column = Column::Boolean(boolean_bit_map.into());
            let marker_column = Column::Nullable(Box::new(NullableColumn {
                column: boolean_column,
                validity: validity.into(),
            }));
            let marker_block = DataBlock::new_from_columns(vec![marker_column]);
            let build_block = self.hash_join_state.row_space.gather(
                &build_indexes[0..build_indexes_idx],
                &generation_state.build_columns,
                &generation_state.build_columns_data_type,
                &generation_state.build_num_rows,
                &mut probe_state.generation_state.string_items_buf,
            )?;
            result_blocks.push(self.merge_eq_block(
                Some(build_block),
                Some(marker_block),
                build_indexes_idx,
            ));

            build_indexes_idx = 0;
        }
        Ok(result_blocks)
    }

    pub(crate) fn join_type(&self) -> JoinType {
        self.hash_join_state.hash_join_desc.join_type.clone()
    }
}
