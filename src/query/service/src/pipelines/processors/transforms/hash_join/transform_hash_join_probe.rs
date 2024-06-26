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

use std::any::Any;
use std::collections::VecDeque;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_sql::optimizer::ColumnSet;
use databend_common_sql::plans::JoinType;

use crate::pipelines::processors::transforms::hash_join::transform_hash_join_build::HashTableType;
use crate::pipelines::processors::transforms::hash_join::HashJoinProbeState;
use crate::pipelines::processors::transforms::hash_join::HashJoinSpiller;
use crate::pipelines::processors::transforms::hash_join::ProbeState;
use crate::pipelines::processors::Event;
use crate::pipelines::processors::InputPort;
use crate::pipelines::processors::OutputPort;
use crate::pipelines::processors::Processor;

enum FinalScanType {
    HashJoin,
    MergeInto,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Step {
    Sync(SyncStep),
    Async(AsyncStep),
    Finish,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SyncStep {
    // Probe hash table.
    Probe,
    // Final scan for right-related join or merge into.
    FinalScan,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AsyncStep {
    // Wait build side hash table to finish.
    WaitBuild,
    // Wait probe side to finish.
    WaitProbe,
    // Spill data blocks.
    Spill,
    // Restore spilled data.
    Restore,
    // Prepare for next round.
    NextRound,
}

pub struct TransformHashJoinProbe {
    input_port: Arc<InputPort>,
    output_port: Arc<OutputPort>,
    // The data blocks from input port.
    input_data_blocks: VecDeque<DataBlock>,
    // The data blocks need to spill.
    data_blocks_need_to_spill: Vec<DataBlock>,
    // The unspilled data blocks need to probe.
    unspilled_data_blocks_need_to_probe: VecDeque<DataBlock>,
    // The restored data blocks from spilled data.
    restored_data_blocks: VecDeque<DataBlock>,
    // The projections for output data blocks.
    projections: ColumnSet,
    // The output data blocks need to send to output port.
    pub(crate) output_data_blocks: VecDeque<DataBlock>,

    pub(crate) join_probe_state: Arc<HashJoinProbeState>,
    pub(crate) probe_state: ProbeState,
    // The max block size used to probe.
    pub(crate) max_block_size: usize,
    // There are three types of hash table:
    // 1. FirstRound: it is the first time the hash table is constructed.
    // 2. Restored: the hash table is restored from the spilled data.
    // 3. Empty: the hash table is empty.
    hash_table_type: HashTableType,
    // There are two types of final scan:
    // 1. HashJoin: the final scan for right-related join type.
    // 2. MergeInto: the final scan for merge into.
    final_scan_type: FinalScanType,

    // States for different steps.
    // Whether spill has happened.
    is_spill_happened: bool,
    // Whether the hash table build phase is finished.
    is_build_finished: bool,
    // Whether the final scan step is finished.
    is_final_scan_finished: bool,
    // Whether the join type can probe first round if spill happened.
    can_probe_first_round: bool,

    // Spill related states.
    // The spiller is used to spill/restore data blocks.
    spiller: HashJoinSpiller,
    // The next partition id to be restored.
    partition_id_to_restore: u8,

    step: Step,
    step_logs: Vec<Step>,
}

impl TransformHashJoinProbe {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        projections: ColumnSet,
        join_probe_state: Arc<HashJoinProbeState>,
        max_block_size: usize,
        func_ctx: FunctionContext,
        join_type: &JoinType,
        with_conjunct: bool,
        has_string_column: bool,
    ) -> Result<Box<dyn Processor>> {
        join_probe_state.probe_attach();
        // Create a hash join spiller.
        let hash_join_state = join_probe_state.hash_join_state.clone();
        let hash_keys = hash_join_state.hash_join_desc.probe_keys.clone();
        let hash_method = join_probe_state.hash_method.clone();
        let spiller = HashJoinSpiller::create(
            join_probe_state.ctx.clone(),
            hash_join_state,
            hash_keys,
            hash_method,
            join_probe_state.hash_join_state.spill_partition_bits,
            false,
        )?;

        let other_predicate = join_probe_state
            .hash_join_state
            .hash_join_desc
            .other_predicate
            .clone();

        let can_probe_first_round = join_probe_state.hash_join_state.can_probe_first_round();
        Ok(Box::new(TransformHashJoinProbe {
            input_port,
            output_port,
            projections,
            input_data_blocks: VecDeque::new(),
            data_blocks_need_to_spill: Vec::new(),
            unspilled_data_blocks_need_to_probe: VecDeque::new(),
            restored_data_blocks: VecDeque::new(),
            output_data_blocks: VecDeque::new(),
            step: Step::Async(AsyncStep::WaitBuild),
            step_logs: vec![Step::Async(AsyncStep::WaitBuild)],
            join_probe_state,
            probe_state: ProbeState::create(
                max_block_size,
                join_type,
                with_conjunct,
                has_string_column,
                func_ctx,
                other_predicate,
            ),
            max_block_size,
            is_spill_happened: false,
            is_final_scan_finished: false,
            // processor_id,
            is_build_finished: false,
            final_scan_type: FinalScanType::HashJoin,
            can_probe_first_round,
            hash_table_type: HashTableType::Empty,
            partition_id_to_restore: 0,
            spiller,
        }))
    }

    fn next_step(&mut self, step: Step) -> Result<Event> {
        let event = match step {
            Step::Sync(_) => Event::Sync,
            Step::Async(_) => Event::Async,
            Step::Finish => {
                self.input_port.finish();
                self.output_port.finish();
                self.finish_build()?;
                Event::Finished
            }
        };
        self.step = step.clone();
        self.step_logs.push(step);
        Ok(event)
    }

    fn probe(&mut self) -> Result<Event> {
        if self.output_port.is_finished() {
            if self.need_final_scan() {
                return self.next_step(Step::Async(AsyncStep::WaitProbe));
            } else {
                return self.next_step(Step::Finish);
            }
        }

        if !self.output_port.can_push() {
            self.input_port.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data_block() {
            self.output_port.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if !self.data_blocks_need_to_spill.is_empty() {
            return self.next_step(Step::Async(AsyncStep::Spill));
        }

        if self.input_port.has_data() {
            let data_block = self.input_port.pull_data().unwrap()?;
            self.add_data_block(data_block);
        }

        if !self.data_blocks_need_to_spill.is_empty() {
            return self.next_step(Step::Async(AsyncStep::Spill));
        }

        if !self.input_data_blocks.is_empty()
            || !self.unspilled_data_blocks_need_to_probe.is_empty()
        {
            return self.next_step(Step::Sync(SyncStep::Probe));
        }

        if !self.input_port.is_finished() {
            self.input_port.set_need_data();
            return Ok(Event::NeedData);
        }

        // Input port is finished
        if !self.restored_data_blocks.is_empty() {
            return self.next_step(Step::Sync(SyncStep::Probe));
        }

        // If there is no Datablocks which need to probe, go to final scan.
        if let Some(final_scan_type) = self.final_scan_type() {
            if !self.is_first_round() || !self.is_spill_happened || self.can_probe_first_round() {
                self.final_scan_type = final_scan_type;
                return self.next_step(Step::Async(AsyncStep::WaitProbe));
            }
        }

        self.next_round()
    }

    fn final_scan(&mut self) -> Result<Event> {
        if self.output_port.is_finished() {
            return self.next_step(Step::Finish);
        }

        if !self.output_port.can_push() {
            self.input_port.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data_block() {
            self.output_port.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if !self.is_final_scan_finished {
            return self.next_step(Step::Sync(SyncStep::FinalScan));
        }

        self.next_round()
    }

    fn wait_build(&mut self) -> Result<Event> {
        if self.is_build_finished() {
            match self.hash_table_type {
                HashTableType::FirstRound => self.probe(),
                HashTableType::Restored => self.next_step(Step::Async(AsyncStep::Restore)),
                HashTableType::Empty => {
                    if self.can_fast_return() {
                        self.next_step(Step::Finish)
                    } else {
                        self.probe()
                    }
                }
                HashTableType::UnFinished => {
                    unreachable!("Hash Table is finished")
                }
            }
        } else {
            self.next_step(Step::Async(AsyncStep::WaitBuild))
        }
    }
}

#[async_trait::async_trait]
impl Processor for TransformHashJoinProbe {
    fn name(&self) -> String {
        "HashJoinProbe".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        match &self.step {
            Step::Sync(step) => match step {
                SyncStep::Probe => self.probe(),
                SyncStep::FinalScan => self.final_scan(),
            },
            Step::Async(step) => match step {
                AsyncStep::WaitBuild | AsyncStep::NextRound => self.wait_build(),
                AsyncStep::WaitProbe => {
                    if self.output_port.is_finished() {
                        self.next_step(Step::Finish)
                    } else {
                        self.final_scan()
                    }
                }
                AsyncStep::Spill | AsyncStep::Restore => self.probe(),
            },
            Step::Finish => self.next_step(Step::Finish),
        }
    }

    fn interrupt(&self) {
        self.join_probe_state.hash_join_state.interrupt()
    }

    fn process(&mut self) -> Result<()> {
        match self.step {
            Step::Sync(SyncStep::Probe) => {
                match self.hash_table_type {
                    HashTableType::FirstRound | HashTableType::Empty => {
                        if let Some(data_block) =
                            self.unspilled_data_blocks_need_to_probe.pop_front()
                        {
                            self.probe_hash_table(data_block)?;
                        } else if let Some(data_block) = self.input_data_blocks.pop_front() {
                            let data_block = data_block.convert_to_full();
                            if self.is_spill_happened() {
                                self.probe_hash_table(data_block.clone())?;
                                self.data_blocks_need_to_spill.push(data_block);
                            } else {
                                self.probe_hash_table(data_block)?;
                            }
                        }
                    }
                    HashTableType::Restored => {
                        if let Some(data_block) = self.restored_data_blocks.pop_front() {
                            self.probe_hash_table(data_block)?;
                        }
                    }
                    _ => {}
                }
                Ok(())
            }
            Step::Sync(SyncStep::FinalScan) => {
                match self.final_scan_type {
                    FinalScanType::HashJoin => {
                        if let Some(task) = self.join_probe_state.final_scan_task() {
                            self.fill_rows(task)?;
                            return Ok(());
                        }
                    }
                    FinalScanType::MergeInto => {
                        if let Some(item) = self
                            .join_probe_state
                            .final_merge_into_partial_unmodified_scan_task()
                        {
                            self.final_merge_into_partial_unmodified_scan(item)?;
                            return Ok(());
                        }
                    }
                }
                // No more task, final scan finished.
                self.is_final_scan_finished = true;
                Ok(())
            }
            _ => unreachable!(),
        }
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match self.step {
            Step::Async(AsyncStep::WaitBuild) => {
                self.hash_table_type = self
                    .join_probe_state
                    .hash_join_state
                    .wait_build_notify()
                    .await?;
                self.is_spill_happened = self
                    .join_probe_state
                    .hash_join_state
                    .is_spill_happened
                    .load(Ordering::Relaxed);
                self.is_build_finished = true;
            }
            Step::Async(AsyncStep::WaitProbe) => {
                match self.final_scan_type {
                    FinalScanType::HashJoin => {
                        self.join_probe_state.probe_done()?;
                    }
                    FinalScanType::MergeInto => {
                        self.join_probe_state
                            .probe_merge_into_partial_modified_done()?;
                    }
                }
                self.join_probe_state.barrier.wait().await;
            }
            Step::Async(AsyncStep::Spill) => {
                let mut build_spilled_partitions = self
                    .join_probe_state
                    .hash_join_state
                    .spilled_partitions
                    .read()
                    .clone();
                let next_partition_id_to_restore = self
                    .join_probe_state
                    .hash_join_state
                    .partition_id
                    .load(Ordering::Relaxed);
                build_spilled_partitions.insert(next_partition_id_to_restore);
                let unspilled_data_blocks = self
                    .spiller
                    .spill(
                        &self.data_blocks_need_to_spill,
                        Some(&build_spilled_partitions),
                    )
                    .await?;
                self.data_blocks_need_to_spill.clear();
                if self.is_left_related_join_type() {
                    Self::add_splitted_data_blocks(
                        &mut self.unspilled_data_blocks_need_to_probe,
                        unspilled_data_blocks,
                        self.max_block_size,
                    );
                }
            }
            Step::Async(AsyncStep::Restore) => {
                Self::add_splitted_data_blocks(
                    &mut self.restored_data_blocks,
                    self.spiller.restore(self.partition_id_to_restore).await?,
                    self.max_block_size,
                );
            }
            Step::Async(AsyncStep::NextRound) => {
                self.reset_probe_state()?;
                self.reset_for_next_round();
                self.join_probe_state.barrier.wait().await;
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}

impl TransformHashJoinProbe {
    fn is_build_finished(&self) -> bool {
        self.is_build_finished
    }

    fn can_probe_first_round(&self) -> bool {
        self.can_probe_first_round
    }

    fn is_first_round(&self) -> bool {
        self.hash_table_type == HashTableType::FirstRound
    }

    // Probe with hashtable
    pub(crate) fn probe_hash_table(&mut self, block: DataBlock) -> Result<()> {
        self.probe_state.clear();
        let data_blocks = self.join_probe_state.probe(block, &mut self.probe_state)?;
        if !data_blocks.is_empty() {
            self.output_data_blocks.extend(data_blocks);
        }
        Ok(())
    }

    // For right-related join type, such as right/full outer join
    // if rows in build side can't find matched rows in probe side
    // create null blocks for unmatched rows in probe side.
    fn fill_rows(&mut self, task: usize) -> Result<()> {
        let data_blocks = self
            .join_probe_state
            .final_scan(task, &mut self.probe_state)?;
        if !data_blocks.is_empty() {
            self.output_data_blocks.extend(data_blocks);
        }
        Ok(())
    }

    // Check if probe side needs to probe
    fn is_spill_happened(&self) -> bool {
        self.is_spill_happened
    }

    fn need_final_scan(&self) -> bool {
        self.join_probe_state.hash_join_state.need_final_scan()
    }

    fn finish_build(&self) -> Result<()> {
        let old_count = self
            .join_probe_state
            .wait_probe_counter
            .fetch_sub(1, Ordering::Relaxed);
        if old_count == 1 {
            self.join_probe_state
                .hash_join_state
                .need_next_round
                .store(false, Ordering::Relaxed);
            self.join_probe_state
                .hash_join_state
                .continue_build_watcher
                .send(true)
                .map_err(|_| ErrorCode::TokioError("continue_build_watcher channel is closed"))?;
        }
        Ok(())
    }

    fn output_data_block(&mut self) -> Option<DataBlock> {
        if let Some(data_block) = self.output_data_blocks.pop_front() {
            Some(data_block.project(&self.projections))
        } else {
            None
        }
    }

    fn add_data_block(&mut self, data_block: DataBlock) {
        if self.is_spill_happened() && !self.can_probe_first_round() {
            self.data_blocks_need_to_spill.push(data_block);
        } else {
            // Split data to `block_size` rows per sub block.
            Self::add_splitted_data_blocks(
                &mut self.input_data_blocks,
                vec![data_block],
                self.max_block_size,
            );
        }
    }

    fn add_splitted_data_blocks(
        data_blocks: &mut VecDeque<DataBlock>,
        data_blocks_to_split: Vec<DataBlock>,
        max_block_size: usize,
    ) {
        // Split data to `block_size` rows per sub block.
        for data_block in data_blocks_to_split {
            // Split data to `block_size` rows per sub block.
            let (sub_blocks, remain_block) = data_block.split_by_rows(max_block_size);
            data_blocks.extend(sub_blocks);
            if let Some(remain) = remain_block {
                data_blocks.push_back(remain);
            }
        }
    }

    fn final_scan_type(&self) -> Option<FinalScanType> {
        if self.join_probe_state.hash_join_state.need_final_scan() {
            Some(FinalScanType::HashJoin)
        } else if self
            .join_probe_state
            .hash_join_state
            .merge_into_need_target_partial_modified_scan()
        {
            Some(FinalScanType::MergeInto)
        } else {
            None
        }
    }

    fn is_left_related_join_type(&self) -> bool {
        matches!(
            self.join_probe_state.join_type(),
            JoinType::Left | JoinType::LeftSingle | JoinType::Full
        )
    }

    // The method is called after finishing each round
    pub fn reset_next_restore_file(&mut self) {
        self.spiller.reset_next_restore_file();
    }

    // Go to next round hash join if still has the partition in `spill_partitions`
    fn next_round(&mut self) -> Result<Event> {
        if self
            .join_probe_state
            .hash_join_state
            .need_next_round
            .load(Ordering::Relaxed)
        {
            self.next_step(Step::Async(AsyncStep::NextRound))
        } else {
            self.next_step(Step::Finish)
        }
    }

    fn reset_probe_state(&mut self) -> Result<()> {
        if self
            .join_probe_state
            .next_round_counter
            .fetch_sub(1, Ordering::Relaxed)
            == 1
        {
            self.join_probe_state
                .hash_join_state
                .build_watcher
                .send(HashTableType::UnFinished)
                .map_err(|_| ErrorCode::TokioError("build_done_watcher channel is closed"))?;
            self.join_probe_state
                .wait_probe_counter
                .store(self.join_probe_state.processor_count, Ordering::Relaxed);
            self.join_probe_state
                .next_round_counter
                .store(self.join_probe_state.processor_count, Ordering::Relaxed);
            self.join_probe_state
                .hash_join_state
                .continue_build_watcher
                .send(true)
                .map_err(|_| ErrorCode::TokioError("continue_build_watcher channel is closed"))?;
        }
        self.partition_id_to_restore = self
            .join_probe_state
            .hash_join_state
            .partition_id
            .load(Ordering::Relaxed);
        Ok(())
    }

    fn reset_for_next_round(&mut self) {
        self.probe_state.reset();
        self.is_build_finished = false;
        self.is_final_scan_finished = false;
        self.reset_next_restore_file();
    }

    // Check if directly go to fast return
    fn can_fast_return(&self) -> bool {
        matches!(
            self.join_probe_state
                .hash_join_state
                .hash_join_desc
                .join_type,
            JoinType::Inner
                | JoinType::Cross
                | JoinType::Right
                | JoinType::RightSingle
                | JoinType::RightAnti
                | JoinType::RightSemi
                | JoinType::LeftSemi
        )
    }
}
