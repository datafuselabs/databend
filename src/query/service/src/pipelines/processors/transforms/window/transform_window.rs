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

// Some variables and functions are named and designed with reference to ClickHouse.
// - https://github.com/ClickHouse/ClickHouse/blob/master/src/Processors/Transforms/WindowTransform.h
// - https://github.com/ClickHouse/ClickHouse/blob/master/src/Processors/Transforms/WindowTransform.cpp

use std::any::Any;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::arithmetics_type::ResultTypeOfUnary;
use databend_common_expression::types::Number;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::SortColumnDescription;
use databend_common_expression::Value;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_sql::executor::physical_plans::LagLeadDefault;
use databend_common_sql::plans::WindowFuncFrameUnits;

use super::frame_bound::FrameBound;
use super::window_function::WindowFuncAggImpl;
use super::window_function::WindowFunctionImpl;
use super::WindowFunctionInfo;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Default)]
struct RowPtr {
    pub block: usize,
    pub row: usize,
}

impl RowPtr {
    #[inline(always)]
    pub fn new(block: usize, row: usize) -> Self {
        Self { block, row }
    }
}

#[derive(Clone)]
struct WindowBlock {
    block: DataBlock,
    builder: ColumnBuilder,
}

/// The input [`DataBlock`] of [`TransformWindow`] should be sorted by partition and order by columns.
///
/// Window function will not change the rows count of the original data.
pub struct TransformWindow<T: Number> {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    state: ProcessorState,
    input_is_finished: bool,

    func: WindowFunctionImpl,

    partition_indices: Vec<usize>,
    // The second field indicate if the order by column is nullable.
    order_by: Vec<SortColumnDescription>,

    /// A queue of data blocks that we need to process.
    /// If partition is ended, we may free the data block from front of the queue.
    blocks: VecDeque<WindowBlock>,
    /// A queue of data blocks that can be output.
    outputs: VecDeque<DataBlock>,

    /// The monotonically increasing index of the current block in the queue.
    first_block: usize,
    next_output_block: usize,

    // Partition: [`partition_start`, `partition_end`). `partition_end` is excluded.
    partition_start: RowPtr,
    partition_end: RowPtr,
    partition_ended: bool,
    partition_size: usize,

    // Frame: [`frame_start`, `frame_end`). `frame_end` is excluded.
    frame_unit: WindowFuncFrameUnits,
    start_bound: FrameBound<T>,
    end_bound: FrameBound<T>,

    // Only used for ROWS frame, default value: 0. (when not used)
    rows_start_bound: usize,
    rows_end_bound: usize,

    // NULL frame is a special RANGE frame, we need to check if the frame is a null frame.
    need_check_null_frame: bool,
    // If current frame is a null frame. This is only used when `need_check_null_frame` is true.
    is_null_frame: bool,

    frame_start: RowPtr,
    frame_end: RowPtr,
    frame_started: bool,
    frame_ended: bool,

    // Can be used to optimize window frame sliding.
    prev_frame_start: RowPtr,
    prev_frame_end: RowPtr,

    current_row: RowPtr,

    /// The start of current peer group.
    /// For ROWS frame, it is the same as `current_row`.
    /// For RANGE frame, `peer_group_start` <= `current_row`
    peer_group_start: RowPtr,
    peer_group_end: RowPtr,
    peer_group_ended: bool,
    need_peer: bool,

    // Used for row_number
    current_row_in_partition: usize,
    // used for dense_rank and rank
    current_rank: usize,
    current_rank_count: usize,
    current_dense_rank: usize,

    // If `is_empty_frame`, the window function result of non-NULL rows will be NULL.
    is_empty_frame: bool,
    // If window function is ranking function
    is_ranking: bool,
}

impl<T: Number> TransformWindow<T> {
    #[inline(always)]
    fn blocks_end(&self) -> RowPtr {
        RowPtr::new(self.first_block + self.blocks.len(), 0)
    }

    #[inline(always)]
    fn block_rows(&self, index: &RowPtr) -> usize {
        self.block_at(index).num_rows()
    }

    #[inline(always)]
    fn block_at(&self, index: &RowPtr) -> &DataBlock {
        &self.blocks[index.block - self.first_block].block
    }

    #[inline(always)]
    fn column_at(&self, index: &RowPtr, column_index: usize) -> &Column {
        self.block_at(index)
            .get_by_offset(column_index)
            .value
            .as_column()
            .unwrap()
    }

    fn add_rows_within_partition(&self, mut cur: RowPtr, mut n: usize) -> RowPtr {
        debug_assert!(cur.ge(&self.partition_start) && cur.le(&self.partition_end));

        let end = &self.partition_end;
        while cur.block < end.block {
            let rows = self.block_rows(&cur);
            if cur.row + n < rows {
                cur.row += n;
                return cur;
            }
            n -= rows - cur.row;
            cur.block += 1;
            cur.row = 0;
        }
        cur.row = end.row.min(cur.row + n);
        cur
    }

    /// Advance the partition end to the next partition or the end of the data.
    fn advance_partition(&mut self) {
        if self.partition_ended {
            return;
        }

        let end = self.blocks_end();

        if self.input_is_finished {
            self.partition_ended = true;
            assert_eq!(self.partition_end, end);
            return;
        }

        if self.partition_end == end {
            return;
        }

        debug_assert!(end.block == self.partition_end.block + 1);

        let partition_by_columns = self.partition_indices.len();
        if partition_by_columns == 0 {
            self.partition_end = end;
            return;
        }

        debug_assert!(self.partition_start <= self.prev_frame_start);

        let block_rows = self.block_rows(&self.partition_end);

        // STEP 1: Increment `self.partition_end` until it reaches the end of the partition or the end of the block.
        while self.partition_end.row < block_rows {
            // STEP 2: Check each partition column to see if it has changed.
            let mut i = 0;
            while i < partition_by_columns {
                // Should use `prev_frame_start` or `peer_group_start` because the block at `partition_start` may already be popped out of the buffer queue.
                let index = if self.is_ranking {
                    &self.peer_group_start
                } else {
                    &self.prev_frame_start
                };
                let start_column = self.column_at(index, self.partition_indices[i]);
                let compare_column = self.column_at(&self.partition_end, self.partition_indices[i]);

                if unsafe {
                    start_column.index_unchecked(index.row)
                        != compare_column.index_unchecked(self.partition_end.row)
                } {
                    break;
                }
                i += 1;
            }

            // STEP 3: If any partition column has changed, we have reached the end of the partition.
            if i < partition_by_columns {
                self.partition_ended = true;
                return;
            }
            self.partition_end.row += 1;
            self.partition_size += 1;
        }

        assert_eq!(self.partition_end.row, block_rows);
        self.partition_end.block += 1;
        self.partition_end.row = 0;
        assert!(!self.partition_ended && self.partition_end == end);
    }

    fn advance_frame_start_rows_preceding(&mut self, n: usize) {
        if self.current_row_in_partition - 1 <= n {
            self.frame_start = self.partition_start;
        } else {
            self.frame_start = self.advance_row(self.prev_frame_start);
        }
        self.frame_started = true;
    }

    fn advance_frame_start_rows_following(&mut self, n: usize) {
        self.frame_start = if self.current_row_in_partition == 1 {
            self.add_rows_within_partition(self.current_row, n)
        } else {
            self.advance_row(self.prev_frame_start)
                .min(self.partition_end)
        };
        self.frame_started = self.partition_ended || self.frame_start < self.partition_end;
    }

    fn advance_frame_end_rows_preceding(&mut self, n: usize) {
        match (self.current_row_in_partition - 1).cmp(&n) {
            Ordering::Less => {
                self.frame_end = self.partition_start;
            }
            Ordering::Equal => {
                self.frame_end = self.advance_row(self.partition_start);
            }
            Ordering::Greater => {
                self.frame_end = self.advance_row(self.prev_frame_end);
            }
        }
        self.frame_ended = true;
    }

    fn advance_frame_end_rows_following(&mut self, n: usize) {
        self.frame_end = if self.current_row_in_partition == 1 {
            let next_end = self.add_rows_within_partition(self.current_row, n);
            self.frame_ended = self.partition_ended || next_end < self.partition_end;
            // `self.frame_end` is excluded.
            self.advance_row(next_end)
        } else {
            self.frame_ended = self.partition_ended || self.prev_frame_end < self.partition_end;
            self.advance_row(self.prev_frame_end)
        }
        .min(self.partition_end);
    }

    /// This function is used for both `ROWS` and `RANGE`.
    fn advance_frame_end_current_row(&mut self) {
        // Every frame must be processed to the end of the input block if the its partition is started.
        debug_assert!(
            self.frame_end.block == self.partition_end.block
                || self.frame_end.block + 1 == self.partition_end.block
        );

        if self.frame_end == self.partition_end {
            self.frame_ended = self.partition_ended;
            return;
        }

        let rows_end = if self.partition_end.row == 0 {
            debug_assert!(self.partition_end == self.blocks_end());
            self.block_rows(&self.frame_end)
        } else {
            self.partition_end.row
        };

        debug_assert!(self.frame_end.row < rows_end);
        while self.frame_end.row < rows_end {
            if !self.are_peers(&self.current_row, &self.frame_end, true) {
                self.frame_ended = true;
                return;
            }
            self.frame_end.row += 1;
        }

        if self.frame_end.row == self.block_rows(&self.frame_end) {
            self.frame_end.block += 1;
            self.frame_end.row = 0;
        }

        debug_assert!(self.frame_end == self.partition_end);
        self.frame_ended = self.partition_ended;
    }

    /// Advance the current row to the next row
    /// if the current row is the last row of the current block, advance the current block and row = 0
    fn advance_row(&self, mut row: RowPtr) -> RowPtr {
        debug_assert!(row.block >= self.first_block);
        if row == self.blocks_end() {
            return row;
        }
        if row.row < self.block_rows(&row) - 1 {
            row.row += 1;
        } else {
            row.block += 1;
            row.row = 0;
        }
        row
    }

    /// Goback the row to the preceding one row
    fn goback_row(&self, mut row: RowPtr) -> RowPtr {
        if row.row != 0 {
            row.row -= 1;
        } else if row.block == 0 {
            row.row = self.block_rows(&row) - 1;
        } else {
            row.block -= 1;
            row.row = self.block_rows(&row) - 1;
        }
        row
    }

    /// calculate peer_group_end
    fn advance_peer_group_end(&mut self, mut row: RowPtr) {
        if !self.need_peer {
            return;
        }

        let current_row = row;
        while row < self.partition_end {
            row = self.advance_row(row);
            if row == self.partition_end {
                break;
            }
            if self.are_peers(&current_row, &row, false) {
                continue;
            } else {
                self.peer_group_end = row;
                self.peer_group_ended = true;
                return;
            }
        }
        self.peer_group_ended = self.partition_ended;
        self.peer_group_end = self.partition_end;
    }

    /// If the two rows are within the same peer group.
    fn are_peers(&self, lhs: &RowPtr, rhs: &RowPtr, for_computing_bound: bool) -> bool {
        if lhs == rhs {
            return true;
        }
        if lhs.block < self.first_block {
            return false;
        }

        if self.frame_unit.is_rows() && for_computing_bound {
            // For ROWS frame, the row's peer is only the row itself.
            return false;
        }

        if self.order_by.is_empty() {
            return true;
        }

        let mut i = 0;
        while i < self.order_by.len() {
            let lhs_col = self.column_at(lhs, self.order_by[i].offset);
            let rhs_col = self.column_at(rhs, self.order_by[i].offset);

            if unsafe { lhs_col.index_unchecked(lhs.row) != rhs_col.index_unchecked(rhs.row) } {
                return false;
            }
            i += 1;
        }
        true
    }

    fn check_outputs(&mut self) {
        while self.next_output_block - self.first_block < self.blocks.len() {
            let block = &mut self.blocks[self.next_output_block - self.first_block];

            if block.block.num_rows() == block.builder.len() {
                // Can output
                let mut output = block.block.clone();
                let data_type = block.builder.data_type();
                // The memory of the builder can be released.
                let builder = std::mem::replace(
                    &mut block.builder,
                    ColumnBuilder::with_capacity(&data_type, 0),
                );
                let new_column = builder.build();
                output.add_column(BlockEntry::new(
                    new_column.data_type(),
                    Value::Column(new_column),
                ));
                self.outputs.push_back(output);
                self.next_output_block += 1;
            } else {
                break;
            }
        }

        // Release memory that is no longer needed.
        let first_used_block = if self.is_ranking {
            self.next_output_block.min(self.peer_group_start.block)
        } else {
            self.next_output_block.min(self.prev_frame_start.block)
        }
        .min(self.current_row.block);

        if self.first_block < first_used_block {
            self.blocks.drain(..first_used_block - self.first_block);
            self.first_block = first_used_block;
        }
    }

    fn apply_aggregate(&self, agg: &WindowFuncAggImpl) -> Result<()> {
        debug_assert!(self.frame_started);
        debug_assert!(self.frame_ended);
        debug_assert!(self.frame_start <= self.frame_end);
        debug_assert!(self.prev_frame_start <= self.prev_frame_end);
        debug_assert!(self.prev_frame_start <= self.frame_start);
        debug_assert!(self.prev_frame_end <= self.frame_end);
        debug_assert!(self.partition_start <= self.frame_start);
        debug_assert!(self.frame_end <= self.partition_end);

        let (rows_start, rows_end, reset) = if self.frame_start == self.prev_frame_start {
            (self.prev_frame_end, self.frame_end, false)
        } else {
            (self.frame_start, self.frame_end, true)
        };

        if reset {
            agg.reset();
        }

        let end_block = if rows_end.row == 0 {
            rows_end.block
        } else {
            rows_end.block + 1
        };

        for block in rows_start.block..end_block {
            let data = &self.blocks[block - self.first_block].block;
            let start_row = if block == rows_start.block {
                rows_start.row
            } else {
                0
            };
            let end_row = if block == rows_end.block {
                rows_end.row
            } else {
                data.num_rows()
            };
            let cols = agg.arg_columns(data);
            for row in start_row..end_row {
                agg.accumulate_row(cols, row)?;
            }
        }

        Ok(())
    }

    #[inline]
    fn merge_result_of_current_row(&mut self) -> Result<()> {
        match &self.func {
            WindowFunctionImpl::Aggregate(agg) => {
                let builder = &mut self.blocks[self.current_row.block - self.first_block].builder;
                agg.merge_result(builder)?;
            }
            WindowFunctionImpl::RowNumber => {
                let builder = &mut self.blocks[self.current_row.block - self.first_block].builder;
                builder.push(ScalarRef::Number(NumberScalar::UInt64(
                    self.current_row_in_partition as u64,
                )));
            }
            WindowFunctionImpl::Rank => {
                let builder = &mut self.blocks[self.current_row.block - self.first_block].builder;
                builder.push(ScalarRef::Number(NumberScalar::UInt64(
                    self.current_rank as u64,
                )));
            }
            WindowFunctionImpl::DenseRank => {
                let builder = &mut self.blocks[self.current_row.block - self.first_block].builder;
                builder.push(ScalarRef::Number(NumberScalar::UInt64(
                    self.current_dense_rank as u64,
                )));
            }
            WindowFunctionImpl::PercentRank => {
                let builder = &mut self.blocks[self.current_row.block - self.first_block].builder;
                let percent = if self.partition_size <= 1 {
                    0_f64
                } else {
                    ((self.current_rank - 1) as f64) / ((self.partition_size - 1) as f64)
                };
                builder.push(ScalarRef::Number(NumberScalar::Float64(percent.into())));
            }
            WindowFunctionImpl::LagLead(ll) => {
                let value = if self.frame_start == self.frame_end {
                    match &ll.default {
                        LagLeadDefault::Null => Scalar::Null,
                        LagLeadDefault::Index(col) => {
                            let block =
                                &self.blocks[self.current_row.block - self.first_block].block;
                            let value = &block.get_by_offset(*col).value;
                            value.index(self.current_row.row).unwrap().to_owned()
                        }
                    }
                } else {
                    let block = &self
                        .blocks
                        .get(self.frame_start.block - self.first_block)
                        .unwrap()
                        .block;
                    let value = &block.get_by_offset(ll.arg).value;
                    value.index(self.frame_start.row).unwrap().to_owned()
                };

                let builder = &mut self.blocks[self.current_row.block - self.first_block].builder;
                builder.push(value.as_ref());
            }
            WindowFunctionImpl::NthValue(func) => {
                let value = if self.frame_start == self.frame_end {
                    Scalar::Null
                } else if let Some(mut n) = func.n {
                    let mut cur = self.frame_start;
                    // n is counting from 1
                    while n > 1 && cur < self.frame_end {
                        cur = self.advance_row(cur);
                        n -= 1;
                    }
                    if cur != self.frame_end {
                        self.get_nth_value_by_ignoring_nulls(cur, func.arg, func.ignore_null, true)
                    } else {
                        // No such row
                        Scalar::Null
                    }
                } else {
                    // last_value
                    let cur = self.goback_row(self.frame_end);
                    debug_assert!(self.frame_start <= cur);
                    self.get_nth_value_by_ignoring_nulls(cur, func.arg, func.ignore_null, false)
                };
                let builder = &mut self.blocks[self.current_row.block - self.first_block].builder;
                builder.push(value.as_ref());
            }
            WindowFunctionImpl::Ntile(ntile) => {
                let num_partition_rows = if self.partition_indices.is_empty() {
                    let mut rows = 0;
                    for i in self.frame_start.block..self.frame_end.block {
                        let row_ptr = RowPtr { block: i, row: 0 };
                        rows += self.block_rows(&row_ptr);
                    }
                    rows
                } else {
                    self.partition_size
                };
                let builder = &mut self.blocks[self.current_row.block - self.first_block].builder;
                let bucket = ntile.compute_nitle(self.current_row_in_partition, num_partition_rows);
                builder.push(ScalarRef::Number(NumberScalar::UInt64(bucket as u64)));
            }
            WindowFunctionImpl::CumeDist => {
                let cume_rows = {
                    let mut rows = 0;
                    let mut row = self.partition_start;
                    while row < self.peer_group_end {
                        row = self.advance_row(row);
                        rows += 1;
                    }
                    rows
                };

                let builder = &mut self.blocks[self.current_row.block - self.first_block].builder;

                let cume_dist = if self.partition_size > 0 {
                    cume_rows as f64 / self.partition_size as f64
                } else {
                    0_f64
                };
                builder.push(ScalarRef::Number(NumberScalar::Float64(cume_dist.into())));
            }
        };

        Ok(())
    }

    #[inline]
    fn if_need_check_null_frame(&self) -> bool {
        self.frame_unit.is_range() && self.order_by.len() == 1 && self.order_by[0].is_nullable
    }

    #[inline]
    fn is_in_null_frame(&self) -> bool {
        // Only RANGE frame could be null frame.
        debug_assert!(self.frame_unit.is_range());
        debug_assert!(self.order_by.len() == 1);
        let col = self.column_at(&self.current_row, self.order_by[0].offset);
        let value = unsafe { col.index_unchecked(self.current_row.row) };
        if value.is_null() {
            return true;
        }
        false
    }

    #[inline]
    fn get_nth_value_by_ignoring_nulls(
        &self,
        mut cur: RowPtr,
        arg_index: usize,
        ignore_null: bool,
        advance: bool,
    ) -> Scalar {
        let block = &self.blocks.get(cur.block - self.first_block).unwrap().block;
        let mut block_entry = block.get_by_offset(arg_index);
        if !ignore_null {
            return match &block_entry.value {
                Value::Scalar(scalar) => scalar.to_owned(),
                Value::Column(col) => unsafe { col.index_unchecked(cur.row) }.to_owned(),
            };
        }

        while (advance && cur < self.frame_end) || (!advance && cur >= self.frame_start) {
            match &block_entry.value {
                Value::Scalar(scalar) => {
                    if scalar != &Scalar::Null {
                        return scalar.to_owned();
                    }
                    // If value is Scalar we can directly skip this block.
                    if advance {
                        cur.block += 1;
                        let block = &self.blocks.get(cur.block - self.first_block).unwrap().block;
                        block_entry = block.get_by_offset(arg_index);
                    } else if cur == self.frame_start {
                        return scalar.to_owned();
                    } else {
                        cur.block -= 1;
                        let block = &self.blocks.get(cur.block - self.first_block).unwrap().block;
                        block_entry = block.get_by_offset(arg_index);
                    }
                }
                Value::Column(col) => {
                    let value = col.index(cur.row).unwrap();
                    if value != ScalarRef::Null {
                        return value.to_owned();
                    }

                    cur = if advance {
                        let advance_cur = self.advance_row(cur);
                        if advance_cur.block != cur.block {
                            block_entry = self
                                .blocks
                                .get(advance_cur.block - self.first_block)
                                .unwrap()
                                .block
                                .get_by_offset(arg_index);
                        }
                        advance_cur
                    } else if cur == self.frame_start {
                        return unsafe { col.index_unchecked(cur.row) }.to_owned();
                    } else {
                        let back_cur = self.goback_row(cur);
                        if back_cur.block != cur.block {
                            block_entry = self
                                .blocks
                                .get(back_cur.block - self.first_block)
                                .unwrap()
                                .block
                                .get_by_offset(arg_index);
                        }
                        back_cur
                    };
                }
            }
        }
        Scalar::Null
    }
}

// For ROWS frame
impl TransformWindow<u64> {
    /// Cannot be cloned because every [`TransformWindow`] has one independent `place`.
    pub fn try_create_rows(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        func: WindowFunctionInfo,
        partition_indices: Vec<usize>,
        order_by: Vec<SortColumnDescription>,
        bounds: (FrameBound<u64>, FrameBound<u64>),
    ) -> Result<Self> {
        let func = WindowFunctionImpl::try_create(func)?;
        let (start_bound, end_bound) = bounds;

        let is_empty_frame = start_bound > end_bound;
        let is_ranking = matches!(
            func,
            WindowFunctionImpl::RowNumber
                | WindowFunctionImpl::Rank
                | WindowFunctionImpl::DenseRank
        );

        let rows_start_bound = start_bound.get_inner().unwrap_or_default() as usize;
        let rows_end_bound = end_bound.get_inner().unwrap_or_default() as usize;

        Ok(Self {
            input,
            output,
            state: ProcessorState::Consume,
            func,
            partition_indices,
            order_by,
            blocks: VecDeque::new(),
            outputs: VecDeque::new(),
            first_block: 0,
            next_output_block: 0,
            partition_start: RowPtr::default(),
            partition_end: RowPtr::default(),
            partition_ended: false,
            partition_size: 0,
            frame_unit: WindowFuncFrameUnits::Rows,
            start_bound,
            end_bound,
            rows_start_bound,
            rows_end_bound,
            need_check_null_frame: false,
            is_null_frame: false,
            frame_start: RowPtr::default(),
            frame_end: RowPtr::default(),
            frame_started: false,
            frame_ended: false,
            prev_frame_start: RowPtr::default(),
            prev_frame_end: RowPtr::default(),
            peer_group_start: RowPtr::default(),
            peer_group_end: RowPtr::default(),
            peer_group_ended: false,
            need_peer: false,
            current_row: RowPtr::default(),
            current_row_in_partition: 1,
            current_rank: 1,
            current_rank_count: 1,
            current_dense_rank: 1,
            input_is_finished: false,
            is_empty_frame,
            is_ranking,
        })
    }
}

// For RANGE frame
impl<T> TransformWindow<T>
where T: Number + ResultTypeOfUnary
{
    /// Cannot be cloned because every [`TransformWindow`] has one independent `place`.
    pub fn try_create_range(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        func: WindowFunctionInfo,
        partition_indices: Vec<usize>,
        order_by: Vec<SortColumnDescription>,
        bounds: (FrameBound<T>, FrameBound<T>),
    ) -> Result<Self> {
        let func = WindowFunctionImpl::try_create(func)?;
        let (start_bound, end_bound) = bounds;

        let is_empty_frame = start_bound > end_bound;
        let is_ranking = matches!(
            func,
            WindowFunctionImpl::RowNumber
                | WindowFunctionImpl::Rank
                | WindowFunctionImpl::DenseRank
        );

        // If the window clause is a specific RANGE window, we should deal with the frame with all NULL values.
        let need_check_null_frame = if order_by.len() == 1 {
            order_by[0].is_nullable
        } else {
            false
        };

        let need_peer = matches!(func, WindowFunctionImpl::CumeDist);

        Ok(Self {
            input,
            output,
            state: ProcessorState::Consume,
            func,
            partition_indices,
            order_by,
            blocks: VecDeque::new(),
            outputs: VecDeque::new(),
            first_block: 0,
            next_output_block: 0,
            partition_start: RowPtr::default(),
            partition_end: RowPtr::default(),
            partition_ended: false,
            partition_size: 0,
            frame_unit: WindowFuncFrameUnits::Range,
            start_bound,
            end_bound,
            rows_start_bound: 0,
            rows_end_bound: 0,
            need_check_null_frame,
            is_null_frame: false,
            frame_start: RowPtr::default(),
            frame_end: RowPtr::default(),
            frame_started: false,
            frame_ended: false,
            prev_frame_start: RowPtr::default(),
            prev_frame_end: RowPtr::default(),
            peer_group_start: RowPtr::default(),
            peer_group_end: RowPtr::default(),
            peer_group_ended: false,
            need_peer,
            current_row: RowPtr::default(),
            current_row_in_partition: 1,
            current_rank: 1,
            current_rank_count: 1,
            current_dense_rank: 1,
            input_is_finished: false,
            is_empty_frame,
            is_ranking,
        })
    }

    /// Used for `RANGE` frame to compare the value of the column at `cmp_row` with the value of the column at `ref_row` add/sub `offset`.
    ///
    /// Returns the ordering of the value at `cmp_row` with the value at `ref_row` add/sub `offset`.
    #[inline]
    fn compare_value_with_offset(cmp_v: T, ref_v: T, offset: T, is_preceding: bool) -> Ordering {
        let res = if is_preceding {
            ref_v.checked_sub(offset)
        } else {
            ref_v.checked_add(offset)
        };

        if let Some(res) = res {
            // Not overflow
            cmp_v.cmp(&res)
        } else if is_preceding {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    }

    fn advance_frame_start(&mut self) {
        if self.frame_started {
            return;
        }
        match &self.start_bound {
            FrameBound::CurrentRow => {
                debug_assert!(self.partition_start <= self.peer_group_start);
                debug_assert!(self.peer_group_start < self.partition_end);
                debug_assert!(self.peer_group_start <= self.current_row);

                self.frame_started = true;
                if self.frame_unit.is_range() {
                    self.frame_start = self.peer_group_start;
                } else {
                    self.frame_start = self.current_row;
                }
            }
            FrameBound::Preceding(Some(n)) => {
                debug_assert!(self.frame_unit.is_rows() || self.order_by.len() == 1);

                if self.is_null_frame {
                    self.frame_started = true;
                    self.frame_start = self.peer_group_start;
                } else if self.frame_unit.is_rows() {
                    self.advance_frame_start_rows_preceding(self.rows_start_bound);
                } else if self.order_by[0].is_nullable {
                    self.advance_frame_start_nullable_range(*n, true);
                } else {
                    self.advance_frame_start_range(*n, true);
                }
            }
            FrameBound::Preceding(_) => {
                self.frame_started = true;
            }
            FrameBound::Following(Some(n)) => {
                debug_assert!(self.frame_unit.is_rows() || self.order_by.len() == 1);

                if self.is_null_frame {
                    self.frame_started = true;
                    self.frame_start = self.peer_group_start;
                } else if self.frame_unit.is_rows() {
                    self.advance_frame_start_rows_following(self.rows_start_bound);
                } else if self.order_by[0].is_nullable {
                    self.advance_frame_start_nullable_range(*n, false);
                } else {
                    self.advance_frame_start_range(*n, false);
                }
            }
            FrameBound::Following(_) => {
                unreachable!()
            }
        }
    }

    fn advance_frame_end(&mut self) {
        debug_assert!(!self.frame_ended);

        match &self.end_bound {
            FrameBound::CurrentRow => {
                self.advance_frame_end_current_row();
            }
            FrameBound::Preceding(Some(n)) => {
                debug_assert!(self.frame_unit.is_rows() || self.order_by.len() == 1);

                if self.is_null_frame {
                    self.advance_frame_end_current_row();
                } else if self.frame_unit.is_rows() {
                    self.advance_frame_end_rows_preceding(self.rows_end_bound);
                } else if self.order_by[0].is_nullable {
                    self.advance_frame_end_nullable_range(*n, true);
                } else {
                    self.advance_frame_end_range(*n, true)
                }
            }
            FrameBound::Preceding(_) => {
                unreachable!()
            }
            FrameBound::Following(Some(n)) => {
                debug_assert!(self.frame_unit.is_rows() || self.order_by.len() == 1);

                if self.is_null_frame {
                    self.advance_frame_end_current_row();
                } else if self.frame_unit.is_rows() {
                    self.advance_frame_end_rows_following(self.rows_end_bound);
                } else if self.order_by[0].is_nullable {
                    self.advance_frame_end_nullable_range(*n, false);
                } else {
                    self.advance_frame_end_range(*n, false);
                }
            }
            FrameBound::Following(_) => {
                self.frame_ended = self.partition_ended;
                self.frame_end = self.partition_end;
            }
        }
    }

    fn compute_on_frame(&mut self) -> Result<()> {
        match &self.func {
            WindowFunctionImpl::Aggregate(agg) => self.apply_aggregate(agg),
            _ => Ok(()),
        }
    }

    /// When adding a [`DataBlock`], we compute the aggregations to the end.
    ///
    /// For each row in the input block,
    /// compute the aggregation result of its window frame and add it into result column buffer.
    ///
    /// If not reach the end bound of the window frame, hold the temporary aggregation value in `state_place`.
    ///
    /// Once collect all the results of one input [`DataBlock`], attach the corresponding result column to the input as output.
    fn add_block(&mut self, data: Option<DataBlock>) -> Result<()> {
        if let Some(data) = data {
            let num_rows = data.num_rows();
            if num_rows != 0 {
                self.blocks.push_back(WindowBlock {
                    block: data.consume_convert_to_full(),
                    builder: ColumnBuilder::with_capacity(&self.func.return_type()?, num_rows),
                });
            }
        }

        // Each loop will do:
        // 1. Try to advance the partition.
        // 2. Try to advance the frame (if the frame is not started or ended, break the loop and end the process).
        // 3.1 Compute the aggregation for current frame.
        // 3.2 Compute general information (row number, rank, ...)
        // 4. If the partition is not ended, break the loop;
        //    else start next partition.
        loop {
            // 1.
            self.advance_partition();

            debug_assert!(self.partition_ended || self.partition_end == self.blocks_end());
            debug_assert!({
                if self.partition_ended && self.partition_end == self.blocks_end() {
                    self.input_is_finished
                } else {
                    true
                }
            });

            while self.current_row < self.partition_end {
                if !self.are_peers(&self.peer_group_start, &self.current_row, false) {
                    self.peer_group_start = self.current_row;
                    self.peer_group_end = self.current_row;
                    self.peer_group_ended = false;
                    self.current_dense_rank += 1;
                    self.current_rank = self.current_row_in_partition;

                    // peer changed, re-calculate peer end.
                    self.advance_peer_group_end(self.peer_group_start);

                    // If current peer group is a null frame, there will be no null frame in this partition again;
                    // if current peer group is not a null frame, we may need to check it in the codes below.
                    self.is_null_frame = false;
                } else if self.is_null_frame {
                    // Only one null frame can exist in one partition, so we don't need to check it again.
                    self.need_check_null_frame = false;
                } else if self.is_ranking {
                    self.peer_group_start = self.current_row;
                }

                // execute only once for each partition.
                if self.peer_group_start == self.partition_start {
                    self.advance_peer_group_end(self.current_row);
                }

                if self.need_peer && self.partition_ended {
                    self.peer_group_ended = true;
                }

                if self.need_peer && !self.peer_group_ended {
                    debug_assert!(!self.input_is_finished);
                    debug_assert!(!self.partition_ended);
                    break;
                }

                // 2.
                if self.need_check_null_frame {
                    self.is_null_frame = self.is_in_null_frame();
                }

                if self.is_empty_frame && !self.is_null_frame {
                    // Non-NULL empty frame, no need to advance bounds.
                    self.func.reset();
                } else if !self.is_ranking {
                    self.advance_frame_start();
                    if !self.frame_started {
                        debug_assert!(!self.input_is_finished);
                        debug_assert!(!self.partition_ended);
                        break;
                    }

                    if self.frame_end < self.frame_start {
                        self.frame_end = self.frame_start;
                    }

                    self.advance_frame_end();
                    if !self.frame_ended {
                        debug_assert!(!self.input_is_finished);
                        debug_assert!(!self.partition_ended);
                        break;
                    }

                    // 3.1
                    self.compute_on_frame()?;
                }

                self.merge_result_of_current_row()?;

                // 3.2
                self.current_row = self.advance_row(self.current_row);
                self.current_row_in_partition += 1;
                self.prev_frame_start = self.frame_start;
                self.prev_frame_end = self.frame_end;
                self.frame_started = false;
                self.frame_ended = false;
            }

            if self.input_is_finished {
                return Ok(());
            }

            // 4.
            if !self.partition_ended {
                debug_assert!(self.partition_end == self.blocks_end());
                break;
            }

            // start to new partition
            {
                // reset function
                self.func.reset();

                // reset partition
                self.partition_start = self.partition_end;
                self.partition_end = self.advance_row(self.partition_end);
                self.partition_ended = false;
                self.partition_size = 1;

                // reset frames
                self.need_check_null_frame = self.if_need_check_null_frame();
                self.is_null_frame = false;
                self.frame_start = self.partition_start;
                self.frame_end = self.partition_start;
                self.prev_frame_start = self.frame_start;
                self.prev_frame_end = self.frame_end;

                // reset peer group
                self.peer_group_start = self.partition_start;
                self.peer_group_end = self.partition_start;

                // reset row number, rank, ...
                self.current_row_in_partition = 1;
                self.current_rank = 1;
                self.current_rank_count = 1;
                self.current_dense_rank = 1;
            }
        }

        Ok(())
    }
}

macro_rules! impl_advance_frame_bound_method {
    ($bound: ident, $op: ident) => {
        paste::paste! {
            impl<T: Number + ResultTypeOfUnary> TransformWindow<T> {
                fn [<advance_frame_ $bound _range>](&mut self, n: T, is_preceding: bool) {
                    let SortColumnDescription {
                        offset,
                        asc,
                        ..
                    } = self.order_by[0];
                    let preceding = asc == is_preceding;
                    let ref_col = self
                        .column_at(&self.current_row, offset)
                        .as_number()
                        .unwrap();
                    let ref_col = T::try_downcast_column(ref_col).unwrap();
                    let ref_v = unsafe { ref_col.get_unchecked(self.current_row.row) };
                    while self.[<frame_ $bound>] < self.partition_end {
                        let cmp_col = self
                            .column_at(&self.[<frame_ $bound>], offset)
                            .as_number()
                            .unwrap();
                        let cmp_col = T::try_downcast_column(cmp_col).unwrap();
                        let cmp_v = unsafe { cmp_col.get_unchecked(self.[<frame_ $bound>].row) };
                        let mut ordering = Self::compare_value_with_offset(*cmp_v, *ref_v, n, preceding);
                        if !asc {
                            ordering = ordering.reverse();
                        }

                        if ordering.$op() {
                            // `self.frame_end` is excluded when aggregating.
                            self.[<frame_ $bound ed>] = true;
                            return;
                        }

                        self.[<frame_ $bound>] = self.advance_row(self.[<frame_ $bound>]);
                    }
                    self.[<frame_ $bound ed>] = self.partition_ended;
                }

                fn [<advance_frame_ $bound _nullable_range>](&mut self, n: T, is_preceding: bool) {
                    let SortColumnDescription {
                        offset: ref_idx,
                        asc,
                        nulls_first,
                        ..
                    } = self.order_by[0];
                    let preceding = asc == is_preceding;
                    // Current row should not be in the null frame.
                    let ref_col = &self
                        .column_at(&self.current_row, ref_idx)
                        .as_nullable()
                        .unwrap()
                        .column;
                    let ref_col = T::try_downcast_column(ref_col.as_number().unwrap()).unwrap();
                    let ref_v = unsafe { ref_col.get_unchecked(self.current_row.row) };
                    while self.[<frame_ $bound>] < self.partition_end {
                        let col = self
                            .column_at(&self.[<frame_ $bound>], ref_idx)
                            .as_nullable()
                            .unwrap();
                        let validity = &col.validity;
                        if unsafe { !validity.get_bit_unchecked(self.[<frame_ $bound>].row) } {
                            // Need to skip null rows.
                            if nulls_first {
                                // The null rows are at front.
                                self.[<frame_ $bound>] = self.advance_row(self.[<frame_ $bound>]);
                                continue;
                            } else {
                                // The null rows are at back.
                                self.[<frame_ $bound ed>] = true;
                                return;
                            }
                        }
                        let cmp_col = T::try_downcast_column(col.column.as_number().unwrap()).unwrap();
                        let cmp_v = unsafe { cmp_col.get_unchecked(self.[<frame_ $bound>].row) };
                        let mut ordering = Self::compare_value_with_offset(*cmp_v, *ref_v, n, preceding);
                        if !asc {
                            ordering = ordering.reverse();
                        }

                        if ordering.$op() {
                            self.[<frame_ $bound ed>] = true;
                            return;
                        }

                        self.[<frame_ $bound>] = self.advance_row(self.[<frame_ $bound>]);
                    }
                    self.[<frame_ $bound ed>] = self.partition_ended;
                }
            }
        }
    };
}

impl_advance_frame_bound_method!(start, is_ge);
impl_advance_frame_bound_method!(end, is_gt);

enum ProcessorState {
    Consume,
    AddBlock(Option<DataBlock>),
    Output,
}

#[async_trait::async_trait]
impl<T> Processor for TransformWindow<T>
where T: Number + ResultTypeOfUnary
{
    fn name(&self) -> String {
        "Transform Window".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        let input_is_finished = self.input.is_finished();
        match self.state {
            ProcessorState::Consume => {
                self.input.set_need_data();
                let has_data = self.input.has_data();
                match (input_is_finished, has_data) {
                    (_, true) => {
                        let data = self.input.pull_data().transpose()?;
                        self.state = ProcessorState::AddBlock(data);
                        Ok(Event::Sync)
                    }
                    (false, false) => Ok(Event::NeedData),
                    (true, _) => {
                        // input_is_finished should be set after adding block.
                        self.input_is_finished = true;
                        if self.next_output_block - self.first_block < self.blocks.len() {
                            // There are still some blocks are not output.
                            self.state = ProcessorState::AddBlock(None);
                            Ok(Event::Sync)
                        } else {
                            self.output.finish();
                            Ok(Event::Finished)
                        }
                    }
                }
            }
            ProcessorState::Output => {
                let output = self.outputs.pop_front().unwrap();
                self.output.push_data(Ok(output));
                if self.outputs.is_empty() {
                    self.state = ProcessorState::Consume;
                }
                Ok(Event::NeedConsume)
            }
            _ => unreachable!(),
        }
    }

    fn process(&mut self) -> Result<()> {
        if let ProcessorState::AddBlock(data) =
            std::mem::replace(&mut self.state, ProcessorState::Consume)
        {
            self.add_block(data)?;
            self.check_outputs();
            self.state = if self.outputs.is_empty() {
                ProcessorState::Consume
            } else {
                ProcessorState::Output
            };
        } else {
            unreachable!()
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use databend_common_base::base::tokio;
    use databend_common_exception::Result;
    use databend_common_expression::block_debug::assert_blocks_eq;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::Column;
    use databend_common_expression::ColumnBuilder;
    use databend_common_expression::DataBlock;
    use databend_common_expression::FromData;
    use databend_common_expression::SortColumnDescription;
    use databend_common_functions::aggregates::AggregateFunctionFactory;
    use databend_common_pipeline_core::processors::connect;
    use databend_common_pipeline_core::processors::Event;
    use databend_common_pipeline_core::processors::InputPort;
    use databend_common_pipeline_core::processors::OutputPort;
    use databend_common_pipeline_core::processors::Processor;
    use databend_common_sql::plans::WindowFuncFrameUnits;

    use super::TransformWindow;
    use super::WindowBlock;
    use crate::pipelines::processors::transforms::window::transform_window::RowPtr;
    use crate::pipelines::processors::transforms::window::FrameBound;
    use crate::pipelines::processors::transforms::window::WindowFunctionInfo;

    fn get_ranking_transform_window(
        bounds: (FrameBound<u64>, FrameBound<u64>),
    ) -> Result<TransformWindow<u64>> {
        let func = WindowFunctionInfo::DenseRank;
        TransformWindow::try_create_range(
            InputPort::create(),
            OutputPort::create(),
            func,
            vec![],
            vec![SortColumnDescription {
                offset: 0,
                asc: false,
                nulls_first: false,
                is_nullable: false,
            }],
            bounds,
        )
    }

    fn get_transform_window_without_partition(
        _unit: WindowFuncFrameUnits,
        bounds: (FrameBound<u64>, FrameBound<u64>),
        arg_type: DataType,
    ) -> Result<TransformWindow<u64>> {
        let agg = AggregateFunctionFactory::instance().get("sum", vec![], vec![arg_type])?;
        let func = WindowFunctionInfo::Aggregate(agg, vec![0]);
        TransformWindow::try_create_rows(
            InputPort::create(),
            OutputPort::create(),
            func,
            vec![],
            vec![SortColumnDescription {
                offset: 0,
                asc: false,
                nulls_first: false,
                is_nullable: false,
            }],
            bounds,
        )
    }

    fn get_transform_window(
        _unit: WindowFuncFrameUnits,
        bounds: (FrameBound<u64>, FrameBound<u64>),
        arg_type: DataType,
    ) -> Result<TransformWindow<u64>> {
        let agg = AggregateFunctionFactory::instance().get("sum", vec![], vec![arg_type])?;
        let func = WindowFunctionInfo::Aggregate(agg, vec![0]);
        TransformWindow::try_create_rows(
            InputPort::create(),
            OutputPort::create(),
            func,
            vec![0],
            vec![],
            bounds,
        )
    }

    fn get_transform_window_with_data(
        unit: WindowFuncFrameUnits,
        bounds: (FrameBound<u64>, FrameBound<u64>),
        column: Column,
    ) -> Result<TransformWindow<u64>> {
        let data_type = column.data_type();
        let num_rows = column.len();
        let mut transform = get_transform_window(unit, bounds, data_type.clone())?;
        transform.blocks.push_back(WindowBlock {
            block: DataBlock::new_from_columns(vec![column]),
            builder: ColumnBuilder::with_capacity(&data_type, num_rows),
        });
        Ok(transform)
    }

    #[test]
    fn test_partition_advance() -> Result<()> {
        {
            let mut transform = get_transform_window_with_data(
                WindowFuncFrameUnits::Rows,
                (FrameBound::CurrentRow, FrameBound::CurrentRow),
                Int32Type::from_data(vec![1, 1, 1]),
            )?;

            transform.advance_partition();

            assert!(!transform.partition_ended);
            assert_eq!(transform.partition_end, RowPtr::new(1, 0));
        }

        {
            let mut transform = get_transform_window_with_data(
                WindowFuncFrameUnits::Rows,
                (FrameBound::CurrentRow, FrameBound::CurrentRow),
                Int32Type::from_data(vec![1, 1, 2]),
            )?;

            transform.advance_partition();

            assert!(transform.partition_ended);
            assert_eq!(transform.partition_end, RowPtr::new(0, 2));
        }
        Ok(())
    }

    #[test]
    fn test_frame_advance() -> Result<()> {
        {
            let mut transform = get_transform_window_with_data(
                WindowFuncFrameUnits::Rows,
                (
                    FrameBound::Following(Some(4)),
                    FrameBound::Following(Some(5)),
                ),
                Int32Type::from_data(vec![1, 1, 1]),
            )?;

            transform.advance_partition();

            assert!(!transform.partition_ended);
            assert_eq!(transform.partition_end, RowPtr::new(1, 0));

            transform.advance_frame_start();
            assert!(!transform.frame_started)
        }

        {
            let mut transform = get_transform_window_with_data(
                WindowFuncFrameUnits::Rows,
                (
                    FrameBound::Preceding(Some(2)),
                    FrameBound::Following(Some(5)),
                ),
                Int32Type::from_data(vec![1, 1, 1]),
            )?;

            transform.advance_partition();
            transform.current_row = RowPtr::new(0, 1);

            assert!(!transform.partition_ended);
            assert_eq!(transform.partition_end, RowPtr::new(1, 0));

            transform.advance_frame_start();
            assert!(transform.frame_started);
            assert_eq!(transform.frame_start, RowPtr::new(0, 0));

            transform.advance_frame_end();
            assert!(!transform.frame_ended);
        }

        {
            let mut transform = get_transform_window_with_data(
                WindowFuncFrameUnits::Rows,
                (
                    FrameBound::Preceding(Some(2)),
                    FrameBound::Following(Some(1)),
                ),
                Int32Type::from_data(vec![1, 1, 1]),
            )?;

            transform.advance_partition();
            transform.current_row = RowPtr::new(0, 1);
            transform.prev_frame_start = RowPtr::new(0, 0);
            transform.prev_frame_end = RowPtr::new(0, 2);

            assert!(!transform.partition_ended);
            assert_eq!(transform.partition_end, RowPtr::new(1, 0));

            transform.advance_frame_start();
            assert!(transform.frame_started);
            assert_eq!(transform.frame_start, RowPtr::new(0, 0));

            transform.advance_frame_end();
            assert!(transform.frame_ended);
            assert_eq!(transform.frame_end, RowPtr::new(1, 0));
        }

        {
            let mut transform = get_transform_window_with_data(
                WindowFuncFrameUnits::Rows,
                (FrameBound::Preceding(None), FrameBound::Following(None)),
                Int32Type::from_data(vec![1, 1, 1, 2]),
            )?;

            transform.advance_partition();
            transform.current_row = RowPtr::new(0, 1);
            transform.prev_frame_start = RowPtr::new(0, 0);
            transform.prev_frame_end = RowPtr::new(0, 3);

            assert!(transform.partition_ended);
            assert_eq!(transform.partition_end, RowPtr::new(0, 3));

            transform.advance_frame_start();
            assert!(transform.frame_started);
            assert_eq!(transform.frame_start, RowPtr::new(0, 0));

            transform.advance_frame_end();
            assert!(transform.frame_ended);
            assert_eq!(transform.frame_end, RowPtr::new(0, 3));
        }

        Ok(())
    }

    #[test]
    fn test_block_release() -> Result<()> {
        // ranking function release early
        {
            let mut transform = get_ranking_transform_window((
                FrameBound::Preceding(None),
                FrameBound::CurrentRow,
            ))?;

            // peer for 3 cross three blocks
            transform.add_block(Some(DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![1, 1, 1, 2, 2, 3, 3, 3]),
            ])))?;

            transform.check_outputs();
            assert_eq!(transform.blocks.len(), 1);

            transform.add_block(Some(DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![3, 3, 3]),
            ])))?;

            transform.check_outputs();
            assert_eq!(transform.blocks.len(), 1);

            transform.add_block(Some(DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![3, 4, 4]),
            ])))?;

            transform.check_outputs();
            assert_eq!(transform.blocks.len(), 1);

            let output = transform.outputs.pop_front().unwrap();

            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 1        | 1        |",
                    "| 1        | 1        |",
                    "| 1        | 1        |",
                    "| 2        | 2        |",
                    "| 2        | 2        |",
                    "| 3        | 3        |",
                    "| 3        | 3        |",
                    "| 3        | 3        |",
                    "+----------+----------+",
                ],
                &[output],
            );

            let output = transform.outputs.pop_front().unwrap();

            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 3        | 3        |",
                    "| 3        | 3        |",
                    "| 3        | 3        |",
                    "+----------+----------+",
                ],
                &[output],
            );

            transform.input_is_finished = true;
            transform.add_block(None)?;
            transform.check_outputs();
            let output = transform.outputs.pop_front().unwrap();

            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 3        | 3        |",
                    "| 4        | 4        |",
                    "| 4        | 4        |",
                    "+----------+----------+",
                ],
                &[output],
            );
        }

        // ranking function release early
        {
            let mut transform = get_ranking_transform_window((
                FrameBound::Preceding(None),
                FrameBound::CurrentRow,
            ))?;

            // peer not cross any blocks
            transform.add_block(Some(DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![1, 1, 1]),
            ])))?;

            transform.check_outputs();
            assert_eq!(transform.blocks.len(), 1);

            transform.add_block(Some(DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![2, 2, 2]),
            ])))?;

            transform.check_outputs();
            assert_eq!(transform.blocks.len(), 1);

            transform.add_block(Some(DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![3, 3, 3]),
            ])))?;

            transform.check_outputs();
            assert_eq!(transform.blocks.len(), 1);

            let output = transform.outputs.pop_front().unwrap();

            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 1        | 1        |",
                    "| 1        | 1        |",
                    "| 1        | 1        |",
                    "+----------+----------+",
                ],
                &[output],
            );

            let output = transform.outputs.pop_front().unwrap();

            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 2        | 2        |",
                    "| 2        | 2        |",
                    "| 2        | 2        |",
                    "+----------+----------+",
                ],
                &[output],
            );

            transform.input_is_finished = true;
            transform.add_block(None)?;
            transform.check_outputs();
            let output = transform.outputs.pop_front().unwrap();

            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 3        | 3        |",
                    "| 3        | 3        |",
                    "| 3        | 3        |",
                    "+----------+----------+",
                ],
                &[output],
            );
        }

        // normal agg function
        {
            let mut transform = get_transform_window_without_partition(
                WindowFuncFrameUnits::Rows,
                (FrameBound::Preceding(None), FrameBound::CurrentRow),
                DataType::Number(NumberDataType::Int32),
            )?;

            transform.add_block(Some(DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![1, 1, 1, 2, 2, 3, 3, 3]),
            ])))?;

            transform.check_outputs();
            assert_eq!(transform.blocks.len(), 1);

            transform.add_block(Some(DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![3, 4, 4]),
            ])))?;

            transform.check_outputs();
            assert_eq!(transform.blocks.len(), 2);

            let output = transform.outputs.pop_front().unwrap();

            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 1        | 1        |",
                    "| 1        | 2        |",
                    "| 1        | 3        |",
                    "| 2        | 5        |",
                    "| 2        | 7        |",
                    "| 3        | 10       |",
                    "| 3        | 13       |",
                    "| 3        | 16       |",
                    "+----------+----------+",
                ],
                &[output],
            );

            transform.input_is_finished = true;

            transform.add_block(None)?;

            transform.check_outputs();

            let output = transform.outputs.pop_front().unwrap();

            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 3        | 19       |",
                    "| 4        | 23       |",
                    "| 4        | 27       |",
                    "+----------+----------+",
                ],
                &[output],
            );
        }

        Ok(())
    }

    #[test]
    fn test_add_block() -> Result<()> {
        {
            let mut transform = get_transform_window(
                WindowFuncFrameUnits::Rows,
                (FrameBound::Preceding(None), FrameBound::Following(None)),
                DataType::Number(NumberDataType::Int32),
            )?;

            transform.add_block(Some(DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![1, 1, 1]),
            ])))?;

            transform.input_is_finished = true;

            transform.add_block(None)?;

            transform.check_outputs();

            let output = transform.outputs.pop_front().unwrap();

            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 1        | 3        |",
                    "| 1        | 3        |",
                    "| 1        | 3        |",
                    "+----------+----------+",
                ],
                &[output],
            );
        }

        {
            let mut transform = get_transform_window(
                WindowFuncFrameUnits::Rows,
                (FrameBound::Preceding(None), FrameBound::Following(None)),
                DataType::Number(NumberDataType::Int32),
            )?;

            transform.add_block(Some(DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![1, 1, 1, 2, 2, 3, 3, 3]),
            ])))?;

            transform.add_block(Some(DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![3, 4, 4]),
            ])))?;

            transform.check_outputs();

            let output = transform.outputs.pop_front().unwrap();

            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 1        | 3        |",
                    "| 1        | 3        |",
                    "| 1        | 3        |",
                    "| 2        | 4        |",
                    "| 2        | 4        |",
                    "| 3        | 12       |",
                    "| 3        | 12       |",
                    "| 3        | 12       |",
                    "+----------+----------+",
                ],
                &[output],
            );

            transform.input_is_finished = true;

            transform.add_block(None)?;

            transform.check_outputs();

            let output = transform.outputs.pop_front().unwrap();

            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 3        | 12       |",
                    "| 4        | 8        |",
                    "| 4        | 8        |",
                    "+----------+----------+",
                ],
                &[output],
            );
        }

        {
            let mut transform = get_transform_window(
                WindowFuncFrameUnits::Rows,
                (FrameBound::Preceding(None), FrameBound::Following(None)),
                DataType::Number(NumberDataType::Int32),
            )?;

            transform.add_block(Some(DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![1, 1, 1, 2, 2, 3, 3, 3]),
            ])))?;

            transform.add_block(Some(DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![5, 4, 4]),
            ])))?;

            transform.check_outputs();

            let output = transform.outputs.pop_front().unwrap();

            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 1        | 3        |",
                    "| 1        | 3        |",
                    "| 1        | 3        |",
                    "| 2        | 4        |",
                    "| 2        | 4        |",
                    "| 3        | 9        |",
                    "| 3        | 9        |",
                    "| 3        | 9        |",
                    "+----------+----------+",
                ],
                &[output],
            );

            transform.input_is_finished = true;

            transform.add_block(None)?;

            transform.check_outputs();

            let output = transform.outputs.pop_front().unwrap();

            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 5        | 5        |",
                    "| 4        | 8        |",
                    "| 4        | 8        |",
                    "+----------+----------+",
                ],
                &[output],
            );
        }

        {
            let mut transform = get_transform_window(
                WindowFuncFrameUnits::Rows,
                (FrameBound::Preceding(None), FrameBound::Following(None)),
                DataType::Number(NumberDataType::Int32),
            )?;

            transform.add_block(Some(DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![1, 1, 1, 2, 2, 3, 3, 3]),
            ])))?;

            transform.add_block(Some(DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![3, 4, 4]),
            ])))?;

            transform.check_outputs();

            let output = transform.outputs.pop_front().unwrap();

            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 1        | 3        |",
                    "| 1        | 3        |",
                    "| 1        | 3        |",
                    "| 2        | 4        |",
                    "| 2        | 4        |",
                    "| 3        | 12       |",
                    "| 3        | 12       |",
                    "| 3        | 12       |",
                    "+----------+----------+",
                ],
                &[output],
            );

            transform.input_is_finished = true;

            transform.add_block(None)?;

            transform.check_outputs();

            let output = transform.outputs.pop_front().unwrap();

            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 3        | 12       |",
                    "| 4        | 8        |",
                    "| 4        | 8        |",
                    "+----------+----------+",
                ],
                &[output],
            );
        }

        {
            let mut transform = get_transform_window(
                WindowFuncFrameUnits::Rows,
                (FrameBound::Preceding(None), FrameBound::Following(Some(1))),
                DataType::Number(NumberDataType::Int32),
            )?;

            transform.add_block(Some(DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![1, 1, 1, 2, 2, 3, 3, 3]),
            ])))?;

            transform.add_block(Some(DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![3, 4, 4]),
            ])))?;

            transform.check_outputs();

            let output = transform.outputs.pop_front().unwrap();

            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 1        | 2        |",
                    "| 1        | 3        |",
                    "| 1        | 3        |",
                    "| 2        | 4        |",
                    "| 2        | 4        |",
                    "| 3        | 6        |",
                    "| 3        | 9        |",
                    "| 3        | 12       |",
                    "+----------+----------+",
                ],
                &[output],
            );

            transform.input_is_finished = true;

            transform.add_block(None)?;

            transform.check_outputs();

            let output = transform.outputs.pop_front().unwrap();

            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 3        | 12       |",
                    "| 4        | 8        |",
                    "| 4        | 8        |",
                    "+----------+----------+",
                ],
                &[output],
            );
        }

        {
            let mut transform = get_transform_window(
                WindowFuncFrameUnits::Rows,
                (
                    FrameBound::Preceding(Some(1)),
                    FrameBound::Following(Some(1)),
                ),
                DataType::Number(NumberDataType::Int32),
            )?;

            transform.add_block(Some(DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![1, 1, 1, 2, 2, 3, 3, 3]),
            ])))?;

            transform.add_block(Some(DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![3, 4, 4]),
            ])))?;

            transform.check_outputs();

            let output = transform.outputs.pop_front().unwrap();

            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 1        | 2        |",
                    "| 1        | 3        |",
                    "| 1        | 2        |",
                    "| 2        | 4        |",
                    "| 2        | 4        |",
                    "| 3        | 6        |",
                    "| 3        | 9        |",
                    "| 3        | 9        |",
                    "+----------+----------+",
                ],
                &[output],
            );

            transform.input_is_finished = true;

            transform.add_block(None)?;

            transform.check_outputs();

            let output = transform.outputs.pop_front().unwrap();

            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 3        | 6        |",
                    "| 4        | 8        |",
                    "| 4        | 8        |",
                    "+----------+----------+",
                ],
                &[output],
            );
        }

        {
            let mut transform = get_transform_window(
                WindowFuncFrameUnits::Rows,
                (FrameBound::Preceding(None), FrameBound::CurrentRow),
                DataType::Number(NumberDataType::Int32),
            )?;

            transform.add_block(Some(DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![1, 1, 1, 2, 2, 3, 3, 3]),
            ])))?;

            transform.add_block(Some(DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![3, 4, 4]),
            ])))?;

            transform.check_outputs();

            let output = transform.outputs.pop_front().unwrap();

            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 1        | 1        |",
                    "| 1        | 2        |",
                    "| 1        | 3        |",
                    "| 2        | 2        |",
                    "| 2        | 4        |",
                    "| 3        | 3        |",
                    "| 3        | 6        |",
                    "| 3        | 9        |",
                    "+----------+----------+",
                ],
                &[output],
            );

            transform.input_is_finished = true;

            transform.add_block(None)?;

            transform.check_outputs();

            let output = transform.outputs.pop_front().unwrap();

            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 3        | 12       |",
                    "| 4        | 4        |",
                    "| 4        | 8        |",
                    "+----------+----------+",
                ],
                &[output],
            );
        }

        {
            let mut transform = get_transform_window(
                WindowFuncFrameUnits::Rows,
                (FrameBound::Preceding(Some(1)), FrameBound::CurrentRow),
                DataType::Number(NumberDataType::Int32),
            )?;

            transform.add_block(Some(DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![1, 1, 1, 2, 2, 3, 3, 3]),
            ])))?;

            transform.add_block(Some(DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![3, 4, 4]),
            ])))?;

            transform.check_outputs();

            let output = transform.outputs.pop_front().unwrap();

            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 1        | 1        |",
                    "| 1        | 2        |",
                    "| 1        | 2        |",
                    "| 2        | 2        |",
                    "| 2        | 4        |",
                    "| 3        | 3        |",
                    "| 3        | 6        |",
                    "| 3        | 6        |",
                    "+----------+----------+",
                ],
                &[output],
            );

            transform.input_is_finished = true;

            transform.add_block(None)?;

            transform.check_outputs();

            let output = transform.outputs.pop_front().unwrap();

            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 3        | 6        |",
                    "| 4        | 4        |",
                    "| 4        | 8        |",
                    "+----------+----------+",
                ],
                &[output],
            );
        }

        Ok(())
    }

    #[allow(clippy::type_complexity)]
    fn get_transform_window_and_ports(
        _unit: WindowFuncFrameUnits,
        bounds: (FrameBound<u64>, FrameBound<u64>),
    ) -> Result<(Box<dyn Processor>, Arc<InputPort>, Arc<OutputPort>)> {
        let agg = AggregateFunctionFactory::instance()
            .get("sum", vec![], vec![DataType::Number(NumberDataType::Int32)])?;
        let func = WindowFunctionInfo::Aggregate(agg, vec![0]);
        let input = InputPort::create();
        let output = OutputPort::create();
        let transform = TransformWindow::try_create_rows(
            input.clone(),
            output.clone(),
            func,
            vec![0],
            vec![],
            bounds,
        )?;

        Ok((Box::new(transform), input, output))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_transform_window() -> Result<()> {
        {
            let upstream_output = OutputPort::create();
            let downstream_input = InputPort::create();
            let (mut transform, input, output) = get_transform_window_and_ports(
                WindowFuncFrameUnits::Rows,
                (
                    FrameBound::Preceding(Some(1)),
                    FrameBound::Following(Some(1)),
                ),
            )?;

            unsafe {
                connect(&input, &upstream_output);
                connect(&downstream_input, &output);
            }

            downstream_input.set_need_data();

            assert!(matches!(transform.event()?, Event::NeedData));
            upstream_output.push_data(Ok(DataBlock::new_from_columns(vec![Int32Type::from_data(
                vec![1, 1, 1, 2, 2, 3, 3, 3],
            )])));

            assert!(matches!(transform.event()?, Event::Sync));
            transform.process()?;

            assert!(matches!(transform.event()?, Event::NeedData));
            upstream_output.push_data(Ok(DataBlock::new_from_columns(vec![Int32Type::from_data(
                vec![3, 4, 4],
            )])));
            upstream_output.finish();

            assert!(matches!(transform.event()?, Event::Sync));
            transform.process()?;

            assert!(matches!(transform.event()?, Event::NeedConsume));
            let block = downstream_input.pull_data().unwrap()?;
            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 1        | 2        |",
                    "| 1        | 3        |",
                    "| 1        | 2        |",
                    "| 2        | 4        |",
                    "| 2        | 4        |",
                    "| 3        | 6        |",
                    "| 3        | 9        |",
                    "| 3        | 9        |",
                    "+----------+----------+",
                ],
                &[block],
            );
            downstream_input.set_need_data();

            assert!(matches!(transform.event()?, Event::Sync));
            transform.process()?;

            assert!(matches!(transform.event()?, Event::NeedConsume));
            let block = downstream_input.pull_data().unwrap()?;
            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 3        | 6        |",
                    "| 4        | 8        |",
                    "| 4        | 8        |",
                    "+----------+----------+",
                ],
                &[block],
            );
            downstream_input.set_need_data();

            assert!(matches!(transform.event()?, Event::Finished));
        }

        {
            let upstream_output = OutputPort::create();
            let downstream_input = InputPort::create();
            let (mut transform, input, output) = get_transform_window_and_ports(
                WindowFuncFrameUnits::Rows,
                (FrameBound::Preceding(None), FrameBound::Following(None)),
            )?;

            unsafe {
                connect(&input, &upstream_output);
                connect(&downstream_input, &output);
            }

            downstream_input.set_need_data();

            assert!(matches!(transform.event()?, Event::NeedData));
            upstream_output.push_data(Ok(DataBlock::new_from_columns(vec![Int32Type::from_data(
                vec![1, 1, 1],
            )])));

            assert!(matches!(transform.event()?, Event::Sync));
            transform.process()?;

            assert!(matches!(transform.event()?, Event::NeedData));
            upstream_output.push_data(Ok(DataBlock::new_from_columns(vec![Int32Type::from_data(
                vec![1, 1, 1],
            )])));

            assert!(matches!(transform.event()?, Event::Sync));
            transform.process()?;

            assert!(matches!(transform.event()?, Event::NeedData));
            upstream_output.push_data(Ok(DataBlock::new_from_columns(vec![Int32Type::from_data(
                vec![1, 1, 1],
            )])));
            upstream_output.finish();

            assert!(matches!(transform.event()?, Event::Sync));
            transform.process()?;

            assert!(matches!(transform.event()?, Event::Sync));
            transform.process()?;

            assert!(matches!(transform.event()?, Event::NeedConsume));
            let block = downstream_input.pull_data().unwrap()?;
            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 1        | 9        |",
                    "| 1        | 9        |",
                    "| 1        | 9        |",
                    "+----------+----------+",
                ],
                &[block],
            );
            downstream_input.set_need_data();

            assert!(matches!(transform.event()?, Event::NeedConsume));
            let block = downstream_input.pull_data().unwrap()?;
            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 1        | 9        |",
                    "| 1        | 9        |",
                    "| 1        | 9        |",
                    "+----------+----------+",
                ],
                &[block],
            );
            downstream_input.set_need_data();

            assert!(matches!(transform.event()?, Event::NeedConsume));
            let block = downstream_input.pull_data().unwrap()?;
            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 1        | 9        |",
                    "| 1        | 9        |",
                    "| 1        | 9        |",
                    "+----------+----------+",
                ],
                &[block],
            );
            downstream_input.set_need_data();

            assert!(matches!(transform.event()?, Event::Finished));
        }

        {
            let upstream_output = OutputPort::create();
            let downstream_input = InputPort::create();
            let (mut transform, input, output) = get_transform_window_and_ports(
                WindowFuncFrameUnits::Rows,
                (
                    FrameBound::Preceding(Some(3)),
                    FrameBound::Preceding(Some(1)),
                ),
            )?;

            unsafe {
                connect(&input, &upstream_output);
                connect(&downstream_input, &output);
            }

            downstream_input.set_need_data();

            assert!(matches!(transform.event()?, Event::NeedData));
            upstream_output.push_data(Ok(DataBlock::new_from_columns(vec![Int32Type::from_data(
                vec![1, 1, 1],
            )])));

            assert!(matches!(transform.event()?, Event::Sync));
            transform.process()?;

            assert!(matches!(transform.event()?, Event::NeedConsume));
            let block = downstream_input.pull_data().unwrap()?;
            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 1        | NULL     |",
                    "| 1        | 1        |",
                    "| 1        | 2        |",
                    "+----------+----------+",
                ],
                &[block],
            );
            downstream_input.set_need_data();

            assert!(matches!(transform.event()?, Event::NeedData));
            upstream_output.push_data(Ok(DataBlock::new_from_columns(vec![Int32Type::from_data(
                vec![1, 1, 1],
            )])));

            assert!(matches!(transform.event()?, Event::Sync));
            transform.process()?;

            assert!(matches!(transform.event()?, Event::NeedConsume));
            let block = downstream_input.pull_data().unwrap()?;
            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 1        | 3        |",
                    "| 1        | 3        |",
                    "| 1        | 3        |",
                    "+----------+----------+",
                ],
                &[block],
            );
            downstream_input.set_need_data();

            assert!(matches!(transform.event()?, Event::NeedData));
            upstream_output.push_data(Ok(DataBlock::new_from_columns(vec![Int32Type::from_data(
                vec![1, 1, 1],
            )])));
            upstream_output.finish();

            assert!(matches!(transform.event()?, Event::Sync));
            transform.process()?;

            assert!(matches!(transform.event()?, Event::NeedConsume));
            let block = downstream_input.pull_data().unwrap()?;
            assert_blocks_eq(
                vec![
                    "+----------+----------+",
                    "| Column 0 | Column 1 |",
                    "+----------+----------+",
                    "| 1        | 3        |",
                    "| 1        | 3        |",
                    "| 1        | 3        |",
                    "+----------+----------+",
                ],
                &[block],
            );
            downstream_input.set_need_data();

            assert!(matches!(transform.event()?, Event::Finished));
        }

        Ok(())
    }
}
