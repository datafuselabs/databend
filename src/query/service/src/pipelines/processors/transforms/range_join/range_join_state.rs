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

use std::sync::atomic;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::bitmap::MutableBitmap;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::FunctionContext;
use databend_common_expression::RemoteExpr;
use databend_common_expression::ScalarRef;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::executor::physical_plans::RangeJoin;
use databend_common_sql::executor::physical_plans::RangeJoinCondition;
use databend_common_sql::executor::physical_plans::RangeJoinType;
use databend_common_sql::plans::JoinType;
use parking_lot::Mutex;
use parking_lot::RwLock;

use crate::pipelines::executor::WatchNotify;
use crate::pipelines::processors::transforms::range_join::IEJoinState;
use crate::pipelines::processors::transforms::wrap_true_validity;
use crate::sessions::QueryContext;

pub struct RangeJoinState {
    pub(crate) ctx: Arc<QueryContext>,
    // The origin data for left/right table
    pub(crate) left_table: RwLock<Vec<DataBlock>>,
    pub(crate) right_table: RwLock<Vec<DataBlock>>,
    pub(crate) right_sorted_blocks: RwLock<Vec<DataBlock>>,
    // For iejoin, it's L1: sort by the first join key
    pub(crate) left_sorted_blocks: RwLock<Vec<DataBlock>>,
    pub(crate) conditions: Vec<RangeJoinCondition>,
    // pub(crate) join_type: JoinType,
    pub(crate) other_conditions: Vec<RemoteExpr>,
    // Pipeline event related
    pub(crate) partition_finished: Mutex<bool>,
    pub(crate) finished_notify: Arc<WatchNotify>,
    pub(crate) left_sinker_count: RwLock<usize>,
    pub(crate) right_sinker_count: RwLock<usize>,
    // Task that need to be executed, pair.0 is left table block, pair.1 is right table block
    pub(crate) tasks: RwLock<Vec<(usize, usize)>>,
    // Row index offset for left/right
    pub(crate) row_offset: RwLock<Vec<(usize, usize)>>,
    pub(crate) finished_tasks: AtomicU64,
    pub(crate) completed_pair: AtomicU64,
    // IEJoin state
    pub(crate) ie_join_state: Option<IEJoinState>,
    pub(crate) join_type: JoinType,
    // A bool indicating for tuple in the LHS if they found a match (used in full outer join)
    pub(crate) left_match: RwLock<MutableBitmap>,
    // A bool indicating for tuple in the RHS if they found a match (used in full outer join)
    pub(crate) right_match: RwLock<MutableBitmap>,
    pub(crate) partition_count: AtomicU64,
}

impl RangeJoinState {
    pub fn new(ctx: Arc<QueryContext>, range_join: &RangeJoin) -> Self {
        let ie_join_state = if matches!(range_join.range_join_type, RangeJoinType::IEJoin) {
            Some(IEJoinState::new(range_join))
        } else {
            None
        };
        Self {
            ctx,
            left_table: RwLock::new(vec![]),
            right_table: RwLock::new(vec![]),
            right_sorted_blocks: Default::default(),
            left_sorted_blocks: Default::default(),
            conditions: range_join.conditions.clone(),
            // join_type: range_join.join_type.clone(),
            other_conditions: range_join.other_conditions.clone(),
            partition_finished: Mutex::new(false),
            finished_notify: Arc::new(WatchNotify::new()),
            left_sinker_count: RwLock::new(0),
            right_sinker_count: RwLock::new(0),
            tasks: RwLock::new(vec![]),
            row_offset: RwLock::new(vec![]),
            finished_tasks: AtomicU64::new(0),
            completed_pair: AtomicU64::new(0),
            ie_join_state,
            join_type: range_join.join_type.clone(),
            left_match: RwLock::new(MutableBitmap::new()),
            right_match: RwLock::new(MutableBitmap::new()),
            partition_count: AtomicU64::new(0),
        }
    }

    pub(crate) fn sink_right(&self, block: DataBlock) -> Result<()> {
        // Sink block to right table
        let mut right_table = self.right_table.write();
        let mut right_block = block;
        if matches!(self.join_type, JoinType::Left) {
            let validity = Bitmap::new_constant(true, right_block.num_rows());
            let nullable_right_columns = right_block
                .columns()
                .iter()
                .map(|c| wrap_true_validity(c, right_block.num_rows(), &validity))
                .collect::<Vec<_>>();
            right_block = DataBlock::new(nullable_right_columns, right_block.num_rows());
        }
        right_table.push(right_block);
        Ok(())
    }

    pub(crate) fn sink_left(&self, block: DataBlock) -> Result<()> {
        // Sink block to left table
        let mut left_table = self.left_table.write();
        let mut left_block = block;
        if matches!(self.join_type, JoinType::Right) {
            let validity = Bitmap::new_constant(true, left_block.num_rows());
            let nullable_left_columns = left_block
                .columns()
                .iter()
                .map(|c| wrap_true_validity(c, left_block.num_rows(), &validity))
                .collect::<Vec<_>>();
            left_block = DataBlock::new(nullable_left_columns, left_block.num_rows());
        }
        left_table.push(left_block);
        Ok(())
    }

    pub(crate) fn left_attach(&self) {
        let mut left_sinker_count = self.left_sinker_count.write();
        *left_sinker_count += 1;
    }

    pub(crate) fn left_detach(&self) -> Result<()> {
        let right_sinker_count = self.right_sinker_count.read();
        let mut left_sinker_count = self.left_sinker_count.write();
        *left_sinker_count -= 1;
        if *left_sinker_count == 0 && *right_sinker_count == 0 {
            // Left and right both finish sink
            // Partition left/right table
            self.partition()?;
            // Set partition finished
            let mut partition_finished = self.partition_finished.lock();
            *partition_finished = true;
            self.finished_notify.notify_waiters();
        }
        Ok(())
    }

    pub(crate) fn right_attach(&self) {
        let mut right_sinker_count = self.right_sinker_count.write();
        *right_sinker_count += 1;
    }

    pub fn task_id(&self) -> Option<usize> {
        let task_id = self.finished_tasks.fetch_add(1, atomic::Ordering::SeqCst);
        if task_id >= self.tasks.read().len() as u64 {
            return None;
        }
        Some(task_id as usize)
    }

    pub(crate) fn right_detach(&self) -> Result<()> {
        let mut right_sinker_count = self.right_sinker_count.write();
        *right_sinker_count -= 1;
        let left_sinker_count = self.left_sinker_count.read();
        if *right_sinker_count == 0 && *left_sinker_count == 0 {
            // Left and right both finish sink
            // Partition left/right table
            self.partition()?;
            // Set partition finished
            let mut partition_finished = self.partition_finished.lock();
            *partition_finished = true;
            self.finished_notify.notify_waiters();
        }
        Ok(())
    }

    pub(crate) async fn wait_merge_finish(&self) -> Result<()> {
        let notified = {
            let partition_finished = self.partition_finished.lock();

            match *partition_finished {
                true => None,
                false => Some(self.finished_notify.notified()),
            }
        };

        if let Some(notified) = notified {
            notified.await;
        }
        Ok(())
    }

    pub(crate) fn partition(&self) -> Result<()> {
        let max_threads = self.ctx.get_settings().get_max_threads()? as usize;
        let left_table = self.left_table.read();
        // Right table is bigger than left table
        let mut right_table = self.right_table.write();
        if !left_table.is_empty()
            && !right_table.is_empty()
            && left_table.len() * right_table.len() < max_threads
        {
            let num_parts = max_threads / left_table.len() + 1;
            // Spit right_table to num_parts equally
            let merged_right_table = DataBlock::concat(&right_table)?;
            let mut indices = Vec::with_capacity(merged_right_table.num_rows());
            for idx in 0..merged_right_table.num_rows() {
                indices.push((idx % num_parts) as u32);
            }
            let scatter_blocks = DataBlock::scatter(&merged_right_table, &indices, num_parts)?;
            right_table.clear();
            for block in scatter_blocks.iter() {
                if !block.is_empty() {
                    right_table.push(block.clone());
                }
            }
        }

        let mut left_sorted_blocks = self.left_sorted_blocks.write();
        let mut right_sorted_blocks = self.right_sorted_blocks.write();

        let mut current_rows = 0;
        for left_block in left_table.iter() {
            // Generate keys block by join keys
            // For example, if join keys are [t1.a + t2.b, t1.c], then key blocks will contain two columns: [t1.a + t2.b, t1.c]
            // We can get the key blocks by evaluating the join keys expressions on the block
            let mut columns = Vec::with_capacity(3);
            // Append join keys columns
            for condition in self.conditions.iter() {
                let func_ctx = FunctionContext::default();
                let evaluator = Evaluator::new(left_block, &func_ctx, &BUILTIN_FUNCTIONS);
                let expr = condition.left_expr.as_expr(&BUILTIN_FUNCTIONS);
                let column = evaluator
                    .run(&expr)?
                    .convert_to_full_column(expr.data_type(), left_block.num_rows());
                columns.push(column);
            }
            // Generate idx column from current_rows to current_rows + block.num_rows()
            let mut column_builder = ColumnBuilder::with_capacity(
                &DataType::Number(NumberDataType::Int64),
                left_block.num_rows(),
            );
            for idx in current_rows..(current_rows + left_block.num_rows()) {
                column_builder.push(ScalarRef::Number(NumberScalar::Int64((idx + 1) as i64)));
            }
            columns.push(column_builder.build());
            let keys_block = DataBlock::new_from_columns(columns);
            left_sorted_blocks.push(keys_block);
            current_rows += left_block.num_rows();
        }

        current_rows = 0;
        for right_block in right_table.iter() {
            // Generate keys block by join keys
            // For example, if join keys are [t1.a + t2.b, t1.c], then key blocks will contain two columns: [t1.a + t2.b, t1.c]
            // We can get the key blocks by evaluating the join keys expressions on the block
            let mut columns = Vec::with_capacity(3);
            // Append join keys columns
            for condition in self.conditions.iter() {
                let func_ctx = FunctionContext::default();
                let evaluator = Evaluator::new(right_block, &func_ctx, &BUILTIN_FUNCTIONS);
                let expr = condition.right_expr.as_expr(&BUILTIN_FUNCTIONS);
                let column = evaluator
                    .run(&expr)?
                    .convert_to_full_column(expr.data_type(), right_block.num_rows());
                columns.push(column);
            }
            // Generate idx column from current_rows to current_rows + block.num_rows()
            let mut column_builder = ColumnBuilder::with_capacity(
                &DataType::Number(NumberDataType::Int64),
                right_block.num_rows(),
            );
            for idx in current_rows..(current_rows + right_block.num_rows()) {
                column_builder.push(ScalarRef::Number(NumberScalar::Int64(-(idx as i64 + 1))));
            }
            columns.push(column_builder.build());
            let keys_block = DataBlock::new_from_columns(columns);
            right_sorted_blocks.push(keys_block);
            current_rows += right_block.num_rows();
        }
        // Add tasks
        let mut row_offset = self.row_offset.write();
        let mut left_offset = 0;
        let mut right_offset = 0;
        let mut tasks = self.tasks.write();
        for (left_idx, left_block) in left_sorted_blocks.iter().enumerate() {
            for (right_idx, right_block) in right_sorted_blocks.iter().enumerate() {
                row_offset.push((left_offset, right_offset));
                tasks.push((left_idx, right_idx));
                right_offset += right_block.num_rows();
            }
            right_offset = 0;
            left_offset += left_block.num_rows();
        }
        self.partition_count
            .fetch_add(tasks.len().try_into().unwrap(), atomic::Ordering::SeqCst);
        // Add Fill task
        left_offset = 0;
        right_offset = 0;
        if matches!(self.join_type, JoinType::Left) {
            let mut left_match = self.left_match.write();
            for (left_idx, left_block) in left_sorted_blocks.iter().enumerate() {
                row_offset.push((left_offset, 0));
                tasks.push((left_idx, 0));
                left_offset += left_block.num_rows();
                left_match.extend_constant(left_block.num_rows(), false);
            }
        }
        if matches!(self.join_type, JoinType::Right) {
            let mut right_match = self.right_match.write();
            for (right_idx, right_block) in right_sorted_blocks.iter().enumerate() {
                row_offset.push((0, right_offset));
                tasks.push((0, right_idx));
                right_offset += right_block.num_rows();
                right_match.extend_constant(right_block.num_rows(), false);
            }
        }
        Ok(())
    }
}
