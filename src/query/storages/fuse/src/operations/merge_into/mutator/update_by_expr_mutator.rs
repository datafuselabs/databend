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

use databend_common_exception::Result;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FieldIndex;
use databend_common_expression::FunctionContext;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Value;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::executor::cast_expr_to_non_null_boolean;

use crate::operations::expr2prdicate;
use crate::operations::get_and;
use crate::operations::get_not;
use crate::operations::get_or;

#[derive(Clone)]
pub struct UpdateByExprMutator {
    expr: Option<Expr>,
    func_ctx: FunctionContext,
    field_index_of_input_schema: HashMap<FieldIndex, usize>,
    origin_input_columns: usize,

    update_lists: Vec<(FieldIndex, RemoteExpr)>,
}

impl UpdateByExprMutator {
    #[allow(dead_code)]
    pub fn create(
        expr: Option<Expr>,
        func_ctx: FunctionContext,
        field_index_of_input_schema: HashMap<FieldIndex, usize>,
        update_lists: Vec<(FieldIndex, RemoteExpr)>,
        origin_input_columns: usize,
    ) -> Self {
        Self {
            expr,
            func_ctx,
            field_index_of_input_schema,
            origin_input_columns,
            update_lists,
        }
    }

    pub fn update_by_expr(&self, data_block: DataBlock) -> Result<DataBlock> {
        let const_expr = Expr::Constant {
            span: None,
            scalar: databend_common_expression::Scalar::Boolean(true),
            data_type: DataType::Boolean,
        };
        let mut expr = if self.expr.is_none() {
            const_expr
        } else {
            self.expr.clone().unwrap()
        };
        expr = cast_expr_to_non_null_boolean(expr)?;
        assert_eq!(expr.data_type(), &DataType::Boolean);

        // it's the first update, after update, we need to add a filter column
        if data_block.num_columns() == self.origin_input_columns {
            assert_eq!(expr.data_type(), &DataType::Boolean);
            self.update_block(data_block, expr, false)
        } else {
            self.update_block(data_block, expr, true)
        }
    }

    fn update_block(
        &self,
        data_block: DataBlock,
        expr: Expr,
        has_filter: bool,
    ) -> Result<DataBlock> {
        let evaluator = Evaluator::new(&data_block, &self.func_ctx, &BUILTIN_FUNCTIONS);
        let mut predicates = expr2prdicate(&evaluator, &expr)?;

        let mut data_block = data_block.clone();
        let (last_filter, origin_block) = if has_filter {
            let filter_entry = data_block.get_by_offset(data_block.num_columns() - 1);
            let old_filter: Value<BooleanType> = filter_entry.value.try_downcast().unwrap();
            // pop filter
            data_block.pop_columns(1);
            // has pop old filter
            let origin_block = data_block.clone();
            // add filter
            let (old_filter_not, _) =
                get_not(old_filter.clone(), &self.func_ctx, data_block.num_rows())?;

            let old_filter_not: Value<BooleanType> = old_filter_not.try_downcast().unwrap();

            let (res, _) = get_and(
                old_filter_not,
                predicates,
                &self.func_ctx,
                data_block.num_rows(),
            )?;

            predicates = res.try_downcast().unwrap();

            data_block.add_column(BlockEntry::new(
                DataType::Boolean,
                Value::upcast(predicates.clone()),
            ));
            let (last_filter, _) = get_or(
                old_filter,
                predicates,
                &self.func_ctx,
                data_block.num_rows(),
            )?;

            (last_filter, origin_block)
        } else {
            let origin_block = data_block.clone();
            data_block.add_column(BlockEntry::new(
                DataType::Boolean,
                Value::upcast(predicates.clone()),
            ));
            (Value::upcast(predicates), origin_block)
        };

        let exprs: Vec<Expr> = self
            .update_lists
            .iter()
            .map(|item| item.1.as_expr(&BUILTIN_FUNCTIONS))
            .collect();

        // get filter and updated columns
        let op = BlockOperator::Map {
            exprs,
            projections: Some(
                (self.origin_input_columns
                    ..self.origin_input_columns + 1 + self.update_lists.len())
                    .collect(),
            ),
        };
        let data_block = op.execute(&self.func_ctx, data_block)?;
        // input_schema column position -> field_index
        let mut updated_column_position = HashMap::new();
        // field_index -> position_in_block (this block is arrived by op.execute above)
        let mut field_index2position = HashMap::new();
        for (idx, (field_index, _)) in self.update_lists.iter().enumerate() {
            updated_column_position.insert(
                self.field_index_of_input_schema.get(field_index).unwrap(),
                field_index,
            );
            // there is a filter column in data_block
            field_index2position.insert(field_index, idx + 1);
        }
        let mut block_entries = Vec::with_capacity(self.origin_input_columns + 1);
        for (idx, block_entry) in origin_block.columns().iter().enumerate() {
            if updated_column_position.contains_key(&idx) {
                let pos = field_index2position
                    .get(updated_column_position.get(&idx).unwrap())
                    .unwrap();
                block_entries.push(data_block.get_by_offset(*pos).clone());
            } else {
                block_entries.push(block_entry.clone());
            }
        }
        // add filter
        block_entries.push(BlockEntry {
            data_type: DataType::Boolean,
            value: last_filter,
        });

        Ok(DataBlock::new(block_entries, data_block.num_rows()))
    }
}
