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

use databend_common_exception::Result;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Value;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::executor::cast_expr_to_non_null_boolean;

use crate::operations::expr2prdicate;
use crate::operations::get_and;
use crate::operations::get_not;

pub struct DeleteByExprMutator {
    expr: Option<Expr>,
    row_id_idx: usize,
    func_ctx: FunctionContext,
    origin_input_columns: usize,
    // if use target_build_optimization, we don't need to give row ids to `matched mutator`
    target_build_optimization: bool,
}

impl DeleteByExprMutator {
    pub fn create(
        expr: Option<Expr>,
        func_ctx: FunctionContext,
        row_id_idx: usize,
        origin_input_columns: usize,
        target_build_optimization: bool,
    ) -> Self {
        Self {
            expr,
            row_id_idx,
            func_ctx,
            origin_input_columns,
            target_build_optimization,
        }
    }

    pub fn delete_by_expr(&self, data_block: DataBlock) -> Result<(DataBlock, DataBlock)> {
        // it's the first delete, after delete, we need to add a filter column
        if data_block.num_columns() == self.origin_input_columns {
            self.delete_block(data_block, false)
        } else {
            self.delete_block(data_block, true)
        }
    }

    fn get_filter(
        &self,
        data_block: &DataBlock,
    ) -> Result<(Value<BooleanType>, Value<BooleanType>)> {
        // filter rows that's is not processed
        let filter_entry = data_block.get_by_offset(data_block.num_columns() - 1);
        let old_filter: Value<BooleanType> = filter_entry.value.try_downcast().unwrap();
        // false means this row is not processed, so use `not` to reverse it.
        let (filter_not, _) = get_not(old_filter.clone(), &self.func_ctx, data_block.num_rows())?;
        let filter_not = filter_not.try_downcast().unwrap();
        Ok((old_filter, filter_not))
    }

    pub(crate) fn get_row_id_block(&self, block: DataBlock) -> DataBlock {
        if self.target_build_optimization {
            DataBlock::empty()
        } else {
            DataBlock::new(
                vec![block.get_by_offset(self.row_id_idx).clone()],
                block.num_rows(),
            )
        }
    }

    fn get_result_block(
        &self,
        predicate: &Value<BooleanType>,
        predicate_not: &Value<BooleanType>, // the rows which can be processed at this time.
        data_block: DataBlock,
    ) -> Result<(DataBlock, DataBlock)> {
        let res_block = data_block.clone().filter_boolean_value(predicate)?;
        let filtered_block = data_block.filter_boolean_value(predicate_not)?;
        Ok((res_block, self.get_row_id_block(filtered_block)))
    }

    // return block after delete, and the rowIds which are deleted
    pub fn delete_block(
        &self,
        data_block: DataBlock,
        has_filter: bool,
    ) -> Result<(DataBlock, DataBlock)> {
        if self.expr.is_none() {
            if has_filter {
                let (old_predicate, filter_not) = self.get_filter(&data_block)?;
                self.get_result_block(&old_predicate, &filter_not, data_block)
            } else {
                // delete all
                Ok((DataBlock::empty(), self.get_row_id_block(data_block)))
            }
        } else {
            let filter: Expr = cast_expr_to_non_null_boolean(self.expr.as_ref().unwrap().clone())?;
            assert_eq!(filter.data_type(), &DataType::Boolean);

            let evaluator = Evaluator::new(&data_block, &self.func_ctx, &BUILTIN_FUNCTIONS);

            let predicates = expr2prdicate(&evaluator, &filter)?;

            // for delete, we don't need to add new filter to the result block,
            // just filter it.
            if has_filter {
                let (_, filter_not) = self.get_filter(&data_block)?;
                let (res, _) = get_and(
                    filter_not,
                    predicates,
                    &self.func_ctx,
                    data_block.num_rows(),
                )?;
                // the rows can be processed by this time
                let res: Value<BooleanType> = res.try_downcast().unwrap();
                let (res_not, _) = get_not(res.clone(), &self.func_ctx, data_block.num_rows())?;

                self.get_result_block(&res_not.try_downcast().unwrap(), &res, data_block)
            } else {
                let filtered_block = data_block.clone().filter_boolean_value(&predicates)?;
                let (predicates_not, _) =
                    get_not(predicates, &self.func_ctx, data_block.num_rows())?;

                // we need to add filter at the end of result block
                let mut res_block = data_block
                    .clone()
                    .filter_boolean_value(&predicates_not.try_downcast().unwrap())?;

                let const_expr = Expr::Constant {
                    span: None,
                    scalar: databend_common_expression::Scalar::Boolean(false),
                    data_type: DataType::Boolean,
                };

                let const_predicates = expr2prdicate(&evaluator, &const_expr)?;

                res_block.add_column(BlockEntry::new(
                    DataType::Boolean,
                    Value::upcast(const_predicates),
                ));

                Ok((res_block, self.get_row_id_block(filtered_block)))
            }
        }
    }
}
