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

use std::sync::Arc;

use common_catalog::plan::AggIndexMeta;
use common_exception::Result;
use common_expression::types::nullable::NullableColumnBuilder;
use common_expression::types::BooleanType;
use common_expression::types::DataType;
use common_expression::types::VariantType;
use common_expression::BlockEntry;
use common_expression::BlockMetaInfoDowncast;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::FieldIndex;
use common_expression::FunctionContext;
use common_expression::ScalarRef;
use common_expression::Value;
use common_functions::BUILTIN_FUNCTIONS;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::Processor;
use common_pipeline_transforms::processors::transforms::Transform;
use common_pipeline_transforms::processors::transforms::Transformer;

use crate::optimizer::ColumnSet;

/// `BlockOperator` takes a `DataBlock` as input and produces a `DataBlock` as output.
#[derive(Clone)]
pub enum BlockOperator {
    /// Batch mode of map which merges map operators into one.
    Map {
        projections: ColumnSet,
        exprs: Vec<Expr>,
    },

    MapWithOutput {
        exprs: Vec<Expr>,
        /// The index of the output columns, based on the exprs.
        projections: ColumnSet,
    },

    /// Filter the input [`DataBlock`] with the predicate `eval`.
    Filter { projections: ColumnSet, expr: Expr },

    /// Reorganize the input [`DataBlock`] with `projection`.
    Project { projection: Vec<FieldIndex> },

    /// Expand the input [`DataBlock`] with set-returning functions.
    FlatMap {
        projections: ColumnSet,
        srf_exprs: Vec<Expr>,
    },
}

impl BlockOperator {
    pub fn execute(&self, func_ctx: &FunctionContext, mut input: DataBlock) -> Result<DataBlock> {
        if input.is_empty() {
            return Ok(DataBlock::empty());
        }
        match self {
            BlockOperator::Map { .. }
            | BlockOperator::MapWithOutput { .. }
            | BlockOperator::Filter { .. }
                if input
                    .get_meta()
                    .and_then(AggIndexMeta::downcast_ref_from)
                    .is_some() =>
            {
                // It's from aggregating index.
                Ok(input)
            }
            BlockOperator::Map { projections, exprs } => {
                for expr in exprs {
                    let evaluator = Evaluator::new(&input, func_ctx, &BUILTIN_FUNCTIONS);
                    let result = evaluator.run(expr)?;
                    let col = BlockEntry::new(expr.data_type().clone(), result);
                    input.add_column(col);
                }
                Ok(input.project(projections))
            }

            BlockOperator::MapWithOutput { exprs, projections } => {
                for expr in exprs {
                    let evaluator = Evaluator::new(&input, func_ctx, &BUILTIN_FUNCTIONS);
                    let result = evaluator.run(expr)?;
                    let col = BlockEntry::new(expr.data_type().clone(), result);
                    input.add_column(col);
                }
                Ok(input.project(projections))
            }

            BlockOperator::Filter { projections, expr } => {
                assert_eq!(expr.data_type(), &DataType::Boolean);

                let evaluator = Evaluator::new(&input, func_ctx, &BUILTIN_FUNCTIONS);
                let filter = evaluator.run(expr)?.try_downcast::<BooleanType>().unwrap();
                let column_nums = input.num_columns();
                let mut columns = Vec::with_capacity(projections.len());
                for index in 0..column_nums {
                    if !projections.contains(&index) {
                        continue;
                    }
                    columns.push(input.get_by_offset(index).clone());
                }
                let data_block = DataBlock::new(columns, input.num_rows());
                data_block.filter_boolean_value(&filter)
            }

            BlockOperator::Project { projection } => {
                let mut result = DataBlock::new(vec![], input.num_rows());
                for index in projection {
                    result.add_column(input.get_by_offset(*index).clone());
                }
                Ok(result)
            }

            BlockOperator::FlatMap {
                projections,
                srf_exprs,
            } => {
                let eval = Evaluator::new(&input, func_ctx, &BUILTIN_FUNCTIONS);

                // [
                //   srf1: [
                //     result_set1: [
                //       col1, col2, ...
                //     ],
                //     ...
                //   ],
                //   ...
                // ]
                let input_num_rows = input.num_rows();
                let mut max_nums_per_row = vec![0; input_num_rows];
                let srf_results = srf_exprs
                    .iter()
                    .map(|srf_expr| eval.run_srf(srf_expr, &mut max_nums_per_row))
                    .collect::<Result<Vec<_>>>()?;
                let mut total_num_rows = 0;
                for max_nums in max_nums_per_row.iter().take(input_num_rows) {
                    total_num_rows += *max_nums;
                }

                let input_num_columns = input.num_columns();
                let mut result = DataBlock::empty();
                let mut block_is_empty = true;
                for index in 0..input_num_columns {
                    if !projections.contains(&index) {
                        continue;
                    }
                    let column = input.get_by_offset(index);
                    let mut builder =
                        ColumnBuilder::with_capacity(&column.data_type, total_num_rows);
                    for (i, max_nums) in max_nums_per_row.iter().take(input_num_rows).enumerate() {
                        let scalar_ref = unsafe { column.value.index_unchecked(i) };
                        for _ in 0..*max_nums {
                            builder.push(scalar_ref.clone());
                        }
                    }
                    let block_entry =
                        BlockEntry::new(column.data_type.clone(), Value::Column(builder.build()));
                    if block_is_empty {
                        result = DataBlock::new(vec![block_entry], total_num_rows);
                        block_is_empty = false;
                    } else {
                        result.add_column(block_entry);
                    }
                }

                for (srf_expr, srf_results) in srf_exprs.iter().zip(srf_results) {
                    if let Expr::FunctionCall { function, .. } = srf_expr {
                        match function.signature.name.as_str() {
                            "json_path_query" => {
                                let mut builder: NullableColumnBuilder<VariantType> =
                                    NullableColumnBuilder::with_capacity(total_num_rows, &[]);
                                for (i, (row_result, repeat_times)) in
                                    srf_results.into_iter().enumerate()
                                {
                                    if let Value::Column(Column::Tuple(fields)) = row_result {
                                        debug_assert!(fields.len() == 1);
                                        match &fields[0] {
                                            Column::Nullable(box nullable_column) => {
                                                match &nullable_column.column {
                                                    Column::Variant(string_column) => {
                                                        for idx in 0..repeat_times {
                                                            builder.push(unsafe {
                                                                string_column.index_unchecked(idx)
                                                            });
                                                        }
                                                        for _ in
                                                            0..(max_nums_per_row[i] - repeat_times)
                                                        {
                                                            builder.push_null();
                                                        }
                                                    }
                                                    _ => unreachable!(
                                                        "json_path_query's return type is: `DataType::Tuple(vec![DataType::Nullable(Box::new(DataType::Variant))])`"
                                                    ),
                                                }
                                            }
                                            _ => unreachable!(
                                                "json_path_query's return type is: `DataType::Tuple(vec![DataType::Nullable(Box::new(DataType::Variant))])`"
                                            ),
                                        };
                                    }
                                }
                                let column = builder.build().upcast();
                                let block_entry = BlockEntry::new(
                                    DataType::Tuple(vec![DataType::Nullable(Box::new(
                                        DataType::Variant,
                                    ))]),
                                    Value::Column(Column::Tuple(vec![Column::Nullable(Box::new(
                                        column,
                                    ))])),
                                );
                                if block_is_empty {
                                    result = DataBlock::new(vec![block_entry], total_num_rows);
                                    block_is_empty = false;
                                } else {
                                    result.add_column(block_entry);
                                }
                            }
                            _ => {
                                let mut result_data_blocks = Vec::with_capacity(input.num_rows());
                                for (i, (mut row_result, repeat_times)) in
                                    srf_results.into_iter().enumerate()
                                {
                                    if let Value::Column(Column::Tuple(fields)) = &mut row_result {
                                        // If the current result set has less rows than the max number of rows,
                                        // we need to pad the result set with null values.
                                        // TODO(leiysky): this can be optimized by using a `zip` array function
                                        if repeat_times < max_nums_per_row[i] {
                                            for field in fields {
                                                match field {
                                                    Column::Null { .. } => {
                                                        *field = ColumnBuilder::repeat(
                                                            &ScalarRef::Null,
                                                            max_nums_per_row[i],
                                                            &DataType::Null,
                                                        )
                                                        .build();
                                                    }
                                                    Column::Nullable(box nullable_column) => {
                                                        let mut column_builder =
                                                            NullableColumnBuilder::from_column(
                                                                (*nullable_column).clone(),
                                                            );
                                                        (0..(max_nums_per_row[i] - repeat_times))
                                                            .for_each(|_| {
                                                                column_builder.push_null();
                                                            });
                                                        *field = Column::Nullable(Box::new(
                                                            column_builder.build(),
                                                        ));
                                                    }
                                                    _ => unreachable!(),
                                                }
                                            }
                                        }
                                    } else {
                                        row_result = Value::Column(
                                            ColumnBuilder::repeat(
                                                &ScalarRef::Tuple(vec![ScalarRef::Null]),
                                                max_nums_per_row[i],
                                                srf_expr.data_type(),
                                            )
                                            .build(),
                                        );
                                    }

                                    let block_entry =
                                        BlockEntry::new(srf_expr.data_type().clone(), row_result);
                                    result_data_blocks.push(DataBlock::new(
                                        vec![block_entry],
                                        max_nums_per_row[i],
                                    ))
                                }
                                let data_block = DataBlock::concat(&result_data_blocks)?;
                                debug_assert!(data_block.num_rows() == total_num_rows);
                                let block_entry = BlockEntry::new(
                                    data_block.get_by_offset(0).data_type.clone(),
                                    data_block.get_by_offset(0).value.clone(),
                                );
                                if block_is_empty {
                                    result = DataBlock::new(vec![block_entry], total_num_rows);
                                    block_is_empty = false;
                                } else {
                                    result.add_column(block_entry);
                                }
                            }
                        }
                    } else {
                        unreachable!("expr is not a set returning function: {srf_expr}");
                    }
                }
                Ok(result)
            }
        }
    }
}

/// `CompoundBlockOperator` is a pipeline of `BlockOperator`s
pub struct CompoundBlockOperator {
    pub operators: Vec<BlockOperator>,
    pub ctx: FunctionContext,
}

impl CompoundBlockOperator {
    pub fn new(
        operators: Vec<BlockOperator>,
        ctx: FunctionContext,
        input_num_columns: usize,
    ) -> Self {
        let operators = Self::compact_map(operators, input_num_columns);
        Self { operators, ctx }
    }

    pub fn create(
        input_port: Arc<InputPort>,
        output_port: Arc<OutputPort>,
        input_num_columns: usize,
        ctx: FunctionContext,
        operators: Vec<BlockOperator>,
    ) -> Box<dyn Processor> {
        let operators = Self::compact_map(operators, input_num_columns);
        Transformer::<Self>::create(input_port, output_port, Self { operators, ctx })
    }

    pub fn compact_map(
        operators: Vec<BlockOperator>,
        input_num_columns: usize,
    ) -> Vec<BlockOperator> {
        let mut results = Vec::with_capacity(operators.len());

        for op in operators {
            match op {
                BlockOperator::Map { projections, exprs } => {
                    if let Some(BlockOperator::Map {
                        projections: pre_projections,
                        exprs: pre_exprs,
                    }) = results.last_mut()
                    {
                        pre_projections.extend(projections);
                        pre_exprs.extend(exprs);
                    } else {
                        results.push(BlockOperator::Map { projections, exprs });
                    }
                }
                _ => results.push(op),
            }
        }

        crate::evaluator::cse::apply_cse(results, input_num_columns)
    }
}

impl Transform for CompoundBlockOperator {
    const NAME: &'static str = "CompoundBlockOperator";

    const SKIP_EMPTY_DATA_BLOCK: bool = true;

    fn transform(&mut self, data_block: DataBlock) -> Result<DataBlock> {
        self.operators
            .iter()
            .try_fold(data_block, |input, op| op.execute(&self.ctx, input))
    }

    fn name(&self) -> String {
        format!(
            "{}({})",
            Self::NAME,
            self.operators
                .iter()
                .map(|op| {
                    match op {
                        BlockOperator::Map { .. } => "Map",
                        BlockOperator::MapWithOutput { .. } => "MapWithOutput",
                        BlockOperator::Filter { .. } => "Filter",
                        BlockOperator::Project { .. } => "Project",
                        BlockOperator::FlatMap { .. } => "FlatMap",
                    }
                    .to_string()
                })
                .collect::<Vec<String>>()
                .join("->")
        )
    }
}
