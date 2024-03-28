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
use std::vec;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::Scalar;
use databend_common_functions::aggregates::AggregateCountFunction;

use crate::binder::wrap_cast;
use crate::binder::ColumnBindingBuilder;
use crate::binder::Visibility;
use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::plans::Aggregate;
use crate::plans::AggregateFunction;
use crate::plans::AggregateMode;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::ComparisonOp;
use crate::plans::ConstantExpr;
use crate::plans::EvalScalar;
use crate::plans::Filter;
use crate::plans::FunctionCall;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::Limit;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::SubqueryExpr;
use crate::plans::SubqueryType;
use crate::plans::UDFCall;
use crate::plans::UDFLambdaCall;
use crate::plans::WindowFuncType;
use crate::IndexType;
use crate::MetadataRef;

#[allow(clippy::enum_variant_names)]
pub enum UnnestResult {
    // Semi/Anti Join, Cross join for EXISTS
    SimpleJoin,
    MarkJoin { marker_index: IndexType },
    SingleJoin { output_index: Option<IndexType> },
}

pub struct FlattenInfo {
    pub from_count_func: bool,
}

/// Rewrite subquery into `Apply` operator
pub struct SubqueryRewriter {
    pub(crate) ctx: Arc<dyn TableContext>,
    pub(crate) metadata: MetadataRef,
    pub(crate) derived_columns: HashMap<IndexType, IndexType>,
}

impl SubqueryRewriter {
    pub fn new(ctx: Arc<dyn TableContext>, metadata: MetadataRef) -> Self {
        Self {
            ctx,
            metadata,
            derived_columns: Default::default(),
        }
    }

    pub fn rewrite(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        match s_expr.plan().clone() {
            RelOperator::EvalScalar(mut plan) => {
                let mut input = self.rewrite(s_expr.child(0)?)?;

                for item in plan.items.iter_mut() {
                    let res = self.try_rewrite_subquery(&item.scalar, &input, false)?;
                    input = res.1;
                    item.scalar = res.0;
                }

                Ok(SExpr::create_unary(Arc::new(plan.into()), Arc::new(input)))
            }
            RelOperator::Filter(mut plan) => {
                let mut input = self.rewrite(s_expr.child(0)?)?;
                for pred in plan.predicates.iter_mut() {
                    // set `is_conjunctive_predicate` as false, cause need `marker` column output
                    let res = self.try_rewrite_subquery(pred, &input, false)?;
                    input = res.1;
                    *pred = res.0;
                }

                Ok(SExpr::create_unary(Arc::new(plan.into()), Arc::new(input)))
            }
            RelOperator::ProjectSet(mut plan) => {
                let mut input = self.rewrite(s_expr.child(0)?)?;
                for item in plan.srfs.iter_mut() {
                    let res = self.try_rewrite_subquery(&item.scalar, &input, false)?;
                    input = res.1;
                    item.scalar = res.0
                }

                Ok(SExpr::create_unary(Arc::new(plan.into()), Arc::new(input)))
            }
            RelOperator::Aggregate(mut plan) => {
                let mut input = self.rewrite(s_expr.child(0)?)?;

                for item in plan.group_items.iter_mut() {
                    let res = self.try_rewrite_subquery(&item.scalar, &input, false)?;
                    input = res.1;
                    item.scalar = res.0;
                }

                for item in plan.aggregate_functions.iter_mut() {
                    let res = self.try_rewrite_subquery(&item.scalar, &input, false)?;
                    input = res.1;
                    item.scalar = res.0;
                }

                Ok(SExpr::create_unary(Arc::new(plan.into()), Arc::new(input)))
            }

            RelOperator::Window(mut plan) => {
                let mut input = self.rewrite(s_expr.child(0)?)?;

                for item in plan.partition_by.iter_mut() {
                    let res = self.try_rewrite_subquery(&item.scalar, &input, false)?;
                    input = res.1;
                    item.scalar = res.0;
                }

                for item in plan.order_by.iter_mut() {
                    let res =
                        self.try_rewrite_subquery(&item.order_by_item.scalar, &input, false)?;
                    input = res.1;
                    item.order_by_item.scalar = res.0;
                }

                if let WindowFuncType::Aggregate(agg) = &mut plan.function {
                    for item in agg.args.iter_mut() {
                        let res = self.try_rewrite_subquery(item, &input, false)?;
                        input = res.1;
                        *item = res.0;
                    }
                }

                Ok(SExpr::create_unary(Arc::new(plan.into()), Arc::new(input)))
            }

            RelOperator::Join(_) | RelOperator::UnionAll(_) | RelOperator::MaterializedCte(_) => {
                Ok(SExpr::create_binary(
                    Arc::new(s_expr.plan().clone()),
                    Arc::new(self.rewrite(s_expr.child(0)?)?),
                    Arc::new(self.rewrite(s_expr.child(1)?)?),
                ))
            }

            RelOperator::Limit(_) | RelOperator::Sort(_) => Ok(SExpr::create_unary(
                Arc::new(s_expr.plan().clone()),
                Arc::new(self.rewrite(s_expr.child(0)?)?),
            )),

            RelOperator::DummyTableScan(_)
            | RelOperator::Scan(_)
            | RelOperator::CteScan(_)
            | RelOperator::ConstantTableScan(_) => Ok(s_expr.clone()),

            _ => Err(ErrorCode::Internal("Invalid plan type")),
        }
    }

    /// Try to extract subquery from a scalar expression. Returns replaced scalar expression
    /// and the subqueries.
    fn try_rewrite_subquery(
        &mut self,
        scalar: &ScalarExpr,
        s_expr: &SExpr,
        is_conjunctive_predicate: bool,
    ) -> Result<(ScalarExpr, SExpr)> {
        match scalar {
            ScalarExpr::BoundColumnRef(_) => Ok((scalar.clone(), s_expr.clone())),
            ScalarExpr::ConstantExpr(_) => Ok((scalar.clone(), s_expr.clone())),
            ScalarExpr::WindowFunction(_) => Ok((scalar.clone(), s_expr.clone())),
            ScalarExpr::AggregateFunction(_) => Ok((scalar.clone(), s_expr.clone())),
            ScalarExpr::LambdaFunction(_) => Ok((scalar.clone(), s_expr.clone())),
            ScalarExpr::FunctionCall(func) => {
                let mut args = vec![];
                let mut s_expr = s_expr.clone();
                for arg in func.arguments.iter() {
                    let res = self.try_rewrite_subquery(arg, &s_expr, false)?;
                    s_expr = res.1;
                    args.push(res.0);
                }

                let expr: ScalarExpr = FunctionCall {
                    span: func.span,
                    params: func.params.clone(),
                    arguments: args,
                    func_name: func.func_name.clone(),
                }
                .into();

                Ok((expr, s_expr))
            }
            ScalarExpr::CastExpr(cast) => {
                let (scalar, s_expr) = self.try_rewrite_subquery(&cast.argument, s_expr, false)?;
                Ok((
                    CastExpr {
                        span: cast.span,
                        is_try: cast.is_try,
                        argument: Box::new(scalar),
                        target_type: cast.target_type.clone(),
                    }
                    .into(),
                    s_expr,
                ))
            }
            ScalarExpr::SubqueryExpr(subquery) => {
                // Rewrite subquery recursively
                let mut subquery = subquery.clone();
                subquery.subquery = Box::new(self.rewrite(&subquery.subquery)?);

                // Check if the subquery is a correlated subquery.
                // If it is, we'll try to flatten it and rewrite to join.
                // If it is not, we'll just rewrite it to join
                let rel_expr = RelExpr::with_s_expr(&subquery.subquery);
                let prop = rel_expr.derive_relational_prop()?;
                let mut flatten_info = FlattenInfo {
                    from_count_func: false,
                };
                let (s_expr, result) = if prop.outer_columns.is_empty() {
                    self.try_rewrite_uncorrelated_subquery(s_expr, &subquery)?
                } else {
                    self.try_decorrelate_subquery(
                        s_expr,
                        &subquery,
                        &mut flatten_info,
                        is_conjunctive_predicate,
                    )?
                };

                // If we unnest the subquery into a simple join, then we can replace the
                // original predicate with a `TRUE` literal to eliminate the conjunction.
                if matches!(result, UnnestResult::SimpleJoin) {
                    return Ok((
                        ScalarExpr::ConstantExpr(ConstantExpr {
                            span: subquery.span,
                            value: Scalar::Boolean(true),
                        }),
                        s_expr,
                    ));
                }
                let (index, name) = if let UnnestResult::MarkJoin { marker_index } = result {
                    (marker_index, marker_index.to_string())
                } else if let UnnestResult::SingleJoin { output_index } = result {
                    if let Some(output_idx) = output_index {
                        // uncorrelated scalar subquery
                        (output_idx, "_if_scalar_subquery".to_string())
                    } else {
                        let mut output_column = subquery.output_column;
                        if let Some(index) = self.derived_columns.get(&output_column.index) {
                            output_column.index = *index;
                        }
                        (
                            output_column.index,
                            format!("scalar_subquery_{:?}", output_column.index),
                        )
                    }
                } else {
                    let index = subquery.output_column.index;
                    (index, format!("subquery_{}", index))
                };

                let data_type = if subquery.typ == SubqueryType::Scalar {
                    Box::new(subquery.data_type.wrap_nullable())
                } else if matches! {result, UnnestResult::MarkJoin {..}} {
                    Box::new(DataType::Nullable(Box::new(DataType::Boolean)))
                } else {
                    subquery.data_type.clone()
                };

                let column_ref = ScalarExpr::BoundColumnRef(BoundColumnRef {
                    span: subquery.span,
                    column: ColumnBindingBuilder::new(name, index, data_type, Visibility::Visible)
                        .build(),
                });

                let scalar = if flatten_info.from_count_func && subquery.typ == SubqueryType::Scalar
                {
                    // convert count aggregate function to `if(count() is not null, count(), 0)`
                    let is_not_null = ScalarExpr::FunctionCall(FunctionCall {
                        span: subquery.span,
                        func_name: "is_not_null".to_string(),
                        params: vec![],
                        arguments: vec![column_ref.clone()],
                    });
                    let cast_column_ref_to_uint64 = ScalarExpr::CastExpr(CastExpr {
                        span: subquery.span,
                        is_try: true,
                        argument: Box::new(column_ref),
                        target_type: Box::new(
                            DataType::Number(NumberDataType::UInt64).wrap_nullable(),
                        ),
                    });
                    let zero = ScalarExpr::ConstantExpr(ConstantExpr {
                        span: subquery.span,
                        value: Scalar::Number(NumberScalar::UInt8(0)),
                    });
                    ScalarExpr::CastExpr(CastExpr {
                        span: subquery.span,
                        is_try: true,
                        argument: Box::new(ScalarExpr::FunctionCall(FunctionCall {
                            span: subquery.span,
                            func_name: "if".to_string(),
                            params: vec![],
                            arguments: vec![is_not_null, cast_column_ref_to_uint64, zero],
                        })),
                        target_type: Box::new(
                            DataType::Number(NumberDataType::UInt64).wrap_nullable(),
                        ),
                    })
                } else if subquery.typ == SubqueryType::NotExists {
                    ScalarExpr::FunctionCall(FunctionCall {
                        span: subquery.span,
                        func_name: "not".to_string(),
                        params: vec![],
                        arguments: vec![column_ref],
                    })
                } else {
                    column_ref
                };
                // After finishing rewriting subquery, we should clear the derived columns.
                self.derived_columns.clear();
                Ok((scalar, s_expr))
            }
            ScalarExpr::UDFCall(udf) => {
                let mut args = vec![];
                let mut s_expr = s_expr.clone();
                for arg in udf.arguments.iter() {
                    let res = self.try_rewrite_subquery(arg, &s_expr, false)?;
                    s_expr = res.1;
                    args.push(res.0);
                }

                let expr: ScalarExpr = UDFCall {
                    span: udf.span,
                    name: udf.name.clone(),
                    func_name: udf.func_name.clone(),
                    display_name: udf.display_name.clone(),
                    udf_type: udf.udf_type.clone(),
                    arg_types: udf.arg_types.clone(),
                    return_type: udf.return_type.clone(),
                    arguments: args,
                }
                .into();

                Ok((expr, s_expr))
            }

            ScalarExpr::UDFLambdaCall(udf) => {
                let mut s_expr = s_expr.clone();
                let res = self.try_rewrite_subquery(&udf.scalar, &s_expr, false)?;
                s_expr = res.1;

                let expr: ScalarExpr = UDFLambdaCall {
                    span: udf.span,
                    func_name: udf.func_name.clone(),
                    scalar: Box::new(res.0),
                }
                .into();

                Ok((expr, s_expr))
            }
        }
    }

    fn try_rewrite_uncorrelated_subquery(
        &mut self,
        left: &SExpr,
        subquery: &SubqueryExpr,
    ) -> Result<(SExpr, UnnestResult)> {
        match subquery.typ {
            SubqueryType::Scalar => self.rewrite_uncorrelated_scalar_subquery(left, subquery),
            SubqueryType::Exists | SubqueryType::NotExists => {
                let mut subquery_expr = *subquery.subquery.clone();
                // Wrap Limit to current subquery
                let limit = Limit {
                    limit: Some(1),
                    offset: 0,
                    before_exchange: false,
                };
                subquery_expr =
                    SExpr::create_unary(Arc::new(limit.into()), Arc::new(subquery_expr.clone()));

                // We will rewrite EXISTS subquery into the form `COUNT(*) = 1`.
                // For example, `EXISTS(SELECT a FROM t WHERE a > 1)` will be rewritten into
                // `(SELECT COUNT(*) = 1 FROM t WHERE a > 1 LIMIT 1)`.
                let agg_func = AggregateCountFunction::try_create("", vec![], vec![])?;
                let agg_func_index = self
                    .metadata
                    .write()
                    .add_derived_column("count(*)".to_string(), agg_func.return_type()?);

                let agg = Aggregate {
                    group_items: vec![],
                    aggregate_functions: vec![ScalarItem {
                        scalar: AggregateFunction {
                            display_name: "count(*)".to_string(),
                            func_name: "count".to_string(),
                            distinct: false,
                            params: vec![],
                            args: vec![],
                            return_type: Box::new(agg_func.return_type()?),
                        }
                        .into(),
                        index: agg_func_index,
                    }],
                    from_distinct: false,
                    mode: AggregateMode::Initial,
                    limit: None,
                    grouping_sets: None,
                };

                let compare = FunctionCall {
                    span: subquery.span,
                    func_name: if subquery.typ == SubqueryType::Exists {
                        "eq".to_string()
                    } else {
                        "noteq".to_string()
                    },
                    params: vec![],
                    arguments: vec![
                        BoundColumnRef {
                            span: subquery.span,
                            column: ColumnBindingBuilder::new(
                                "count(*)".to_string(),
                                agg_func_index,
                                Box::new(agg_func.return_type()?),
                                Visibility::Visible,
                            )
                            .build(),
                        }
                        .into(),
                        ConstantExpr {
                            span: subquery.span,
                            value: Scalar::Number(NumberScalar::UInt64(1)),
                        }
                        .into(),
                    ],
                };
                let filter = Filter {
                    predicates: vec![compare.into()],
                };

                // Filter: COUNT(*) = 1 or COUNT(*) != 1
                //     Aggregate: COUNT(*)
                let rewritten_subquery = SExpr::create_unary(
                    Arc::new(filter.into()),
                    Arc::new(SExpr::create_unary(
                        Arc::new(agg.into()),
                        Arc::new(subquery_expr),
                    )),
                );
                let cross_join = Join {
                    left_conditions: vec![],
                    right_conditions: vec![],
                    non_equi_conditions: vec![],
                    join_type: JoinType::Cross,
                    marker_index: None,
                    from_correlated_subquery: false,
                    need_hold_hash_table: false,
                    is_lateral: false,
                    single_to_inner: None,
                }
                .into();
                Ok((
                    SExpr::create_binary(
                        Arc::new(cross_join),
                        Arc::new(left.clone()),
                        Arc::new(rewritten_subquery),
                    ),
                    UnnestResult::SimpleJoin,
                ))
            }
            SubqueryType::Any => {
                let output_column = subquery.output_column.clone();
                let column_name = format!("subquery_{}", output_column.index);
                let left_condition = wrap_cast(
                    &ScalarExpr::BoundColumnRef(BoundColumnRef {
                        span: subquery.span,
                        column: ColumnBindingBuilder::new(
                            column_name,
                            output_column.index,
                            output_column.data_type,
                            Visibility::Visible,
                        )
                        .table_index(output_column.table_index)
                        .build(),
                    }),
                    &subquery.data_type,
                );
                let child_expr = *subquery.child_expr.as_ref().unwrap().clone();
                let op = *subquery.compare_op.as_ref().unwrap();
                let (right_condition, is_non_equi_condition) =
                    check_child_expr_in_subquery(&child_expr, &op)?;
                let (left_conditions, right_conditions, non_equi_conditions) =
                    if !is_non_equi_condition {
                        (vec![left_condition], vec![right_condition], vec![])
                    } else {
                        let other_condition = ScalarExpr::FunctionCall(FunctionCall {
                            span: subquery.span,
                            func_name: op.to_func_name().to_string(),
                            params: vec![],
                            arguments: vec![right_condition, left_condition],
                        });
                        (vec![], vec![], vec![other_condition])
                    };
                // Add a marker column to save comparison result.
                // The column is Nullable(Boolean), the data value is TRUE, FALSE, or NULL.
                // If subquery contains NULL, the comparison result is TRUE or NULL.
                // Such as t1.a => {1, 3, 4}, select t1.a in (1, 2, NULL) from t1; The sql will return {true, null, null}.
                // If subquery doesn't contain NULL, the comparison result is FALSE, TRUE, or NULL.
                let marker_index = if let Some(idx) = subquery.projection_index {
                    idx
                } else {
                    self.metadata.write().add_derived_column(
                        "marker".to_string(),
                        DataType::Nullable(Box::new(DataType::Boolean)),
                    )
                };
                // Consider the sql: select * from t1 where t1.a = any(select t2.a from t2);
                // Will be transferred to:select t1.a, t2.a, marker_index from t1, t2 where t2.a = t1.a;
                // Note that subquery is the right table, and it'll be the build side.
                let mark_join = Join {
                    left_conditions: right_conditions,
                    right_conditions: left_conditions,
                    non_equi_conditions,
                    join_type: JoinType::RightMark,
                    marker_index: Some(marker_index),
                    from_correlated_subquery: false,
                    need_hold_hash_table: false,
                    is_lateral: false,
                    single_to_inner: None,
                }
                .into();
                let s_expr = SExpr::create_binary(
                    Arc::new(mark_join),
                    Arc::new(left.clone()),
                    Arc::new(*subquery.subquery.clone()),
                );
                Ok((s_expr, UnnestResult::MarkJoin { marker_index }))
            }
            _ => unreachable!(),
        }
    }

    fn rewrite_uncorrelated_scalar_subquery(
        &mut self,
        left: &SExpr,
        subquery: &SubqueryExpr,
    ) -> Result<(SExpr, UnnestResult)> {
        // Use cross join which brings chance to push down filter under cross join.
        // Such as `SELECT * FROM c WHERE c_id=(SELECT max(c_id) FROM o WHERE ship='WA');`
        // We can push down `c_id = max(c_id)` to cross join then make it as inner join.
        let join_plan = Join {
            left_conditions: vec![],
            right_conditions: vec![],
            non_equi_conditions: vec![],
            join_type: JoinType::Cross,
            marker_index: None,
            from_correlated_subquery: false,
            need_hold_hash_table: false,
            is_lateral: false,
            single_to_inner: None,
        }
        .into();

        // For some cases, empty result set will be occur, we should return null instead of empty set.
        // So let wrap an expression: `if(count()=0, null, any(subquery.output_column)`
        let count_func = ScalarExpr::AggregateFunction(AggregateFunction {
            func_name: "count".to_string(),
            distinct: false,
            params: vec![],
            args: vec![ScalarExpr::BoundColumnRef(BoundColumnRef {
                span: None,
                column: subquery.output_column.clone(),
            })],
            return_type: Box::new(DataType::Number(NumberDataType::UInt64)),
            display_name: "count".to_string(),
        });
        let any_func = ScalarExpr::AggregateFunction(AggregateFunction {
            func_name: "any".to_string(),
            distinct: false,
            params: vec![],
            return_type: subquery.output_column.data_type.clone(),
            args: vec![ScalarExpr::BoundColumnRef(BoundColumnRef {
                span: None,
                column: subquery.output_column.clone(),
            })],
            display_name: "any".to_string(),
        });
        // Add `count_func` and `any_func` to metadata
        let count_idx = self.metadata.write().add_derived_column(
            "_count_scalar_subquery".to_string(),
            DataType::Number(NumberDataType::UInt64),
        );
        let any_idx = self.metadata.write().add_derived_column(
            "_any_scalar_subquery".to_string(),
            *subquery.output_column.data_type.clone(),
        );
        // Aggregate operator
        let agg = SExpr::create_unary(
            Arc::new(
                Aggregate {
                    mode: AggregateMode::Initial,
                    group_items: vec![],
                    aggregate_functions: vec![
                        ScalarItem {
                            scalar: count_func,
                            index: count_idx,
                        },
                        ScalarItem {
                            scalar: any_func,
                            index: any_idx,
                        },
                    ],
                    from_distinct: false,
                    limit: None,
                    grouping_sets: None,
                }
                .into(),
            ),
            Arc::new(*subquery.subquery.clone()),
        );

        let limit = SExpr::create_unary(
            Arc::new(
                Limit {
                    limit: Some(1),
                    offset: 0,
                    before_exchange: false,
                }
                .into(),
            ),
            Arc::new(agg),
        );

        // Wrap expression
        let count_col_ref = ScalarExpr::BoundColumnRef(BoundColumnRef {
            span: None,
            column: ColumnBindingBuilder::new(
                "_count_scalar_subquery".to_string(),
                count_idx,
                Box::new(DataType::Number(NumberDataType::UInt64)),
                Visibility::Visible,
            )
            .build(),
        });
        let any_col_ref = ScalarExpr::BoundColumnRef(BoundColumnRef {
            span: None,
            column: ColumnBindingBuilder::new(
                "_any_scalar_subquery".to_string(),
                any_idx,
                subquery.output_column.data_type.clone(),
                Visibility::Visible,
            )
            .build(),
        });
        let eq_func = ScalarExpr::FunctionCall(FunctionCall {
            span: None,
            func_name: "eq".to_string(),
            params: vec![],
            arguments: vec![
                count_col_ref,
                ScalarExpr::ConstantExpr(ConstantExpr {
                    span: None,
                    value: Scalar::Number(NumberScalar::UInt8(0)),
                }),
            ],
        });
        // If function
        let if_func = ScalarExpr::FunctionCall(FunctionCall {
            span: None,
            func_name: "if".to_string(),
            params: vec![],
            arguments: vec![
                eq_func,
                ScalarExpr::ConstantExpr(ConstantExpr {
                    span: None,
                    value: Scalar::Null,
                }),
                any_col_ref,
            ],
        });
        let if_func_idx = self.metadata.write().add_derived_column(
            "_if_scalar_subquery".to_string(),
            *subquery.output_column.data_type.clone(),
        );
        let scalar_expr = SExpr::create_unary(
            Arc::new(
                EvalScalar {
                    items: vec![ScalarItem {
                        scalar: if_func,
                        index: if_func_idx,
                    }],
                }
                .into(),
            ),
            Arc::new(limit),
        );

        let s_expr = SExpr::create_binary(
            Arc::new(join_plan),
            Arc::new(left.clone()),
            Arc::new(scalar_expr),
        );
        Ok((s_expr, UnnestResult::SingleJoin {
            output_index: Some(if_func_idx),
        }))
    }
}

pub fn check_child_expr_in_subquery(
    child_expr: &ScalarExpr,
    op: &ComparisonOp,
) -> Result<(ScalarExpr, bool)> {
    match child_expr {
        ScalarExpr::BoundColumnRef(_) => Ok((child_expr.clone(), op != &ComparisonOp::Equal)),
        ScalarExpr::FunctionCall(func) => {
            if func.func_name.eq("tuple") {
                return Ok((child_expr.clone(), op != &ComparisonOp::Equal));
            }
            Err(ErrorCode::Internal(format!(
                "Invalid child expr in subquery: {:?}",
                child_expr
            )))
        }
        ScalarExpr::ConstantExpr(_) => Ok((child_expr.clone(), true)),
        ScalarExpr::CastExpr(cast) => {
            let arg = &cast.argument;
            let (_, is_non_equi_condition) = check_child_expr_in_subquery(arg, op)?;
            Ok((child_expr.clone(), is_non_equi_condition))
        }
        other => Err(ErrorCode::Internal(format!(
            "Invalid child expr in subquery: {:?}",
            other
        ))),
    }
}
