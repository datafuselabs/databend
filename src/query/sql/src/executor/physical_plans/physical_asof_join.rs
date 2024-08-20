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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;

use crate::binder::bind_window_function_info;
use crate::binder::ColumnBindingBuilder;
use crate::binder::WindowFunctionInfo;
use crate::binder::WindowOrderByInfo;
use crate::executor::explain::PlanStatsInfo;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::ColumnSet;
use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::plans::BoundColumnRef;
use crate::plans::ComparisonOp;
use crate::plans::ConstantExpr;
use crate::plans::FunctionCall;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::LagLeadFunction;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::WindowFunc;
use crate::plans::WindowFuncFrame;
use crate::plans::WindowFuncFrameBound;
use crate::plans::WindowFuncFrameUnits;
use crate::plans::WindowFuncType;
use crate::plans::WindowOrderBy;
use crate::ColumnEntry;
use crate::DerivedColumn;
use crate::Visibility;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct AsofJoin {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
    pub non_equi_conditions: Vec<RemoteExpr>,
    // Now only support inner join, will support left/right join later
    pub join_type: JoinType,

    pub output_schema: DataSchemaRef,

    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl AsofJoin {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.output_schema.clone())
    }
}

impl PhysicalPlanBuilder {
    pub async fn build_asof_join(
        &mut self,
        join: &Join,
        s_expr: &SExpr,
        required: (ColumnSet, ColumnSet),
        mut range_conditions: Vec<ScalarExpr>,
        mut other_conditions: Vec<ScalarExpr>,
    ) -> Result<PhysicalPlan> {
        let mut window_index: usize = 0;

        if range_conditions.is_empty() {
            return Err(ErrorCode::Internal("Missing inequality condition!"));
        }
        if range_conditions.len() > 1 {
            return Err(ErrorCode::Internal("Multiple inequalities condition!"));
        }
        let (window_func, right_column, left_type) =
            self.bind_window_func(join, s_expr, &range_conditions, &mut other_conditions)?;
        let window_plan = self.build_window_plan(&window_func, s_expr, &mut window_index)?;
        self.add_range_condition(
            &window_func,
            window_index,
            &mut range_conditions,
            right_column,
            left_type,
        )?;
        let mut ss_expr = s_expr.clone();
        ss_expr.children[0] = Arc::new(window_plan);
        ss_expr.children[1] = Arc::new(s_expr.child(0)?.clone());
        let join_type = match join.join_type {
            JoinType::Asof => Ok(JoinType::Inner),
            JoinType::LeftAsof => Ok(JoinType::Left),
            JoinType::RightAsof => Ok(JoinType::Right),
            _ => Err(ErrorCode::Internal("unsupported join type!")),
        }?;
        let left_prop = RelExpr::with_s_expr(ss_expr.child(0)?).derive_relational_prop()?;
        let right_prop = RelExpr::with_s_expr(ss_expr.child(1)?).derive_relational_prop()?;
        let left_required = required.0.union(&left_prop.used_columns).cloned().collect();
        let right_required = required
            .1
            .union(&right_prop.used_columns)
            .cloned()
            .collect();
        self.build_range_join(
            join_type,
            &ss_expr,
            left_required,
            right_required,
            range_conditions,
            other_conditions,
        )
        .await
    }

    fn add_range_condition(
        &mut self,
        window_func: &WindowFunc,
        window_index: usize,
        range_conditions: &mut Vec<ScalarExpr>,
        right_column: ScalarExpr,
        left_type: DataType,
    ) -> Result<bool> {
        let mut folded_args: Vec<ScalarExpr> = Vec::with_capacity(2);
        let mut func_name = String::from("eq");
        // Generate a ColumnBinding for each argument of aggregates
        let column = ColumnBindingBuilder::new(
            window_func.display_name.clone(),
            window_index,
            Box::new(left_type),
            Visibility::Visible,
        )
        .build();
        folded_args.push(right_column.clone());
        folded_args.push(
            BoundColumnRef {
                span: right_column.span(),
                column,
            }
            .into(),
        );
        for condition in range_conditions.iter() {
            if let ScalarExpr::FunctionCall(func) = condition {
                match ComparisonOp::try_from_func_name(func.func_name.as_str()).unwrap() {
                    ComparisonOp::GTE => {
                        func_name = String::from("lt");
                    }
                    ComparisonOp::GT => {
                        func_name = String::from("lte");
                    }
                    ComparisonOp::LT => {
                        func_name = String::from("gte");
                    }
                    ComparisonOp::LTE => {
                        func_name = String::from("gt");
                    }
                    _ => unreachable!("must be range condition!"),
                }
            }
        }
        range_conditions.push(
            FunctionCall {
                span: range_conditions[0].span(),
                params: vec![],
                arguments: folded_args,
                func_name,
            }
            .into(),
        );
        Ok(true)
    }

    fn bind_window_func(
        &mut self,
        join: &Join,
        s_expr: &SExpr,
        range_conditions: &[ScalarExpr],
        other_conditions: &mut Vec<ScalarExpr>,
    ) -> Result<(WindowFunc, ScalarExpr, DataType), ErrorCode> {
        let right_prop = RelExpr::with_s_expr(s_expr.child(0)?).derive_relational_prop()?;
        let left_prop = RelExpr::with_s_expr(s_expr.child(1)?).derive_relational_prop()?;

        let mut right_column = range_conditions[0].clone();
        let mut left_column = range_conditions[0].clone();
        let mut order_items: Vec<WindowOrderBy> = Vec::with_capacity(range_conditions.len());
        let mut constant_default = ConstantExpr {
            span: right_column.span(),
            value: Scalar::Null,
        };
        for condition in range_conditions.iter() {
            if let ScalarExpr::FunctionCall(func) = condition {
                if func.arguments.len() == 2 {
                    for arg in func.arguments.iter() {
                        if let ScalarExpr::BoundColumnRef(_) = arg {
                            let asc =
                                match ComparisonOp::try_from_func_name(func.func_name.as_str())
                                    .unwrap()
                                {
                                    ComparisonOp::GT | ComparisonOp::GTE => Ok(Some(true)),
                                    ComparisonOp::LT | ComparisonOp::LTE => Ok(Some(false)),
                                    _ => Err(ErrorCode::Internal("must be range condition!")),
                                }?;
                            if arg.used_columns().is_subset(&left_prop.output_columns) {
                                left_column = arg.clone();
                                constant_default.span = left_column.span();
                                constant_default.value = left_column
                                    .data_type()?
                                    .remove_nullable()
                                    .infinity()
                                    .unwrap();
                                if let Some(false) = asc {
                                    constant_default.value = left_column
                                        .data_type()?
                                        .remove_nullable()
                                        .ninfinity()
                                        .unwrap();
                                }
                                order_items.push(WindowOrderBy {
                                    expr: arg.clone(),
                                    asc,
                                    nulls_first: Some(true),
                                });
                            }
                            if arg.used_columns().is_subset(&right_prop.output_columns) {
                                right_column = arg.clone();
                            }
                        } else {
                            return Err(ErrorCode::Internal(
                                "Cannot downcast Scalar to BoundColumnRef",
                            ));
                        }
                    }
                }
            }
        }

        let mut partition_items: Vec<ScalarExpr> = Vec::with_capacity(join.equi_conditions.len());
        let mut other_args: Vec<ScalarExpr> = Vec::with_capacity(2);
        for condition in join.equi_conditions.iter() {
            if matches!(condition.right, ScalarExpr::BoundColumnRef(_))
                && matches!(condition.left, ScalarExpr::BoundColumnRef(_))
            {
                partition_items.push(condition.right.clone());
                other_args.clear();
                other_args.push(condition.left.clone());
                other_args.push(condition.right.clone());
                other_conditions.push(
                    FunctionCall {
                        span: range_conditions[0].span(),
                        params: vec![],
                        arguments: other_args.clone(),
                        func_name: String::from("eq"),
                    }
                    .into(),
                );
            } else {
                return Err(ErrorCode::Internal(
                    "Cannot downcast Scalar to BoundColumnRef",
                ));
            }
        }
        let func_type = WindowFuncType::LagLead(LagLeadFunction {
            is_lag: false,
            arg: Box::new(left_column.clone()),
            offset: 1,
            default: Some(Box::new(constant_default.into())),
            return_type: Box::new(left_column.data_type()?.clone()),
        });
        let window_func = WindowFunc {
            span: range_conditions[0].span(),
            display_name: func_type.func_name(),
            partition_by: partition_items,
            func: func_type,
            order_by: order_items,
            frame: WindowFuncFrame {
                units: WindowFuncFrameUnits::Rows,
                start_bound: WindowFuncFrameBound::Following(Some(Scalar::Number(
                    NumberScalar::UInt64(1),
                ))),
                end_bound: WindowFuncFrameBound::Following(Some(Scalar::Number(
                    NumberScalar::UInt64(1),
                ))),
            },
        };
        Ok((window_func, right_column, left_column.data_type()?.clone()))
    }

    fn build_window_plan(
        &mut self,
        window: &WindowFunc,
        s_expr: &SExpr,
        window_index: &mut usize,
    ) -> Result<SExpr> {
        let mut window_args = vec![];
        let window_func_name = window.func.func_name();
        let func = match &window.func {
            WindowFuncType::LagLead(ll) => {
                let (new_arg, new_default) =
                    self.replace_lag_lead_args(&mut window_args, &window_func_name, ll)?;

                WindowFuncType::LagLead(LagLeadFunction {
                    is_lag: ll.is_lag,
                    arg: Box::new(new_arg),
                    offset: ll.offset,
                    default: new_default,
                    return_type: ll.return_type.clone(),
                })
            }
            func => func.clone(),
        };

        // resolve partition by
        let mut partition_by_items = vec![];
        for (i, part) in window.partition_by.iter().enumerate() {
            let part = part.clone();
            let name = format!("{window_func_name}_part_{i}");
            let replaced_part = self.replace_expr(&name, &part)?;
            partition_by_items.push(ScalarItem {
                index: replaced_part.column.index,
                scalar: part,
            });
        }

        // resolve order by
        let mut order_by_items = vec![];
        for (i, order) in window.order_by.iter().enumerate() {
            let order_expr = order.expr.clone();
            let name = format!("{window_func_name}_order_{i}");
            let replaced_order = self.replace_expr(&name, &order_expr)?;
            order_by_items.push(WindowOrderByInfo {
                order_by_item: ScalarItem {
                    index: replaced_order.column.index,
                    scalar: order_expr,
                },
                asc: order.asc,
                nulls_first: order.nulls_first,
            });
        }

        let index = self.metadata.write().add_derived_column(
            window.display_name.clone(),
            window.func.return_type(),
            None,
        );

        *window_index = index;

        let window_info = WindowFunctionInfo {
            span: window.span,
            index,
            partition_by_items,
            func,
            arguments: window_args,
            order_by_items,
            frame: window.frame.clone(),
        };
        bind_window_function_info(&self.ctx, &window_info, s_expr.child(1)?.clone())
    }

    fn replace_lag_lead_args(
        &mut self,
        window_args: &mut Vec<ScalarItem>,
        window_func_name: &String,
        f: &LagLeadFunction,
    ) -> Result<(ScalarExpr, Option<Box<ScalarExpr>>)> {
        let arg = (*f.arg).clone();
        let name = format!("{window_func_name}_arg");
        let replaced_arg = self.replace_expr(&name, &arg)?;
        window_args.push(ScalarItem {
            scalar: arg,
            index: replaced_arg.column.index,
        });
        let new_default = match &f.default {
            None => None,
            Some(d) => {
                let d = (**d).clone();
                let name = format!("{window_func_name}_default_value");
                let replaced_default = self.replace_expr(&name, &d)?;
                window_args.push(ScalarItem {
                    scalar: d,
                    index: replaced_default.column.index,
                });
                Some(Box::new(replaced_default.into()))
            }
        };

        Ok((replaced_arg.into(), new_default))
    }

    fn replace_expr(&self, name: &str, arg: &ScalarExpr) -> Result<BoundColumnRef> {
        if let ScalarExpr::BoundColumnRef(col) = &arg {
            Ok(col.clone())
        } else {
            for entry in self.metadata.read().columns() {
                if let ColumnEntry::DerivedColumn(DerivedColumn {
                    scalar_expr,
                    alias,
                    column_index,
                    data_type,
                    ..
                }) = entry
                {
                    if scalar_expr.as_ref() == Some(arg) {
                        // Generate a ColumnBinding for each argument of aggregates
                        let column = ColumnBindingBuilder::new(
                            alias.to_string(),
                            *column_index,
                            Box::new(data_type.clone()),
                            Visibility::Visible,
                        )
                        .build();

                        return Ok(BoundColumnRef {
                            span: arg.span(),
                            column,
                        });
                    }
                }
            }
            let ty = arg.data_type()?;

            let index = self.metadata.write().add_derived_column(
                name.to_string(),
                ty.clone(),
                Some(arg.clone()),
            );

            // Generate a ColumnBinding for each argument of aggregates
            let column = ColumnBindingBuilder::new(
                name.to_string(),
                index,
                Box::new(ty),
                Visibility::Visible,
            )
            .build();
            Ok(BoundColumnRef {
                span: arg.span(),
                column,
            })
        }
    }
}
