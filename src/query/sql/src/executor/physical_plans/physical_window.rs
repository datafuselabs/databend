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

use std::fmt::Display;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check;
use databend_common_expression::type_check::common_super_type;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::FunctionContext;
use databend_common_expression::RawExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::binder::wrap_cast;
use crate::executor::explain::PlanStatsInfo;
use crate::executor::physical_plans::common::AggregateFunctionDesc;
use crate::executor::physical_plans::common::AggregateFunctionSignature;
use crate::executor::physical_plans::common::SortDesc;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::SExpr;
use crate::plans::WindowFuncFrame;
use crate::plans::WindowFuncFrameBound;
use crate::plans::WindowFuncType;
use crate::ColumnSet;
use crate::IndexType;
use crate::ScalarExpr;
use crate::TypeCheck;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Window {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub index: Vec<IndexType>,
    pub input: Box<PhysicalPlan>,
    pub func: Vec<WindowFunction>,
    pub partition_by: Vec<IndexType>,
    pub order_by: Vec<SortDesc>,
    pub window_frame: WindowFuncFrame,
    pub limit: Option<usize>,
}

impl Window {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        let input_schema = self.input.output_schema()?;
        let mut fields = Vec::with_capacity(input_schema.fields().len() + 1);
        fields.extend_from_slice(input_schema.fields());
        for (index, func) in self.index.iter().zip(self.func.iter()) {
            fields.push(DataField::new(&index.to_string(), func.data_type()?));
        }
        Ok(DataSchemaRefExt::create(fields))
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum WindowFunction {
    Aggregate(AggregateFunctionDesc),
    RowNumber,
    Rank,
    DenseRank,
    PercentRank,
    LagLead(LagLeadFunctionDesc),
    NthValue(NthValueFunctionDesc),
    Ntile(NtileFunctionDesc),
    CumeDist,
}

impl WindowFunction {
    fn data_type(&self) -> Result<DataType> {
        match self {
            WindowFunction::Aggregate(agg) => agg.sig.return_type(),
            WindowFunction::RowNumber | WindowFunction::Rank | WindowFunction::DenseRank => {
                Ok(DataType::Number(NumberDataType::UInt64))
            }
            WindowFunction::PercentRank | WindowFunction::CumeDist => {
                Ok(DataType::Number(NumberDataType::Float64))
            }
            WindowFunction::LagLead(f) => Ok(f.return_type.clone()),
            WindowFunction::NthValue(f) => Ok(f.return_type.clone()),
            WindowFunction::Ntile(f) => Ok(f.return_type.clone()),
        }
    }
}

impl Display for WindowFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            WindowFunction::Aggregate(agg) => write!(f, "{}", agg.sig.name),
            WindowFunction::RowNumber => write!(f, "row_number"),
            WindowFunction::Rank => write!(f, "rank"),
            WindowFunction::DenseRank => write!(f, "dense_rank"),
            WindowFunction::PercentRank => write!(f, "percent_rank"),
            WindowFunction::LagLead(lag_lead) if lag_lead.is_lag => write!(f, "lag"),
            WindowFunction::LagLead(_) => write!(f, "lead"),
            WindowFunction::NthValue(_) => write!(f, "nth_value"),
            WindowFunction::Ntile(_) => write!(f, "ntile"),
            WindowFunction::CumeDist => write!(f, "cume_dist"),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum LagLeadDefault {
    Null,
    Index(IndexType),
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LagLeadFunctionDesc {
    pub is_lag: bool,
    pub offset: u64,
    pub arg: usize,
    pub return_type: DataType,
    pub default: LagLeadDefault,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct NthValueFunctionDesc {
    pub n: Option<u64>,
    pub arg: usize,
    pub return_type: DataType,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct NtileFunctionDesc {
    pub n: u64,
    pub return_type: DataType,
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_window(
        &mut self,
        s_expr: &SExpr,
        window: &crate::plans::Window,
        mut required: ColumnSet,
        _stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        // 1. DO NOT Prune unused Columns cause window may not in required, eg:
        // select s1.a from ( select t1.a as a, dense_rank() over(order by t1.a desc) as rk
        // from (select 'a1' as a) t1 ) s1
        // left join ( select dense_rank() over(order by t1.a desc) as rk
        // from (select 'a2' as a) t1 )s2 on s1.rk=s2.rk;

        // The scalar items in window function is not replaced yet.
        // The will be replaced in physical plan builder.
        window.arguments.iter().for_each(|items| {
            for item in items {
                required.extend(item.scalar.used_columns());
                required.insert(item.index);
            }
        });
        window.partition_by.iter().for_each(|item| {
            required.extend(item.scalar.used_columns());
            required.insert(item.index);
        });
        window.order_by.iter().for_each(|item| {
            required.extend(item.order_by_item.scalar.used_columns());
            required.insert(item.order_by_item.index);
        });

        // 2. Build physical plan.
        let input = self.build(s_expr.child(0)?, required).await?;
        let mut w = window.clone();

        let input_schema = input.output_schema()?;

        if w.frame.units.is_range() && w.order_by.len() == 1 {
            let order_by = &mut w.order_by[0].order_by_item.scalar;

            let mut start = match &mut w.frame.start_bound {
                WindowFuncFrameBound::Preceding(scalar)
                | WindowFuncFrameBound::Following(scalar) => scalar.as_mut(),
                _ => None,
            };
            let mut end = match &mut w.frame.end_bound {
                WindowFuncFrameBound::Preceding(scalar)
                | WindowFuncFrameBound::Following(scalar) => scalar.as_mut(),
                _ => None,
            };

            let mut common_ty = order_by.type_check(&*input_schema)?.data_type().clone();
            for scalar in start.iter_mut().chain(end.iter_mut()) {
                let ty = scalar.as_ref().infer_data_type();
                common_ty = common_super_type(
                    common_ty.clone(),
                    ty.clone(),
                    &BUILTIN_FUNCTIONS.default_cast_rules,
                )
                .ok_or_else(|| {
                    ErrorCode::IllegalDataType(format!(
                        "Cannot find common type for {:?} and {:?}",
                        &common_ty, &ty
                    ))
                })?;
            }

            *order_by = wrap_cast(order_by, &common_ty);
            for scalar in start.iter_mut().chain(end.iter_mut()) {
                let raw_expr = RawExpr::<usize>::Cast {
                    span: w.span,
                    is_try: false,
                    expr: Box::new(RawExpr::Constant {
                        span: w.span,
                        scalar: scalar.clone(),
                    }),
                    dest_type: common_ty.clone(),
                };
                let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS)?;
                let (expr, _) =
                    ConstantFolder::fold(&expr, &FunctionContext::default(), &BUILTIN_FUNCTIONS);
                if let databend_common_expression::Expr::Constant {
                    scalar: new_scalar, ..
                } = expr
                {
                    if new_scalar.is_positive() {
                        **scalar = new_scalar;
                        continue;
                    }
                }
                return Err(ErrorCode::SemanticError(
                    "Only positive numbers are allowed in RANGE offset".to_string(),
                )
                .set_span(w.span));
            }
        }

        let order_by_items = w
            .order_by
            .iter()
            .map(|v| SortDesc {
                asc: v.asc.unwrap_or(true),
                nulls_first: v.nulls_first.unwrap_or(false),
                order_by: v.order_by_item.index,
                display_name: self.metadata.read().column(v.order_by_item.index).name(),
            })
            .collect::<Vec<_>>();
        let partition_items = w.partition_by.iter().map(|v| v.index).collect::<Vec<_>>();

        let mut func = vec![];
        for (i, f) in w.function.iter().enumerate() {
            let function = match &f {
                WindowFuncType::Aggregate(agg) => {
                    WindowFunction::Aggregate(AggregateFunctionDesc {
                        sig: AggregateFunctionSignature {
                            name: agg.func_name.clone(),
                            args: agg
                                .args
                                .iter()
                                .map(|s| s.data_type())
                                .collect::<Result<_>>()?,
                            params: agg.params.clone(),
                        },
                        output_column: w.index[i],
                        arg_indices: agg
                            .args
                            .iter()
                            .map(|arg| {
                                if let ScalarExpr::BoundColumnRef(col) = arg {
                                    Ok(col.column.index)
                                } else {
                                    Err(ErrorCode::Internal(
                                        "Aggregate function argument must be a BoundColumnRef"
                                            .to_string(),
                                    ))
                                }
                            })
                            .collect::<Result<_>>()?,
                        display: ScalarExpr::AggregateFunction(agg.clone())
                            .as_expr()?
                            .sql_display(),
                    })
                }
                WindowFuncType::LagLead(lag_lead) => {
                    let new_default = match &lag_lead.default {
                        None => LagLeadDefault::Null,
                        Some(d) => match d {
                            box ScalarExpr::BoundColumnRef(col) => {
                                LagLeadDefault::Index(col.column.index)
                            }
                            _ => unreachable!(),
                        },
                    };
                    WindowFunction::LagLead(LagLeadFunctionDesc {
                        is_lag: lag_lead.is_lag,
                        offset: lag_lead.offset,
                        return_type: *lag_lead.return_type.clone(),
                        arg: if let ScalarExpr::BoundColumnRef(col) = *lag_lead.arg.clone() {
                            Ok(col.column.index)
                        } else {
                            Err(ErrorCode::Internal(
                                "Window's lag function argument must be a BoundColumnRef"
                                    .to_string(),
                            ))
                        }?,
                        default: new_default,
                    })
                }

                WindowFuncType::NthValue(func) => WindowFunction::NthValue(NthValueFunctionDesc {
                    n: func.n,
                    return_type: *func.return_type.clone(),
                    arg: if let ScalarExpr::BoundColumnRef(col) = &*func.arg {
                        Ok(col.column.index)
                    } else {
                        Err(ErrorCode::Internal(
                            "Window's nth_value function argument must be a BoundColumnRef"
                                .to_string(),
                        ))
                    }?,
                }),
                WindowFuncType::Ntile(func) => WindowFunction::Ntile(NtileFunctionDesc {
                    n: func.n,
                    return_type: *func.return_type.clone(),
                }),
                WindowFuncType::RowNumber => WindowFunction::RowNumber,
                WindowFuncType::Rank => WindowFunction::Rank,
                WindowFuncType::DenseRank => WindowFunction::DenseRank,
                WindowFuncType::PercentRank => WindowFunction::PercentRank,
                WindowFuncType::CumeDist => WindowFunction::CumeDist,
            };
            func.push(function);
        }

        Ok(PhysicalPlan::Window(Window {
            plan_id: 0,
            index: w.index.clone(),
            input: Box::new(input),
            func,
            partition_by: partition_items,
            order_by: order_by_items,
            window_frame: w.frame.clone(),
            limit: w.limit,
        }))
    }
}
