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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check::common_super_type;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::RemoteExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::binder::wrap_cast;
use crate::binder::JoinPredicate;
use crate::executor::explain::PlanStatsInfo;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::ColumnSet;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::SExpr;
use crate::plans::JoinType;
use crate::ScalarExpr;
use crate::TypeCheck;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RangeJoin {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
    // The first two conditions: (>, >=, <, <=)
    // Condition's left/right side only contains one table's column
    pub conditions: Vec<RangeJoinCondition>,
    // The other conditions
    pub other_conditions: Vec<RemoteExpr>,
    // Now only support inner join, will support left/right join later
    pub join_type: JoinType,
    pub range_join_type: RangeJoinType,
    pub output_schema: DataSchemaRef,

    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl RangeJoin {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.output_schema.clone())
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum RangeJoinType {
    IEJoin,
    Merge,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RangeJoinCondition {
    pub left_expr: RemoteExpr,
    pub right_expr: RemoteExpr,
    // "gt" | "lt" | "gte" | "lte"
    pub operator: String,
}

impl PhysicalPlanBuilder {
    pub async fn build_range_join(
        &mut self,
        join_type: JoinType,
        s_expr: &SExpr,
        left_required: ColumnSet,
        right_required: ColumnSet,
        mut range_conditions: Vec<ScalarExpr>,
        mut other_conditions: Vec<ScalarExpr>,
    ) -> Result<PhysicalPlan> {
        let left_prop = RelExpr::with_s_expr(s_expr.child(1)?).derive_relational_prop()?;
        let right_prop = RelExpr::with_s_expr(s_expr.child(0)?).derive_relational_prop()?;

        debug_assert!(!range_conditions.is_empty());

        let range_join_type = if range_conditions.len() >= 2 {
            // Contain more than 2 ie conditions, use ie join
            while range_conditions.len() > 2 {
                other_conditions.push(range_conditions.pop().unwrap());
            }
            RangeJoinType::IEJoin
        } else {
            RangeJoinType::Merge
        };

        // Construct IEJoin
        let left_side = self.build(s_expr.child(1)?, left_required).await?;
        let right_side = self.build(s_expr.child(0)?, right_required).await?;

        let left_schema = match join_type {
            JoinType::Right | JoinType::RightSingle | JoinType::Full => {
                let left_schema = left_side.output_schema()?;
                // Wrap nullable type for columns in build side.
                let left_schema = DataSchemaRefExt::create(
                    left_schema
                        .fields()
                        .iter()
                        .map(|field| {
                            DataField::new(field.name(), field.data_type().wrap_nullable())
                        })
                        .collect::<Vec<_>>(),
                );
                left_schema
            }
            _ => left_side.output_schema()?,
        };
        let right_schema = match join_type {
            JoinType::Left | JoinType::LeftSingle | JoinType::Full => {
                let right_schema = right_side.output_schema()?;
                // Wrap nullable type for columns in build side.
                let right_schema = DataSchemaRefExt::create(
                    right_schema
                        .fields()
                        .iter()
                        .map(|field| {
                            DataField::new(field.name(), field.data_type().wrap_nullable())
                        })
                        .collect::<Vec<_>>(),
                );
                right_schema
            }
            _ => right_side.output_schema()?,
        };

        let merged_schema = DataSchemaRefExt::create(
            left_schema
                .fields()
                .iter()
                .chain(right_schema.fields())
                .cloned()
                .collect::<Vec<_>>(),
        );

        Ok(PhysicalPlan::RangeJoin(RangeJoin {
            plan_id: 0,
            left: Box::new(left_side),
            right: Box::new(right_side),
            conditions: range_conditions
                .iter()
                .map(|scalar| {
                    resolve_range_condition(
                        scalar,
                        &left_schema,
                        &right_schema,
                        &left_prop,
                        &right_prop,
                    )
                })
                .collect::<Result<_>>()?,
            other_conditions: other_conditions
                .iter()
                .map(|scalar| resolve_scalar(scalar, &merged_schema))
                .collect::<Result<_>>()?,
            join_type,
            range_join_type,
            output_schema: merged_schema,
            stat_info: Some(self.build_plan_stat_info(s_expr)?),
        }))
    }
}

fn resolve_range_condition(
    expr: &ScalarExpr,
    left_schema: &DataSchemaRef,
    right_schema: &DataSchemaRef,
    left_prop: &RelationalProperty,
    right_prop: &RelationalProperty,
) -> Result<RangeJoinCondition> {
    match expr {
        ScalarExpr::FunctionCall(func) => {
            let mut left = None;
            let mut right = None;
            let mut opposite = false;
            let mut arg1 = func.arguments[0].clone();
            let mut arg2 = func.arguments[1].clone();
            // Try to find common type for left_expr/right_expr
            let arg1_data_type = arg1.data_type()?;
            let arg2_data_type = arg2.data_type()?;
            if arg1_data_type.ne(&arg2_data_type) {
                let common_type = common_super_type(
                    arg1_data_type.clone(),
                    arg2_data_type.clone(),
                    &BUILTIN_FUNCTIONS.default_cast_rules,
                )
                .ok_or_else(|| {
                    ErrorCode::IllegalDataType(format!(
                        "Cannot find common type for {arg1_data_type} and {arg2_data_type}"
                    ))
                })?;
                arg1 = wrap_cast(&arg1, &common_type);
                arg2 = wrap_cast(&arg2, &common_type);
            };
            for (idx, arg) in [arg1, arg2].iter().enumerate() {
                let join_predicate = JoinPredicate::new(arg, left_prop, right_prop);
                match join_predicate {
                    JoinPredicate::Left(_) => {
                        left = Some(arg.type_check(left_schema.as_ref())?.project_column_ref(
                            |index| left_schema.index_of(&index.to_string()).unwrap(),
                        ));
                    }
                    JoinPredicate::Right(_) => {
                        if idx == 0 {
                            opposite = true;
                        }
                        right = Some(arg.type_check(right_schema.as_ref())?.project_column_ref(
                            |index| right_schema.index_of(&index.to_string()).unwrap(),
                        ));
                    }
                    JoinPredicate::ALL(_)
                    | JoinPredicate::Both { .. }
                    | JoinPredicate::Other(_) => unreachable!(),
                }
            }
            let op = if opposite {
                match func.func_name.as_str() {
                    "gt" => "lt",
                    "lt" => "gt",
                    "gte" => "lte",
                    "lte" => "gte",
                    _ => unreachable!(),
                }
            } else {
                func.func_name.as_str()
            };
            Ok(RangeJoinCondition {
                left_expr: left.unwrap().as_remote_expr(),
                right_expr: right.unwrap().as_remote_expr(),
                operator: op.to_string(),
            })
        }
        _ => unreachable!(),
    }
}

fn resolve_scalar(scalar: &ScalarExpr, schema: &DataSchemaRef) -> Result<RemoteExpr> {
    let expr = scalar
        .type_check(schema.as_ref())?
        .project_column_ref(|index| schema.index_of(&index.to_string()).unwrap());
    Ok(expr.as_remote_expr())
}
