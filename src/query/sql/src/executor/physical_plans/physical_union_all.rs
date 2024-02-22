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
use databend_common_expression::types::DataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_functions::BUILTIN_FUNCTIONS;

use crate::binder::wrap_cast;
use crate::executor::explain::PlanStatsInfo;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::SExpr;
use crate::optimizer::StatInfo;
use crate::plans::BoundColumnRef;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::ScalarItem;
use crate::ColumnBindingBuilder;
use crate::ColumnSet;
use crate::IndexType;
use crate::ScalarExpr;
use crate::Visibility;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct UnionAll {
    // A unique id of operator in a `PhysicalPlan` tree, only used for display.
    pub plan_id: u32,
    pub children: Vec<Box<PhysicalPlan>>,
    pub output_cols: Vec<Vec<IndexType>>,
    pub schema: DataSchemaRef,

    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

impl UnionAll {
    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(self.schema.clone())
    }
}

impl PhysicalPlanBuilder {
    pub(crate) async fn build_union_all(
        &mut self,
        s_expr: &SExpr,
        union_all: &crate::plans::UnionAll,
        required: ColumnSet,
        stat_info: PlanStatsInfo,
    ) -> Result<PhysicalPlan> {
        // Flatten union plan
        let mut union_children = vec![];
        let mut output_cols = vec![];
        self.flatten_union(
            s_expr,
            union_all,
            required,
            &mut union_children,
            &mut output_cols,
        )
        .await?;

        debug_assert!(union_children.len() >= 2);
        let left_schema = union_children[0].output_schema()?;
        let right_schema = union_children[1].output_schema()?;
        let mut common_types = vec![];
        for (l, r) in output_cols[0].iter().zip(output_cols[1].iter()) {
            let left_field = left_schema.field_with_name(&l.to_string())?;
            let right_field = right_schema.field_with_name(&r.to_string())?;

            let common_type = common_super_type(
                left_field.data_type().clone(),
                right_field.data_type().clone(),
                &BUILTIN_FUNCTIONS.default_cast_rules,
            );
            common_types.push(common_type.ok_or_else(|| {
                ErrorCode::SemanticError(format!(
                    "SetOperation's types cannot be matched, left column {:?}, type: {:?}, right column {:?}, type: {:?}",
                    left_field.name(),
                    left_field.data_type(),
                    right_field.name(),
                    right_field.data_type()
                ))
            })?)
        }

        for (right_plan, right_output_cols) in union_children.iter().zip(output_cols.iter()).skip(2)
        {
            common_types =
                common_super_types_for_union(common_types, right_plan, right_output_cols)?;
        }

        async fn cast_plan(
            plan_builder: &mut PhysicalPlanBuilder,
            plan: Box<PhysicalPlan>,
            plan_schema: &DataSchema,
            indexes: &[IndexType],
            common_types: &[DataType],
            stat_info: PlanStatsInfo,
        ) -> Result<Box<PhysicalPlan>> {
            debug_assert!(indexes.len() == common_types.len());
            let scalar_items = indexes
                .iter()
                .map(|index| plan_schema.field_with_name(&index.to_string()).unwrap())
                .zip(common_types)
                .filter(|(f, common_ty)| f.data_type() != *common_ty)
                .map(|(f, common_ty)| {
                    let column = ColumnBindingBuilder::new(
                        f.name().clone(),
                        f.name().parse().unwrap(),
                        Box::new(f.data_type().clone()),
                        Visibility::Visible,
                    )
                    .build();
                    let cast_expr = wrap_cast(
                        &ScalarExpr::BoundColumnRef(BoundColumnRef { span: None, column }),
                        common_ty,
                    );
                    ScalarItem {
                        scalar: cast_expr,
                        index: f.name().parse().unwrap(),
                    }
                })
                .collect::<Vec<_>>();

            let new_plan = if scalar_items.is_empty() {
                plan
            } else {
                Box::new(plan_builder.create_eval_scalar(
                    &crate::plans::EvalScalar {
                        items: scalar_items,
                    },
                    indexes.to_vec(),
                    *plan,
                    stat_info,
                )?)
            };

            Ok(new_plan)
        }

        for (plan, idxes) in union_children.iter_mut().zip(output_cols.iter()) {
            let schema = plan.output_schema()?;
            *plan = cast_plan(
                self,
                plan.clone(),
                &schema,
                idxes,
                &common_types,
                stat_info.clone(),
            )
            .await?;
        }

        let fields = output_cols[0]
            .iter()
            .zip(&common_types)
            .map(|(index, ty)| DataField::new(&index.to_string(), ty.clone()))
            .collect::<Vec<_>>();

        Ok(PhysicalPlan::UnionAll(UnionAll {
            plan_id: 0,
            children: union_children,
            schema: DataSchemaRefExt::create(fields),
            output_cols,
            stat_info: Some(stat_info),
        }))
    }

    #[async_recursion::async_recursion]
    async fn flatten_union(
        &mut self,
        s_expr: &SExpr,
        union: &crate::plans::UnionAll,
        required: ColumnSet,
        union_children: &mut Vec<Box<PhysicalPlan>>,
        output_cols: &mut Vec<Vec<IndexType>>,
    ) -> Result<()> {
        let left_required = union.left_cols.iter().fold(required.clone(), |mut acc, v| {
            acc.insert(*v);
            acc
        });
        let left_plan = self.build(s_expr.child(0)?, left_required).await?;
        union_children.push(Box::new(left_plan));
        output_cols.push(union.left_cols.clone());
        let right_s_expr = s_expr.child(1)?;
        if right_s_expr.plan.rel_op() != RelOp::UnionAll {
            let right_required = union.right_cols.iter().fold(required, |mut acc, v| {
                acc.insert(*v);
                acc
            });
            let plan = self.build(right_s_expr, right_required).await?;
            union_children.push(Box::new(plan));
            output_cols.push(union.right_cols.clone());
            return Ok(());
        }
        self.flatten_union(
            right_s_expr,
            &right_s_expr.plan().clone().try_into()?,
            required,
            union_children,
            output_cols,
        )
        .await
    }
}

fn common_super_types_for_union(
    common_types: Vec<DataType>,
    right_plan: &PhysicalPlan,
    right_output_cols: &[IndexType],
) -> Result<Vec<DataType>> {
    let mut new_common_types = Vec::with_capacity(common_types.len());
    let right_schema = right_plan.output_schema()?;
    for (left_type, idx) in common_types.iter().zip(right_output_cols.iter()) {
        let right_field = right_schema.field_with_name(&idx.to_string())?;
        let common_type = common_super_type(
            left_type.clone(),
            right_field.data_type().clone(),
            &BUILTIN_FUNCTIONS.default_cast_rules,
        );
        new_common_types.push(common_type.ok_or_else(|| {
            ErrorCode::SemanticError(format!(
                "SetOperation's types cannot be matched, left type: {:?}, right type: {:?}",
                left_type,
                right_field.data_type()
            ))
        })?)
    }
    Ok(new_common_types)
}
