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
    pub left: Box<PhysicalPlan>,
    pub right: Box<PhysicalPlan>,
    pub pairs: Vec<(String, String)>,
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
        // 1. Prune unused Columns.
        let left_required = union_all.pairs.iter().fold(required.clone(), |mut acc, v| {
            acc.insert(v.0);
            acc
        });
        let right_required = union_all.pairs.iter().fold(required, |mut acc, v| {
            acc.insert(v.1);
            acc
        });

        // 2. Build physical plan.
        let left_plan = self.build(s_expr.child(0)?, left_required).await?;
        let right_plan = self.build(s_expr.child(1)?, right_required).await?;
        let left_schema = left_plan.output_schema()?;
        let right_schema = right_plan.output_schema()?;

        let common_types = union_all.pairs.iter().map(|(l, r)| {
            let left_field = left_schema.field_with_name(&l.to_string()).unwrap();
            let right_field = right_schema.field_with_name(&r.to_string()).unwrap();

            let common_type = common_super_type(
                left_field.data_type().clone(),
                right_field.data_type().clone(),
                &BUILTIN_FUNCTIONS.default_cast_rules,
            );
            common_type.ok_or_else(|| {
                ErrorCode::SemanticError(format!(
                    "SetOperation's types cannot be matched, left column {:?}, type: {:?}, right column {:?}, type: {:?}",
                    left_field.name(),
                    left_field.data_type(),
                    right_field.name(),
                    right_field.data_type()
                ))
            })
        }).collect::<Result<Vec<_>>>()?;

        async fn cast_plan(
            plan_builder: &mut PhysicalPlanBuilder,
            plan: PhysicalPlan,
            plan_schema: &DataSchema,
            indexes: &[IndexType],
            common_types: &[DataType],
            stat_info: PlanStatsInfo,
        ) -> Result<PhysicalPlan> {
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
                plan_builder.create_eval_scalar(
                    &crate::plans::EvalScalar {
                        items: scalar_items,
                    },
                    indexes.to_vec(),
                    plan,
                    stat_info,
                )?
            };

            Ok(new_plan)
        }

        let left_indexes = union_all.pairs.iter().map(|(l, _)| *l).collect::<Vec<_>>();
        let right_indexes = union_all.pairs.iter().map(|(_, r)| *r).collect::<Vec<_>>();
        let left_plan = cast_plan(
            self,
            left_plan,
            left_schema.as_ref(),
            &left_indexes,
            &common_types,
            stat_info.clone(),
        )
        .await?;
        let right_plan = cast_plan(
            self,
            right_plan,
            right_schema.as_ref(),
            &right_indexes,
            &common_types,
            stat_info.clone(),
        )
        .await?;

        let pairs = union_all
            .pairs
            .iter()
            .map(|(l, r)| (l.to_string(), r.to_string()))
            .collect::<Vec<_>>();
        let fields = left_indexes
            .iter()
            .zip(&common_types)
            .map(|(index, ty)| DataField::new(&index.to_string(), ty.clone()))
            .collect::<Vec<_>>();

        Ok(PhysicalPlan::UnionAll(UnionAll {
            plan_id: 0,
            left: Box::new(left_plan),
            right: Box::new(right_plan),
            pairs,
            schema: DataSchemaRefExt::create(fields),

            stat_info: Some(stat_info),
        }))
    }
}
