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
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;

use crate::binder::ColumnBindingBuilder;
use crate::binder::Visibility;
use crate::optimizer::decorrelate::subquery_rewriter::FlattenInfo;
use crate::optimizer::ColumnSet;
use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::optimizer::SubqueryRewriter;
use crate::plans::Aggregate;
use crate::plans::AggregateFunction;
use crate::plans::AggregateMode;
use crate::plans::BoundColumnRef;
use crate::plans::EvalScalar;
use crate::plans::ExpressionScan;
use crate::plans::Filter;
use crate::plans::Join;
use crate::plans::JoinEquiCondition;
use crate::plans::JoinType;
use crate::plans::ProjectSet;
use crate::plans::RelOperator;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::Scan;
use crate::plans::Sort;
use crate::plans::SrfItem;
use crate::plans::UnionAll;
use crate::plans::Window;
use crate::BaseTableColumn;
use crate::ColumnEntry;
use crate::DerivedColumn;
use crate::IndexType;
use crate::TableInternalColumn;
use crate::VirtualColumn;

impl SubqueryRewriter {
    pub fn flatten_plan(
        &mut self,
        plan: &SExpr,
        correlated_columns: &ColumnSet,
        flatten_info: &mut FlattenInfo,
        need_cross_join: bool,
    ) -> Result<SExpr> {
        let rel_expr = RelExpr::with_s_expr(plan);
        let prop = rel_expr.derive_relational_prop()?;
        if prop.outer_columns.is_empty() {
            if !need_cross_join {
                return Ok(plan.clone());
            }
            // Construct a Scan plan by correlated columns.
            // Finally, generate a cross join, so we finish flattening the subquery.
            let mut metadata = self.metadata.write();
            // Currently, we don't support left plan's from clause contains subquery.
            // Such as: select t2.a from (select a + 1 as a from t) as t2 where (select sum(a) from t as t1 where t1.a < t2.a) = 1;
            let table_index = metadata
                .table_index_by_column_indexes(correlated_columns)
                .unwrap();
            let mut data_types = Vec::with_capacity(correlated_columns.len());
            let mut scalar_items = vec![];
            let mut scan_columns = ColumnSet::new();
            for correlated_column in correlated_columns.iter() {
                let column_entry = metadata.column(*correlated_column).clone();
                let name = column_entry.name();
                let data_type = column_entry.data_type();
                data_types.push(data_type.clone());
                let derived_col = metadata.add_derived_column(name.to_string(), data_type, None);
                self.derived_columns.insert(*correlated_column, derived_col);
                if let ColumnEntry::DerivedColumn(derived_column) = &column_entry {
                    if let Some(scalar) = &derived_column.scalar_expr {
                        // Replace columns in `scalar` to derived columns.
                        let mut scalar = scalar.clone();
                        for col in scalar.used_columns().iter() {
                            if let Some(new_col) = self.derived_columns.get(col) {
                                scalar.replace_column(*col, *new_col)?;
                            } else {
                                scan_columns.insert(*col);
                            }
                        }
                        scalar_items.push(ScalarItem {
                            scalar,
                            index: derived_col,
                        });
                    }
                } else {
                    scan_columns.insert(derived_col);
                }
            }
            let mut logical_get = SExpr::create_leaf(Arc::new(
                Scan {
                    table_index,
                    columns: scan_columns,
                    ..Default::default()
                }
                .into(),
            ));
            if !scalar_items.is_empty() {
                // Wrap `EvalScalar` to `logical_get`.
                logical_get = SExpr::create_unary(
                    Arc::new(
                        EvalScalar {
                            items: scalar_items,
                        }
                        .into(),
                    ),
                    Arc::new(logical_get),
                );
            }
            if self.ctx.get_cluster().is_empty() {
                // Wrap logical get with distinct to eliminate duplicates rows.
                let mut group_items = Vec::with_capacity(self.derived_columns.len());
                for (index, column_index) in self.derived_columns.values().cloned().enumerate() {
                    group_items.push(ScalarItem {
                        scalar: ScalarExpr::BoundColumnRef(BoundColumnRef {
                            span: None,
                            column: ColumnBindingBuilder::new(
                                "".to_string(),
                                column_index,
                                Box::new(data_types[index].clone()),
                                Visibility::Visible,
                            )
                            .table_index(Some(table_index))
                            .build(),
                        }),
                        index: column_index,
                    });
                }
                logical_get = SExpr::create_unary(
                    Arc::new(
                        Aggregate {
                            mode: AggregateMode::Initial,
                            group_items,
                            aggregate_functions: vec![],
                            from_distinct: false,
                            limit: None,
                            grouping_sets: None,
                        }
                        .into(),
                    ),
                    Arc::new(logical_get),
                );
            }

            let cross_join = Join {
                equi_conditions: JoinEquiCondition::new_conditions(vec![], vec![], vec![]),
                non_equi_conditions: vec![],
                join_type: JoinType::Cross,
                marker_index: None,
                from_correlated_subquery: false,
                need_hold_hash_table: false,
                is_lateral: false,
                single_to_inner: None,
                build_side_cache_info: None,
            }
            .into();

            return Ok(SExpr::create_binary(
                Arc::new(cross_join),
                Arc::new(logical_get),
                Arc::new(plan.clone()),
            ));
        }

        match plan.plan() {
            RelOperator::EvalScalar(eval_scalar) => self.flatten_eval_scalar(
                plan,
                eval_scalar,
                correlated_columns,
                flatten_info,
                need_cross_join,
            ),
            RelOperator::ProjectSet(project_set) => self.flatten_project_set(
                plan,
                project_set,
                correlated_columns,
                flatten_info,
                need_cross_join,
            ),
            RelOperator::Filter(filter) => self.flatten_filter(
                filter,
                plan,
                correlated_columns,
                flatten_info,
                need_cross_join,
            ),
            RelOperator::Join(join) => {
                self.flatten_join(join, plan, correlated_columns, flatten_info)
            }
            RelOperator::Aggregate(aggregate) => self.flatten_aggregate(
                aggregate,
                plan,
                correlated_columns,
                flatten_info,
                need_cross_join,
            ),
            RelOperator::Sort(sort) => self.flatten_sort(
                plan,
                sort,
                correlated_columns,
                flatten_info,
                need_cross_join,
            ),

            RelOperator::Limit(_) => {
                self.flatten_limit(plan, correlated_columns, flatten_info, need_cross_join)
            }

            RelOperator::UnionAll(op) => {
                self.flatten_union_all(op, plan, correlated_columns, flatten_info, need_cross_join)
            }

            RelOperator::Window(op) => {
                self.flatten_window(plan, op, correlated_columns, flatten_info)
            }

            RelOperator::ExpressionScan(scan) => {
                self.flatten_expression_scan(plan, scan, correlated_columns)
            }

            _ => Err(ErrorCode::Internal(
                "Invalid plan type for flattening subquery",
            )),
        }
    }

    fn flatten_eval_scalar(
        &mut self,
        plan: &SExpr,
        eval_scalar: &EvalScalar,
        correlated_columns: &ColumnSet,
        flatten_info: &mut FlattenInfo,
        mut need_cross_join: bool,
    ) -> Result<SExpr> {
        if eval_scalar
            .used_columns()?
            .iter()
            .any(|index| correlated_columns.contains(index))
        {
            need_cross_join = true;
        }
        let flatten_plan = self.flatten_plan(
            plan.child(0)?,
            correlated_columns,
            flatten_info,
            need_cross_join,
        )?;
        let mut items = Vec::with_capacity(eval_scalar.items.len());
        for item in eval_scalar.items.iter() {
            let new_item = ScalarItem {
                scalar: self.flatten_scalar(&item.scalar, correlated_columns)?,
                index: item.index,
            };
            items.push(new_item);
        }
        let metadata = self.metadata.read();
        for derived_column in self.derived_columns.values() {
            let column_entry = metadata.column(*derived_column);
            let column_binding = ColumnBindingBuilder::new(
                column_entry.name(),
                *derived_column,
                Box::from(column_entry.data_type()),
                Visibility::Visible,
            )
            .build();
            items.push(ScalarItem {
                scalar: ScalarExpr::BoundColumnRef(BoundColumnRef {
                    span: None,
                    column: column_binding,
                }),
                index: *derived_column,
            });
        }
        Ok(SExpr::create_unary(
            Arc::new(EvalScalar { items }.into()),
            Arc::new(flatten_plan),
        ))
    }

    fn flatten_project_set(
        &mut self,
        plan: &SExpr,
        project_set: &ProjectSet,
        correlated_columns: &ColumnSet,
        flatten_info: &mut FlattenInfo,
        mut need_cross_join: bool,
    ) -> Result<SExpr> {
        if project_set
            .srfs
            .iter()
            .map(|srf| srf.scalar.used_columns())
            .fold(ColumnSet::new(), |mut acc, v| {
                acc.extend(v);
                acc
            })
            .iter()
            .any(|index| correlated_columns.contains(index))
        {
            need_cross_join = true;
        }
        let flatten_plan = self.flatten_plan(
            plan.child(0)?,
            correlated_columns,
            flatten_info,
            need_cross_join,
        )?;
        let mut srfs = Vec::with_capacity(project_set.srfs.len());
        for item in project_set.srfs.iter() {
            let new_item = SrfItem {
                scalar: self.flatten_scalar(&item.scalar, correlated_columns)?,
                index: item.index,
            };
            srfs.push(new_item);
        }
        let mut scalar_items = vec![];
        let metadata = self.metadata.read();
        for derived_column in self.derived_columns.values() {
            let column_entry = metadata.column(*derived_column);
            let column_binding = ColumnBindingBuilder::new(
                column_entry.name(),
                *derived_column,
                Box::from(column_entry.data_type()),
                Visibility::Visible,
            )
            .build();
            scalar_items.push(ScalarItem {
                scalar: ScalarExpr::BoundColumnRef(BoundColumnRef {
                    span: None,
                    column: column_binding,
                }),
                index: *derived_column,
            });
        }
        Ok(SExpr::create_unary(
            Arc::new(ProjectSet { srfs }.into()),
            Arc::new(SExpr::create_unary(
                Arc::new(
                    EvalScalar {
                        items: scalar_items,
                    }
                    .into(),
                ),
                Arc::new(flatten_plan),
            )),
        ))
    }

    fn flatten_filter(
        &mut self,
        filter: &Filter,
        plan: &SExpr,
        correlated_columns: &ColumnSet,
        flatten_info: &mut FlattenInfo,
        mut need_cross_join: bool,
    ) -> Result<SExpr> {
        let mut predicates = Vec::with_capacity(filter.predicates.len());
        if !need_cross_join {
            need_cross_join = self.join_outer_inner_table(filter, correlated_columns)?;
            if need_cross_join {
                self.derived_columns.clear();
            }
        }
        let flatten_plan = self.flatten_plan(
            plan.child(0)?,
            correlated_columns,
            flatten_info,
            need_cross_join,
        )?;
        for predicate in filter.predicates.iter() {
            predicates.push(self.flatten_scalar(predicate, correlated_columns)?);
        }

        let filter_plan = Filter { predicates }.into();
        Ok(SExpr::create_unary(
            Arc::new(filter_plan),
            Arc::new(flatten_plan),
        ))
    }

    fn flatten_join(
        &mut self,
        join: &Join,
        plan: &SExpr,
        correlated_columns: &ColumnSet,
        flatten_info: &mut FlattenInfo,
    ) -> Result<SExpr> {
        // Helper function to check if conditions need a cross join
        fn needs_cross_join(
            conditions: &[JoinEquiCondition],
            correlated_columns: &HashSet<IndexType>,
            left_side: bool,
        ) -> bool {
            conditions.iter().any(|condition| {
                let condition = if left_side {
                    &condition.left
                } else {
                    &condition.right
                };
                condition
                    .used_columns()
                    .iter()
                    .any(|col| correlated_columns.contains(col))
            })
        }

        // Helper function to process conditions
        fn process_conditions(
            conditions: &[ScalarExpr],
            correlated_columns: &HashSet<IndexType>,
            derived_columns: &HashMap<IndexType, IndexType>,
            need_cross_join: bool,
        ) -> Result<Vec<ScalarExpr>> {
            if need_cross_join {
                conditions
                    .iter()
                    .map(|condition| {
                        let mut new_condition = condition.clone();
                        for col in condition.used_columns() {
                            if correlated_columns.contains(&col) {
                                let new_col = derived_columns.get(&col).ok_or_else(|| {
                                    ErrorCode::Internal("Missing derived columns")
                                })?;
                                new_condition.replace_column(col, *new_col)?;
                            }
                        }
                        Ok(new_condition)
                    })
                    .collect()
            } else {
                Ok(conditions.to_vec())
            }
        }

        let mut left_need_cross_join =
            needs_cross_join(&join.equi_conditions, correlated_columns, true);
        let mut right_need_cross_join =
            needs_cross_join(&join.equi_conditions, correlated_columns, false);

        let join_rel_expr = RelExpr::with_s_expr(plan);
        let left_prop = join_rel_expr.derive_relational_prop_child(0)?;
        let right_prop = join_rel_expr.derive_relational_prop_child(1)?;

        for condition in join.non_equi_conditions.iter() {
            for col in condition.used_columns() {
                if correlated_columns.contains(&col) {
                    if left_prop.output_columns.contains(&col) {
                        left_need_cross_join = true;
                    } else if right_prop.output_columns.contains(&col) {
                        right_need_cross_join = true;
                    }
                }
            }
        }

        let left_flatten_plan = self.flatten_plan(
            plan.child(0)?,
            correlated_columns,
            flatten_info,
            left_need_cross_join,
        )?;
        let right_flatten_plan = self.flatten_plan(
            plan.child(1)?,
            correlated_columns,
            flatten_info,
            right_need_cross_join,
        )?;

        let left_conditions = join
            .equi_conditions
            .iter()
            .map(|condition| condition.left.clone())
            .collect::<Vec<_>>();
        let left_conditions = process_conditions(
            &left_conditions,
            correlated_columns,
            &self.derived_columns,
            left_need_cross_join,
        )?;
        let right_conditions = join
            .equi_conditions
            .iter()
            .map(|condition| condition.right.clone())
            .collect::<Vec<_>>();
        let right_conditions = process_conditions(
            &right_conditions,
            correlated_columns,
            &self.derived_columns,
            right_need_cross_join,
        )?;
        let non_equi_conditions = process_conditions(
            &join.non_equi_conditions,
            correlated_columns,
            &self.derived_columns,
            true,
        )?;

        Ok(SExpr::create_binary(
            Arc::new(
                Join {
                    equi_conditions: JoinEquiCondition::new_conditions(
                        left_conditions,
                        right_conditions,
                        vec![],
                    ),
                    non_equi_conditions,
                    join_type: join.join_type.clone(),
                    marker_index: join.marker_index,
                    from_correlated_subquery: false,
                    need_hold_hash_table: false,
                    is_lateral: false,
                    single_to_inner: None,
                    build_side_cache_info: None,
                }
                .into(),
            ),
            Arc::new(left_flatten_plan),
            Arc::new(right_flatten_plan),
        ))
    }

    fn flatten_aggregate(
        &mut self,
        aggregate: &Aggregate,
        plan: &SExpr,
        correlated_columns: &ColumnSet,
        flatten_info: &mut FlattenInfo,
        mut need_cross_join: bool,
    ) -> Result<SExpr> {
        if aggregate
            .used_columns()?
            .iter()
            .any(|index| correlated_columns.contains(index))
        {
            need_cross_join = true;
        }
        let flatten_plan = self.flatten_plan(
            plan.child(0)?,
            correlated_columns,
            flatten_info,
            need_cross_join,
        )?;
        let mut group_items = Vec::with_capacity(aggregate.group_items.len());
        for item in aggregate.group_items.iter() {
            let scalar = self.flatten_scalar(&item.scalar, correlated_columns)?;
            group_items.push(ScalarItem {
                scalar,
                index: item.index,
            })
        }
        for derived_column in self.derived_columns.values() {
            let column_binding = {
                let metadata = self.metadata.read();
                let column_entry = metadata.column(*derived_column);
                let data_type = match column_entry {
                    ColumnEntry::BaseTableColumn(BaseTableColumn { data_type, .. }) => {
                        DataType::from(data_type)
                    }
                    ColumnEntry::DerivedColumn(DerivedColumn { data_type, .. }) => {
                        data_type.clone()
                    }
                    ColumnEntry::InternalColumn(TableInternalColumn {
                        internal_column, ..
                    }) => internal_column.data_type(),
                    ColumnEntry::VirtualColumn(VirtualColumn { data_type, .. }) => {
                        DataType::from(data_type)
                    }
                };
                ColumnBindingBuilder::new(
                    format!("subquery_{}", derived_column),
                    *derived_column,
                    Box::from(data_type.clone()),
                    Visibility::Visible,
                )
                .build()
            };
            group_items.push(ScalarItem {
                scalar: ScalarExpr::BoundColumnRef(BoundColumnRef {
                    span: None,
                    column: column_binding,
                }),
                index: *derived_column,
            });
        }
        let mut agg_items = Vec::with_capacity(aggregate.aggregate_functions.len());
        for item in aggregate.aggregate_functions.iter() {
            let scalar = self.flatten_scalar(&item.scalar, correlated_columns)?;
            if let ScalarExpr::AggregateFunction(AggregateFunction { func_name, .. }) = &scalar {
                // For scalar subquery, we'll convert it to single join.
                // Single join is similar to left outer join, if there isn't matched row in the right side, we'll add NULL value for the right side.
                // But for count aggregation function, NULL values should be 0.
                if aggregate.aggregate_functions.len() == 1
                    && (func_name.eq_ignore_ascii_case("count") || func_name.eq("count_distinct"))
                {
                    flatten_info.from_count_func = true;
                }
            }
            agg_items.push(ScalarItem {
                scalar,
                index: item.index,
            })
        }
        Ok(SExpr::create_unary(
            Arc::new(
                Aggregate {
                    mode: AggregateMode::Initial,
                    group_items,
                    aggregate_functions: agg_items,
                    from_distinct: aggregate.from_distinct,
                    limit: aggregate.limit,
                    grouping_sets: aggregate.grouping_sets.clone(),
                }
                .into(),
            ),
            Arc::new(flatten_plan),
        ))
    }

    fn flatten_sort(
        &mut self,
        plan: &SExpr,
        sort: &Sort,
        correlated_columns: &ColumnSet,
        flatten_info: &mut FlattenInfo,
        need_cross_join: bool,
    ) -> Result<SExpr> {
        // Currently, we don't support sort contain subquery.
        let flatten_plan = self.flatten_plan(
            plan.child(0)?,
            correlated_columns,
            flatten_info,
            need_cross_join,
        )?;
        // Check if sort contains `count() or distinct count()`.
        if sort.items.iter().any(|item| {
            let metadata = self.metadata.read();
            let col = metadata.column(item.index);
            if let ColumnEntry::DerivedColumn(derived_col) = col {
                // A little tricky here, we'll check if a sort item is a count aggregation function later.
                derived_col.alias.to_lowercase().starts_with("count")
            } else {
                false
            }
        }) {
            flatten_info.from_count_func = false;
        }
        Ok(SExpr::create_unary(
            Arc::new(plan.plan().clone()),
            Arc::new(flatten_plan),
        ))
    }

    fn flatten_limit(
        &mut self,
        plan: &SExpr,
        correlated_columns: &ColumnSet,
        flatten_info: &mut FlattenInfo,
        need_cross_join: bool,
    ) -> Result<SExpr> {
        // Currently, we don't support limit contain subquery.
        let flatten_plan = self.flatten_plan(
            plan.child(0)?,
            correlated_columns,
            flatten_info,
            need_cross_join,
        )?;
        Ok(SExpr::create_unary(
            Arc::new(plan.plan().clone()),
            Arc::new(flatten_plan),
        ))
    }

    fn flatten_window(
        &mut self,
        plan: &SExpr,
        op: &Window,
        correlated_columns: &ColumnSet,
        flatten_info: &mut FlattenInfo,
    ) -> Result<SExpr> {
        if op
            .used_columns()?
            .iter()
            .any(|index| correlated_columns.contains(index))
        {
            return Err(ErrorCode::Internal(
                "correlated columns in window functions not supported",
            ));
        }
        let flatten_plan =
            self.flatten_plan(plan.child(0)?, correlated_columns, flatten_info, true)?;
        let mut partition_by = op.partition_by.clone();
        for derived_column in self.derived_columns.values() {
            let column_binding = {
                let metadata = self.metadata.read();
                let column_entry = metadata.column(*derived_column);
                let data_type = match column_entry {
                    ColumnEntry::BaseTableColumn(BaseTableColumn { data_type, .. }) => {
                        DataType::from(data_type)
                    }
                    ColumnEntry::DerivedColumn(DerivedColumn { data_type, .. }) => {
                        data_type.clone()
                    }
                    ColumnEntry::InternalColumn(TableInternalColumn {
                        internal_column, ..
                    }) => internal_column.data_type(),
                    ColumnEntry::VirtualColumn(VirtualColumn { data_type, .. }) => {
                        DataType::from(data_type)
                    }
                };
                ColumnBindingBuilder::new(
                    format!("subquery_{}", derived_column),
                    *derived_column,
                    Box::from(data_type.clone()),
                    Visibility::Visible,
                )
                .build()
            };
            partition_by.push(ScalarItem {
                scalar: ScalarExpr::BoundColumnRef(BoundColumnRef {
                    span: None,
                    column: column_binding,
                }),
                index: *derived_column,
            });
        }
        Ok(SExpr::create_unary(
            Arc::new(
                Window {
                    span: op.span,
                    index: op.index.clone(),
                    function: op.function.clone(),
                    arguments: op.arguments.clone(),
                    partition_by,
                    order_by: op.order_by.clone(),
                    frame: op.frame.clone(),
                    limit: op.limit,
                }
                .into(),
            ),
            Arc::new(flatten_plan),
        ))
    }

    fn flatten_union_all(
        &mut self,
        op: &UnionAll,
        plan: &SExpr,
        correlated_columns: &ColumnSet,
        flatten_info: &mut FlattenInfo,
        mut need_cross_join: bool,
    ) -> Result<SExpr> {
        if op
            .used_columns()?
            .iter()
            .any(|index| correlated_columns.contains(index))
        {
            need_cross_join = true;
        }
        let left_flatten_plan = self.flatten_plan(
            plan.child(0)?,
            correlated_columns,
            flatten_info,
            need_cross_join,
        )?;
        let right_flatten_plan = self.flatten_plan(
            plan.child(1)?,
            correlated_columns,
            flatten_info,
            need_cross_join,
        )?;
        Ok(SExpr::create_binary(
            Arc::new(op.clone().into()),
            Arc::new(left_flatten_plan),
            Arc::new(right_flatten_plan),
        ))
    }

    fn flatten_expression_scan(
        &mut self,
        plan: &SExpr,
        scan: &ExpressionScan,
        correlated_columns: &ColumnSet,
    ) -> Result<SExpr> {
        let binder = self.binder.as_ref().unwrap();
        for correlated_column in correlated_columns.iter() {
            let derived_column_index = binder
                .expression_scan_context
                .get_derived_column(scan.expression_scan_index, *correlated_column);
            self.derived_columns
                .insert(*correlated_column, derived_column_index);
        }
        Ok(plan.clone())
    }
}
