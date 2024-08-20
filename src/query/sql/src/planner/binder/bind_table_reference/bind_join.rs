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

use async_recursion::async_recursion;
use databend_common_ast::ast::split_conjunctions_expr;
use databend_common_ast::ast::split_equivalent_predicate_expr;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::JoinCondition;
use databend_common_ast::ast::JoinOperator;
use databend_common_ast::Span;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use indexmap::IndexMap;

use crate::binder::CteInfo;
use crate::binder::Finder;
use crate::binder::JoinPredicate;
use crate::binder::Visibility;
use crate::normalize_identifier;
use crate::optimizer::ColumnSet;
use crate::optimizer::FlattenInfo;
use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::optimizer::SubqueryRewriter;
use crate::planner::binder::scalar::ScalarBinder;
use crate::planner::binder::Binder;
use crate::planner::semantic::NameResolutionContext;
use crate::plans::BoundColumnRef;
use crate::plans::Filter;
use crate::plans::HashJoinBuildCacheInfo;
use crate::plans::Join;
use crate::plans::JoinEquiCondition;
use crate::plans::JoinType;
use crate::plans::ScalarExpr;
use crate::plans::Visitor;
use crate::BindContext;
use crate::ColumnBinding;
use crate::IndexType;
use crate::MetadataRef;

pub struct JoinConditions {
    pub(crate) left_conditions: Vec<ScalarExpr>,
    pub(crate) right_conditions: Vec<ScalarExpr>,
    pub(crate) non_equi_conditions: Vec<ScalarExpr>,
    pub(crate) other_conditions: Vec<ScalarExpr>,
}

impl Binder {
    pub(crate) fn bind_join(
        &mut self,
        bind_context: &mut BindContext,
        join: &databend_common_ast::ast::Join,
    ) -> Result<(SExpr, BindContext)> {
        let (left_child, mut left_context) = self.bind_table_reference(bind_context, &join.left)?;
        let left_column_bindings = left_context.columns.clone();

        let cache_column_bindings = left_column_bindings.clone();
        let mut cache_column_indexes = Vec::with_capacity(cache_column_bindings.len());
        for column in cache_column_bindings.iter() {
            cache_column_indexes.push(column.index);
        }
        let cache_idx = self
            .expression_scan_context
            .add_hash_join_build_cache(cache_column_bindings, cache_column_indexes);

        if join.right.is_lateral_table_function() {
            let (result_expr, bind_context) = self.bind_lateral_table_function(
                &mut left_context,
                left_child.clone(),
                &join.right,
            )?;
            return Ok((result_expr, bind_context));
        }
        let (right_child, right_context) = if join.right.is_lateral_subquery() {
            self.bind_table_reference(&mut left_context, &join.right)?
        } else {
            self.bind_table_reference(bind_context, &join.right)?
        };

        let right_column_bindings = right_context.columns.clone();

        let mut bind_context = bind_context.replace();

        self.check_table_name_and_condition(
            &left_column_bindings,
            &right_column_bindings,
            &join.op,
            &join.condition,
        )?;

        let join_conditions = self.generate_join_condition(
            &mut bind_context,
            &join.op,
            &join.condition,
            &left_column_bindings,
            &right_column_bindings,
        )?;

        let build_side_cache_info = self.expression_scan_context.generate_cache_info(cache_idx);

        let join_type = join_type(&join.op);
        let s_expr = self.bind_join_with_type(
            join_type.clone(),
            join_conditions,
            left_child,
            right_child,
            build_side_cache_info,
        )?;

        let bind_context = join_bind_context(&join_type, bind_context, left_context, right_context);

        Ok((s_expr, bind_context))
    }

    // TODO: unify this function with bind_join
    #[async_recursion(#[recursive::recursive])]
    pub(crate) async fn bind_merge_into_join(
        &mut self,
        bind_context: &mut BindContext,
        left_context: BindContext,
        right_context: BindContext,
        left_child: SExpr,
        right_child: SExpr,
        join_op: JoinOperator,
        join_condition: JoinCondition,
    ) -> Result<(SExpr, BindContext)> {
        self.check_table_name_and_condition(
            &left_context.columns,
            &right_context.columns,
            &join_op,
            &join_condition,
        )?;

        let mut bind_context = bind_context.replace();

        let join_conditions = self.generate_join_condition(
            &mut bind_context,
            &join_op,
            &join_condition,
            &left_context.columns,
            &right_context.columns,
        )?;

        let join_type = join_type(&join_op);
        let s_expr = self.bind_join_with_type(
            join_type.clone(),
            join_conditions,
            left_child,
            right_child,
            None,
        )?;
        let bind_context = join_bind_context(&join_type, bind_context, left_context, right_context);

        Ok((s_expr, bind_context))
    }

    fn generate_join_condition(
        &self,
        bind_context: &mut BindContext,
        join_op: &JoinOperator,
        join_condition: &JoinCondition,
        left_column_bindings: &[ColumnBinding],
        right_column_bindings: &[ColumnBinding],
    ) -> Result<JoinConditions> {
        let mut left_join_conditions: Vec<ScalarExpr> = vec![];
        let mut right_join_conditions: Vec<ScalarExpr> = vec![];
        let mut non_equi_conditions: Vec<ScalarExpr> = vec![];
        let mut other_conditions: Vec<ScalarExpr> = vec![];
        let mut join_condition_resolver = JoinConditionResolver::new(
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            self.m_cte_bound_ctx.clone(),
            self.ctes_map.clone(),
            join_op.clone(),
            left_column_bindings,
            right_column_bindings,
            bind_context,
            join_condition,
        );

        join_condition_resolver.resolve(
            &mut left_join_conditions,
            &mut right_join_conditions,
            &mut non_equi_conditions,
            &mut other_conditions,
            join_op,
        )?;

        Ok(JoinConditions {
            left_conditions: left_join_conditions,
            right_conditions: right_join_conditions,
            non_equi_conditions,
            other_conditions,
        })
    }

    pub(crate) fn bind_join_with_type(
        &mut self,
        mut join_type: JoinType,
        join_conditions: JoinConditions,
        mut left_child: SExpr,
        mut right_child: SExpr,
        build_side_cache_info: Option<HashJoinBuildCacheInfo>,
    ) -> Result<SExpr> {
        let mut left_conditions = join_conditions.left_conditions;
        let mut right_conditions = join_conditions.right_conditions;
        let mut non_equi_conditions = join_conditions.non_equi_conditions;
        let other_conditions = join_conditions.other_conditions;

        if join_type == JoinType::Cross
            && (!left_conditions.is_empty() || !right_conditions.is_empty())
        {
            return Err(ErrorCode::SemanticError(
                "Join conditions should be empty in cross join",
            ));
        }
        if matches!(
            join_type,
            JoinType::Asof | JoinType::LeftAsof | JoinType::RightAsof
        ) && non_equi_conditions.is_empty()
        {
            return Err(ErrorCode::SemanticError("Missing inequality condition!"));
        }
        self.push_down_other_conditions(
            &join_type,
            &mut left_child,
            &mut right_child,
            other_conditions,
            &mut non_equi_conditions,
        )?;

        let right_prop = RelExpr::with_s_expr(&right_child).derive_relational_prop()?;
        let mut is_lateral = false;
        let mut is_null_equal = Vec::new();
        if !right_prop.outer_columns.is_empty() {
            // If there are outer columns in right child, then the join is a correlated lateral join
            let mut decorrelator =
                SubqueryRewriter::new(self.ctx.clone(), self.metadata.clone(), Some(self.clone()));
            right_child = decorrelator.flatten_plan(
                &right_child,
                &right_prop.outer_columns,
                &mut FlattenInfo {
                    from_count_func: false,
                },
                false,
            )?;
            let original_num_conditions = left_conditions.len();
            decorrelator.add_equi_conditions(
                None,
                &right_prop.outer_columns,
                &mut right_conditions,
                &mut left_conditions,
            )?;
            if build_side_cache_info.is_some() {
                let num_conditions = left_conditions.len();
                for i in original_num_conditions..num_conditions {
                    is_null_equal.push(i);
                }
            }
            if join_type == JoinType::Cross {
                join_type = JoinType::Inner;
            }
            is_lateral = true;
        }

        let (left_child, right_child, join_type, left_conditions, right_conditions) =
            // If there are cache indexes used in the expression scan context, we swap the left and right child
            // to make left child as the build side.
            if !self.expression_scan_context.used_cache_indexes.is_empty() {
                (
                    right_child,
                    left_child,
                    join_type.opposite(),
                    right_conditions,
                    left_conditions,
                )
            } else {
                (
                    left_child,
                    right_child,
                    join_type,
                    left_conditions,
                    right_conditions,
                )
            };
        let logical_join = Join {
            equi_conditions: JoinEquiCondition::new_conditions(
                left_conditions,
                right_conditions,
                is_null_equal,
            ),
            non_equi_conditions,
            join_type,
            marker_index: None,
            from_correlated_subquery: false,
            need_hold_hash_table: false,
            is_lateral,
            single_to_inner: None,
            build_side_cache_info,
        };
        Ok(SExpr::create_binary(
            Arc::new(logical_join.into()),
            Arc::new(left_child),
            Arc::new(right_child),
        ))
    }

    fn push_down_other_conditions(
        &self,
        join_type: &JoinType,
        left_child: &mut SExpr,
        right_child: &mut SExpr,
        other_conditions: Vec<ScalarExpr>,
        non_equi_conditions: &mut Vec<ScalarExpr>,
    ) -> Result<()> {
        if other_conditions.is_empty() {
            return Ok(());
        }
        let left_prop = RelExpr::with_s_expr(left_child).derive_relational_prop()?;
        let right_prop = RelExpr::with_s_expr(right_child).derive_relational_prop()?;

        let mut left_push_down = vec![];
        let mut right_push_down = vec![];
        let mut need_push_down = false;

        for predicate in other_conditions.iter() {
            let pred = JoinPredicate::new(predicate, &left_prop, &right_prop);
            match pred {
                JoinPredicate::ALL(_) => match join_type {
                    JoinType::Cross
                    | JoinType::Inner
                    | JoinType::Asof
                    | JoinType::LeftAsof
                    | JoinType::RightAsof
                    | JoinType::LeftSemi
                    | JoinType::LeftAnti
                    | JoinType::RightSemi
                    | JoinType::RightAnti => {
                        need_push_down = true;
                        left_push_down.push(predicate.clone());
                        right_push_down.push(predicate.clone());
                    }
                    JoinType::Left | JoinType::LeftSingle | JoinType::RightMark => {
                        need_push_down = true;
                        right_push_down.push(predicate.clone());
                    }
                    JoinType::Right | JoinType::RightSingle | JoinType::LeftMark => {
                        need_push_down = true;
                        left_push_down.push(predicate.clone());
                    }
                    JoinType::Full => non_equi_conditions.push(predicate.clone()),
                },
                JoinPredicate::Left(_) => {
                    need_push_down = true;
                    left_push_down.push(predicate.clone());
                }
                JoinPredicate::Right(_) => {
                    need_push_down = true;
                    right_push_down.push(predicate.clone());
                }
                _ => {
                    non_equi_conditions.push(predicate.clone());
                }
            }
        }

        if !need_push_down {
            return Ok(());
        }

        if !left_push_down.is_empty() {
            *left_child = SExpr::create_unary(
                Arc::new(
                    Filter {
                        predicates: left_push_down,
                    }
                    .into(),
                ),
                Arc::new(left_child.clone()),
            );
        }

        if !right_push_down.is_empty() {
            *right_child = SExpr::create_unary(
                Arc::new(
                    Filter {
                        predicates: right_push_down,
                    }
                    .into(),
                ),
                Arc::new(right_child.clone()),
            );
        }

        Ok(())
    }

    fn check_table_name_and_condition(
        &self,
        left_column_bindings: &[ColumnBinding],
        right_column_bindings: &[ColumnBinding],
        join_op: &JoinOperator,
        join_condition: &JoinCondition,
    ) -> Result<()> {
        check_duplicate_join_tables(left_column_bindings, right_column_bindings)?;

        match join_op {
            JoinOperator::LeftOuter
            | JoinOperator::RightOuter
            | JoinOperator::FullOuter
            | JoinOperator::LeftAsof
            | JoinOperator::RightAsof
                if join_condition == &JoinCondition::None =>
            {
                return Err(ErrorCode::SemanticError(
                    "outer join should contain join conditions".to_string(),
                ));
            }
            JoinOperator::CrossJoin if join_condition != &JoinCondition::None => {
                return Err(ErrorCode::SemanticError(
                    "cross join should not contain join conditions".to_string(),
                ));
            }
            JoinOperator::Asof if join_condition == &JoinCondition::None => {
                return Err(ErrorCode::SemanticError(
                    "asof join should contain join conditions".to_string(),
                ));
            }
            _ => (),
        };

        Ok(())
    }
}

// Wrap nullable for column binding depending on join type.
fn wrap_nullable_for_column(
    join_type: &JoinOperator,
    left_column_bindings: &[ColumnBinding],
    right_column_bindings: &[ColumnBinding],
    bind_context: &mut BindContext,
) {
    match join_type {
        JoinOperator::LeftOuter | JoinOperator::LeftAsof => {
            for column in left_column_bindings {
                bind_context.add_column_binding(column.clone());
            }
            for column in right_column_bindings {
                let mut nullable_column = column.clone();
                nullable_column.data_type = Box::new(column.data_type.wrap_nullable());
                bind_context.add_column_binding(nullable_column);
            }
        }
        JoinOperator::RightOuter | JoinOperator::RightAsof => {
            for column in left_column_bindings {
                let mut nullable_column = column.clone();
                nullable_column.data_type = Box::new(column.data_type.wrap_nullable());
                bind_context.add_column_binding(nullable_column);
            }

            for column in right_column_bindings {
                bind_context.add_column_binding(column.clone());
            }
        }
        JoinOperator::FullOuter => {
            for column in left_column_bindings {
                let mut nullable_column = column.clone();
                nullable_column.data_type = Box::new(column.data_type.wrap_nullable());
                bind_context.add_column_binding(nullable_column);
            }

            for column in right_column_bindings {
                let mut nullable_column = column.clone();
                nullable_column.data_type = Box::new(column.data_type.wrap_nullable());
                bind_context.add_column_binding(nullable_column);
            }
        }
        _ => {
            for column in left_column_bindings {
                bind_context.add_column_binding(column.clone());
            }
            for column in right_column_bindings {
                bind_context.add_column_binding(column.clone());
            }
        }
    }
}

pub fn check_duplicate_join_tables(
    left_column_bindings: &[ColumnBinding],
    right_column_bindings: &[ColumnBinding],
) -> Result<()> {
    let left_table_name = if left_column_bindings.is_empty() {
        None
    } else {
        left_column_bindings[0].table_name.as_ref()
    };

    let right_table_name = if right_column_bindings.is_empty() {
        None
    } else {
        right_column_bindings[0].table_name.as_ref()
    };

    if let Some(left) = left_table_name {
        if let Some(right) = right_table_name {
            if left.eq(right) {
                return Err(ErrorCode::SemanticError(format!(
                    "Duplicated table name {} in the same FROM clause",
                    left
                )));
            }
        }
    }
    Ok(())
}

struct JoinConditionResolver<'a> {
    ctx: Arc<dyn TableContext>,
    name_resolution_ctx: &'a NameResolutionContext,
    metadata: MetadataRef,
    m_cte_bound_ctx: HashMap<IndexType, BindContext>,
    ctes_map: Box<IndexMap<String, CteInfo>>,
    join_op: JoinOperator,
    left_column_bindings: &'a [ColumnBinding],
    right_column_bindings: &'a [ColumnBinding],
    join_context: &'a mut BindContext,
    join_condition: &'a JoinCondition,
}

impl<'a> JoinConditionResolver<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        ctx: Arc<dyn TableContext>,
        name_resolution_ctx: &'a NameResolutionContext,
        metadata: MetadataRef,
        m_cte_bound_ctx: HashMap<IndexType, BindContext>,
        ctes_map: Box<IndexMap<String, CteInfo>>,
        join_op: JoinOperator,
        left_column_bindings: &'a [ColumnBinding],
        right_column_bindings: &'a [ColumnBinding],
        join_context: &'a mut BindContext,
        join_condition: &'a JoinCondition,
    ) -> Self {
        Self {
            ctx,
            name_resolution_ctx,
            metadata,
            m_cte_bound_ctx,
            ctes_map,
            join_op,
            left_column_bindings,
            right_column_bindings,
            join_context,
            join_condition,
        }
    }

    pub fn resolve(
        &mut self,
        left_join_conditions: &mut Vec<ScalarExpr>,
        right_join_conditions: &mut Vec<ScalarExpr>,
        non_equi_conditions: &mut Vec<ScalarExpr>,
        other_join_conditions: &mut Vec<ScalarExpr>,
        join_op: &JoinOperator,
    ) -> Result<()> {
        match &self.join_condition {
            JoinCondition::On(cond) => {
                self.resolve_on(
                    cond,
                    left_join_conditions,
                    right_join_conditions,
                    non_equi_conditions,
                    other_join_conditions,
                )?;
            }
            JoinCondition::Using(identifiers) => {
                let using_columns = identifiers
                    .iter()
                    .map(|ident| {
                        (
                            ident.span,
                            normalize_identifier(ident, self.name_resolution_ctx).name,
                        )
                    })
                    .collect();
                self.resolve_using(
                    using_columns,
                    left_join_conditions,
                    right_join_conditions,
                    join_op,
                )?;
            }
            JoinCondition::Natural => {
                // NATURAL is a shorthand form of USING: it forms a USING list consisting of all column names that appear in both input tables
                // As with USING, these columns appear only once in the output table
                // Todo(xudong963) If there are no common column names, NATURAL JOIN behaves like JOIN ... ON TRUE, producing a cross-product join.
                let mut using_columns = vec![];
                // Find common columns in both input tables
                self.find_using_columns(&mut using_columns)?;
                self.resolve_using(
                    using_columns,
                    left_join_conditions,
                    right_join_conditions,
                    join_op,
                )?
            }
            JoinCondition::None => {
                wrap_nullable_for_column(
                    &self.join_op,
                    self.left_column_bindings,
                    self.right_column_bindings,
                    self.join_context,
                );
            }
        }

        self.check_join_allowed_scalar_expr(left_join_conditions)?;
        self.check_join_allowed_scalar_expr(right_join_conditions)?;
        self.check_join_allowed_scalar_expr(non_equi_conditions)?;
        self.check_join_allowed_scalar_expr(other_join_conditions)?;

        Ok(())
    }

    fn check_join_allowed_scalar_expr(&mut self, scalars: &Vec<ScalarExpr>) -> Result<()> {
        let f = |scalar: &ScalarExpr| {
            matches!(
                scalar,
                ScalarExpr::AggregateFunction(_)
                    | ScalarExpr::WindowFunction(_)
                    | ScalarExpr::AsyncFunctionCall(_)
            )
        };
        for scalar in scalars {
            let mut finder = Finder::new(&f);
            finder.visit(scalar)?;
            if !finder.scalars().is_empty() {
                return Err(ErrorCode::SemanticError(
                    "Join condition can't contain aggregate or window functions".to_string(),
                )
                .set_span(scalar.span()));
            }
        }
        Ok(())
    }

    fn resolve_on(
        &mut self,
        condition: &Expr,
        left_join_conditions: &mut Vec<ScalarExpr>,
        right_join_conditions: &mut Vec<ScalarExpr>,
        non_equi_conditions: &mut Vec<ScalarExpr>,
        other_join_conditions: &mut Vec<ScalarExpr>,
    ) -> Result<()> {
        let conjunctions = split_conjunctions_expr(condition);
        for expr in conjunctions.iter() {
            self.resolve_predicate(
                expr,
                left_join_conditions,
                right_join_conditions,
                non_equi_conditions,
                other_join_conditions,
            )?;
        }
        wrap_nullable_for_column(
            &self.join_op,
            self.left_column_bindings,
            self.right_column_bindings,
            self.join_context,
        );
        Ok(())
    }

    fn resolve_predicate(
        &self,
        predicate: &Expr,
        left_join_conditions: &mut Vec<ScalarExpr>,
        right_join_conditions: &mut Vec<ScalarExpr>,
        non_equi_conditions: &mut Vec<ScalarExpr>,
        other_join_conditions: &mut Vec<ScalarExpr>,
    ) -> Result<()> {
        let mut join_context = (*self.join_context).clone();
        wrap_nullable_for_column(
            &self.join_op,
            self.left_column_bindings,
            self.right_column_bindings,
            &mut join_context,
        );
        let mut scalar_binder = ScalarBinder::new(
            &mut join_context,
            self.ctx.clone(),
            self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
            self.m_cte_bound_ctx.clone(),
            self.ctes_map.clone(),
        );
        // Given two tables: t1(a, b), t2(a, b)
        // A predicate can be regarded as an equi-predicate iff:
        //
        //   - The predicate is literally an equivalence expression, e.g. `t1.a = t2.a`
        //   - Each side of `=` only contains columns from one table and the both sides are disjoint.
        //     For example, `t1.a + t1.b = t2.a` is a valid one while `t1.a + t2.a = t2.b` isn't.
        //
        // Only equi-predicate can be exploited by common join algorithms(e.g. sort-merge join, hash join).

        let mut added = if let Some((left, right)) = split_equivalent_predicate_expr(predicate) {
            let (left, _) = scalar_binder.bind(&left)?;
            let (right, _) = scalar_binder.bind(&right)?;
            self.add_equi_conditions(left, right, left_join_conditions, right_join_conditions)?
        } else {
            false
        };
        if !added {
            added = self.add_other_conditions(predicate, other_join_conditions)?;
            if !added {
                let (predicate, _) = scalar_binder.bind(predicate)?;
                non_equi_conditions.push(predicate);
            }
        }
        Ok(())
    }

    fn resolve_using(
        &mut self,
        using_columns: Vec<(Span, String)>,
        left_join_conditions: &mut Vec<ScalarExpr>,
        right_join_conditions: &mut Vec<ScalarExpr>,
        join_op: &JoinOperator,
    ) -> Result<()> {
        wrap_nullable_for_column(
            &self.join_op,
            self.left_column_bindings,
            self.right_column_bindings,
            self.join_context,
        );
        let left_columns_len = self.left_column_bindings.len();
        for (span, join_key) in using_columns.iter() {
            let join_key_name = join_key.as_str();
            let left_scalar = if let Some(col_binding) = self.join_context.columns
                [0..left_columns_len]
                .iter()
                .find(|col_binding| col_binding.column_name == join_key_name)
            {
                ScalarExpr::BoundColumnRef(BoundColumnRef {
                    span: *span,
                    column: col_binding.clone(),
                })
            } else {
                return Err(ErrorCode::SemanticError(format!(
                    "column {} specified in USING clause does not exist in left table",
                    join_key_name
                ))
                .set_span(*span));
            };

            let right_scalar = if let Some(col_binding) = self.join_context.columns
                [left_columns_len..]
                .iter()
                .find(|col_binding| col_binding.column_name == join_key_name)
            {
                ScalarExpr::BoundColumnRef(BoundColumnRef {
                    span: *span,
                    column: col_binding.clone(),
                })
            } else {
                return Err(ErrorCode::SemanticError(format!(
                    "column {} specified in USING clause does not exist in right table",
                    join_key_name
                ))
                .set_span(*span));
            };
            let idx = !matches!(join_op, JoinOperator::RightOuter) as usize;
            if let Some(col_binding) = self
                .join_context
                .columns
                .iter_mut()
                .filter(|col_binding| {
                    col_binding.column_name == join_key_name
                        && col_binding.visibility != Visibility::UnqualifiedWildcardInVisible
                })
                .nth(idx)
            {
                // Always make the second using column in the join_context invisible in unqualified wildcard.
                col_binding.visibility = Visibility::UnqualifiedWildcardInVisible;
            }

            self.add_equi_conditions(
                left_scalar,
                right_scalar,
                left_join_conditions,
                right_join_conditions,
            )?;
        }
        Ok(())
    }

    fn add_equi_conditions(
        &self,
        left: ScalarExpr,
        right: ScalarExpr,
        left_join_conditions: &mut Vec<ScalarExpr>,
        right_join_conditions: &mut Vec<ScalarExpr>,
    ) -> Result<bool> {
        let left_used_columns = left.used_columns();
        let right_used_columns = right.used_columns();
        let (left_columns, right_columns) = self.left_right_columns()?;

        if !left_used_columns.is_empty() && !right_used_columns.is_empty() {
            if left_used_columns.is_subset(&left_columns)
                && right_used_columns.is_subset(&right_columns)
            {
                left_join_conditions.push(left);
                right_join_conditions.push(right);
                return Ok(true);
            } else if left_used_columns.is_subset(&right_columns)
                && right_used_columns.is_subset(&left_columns)
            {
                left_join_conditions.push(right);
                right_join_conditions.push(left);
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn add_other_conditions(
        &self,
        predicate: &Expr,
        other_join_conditions: &mut Vec<ScalarExpr>,
    ) -> Result<bool> {
        let mut join_context = (*self.join_context).clone();
        wrap_nullable_for_column(
            &JoinOperator::Inner,
            self.left_column_bindings,
            self.right_column_bindings,
            &mut join_context,
        );
        let mut scalar_binder = ScalarBinder::new(
            &mut join_context,
            self.ctx.clone(),
            self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
            self.m_cte_bound_ctx.clone(),
            self.ctes_map.clone(),
        );
        let (predicate, _) = scalar_binder.bind(predicate)?;
        let predicate_used_columns = predicate.used_columns();
        let (left_columns, right_columns) = self.left_right_columns()?;
        match self.join_op {
            JoinOperator::LeftOuter => {
                if predicate_used_columns.is_subset(&right_columns) {
                    other_join_conditions.push(predicate);
                    return Ok(true);
                }
            }
            JoinOperator::RightOuter => {
                if predicate_used_columns.is_subset(&left_columns) {
                    other_join_conditions.push(predicate);
                    return Ok(true);
                }
            }
            JoinOperator::Inner => {
                if predicate_used_columns.is_subset(&left_columns)
                    || predicate_used_columns.is_subset(&right_columns)
                {
                    other_join_conditions.push(predicate);
                    return Ok(true);
                }
            }
            _ => {
                return Ok(false);
            }
        }
        Ok(false)
    }

    fn left_right_columns(&self) -> Result<(ColumnSet, ColumnSet)> {
        let left_columns: ColumnSet =
            self.left_column_bindings
                .iter()
                .fold(ColumnSet::new(), |mut acc, v| {
                    acc.insert(v.index);
                    acc
                });
        let right_columns: ColumnSet =
            self.right_column_bindings
                .iter()
                .fold(ColumnSet::new(), |mut acc, v| {
                    acc.insert(v.index);
                    acc
                });
        Ok((left_columns, right_columns))
    }

    fn find_using_columns(&self, using_columns: &mut Vec<(Span, String)>) -> Result<()> {
        for left_column in self.left_column_bindings {
            for right_column in self.right_column_bindings {
                if left_column.column_name == right_column.column_name {
                    using_columns.push((None, left_column.column_name.clone()));
                }
            }
        }
        Ok(())
    }
}

fn join_type(join_type: &JoinOperator) -> JoinType {
    match join_type {
        JoinOperator::CrossJoin => JoinType::Cross,
        JoinOperator::Inner => JoinType::Inner,
        JoinOperator::LeftOuter => JoinType::Left,
        JoinOperator::RightOuter => JoinType::Right,
        JoinOperator::FullOuter => JoinType::Full,
        JoinOperator::LeftSemi => JoinType::LeftSemi,
        JoinOperator::RightSemi => JoinType::RightSemi,
        JoinOperator::LeftAnti => JoinType::LeftAnti,
        JoinOperator::RightAnti => JoinType::RightAnti,
        JoinOperator::Asof => JoinType::Asof,
        JoinOperator::LeftAsof => JoinType::LeftAsof,
        JoinOperator::RightAsof => JoinType::RightAsof,
    }
}

fn join_bind_context(
    join_type: &JoinType,
    bind_context: BindContext,
    left_context: BindContext,
    right_context: BindContext,
) -> BindContext {
    match join_type {
        JoinType::LeftSemi | JoinType::LeftAnti => left_context,
        JoinType::RightSemi | JoinType::RightAnti => right_context,
        _ => bind_context,
    }
}
