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

use async_recursion::async_recursion;
use databend_common_ast::ast::BinaryOperator;
use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::ColumnPosition;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Expr::Array;
use databend_common_ast::ast::FunctionCall;
use databend_common_ast::ast::GroupBy;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Join;
use databend_common_ast::ast::JoinCondition;
use databend_common_ast::ast::JoinOperator;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::OrderByExpr;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::SelectStmt;
use databend_common_ast::ast::SelectTarget;
use databend_common_ast::ast::SetExpr;
use databend_common_ast::ast::SetOperator;
use databend_common_ast::ast::TableReference;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::Span;
use databend_common_expression::type_check::common_super_type;
use databend_common_expression::types::DataType;
use databend_common_expression::ROW_ID_COLUMN_ID;
use databend_common_expression::ROW_ID_COL_NAME;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::get_license_manager;
use derive_visitor::Drive;
use derive_visitor::Visitor;
use log::warn;

use super::sort::OrderItem;
use super::Finder;
use crate::binder::join::JoinConditions;
use crate::binder::project_set::SrfCollector;
use crate::binder::scalar_common::split_conjunctions;
use crate::binder::ColumnBindingBuilder;
use crate::binder::CteInfo;
use crate::binder::ExprContext;
use crate::binder::INTERNAL_COLUMN_FACTORY;
use crate::normalize_identifier;
use crate::optimizer::SExpr;
use crate::planner::binder::scalar::ScalarBinder;
use crate::planner::binder::BindContext;
use crate::planner::binder::Binder;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::EvalScalar;
use crate::plans::Filter;
use crate::plans::JoinType;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::UnionAll;
use crate::plans::Visitor as _;
use crate::ColumnBinding;
use crate::ColumnEntry;
use crate::IndexType;
use crate::UdfRewriter;
use crate::VirtualColumnRewriter;
use crate::Visibility;

// A normalized IR for `SELECT` clause.
#[derive(Debug, Default)]
pub struct SelectList<'a> {
    pub items: Vec<SelectItem<'a>>,
}

#[derive(Debug)]
pub struct SelectItem<'a> {
    pub select_target: &'a SelectTarget,
    pub scalar: ScalarExpr,
    pub alias: String,
}

impl Binder {
    #[async_backtrace::framed]
    pub(crate) async fn bind_select_stmt(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &SelectStmt,
        order_by: &[OrderByExpr],
        limit: usize,
    ) -> Result<(SExpr, BindContext)> {
        if let Some(hints) = &stmt.hints {
            if let Some(e) = self.opt_hints_set_var(bind_context, hints).await.err() {
                warn!(
                    "In SELECT resolve optimize hints {:?} failed, err: {:?}",
                    hints, e
                );
            }
        }
        let (mut s_expr, mut from_context) = if stmt.from.is_empty() {
            let select_list = &stmt.select_list;
            self.bind_one_table(bind_context, select_list).await?
        } else {
            let mut max_column_position = MaxColumnPosition::new();
            stmt.drive(&mut max_column_position);
            self.metadata
                .write()
                .set_max_column_position(max_column_position.max_pos);

            let cross_joins = stmt
                .from
                .iter()
                .cloned()
                .reduce(|left, right| TableReference::Join {
                    span: None,
                    join: Join {
                        op: JoinOperator::CrossJoin,
                        condition: JoinCondition::None,
                        left: Box::new(left),
                        right: Box::new(right),
                    },
                })
                .unwrap();
            self.bind_table_reference(bind_context, &cross_joins)
                .await?
        };

        let mut rewriter = SelectRewriter::new(
            from_context.all_column_bindings(),
            self.name_resolution_ctx.unquoted_ident_case_sensitive,
        );
        let new_stmt = rewriter.rewrite(stmt)?;
        let stmt = new_stmt.as_ref().unwrap_or(stmt);

        // Collect set returning functions
        let set_returning_functions = {
            let mut collector = SrfCollector::new();
            stmt.select_list.iter().for_each(|item| {
                if let SelectTarget::AliasedExpr { expr, .. } = item {
                    collector.visit(expr);
                }
            });
            collector.into_srfs()
        };

        // Bind set returning functions
        s_expr = self
            .bind_project_set(&mut from_context, &set_returning_functions, s_expr)
            .await?;

        // Try put window definitions into bind context.
        // This operation should be before `normalize_select_list` because window functions can be used in select list.
        self.analyze_window_definition(&mut from_context, &stmt.window_list)?;

        // Generate a analyzed select list with from context
        let mut select_list = self
            .normalize_select_list(&mut from_context, &stmt.select_list)
            .await?;

        // This will potentially add some alias group items to `from_context` if find some.
        if let Some(group_by) = stmt.group_by.as_ref() {
            self.analyze_group_items(&mut from_context, &select_list, group_by)
                .await?;
        }

        self.analyze_aggregate_select(&mut from_context, &mut select_list)?;

        // `analyze_window` should behind `analyze_aggregate_select`,
        // because `analyze_window` will rewrite the aggregate functions in the window function's arguments.
        self.analyze_window(&mut from_context, &mut select_list)?;

        let aliases = select_list
            .items
            .iter()
            .map(|item| (item.alias.clone(), item.scalar.clone()))
            .collect::<Vec<_>>();

        // To support using aliased column in `WHERE` clause,
        // we should bind where after `select_list` is rewritten.
        let where_scalar = if let Some(expr) = &stmt.selection {
            let (new_expr, scalar) = self
                .bind_where(&mut from_context, &aliases, expr, s_expr)
                .await?;
            s_expr = new_expr;
            Some(scalar)
        } else {
            None
        };

        // `analyze_projection` should behind `analyze_aggregate_select` because `analyze_aggregate_select` will rewrite `grouping`.
        let (mut scalar_items, projections) = self.analyze_projection(
            &from_context.aggregate_info,
            &from_context.windows,
            &select_list,
        )?;

        let having = if let Some(having) = &stmt.having {
            Some(
                self.analyze_aggregate_having(&mut from_context, &aliases, having)
                    .await?,
            )
        } else {
            None
        };

        let qualify = if let Some(qualify) = &stmt.qualify {
            Some(
                self.analyze_window_qualify(&mut from_context, &aliases, qualify)
                    .await?,
            )
        } else {
            None
        };

        let order_items = self
            .analyze_order_items(
                &mut from_context,
                &mut scalar_items,
                &aliases,
                &projections,
                order_by,
                stmt.distinct,
            )
            .await?;

        // After all analysis is done.
        if set_returning_functions.is_empty() {
            // Ignore SRFs.
            self.analyze_lazy_materialization(
                &from_context,
                stmt,
                &scalar_items,
                &select_list,
                &where_scalar,
                &order_items.items,
                limit,
            )?;
        }

        if !from_context.aggregate_info.aggregate_functions.is_empty()
            || !from_context.aggregate_info.group_items.is_empty()
        {
            s_expr = self.bind_aggregate(&mut from_context, s_expr).await?;
        }

        if let Some(having) = having {
            s_expr = self.bind_having(&mut from_context, having, s_expr).await?;
        }

        // bind window
        // window run after the HAVING clause but before the ORDER BY clause.
        for window_info in &from_context.windows.window_functions {
            s_expr = self.bind_window_function(window_info, s_expr).await?;
        }

        if let Some(qualify) = qualify {
            s_expr = self
                .bind_qualify(&mut from_context, qualify, s_expr)
                .await?;
        }

        if stmt.distinct {
            s_expr = self.bind_distinct(
                stmt.span,
                &from_context,
                &projections,
                &mut scalar_items,
                s_expr,
            )?;
        }

        if !order_by.is_empty() {
            s_expr = self
                .bind_order_by(
                    &from_context,
                    order_items,
                    &select_list,
                    &mut scalar_items,
                    s_expr,
                )
                .await?;
        }

        s_expr = self.bind_projection(&mut from_context, &projections, &scalar_items, s_expr)?;

        // rewrite udf for interpreter udf
        let mut udf_rewriter = UdfRewriter::new(self.metadata.clone(), true);
        s_expr = udf_rewriter.rewrite(&s_expr)?;

        // rewrite udf for server udf
        let mut udf_rewriter = UdfRewriter::new(self.metadata.clone(), false);
        s_expr = udf_rewriter.rewrite(&s_expr)?;

        // rewrite variant inner fields as virtual columns
        let mut virtual_column_rewriter =
            VirtualColumnRewriter::new(self.ctx.clone(), self.metadata.clone());
        s_expr = virtual_column_rewriter.rewrite(&s_expr).await?;

        // check inverted index license
        if !from_context.inverted_index_map.is_empty() {
            let license_manager = get_license_manager();
            license_manager
                .manager
                .check_enterprise_enabled(self.ctx.get_license_key(), Feature::InvertedIndex)?;
        }
        // add internal column binding into expr
        s_expr = from_context.add_internal_column_into_expr(s_expr);

        let mut output_context = BindContext::new();
        output_context.parent = from_context.parent;
        output_context.columns = from_context.columns;

        Ok((s_expr, output_context))
    }

    #[async_recursion]
    #[async_backtrace::framed]
    pub(crate) async fn bind_set_expr(
        &mut self,
        bind_context: &mut BindContext,
        set_expr: &SetExpr,
        order_by: &[OrderByExpr],
        limit: usize,
    ) -> Result<(SExpr, BindContext)> {
        match set_expr {
            SetExpr::Select(stmt) => {
                self.bind_select_stmt(bind_context, stmt, order_by, limit)
                    .await
            }
            SetExpr::Query(stmt) => self.bind_query(bind_context, stmt).await,
            SetExpr::SetOperation(set_operation) => {
                self.bind_set_operator(
                    bind_context,
                    &set_operation.left,
                    &set_operation.right,
                    &set_operation.op,
                    &set_operation.all,
                )
                .await
            }
            SetExpr::Values { span, values } => self.bind_values(bind_context, *span, values).await,
        }
    }

    #[async_recursion]
    #[async_backtrace::framed]
    pub(crate) async fn bind_query(
        &mut self,
        bind_context: &mut BindContext,
        query: &Query,
    ) -> Result<(SExpr, BindContext)> {
        if let Some(with) = &query.with {
            for (idx, cte) in with.ctes.iter().enumerate() {
                let table_name =
                    normalize_identifier(&cte.alias.name, &self.name_resolution_ctx).name;
                if bind_context.cte_map_ref.contains_key(&table_name) {
                    return Err(ErrorCode::SemanticError(format!(
                        "duplicate cte {table_name}"
                    )));
                }
                let cte_info = CteInfo {
                    columns_alias: cte
                        .alias
                        .columns
                        .iter()
                        .map(|c| normalize_identifier(c, &self.name_resolution_ctx).name)
                        .collect(),
                    query: *cte.query.clone(),
                    materialized: cte.materialized,
                    cte_idx: idx,
                    used_count: 0,
                    columns: vec![],
                };
                self.ctes_map.insert(table_name.clone(), cte_info.clone());
                bind_context.cte_map_ref.insert(table_name, cte_info);
            }
        }

        let (limit, offset) = if !query.limit.is_empty() {
            if query.limit.len() == 1 {
                Self::analyze_limit(Some(&query.limit[0]), &query.offset)?
            } else {
                Self::analyze_limit(Some(&query.limit[1]), &Some(query.limit[0].clone()))?
            }
        } else if query.offset.is_some() {
            Self::analyze_limit(None, &query.offset)?
        } else {
            (None, 0)
        };

        let (mut s_expr, bind_context) = match query.body {
            SetExpr::Select(_) | SetExpr::Query(_) => {
                self.bind_set_expr(
                    bind_context,
                    &query.body,
                    &query.order_by,
                    limit.unwrap_or_default(),
                )
                .await?
            }
            SetExpr::SetOperation(_) | SetExpr::Values { .. } => {
                let (mut s_expr, mut bind_context) = self
                    .bind_set_expr(bind_context, &query.body, &[], limit.unwrap_or_default())
                    .await?;
                if !query.order_by.is_empty() {
                    s_expr = self
                        .bind_order_by_for_set_operation(&mut bind_context, s_expr, &query.order_by)
                        .await?;
                }
                (s_expr, bind_context)
            }
        };

        if !query.limit.is_empty() || query.offset.is_some() {
            s_expr = Self::bind_limit(s_expr, limit, offset);
        }

        Ok((s_expr, bind_context))
    }

    #[async_backtrace::framed]
    pub async fn bind_where(
        &mut self,
        bind_context: &mut BindContext,
        aliases: &[(String, ScalarExpr)],
        expr: &Expr,
        child: SExpr,
    ) -> Result<(SExpr, ScalarExpr)> {
        let last_expr_context = bind_context.expr_context.clone();
        bind_context.set_expr_context(ExprContext::WhereClause);

        let mut scalar_binder = ScalarBinder::new(
            bind_context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            aliases,
            self.m_cte_bound_ctx.clone(),
            self.ctes_map.clone(),
        );
        let (scalar, _) = scalar_binder.bind(expr).await?;

        let f = |scalar: &ScalarExpr| {
            matches!(
                scalar,
                ScalarExpr::AggregateFunction(_) | ScalarExpr::WindowFunction(_)
            )
        };

        let mut finder = Finder::new(&f);
        finder.visit(&scalar)?;
        if !finder.scalars().is_empty() {
            return Err(ErrorCode::SemanticError(
                "Where clause can't contain aggregate or window functions".to_string(),
            )
            .set_span(scalar.span()));
        }

        let filter_plan = Filter {
            predicates: split_conjunctions(&scalar),
        };
        let new_expr = SExpr::create_unary(Arc::new(filter_plan.into()), Arc::new(child));
        bind_context.set_expr_context(last_expr_context);
        Ok((new_expr, scalar))
    }

    #[async_backtrace::framed]
    pub(super) async fn bind_set_operator(
        &mut self,
        bind_context: &mut BindContext,
        left: &SetExpr,
        right: &SetExpr,
        op: &SetOperator,
        all: &bool,
    ) -> Result<(SExpr, BindContext)> {
        let (left_expr, left_bind_context) = self.bind_set_expr(bind_context, left, &[], 0).await?;
        let (right_expr, right_bind_context) =
            self.bind_set_expr(bind_context, right, &[], 0).await?;

        if left_bind_context.columns.len() != right_bind_context.columns.len() {
            return Err(ErrorCode::SemanticError(
                "SetOperation must have the same number of columns",
            ));
        }

        match (op, all) {
            (SetOperator::Intersect, false) => {
                // Transfer Intersect to Semi join
                self.bind_intersect(
                    left.span(),
                    right.span(),
                    left_bind_context,
                    right_bind_context,
                    left_expr,
                    right_expr,
                )
            }
            (SetOperator::Except, false) => {
                // Transfer Except to Anti join
                self.bind_except(
                    left.span(),
                    right.span(),
                    left_bind_context,
                    right_bind_context,
                    left_expr,
                    right_expr,
                )
            }
            (SetOperator::Union, true) => self.bind_union(
                left.span(),
                right.span(),
                left_bind_context,
                right_bind_context,
                left_expr,
                right_expr,
                false,
            ),
            (SetOperator::Union, false) => self.bind_union(
                left.span(),
                right.span(),
                left_bind_context,
                right_bind_context,
                left_expr,
                right_expr,
                true,
            ),
            _ => Err(ErrorCode::Unimplemented(
                "Unsupported query type, currently, databend only support intersect distinct and except distinct",
            )),
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub fn bind_union(
        &mut self,
        left_span: Span,
        right_span: Span,
        left_context: BindContext,
        right_context: BindContext,
        left_expr: SExpr,
        right_expr: SExpr,
        distinct: bool,
    ) -> Result<(SExpr, BindContext)> {
        let mut coercion_types = Vec::with_capacity(left_context.columns.len());
        for (left_col, right_col) in left_context
            .columns
            .iter()
            .zip(right_context.columns.iter())
        {
            if left_col.data_type != right_col.data_type {
                if let Some(data_type) = common_super_type(
                    *left_col.data_type.clone(),
                    *right_col.data_type.clone(),
                    &BUILTIN_FUNCTIONS.default_cast_rules,
                ) {
                    coercion_types.push(data_type);
                } else {
                    return Err(ErrorCode::SemanticError(format!(
                        "SetOperation's types cannot be matched, left column {:?}, type: {:?}, right column {:?}, type: {:?}",
                        left_col.column_name,
                        left_col.data_type,
                        right_col.column_name,
                        right_col.data_type
                    )));
                }
            } else {
                coercion_types.push(*left_col.data_type.clone());
            }
        }
        let (new_bind_context, pairs, left_expr, right_expr) = self.coercion_union_type(
            left_span,
            right_span,
            left_context,
            right_context,
            left_expr,
            right_expr,
            coercion_types,
        )?;

        let union_plan = UnionAll { pairs };
        let mut new_expr = SExpr::create_binary(
            Arc::new(union_plan.into()),
            Arc::new(left_expr),
            Arc::new(right_expr),
        );

        if distinct {
            new_expr = self.bind_distinct(
                left_span,
                &new_bind_context,
                new_bind_context.all_column_bindings(),
                &mut HashMap::new(),
                new_expr,
            )?;
        }

        Ok((new_expr, new_bind_context))
    }

    pub fn bind_intersect(
        &mut self,
        left_span: Span,
        right_span: Span,
        left_context: BindContext,
        right_context: BindContext,
        left_expr: SExpr,
        right_expr: SExpr,
    ) -> Result<(SExpr, BindContext)> {
        self.bind_intersect_or_except(
            left_span,
            right_span,
            left_context,
            right_context,
            left_expr,
            right_expr,
            JoinType::LeftSemi,
        )
    }

    pub fn bind_except(
        &mut self,
        left_span: Span,
        right_span: Span,
        left_context: BindContext,
        right_context: BindContext,
        left_expr: SExpr,
        right_expr: SExpr,
    ) -> Result<(SExpr, BindContext)> {
        self.bind_intersect_or_except(
            left_span,
            right_span,
            left_context,
            right_context,
            left_expr,
            right_expr,
            JoinType::LeftAnti,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn bind_intersect_or_except(
        &mut self,
        left_span: Span,
        right_span: Span,
        left_context: BindContext,
        right_context: BindContext,
        left_expr: SExpr,
        right_expr: SExpr,
        join_type: JoinType,
    ) -> Result<(SExpr, BindContext)> {
        let left_expr = self.bind_distinct(
            left_span,
            &left_context,
            left_context.all_column_bindings(),
            &mut HashMap::new(),
            left_expr,
        )?;
        let mut left_conditions = Vec::with_capacity(left_context.columns.len());
        let mut right_conditions = Vec::with_capacity(right_context.columns.len());
        assert_eq!(left_context.columns.len(), right_context.columns.len());
        for (left_column, right_column) in left_context
            .columns
            .iter()
            .zip(right_context.columns.iter())
        {
            left_conditions.push(
                BoundColumnRef {
                    span: left_span,
                    column: left_column.clone(),
                }
                .into(),
            );
            right_conditions.push(
                BoundColumnRef {
                    span: right_span,
                    column: right_column.clone(),
                }
                .into(),
            );
        }
        let join_conditions = JoinConditions {
            left_conditions,
            right_conditions,
            non_equi_conditions: vec![],
            other_conditions: vec![],
        };
        let s_expr = self.bind_join_with_type(join_type, join_conditions, left_expr, right_expr)?;
        Ok((s_expr, left_context))
    }

    #[allow(clippy::type_complexity)]
    #[allow(clippy::too_many_arguments)]
    fn coercion_union_type(
        &self,
        left_span: Span,
        right_span: Span,
        left_bind_context: BindContext,
        right_bind_context: BindContext,
        mut left_expr: SExpr,
        mut right_expr: SExpr,
        coercion_types: Vec<DataType>,
    ) -> Result<(BindContext, Vec<(IndexType, IndexType)>, SExpr, SExpr)> {
        let mut left_scalar_items = Vec::with_capacity(left_bind_context.columns.len());
        let mut right_scalar_items = Vec::with_capacity(right_bind_context.columns.len());
        let mut new_bind_context = BindContext::new();
        let mut pairs = Vec::with_capacity(left_bind_context.columns.len());
        for (idx, (left_col, right_col)) in left_bind_context
            .columns
            .iter()
            .zip(right_bind_context.columns.iter())
            .enumerate()
        {
            let left_index = if *left_col.data_type != coercion_types[idx] {
                let new_column_index = self
                    .metadata
                    .write()
                    .add_derived_column(left_col.column_name.clone(), coercion_types[idx].clone());
                let column_binding = ColumnBindingBuilder::new(
                    left_col.column_name.clone(),
                    new_column_index,
                    Box::new(coercion_types[idx].clone()),
                    Visibility::Visible,
                )
                .build();
                let left_coercion_expr = CastExpr {
                    span: left_span,
                    is_try: false,
                    argument: Box::new(
                        BoundColumnRef {
                            span: left_span,
                            column: left_col.clone(),
                        }
                        .into(),
                    ),
                    target_type: Box::new(coercion_types[idx].clone()),
                };
                left_scalar_items.push(ScalarItem {
                    scalar: left_coercion_expr.into(),
                    index: new_column_index,
                });
                new_bind_context.add_column_binding(column_binding);
                new_column_index
            } else {
                new_bind_context.add_column_binding(left_col.clone());
                left_col.index
            };
            let right_index = if *right_col.data_type != coercion_types[idx] {
                let new_column_index = self
                    .metadata
                    .write()
                    .add_derived_column(right_col.column_name.clone(), coercion_types[idx].clone());
                let right_coercion_expr = CastExpr {
                    span: right_span,
                    is_try: false,
                    argument: Box::new(
                        BoundColumnRef {
                            span: right_span,
                            column: right_col.clone(),
                        }
                        .into(),
                    ),
                    target_type: Box::new(coercion_types[idx].clone()),
                };
                right_scalar_items.push(ScalarItem {
                    scalar: right_coercion_expr.into(),
                    index: new_column_index,
                });
                new_column_index
            } else {
                right_col.index
            };
            pairs.push((left_index, right_index));
        }
        if !left_scalar_items.is_empty() {
            left_expr = SExpr::create_unary(
                Arc::new(
                    EvalScalar {
                        items: left_scalar_items,
                    }
                    .into(),
                ),
                Arc::new(left_expr),
            );
        }
        if !right_scalar_items.is_empty() {
            right_expr = SExpr::create_unary(
                Arc::new(
                    EvalScalar {
                        items: right_scalar_items,
                    }
                    .into(),
                ),
                Arc::new(right_expr),
            );
        }
        Ok((new_bind_context, pairs, left_expr, right_expr))
    }

    #[allow(clippy::too_many_arguments)]
    fn analyze_lazy_materialization(
        &self,
        bind_context: &BindContext,
        stmt: &SelectStmt,
        scalar_items: &HashMap<IndexType, ScalarItem>,
        select_list: &SelectList,
        where_scalar: &Option<ScalarExpr>,
        order_by: &[OrderItem],
        limit: usize,
    ) -> Result<()> {
        // Only simple single table queries with limit are supported.
        // e.g.
        // SELECT ... FROM t WHERE ... LIMIT ...
        // SELECT ... FROM t WHERE ... ORDER BY ... LIMIT ...
        if stmt.group_by.is_some()
            || stmt.having.is_some()
            || stmt.distinct
            || !bind_context.aggregate_info.group_items.is_empty()
            || !bind_context.aggregate_info.aggregate_functions.is_empty()
        {
            return Ok(());
        }

        let mut metadata = self.metadata.write();
        if metadata.tables().len() != 1 {
            // Only support single table query.
            return Ok(());
        }

        // As we don't if this is subquery, we need add required cols to metadata's non_lazy_columns,
        // so if the inner query not match the lazy materialized but outer query matched, we can prevent
        // the cols that inner query required not be pruned when analyze outer query.
        {
            let mut non_lazy_cols = HashSet::new();
            for s in select_list.items.iter() {
                // The TableScan's schema uses name_mapping to prune columns,
                // all lazy columns will be skipped to add to name_mapping in TableScan.
                // When build physical window plan, if window's order by or partition by provided,
                // we need create a `EvalScalar` for physical window inputs, so we should keep the window
                // used cols not be pruned.
                if let ScalarExpr::WindowFunction(_) = &s.scalar {
                    non_lazy_cols.extend(s.scalar.used_columns())
                }
            }
            metadata.add_non_lazy_columns(non_lazy_cols);
        }

        let limit_threadhold = self.ctx.get_settings().get_lazy_read_threshold()? as usize;

        let where_cols = where_scalar
            .as_ref()
            .map(|w| w.used_columns())
            .unwrap_or_default();

        if limit == 0 || limit > limit_threadhold || (order_by.is_empty() && where_cols.is_empty())
        {
            return Ok(());
        }

        if !metadata
            .table(0)
            .table()
            .supported_internal_column(ROW_ID_COLUMN_ID)
        {
            return Ok(());
        }

        let cols = metadata.columns();

        let virtual_cols = cols
            .iter()
            .filter(|col| matches!(col, ColumnEntry::VirtualColumn(_)))
            .map(|col| col.index())
            .collect::<Vec<_>>();

        if !virtual_cols.is_empty() {
            // Virtual columns are not supported now.
            return Ok(());
        }

        let mut order_by_cols = HashSet::with_capacity(order_by.len());
        for o in order_by {
            if let Some(scalar) = scalar_items.get(&o.index) {
                let cols = scalar.scalar.used_columns();
                order_by_cols.extend(cols);
            } else {
                // Is a col ref not appears in select list.
                order_by_cols.insert(o.index);
            }
        }

        let mut non_lazy_cols = order_by_cols;
        non_lazy_cols.extend(where_cols);

        let mut select_cols = HashSet::with_capacity(select_list.items.len());
        for s in select_list.items.iter() {
            if let ScalarExpr::WindowFunction(_) = &s.scalar {
                continue;
            } else {
                select_cols.extend(s.scalar.used_columns())
            }
        }

        // If there are derived columns, we can't use lazy materialization.
        // (As the derived columns may come from a CTE, the rows fetcher can't know where to fetch the data.)
        if select_cols
            .iter()
            .any(|col| matches!(metadata.column(*col), ColumnEntry::DerivedColumn(_)))
        {
            return Ok(());
        }

        let internal_cols = cols
            .iter()
            .filter(|col| matches!(col, ColumnEntry::InternalColumn(_)))
            .map(|col| col.index())
            .collect::<HashSet<_>>();

        // add internal_cols to non_lazy_cols
        non_lazy_cols.extend(internal_cols);

        // add previous(subquery) stored non_lazy_columns to non_lazy_cols
        non_lazy_cols.extend(metadata.non_lazy_columns());

        let lazy_cols = select_cols.difference(&non_lazy_cols).copied().collect();
        metadata.add_lazy_columns(lazy_cols);

        // Single table, the table index is 0.
        let table_index = 0;
        if metadata.row_id_index_by_table_index(table_index).is_none() {
            let internal_column = INTERNAL_COLUMN_FACTORY
                .get_internal_column(ROW_ID_COL_NAME)
                .unwrap();
            let index = metadata.add_internal_column(table_index, internal_column);
            metadata.set_table_row_id_index(table_index, index);
        }

        Ok(())
    }
}

/// It is useful when implementing some SQL syntax sugar,
///
/// [`column_binding`] contains the column binding information of the SelectStmt.
///
/// to rewrite the SelectStmt, just add a new rewrite_* function and call it in the `rewrite` function.
struct SelectRewriter<'a> {
    column_binding: &'a [ColumnBinding],
    new_stmt: Option<SelectStmt>,
    is_unquoted_ident_case_sensitive: bool,
}

// helper functions to SelectRewriter
impl<'a> SelectRewriter<'a> {
    fn compare_unquoted_ident(&self, a: &str, b: &str) -> bool {
        if self.is_unquoted_ident_case_sensitive {
            a == b
        } else {
            a.eq_ignore_ascii_case(b)
        }
    }

    fn parse_aggregate_function(expr: &Expr) -> Result<(&Identifier, &[Expr])> {
        match expr {
            Expr::FunctionCall {
                func: FunctionCall { name, args, .. },
                ..
            } => Ok((name, args)),
            _ => Err(ErrorCode::SyntaxException("Aggregate function is required")),
        }
    }

    fn expr_eq_from_col_and_value(col: Identifier, value: Expr) -> Expr {
        Expr::BinaryOp {
            span: None,
            left: Box::new(Expr::ColumnRef {
                span: None,
                column: ColumnRef {
                    database: None,
                    table: None,
                    column: ColumnID::Name(col),
                },
            }),
            op: BinaryOperator::Eq,
            right: Box::new(value),
        }
    }

    fn target_func_from_name_args(
        name: Identifier,
        args: Vec<Expr>,
        alias: Option<Identifier>,
    ) -> SelectTarget {
        SelectTarget::AliasedExpr {
            expr: Box::new(Expr::FunctionCall {
                span: Span::default(),
                func: FunctionCall {
                    distinct: false,
                    name,
                    args,
                    params: vec![],
                    window: None,
                    lambda: None,
                },
            }),
            alias,
        }
    }

    fn expr_literal_array_from_vec_ident(exprs: Vec<Identifier>) -> Expr {
        Array {
            span: Span::default(),
            exprs: exprs
                .into_iter()
                .map(|expr| Expr::Literal {
                    span: None,
                    value: Literal::String(expr.name),
                })
                .collect(),
        }
    }

    fn expr_column_ref_array_from_vec_ident(exprs: Vec<Identifier>) -> Expr {
        Array {
            span: Span::default(),
            exprs: exprs
                .into_iter()
                .map(|expr| Expr::ColumnRef {
                    span: None,
                    column: ColumnRef {
                        database: None,
                        table: None,
                        column: ColumnID::Name(expr),
                    },
                })
                .collect(),
        }
    }

    // For Expr::Literal, expr.to_string() is quoted, sometimes we need the raw string.
    fn raw_string_from_literal_expr(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Literal { value, .. } => match value {
                Literal::String(v) => Some(v.clone()),
                _ => Some(expr.to_string()),
            },
            _ => None,
        }
    }
}

impl<'a> SelectRewriter<'a> {
    fn new(column_binding: &'a [ColumnBinding], is_unquoted_ident_case_sensitive: bool) -> Self {
        SelectRewriter {
            column_binding,
            new_stmt: None,
            is_unquoted_ident_case_sensitive,
        }
    }

    fn rewrite(&mut self, stmt: &SelectStmt) -> Result<Option<SelectStmt>> {
        self.rewrite_pivot(stmt)?;
        self.rewrite_unpivot(stmt)?;
        Ok(self.new_stmt.take())
    }

    fn rewrite_pivot(&mut self, stmt: &SelectStmt) -> Result<()> {
        if stmt.from.len() != 1 || stmt.from[0].pivot().is_none() {
            return Ok(());
        }
        let pivot = stmt.from[0].pivot().unwrap();
        let (aggregate_name, aggregate_args) = Self::parse_aggregate_function(&pivot.aggregate)?;
        let aggregate_columns = aggregate_args
            .iter()
            .map(|expr| match expr {
                Expr::ColumnRef { column, .. } => Some(column.clone()),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()
            .ok_or_else(|| ErrorCode::SyntaxException("Aggregate column not found"))?;
        let aggregate_column_names = aggregate_columns
            .iter()
            .map(|col| col.column.name())
            .collect::<Vec<_>>();
        let new_group_by = stmt.group_by.clone().unwrap_or_else(|| {
            GroupBy::Normal(
                self.column_binding
                    .iter()
                    .filter(|col_bind| {
                        !self
                            .compare_unquoted_ident(&col_bind.column_name, &pivot.value_column.name)
                            && !aggregate_column_names
                                .iter()
                                .any(|col| self.compare_unquoted_ident(col, &col_bind.column_name))
                    })
                    .map(|col| Expr::Literal {
                        span: Span::default(),
                        value: Literal::UInt64(col.index as u64 + 1),
                    })
                    .collect(),
            )
        });

        let mut new_select_list = stmt.select_list.clone();
        if let Some(star) = new_select_list.iter_mut().find(|target| target.is_star()) {
            let mut exclude_columns: Vec<_> = aggregate_columns
                .iter()
                .map(|c| Identifier::from_name(stmt.span, c.column.name()))
                .collect();
            exclude_columns.push(pivot.value_column.clone());
            star.exclude(exclude_columns);
        };
        let new_aggregate_name = Identifier {
            name: format!("{}_if", aggregate_name.name),
            ..aggregate_name.clone()
        };
        for value in &pivot.values {
            let mut args = aggregate_args.to_vec();
            args.push(Self::expr_eq_from_col_and_value(
                pivot.value_column.clone(),
                value.clone(),
            ));
            let alias = Self::raw_string_from_literal_expr(value)
                .ok_or_else(|| ErrorCode::SyntaxException("Pivot value should be literal"))?;
            new_select_list.push(Self::target_func_from_name_args(
                new_aggregate_name.clone(),
                args,
                Some(Identifier::from_name(stmt.span, &alias)),
            ));
        }

        if let Some(ref mut new_stmt) = self.new_stmt {
            new_stmt.select_list = new_select_list;
            new_stmt.group_by = Some(new_group_by);
        } else {
            self.new_stmt = Some(SelectStmt {
                select_list: new_select_list,
                group_by: Some(new_group_by),
                ..stmt.clone()
            });
        }
        Ok(())
    }

    fn rewrite_unpivot(&mut self, stmt: &SelectStmt) -> Result<()> {
        if stmt.from.len() != 1 || stmt.from[0].unpivot().is_none() {
            return Ok(());
        }
        let unpivot = stmt.from[0].unpivot().unwrap();
        let mut new_select_list = stmt.select_list.clone();
        if let Some(star) = new_select_list.iter_mut().find(|target| target.is_star()) {
            star.exclude(unpivot.names.clone());
        };
        new_select_list.push(Self::target_func_from_name_args(
            Identifier::from_name(stmt.span, "unnest"),
            vec![Self::expr_literal_array_from_vec_ident(
                unpivot.names.clone(),
            )],
            Some(unpivot.column_name.clone()),
        ));
        new_select_list.push(Self::target_func_from_name_args(
            Identifier::from_name(stmt.span, "unnest"),
            vec![Self::expr_column_ref_array_from_vec_ident(
                unpivot.names.clone(),
            )],
            Some(unpivot.value_column.clone()),
        ));

        if let Some(ref mut new_stmt) = self.new_stmt {
            new_stmt.select_list = new_select_list;
        } else {
            self.new_stmt = Some(SelectStmt {
                select_list: new_select_list,
                ..stmt.clone()
            });
        };
        Ok(())
    }
}

#[derive(Visitor)]
#[visitor(ColumnPosition(enter))]
pub struct MaxColumnPosition {
    pub max_pos: usize,
}

impl MaxColumnPosition {
    pub fn new() -> Self {
        Self { max_pos: 0 }
    }
}

impl MaxColumnPosition {
    fn enter_column_position(&mut self, pos: &ColumnPosition) {
        if pos.pos > self.max_pos {
            self.max_pos = pos.pos;
        }
    }
}
