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
use databend_common_ast::ast::SelectStmt;
use databend_common_ast::ast::SelectTarget;
use databend_common_ast::ast::TableReference;
use databend_common_ast::Span;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use derive_visitor::Drive;
use derive_visitor::Visitor;
use log::warn;

use crate::binder::project_set::SrfCollector;
use crate::optimizer::SExpr;
use crate::planner::binder::BindContext;
use crate::planner::binder::Binder;
use crate::AsyncFunctionRewriter;
use crate::ColumnBinding;
use crate::UdfRewriter;
use crate::VirtualColumnRewriter;

// A normalized IR for `SELECT` clause.
#[derive(Debug, Default)]
pub struct SelectList {}

impl Binder {
    #[async_backtrace::framed]
    pub(crate) fn bind_select(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &SelectStmt,
        order_by: &[OrderByExpr],
        limit: Option<usize>,
    ) -> Result<(SExpr, BindContext)> {
        if let Some(hints) = &stmt.hints {
            if let Some(e) = self.opt_hints_set_var(bind_context, hints).err() {
                warn!(
                    "In SELECT resolve optimize hints {:?} failed, err: {:?}",
                    hints, e
                );
            }
        }
        let (mut s_expr, mut from_context) = if stmt.from.is_empty() {
            let select_list = &stmt.select_list;
            self.bind_dummy_table(bind_context, select_list)?
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
            self.bind_table_reference(bind_context, &cross_joins)?
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
        s_expr = self.bind_project_set(&mut from_context, &set_returning_functions, s_expr)?;

        // Try put window definitions into bind context.
        // This operation should be before `normalize_select_list` because window functions can be used in select list.
        self.analyze_window_definition(&mut from_context, &stmt.window_list)?;

        // Generate a analyzed select list with from context
        let mut select_list = self.normalize_select_list(&mut from_context, &stmt.select_list)?;

        // This will potentially add some alias group items to `from_context` if find some.
        if let Some(group_by) = stmt.group_by.as_ref() {
            self.analyze_group_items(&mut from_context, &select_list, group_by)?;
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
            let (new_expr, scalar) = self.bind_where(&mut from_context, &aliases, expr, s_expr)?;
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
            Some(self.analyze_aggregate_having(&mut from_context, &aliases, having)?)
        } else {
            None
        };

        let qualify = if let Some(qualify) = &stmt.qualify {
            Some(self.analyze_window_qualify(&mut from_context, &aliases, qualify)?)
        } else {
            None
        };

        let order_items = self.analyze_order_items(
            &mut from_context,
            &mut scalar_items,
            &aliases,
            &projections,
            order_by,
            stmt.distinct,
        )?;

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
                limit.unwrap_or_default(),
            )?;
        }

        if !from_context.aggregate_info.aggregate_functions.is_empty()
            || !from_context.aggregate_info.group_items.is_empty()
        {
            s_expr = self.bind_aggregate(&mut from_context, s_expr)?;
        }

        if let Some(having) = having {
            s_expr = self.bind_having(&mut from_context, having, s_expr)?;
        }

        // bind window
        // window run after the HAVING clause but before the ORDER BY clause.
        for window_info in &from_context.windows.window_functions {
            s_expr = self.bind_window_function(window_info, s_expr)?;
        }

        if let Some(qualify) = qualify {
            s_expr = self.bind_qualify(&mut from_context, qualify, s_expr)?;
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
            s_expr = self.bind_order_by(
                &from_context,
                order_items,
                &select_list,
                &mut scalar_items,
                s_expr,
            )?;
        }

        s_expr = self.bind_projection(&mut from_context, &projections, &scalar_items, s_expr)?;

        // rewrite async function to async function plan
        let mut async_func_rewriter = AsyncFunctionRewriter::new();
        s_expr = async_func_rewriter.rewrite(&s_expr)?;

        // rewrite udf for interpreter udf
        let mut udf_rewriter = UdfRewriter::new(self.metadata.clone(), true);
        s_expr = udf_rewriter.rewrite(&s_expr)?;

        // rewrite udf for server udf
        let mut udf_rewriter = UdfRewriter::new(self.metadata.clone(), false);
        s_expr = udf_rewriter.rewrite(&s_expr)?;

        // rewrite variant inner fields as virtual columns
        let mut virtual_column_rewriter =
            VirtualColumnRewriter::new(self.ctx.clone(), self.metadata.clone());
        s_expr = virtual_column_rewriter.rewrite(&s_expr)?;

        // check inverted index license
        if !from_context.inverted_index_map.is_empty() {
            LicenseManagerSwitch::instance()
                .check_enterprise_enabled(self.ctx.get_license_key(), Feature::InvertedIndex)?;
        }
        // add internal column binding into expr
        s_expr = from_context.add_internal_column_into_expr(s_expr)?;

        let mut output_context = BindContext::new();
        output_context.parent = from_context.parent;
        output_context.columns = from_context.columns;

        Ok((s_expr, output_context))
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
