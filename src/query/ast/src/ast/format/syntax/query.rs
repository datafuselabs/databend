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

use pretty::RcDoc;

use super::expr::pretty_expr;
use crate::ast::format::syntax::inline_comma;
use crate::ast::format::syntax::inline_dot;
use crate::ast::format::syntax::interweave_comma;
use crate::ast::format::syntax::parenthesized;
use crate::ast::format::syntax::NEST_FACTOR;
use crate::ast::Expr;
use crate::ast::GroupBy;
use crate::ast::JoinCondition;
use crate::ast::JoinOperator;
use crate::ast::OrderByExpr;
use crate::ast::Query;
use crate::ast::SelectTarget;
use crate::ast::SetExpr;
use crate::ast::SetOperator;
use crate::ast::TableReference;
use crate::ast::WindowDefinition;
use crate::ast::With;
use crate::ast::CTE;

pub(crate) fn pretty_query(query: Query) -> RcDoc<'static> {
    pretty_with(query.with)
        .append(pretty_body(query.body))
        .append(pretty_order_by(query.order_by))
        .append(pretty_limit(query.limit))
        .append(pretty_offset(query.offset))
        .group()
}

fn pretty_with(with: Option<With>) -> RcDoc<'static> {
    if let Some(with) = with {
        RcDoc::text("WITH")
            .append(if with.recursive {
                RcDoc::space().append(RcDoc::text("RECURSIVE"))
            } else {
                RcDoc::nil()
            })
            .append(RcDoc::line().nest(NEST_FACTOR))
            .append(
                interweave_comma(with.ctes.into_iter().map(pretty_cte))
                    .nest(NEST_FACTOR)
                    .group(),
            )
            .append(RcDoc::line())
    } else {
        RcDoc::nil()
    }
}

fn pretty_cte(cte: CTE) -> RcDoc<'static> {
    RcDoc::text(format!("{} AS ", cte.alias))
        .append(RcDoc::softline())
        .append(if cte.materialized {
            RcDoc::text("MATERIALIZED ".to_string())
        } else {
            RcDoc::nil()
        })
        .append(parenthesized(pretty_query(*cte.query)))
}

fn pretty_body(body: SetExpr) -> RcDoc<'static> {
    match body {
        SetExpr::Select(select_stmt) => if select_stmt.distinct {
            RcDoc::text("SELECT DISTINCT")
        } else {
            RcDoc::text("SELECT")
        }
        .append(pretty_select_list(select_stmt.select_list))
        .append(pretty_from(select_stmt.from))
        .append(pretty_selection(select_stmt.selection))
        .append(pretty_group_by(select_stmt.group_by))
        .append(pretty_having(select_stmt.having))
        .append(pretty_window(select_stmt.window_list)),
        SetExpr::Query(query) => parenthesized(pretty_query(*query)),
        SetExpr::SetOperation(set_operation) => pretty_body(*set_operation.left)
            .append(
                RcDoc::line()
                    .append(match set_operation.op {
                        SetOperator::Union => RcDoc::text("UNION"),
                        SetOperator::Except => RcDoc::text("EXCEPT"),
                        SetOperator::Intersect => RcDoc::text("INTERSECT"),
                    })
                    .append(if set_operation.all {
                        RcDoc::space().append(RcDoc::text("ALL"))
                    } else {
                        RcDoc::nil()
                    }),
            )
            .append(RcDoc::line())
            .append(pretty_body(*set_operation.right)),
        SetExpr::Values { values, .. } => {
            RcDoc::text("VALUES").append(inline_comma(values.into_iter().map(|row_values| {
                RcDoc::text("(")
                    .append(inline_comma(row_values.into_iter().map(pretty_expr)))
                    .append(RcDoc::text(")"))
            })))
        }
    }
}

fn pretty_select_list(select_list: Vec<SelectTarget>) -> RcDoc<'static> {
    if select_list.len() > 1 {
        RcDoc::line()
    } else {
        RcDoc::space()
    }
    .nest(NEST_FACTOR)
    .append(
        interweave_comma(
            select_list
                .into_iter()
                .map(|select_target| match select_target {
                    SelectTarget::AliasedExpr { expr, alias } => {
                        pretty_expr(*expr).append(if let Some(alias) = alias {
                            RcDoc::space()
                                .append(RcDoc::text("AS"))
                                .append(RcDoc::space())
                                .append(RcDoc::text(alias.to_string()))
                        } else {
                            RcDoc::nil()
                        })
                    }
                    SelectTarget::StarColumns {
                        qualified: object_name,
                        column_filter,
                    } => {
                        let docs = inline_dot(
                            object_name
                                .into_iter()
                                .map(|indirection| RcDoc::text(indirection.to_string())),
                        )
                        .group();
                        docs.append(if let Some(filter) = column_filter {
                            match filter {
                                crate::ast::ColumnFilter::Excludes(exclude) => RcDoc::line()
                                    .append(
                                        RcDoc::text("EXCLUDE").append(
                                            if exclude.len() > 1 {
                                                RcDoc::line()
                                            } else {
                                                RcDoc::space()
                                            }
                                            .nest(NEST_FACTOR),
                                        ),
                                    )
                                    .append(
                                        interweave_comma(exclude.into_iter().map(|ident| {
                                            RcDoc::space()
                                                .append(RcDoc::space())
                                                .append(RcDoc::text(ident.to_string()))
                                        }))
                                        .nest(NEST_FACTOR)
                                        .group(),
                                    ),
                                crate::ast::ColumnFilter::Lambda(lambda) => RcDoc::line()
                                    .append(RcDoc::text("("))
                                    .append(inline_comma(
                                        lambda
                                            .params
                                            .iter()
                                            .map(|ident| RcDoc::text(ident.to_string())),
                                    ))
                                    .append(RcDoc::text("->"))
                                    .append(pretty_expr(*lambda.expr)),
                            }
                        } else {
                            RcDoc::nil()
                        })
                    }
                }),
        )
        .nest(NEST_FACTOR)
        .group(),
    )
}

fn pretty_from(from: Vec<TableReference>) -> RcDoc<'static> {
    if !from.is_empty() {
        RcDoc::line()
            .append(RcDoc::text("FROM").append(RcDoc::line().nest(NEST_FACTOR)))
            .append(
                interweave_comma(from.into_iter().map(pretty_table))
                    .nest(NEST_FACTOR)
                    .group(),
            )
    } else {
        RcDoc::nil()
    }
}

fn pretty_selection(selection: Option<Expr>) -> RcDoc<'static> {
    if let Some(selection) = selection {
        RcDoc::line().append(RcDoc::text("WHERE")).append(
            RcDoc::line()
                .nest(NEST_FACTOR)
                .append(pretty_expr(selection).nest(NEST_FACTOR).group()),
        )
    } else {
        RcDoc::nil()
    }
}

fn pretty_group_set(set: Vec<Expr>) -> RcDoc<'static> {
    RcDoc::nil()
        .append(RcDoc::text("("))
        .append(inline_comma(set.into_iter().map(pretty_expr)))
        .append(RcDoc::text(")"))
}

fn pretty_group_by(group_by: Option<GroupBy>) -> RcDoc<'static> {
    if let Some(group_by) = group_by {
        match group_by {
            GroupBy::Normal(exprs) => RcDoc::line()
                .append(
                    RcDoc::text("GROUP BY").append(
                        if exprs.len() > 1 {
                            RcDoc::line()
                        } else {
                            RcDoc::space()
                        }
                        .nest(NEST_FACTOR),
                    ),
                )
                .append(
                    interweave_comma(exprs.into_iter().map(pretty_expr))
                        .nest(NEST_FACTOR)
                        .group(),
                ),
            GroupBy::All => RcDoc::line().append(RcDoc::text("GROUP BY ALL")),
            GroupBy::GroupingSets(sets) => RcDoc::line()
                .append(
                    RcDoc::text("GROUP BY GROUPING SETS (").append(RcDoc::line().nest(NEST_FACTOR)),
                )
                .append(
                    interweave_comma(sets.into_iter().map(pretty_group_set))
                        .nest(NEST_FACTOR)
                        .group(),
                )
                .append(RcDoc::line())
                .append(RcDoc::text(")")),
            GroupBy::Rollup(exprs) => RcDoc::line()
                .append(RcDoc::text("GROUP BY ROLLUP (").append(RcDoc::line().nest(NEST_FACTOR)))
                .append(
                    interweave_comma(exprs.into_iter().map(pretty_expr))
                        .nest(NEST_FACTOR)
                        .group(),
                )
                .append(RcDoc::line())
                .append(RcDoc::text(")")),
            GroupBy::Cube(exprs) => RcDoc::line()
                .append(RcDoc::text("GROUP BY CUBE (").append(RcDoc::line().nest(NEST_FACTOR)))
                .append(
                    interweave_comma(exprs.into_iter().map(pretty_expr))
                        .nest(NEST_FACTOR)
                        .group(),
                )
                .append(RcDoc::line())
                .append(RcDoc::text(")")),
        }
    } else {
        RcDoc::nil()
    }
}

fn pretty_having(having: Option<Expr>) -> RcDoc<'static> {
    if let Some(having) = having {
        RcDoc::line()
            .append(RcDoc::text("HAVING").append(RcDoc::line().nest(NEST_FACTOR)))
            .append(pretty_expr(having))
    } else {
        RcDoc::nil()
    }
}

fn pretty_window(window: Option<Vec<WindowDefinition>>) -> RcDoc<'static> {
    if let Some(window) = window {
        RcDoc::line()
            .append(RcDoc::text("WINDOW").append(RcDoc::line().nest(NEST_FACTOR)))
            .append(
                interweave_comma(window.into_iter().map(pretty_window_def))
                    .nest(NEST_FACTOR)
                    .group(),
            )
    } else {
        RcDoc::nil()
    }
}

fn pretty_window_def(def: WindowDefinition) -> RcDoc<'static> {
    RcDoc::text(def.name.to_string())
        .append(RcDoc::space())
        .append(RcDoc::text("AS ("))
        .append(RcDoc::text(def.spec.to_string()))
        .append(RcDoc::text(")"))
}

pub(crate) fn pretty_table(table: TableReference) -> RcDoc<'static> {
    match table {
        TableReference::Table {
            span: _,
            catalog,
            database,
            table,
            alias,
            temporal,
            consume,
            pivot,
            unpivot,
        } => if let Some(catalog) = catalog {
            RcDoc::text(catalog.to_string()).append(RcDoc::text("."))
        } else {
            RcDoc::nil()
        }
        .append(if let Some(database) = database {
            RcDoc::text(database.to_string()).append(RcDoc::text("."))
        } else {
            RcDoc::nil()
        })
        .append(RcDoc::text(table.to_string()))
        .append(if let Some(temporal) = temporal {
            RcDoc::text(format!(" {temporal}"))
        } else {
            RcDoc::nil()
        })
        .append(if consume {
            RcDoc::text(" WITH CONSUME")
        } else {
            RcDoc::nil()
        })
        .append(if let Some(pivot) = pivot {
            RcDoc::text(format!(" {pivot}"))
        } else {
            RcDoc::nil()
        })
        .append(if let Some(unpivot) = unpivot {
            RcDoc::text(format!(" {unpivot}"))
        } else {
            RcDoc::nil()
        })
        .append(if let Some(alias) = alias {
            RcDoc::text(format!(" AS {alias}"))
        } else {
            RcDoc::nil()
        }),
        TableReference::Subquery {
            span: _,
            lateral,
            subquery,
            alias,
        } => (if lateral {
            RcDoc::text("LATERAL")
        } else {
            RcDoc::nil()
        })
        .append(parenthesized(pretty_query(*subquery)))
        .append(if let Some(alias) = alias {
            RcDoc::text(format!(" AS {alias}"))
        } else {
            RcDoc::nil()
        }),
        TableReference::TableFunction {
            span: _,
            lateral,
            name,
            params,
            named_params,
            alias,
        } => {
            let separator = if !named_params.is_empty() && !params.is_empty() {
                RcDoc::text(", ")
            } else {
                RcDoc::nil()
            };
            if lateral {
                RcDoc::text("LATERAL ")
            } else {
                RcDoc::nil()
            }
            .append(RcDoc::text(name.to_string()))
            .append(RcDoc::text("("))
            .append(inline_comma(params.into_iter().map(pretty_expr)))
            .append(separator)
            .append(inline_comma(named_params.into_iter().map(|(k, v)| {
                RcDoc::text(k.to_string())
                    .append(RcDoc::text("=>"))
                    .append(pretty_expr(v))
            })))
            .append(RcDoc::text(")"))
            .append(if let Some(alias) = alias {
                RcDoc::text(format!(" AS {alias}"))
            } else {
                RcDoc::nil()
            })
        }
        TableReference::Join { span: _, join } => pretty_table(*join.left)
            .append(RcDoc::line())
            .append(if join.condition == JoinCondition::Natural {
                RcDoc::text("NATURAL").append(RcDoc::space())
            } else {
                RcDoc::nil()
            })
            .append(match join.op {
                JoinOperator::Inner => RcDoc::text("INNER JOIN"),
                JoinOperator::LeftOuter => RcDoc::text("LEFT OUTER JOIN"),
                JoinOperator::RightOuter => RcDoc::text("RIGHT OUTER JOIN"),
                JoinOperator::FullOuter => RcDoc::text("FULL OUTER JOIN"),
                JoinOperator::CrossJoin => RcDoc::text("CROSS JOIN"),
                JoinOperator::LeftAnti => RcDoc::text("LEFT ANTI JOIN"),
                JoinOperator::RightAnti => RcDoc::text("RIGHT ANTI JOIN"),
                JoinOperator::LeftSemi => RcDoc::text("LEFT SEMI JOIN"),
                JoinOperator::RightSemi => RcDoc::text("RIGHT SEMI JOIN"),
                JoinOperator::Asof => RcDoc::text("ASOF JOIN"),
                JoinOperator::LeftAsof => RcDoc::text("ASOF LEFT JOIN"),
                JoinOperator::RightAsof => RcDoc::text("ASOF RIGHT JOIN"),
            })
            .append(RcDoc::space().append(pretty_table(*join.right)))
            .append(match &join.condition {
                JoinCondition::On(expr) => RcDoc::space()
                    .append(RcDoc::text("ON"))
                    .append(RcDoc::space())
                    .append(pretty_expr(*expr.clone())),
                JoinCondition::Using(idents) => RcDoc::space()
                    .append(RcDoc::text("USING("))
                    .append(inline_comma(
                        idents.iter().map(|ident| RcDoc::text(ident.to_string())),
                    ))
                    .append(RcDoc::text(")")),
                _ => RcDoc::nil(),
            }),
        TableReference::Location {
            span: _,
            location,
            options,
            alias,
        } => RcDoc::text(location.to_string())
            .append(options.to_string())
            .append(if let Some(a) = alias {
                RcDoc::text(format!(" AS {a}"))
            } else {
                RcDoc::nil()
            }),
    }
}

fn pretty_order_by(order_by: Vec<OrderByExpr>) -> RcDoc<'static> {
    if !order_by.is_empty() {
        RcDoc::line()
            .append(
                RcDoc::text("ORDER BY").append(
                    if order_by.len() > 1 {
                        RcDoc::line()
                    } else {
                        RcDoc::space()
                    }
                    .nest(NEST_FACTOR),
                ),
            )
            .append(
                interweave_comma(order_by.into_iter().map(pretty_order_by_expr))
                    .nest(NEST_FACTOR)
                    .group(),
            )
    } else {
        RcDoc::nil()
    }
}

fn pretty_limit(limit: Vec<Expr>) -> RcDoc<'static> {
    if !limit.is_empty() {
        RcDoc::line()
            .append(
                RcDoc::text("LIMIT").append(
                    if limit.len() > 1 {
                        RcDoc::line()
                    } else {
                        RcDoc::space()
                    }
                    .nest(NEST_FACTOR),
                ),
            )
            .append(
                interweave_comma(limit.into_iter().map(pretty_expr))
                    .nest(NEST_FACTOR)
                    .group(),
            )
    } else {
        RcDoc::nil()
    }
}

fn pretty_offset(offset: Option<Expr>) -> RcDoc<'static> {
    if let Some(offset) = offset {
        RcDoc::line()
            .append(RcDoc::text("OFFSET").append(RcDoc::space().nest(NEST_FACTOR)))
            .append(pretty_expr(offset))
    } else {
        RcDoc::nil()
    }
}

fn pretty_order_by_expr(order_by_expr: OrderByExpr) -> RcDoc<'static> {
    RcDoc::text(order_by_expr.expr.to_string())
        .append(if let Some(asc) = order_by_expr.asc {
            if asc {
                RcDoc::space().append(RcDoc::text("ASC"))
            } else {
                RcDoc::space().append(RcDoc::text("DESC"))
            }
        } else {
            RcDoc::nil()
        })
        .append(if let Some(nulls_first) = order_by_expr.nulls_first {
            if nulls_first {
                RcDoc::space().append(RcDoc::text("NULLS FIRST"))
            } else {
                RcDoc::space().append(RcDoc::text("NULLS LAST"))
            }
        } else {
            RcDoc::nil()
        })
}
