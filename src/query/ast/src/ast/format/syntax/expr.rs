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

use super::query::pretty_query;
use crate::ast::format::syntax::inline_comma;
use crate::ast::format::syntax::interweave_comma;
use crate::ast::format::syntax::parenthesized;
use crate::ast::format::syntax::NEST_FACTOR;
use crate::ast::BinaryOperator;
use crate::ast::Expr;
use crate::ast::FunctionCall;
use crate::ast::MapAccessor;

pub(crate) fn pretty_expr(expr: Expr) -> RcDoc<'static> {
    match expr {
        Expr::ColumnRef { column, .. } => if let Some(database) = column.database {
            RcDoc::text(database.to_string()).append(RcDoc::text("."))
        } else {
            RcDoc::nil()
        }
        .append(if let Some(table) = column.table {
            RcDoc::text(table.to_string()).append(RcDoc::text("."))
        } else {
            RcDoc::nil()
        })
        .append(RcDoc::text(column.column.to_string())),
        Expr::IsNull { expr, not, .. } => pretty_expr(*expr)
            .append(RcDoc::space())
            .append(RcDoc::text("IS"))
            .append(if not {
                RcDoc::space().append(RcDoc::text("NOT"))
            } else {
                RcDoc::nil()
            })
            .append(RcDoc::space())
            .append(RcDoc::text("NULL")),
        Expr::IsDistinctFrom {
            left, right, not, ..
        } => pretty_expr(*left)
            .append(RcDoc::space())
            .append(RcDoc::text("IS"))
            .append(if not {
                RcDoc::space().append(RcDoc::text("NOT"))
            } else {
                RcDoc::nil()
            })
            .append(RcDoc::space())
            .append(RcDoc::text("DISTINCT FROM"))
            .append(RcDoc::space())
            .append(pretty_expr(*right)),
        Expr::InList {
            expr, list, not, ..
        } => pretty_expr(*expr)
            .append(if not {
                RcDoc::space().append(RcDoc::text("NOT"))
            } else {
                RcDoc::nil()
            })
            .append(RcDoc::space())
            .append(RcDoc::text("IN ("))
            .append(inline_comma(list.into_iter().map(pretty_expr)))
            .append(RcDoc::text(")")),
        Expr::InSubquery {
            expr,
            subquery,
            not,
            ..
        } => pretty_expr(*expr)
            .append(if not {
                RcDoc::space().append(RcDoc::text("NOT"))
            } else {
                RcDoc::nil()
            })
            .append(RcDoc::space())
            .append(RcDoc::text("IN ("))
            .append(pretty_query(*subquery))
            .append(RcDoc::text(")")),
        Expr::Between {
            expr,
            low,
            high,
            not,
            ..
        } => pretty_expr(*expr)
            .append(if not {
                RcDoc::space().append(RcDoc::text("NOT"))
            } else {
                RcDoc::nil()
            })
            .append(RcDoc::space())
            .append(RcDoc::text("BETWEEN"))
            .append(RcDoc::space())
            .append(pretty_expr(*low))
            .append(RcDoc::space())
            .append(RcDoc::text("AND"))
            .append(RcDoc::space())
            .append(pretty_expr(*high)),
        Expr::UnaryOp { op, expr, .. } => RcDoc::text("(")
            .append(RcDoc::text(op.to_string()))
            .append(RcDoc::space())
            .append(pretty_expr(*expr))
            .append(RcDoc::text(")")),
        Expr::BinaryOp {
            op, left, right, ..
        } => match op {
            BinaryOperator::And | BinaryOperator::Or | BinaryOperator::Xor => parenthesized(
                pretty_expr(*left)
                    .append(RcDoc::line_())
                    .append(RcDoc::text(op.to_string()))
                    .append(RcDoc::space())
                    .append(pretty_expr(*right)),
            ),
            _ => RcDoc::text("(")
                .append(pretty_expr(*left))
                .append(RcDoc::space())
                .append(RcDoc::text(op.to_string()))
                .append(RcDoc::space())
                .append(pretty_expr(*right))
                .append(RcDoc::text(")")),
        },
        Expr::JsonOp {
            op, left, right, ..
        } => RcDoc::text("(")
            .append(pretty_expr(*left))
            .append(RcDoc::space())
            .append(RcDoc::text(op.to_string()))
            .append(RcDoc::space())
            .append(pretty_expr(*right))
            .append(RcDoc::text(")")),
        Expr::Cast {
            expr,
            target_type,
            pg_style,
            ..
        } => {
            if pg_style {
                pretty_expr(*expr)
                    .append(RcDoc::text("::"))
                    .append(RcDoc::text(target_type.to_string()))
            } else {
                RcDoc::text("CAST(")
                    .append(pretty_expr(*expr))
                    .append(RcDoc::space())
                    .append(RcDoc::text("AS"))
                    .append(RcDoc::space())
                    .append(RcDoc::text(target_type.to_string()))
                    .append(RcDoc::text(")"))
            }
        }
        Expr::TryCast {
            expr, target_type, ..
        } => RcDoc::text("TRY_CAST(")
            .append(pretty_expr(*expr))
            .append(RcDoc::space())
            .append(RcDoc::text("AS"))
            .append(RcDoc::space())
            .append(RcDoc::text(target_type.to_string()))
            .append(RcDoc::text(")")),
        Expr::Extract {
            kind: field, expr, ..
        } => RcDoc::text("EXTRACT(")
            .append(RcDoc::text(field.to_string()))
            .append(RcDoc::space())
            .append(RcDoc::text("FROM"))
            .append(RcDoc::space())
            .append(pretty_expr(*expr))
            .append(RcDoc::text(")")),
        Expr::DatePart {
            kind: field, expr, ..
        } => RcDoc::text("DATE_PART(")
            .append(RcDoc::text(field.to_string()))
            .append(RcDoc::space())
            .append(RcDoc::text(","))
            .append(RcDoc::space())
            .append(pretty_expr(*expr))
            .append(RcDoc::text(")")),
        Expr::Position {
            substr_expr,
            str_expr,
            ..
        } => RcDoc::text("POSITION(")
            .append(pretty_expr(*substr_expr))
            .append(RcDoc::space())
            .append(RcDoc::text("IN"))
            .append(RcDoc::space())
            .append(pretty_expr(*str_expr))
            .append(RcDoc::text(")")),
        Expr::Substring {
            expr,
            substring_from,
            substring_for,
            ..
        } => RcDoc::text("SUBSTRING(")
            .append(pretty_expr(*expr))
            .append(RcDoc::space())
            .append(RcDoc::text("FROM"))
            .append(RcDoc::space())
            .append(pretty_expr(*substring_from))
            .append(if let Some(substring_for) = substring_for {
                RcDoc::space()
                    .append(RcDoc::text("FOR"))
                    .append(RcDoc::space())
                    .append(pretty_expr(*substring_for))
            } else {
                RcDoc::nil()
            })
            .append(RcDoc::text(")")),
        Expr::Trim {
            expr, trim_where, ..
        } => RcDoc::text("TRIM(")
            .append(if let Some((trim_where, trim_expr)) = trim_where {
                RcDoc::text(trim_where.to_string())
                    .append(RcDoc::space())
                    .append(pretty_expr(*trim_expr))
                    .append(RcDoc::space())
                    .append(RcDoc::text("FROM"))
                    .append(RcDoc::space())
            } else {
                RcDoc::nil()
            })
            .append(pretty_expr(*expr))
            .append(RcDoc::text(")")),
        Expr::Literal { value, .. } => RcDoc::text(value.to_string()),
        Expr::CountAll { window, .. } => {
            RcDoc::text("COUNT(*)").append(if let Some(window) = window {
                RcDoc::text(" OVER (")
                    .append(RcDoc::text(window.to_string()))
                    .append(")")
            } else {
                RcDoc::nil()
            })
        }
        Expr::Tuple { exprs, .. } => RcDoc::text("(")
            .append(inline_comma(exprs.into_iter().map(pretty_expr)))
            .append(RcDoc::text(")")),
        Expr::FunctionCall { func, .. } => {
            let FunctionCall {
                name,
                distinct,
                args,
                params,
                window,
                lambda,
            } = func;

            RcDoc::text(name.to_string())
                .append(if !params.is_empty() {
                    RcDoc::text("(")
                        .append(inline_comma(
                            params
                                .into_iter()
                                .map(|literal| RcDoc::text(literal.to_string())),
                        ))
                        .append(")")
                } else {
                    RcDoc::nil()
                })
                .append(RcDoc::text("("))
                .append(if distinct {
                    RcDoc::text("DISTINCT").append(RcDoc::space())
                } else {
                    RcDoc::nil()
                })
                .append(inline_comma(args.into_iter().map(pretty_expr)))
                .append(if let Some(lambda) = lambda {
                    if lambda.params.len() == 1 {
                        RcDoc::text(lambda.params[0].to_string())
                    } else {
                        RcDoc::text("(")
                            .append(inline_comma(
                                lambda
                                    .params
                                    .iter()
                                    .map(|param| RcDoc::text(param.to_string())),
                            ))
                            .append(RcDoc::text(")"))
                    }
                    .append(RcDoc::text(" -> "))
                    .append(pretty_expr(*lambda.expr))
                } else {
                    RcDoc::nil()
                })
                .append(RcDoc::text(")"))
                .append(if let Some(window) = window {
                    if let Some(ignore_null) = window.ignore_nulls {
                        if ignore_null {
                            RcDoc::text(" IGNORE NULLS ")
                        } else {
                            RcDoc::text(" RESPECT NULLS ")
                        }
                        .append("OVER (")
                    } else {
                        RcDoc::text(" OVER (")
                    }
                    .append(RcDoc::text(window.window.to_string()))
                    .append(")")
                } else {
                    RcDoc::nil()
                })
        }
        Expr::Case {
            operand,
            conditions,
            results,
            else_result,
            ..
        } => RcDoc::text("CASE")
            .append(if let Some(op) = operand {
                RcDoc::space().append(RcDoc::text(op.to_string()))
            } else {
                RcDoc::nil()
            })
            .append(
                RcDoc::line()
                    .append(interweave_comma(conditions.iter().zip(results).map(
                        |(cond, res)| {
                            RcDoc::text("WHEN")
                                .append(RcDoc::space())
                                .append(pretty_expr(cond.clone()))
                                .append(RcDoc::space())
                                .append(RcDoc::text("THEN"))
                                .append(RcDoc::space())
                                .append(pretty_expr(res))
                        },
                    )))
                    .nest(NEST_FACTOR)
                    .group(),
            )
            .append(if let Some(el) = else_result {
                RcDoc::line()
                    .nest(NEST_FACTOR)
                    .append(RcDoc::text("ELSE"))
                    .append(RcDoc::space())
                    .append(pretty_expr(*el))
            } else {
                RcDoc::nil()
            })
            .append(RcDoc::line())
            .append(RcDoc::text("END")),
        Expr::Exists { not, subquery, .. } => if not {
            RcDoc::text("NOT").append(RcDoc::space())
        } else {
            RcDoc::nil()
        }
        .append(RcDoc::text("EXISTS"))
        .append(RcDoc::space())
        .append(parenthesized(pretty_query(*subquery))),
        Expr::Subquery {
            subquery, modifier, ..
        } => if let Some(m) = modifier {
            RcDoc::text(m.to_string()).append(RcDoc::space())
        } else {
            RcDoc::nil()
        }
        .append(parenthesized(pretty_query(*subquery))),
        Expr::MapAccess { expr, accessor, .. } => pretty_expr(*expr).append(match accessor {
            MapAccessor::Bracket { key } => RcDoc::text("[")
                .append(RcDoc::text(key.to_string()))
                .append(RcDoc::text("]")),
            MapAccessor::DotNumber { key } => RcDoc::text(".").append(RcDoc::text(key.to_string())),
            MapAccessor::Colon { key } => RcDoc::text(":").append(RcDoc::text(key.to_string())),
        }),
        Expr::Array { exprs, .. } => RcDoc::text("[")
            .append(inline_comma(exprs.into_iter().map(pretty_expr)))
            .append(RcDoc::text("]")),
        Expr::Map { kvs, .. } => RcDoc::text("{")
            .append(inline_comma(kvs.into_iter().map(|(k, v)| {
                RcDoc::text(k.to_string())
                    .append(RcDoc::text(":"))
                    .append(pretty_expr(v))
            })))
            .append(RcDoc::text("}")),
        Expr::Interval { expr, unit, .. } => RcDoc::text("INTERVAL")
            .append(RcDoc::space())
            .append(pretty_expr(*expr))
            .append(RcDoc::space())
            .append(RcDoc::text(unit.to_string())),
        Expr::DateAdd {
            unit,
            interval,
            date,
            ..
        } => RcDoc::text("DATE_ADD(")
            .append(RcDoc::text(unit.to_string()))
            .append(RcDoc::text(","))
            .append(RcDoc::space())
            .append(RcDoc::text("INTERVAL"))
            .append(RcDoc::space())
            .append(pretty_expr(*interval))
            .append(RcDoc::text(","))
            .append(RcDoc::space())
            .append(pretty_expr(*date))
            .append(RcDoc::text(")")),
        Expr::DateDiff {
            unit,
            date_start,
            date_end,
            ..
        } => RcDoc::text("DATE_DIFF(")
            .append(RcDoc::text(unit.to_string()))
            .append(RcDoc::text(","))
            .append(RcDoc::space())
            .append(pretty_expr(*date_start))
            .append(RcDoc::text(","))
            .append(RcDoc::space())
            .append(pretty_expr(*date_end))
            .append(RcDoc::text(")")),
        Expr::DateSub {
            unit,
            interval,
            date,
            ..
        } => RcDoc::text("DATE_SUB(")
            .append(RcDoc::text(unit.to_string()))
            .append(RcDoc::text(","))
            .append(RcDoc::space())
            .append(RcDoc::text("INTERVAL"))
            .append(RcDoc::space())
            .append(pretty_expr(*interval))
            .append(RcDoc::text(","))
            .append(RcDoc::space())
            .append(pretty_expr(*date))
            .append(RcDoc::text(")")),
        Expr::DateTrunc { unit, date, .. } => RcDoc::text("DATE_TRUNC(")
            .append(RcDoc::text(unit.to_string()))
            .append(RcDoc::text(","))
            .append(RcDoc::space())
            .append(pretty_expr(*date))
            .append(RcDoc::text(")")),
        Expr::Hole { name, .. } => RcDoc::text(":").append(RcDoc::text(name.to_string())),
    }
}
