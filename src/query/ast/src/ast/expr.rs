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
use std::fmt::Formatter;

use derive_visitor::Drive;
use derive_visitor::DriveMut;
use enum_as_inner::EnumAsInner;
use ethnum::i256;
use pratt::Affix;
use pratt::Associativity;

use super::ColumnRef;
use super::OrderByExpr;
use crate::ast::display_decimal_256;
use crate::ast::quote::QuotedString;
use crate::ast::write_comma_separated_list;
use crate::ast::Identifier;
use crate::ast::Query;
use crate::span::merge_span;
use crate::ParseError;
use crate::Result;
use crate::Span;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum Expr {
    /// Column reference, with indirection like `table.column`
    ColumnRef {
        span: Span,
        column: ColumnRef,
    },
    /// `IS [ NOT ] NULL` expression
    IsNull {
        span: Span,
        expr: Box<Expr>,
        not: bool,
    },
    /// `IS [NOT] DISTINCT` expression
    IsDistinctFrom {
        span: Span,
        left: Box<Expr>,
        right: Box<Expr>,
        not: bool,
    },
    /// `[ NOT ] IN (expr, ...)`
    InList {
        span: Span,
        expr: Box<Expr>,
        list: Vec<Expr>,
        not: bool,
    },
    /// `[ NOT ] IN (SELECT ...)`
    InSubquery {
        span: Span,
        expr: Box<Expr>,
        subquery: Box<Query>,
        not: bool,
    },
    /// `BETWEEN ... AND ...`
    Between {
        span: Span,
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        not: bool,
    },
    /// Binary operation
    BinaryOp {
        span: Span,
        op: BinaryOperator,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    /// JSON operation
    JsonOp {
        span: Span,
        op: JsonOperator,
        left: Box<Expr>,
        right: Box<Expr>,
    },
    /// Unary operation
    UnaryOp {
        span: Span,
        op: UnaryOperator,
        expr: Box<Expr>,
    },
    /// `CAST` expression, like `CAST(expr AS target_type)`
    Cast {
        span: Span,
        expr: Box<Expr>,
        target_type: TypeName,
        pg_style: bool,
    },
    /// `TRY_CAST` expression`
    TryCast {
        span: Span,
        expr: Box<Expr>,
        target_type: TypeName,
    },
    /// EXTRACT(IntervalKind FROM <expr>)
    Extract {
        span: Span,
        kind: IntervalKind,
        expr: Box<Expr>,
    },
    /// DATE_PART(IntervalKind, <expr>)
    DatePart {
        span: Span,
        kind: IntervalKind,
        expr: Box<Expr>,
    },
    /// POSITION(<expr> IN <expr>)
    Position {
        span: Span,
        substr_expr: Box<Expr>,
        str_expr: Box<Expr>,
    },
    /// SUBSTRING(<expr> [FROM <expr>] [FOR <expr>])
    Substring {
        span: Span,
        expr: Box<Expr>,
        substring_from: Box<Expr>,
        substring_for: Option<Box<Expr>>,
    },
    /// TRIM([[BOTH | LEADING | TRAILING] <expr> FROM] <expr>)
    /// Or
    /// TRIM(<expr>)
    Trim {
        span: Span,
        expr: Box<Expr>,
        // ([BOTH | LEADING | TRAILING], <expr>)
        trim_where: Option<(TrimWhere, Box<Expr>)>,
    },
    /// A literal value, such as string, number, date or NULL
    Literal {
        span: Span,
        value: Literal,
    },
    /// `COUNT(*)` expression
    CountAll {
        span: Span,
        window: Option<Window>,
    },
    /// `(foo, bar)`
    Tuple {
        span: Span,
        exprs: Vec<Expr>,
    },
    /// Scalar/Agg/Window function call
    FunctionCall {
        span: Span,
        func: FunctionCall,
    },
    /// `CASE ... WHEN ... ELSE ...` expression
    Case {
        span: Span,
        operand: Option<Box<Expr>>,
        conditions: Vec<Expr>,
        results: Vec<Expr>,
        else_result: Option<Box<Expr>>,
    },
    /// `EXISTS` expression
    Exists {
        span: Span,
        /// Indicate if this is a `NOT EXISTS`
        not: bool,
        subquery: Box<Query>,
    },
    /// Scalar/ANY/ALL/SOME subquery
    Subquery {
        span: Span,
        modifier: Option<SubqueryModifier>,
        subquery: Box<Query>,
    },
    /// Access elements of `Array`, `Map` and `Variant` by index or key, like `arr[0]`, or `obj:k1`
    MapAccess {
        span: Span,
        expr: Box<Expr>,
        accessor: MapAccessor,
    },
    /// The `Array` expr
    Array {
        span: Span,
        exprs: Vec<Expr>,
    },
    /// The `Map` expr
    Map {
        span: Span,
        kvs: Vec<(Literal, Expr)>,
    },
    /// The `Interval 1 DAY` expr
    Interval {
        span: Span,
        expr: Box<Expr>,
        unit: IntervalKind,
    },
    DateAdd {
        span: Span,
        unit: IntervalKind,
        interval: Box<Expr>,
        date: Box<Expr>,
    },
    DateSub {
        span: Span,
        unit: IntervalKind,
        interval: Box<Expr>,
        date: Box<Expr>,
    },
    DateTrunc {
        span: Span,
        unit: IntervalKind,
        date: Box<Expr>,
    },
    Hole {
        span: Span,
        name: String,
    },
}

impl Expr {
    pub fn span(&self) -> Span {
        match self {
            Expr::ColumnRef { span, .. }
            | Expr::IsNull { span, .. }
            | Expr::IsDistinctFrom { span, .. }
            | Expr::InList { span, .. }
            | Expr::InSubquery { span, .. }
            | Expr::Between { span, .. }
            | Expr::BinaryOp { span, .. }
            | Expr::JsonOp { span, .. }
            | Expr::UnaryOp { span, .. }
            | Expr::Cast { span, .. }
            | Expr::TryCast { span, .. }
            | Expr::Extract { span, .. }
            | Expr::DatePart { span, .. }
            | Expr::Position { span, .. }
            | Expr::Substring { span, .. }
            | Expr::Trim { span, .. }
            | Expr::Literal { span, .. }
            | Expr::CountAll { span, .. }
            | Expr::Tuple { span, .. }
            | Expr::FunctionCall { span, .. }
            | Expr::Case { span, .. }
            | Expr::Exists { span, .. }
            | Expr::Subquery { span, .. }
            | Expr::MapAccess { span, .. }
            | Expr::Array { span, .. }
            | Expr::Map { span, .. }
            | Expr::Interval { span, .. }
            | Expr::DateAdd { span, .. }
            | Expr::DateSub { span, .. }
            | Expr::DateTrunc { span, .. }
            | Expr::Hole { span, .. } => *span,
        }
    }

    pub fn whole_span(&self) -> Span {
        match self {
            Expr::ColumnRef { span, .. } => *span,
            Expr::IsNull { span, expr, .. } => merge_span(*span, expr.whole_span()),
            Expr::IsDistinctFrom {
                span, left, right, ..
            } => merge_span(merge_span(*span, left.whole_span()), right.whole_span()),
            Expr::InList {
                span, expr, list, ..
            } => {
                let mut span = merge_span(*span, expr.whole_span());
                for item in list {
                    span = merge_span(span, item.whole_span());
                }
                span
            }
            Expr::InSubquery {
                span,
                expr,
                subquery,
                ..
            } => merge_span(merge_span(*span, expr.whole_span()), subquery.span),
            Expr::Between {
                span,
                expr,
                low,
                high,
                ..
            } => merge_span(
                merge_span(*span, expr.whole_span()),
                merge_span(low.whole_span(), high.whole_span()),
            ),
            Expr::BinaryOp {
                span, left, right, ..
            } => merge_span(merge_span(*span, left.whole_span()), right.whole_span()),
            Expr::JsonOp {
                span, left, right, ..
            } => merge_span(merge_span(*span, left.whole_span()), right.whole_span()),
            Expr::UnaryOp { span, expr, .. } => merge_span(*span, expr.whole_span()),
            Expr::Cast { span, expr, .. } => merge_span(*span, expr.whole_span()),
            Expr::TryCast { span, expr, .. } => merge_span(*span, expr.whole_span()),
            Expr::Extract { span, expr, .. } => merge_span(*span, expr.whole_span()),
            Expr::DatePart { span, expr, .. } => merge_span(*span, expr.whole_span()),
            Expr::Position {
                span,
                substr_expr,
                str_expr,
                ..
            } => merge_span(
                merge_span(*span, substr_expr.whole_span()),
                str_expr.whole_span(),
            ),
            Expr::Substring {
                span,
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                let mut span = merge_span(
                    merge_span(*span, expr.whole_span()),
                    substring_from.whole_span(),
                );
                if let Some(substring_for) = substring_for {
                    span = merge_span(span, substring_for.whole_span());
                }
                span
            }
            Expr::Trim { span, expr, .. } => merge_span(*span, expr.whole_span()),
            Expr::Literal { span, .. } => *span,
            Expr::CountAll { span, .. } => *span,
            Expr::Tuple { span, exprs } => {
                let mut span = *span;
                for expr in exprs {
                    span = merge_span(span, expr.whole_span());
                }
                span
            }
            Expr::FunctionCall { span, .. } => *span,
            Expr::Case {
                span,
                operand,
                conditions,
                results,
                else_result,
            } => {
                let mut span = *span;
                if let Some(operand) = operand {
                    span = merge_span(span, operand.whole_span());
                }
                for (cond, res) in conditions.iter().zip(results) {
                    span = merge_span(merge_span(span, cond.whole_span()), res.whole_span());
                }
                if let Some(else_result) = else_result {
                    span = merge_span(span, else_result.whole_span());
                }
                span
            }
            Expr::Exists { span, subquery, .. } => merge_span(*span, subquery.span),
            Expr::Subquery { span, subquery, .. } => merge_span(*span, subquery.span),
            Expr::MapAccess { span, expr, .. } => merge_span(*span, expr.whole_span()),
            Expr::Array { span, exprs } => {
                let mut span = *span;
                for expr in exprs {
                    span = merge_span(span, expr.whole_span());
                }
                span
            }
            Expr::Map { span, kvs } => {
                let mut span = *span;
                for (_, v) in kvs {
                    span = merge_span(span, v.whole_span());
                }
                span
            }
            Expr::Interval { span, expr, .. } => merge_span(*span, expr.whole_span()),
            Expr::DateAdd {
                span,
                interval,
                date,
                ..
            } => merge_span(merge_span(*span, interval.whole_span()), date.whole_span()),
            Expr::DateSub {
                span,
                interval,
                date,
                ..
            } => merge_span(merge_span(*span, interval.whole_span()), date.whole_span()),
            Expr::DateTrunc { span, date, .. } => merge_span(*span, date.whole_span()),
            Expr::Hole { span, .. } => *span,
        }
    }

    pub fn all_function_like_syntaxes() -> &'static [&'static str] {
        &[
            "CAST",
            "TRY_CAST",
            "EXTRACT",
            "DATE_PART",
            "POSITION",
            "SUBSTRING",
            "TRIM",
            "DATE_ADD",
            "DATE_SUB",
            "DATE_TRUNC",
        ]
    }
}

impl Display for Expr {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        fn needs_parentheses(parent: Option<Affix>, child: Affix, is_left: bool) -> bool {
            match (parent, child) {
                (Some(Affix::Infix(parent_prec, parent_assoc)), Affix::Infix(child_prec, _)) => {
                    if parent_prec < child_prec {
                        return false;
                    }
                    if parent_prec > child_prec {
                        return true;
                    }
                    if matches!(parent_assoc, Associativity::Left) && !is_left {
                        return true;
                    }
                    if matches!(parent_assoc, Associativity::Right) && is_left {
                        return true;
                    }
                }
                (
                    Some(
                        Affix::Infix(parent_prec, _)
                        | Affix::Prefix(parent_prec)
                        | Affix::Postfix(parent_prec),
                    ),
                    Affix::Infix(child_prec, _)
                    | Affix::Prefix(child_prec)
                    | Affix::Postfix(child_prec),
                ) => {
                    return parent_prec > child_prec;
                }
                _ => (),
            }
            false
        }

        #[recursive::recursive]
        fn write_expr(
            expr: &Expr,
            parent: Option<Affix>,
            is_left: bool,
            f: &mut Formatter,
        ) -> std::fmt::Result {
            let affix = expr.affix();
            let need_paren = needs_parentheses(parent, affix, is_left);

            if need_paren {
                write!(f, "(")?;
            }

            match expr {
                Expr::ColumnRef { column, .. } => {
                    if f.alternate() {
                        write!(f, "{column:#}")?;
                    } else {
                        write!(f, "{column}")?;
                    }
                }
                Expr::IsNull { expr, not, .. } => {
                    write_expr(expr, Some(affix), true, f)?;
                    write!(f, " IS")?;
                    if *not {
                        write!(f, " NOT")?;
                    }
                    write!(f, " NULL")?;
                }
                Expr::IsDistinctFrom {
                    left, right, not, ..
                } => {
                    write_expr(left, Some(affix), true, f)?;
                    write!(f, " IS")?;
                    if *not {
                        write!(f, " NOT")?;
                    }
                    write!(f, " DISTINCT FROM ")?;
                    write_expr(right, Some(affix), true, f)?;
                }

                Expr::InList {
                    expr, list, not, ..
                } => {
                    write_expr(expr, Some(affix), true, f)?;
                    if *not {
                        write!(f, " NOT")?;
                    }
                    write!(f, " IN(")?;
                    write_comma_separated_list(f, list)?;
                    write!(f, ")")?;
                }
                Expr::InSubquery {
                    expr,
                    subquery,
                    not,
                    ..
                } => {
                    write_expr(expr, Some(affix), true, f)?;
                    if *not {
                        write!(f, " NOT")?;
                    }
                    write!(f, " IN({subquery})")?;
                }
                Expr::Between {
                    expr,
                    low,
                    high,
                    not,
                    ..
                } => {
                    write_expr(expr, Some(affix), true, f)?;
                    if *not {
                        write!(f, " NOT")?;
                    }
                    write!(f, " BETWEEN {low} AND {high}")?;
                }
                Expr::UnaryOp { op, expr, .. } => {
                    match op {
                        // TODO (xieqijun) Maybe special attribute are provided to check whether the symbol is before or after.
                        UnaryOperator::Factorial => {
                            write_expr(expr, Some(affix), true, f)?;
                            write!(f, " {op}")?;
                        }
                        _ => {
                            write!(f, "{op} ")?;
                            write_expr(expr, Some(affix), true, f)?;
                        }
                    }
                }
                Expr::BinaryOp {
                    op, left, right, ..
                } => {
                    write_expr(left, Some(affix), true, f)?;
                    write!(f, " {op} ")?;
                    write_expr(right, Some(affix), false, f)?;
                }
                Expr::JsonOp {
                    op, left, right, ..
                } => {
                    write_expr(left, Some(affix), true, f)?;
                    write!(f, " {op} ")?;
                    write_expr(right, Some(affix), true, f)?;
                }
                Expr::Cast {
                    expr,
                    target_type,
                    pg_style,
                    ..
                } => {
                    if *pg_style {
                        write_expr(expr, Some(affix), true, f)?;
                        write!(f, "::{target_type}")?;
                    } else {
                        write!(f, "CAST({expr} AS {target_type})")?;
                    }
                }
                Expr::TryCast {
                    expr, target_type, ..
                } => {
                    write!(f, "TRY_CAST({expr} AS {target_type})")?;
                }
                Expr::Extract {
                    kind: field, expr, ..
                } => {
                    write!(f, "EXTRACT({field} FROM {expr})")?;
                }
                Expr::DatePart {
                    kind: field, expr, ..
                } => {
                    write!(f, "DATE_PART({field}, {expr})")?;
                }
                Expr::Position {
                    substr_expr,
                    str_expr,
                    ..
                } => {
                    write!(f, "POSITION({substr_expr} IN {str_expr})")?;
                }
                Expr::Substring {
                    expr,
                    substring_from,
                    substring_for,
                    ..
                } => {
                    write!(f, "SUBSTRING({expr} FROM {substring_from}")?;
                    if let Some(substring_for) = substring_for {
                        write!(f, " FOR {substring_for}")?;
                    }
                    write!(f, ")")?;
                }
                Expr::Trim {
                    expr, trim_where, ..
                } => {
                    write!(f, "TRIM(")?;
                    if let Some((trim_where, trim_str)) = trim_where {
                        write!(f, "{trim_where} {trim_str} FROM ")?;
                    }
                    write!(f, "{expr})")?;
                }
                Expr::Literal { value, .. } => {
                    write!(f, "{value}")?;
                }
                Expr::CountAll { window, .. } => {
                    write!(f, "COUNT(*)")?;
                    if let Some(window) = window {
                        write!(f, " OVER {window}")?;
                    }
                }
                Expr::Tuple { exprs, .. } => {
                    write!(f, "(")?;
                    write_comma_separated_list(f, exprs)?;
                    if exprs.len() == 1 {
                        write!(f, ",")?;
                    }
                    write!(f, ")")?;
                }
                Expr::FunctionCall { func, .. } => {
                    write!(f, "{func}")?;
                }
                Expr::Case {
                    operand,
                    conditions,
                    results,
                    else_result,
                    ..
                } => {
                    write!(f, "CASE")?;
                    if let Some(op) = operand {
                        write!(f, " {op} ")?;
                    }
                    for (cond, res) in conditions.iter().zip(results) {
                        write!(f, " WHEN {cond} THEN {res}")?;
                    }
                    if let Some(el) = else_result {
                        write!(f, " ELSE {el}")?;
                    }
                    write!(f, " END")?;
                }
                Expr::Exists { not, subquery, .. } => {
                    if *not {
                        write!(f, "NOT ")?;
                    }
                    write!(f, "EXISTS ({subquery})")?;
                }
                Expr::Subquery {
                    subquery, modifier, ..
                } => {
                    if let Some(m) = modifier {
                        write!(f, "{m} ")?;
                    }
                    write!(f, "({subquery})")?;
                }
                Expr::MapAccess { expr, accessor, .. } => {
                    write_expr(expr, Some(affix), true, f)?;
                    match accessor {
                        MapAccessor::Bracket { key } => write!(f, "[{key}]")?,
                        MapAccessor::DotNumber { key } => write!(f, ".{key}")?,
                        MapAccessor::Colon { key } => write!(f, ":{key}")?,
                    }
                }
                Expr::Array { exprs, .. } => {
                    write!(f, "[")?;
                    write_comma_separated_list(f, exprs)?;
                    write!(f, "]")?;
                }
                Expr::Map { kvs, .. } => {
                    write!(f, "{{")?;
                    for (i, (k, v)) in kvs.iter().enumerate() {
                        if i > 0 {
                            write!(f, ",")?;
                        }
                        write!(f, "{k}:{v}")?;
                    }
                    write!(f, "}}")?;
                }
                Expr::Interval { expr, unit, .. } => {
                    write!(f, "INTERVAL {expr} {unit}")?;
                }
                Expr::DateAdd {
                    unit,
                    interval,
                    date,
                    ..
                } => {
                    write!(f, "DATE_ADD({unit}, {interval}, {date})")?;
                }
                Expr::DateSub {
                    unit,
                    interval,
                    date,
                    ..
                } => {
                    write!(f, "DATE_SUB({unit}, {interval}, {date})")?;
                }
                Expr::DateTrunc { unit, date, .. } => {
                    write!(f, "DATE_TRUNC({unit}, {date})")?;
                }
                Expr::Hole { name, .. } => {
                    write!(f, ":{name}")?;
                }
            }

            if need_paren {
                write!(f, ")")?;
            }

            Ok(())
        }

        write_expr(self, None, true, f)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum IntervalKind {
    Year,
    Quarter,
    Month,
    Day,
    Hour,
    Minute,
    Second,
    Doy,
    Week,
    Dow,
}

impl Display for IntervalKind {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(match self {
            IntervalKind::Year => "YEAR",
            IntervalKind::Quarter => "QUARTER",
            IntervalKind::Month => "MONTH",
            IntervalKind::Day => "DAY",
            IntervalKind::Hour => "HOUR",
            IntervalKind::Minute => "MINUTE",
            IntervalKind::Second => "SECOND",
            IntervalKind::Doy => "DOY",
            IntervalKind::Dow => "DOW",
            IntervalKind::Week => "WEEK",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum SubqueryModifier {
    Any,
    All,
    Some,
}

impl Display for SubqueryModifier {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            SubqueryModifier::Any => write!(f, "ANY"),
            SubqueryModifier::All => write!(f, "ALL"),
            SubqueryModifier::Some => write!(f, "SOME"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum Literal {
    UInt64(u64),
    Float64(f64),
    Decimal256 {
        #[drive(skip)]
        value: i256,
        precision: u8,
        scale: u8,
    },
    // Quoted string literal value
    String(String),
    Boolean(bool),
    Null,
}

impl Literal {
    pub fn as_double(&self) -> Result<f64> {
        match self {
            Literal::UInt64(val) => Ok(*val as f64),
            Literal::Float64(val) => Ok(*val),
            Literal::Decimal256 { value, scale, .. } => {
                let div = 10_f64.powi(*scale as i32);
                Ok(value.as_f64() / div)
            }
            _ => Err(ParseError(
                None,
                format!("Cannot convert {:?} to double", self),
            )),
        }
    }
}

impl Display for Literal {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            Literal::UInt64(val) => {
                write!(f, "{val}")
            }
            Literal::Decimal256 { value, scale, .. } => {
                write!(f, "{}", display_decimal_256(*value, *scale))
            }
            Literal::Float64(val) => {
                if val.is_infinite() {
                    if val.is_sign_positive() {
                        write!(f, "'+INFINITY'::FLOAT64")
                    } else {
                        write!(f, "'-INFINITY'::FLOAT64")
                    }
                } else if val.is_nan() {
                    write!(f, "'NaN'::FLOAT64")
                } else {
                    write!(f, "{val}")
                }
            }
            Literal::String(val) => {
                write!(f, "{}", QuotedString(val, '\''))
            }
            Literal::Boolean(val) => {
                if *val {
                    write!(f, "TRUE")
                } else {
                    write!(f, "FALSE")
                }
            }
            Literal::Null => {
                write!(f, "NULL")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct FunctionCall {
    /// Set to true if the function is aggregate function with `DISTINCT`, like `COUNT(DISTINCT a)`
    pub distinct: bool,
    pub name: Identifier,
    pub args: Vec<Expr>,
    pub params: Vec<Expr>,
    pub window: Option<WindowDesc>,
    pub lambda: Option<Lambda>,
}

impl Display for FunctionCall {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        let FunctionCall {
            distinct,
            name,
            args,
            params,
            window,
            lambda,
        } = self;
        write!(f, "{name}")?;
        if !params.is_empty() {
            write!(f, "(")?;
            write_comma_separated_list(f, params)?;
            write!(f, ")")?;
        }
        write!(f, "(")?;
        if *distinct {
            write!(f, "DISTINCT ")?;
        }
        write_comma_separated_list(f, args)?;
        if let Some(lambda) = lambda {
            write!(f, ", {lambda}")?;
        }
        write!(f, ")")?;

        if let Some(window) = window {
            if let Some(ignore_null) = window.ignore_nulls {
                if ignore_null {
                    write!(f, " IGNORE NULLS")?;
                } else {
                    write!(f, " RESPECT NULLS")?;
                }
            }
            write!(f, " OVER {}", window.window)?;
        }
        Ok(())
    }
}

/// The display style for a map access expression
#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum MapAccessor {
    /// `[0][1]`
    Bracket { key: Box<Expr> },
    /// `.1`
    DotNumber { key: u64 },
    /// `:a:b`
    Colon { key: Identifier },
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum TypeName {
    Boolean,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Int8,
    Int16,
    Int32,
    Int64,
    Float32,
    Float64,
    Decimal {
        precision: u8,
        scale: u8,
    },
    Date,
    Timestamp,
    Binary,
    String,
    Array(Box<TypeName>),
    Map {
        key_type: Box<TypeName>,
        val_type: Box<TypeName>,
    },
    Bitmap,
    Tuple {
        fields_name: Option<Vec<Identifier>>,
        fields_type: Vec<TypeName>,
    },
    Variant,
    Geometry,
    Nullable(Box<TypeName>),
    NotNull(Box<TypeName>),
}

impl TypeName {
    pub fn is_nullable(&self) -> bool {
        matches!(self, TypeName::Nullable(_))
    }

    pub fn wrap_nullable(self) -> Self {
        if !self.is_nullable() {
            Self::Nullable(Box::new(self))
        } else {
            self
        }
    }

    pub fn wrap_not_null(self) -> Self {
        match self {
            Self::NotNull(_) => self,
            _ => Self::NotNull(Box::new(self)),
        }
    }
}

impl Display for TypeName {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            TypeName::Boolean => {
                write!(f, "BOOLEAN")?;
            }
            TypeName::UInt8 => {
                write!(f, "UInt8")?;
            }
            TypeName::UInt16 => {
                write!(f, "UInt16")?;
            }
            TypeName::UInt32 => {
                write!(f, "UInt32")?;
            }
            TypeName::UInt64 => {
                write!(f, "UInt64")?;
            }
            TypeName::Int8 => {
                write!(f, "Int8")?;
            }
            TypeName::Int16 => {
                write!(f, "Int16")?;
            }
            TypeName::Int32 => {
                write!(f, "Int32")?;
            }
            TypeName::Int64 => {
                write!(f, "Int64")?;
            }
            TypeName::Float32 => {
                write!(f, "Float32")?;
            }
            TypeName::Float64 => {
                write!(f, "Float64")?;
            }
            TypeName::Decimal { precision, scale } => {
                write!(f, "Decimal({}, {})", precision, scale)?;
            }
            TypeName::Date => {
                write!(f, "DATE")?;
            }
            TypeName::Timestamp => {
                write!(f, "TIMESTAMP")?;
            }
            TypeName::Binary => {
                write!(f, "BINARY")?;
            }
            TypeName::String => {
                write!(f, "STRING")?;
            }
            TypeName::Array(ty) => {
                write!(f, "ARRAY({})", ty)?;
            }
            TypeName::Map { key_type, val_type } => {
                write!(f, "MAP({}, {})", key_type, val_type)?;
            }
            TypeName::Bitmap => {
                write!(f, "BITMAP")?;
            }
            TypeName::Tuple {
                fields_name,
                fields_type,
            } => {
                write!(f, "TUPLE(")?;
                let mut first = true;
                match fields_name {
                    Some(fields_name) => {
                        for (name, ty) in fields_name.iter().zip(fields_type.iter()) {
                            if !first {
                                write!(f, ", ")?;
                            }
                            first = false;
                            write!(f, "{} {}", name, ty)?;
                        }
                    }
                    None => {
                        for ty in fields_type.iter() {
                            if !first {
                                write!(f, ", ")?;
                            }
                            first = false;
                            write!(f, "{}", ty)?;
                        }
                    }
                }
                write!(f, ")")?;
            }
            TypeName::Variant => {
                write!(f, "VARIANT")?;
            }
            TypeName::Geometry => {
                write!(f, "GEOMETRY")?;
            }
            TypeName::Nullable(ty) => {
                write!(f, "{} NULL", ty)?;
            }
            TypeName::NotNull(ty) => {
                write!(f, "{} NOT NULL", ty)?;
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum TrimWhere {
    Both,
    Leading,
    Trailing,
}

impl Display for TrimWhere {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.write_str(match self {
            TrimWhere::Both => "BOTH",
            TrimWhere::Leading => "LEADING",
            TrimWhere::Trailing => "TRAILING",
        })
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct WindowDesc {
    pub ignore_nulls: Option<bool>,
    pub window: Window,
}

#[derive(Debug, Clone, PartialEq, EnumAsInner, Drive, DriveMut)]
pub enum Window {
    WindowReference(WindowRef),
    WindowSpec(WindowSpec),
}

impl Display for Window {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match *self {
            Window::WindowReference(ref window_ref) => write!(f, "{}", window_ref),
            Window::WindowSpec(ref window_spec) => write!(f, "{}", window_spec),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct WindowDefinition {
    pub name: Identifier,
    pub spec: WindowSpec,
}

impl Display for WindowDefinition {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{} AS {}", self.name, self.spec)
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct WindowRef {
    pub window_name: Identifier,
}

impl Display for WindowRef {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.window_name)
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct WindowSpec {
    pub existing_window_name: Option<Identifier>,
    pub partition_by: Vec<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub window_frame: Option<WindowFrame>,
}

impl Display for WindowSpec {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "(")?;
        if let Some(existing_window_name) = &self.existing_window_name {
            write!(f, " {existing_window_name}")?;
        }

        if !self.partition_by.is_empty() {
            write!(f, " PARTITION BY ")?;
            write_comma_separated_list(f, &self.partition_by)?;
        }

        if !self.order_by.is_empty() {
            write!(f, " ORDER BY ")?;
            write_comma_separated_list(f, &self.order_by)?;
        }

        if let Some(frame) = &self.window_frame {
            match frame.units {
                WindowFrameUnits::Rows => {
                    write!(f, " ROWS")?;
                }
                WindowFrameUnits::Range => {
                    write!(f, " RANGE")?;
                }
            }

            let format_frame = |frame: &WindowFrameBound| -> String {
                match frame {
                    WindowFrameBound::CurrentRow => "CURRENT ROW".to_string(),
                    WindowFrameBound::Preceding(None) => "UNBOUNDED PRECEDING".to_string(),
                    WindowFrameBound::Following(None) => "UNBOUNDED FOLLOWING".to_string(),
                    WindowFrameBound::Preceding(Some(n)) => format!("{} PRECEDING", n),
                    WindowFrameBound::Following(Some(n)) => format!("{} FOLLOWING", n),
                }
            };
            write!(
                f,
                " BETWEEN {} AND {}",
                format_frame(&frame.start_bound),
                format_frame(&frame.end_bound)
            )?
        }
        write!(f, " )")?;
        Ok(())
    }
}

/// `RANGE UNBOUNDED PRECEDING` or `ROWS BETWEEN 5 PRECEDING AND CURRENT ROW`.
#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct WindowFrame {
    pub units: WindowFrameUnits,
    pub start_bound: WindowFrameBound,
    pub end_bound: WindowFrameBound,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, EnumAsInner, Drive, DriveMut)]
pub enum WindowFrameUnits {
    Rows,
    Range,
}

/// Specifies [WindowFrame]'s `start_bound` and `end_bound`
#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub enum WindowFrameBound {
    /// `CURRENT ROW`
    CurrentRow,
    /// `<N> PRECEDING` or `UNBOUNDED PRECEDING`
    Preceding(Option<Box<Expr>>),
    /// `<N> FOLLOWING` or `UNBOUNDED FOLLOWING`.
    Following(Option<Box<Expr>>),
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct Lambda {
    pub params: Vec<Identifier>,
    pub expr: Box<Expr>,
}

impl Display for Lambda {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        if self.params.len() == 1 {
            write!(f, "{}", self.params[0])?;
        } else {
            write!(f, "(")?;
            write_comma_separated_list(f, self.params.clone())?;
            write!(f, ")")?;
        }
        write!(f, " -> {}", self.expr)?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum BinaryOperator {
    Plus,
    Minus,
    Multiply,
    Div,
    Divide,
    IntDiv,
    Modulo,
    StringConcat,
    // `>` operator
    Gt,
    // `<` operator
    Lt,
    // `>=` operator
    Gte,
    // `<=` operator
    Lte,
    Eq,
    NotEq,
    Caret,
    And,
    Or,
    Xor,
    Like,
    NotLike,
    Regexp,
    RLike,
    NotRegexp,
    NotRLike,
    SoundsLike,
    BitwiseOr,
    BitwiseAnd,
    BitwiseXor,
    BitwiseShiftLeft,
    BitwiseShiftRight,
    L2Distance,
}

impl BinaryOperator {
    pub fn to_contrary(&self) -> Result<Self> {
        match &self {
            BinaryOperator::Gt => Ok(BinaryOperator::Lte),
            BinaryOperator::Lt => Ok(BinaryOperator::Gte),
            BinaryOperator::Gte => Ok(BinaryOperator::Lt),
            BinaryOperator::Lte => Ok(BinaryOperator::Gt),
            BinaryOperator::Eq => Ok(BinaryOperator::NotEq),
            BinaryOperator::NotEq => Ok(BinaryOperator::Eq),
            _ => Err(ParseError(
                None,
                format!("Converting {self} to its contrary is not currently supported"),
            )),
        }
    }

    pub fn to_func_name(&self) -> String {
        match self {
            BinaryOperator::StringConcat => "concat".to_string(),
            BinaryOperator::BitwiseOr => "bit_or".to_string(),
            BinaryOperator::BitwiseAnd => "bit_and".to_string(),
            BinaryOperator::BitwiseXor => "bit_xor".to_string(),
            BinaryOperator::BitwiseShiftLeft => "bit_shift_left".to_string(),
            BinaryOperator::BitwiseShiftRight => "bit_shift_right".to_string(),
            BinaryOperator::Caret => "pow".to_string(),
            BinaryOperator::L2Distance => "l2_distance".to_string(),
            _ => {
                let name = format!("{:?}", self);
                name.to_lowercase()
            }
        }
    }
}

impl Display for BinaryOperator {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            BinaryOperator::Plus => {
                write!(f, "+")
            }
            BinaryOperator::Minus => {
                write!(f, "-")
            }
            BinaryOperator::Multiply => {
                write!(f, "*")
            }
            BinaryOperator::Div => {
                write!(f, "DIV")
            }
            BinaryOperator::Divide => {
                write!(f, "/")
            }
            BinaryOperator::IntDiv => {
                write!(f, "//")
            }
            BinaryOperator::Modulo => {
                write!(f, "%")
            }
            BinaryOperator::StringConcat => {
                write!(f, "||")
            }
            BinaryOperator::Gt => {
                write!(f, ">")
            }
            BinaryOperator::Lt => {
                write!(f, "<")
            }
            BinaryOperator::Gte => {
                write!(f, ">=")
            }
            BinaryOperator::Lte => {
                write!(f, "<=")
            }
            BinaryOperator::Eq => {
                write!(f, "=")
            }
            BinaryOperator::NotEq => {
                write!(f, "<>")
            }
            BinaryOperator::Caret => {
                write!(f, "^")
            }
            BinaryOperator::And => {
                write!(f, "AND")
            }
            BinaryOperator::Or => {
                write!(f, "OR")
            }
            BinaryOperator::Xor => {
                write!(f, "XOR")
            }
            BinaryOperator::Like => {
                write!(f, "LIKE")
            }
            BinaryOperator::NotLike => {
                write!(f, "NOT LIKE")
            }
            BinaryOperator::Regexp => {
                write!(f, "REGEXP")
            }
            BinaryOperator::RLike => {
                write!(f, "RLIKE")
            }
            BinaryOperator::NotRegexp => {
                write!(f, "NOT REGEXP")
            }
            BinaryOperator::NotRLike => {
                write!(f, "NOT RLIKE")
            }
            BinaryOperator::SoundsLike => {
                write!(f, "SOUNDS LIKE")
            }
            BinaryOperator::BitwiseOr => {
                write!(f, "|")
            }
            BinaryOperator::BitwiseAnd => {
                write!(f, "&")
            }
            BinaryOperator::BitwiseXor => {
                write!(f, "#")
            }
            BinaryOperator::BitwiseShiftLeft => {
                write!(f, "<<")
            }
            BinaryOperator::BitwiseShiftRight => {
                write!(f, ">>")
            }
            BinaryOperator::L2Distance => {
                write!(f, "<->")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum JsonOperator {
    /// -> keeps the value as json
    Arrow,
    /// ->> keeps the value as text or int.
    LongArrow,
    /// #> Extracts JSON sub-object at the specified path
    HashArrow,
    /// #>> Extracts JSON sub-object at the specified path as text
    HashLongArrow,
    /// ? Checks whether text key exist as top-level key or array element.
    Question,
    /// ?| Checks whether any of the text keys exist as top-level keys or array elements.
    QuestionOr,
    /// ?& Checks whether all of the text keys exist as top-level keys or array elements.
    QuestionAnd,
    /// @> Checks whether left json contains the right json
    AtArrow,
    /// <@ Checks whether right json contains the left json
    ArrowAt,
    /// @? Checks whether JSON path return any item for the specified JSON value
    AtQuestion,
    /// @@ Returns the result of a JSON path predicate check for the specified JSON value.
    AtAt,
    /// #- Deletes the field or array element at the specified keypath.
    HashMinus,
}

impl JsonOperator {
    pub fn to_func_name(&self) -> String {
        match self {
            JsonOperator::Arrow => "get".to_string(),
            JsonOperator::LongArrow => "get_string".to_string(),
            JsonOperator::HashArrow => "get_by_keypath".to_string(),
            JsonOperator::HashLongArrow => "get_by_keypath_string".to_string(),
            JsonOperator::Question => "json_exists_key".to_string(),
            JsonOperator::QuestionOr => "json_exists_any_keys".to_string(),
            JsonOperator::QuestionAnd => "json_exists_all_keys".to_string(),
            JsonOperator::AtArrow => "json_contains_in_left".to_string(),
            JsonOperator::ArrowAt => "json_contains_in_right".to_string(),
            JsonOperator::AtQuestion => "json_path_exists".to_string(),
            JsonOperator::AtAt => "json_path_match".to_string(),
            JsonOperator::HashMinus => "delete_by_keypath".to_string(),
        }
    }
}

impl Display for JsonOperator {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            JsonOperator::Arrow => {
                write!(f, "->")
            }
            JsonOperator::LongArrow => {
                write!(f, "->>")
            }
            JsonOperator::HashArrow => {
                write!(f, "#>")
            }
            JsonOperator::HashLongArrow => {
                write!(f, "#>>")
            }
            JsonOperator::Question => {
                write!(f, "?")
            }
            JsonOperator::QuestionOr => {
                write!(f, "?|")
            }
            JsonOperator::QuestionAnd => {
                write!(f, "?&")
            }
            JsonOperator::AtArrow => {
                write!(f, "@>")
            }
            JsonOperator::ArrowAt => {
                write!(f, "<@")
            }
            JsonOperator::AtQuestion => {
                write!(f, "@?")
            }
            JsonOperator::AtAt => {
                write!(f, "@@")
            }
            JsonOperator::HashMinus => {
                write!(f, "#-")
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum UnaryOperator {
    Plus,
    Minus,
    Not,
    Factorial,
    SquareRoot,
    CubeRoot,
    Abs,
    BitwiseNot,
}

impl Display for UnaryOperator {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            UnaryOperator::Plus => {
                write!(f, "+")
            }
            UnaryOperator::Minus => {
                write!(f, "-")
            }
            UnaryOperator::Not => {
                write!(f, "NOT")
            }
            UnaryOperator::SquareRoot => {
                write!(f, "|/")
            }
            UnaryOperator::CubeRoot => {
                write!(f, "||/")
            }
            UnaryOperator::Factorial => {
                write!(f, "!")
            }
            UnaryOperator::Abs => {
                write!(f, "@")
            }
            UnaryOperator::BitwiseNot => {
                write!(f, "~")
            }
        }
    }
}

impl UnaryOperator {
    pub fn to_func_name(&self) -> String {
        match self {
            UnaryOperator::SquareRoot => "sqrt".to_string(),
            UnaryOperator::CubeRoot => "cbrt".to_string(),
            UnaryOperator::BitwiseNot => "bit_not".to_string(),
            _ => {
                let name = format!("{:?}", self);
                name.to_lowercase()
            }
        }
    }
}

pub fn split_conjunctions_expr(expr: &Expr) -> Vec<Expr> {
    match expr {
        Expr::BinaryOp {
            op, left, right, ..
        } if op == &BinaryOperator::And => {
            let mut result = split_conjunctions_expr(left);
            result.extend(split_conjunctions_expr(right));
            result
        }
        _ => vec![expr.clone()],
    }
}

pub fn split_equivalent_predicate_expr(expr: &Expr) -> Option<(Expr, Expr)> {
    match expr {
        Expr::BinaryOp {
            op, left, right, ..
        } if op == &BinaryOperator::Eq => Some((*left.clone(), *right.clone())),
        _ => None,
    }
}
