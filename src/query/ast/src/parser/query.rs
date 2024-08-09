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

use nom::branch::alt;
use nom::combinator::consumed;
use nom::combinator::map;
use nom::combinator::value;
use nom::error::context;
use pratt::Affix;
use pratt::Associativity;
use pratt::PrattParser;
use pratt::Precedence;

use super::stage::file_location;
use super::stage::select_stage_option;
use crate::ast::*;
use crate::parser::common::*;
use crate::parser::expr::*;
use crate::parser::input::Input;
use crate::parser::input::WithSpan;
use crate::parser::statement::hint;
use crate::parser::statement::top_n;
use crate::parser::token::*;
use crate::parser::ErrorKind;
use crate::rule;

pub fn query(i: Input) -> IResult<Query> {
    context(
        "`SELECT ...`",
        map(set_operation, |set_expr| set_expr.into_query()),
    )(i)
}

pub fn set_operation(i: Input) -> IResult<SetExpr> {
    let (rest, set_operation_elements) = rule! { #set_operation_element+ }(i)?;
    let iter = &mut set_operation_elements.into_iter();
    run_pratt_parser(SetOperationParser, iter, rest, i)
}

#[derive(Debug, Clone, PartialEq)]
pub enum SetOperationElement {
    With(With),
    SelectStmt {
        hints: Option<Hint>,
        distinct: bool,
        top_n: Option<u64>,
        select_list: Vec<SelectTarget>,
        from: Vec<TableReference>,
        selection: Option<Expr>,
        group_by: Option<GroupBy>,
        having: Option<Expr>,
        window_list: Option<Vec<WindowDefinition>>,
        qualify: Option<Expr>,
    },
    SetOperation {
        op: SetOperator,
        all: bool,
    },
    Values(Vec<Vec<Expr>>),
    OrderBy {
        order_by: Vec<OrderByExpr>,
    },
    Limit {
        limit: Vec<Expr>,
    },
    Offset {
        offset: Expr,
    },
    IgnoreResult,
    Group(SetExpr),
}

pub fn set_operation_element(i: Input) -> IResult<WithSpan<SetOperationElement>> {
    let with = map(with, SetOperationElement::With);
    let set_operator = map(
        rule! {
            ( UNION | EXCEPT | INTERSECT ) ~ ALL?
        },
        |(op, all)| {
            let op = match op.kind {
                UNION => SetOperator::Union,
                INTERSECT => SetOperator::Intersect,
                EXCEPT => SetOperator::Except,
                _ => unreachable!(),
            };
            SetOperationElement::SetOperation {
                op,
                all: all.is_some(),
            }
        },
    );
    let select_stmt = map_res(
        rule! {
            ( FROM ~ ^#comma_separated_list1(table_reference) )?
            ~ SELECT ~ #hint? ~ DISTINCT? ~ #top_n? ~ ^#comma_separated_list1(select_target)
            ~ ( FROM ~ ^#comma_separated_list1(table_reference) )?
            ~ ( WHERE ~ ^#expr )?
            ~ ( GROUP ~ ^BY ~ ^#group_by_items )?
            ~ ( HAVING ~ ^#expr )?
            ~ ( WINDOW ~ ^#comma_separated_list1(window_clause) )?
            ~ ( QUALIFY ~ ^#expr )?
        },
        |(
            opt_from_block_first,
            _select,
            opt_hints,
            opt_distinct,
            opt_top_n,
            select_list,
            opt_from_block_second,
            opt_where_block,
            opt_group_by_block,
            opt_having_block,
            opt_window_block,
            opt_qualify_block,
        )| {
            if opt_from_block_first.is_some() && opt_from_block_second.is_some() {
                return Err(nom::Err::Failure(ErrorKind::Other(
                    "duplicated FROM clause",
                )));
            }

            Ok(SetOperationElement::SelectStmt {
                hints: opt_hints,
                distinct: opt_distinct.is_some(),
                top_n: opt_top_n,
                select_list,
                from: opt_from_block_first
                    .or(opt_from_block_second)
                    .map(|(_, table_refs)| table_refs)
                    .unwrap_or_default(),
                selection: opt_where_block.map(|(_, selection)| selection),
                group_by: opt_group_by_block.map(|(_, _, group_by)| group_by),
                having: opt_having_block.map(|(_, having)| having),
                window_list: opt_window_block.map(|(_, windows)| windows),
                qualify: opt_qualify_block.map(|(_, qualify)| qualify),
            })
        },
    );

    let values = map(
        rule! {
            VALUES ~ ^#comma_separated_list1(row_values)
        },
        |(_, values)| SetOperationElement::Values(values),
    );
    let order_by = map(
        rule! {
            ORDER ~ ^BY ~ ^#comma_separated_list1(order_by_expr)
        },
        |(_, _, order_by)| SetOperationElement::OrderBy { order_by },
    );
    let limit = map(
        rule! {
            LIMIT ~ ^#comma_separated_list1(expr)
        },
        |(_, limit)| SetOperationElement::Limit { limit },
    );
    let offset = map(
        rule! {
            OFFSET ~ ^#expr
        },
        |(_, offset)| SetOperationElement::Offset { offset },
    );
    let ignore_result = map(
        rule! {
            IGNORE_RESULT
        },
        |_| SetOperationElement::IgnoreResult,
    );
    let group = map(
        rule! {
           "(" ~ #set_operation ~ ^")"
        },
        |(_, set_expr, _)| SetOperationElement::Group(set_expr),
    );

    map(
        consumed(rule! {
            #group
            | #with
            | #set_operator
            | #select_stmt
            | #values
            | #order_by
            | #limit
            | #offset
            | #ignore_result
        }),
        |(span, elem)| WithSpan { span, elem },
    )(i)
}

struct SetOperationParser;

impl<'a, I: Iterator<Item = WithSpan<'a, SetOperationElement>>> PrattParser<I>
    for SetOperationParser
{
    type Error = &'static str;
    type Input = WithSpan<'a, SetOperationElement>;
    type Output = SetExpr;

    fn query(&mut self, input: &Self::Input) -> Result<Affix, &'static str> {
        let affix = match &input.elem {
            SetOperationElement::SetOperation { op, .. } => match op {
                SetOperator::Union | SetOperator::Except => {
                    Affix::Infix(Precedence(10), Associativity::Left)
                }
                SetOperator::Intersect => Affix::Infix(Precedence(20), Associativity::Left),
            },
            SetOperationElement::With(_) => Affix::Prefix(Precedence(5)),
            SetOperationElement::OrderBy { .. } => Affix::Postfix(Precedence(5)),
            SetOperationElement::Limit { .. } => Affix::Postfix(Precedence(5)),
            SetOperationElement::Offset { .. } => Affix::Postfix(Precedence(5)),
            SetOperationElement::IgnoreResult => Affix::Postfix(Precedence(5)),
            _ => Affix::Nilfix,
        };
        Ok(affix)
    }

    fn primary(&mut self, input: Self::Input) -> Result<Self::Output, &'static str> {
        let set_expr = match input.elem {
            SetOperationElement::Group(expr) => expr,
            SetOperationElement::SelectStmt {
                hints,
                distinct,
                top_n,
                select_list,
                from,
                selection,
                group_by,
                having,
                window_list,
                qualify,
            } => SetExpr::Select(Box::new(SelectStmt {
                span: transform_span(input.span.tokens),
                hints,
                top_n,
                distinct,
                select_list,
                from,
                selection,
                group_by,
                having,
                window_list,
                qualify,
            })),
            SetOperationElement::Values(values) => SetExpr::Values {
                span: transform_span(input.span.tokens),
                values,
            },
            _ => unreachable!(),
        };
        Ok(set_expr)
    }

    fn infix(
        &mut self,
        lhs: Self::Output,
        input: Self::Input,
        rhs: Self::Output,
    ) -> Result<Self::Output, &'static str> {
        let set_expr = match input.elem {
            SetOperationElement::SetOperation { op, all, .. } => {
                SetExpr::SetOperation(Box::new(SetOperation {
                    span: transform_span(input.span.tokens),
                    op,
                    all,
                    left: Box::new(lhs),
                    right: Box::new(rhs),
                }))
            }
            _ => unreachable!(),
        };
        Ok(set_expr)
    }

    fn prefix(&mut self, op: Self::Input, rhs: Self::Output) -> Result<Self::Output, Self::Error> {
        let mut query = rhs.into_query();
        match op.elem {
            SetOperationElement::With(with) => {
                if query.with.is_some() {
                    return Err("duplicated WITH clause");
                }
                query.with = Some(with);
            }
            _ => unreachable!(),
        }
        Ok(SetExpr::Query(Box::new(query)))
    }

    fn postfix(&mut self, lhs: Self::Output, op: Self::Input) -> Result<Self::Output, Self::Error> {
        let mut query = lhs.into_query();
        match op.elem {
            SetOperationElement::OrderBy { order_by } => {
                if !query.order_by.is_empty() {
                    return Err("duplicated ORDER BY clause");
                }
                if !query.limit.is_empty() {
                    return Err("ORDER BY must appear before LIMIT");
                }
                if query.offset.is_some() {
                    return Err("ORDER BY must appear before OFFSET");
                }
                query.order_by = order_by;
            }
            SetOperationElement::Limit { limit } => {
                if query.limit.is_empty() && limit.len() > 2 {
                    return Err("[LIMIT n OFFSET m] or [LIMIT n,m]");
                }
                if !query.limit.is_empty() {
                    return Err("duplicated LIMIT clause");
                }
                if query.offset.is_some() {
                    return Err("LIMIT must appear before OFFSET");
                }
                query.limit = limit;
            }
            SetOperationElement::Offset { offset } => {
                if query.limit.len() == 2 {
                    return Err("LIMIT n,m should not appear OFFSET");
                }
                if query.offset.is_some() {
                    return Err("duplicated OFFSET clause");
                }
                query.offset = Some(offset);
            }
            SetOperationElement::IgnoreResult => {
                query.ignore_result = true;
            }
            _ => unreachable!(),
        }
        Ok(SetExpr::Query(Box::new(query)))
    }
}

pub fn row_values(i: Input) -> IResult<Vec<Expr>> {
    map(
        rule! {"(" ~ #comma_separated_list1(expr) ~ ")"},
        |(_, row_values, _)| row_values,
    )(i)
}

pub fn with(i: Input) -> IResult<With> {
    let cte = map(
        consumed(rule! {
            #table_alias_without_as ~ AS ~ MATERIALIZED? ~ "(" ~ #query ~ ")"
        }),
        |(span, (table_alias, _, materialized, _, query, _))| CTE {
            span: transform_span(span.tokens),
            alias: table_alias,
            materialized: materialized.is_some(),
            query: Box::new(query),
        },
    );

    map(
        consumed(rule! {
            WITH ~ RECURSIVE? ~ ^#comma_separated_list1(cte)
        }),
        |(span, (_, recursive, ctes))| With {
            span: transform_span(span.tokens),
            recursive: recursive.is_some(),
            ctes,
        },
    )(i)
}

pub fn exclude_col(i: Input) -> IResult<Vec<Identifier>> {
    let var = map(
        rule! {
            #ident
        },
        |col| vec![col],
    );
    let vars = map(
        rule! {
             "(" ~ ^#comma_separated_list1(ident) ~ ^")"
        },
        |(_, cols, _)| cols,
    );

    rule!(
        #var
        | #vars
    )(i)
}

#[allow(clippy::type_complexity)]
pub fn select_target(i: Input) -> IResult<SelectTarget> {
    fn qualified_wildcard_transform(
        res: Option<(Identifier, &Token<'_>, Option<(Identifier, &Token<'_>)>)>,
        star: &Token<'_>,
        opt_exclude: Option<(&Token<'_>, Vec<Identifier>)>,
    ) -> SelectTarget {
        let column_filter = opt_exclude.map(|(_, exclude)| ColumnFilter::Excludes(exclude));
        match res {
            Some((fst, _, Some((snd, _)))) => SelectTarget::StarColumns {
                qualified: vec![
                    Indirection::Identifier(fst),
                    Indirection::Identifier(snd),
                    Indirection::Star(Some(star.span)),
                ],
                column_filter,
            },
            Some((fst, _, None)) => SelectTarget::StarColumns {
                qualified: vec![
                    Indirection::Identifier(fst),
                    Indirection::Star(Some(star.span)),
                ],
                column_filter,
            },
            None => SelectTarget::StarColumns {
                qualified: vec![Indirection::Star(Some(star.span))],
                column_filter,
            },
        }
    }

    let qualified_wildcard = alt((
        // select * exclude ...
        map(
            rule! {
               ( #ident ~ "." ~ ( #ident ~ "." )? )? ~ "*" ~ ( EXCLUDE ~ #exclude_col )?
            },
            |(res, star, opt_exclude)| qualified_wildcard_transform(res, star, opt_exclude),
        ),
        // select columns(* exclude ...)
        map(
            rule! {
              COLUMNS ~ "(" ~  ( #ident ~ "." ~ ( #ident ~ "." )? )? ~ "*" ~ ( EXCLUDE ~ #exclude_col )? ~ ")"
            },
            |(_, _, res, star, opt_exclude, _)| {
                qualified_wildcard_transform(res, star, opt_exclude)
            },
        ),
    ));

    // columns('.*abc.*')
    let columns_regexp = map(
        rule! {
            COLUMNS ~ "(" ~ #literal_string ~ ")"
        },
        |(t, _, s, _)| SelectTarget::StarColumns {
            qualified: vec![Indirection::Star(Some(t.span))],
            column_filter: Some(ColumnFilter::Lambda(Lambda {
                params: vec![Identifier::from_name(Some(t.span), "_t")],
                expr: Box::new(Expr::BinaryOp {
                    span: Some(t.span),
                    op: BinaryOperator::Regexp,
                    left: Box::new(Expr::ColumnRef {
                        span: None,
                        column: ColumnRef {
                            database: None,
                            table: None,
                            column: ColumnID::Name(Identifier::from_name(Some(t.span), "_t")),
                        },
                    }),
                    right: Box::new(Expr::Literal {
                        span: Some(t.span),
                        value: Literal::String(s),
                    }),
                }),
            })),
        },
    );

    // columns(a -> filter)
    let columns_lambda = map(
        rule! {
            COLUMNS ~ "(" ~ #ident ~ "->" ~ #subexpr(0) ~ ")"
        },
        |(t, _, ident, _, expr, _)| SelectTarget::StarColumns {
            qualified: vec![Indirection::Star(Some(t.span))],
            column_filter: Some(ColumnFilter::Lambda(Lambda {
                params: vec![ident],
                expr: Box::new(expr),
            })),
        },
    );

    let projection = map(
        rule! {
            #expr ~ #alias_name?
        },
        |(expr, alias)| SelectTarget::AliasedExpr {
            expr: Box::new(expr),
            alias,
        },
    );

    rule!(
        #qualified_wildcard
        | #columns_regexp
        | #columns_lambda
        | #projection
    )(i)
}

pub fn travel_point(i: Input) -> IResult<TimeTravelPoint> {
    let at_stream = map(
        rule! { "(" ~ STREAM ~ "=>" ~  #dot_separated_idents_1_to_3 ~ ")" },
        |(_, _, _, (catalog, database, name), _)| TimeTravelPoint::Stream {
            catalog,
            database,
            name,
        },
    );

    rule!(
        #at_stream | #at_snapshot_or_ts
    )(i)
}

pub fn at_snapshot_or_ts(i: Input) -> IResult<TimeTravelPoint> {
    let at_snapshot = map(
        rule! { "(" ~ SNAPSHOT ~ "=>" ~ #literal_string ~ ")" },
        |(_, _, _, s, _)| TimeTravelPoint::Snapshot(s),
    );
    let at_timestamp = map(
        rule! { "(" ~ TIMESTAMP ~ "=>" ~ #expr ~ ")" },
        |(_, _, _, e, _)| TimeTravelPoint::Timestamp(Box::new(e)),
    );
    let at_offset = map(
        rule! { "(" ~ OFFSET ~ "=>" ~ #expr ~ ")" },
        |(_, _, _, e, _)| TimeTravelPoint::Offset(Box::new(e)),
    );

    rule!(
        #at_snapshot | #at_timestamp | #at_offset
    )(i)
}

pub fn temporal_clause(i: Input) -> IResult<TemporalClause> {
    let time_travel = map(
        rule! {
            AT ~ ^#travel_point
        },
        |(_, travel_point)| TemporalClause::TimeTravel(travel_point),
    );

    let changes = map(
        rule! {
            CHANGES ~ "(" ~ INFORMATION ~ "=>" ~ ( DEFAULT | APPEND_ONLY ) ~ ")" ~ AT ~ ^#travel_point ~ (END ~ ^#at_snapshot_or_ts)?
        },
        |(_, _, _, _, changes_type, _, _, at_point, opt_end_point)| {
            let append_only = matches!(changes_type.kind, APPEND_ONLY);
            TemporalClause::Changes(ChangesInterval {
                append_only,
                at_point,
                end_point: opt_end_point.map(|p| p.1),
            })
        },
    );

    rule!(
        #time_travel
        | #changes
    )(i)
}

pub fn alias_name(i: Input) -> IResult<Identifier> {
    let short_alias = map(
        rule! {
            #ident
            ~ #error_hint(
                rule! { AS },
                "an alias without `AS` keyword has already been defined before this one, \
                    please remove one of them"
            )
        },
        |(ident, _)| ident,
    );
    let as_alias = map(
        rule! {
            AS ~ #ident_after_as
        },
        |(_, name)| name,
    );

    rule!(
        #short_alias
        | #as_alias
    )(i)
}

pub fn table_alias(i: Input) -> IResult<TableAlias> {
    map(
        rule! { #alias_name ~ ( "(" ~ ^#comma_separated_list1(ident) ~ ^")" )? },
        |(name, opt_columns)| TableAlias {
            name,
            columns: opt_columns.map(|(_, cols, _)| cols).unwrap_or_default(),
        },
    )(i)
}

pub fn table_alias_without_as(i: Input) -> IResult<TableAlias> {
    map(
        rule! { #ident ~ ( "(" ~ ^#comma_separated_list1(ident) ~ ^")" )? },
        |(name, opt_columns)| TableAlias {
            name,
            columns: opt_columns.map(|(_, cols, _)| cols).unwrap_or_default(),
        },
    )(i)
}

pub fn join_operator(i: Input) -> IResult<JoinOperator> {
    alt((
        value(JoinOperator::Inner, rule! { INNER }),
        value(JoinOperator::LeftSemi, rule! { LEFT? ~ SEMI }),
        value(JoinOperator::RightSemi, rule! { RIGHT ~ SEMI }),
        value(JoinOperator::LeftAnti, rule! { LEFT? ~ ANTI }),
        value(JoinOperator::RightAnti, rule! { RIGHT ~ ANTI }),
        value(JoinOperator::LeftOuter, rule! { LEFT ~ OUTER? }),
        value(JoinOperator::RightOuter, rule! { RIGHT ~ OUTER? }),
        value(JoinOperator::FullOuter, rule! { FULL ~ OUTER? }),
        value(JoinOperator::CrossJoin, rule! { CROSS }),
    ))(i)
}

pub fn order_by_expr(i: Input) -> IResult<OrderByExpr> {
    let nulls_first = map(
        rule! {
            NULLS ~ ( FIRST | LAST )
        },
        |(_, first_last)| first_last.kind == FIRST,
    );

    map(
        rule! {
            #expr ~ ( ASC | DESC )? ~ #nulls_first?
        },
        |(expr, opt_asc, opt_nulls_first)| OrderByExpr {
            expr,
            asc: opt_asc.map(|asc| asc.kind == ASC),
            nulls_first: opt_nulls_first,
        },
    )(i)
}

pub fn table_reference(i: Input) -> IResult<TableReference> {
    let (rest, table_reference_elements) = rule! { #table_reference_element+ }(i)?;
    let iter = &mut table_reference_elements.into_iter();
    run_pratt_parser(TableReferenceParser, iter, rest, i)
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableFunctionParam {
    // func(name => arg)
    Named { name: Identifier, value: Expr },
    // func(arg)
    Normal(Expr),
}

pub fn table_function_param(i: Input) -> IResult<TableFunctionParam> {
    let named = map(rule! { #ident ~ "=>" ~ #expr  }, |(name, _, value)| {
        TableFunctionParam::Named { name, value }
    });
    let normal = map(rule! { #expr }, TableFunctionParam::Normal);

    rule!(
        #named | #normal
    )(i)
}

#[derive(Debug, Clone, PartialEq)]
pub enum TableReferenceElement {
    Table {
        catalog: Option<Identifier>,
        database: Option<Identifier>,
        table: Identifier,
        alias: Option<TableAlias>,
        temporal: Option<TemporalClause>,
        consume: bool,
        pivot: Option<Box<Pivot>>,
        unpivot: Option<Box<Unpivot>>,
        sample: Option<Sample>,
    },
    // `TABLE(expr)[ AS alias ]`
    TableFunction {
        /// If the table function is a lateral table function
        lateral: bool,
        name: Identifier,
        params: Vec<TableFunctionParam>,
        alias: Option<TableAlias>,
    },
    // Derived table, which can be a subquery or joined tables or combination of them
    Subquery {
        /// If the subquery is a lateral subquery
        lateral: bool,
        subquery: Box<Query>,
        alias: Option<TableAlias>,
    },
    // [NATURAL] [INNER|OUTER|CROSS|...] JOIN
    Join {
        op: JoinOperator,
        natural: bool,
    },
    // ON expr | USING (ident, ...)
    JoinCondition(JoinCondition),
    Group(TableReference),
    Stage {
        location: FileLocation,
        options: Vec<SelectStageOption>,
        alias: Option<TableAlias>,
    },
}

pub fn table_reference_element(i: Input) -> IResult<WithSpan<TableReferenceElement>> {
    // PIVOT(expr FOR col IN (ident, ...))
    let pivot = map(
        rule! {
           PIVOT ~ "(" ~ #expr ~ FOR ~ #ident ~ IN ~ "(" ~ #comma_separated_list1(expr) ~ ")" ~ ")"
        },
        |(_pivot, _, aggregate, _for, value_column, _in, _, values, _, _)| Pivot {
            aggregate,
            value_column,
            values,
        },
    );
    // UNPIVOT(ident for ident IN (ident, ...))
    let unpivot = map(
        rule! {
            UNPIVOT ~ "(" ~ #ident ~ FOR ~ #ident ~ IN ~ "(" ~ #comma_separated_list1(ident) ~ ")" ~ ")"
        },
        |(_unpivot, _, value_column, _for, column_name, _in, _, names, _, _)| Unpivot {
            value_column,
            column_name,
            names,
        },
    );
    let aliased_table = map(
        rule! {
            #dot_separated_idents_1_to_3 ~ #temporal_clause? ~ (WITH ~ CONSUME)? ~ #table_alias? ~ #pivot? ~ #unpivot? ~ SAMPLE? ~ (ROW | BLOCK)? ~ ("(" ~ #expr ~ ROWS? ~ ")")?
        },
        |(
            (catalog, database, table),
            temporal,
            opt_consume,
            alias,
            pivot,
            unpivot,
            sample,
            level,
            sample_conf,
        )| {
            let mut table_sample = None;
            if sample.is_some() {
                let sample_level = match level {
                    // If the sample level is not specified, it defaults to ROW
                    Some(level) => match level.kind {
                        ROW => SampleLevel::ROW,
                        BLOCK => SampleLevel::BLOCK,
                        _ => unreachable!(),
                    },
                    None => SampleLevel::ROW,
                };
                let mut default_sample_conf = SampleConfig::Probability(Literal::Float64(100.0));
                if let Some((_, expr, rows, _)) = sample_conf {
                    if let Expr::Literal { value, .. } = expr {
                        default_sample_conf = if rows.is_some() {
                            SampleConfig::RowsNum(value)
                        } else {
                            SampleConfig::Probability(value)
                        };
                    }
                }
                table_sample = Some(Sample {
                    sample_level,
                    sample_conf: default_sample_conf,
                })
            };
            TableReferenceElement::Table {
                catalog,
                database,
                table,
                alias,
                temporal,
                consume: opt_consume.is_some(),
                pivot: pivot.map(Box::new),
                unpivot: unpivot.map(Box::new),
                sample: table_sample,
            }
        },
    );
    let join = map(
        rule! {
            NATURAL? ~ #join_operator? ~ JOIN
        },
        |(opt_natural, opt_op, _)| TableReferenceElement::Join {
            op: opt_op.unwrap_or(JoinOperator::Inner),
            natural: opt_natural.is_some(),
        },
    );
    let join_condition_on = map(
        rule! {
            ON ~ #expr
        },
        |(_, expr)| TableReferenceElement::JoinCondition(JoinCondition::On(Box::new(expr))),
    );
    let join_condition_using = map(
        rule! {
            USING ~ "(" ~ #comma_separated_list1(ident) ~ ")"
        },
        |(_, _, idents, _)| TableReferenceElement::JoinCondition(JoinCondition::Using(idents)),
    );
    let table_function = map(
        rule! {
            LATERAL? ~ #function_name ~ "(" ~ #comma_separated_list0(table_function_param) ~ ")" ~ #table_alias?
        },
        |(lateral, name, _, params, _, alias)| TableReferenceElement::TableFunction {
            lateral: lateral.is_some(),
            name,
            params,
            alias,
        },
    );
    let subquery = map(
        rule! {
            LATERAL? ~ "(" ~ #query ~ ")" ~ #table_alias?
        },
        |(lateral, _, subquery, _, alias)| TableReferenceElement::Subquery {
            lateral: lateral.is_some(),
            subquery: Box::new(subquery),
            alias,
        },
    );

    let group = map(
        rule! {
           "(" ~ #table_reference ~ ^")"
        },
        |(_, table_ref, _)| TableReferenceElement::Group(table_ref),
    );
    let aliased_stage = map(
        rule! {
            #file_location ~  ( "(" ~ (#select_stage_option ~ ","?)* ~ ^")" )? ~ #table_alias?
        },
        |(location, options, alias)| {
            let options = options
                .map(|(_, options, _)| options.into_iter().map(|(option, _)| option).collect())
                .unwrap_or_default();
            TableReferenceElement::Stage {
                location,
                alias,
                options,
            }
        },
    );

    let (rest, (span, elem)) = consumed(rule! {
        #aliased_stage
        | #table_function
        | #aliased_table
        | #subquery
        | #group
        | #join
        | #join_condition_on
        | #join_condition_using
    })(i)?;
    Ok((rest, WithSpan { span, elem }))
}

struct TableReferenceParser;

impl<'a, I: Iterator<Item = WithSpan<'a, TableReferenceElement>>> PrattParser<I>
    for TableReferenceParser
{
    type Error = &'static str;
    type Input = WithSpan<'a, TableReferenceElement>;
    type Output = TableReference;

    fn query(&mut self, input: &Self::Input) -> Result<Affix, &'static str> {
        let affix = match &input.elem {
            TableReferenceElement::Join { .. } => Affix::Infix(Precedence(10), Associativity::Left),
            TableReferenceElement::JoinCondition(..) => Affix::Postfix(Precedence(5)),
            _ => Affix::Nilfix,
        };
        Ok(affix)
    }

    fn primary(&mut self, input: Self::Input) -> Result<Self::Output, &'static str> {
        let table_ref = match input.elem {
            TableReferenceElement::Group(table_ref) => table_ref,
            TableReferenceElement::Table {
                catalog,
                database,
                table,
                alias,
                temporal,
                consume,
                pivot,
                unpivot,
                sample,
            } => TableReference::Table {
                span: transform_span(input.span.tokens),
                catalog,
                database,
                table,
                alias,
                temporal,
                consume,
                pivot,
                unpivot,
                sample,
            },
            TableReferenceElement::TableFunction {
                lateral,
                name,
                params,
                alias,
            } => {
                let normal_params = params
                    .iter()
                    .filter_map(|p| match p {
                        TableFunctionParam::Normal(p) => Some(p.clone()),
                        _ => None,
                    })
                    .collect();
                let named_params = params
                    .into_iter()
                    .filter_map(|p| match p {
                        TableFunctionParam::Named { name, value } => Some((name, value)),
                        _ => None,
                    })
                    .collect();
                TableReference::TableFunction {
                    span: transform_span(input.span.tokens),
                    lateral,
                    name,
                    params: normal_params,
                    named_params,
                    alias,
                }
            }
            TableReferenceElement::Subquery {
                lateral,
                subquery,
                alias,
            } => TableReference::Subquery {
                span: transform_span(input.span.tokens),
                lateral,
                subquery,
                alias,
            },
            TableReferenceElement::Stage {
                location,
                options,
                alias,
            } => {
                let options = SelectStageOptions::from(options);
                TableReference::Location {
                    span: transform_span(input.span.tokens),
                    location,
                    options,
                    alias,
                }
            }
            _ => unreachable!(),
        };
        Ok(table_ref)
    }

    fn infix(
        &mut self,
        lhs: Self::Output,
        input: Self::Input,
        rhs: Self::Output,
    ) -> Result<Self::Output, &'static str> {
        let table_ref = match input.elem {
            TableReferenceElement::Join { op, natural } => {
                let condition = if natural {
                    JoinCondition::Natural
                } else {
                    JoinCondition::None
                };
                TableReference::Join {
                    span: transform_span(input.span.tokens),
                    join: Join {
                        op,
                        condition,
                        left: Box::new(lhs),
                        right: Box::new(rhs),
                    },
                }
            }
            _ => unreachable!(),
        };
        Ok(table_ref)
    }

    fn prefix(
        &mut self,
        _op: Self::Input,
        _rhs: Self::Output,
    ) -> Result<Self::Output, Self::Error> {
        unreachable!()
    }

    fn postfix(
        &mut self,
        mut lhs: Self::Output,
        op: Self::Input,
    ) -> Result<Self::Output, Self::Error> {
        match op.elem {
            TableReferenceElement::JoinCondition(new_condition) => match &mut lhs {
                TableReference::Join {
                    join: Join { condition, .. },
                    ..
                } => match *condition {
                    JoinCondition::None => {
                        *condition = new_condition;
                        Ok(lhs)
                    }
                    JoinCondition::Natural => Err("join condition conflicting with NATURAL"),
                    _ => Err("join condition already set"),
                },
                _ => Err("join condition must apply to a join"),
            },
            _ => unreachable!(),
        }
    }
}

pub fn group_by_items(i: Input) -> IResult<GroupBy> {
    let normal = map(rule! { ^#comma_separated_list1(expr) }, |groups| {
        GroupBy::Normal(groups)
    });

    let all = map(rule! { ALL }, |_| GroupBy::All);

    let cube = map(
        rule! { CUBE ~ "(" ~ ^#comma_separated_list1(expr) ~ ")" },
        |(_, _, groups, _)| GroupBy::Cube(groups),
    );
    let rollup = map(
        rule! { ROLLUP ~ "(" ~ ^#comma_separated_list1(expr) ~ ")" },
        |(_, _, groups, _)| GroupBy::Rollup(groups),
    );
    let group_set = alt((
        map(rule! {"(" ~ ")"}, |(_, _)| vec![]), // empty grouping set
        map(
            rule! {"(" ~ #comma_separated_list1(expr) ~ ")"},
            |(_, sets, _)| sets,
        ),
        map(rule! { #expr }, |e| vec![e]),
    ));
    let group_sets = map(
        rule! { GROUPING ~ SETS ~ "(" ~ ^#comma_separated_list1(group_set) ~ ")"  },
        |(_, _, _, sets, _)| GroupBy::GroupingSets(sets),
    );
    rule!(#all | #group_sets | #cube | #rollup | #normal)(i)
}

pub fn window_frame_bound(i: Input) -> IResult<WindowFrameBound> {
    alt((
        value(WindowFrameBound::CurrentRow, rule! { CURRENT ~ ROW }),
        value(
            WindowFrameBound::Preceding(None),
            rule! { UNBOUNDED ~ PRECEDING },
        ),
        map(rule! { #subexpr(0) ~ PRECEDING }, |(expr, _)| {
            WindowFrameBound::Preceding(Some(Box::new(expr)))
        }),
        value(
            WindowFrameBound::Following(None),
            rule! { UNBOUNDED ~ FOLLOWING },
        ),
        map(rule! { #subexpr(0) ~ FOLLOWING }, |(expr, _)| {
            WindowFrameBound::Following(Some(Box::new(expr)))
        }),
    ))(i)
}

pub fn window_frame_between(i: Input) -> IResult<(WindowFrameBound, WindowFrameBound)> {
    alt((
        map(
            rule! { BETWEEN ~ #window_frame_bound ~ AND ~ #window_frame_bound },
            |(_, s, _, e)| (s, e),
        ),
        map(rule! { #window_frame_bound }, |s| {
            (s, WindowFrameBound::CurrentRow)
        }),
    ))(i)
}

pub fn window_spec(i: Input) -> IResult<WindowSpec> {
    map(
        rule! {
            #ident?
            ~ ( PARTITION ~ ^BY ~ ^#comma_separated_list1(subexpr(0)) )?
            ~ ( ORDER ~ ^BY ~ ^#comma_separated_list1(order_by_expr) )?
            ~ ( (ROWS | RANGE) ~ ^#window_frame_between )?
        },
        |(existing_window_name, opt_partition, opt_order, between)| WindowSpec {
            existing_window_name,
            partition_by: opt_partition.map(|x| x.2).unwrap_or_default(),
            order_by: opt_order.map(|x| x.2).unwrap_or_default(),
            window_frame: between.map(|x| {
                let unit = match x.0.kind {
                    ROWS => WindowFrameUnits::Rows,
                    RANGE => WindowFrameUnits::Range,
                    _ => unreachable!(),
                };
                let bw = x.1;
                WindowFrame {
                    units: unit,
                    start_bound: bw.0,
                    end_bound: bw.1,
                }
            }),
        },
    )(i)
}

pub fn window_spec_ident(i: Input) -> IResult<Window> {
    alt((
        map(
            rule! {
               "(" ~ #window_spec ~ ")"
            },
            |(_, spec, _)| Window::WindowSpec(spec),
        ),
        map(
            rule! {
                #ident
            },
            |window_name| Window::WindowReference(WindowRef { window_name }),
        ),
    ))(i)
}

pub fn window_function(i: Input) -> IResult<WindowDesc> {
    map(
        rule! {
        (( IGNORE | RESPECT ) ~ NULLS)? ~ (OVER ~ #window_spec_ident)
        },
        |(opt_ignore_nulls, window)| WindowDesc {
            ignore_nulls: opt_ignore_nulls.map(|key| key.0.kind == IGNORE),
            window: window.1,
        },
    )(i)
}

pub fn window_clause(i: Input) -> IResult<WindowDefinition> {
    map(
        rule! {
            #ident ~ AS ~ "(" ~ #window_spec ~ ")"
        },
        |(ident, _, _, window, _)| WindowDefinition {
            name: ident,
            spec: window,
        },
    )(i)
}
