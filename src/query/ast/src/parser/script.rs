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

use nom::combinator::consumed;
use nom::combinator::map;

use crate::ast::*;
use crate::parser::common::*;
use crate::parser::expr::*;
use crate::parser::input::Input;
use crate::parser::statement::*;
use crate::parser::token::*;
use crate::rule;

pub fn script_stmts(i: Input) -> IResult<Vec<ScriptStatement>> {
    semicolon_terminated_list1(script_stmt)(i)
}

pub fn script_stmt(i: Input) -> IResult<ScriptStatement> {
    let let_stmt_stmt = map(
        consumed(rule! {
            LET ~^#ident ~ RESULTSET ~ ^":=" ~ ^#statement_body
        }),
        |(span, (_, name, _, _, stmt))| ScriptStatement::LetStatement {
            span: transform_span(span.tokens),
            declare: StatementDeclare { name, stmt },
        },
    );
    let let_var_stmt = map(
        consumed(rule! {
            LET ~^#ident ~ #type_name? ~ ^":=" ~ ^#expr
        }),
        |(span, (_, name, data_type, _, default))| ScriptStatement::LetVar {
            span: transform_span(span.tokens),
            declare: VariableDeclare {
                name,
                data_type,
                default,
            },
        },
    );
    let run_stmt = map(
        consumed(rule! {
            #statement_body
        }),
        |(span, stmt)| ScriptStatement::RunStatement {
            span: transform_span(span.tokens),
            stmt,
        },
    );
    let assign_stmt = map(
        consumed(rule! {
            #ident ~ ":=" ~ ^#expr
        }),
        |(span, (name, _, value))| ScriptStatement::Assign {
            span: transform_span(span.tokens),
            name,
            value,
        },
    );
    let return_stmt = map(
        consumed(rule! {
            RETURN ~ #expr?
        }),
        |(span, (_, value))| ScriptStatement::Return {
            span: transform_span(span.tokens),
            value,
        },
    );
    let for_loop_stmt = map(
        consumed(rule! {
            FOR ~ ^#ident ~ ^IN ~ REVERSE?
            ~ #expr ~ TO ~ #expr ~ ^DO
            ~ ^#semicolon_terminated_list1(script_stmt)
            ~ ^END ~ ^FOR ~ #ident?
        }),
        |(
            span,
            (_, variable, _, is_reverse, lower_bound, _, upper_bound, _, body, _, _, label),
        )| ScriptStatement::ForLoop {
            span: transform_span(span.tokens),
            variable,
            is_reverse: is_reverse.is_some(),
            lower_bound,
            upper_bound,
            body,
            label,
        },
    );
    let for_in_stmt = map(
        consumed(rule! {
            FOR ~ ^#ident ~ ^IN ~ #ident ~ ^DO
            ~ ^#semicolon_terminated_list1(script_stmt)
            ~ ^END ~ ^FOR ~ #ident?
        }),
        |(span, (_, variable, _, resultset, _, body, _, _, label))| ScriptStatement::ForIn {
            span: transform_span(span.tokens),
            variable,
            resultset,
            body,
            label,
        },
    );
    let while_loop_stmt = map(
        consumed(rule! {
            WHILE ~ ^#expr ~ ^DO
            ~ ^#semicolon_terminated_list1(script_stmt)
            ~ ^END ~ ^WHILE ~ #ident?
        }),
        |(span, (_, condition, _, body, _, _, label))| ScriptStatement::WhileLoop {
            span: transform_span(span.tokens),
            condition,
            body,
            label,
        },
    );
    let repeat_loop_stmt = map(
        consumed(rule! {
            REPEAT
            ~ ^#semicolon_terminated_list1(script_stmt)
            ~ ^UNTIL ~ ^#expr
            ~ ^END ~ ^REPEAT ~ #ident?
        }),
        |(span, (_, body, _, until_condition, _, _, label))| ScriptStatement::RepeatLoop {
            span: transform_span(span.tokens),
            body,
            until_condition,
            label,
        },
    );
    let loop_stmt = map(
        consumed(rule! {
            LOOP ~ ^#semicolon_terminated_list1(script_stmt) ~ ^END ~ ^LOOP ~ #ident?
        }),
        |(span, (_, body, _, _, label))| ScriptStatement::Loop {
            span: transform_span(span.tokens),
            body,
            label,
        },
    );
    let break_stmt = map(
        consumed(rule! {
            BREAK ~ #ident?
        }),
        |(span, (_, label))| ScriptStatement::Break {
            span: transform_span(span.tokens),
            label,
        },
    );
    let continue_stmt = map(
        consumed(rule! {
            CONTINUE ~ #ident?
        }),
        |(span, (_, label))| ScriptStatement::Continue {
            span: transform_span(span.tokens),
            label,
        },
    );
    let case_stmt = map(
        consumed(rule! {
            CASE ~ #expr?
            ~ ( WHEN ~ ^#expr ~ ^THEN ~ ^#semicolon_terminated_list1(script_stmt) )+
            ~ ( ELSE ~ ^#semicolon_terminated_list1(script_stmt) )?
            ~ ^END ~ CASE?
        }),
        |(span, (_, operand, branches, else_result, _, _))| {
            let (conditions, results) = branches
                .into_iter()
                .map(|(_, cond, _, result)| (cond, result))
                .unzip();
            let else_result = else_result.map(|(_, result)| result);
            ScriptStatement::Case {
                span: transform_span(span.tokens),
                operand,
                conditions,
                results,
                else_result,
            }
        },
    );
    let if_stmt = map(
        consumed(rule! {
            IF ~ ^#expr ~ ^THEN ~ ^#semicolon_terminated_list1(script_stmt)
            ~ ( ELSEIF ~ ^#expr ~ ^THEN ~ ^#semicolon_terminated_list1(script_stmt) )*
            ~ ( ELSE ~ ^#semicolon_terminated_list1(script_stmt) )?
            ~ ^END ~ ^IF
        }),
        |(span, (_, condition, _, result, else_ifs, else_result, _, _))| {
            let (mut conditions, mut results) = (vec![condition], vec![result]);
            for (_, cond, _, result) in else_ifs {
                conditions.push(cond);
                results.push(result);
            }
            let else_result = else_result.map(|(_, result)| result);
            ScriptStatement::If {
                span: transform_span(span.tokens),
                conditions,
                results,
                else_result,
            }
        },
    );

    rule!(
        #let_stmt_stmt
        | #let_var_stmt
        | #run_stmt
        | #assign_stmt
        | #return_stmt
        | #for_loop_stmt
        | #for_in_stmt
        | #while_loop_stmt
        | #repeat_loop_stmt
        | #loop_stmt
        | #break_stmt
        | #continue_stmt
        | #case_stmt
        | #if_stmt
    )(i)
}
