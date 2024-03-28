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

use databend_common_exception::Span;

use crate::ast::Expr;
use crate::ast::Identifier;
use crate::ast::Statement;
use crate::ast::TypeName;

const INDENT_DEPTH: usize = 4;

#[derive(Debug, Clone, PartialEq)]
pub struct CreateStoredProceduer {
    pub or_replace: bool,
    pub name: Identifier,
    pub returns: TypeName,
    pub body: ScriptBody,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ScriptBody {
    declare: Vec<VariableDeclare>,
    body: Vec<ScriptStatement>,
    exception_body: Option<ScriptStatement>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VariableDeclare {
    pub name: Identifier,
    pub data_type: Option<TypeName>,
    pub default: Expr,
}

impl Display for VariableDeclare {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let VariableDeclare {
            name,
            data_type,
            default,
        } = self;
        if let Some(data_type) = data_type {
            write!(f, "{name} {data_type} := {default}")?;
        } else {
            write!(f, "{name} := {default}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct StatementDeclare {
    pub name: Identifier,
    pub stmt: Statement,
}

impl Display for StatementDeclare {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let StatementDeclare { name, stmt } = self;
        write!(f, "{name} RESULTSET := {stmt}")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ScriptStatement {
    LetVar {
        span: Span,
        declare: VariableDeclare,
    },
    LetStatement {
        span: Span,
        declare: StatementDeclare,
    },
    RunStatement {
        span: Span,
        stmt: Statement,
    },
    Assign {
        span: Span,
        name: Identifier,
        value: Expr,
    },
    Return {
        span: Span,
        value: Option<Expr>,
    },
    ForLoop {
        span: Span,
        variable: Identifier,
        is_reverse: bool,
        lower_bound: Expr,
        upper_bound: Expr,
        body: Vec<ScriptStatement>,
        label: Option<Identifier>,
    },
    ForIn {
        span: Span,
        variable: Identifier,
        resultset: Identifier,
        body: Vec<ScriptStatement>,
        label: Option<Identifier>,
    },
    WhileLoop {
        span: Span,
        condition: Expr,
        body: Vec<ScriptStatement>,
        label: Option<Identifier>,
    },
    RepeatLoop {
        span: Span,
        body: Vec<ScriptStatement>,
        until_condition: Expr,
        label: Option<Identifier>,
    },
    Loop {
        span: Span,
        body: Vec<ScriptStatement>,
        label: Option<Identifier>,
    },
    Break {
        span: Span,
        label: Option<Identifier>,
    },
    Continue {
        span: Span,
        label: Option<Identifier>,
    },
    Case {
        span: Span,
        operand: Option<Expr>,
        conditions: Vec<Expr>,
        results: Vec<Vec<ScriptStatement>>,
        else_result: Option<Vec<ScriptStatement>>,
    },
    If {
        span: Span,
        conditions: Vec<Expr>,
        results: Vec<Vec<ScriptStatement>>,
        else_result: Option<Vec<ScriptStatement>>,
    },
}

impl Display for ScriptStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ScriptStatement::LetVar { declare, .. } => write!(f, "LET {declare}"),
            ScriptStatement::LetStatement { declare, .. } => write!(f, "LET {declare}"),
            ScriptStatement::RunStatement { stmt, .. } => write!(f, "{stmt}"),
            ScriptStatement::Assign { name, value, .. } => write!(f, "{name} := {value}"),
            ScriptStatement::Return { value, .. } => {
                if let Some(value) = value {
                    write!(f, "RETURN {value}")
                } else {
                    write!(f, "RETURN")
                }
            }
            ScriptStatement::ForLoop {
                variable,
                is_reverse,
                lower_bound,
                upper_bound,
                body,
                label,
                ..
            } => {
                let reverse = if *is_reverse { " REVERSE" } else { "" };
                writeln!(
                    f,
                    "FOR {variable} IN{reverse} {lower_bound} TO {upper_bound} DO"
                )?;
                for stmt in body {
                    writeln!(
                        f,
                        "{}",
                        indent::indent_all_by(INDENT_DEPTH, format!("{stmt};"))
                    )?;
                }
                write!(f, "END FOR")?;
                if let Some(label) = label {
                    write!(f, " {label}")?;
                }
                Ok(())
            }
            ScriptStatement::ForIn {
                variable,
                resultset,
                body,
                label,
                ..
            } => {
                writeln!(f, "FOR {variable} IN {resultset} DO")?;
                for stmt in body {
                    writeln!(
                        f,
                        "{}",
                        indent::indent_all_by(INDENT_DEPTH, format!("{stmt};"))
                    )?;
                }
                write!(f, "END FOR")?;
                if let Some(label) = label {
                    write!(f, " {label}")?;
                }
                Ok(())
            }
            ScriptStatement::WhileLoop {
                condition,
                body,
                label,
                ..
            } => {
                writeln!(f, "WHILE {condition} DO")?;
                for stmt in body {
                    writeln!(
                        f,
                        "{}",
                        indent::indent_all_by(INDENT_DEPTH, format!("{stmt};"))
                    )?;
                }
                write!(f, "END WHILE")?;
                if let Some(label) = label {
                    write!(f, " {label}")?;
                }
                Ok(())
            }
            ScriptStatement::RepeatLoop {
                until_condition,
                body,
                label,
                ..
            } => {
                writeln!(f, "REPEAT")?;
                for stmt in body {
                    writeln!(
                        f,
                        "{}",
                        indent::indent_all_by(INDENT_DEPTH, format!("{stmt};"))
                    )?;
                }
                writeln!(f, "UNTIL {until_condition}")?;
                write!(f, "END REPEAT")?;
                if let Some(label) = label {
                    write!(f, " {label}")?;
                }
                Ok(())
            }
            ScriptStatement::Loop { body, label, .. } => {
                writeln!(f, "LOOP")?;
                for stmt in body {
                    writeln!(
                        f,
                        "{}",
                        indent::indent_all_by(INDENT_DEPTH, format!("{stmt};"))
                    )?;
                }
                write!(f, "END LOOP")?;
                if let Some(label) = label {
                    write!(f, " {label}")?;
                }
                Ok(())
            }
            ScriptStatement::Break { label, .. } => {
                write!(f, "BREAK")?;
                if let Some(label) = label {
                    write!(f, " {label}")?;
                }
                Ok(())
            }
            ScriptStatement::Continue { label, .. } => {
                write!(f, "CONTINUE")?;
                if let Some(label) = label {
                    write!(f, " {label}")?;
                }
                Ok(())
            }
            ScriptStatement::Case {
                operand,
                conditions,
                results,
                else_result,
                ..
            } => {
                if let Some(operand) = operand {
                    writeln!(f, "CASE {operand}")?;
                } else {
                    writeln!(f, "CASE")?;
                }
                for (condition, result) in conditions.iter().zip(results.iter()) {
                    writeln!(f, "{:INDENT_DEPTH$}WHEN {condition} THEN", " ")?;
                    for stmt in result {
                        writeln!(
                            f,
                            "{}",
                            indent::indent_all_by(INDENT_DEPTH * 2, format!("{stmt};"))
                        )?;
                    }
                }
                if let Some(else_result) = else_result {
                    writeln!(f, "{:INDENT_DEPTH$}ELSE", " ")?;
                    for stmt in else_result {
                        writeln!(
                            f,
                            "{}",
                            indent::indent_all_by(INDENT_DEPTH * 2, format!("{stmt};"))
                        )?;
                    }
                }
                write!(f, "END CASE")
            }
            ScriptStatement::If {
                conditions,
                results,
                else_result,
                ..
            } => {
                for (i, (condition, result)) in conditions.iter().zip(results.iter()).enumerate() {
                    if i == 0 {
                        writeln!(f, "IF {condition} THEN")?;
                    } else {
                        writeln!(f, "ELSEIF {condition} THEN")?;
                    }
                    for stmt in result {
                        writeln!(
                            f,
                            "{}",
                            indent::indent_all_by(INDENT_DEPTH, format!("{stmt};"))
                        )?;
                    }
                }
                if let Some(else_result) = else_result {
                    writeln!(f, "ELSE")?;
                    for stmt in else_result {
                        writeln!(
                            f,
                            "{}",
                            indent::indent_all_by(INDENT_DEPTH, format!("{stmt};"))
                        )?;
                    }
                }
                write!(f, "END IF")
            }
        }
    }
}
