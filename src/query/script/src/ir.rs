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

use std::fmt;
use std::fmt::Display;
use std::hash::Hash;

use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::Statement;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::Span;
use derive_visitor::DriveMut;
use derive_visitor::VisitorMut;

pub type VarRef = Ref<0>;
pub type SetRef = Ref<1>;
pub type IterRef = Ref<2>;
pub type LabelRef = Ref<3>;

#[derive(Debug, Clone)]
pub struct Ref<const REFKIND: usize> {
    pub span: Span,
    pub index: usize,
    pub display_name: String,
}

impl<const REFKIND: usize> PartialEq for Ref<REFKIND> {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index
    }
}

impl<const REFKIND: usize> Eq for Ref<REFKIND> {}

impl<const REFKIND: usize> Hash for Ref<REFKIND> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.index.hash(state);
    }
}

impl<const REFKIND: usize> Display for Ref<REFKIND> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}({})", self.display_name, self.index)
    }
}

#[derive(Default)]
pub struct RefAllocator {
    next_index: usize,
}

impl<const REFKIND: usize> Ref<REFKIND> {
    pub fn new(span: Span, name: &str, allocator: &mut RefAllocator) -> Self {
        let index = allocator.next_index;
        allocator.next_index += 1;
        Ref {
            span,
            index,
            display_name: name.to_string(),
        }
    }

    pub fn new_internal(span: Span, hint: &str, allocator: &mut RefAllocator) -> Self {
        let index = allocator.next_index;
        allocator.next_index += 1;
        Ref {
            span,
            index,
            display_name: format!("__{hint}{index}"),
        }
    }

    pub fn placeholder(index: usize) -> Self {
        Ref {
            span: None,
            index,
            display_name: format!(":{}", index),
        }
    }
}

#[derive(Debug, Clone)]
pub enum ScriptIR {
    /// Executes a SQL query and stores the result in a named result set.
    Query {
        stmt: StatementTemplate,
        to_set: SetRef,
    },
    /// Initializes an iterator for a given result set.
    Iter { set: SetRef, to_iter: IterRef },
    /// Retrieves a cell value in the current row to a variable.
    Read {
        iter: IterRef,
        column: ColumnAccess,
        to_var: VarRef,
    },
    /// Forward the iterator to the next line.
    Next { iter: IterRef },
    /// Defines a label.
    Label { label: LabelRef },
    /// Jumps to a specified label if the iterator has reached the end of the result set.
    JumpIfEnded { iter: IterRef, to_label: LabelRef },
    /// Jumps to a specified label if the condition is true.
    JumpIfTrue {
        condition: VarRef,
        to_label: LabelRef,
    },
    /// Uncoditionally jumps to a specified label.
    Goto { to_label: LabelRef },
    /// Returns from the script.
    Return,
    /// Returns a variable from the script.
    ReturnVar { var: VarRef },
    /// Returns a result set from the script.
    ReturnSet { set: SetRef },
}

impl Display for ScriptIR {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScriptIR::Query {
                stmt: query,
                to_set,
            } => write!(f, "QUERY {query}, {to_set}")?,
            ScriptIR::Iter { set, to_iter } => write!(f, "ITER {set}, {to_iter}")?,
            ScriptIR::Read {
                iter,
                column,
                to_var,
            } => write!(f, "READ {iter}, {column}, {to_var}")?,
            ScriptIR::Next { iter } => {
                write!(f, "NEXT {iter}")?;
            }
            ScriptIR::Label { label } => write!(f, "{label}:")?,
            ScriptIR::JumpIfEnded { iter, to_label } => {
                write!(f, "JUMP_IF_ENDED {iter}, {to_label}")?
            }
            ScriptIR::JumpIfTrue {
                condition,
                to_label,
            } => write!(f, "JUMP_IF_TRUE {condition}, {to_label}")?,
            ScriptIR::Goto { to_label } => write!(f, "GOTO {to_label}")?,
            ScriptIR::Return => write!(f, "RETURN")?,
            ScriptIR::ReturnVar { var } => write!(f, "RETURN {var}")?,
            ScriptIR::ReturnSet { set } => write!(f, "RETURN {set}")?,
        };
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum ColumnAccess {
    Position(usize),
    Name(String),
}

impl Display for ColumnAccess {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColumnAccess::Position(index) => write!(f, "${}", index),
            ColumnAccess::Name(name) => write!(f, "\"{}\"", name),
        }
    }
}

impl Display for StatementTemplate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.stmt)
    }
}

#[derive(Debug, Clone)]
pub struct StatementTemplate {
    pub span: Span,
    pub stmt: Statement,
}

impl StatementTemplate {
    pub fn new(span: Span, stmt: Statement) -> Self {
        StatementTemplate { span, stmt }
    }

    pub fn subst(&self, lookup_var: impl Fn(VarRef) -> Result<Literal>) -> Result<Statement> {
        #[derive(VisitorMut)]
        #[visitor(Expr(enter), Identifier(enter))]
        struct SubstVisitor<'a> {
            lookup_var: &'a dyn Fn(VarRef) -> Result<Literal>,
            error: Option<ErrorCode>,
        }

        impl SubstVisitor<'_> {
            fn enter_expr(&mut self, expr: &mut Expr) {
                if let Expr::Hole { span, name } = expr {
                    let index = name.parse::<usize>().unwrap();
                    let value = (self.lookup_var)(VarRef::placeholder(index));
                    match value {
                        Ok(value) => {
                            *expr = Expr::Literal { span: *span, value };
                        }
                        Err(e) => {
                            self.error = Some(e.set_span(*span));
                        }
                    }
                }
            }

            fn enter_identifier(&mut self, ident: &mut Identifier) {
                if ident.is_hole {
                    let index = ident.name.parse::<usize>().unwrap();
                    let value = (self.lookup_var)(VarRef::placeholder(index));
                    match value {
                        Ok(Literal::String(name)) => {
                            *ident = Identifier::from_name(ident.span, name);
                        }
                        Ok(value) => {
                            self.error = Some(
                                ErrorCode::ScriptSemanticError(format!(
                                    "expected string literal, got {value}"
                                ))
                                .set_span(ident.span),
                            );
                        }
                        Err(e) => {
                            self.error = Some(e.set_span(ident.span));
                        }
                    }
                }
            }
        }

        let mut stmt = self.stmt.clone();
        let mut visitor = SubstVisitor {
            lookup_var: &lookup_var,
            error: None,
        };
        stmt.drive_mut(&mut visitor);

        if let Some(e) = visitor.error {
            return Err(e);
        }

        Ok(stmt)
    }
}
