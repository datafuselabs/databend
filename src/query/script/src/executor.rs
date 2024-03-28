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

use databend_common_ast::ast::Literal;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::ir::ColumnAccess;
use crate::ir::IterRef;
use crate::ir::LabelRef;
use crate::ir::ScriptIR;
use crate::ir::SetRef;
use crate::ir::VarRef;

pub trait Client {
    type Scalar: Clone;
    type DataBlock: Clone;

    fn query(&self, query: &str) -> Result<Self::DataBlock>;
    fn scalar_to_literal(&self, scalar: &Self::Scalar) -> Literal;
    fn read_from_block(
        &self,
        block: &Self::DataBlock,
        row: usize,
        col: &ColumnAccess,
    ) -> Self::Scalar;
    fn block_len(&self, block: &Self::DataBlock) -> usize;
    fn is_true(&self, scalar: &Self::Scalar) -> bool;
}

#[derive(Debug, Clone)]
pub enum ReturnValue<C: Client> {
    Var(C::Scalar),
    Set(C::DataBlock),
}

#[derive(Debug)]
struct Cursor {
    set: SetRef,
    row: usize,
    len: usize,
}

#[derive(Debug)]
pub struct Executor<C: Client> {
    client: C,
    code: Vec<ScriptIR>,
    max_steps: usize,
    vars: HashMap<VarRef, C::Scalar>,
    sets: HashMap<SetRef, C::DataBlock>,
    iters: HashMap<IterRef, Cursor>,
    label_to_pc: HashMap<LabelRef, usize>,
    return_value: Option<ReturnValue<C>>,
    pc: usize,
}

impl<C: Client> Executor<C> {
    pub fn load(client: C, code: Vec<ScriptIR>, max_steps: usize) -> Self {
        assert!(!code.is_empty());

        let mut label_to_pc = HashMap::new();
        for (pc, line) in code.iter().enumerate() {
            if let ScriptIR::Label { label } = line {
                label_to_pc.insert(label.clone(), pc);
            }
        }

        Executor {
            client,
            code,
            max_steps,
            vars: HashMap::new(),
            sets: HashMap::new(),
            iters: HashMap::new(),
            label_to_pc,
            return_value: None,
            pc: 0,
        }
    }

    pub fn run(&mut self) -> Result<Option<ReturnValue<C>>> {
        for _ in 0..self.max_steps {
            if self.pc >= self.code.len() {
                return Ok(self.return_value.take());
            }
            self.step()?;
        }

        Err(ErrorCode::ScriptExecutionError(format!(
            "max steps exceeded: {}",
            self.max_steps
        )))
    }

    fn step(&mut self) -> Result<()> {
        let line = self
            .code
            .get(self.pc)
            .ok_or_else(|| {
                ErrorCode::ScriptExecutionError(format!("pc out of bounds: {}", self.pc))
            })?
            .clone();
        match &line {
            ScriptIR::Query { stmt, to_set } => {
                let sql = stmt
                    .subst(|var| Ok(self.client.scalar_to_literal(self.get_var(&var)?)))?
                    .to_string();
                let block = self
                    .client
                    .query(&sql)
                    .map_err(|err| err.set_span(stmt.span))?;
                self.sets.insert(to_set.clone(), block);
            }
            ScriptIR::Iter { set, to_iter } => {
                let block = self.get_set(set)?;
                let cursor = Cursor {
                    set: set.clone(),
                    row: 0,
                    len: self.client.block_len(block),
                };
                self.iters.insert(to_iter.clone(), cursor);
            }
            ScriptIR::Read {
                iter,
                column,
                to_var,
            } => {
                let cursor = self.get_iter(iter)?;
                let block = self.get_set(&cursor.set)?;
                let scalar = self.client.read_from_block(block, cursor.row, column);
                self.vars.insert(to_var.clone(), scalar);
            }
            ScriptIR::Next { iter } => {
                let cursor = self.get_iter_mut(iter)?;
                assert!(cursor.row < cursor.len);
                cursor.row += 1;
            }
            ScriptIR::Label { .. } => {}
            ScriptIR::JumpIfEnded { iter, to_label } => {
                let cursor = self.get_iter(iter)?;
                if cursor.row >= cursor.len {
                    self.goto(to_label)?;
                }
            }
            ScriptIR::JumpIfTrue {
                condition,
                to_label,
            } => {
                let scalar = self.get_var(condition)?;
                if self.client.is_true(scalar) {
                    self.goto(to_label)?;
                }
            }
            ScriptIR::Goto { to_label } => {
                self.goto(to_label)?;
            }
            ScriptIR::Return => {
                self.goto_end();
            }
            ScriptIR::ReturnVar { var } => {
                self.return_value = Some(ReturnValue::Var(self.get_var(var)?.clone()));
                self.goto_end();
            }
            ScriptIR::ReturnSet { set } => {
                self.return_value = Some(ReturnValue::Set(self.get_set(set)?.clone()));
                self.goto_end();
            }
        }

        self.pc += 1;

        Ok(())
    }

    fn get_var(&self, var: &VarRef) -> Result<&C::Scalar> {
        self.vars
            .get(var)
            .ok_or_else(|| ErrorCode::ScriptExecutionError(format!("unknown var: {var}")))
    }

    fn get_set(&self, set: &SetRef) -> Result<&C::DataBlock> {
        self.sets
            .get(set)
            .ok_or_else(|| ErrorCode::ScriptExecutionError(format!("unknown set: {set}")))
    }

    fn get_iter(&self, iter: &IterRef) -> Result<&Cursor> {
        self.iters
            .get(iter)
            .ok_or_else(|| ErrorCode::ScriptExecutionError(format!("unknown iter: {iter}")))
    }

    fn get_iter_mut(&mut self, iter: &IterRef) -> Result<&mut Cursor> {
        self.iters
            .get_mut(iter)
            .ok_or_else(|| ErrorCode::ScriptExecutionError(format!("unknown iter: {iter}")))
    }

    fn goto(&mut self, label: &LabelRef) -> Result<()> {
        self.pc = *self
            .label_to_pc
            .get(label)
            .ok_or_else(|| ErrorCode::ScriptExecutionError(format!("unknown label: {label}")))?;
        Ok(())
    }

    fn goto_end(&mut self) {
        self.pc = self.code.len();
    }
}
