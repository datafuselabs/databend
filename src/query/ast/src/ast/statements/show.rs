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

use crate::ast::Expr;

#[derive(Debug, Clone, PartialEq)]
pub enum ShowLimit {
    Like { pattern: String },
    Where { selection: Box<Expr> },
}

impl Display for ShowLimit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ShowLimit::Like { pattern } => write!(f, "LIKE '{pattern}'"),
            ShowLimit::Where { selection } => write!(f, "WHERE {selection}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ShowOptions { 
    pub limit_option: Option<ShowLimit>,
    pub limit: Option<u64>,
}

impl Display for ShowOptions {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(limit_option) = self.limit_option.clone() {
            write!(f, "{}", limit_option)?;
        }
        if let Some(limit) = self.limit.clone() {
            write!(f, " LIMIT = '{}'", limit)?;
        }
        Ok(())
    }
}