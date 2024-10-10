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

use derive_visitor::Drive;
use derive_visitor::DriveMut;

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum ExplainKind {
    Ast(String),
    Syntax(String),
    // The display string will be filled by optimizer, as we
    // don't want to expose `Memo` to other crates.
    Memo(String),
    Graph,
    Pipeline,
    Fragments,

    // `EXPLAIN RAW` and `EXPLAIN OPTIMIZED` will be deprecated in the future,
    // use explain options instead
    Raw,
    Optimized,

    Plan,

    Join,

    // Explain analyze plan
    AnalyzePlan,

    Graphical,
}

#[derive(Debug, Clone, PartialEq, Eq, Drive, DriveMut)]
pub enum ExplainOption {
    Verbose,
    Logical,
    Optimized,
}
