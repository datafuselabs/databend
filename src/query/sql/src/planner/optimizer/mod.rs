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

mod aggregate;
mod cascades;
mod cost;
mod decorrelate;
mod distributed;
mod extract;
mod filter;
mod format;
mod group;
mod hyper_dp;
mod join;
mod m_expr;
mod memo;
#[allow(clippy::module_inception)]
mod optimizer;
mod property;
mod rule;
pub mod s_expr;
mod statistics;
mod util;

pub use cascades::CascadesOptimizer;
pub use decorrelate::FlattenInfo;
pub use decorrelate::SubqueryRewriter;
pub use extract::PatternExtractor;
pub use hyper_dp::DPhpy;
pub use m_expr::MExpr;
pub use memo::Memo;
pub use optimizer::optimize;
pub use optimizer::optimize_query;
pub use optimizer::OptimizerContext;
pub use optimizer::RecursiveOptimizer;
pub use property::*;
pub use rule::agg_index;
pub use rule::try_push_down_filter_join;
pub use rule::RuleFactory;
pub use rule::RuleID;
pub use rule::RuleSet;
pub use rule::DEFAULT_REWRITE_RULES;
pub use s_expr::get_udf_names;
pub use s_expr::SExpr;
pub use util::contains_local_table_scan;
