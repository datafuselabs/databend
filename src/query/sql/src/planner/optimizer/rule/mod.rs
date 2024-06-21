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

mod factory;
mod rewrite;
#[allow(clippy::module_inception)]
mod rule;
mod rule_set;
mod transform;
mod transform_result;
mod utils;

pub use factory::RuleFactory;
pub use rewrite::agg_index;
pub use rewrite::try_push_down_filter_join;
pub use rule::Rule;
pub use rule::RuleID;
pub use rule::RulePtr;
pub use rule::DEFAULT_REWRITE_RULES;
pub use rule_set::AppliedRules;
pub use rule_set::RuleSet;
pub use transform_result::TransformResult;
pub use utils::constant;
pub use utils::window;
