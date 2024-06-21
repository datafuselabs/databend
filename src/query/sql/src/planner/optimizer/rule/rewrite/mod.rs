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

pub mod agg_index;
mod push_down_filter_join;
mod rule_commute_join;
mod rule_eliminate_eval_scalar;
mod rule_eliminate_filter;
mod rule_eliminate_sort;
mod rule_fold_count_aggregate;
mod rule_merge_eval_scalar;
mod rule_merge_filter;
mod rule_merge_window;
mod rule_merge_window_eval_scalar;
mod rule_normalize_scalar;
mod rule_push_down_filter_aggregate;
mod rule_push_down_filter_eval_scalar;
mod rule_push_down_filter_join;
mod rule_push_down_filter_project_set;
mod rule_push_down_filter_scan;
mod rule_push_down_filter_sort;
mod rule_push_down_filter_union;
mod rule_push_down_filter_window;
mod rule_push_down_limit_aggregate;
mod rule_push_down_limit_expression;
mod rule_push_down_limit_join;
mod rule_push_down_limit_scan;
mod rule_push_down_limit_sort;
mod rule_push_down_limit_union;
mod rule_push_down_limit_window;
mod rule_push_down_prewhere;
mod rule_push_down_sort_scan;
mod rule_semi_to_inner_join;
mod rule_split_aggregate;
mod rule_try_apply_agg_index;

pub use rule_commute_join::RuleCommuteJoin;
pub use rule_eliminate_eval_scalar::RuleEliminateEvalScalar;
pub use rule_eliminate_filter::RuleEliminateFilter;
pub use rule_eliminate_sort::RuleEliminateSort;
pub use rule_fold_count_aggregate::RuleFoldCountAggregate;
pub use rule_merge_eval_scalar::RuleMergeEvalScalar;
pub use rule_merge_filter::RuleMergeFilter;
pub use rule_merge_window::RuleMergeWindow;
pub use rule_merge_window_eval_scalar::RuleMergeWindowEvalScalar;
pub use rule_normalize_scalar::RuleNormalizeScalarFilter;
pub use rule_push_down_filter_aggregate::RulePushDownFilterAggregate;
pub use rule_push_down_filter_eval_scalar::RulePushDownFilterEvalScalar;
pub use rule_push_down_filter_join::try_push_down_filter_join;
pub use rule_push_down_filter_join::RulePushDownFilterJoin;
pub use rule_push_down_filter_project_set::RulePushDownFilterProjectSet;
pub use rule_push_down_filter_scan::RulePushDownFilterScan;
pub use rule_push_down_filter_sort::RulePushDownFilterSort;
pub use rule_push_down_filter_union::RulePushDownFilterUnion;
pub use rule_push_down_filter_window::RulePushDownFilterWindow;
pub use rule_push_down_limit_aggregate::RulePushDownLimitAggregate;
pub use rule_push_down_limit_expression::RulePushDownLimitEvalScalar;
pub use rule_push_down_limit_join::RulePushDownLimitOuterJoin;
pub use rule_push_down_limit_scan::RulePushDownLimitScan;
pub use rule_push_down_limit_sort::RulePushDownLimitSort;
pub use rule_push_down_limit_union::RulePushDownLimitUnion;
pub use rule_push_down_limit_window::RulePushDownLimitWindow;
pub use rule_push_down_prewhere::RulePushDownPrewhere;
pub use rule_push_down_sort_scan::RulePushDownSortScan;
pub use rule_semi_to_inner_join::RuleSemiToInnerJoin;
pub use rule_split_aggregate::RuleSplitAggregate;
pub use rule_try_apply_agg_index::RuleTryApplyAggIndex;
