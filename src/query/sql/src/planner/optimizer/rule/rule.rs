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
use std::sync::LazyLock;

use databend_common_exception::Result;
use num_derive::FromPrimitive;
use num_derive::ToPrimitive;

use crate::optimizer::extract::Matcher;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::SExpr;

pub static DEFAULT_REWRITE_RULES: LazyLock<Vec<RuleID>> = LazyLock::new(|| {
    vec![
        RuleID::NormalizeScalarFilter,
        RuleID::EliminateFilter,
        RuleID::EliminateSort,
        RuleID::MergeFilter,
        RuleID::MergeEvalScalar,
        RuleID::MergeWindowEvalScalar,
        RuleID::MergeWindow,
        RuleID::PushDownFilterUnion,
        RuleID::PushDownFilterAggregate,
        RuleID::PushDownFilterWindow,
        RuleID::PushDownLimitUnion,
        RuleID::PushDownLimitEvalScalar,
        RuleID::PushDownLimitSort,
        RuleID::PushDownLimitWindow,
        RuleID::PushDownLimitAggregate,
        RuleID::PushDownLimitOuterJoin,
        RuleID::PushDownLimitScan,
        RuleID::PushDownFilterSort,
        RuleID::PushDownFilterEvalScalar,
        RuleID::PushDownFilterJoin,
        RuleID::PushDownFilterProjectSet,
        RuleID::SemiToInnerJoin,
        RuleID::FoldCountAggregate,
        RuleID::TryApplyAggIndex,
        RuleID::SplitAggregate,
        RuleID::PushDownFilterScan,
        RuleID::PushDownPrewhere, /* PushDownPrwhere should be after all rules except PushDownFilterScan */
        RuleID::PushDownSortScan, // PushDownSortScan should be after PushDownPrewhere
    ]
});

pub type RulePtr = Box<dyn Rule>;

pub trait Rule {
    fn id(&self) -> RuleID;

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()>;

    fn matchers(&self) -> &[Matcher];

    fn transformation(&self) -> bool {
        true
    }
}

// If add a new rule, please add it to the operator's corresponding `transformation_candidate_rules`
// Such as `PushDownFilterAggregate` is related to `Filter` operator.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash, FromPrimitive, ToPrimitive)]
pub enum RuleID {
    // Rewrite rules
    NormalizeScalarFilter,
    PushDownFilterAggregate,
    PushDownFilterEvalScalar,
    PushDownFilterUnion,
    PushDownFilterJoin,
    PushDownFilterScan,
    PushDownFilterSort,
    PushDownFilterProjectSet,
    PushDownFilterWindow,
    PushDownLimitUnion,
    PushDownLimitOuterJoin,
    PushDownLimitEvalScalar,
    PushDownLimitSort,
    PushDownLimitWindow,
    PushDownLimitAggregate,
    PushDownLimitScan,
    PushDownSortScan,
    SemiToInnerJoin,
    EliminateEvalScalar,
    EliminateFilter,
    EliminateSort,
    MergeEvalScalar,
    MergeFilter,
    MergeWindow,
    SplitAggregate,
    FoldCountAggregate,
    PushDownPrewhere,
    TryApplyAggIndex,
    CommuteJoin,

    // Exploration rules
    CommuteJoinBaseTable,
    LeftExchangeJoin,
    EagerAggregation,
    MergeWindowEvalScalar,
}

impl Display for RuleID {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            RuleID::PushDownFilterUnion => write!(f, "PushDownFilterUnion"),
            RuleID::PushDownFilterEvalScalar => write!(f, "PushDownFilterEvalScalar"),
            RuleID::PushDownFilterJoin => write!(f, "PushDownFilterJoin"),
            RuleID::PushDownFilterScan => write!(f, "PushDownFilterScan"),
            RuleID::PushDownFilterSort => write!(f, "PushDownFilterSort"),
            RuleID::PushDownFilterProjectSet => write!(f, "PushDownFilterProjectSet"),
            RuleID::PushDownLimitUnion => write!(f, "PushDownLimitUnion"),
            RuleID::PushDownLimitOuterJoin => write!(f, "PushDownLimitOuterJoin"),
            RuleID::PushDownLimitEvalScalar => write!(f, "PushDownLimitEvalScalar"),
            RuleID::PushDownLimitSort => write!(f, "PushDownLimitSort"),
            RuleID::PushDownLimitAggregate => write!(f, "PushDownLimitAggregate"),
            RuleID::PushDownFilterAggregate => write!(f, "PushDownFilterAggregate"),
            RuleID::PushDownLimitScan => write!(f, "PushDownLimitScan"),
            RuleID::PushDownSortScan => write!(f, "PushDownSortScan"),
            RuleID::PushDownLimitWindow => write!(f, "PushDownLimitWindow"),
            RuleID::PushDownFilterWindow => write!(f, "PushDownFilterWindow"),
            RuleID::EliminateEvalScalar => write!(f, "EliminateEvalScalar"),
            RuleID::EliminateFilter => write!(f, "EliminateFilter"),
            RuleID::EliminateSort => write!(f, "EliminateSort"),
            RuleID::MergeEvalScalar => write!(f, "MergeEvalScalar"),
            RuleID::MergeFilter => write!(f, "MergeFilter"),
            RuleID::NormalizeScalarFilter => write!(f, "NormalizeScalarFilter"),
            RuleID::SplitAggregate => write!(f, "SplitAggregate"),
            RuleID::FoldCountAggregate => write!(f, "FoldCountAggregate"),
            RuleID::PushDownPrewhere => write!(f, "PushDownPrewhere"),

            RuleID::CommuteJoin => write!(f, "CommuteJoin"),
            RuleID::CommuteJoinBaseTable => write!(f, "CommuteJoinBaseTable"),
            RuleID::LeftExchangeJoin => write!(f, "LeftExchangeJoin"),
            RuleID::EagerAggregation => write!(f, "EagerAggregation"),
            RuleID::TryApplyAggIndex => write!(f, "TryApplyAggIndex"),
            RuleID::SemiToInnerJoin => write!(f, "SemiToInnerJoin"),
            RuleID::MergeWindow => write!(f, "MergeWindow"),
            RuleID::MergeWindowEvalScalar => write!(f, "MergeWindowEvalScalar"),
        }
    }
}
