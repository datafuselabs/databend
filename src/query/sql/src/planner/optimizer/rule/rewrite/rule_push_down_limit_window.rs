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

use std::cmp;
use std::sync::Arc;

use databend_common_exception::Result;

use crate::optimizer::extract::Matcher;
use crate::optimizer::rule::Rule;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::plans::Limit;
use crate::plans::RelOp;
use crate::plans::RelOperator;
use crate::plans::Window as LogicalWindow;
use crate::plans::WindowFuncFrame;
use crate::plans::WindowFuncFrameBound;
use crate::plans::WindowFuncFrameUnits;
use crate::plans::WindowFuncType;

/// Input:  Limit
///           \
///          Window
///             \
///              *
///
/// Output: Limit
///           \
///          Window(padding limit)
///             \
///               *
pub struct RulePushDownLimitWindow {
    id: RuleID,
    matchers: Vec<Matcher>,
    max_limit: usize,
}

impl RulePushDownLimitWindow {
    pub fn new(max_limit: usize) -> Self {
        Self {
            id: RuleID::PushDownLimitWindow,
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Limit,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::Window,
                    children: vec![Matcher::Leaf],
                }],
            }],
            max_limit,
        }
    }
}

impl Rule for RulePushDownLimitWindow {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let limit: Limit = s_expr.plan().clone().try_into()?;
        if let Some(mut count) = limit.limit {
            count += limit.offset;
            let window = s_expr.child(0)?;
            let mut window_limit: LogicalWindow = window.plan().clone().try_into()?;
            if should_apply(window.child(0)?, &window_limit)? {
                let limit = window_limit.limit.map_or(count, |c| cmp::max(c, count));

                if limit <= self.max_limit {
                    window_limit.limit = Some(limit);
                    let sort = SExpr::create_unary(
                        Arc::new(RelOperator::Window(window_limit)),
                        Arc::new(window.child(0)?.clone()),
                    );

                    let mut result = s_expr.replace_children(vec![Arc::new(sort)]);
                    result.set_applied_rule(&self.id);
                    state.add_result(result);
                }
            }
        }
        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}

fn should_apply(child: &SExpr, window: &LogicalWindow) -> Result<bool> {
    let child_window_exists = child_has_window(child)?;
    // ranking functions are frame insensitive
    // TODO : consider if window functions contains a rank func
    if window.function.iter().all(is_ranking_function) {
        Ok(!child_window_exists)
    } else {
        Ok(is_valid_frame(&window.frame) && !child_window_exists)
    }
}

fn is_ranking_function(func: &WindowFuncType) -> bool {
    matches!(
        func,
        WindowFuncType::RowNumber | WindowFuncType::Rank | WindowFuncType::DenseRank
    )
}

fn is_valid_frame(frame: &WindowFuncFrame) -> bool {
    matches!(frame.units, WindowFuncFrameUnits::Rows)
        && matches!(frame.start_bound, WindowFuncFrameBound::Preceding(_))
        && matches!(frame.end_bound, WindowFuncFrameBound::CurrentRow)
}

fn child_has_window(child: &SExpr) -> Result<bool> {
    match child.plan() {
        RelOperator::Window(_) => Ok(true),
        RelOperator::Scan(_) => Ok(false), // finish recursion
        _ => child_has_window(child.child(0)?),
    }
}
