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

use std::sync::Arc;

use databend_common_exception::Result;

use crate::optimizer::extract::Matcher;
use crate::optimizer::rule::Rule;
use crate::optimizer::rule::RuleID;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::SExpr;
use crate::plans::RelOp;
use crate::plans::Window;
use crate::optimizer::rule::window::merge_window_op;

// Merge two `Window`s that split a EvalScalar into one
pub struct RuleMergeWindowEvalScalar {
    id: RuleID,
    matchers: Vec<Matcher>,
}

impl RuleMergeWindowEvalScalar {
    pub fn new() -> Self {
        Self {
            id: RuleID::MergeWindowEvalScalar,
            // Window
            // \
            // EvalScalar
            // \
            //  Window
            //  \
            //    *
            matchers: vec![Matcher::MatchOp {
                op_type: RelOp::Window,
                children: vec![Matcher::MatchOp {
                    op_type: RelOp::EvalScalar,
                    children: vec![Matcher::MatchOp {
                        op_type: RelOp::Window,
                        children: vec![Matcher::Leaf],
                    }],
                }],
            }],
        }
    }
}

impl Rule for RuleMergeWindowEvalScalar {
    fn id(&self) -> RuleID {
        self.id
    }

    fn apply(&self, s_expr: &SExpr, state: &mut TransformResult) -> Result<()> {
        let up_window: Window = s_expr.plan().clone().try_into()?;
        let down_window: Window = s_expr.child(0)?.child(0)?.plan().clone().try_into()?;

        if let Some(merged) = merge_window_op(up_window, down_window) {
            // merge window, down eval scalar
            let new_expr =
                SExpr::create_unary(Arc::new(merged.into()), Arc::new(s_expr.child(0)?.clone()));

            let new_expr = SExpr::create_unary(
                Arc::new(new_expr.plan().clone()),
                Arc::new(s_expr.child(0)?.child(0)?.child(0)?.clone()),
            );
            state.add_result(new_expr);
        }

        Ok(())
    }

    fn matchers(&self) -> &[Matcher] {
        &self.matchers
    }
}
