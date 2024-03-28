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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_ast::ast::ExplainKind;
use databend_common_catalog::merge_into_join::MergeIntoJoin;
use databend_common_catalog::merge_into_join::MergeIntoJoinType;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use educe::Educe;
use log::info;

use super::distributed::MergeSourceOptimizer;
use super::format::display_memo;
use super::Memo;
use crate::binder::MergeIntoType;
use crate::optimizer::aggregate::RuleNormalizeAggregateOptimizer;
use crate::optimizer::cascades::CascadesOptimizer;
use crate::optimizer::decorrelate::decorrelate_subquery;
use crate::optimizer::distributed::optimize_distributed_query;
use crate::optimizer::distributed::SortAndLimitPushDownOptimizer;
use crate::optimizer::filter::DeduplicateJoinConditionOptimizer;
use crate::optimizer::filter::PullUpFilterOptimizer;
use crate::optimizer::hyper_dp::DPhpy;
use crate::optimizer::join::SingleToInnerOptimizer;
use crate::optimizer::rule::TransformResult;
use crate::optimizer::util::contains_local_table_scan;
use crate::optimizer::RuleFactory;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::optimizer::DEFAULT_REWRITE_RULES;
use crate::plans::CopyIntoLocationPlan;
use crate::plans::Exchange;
use crate::plans::Join;
use crate::plans::MergeInto;
use crate::plans::Plan;
use crate::plans::RelOperator;
use crate::MetadataRef;

#[derive(Clone, Educe)]
#[educe(Debug)]
pub struct OptimizerContext {
    #[educe(Debug(ignore))]
    table_ctx: Arc<dyn TableContext>,
    metadata: MetadataRef,

    // Optimizer configurations
    enable_distributed_optimization: bool,
    enable_join_reorder: bool,
    enable_dphyp: bool,
}

impl OptimizerContext {
    pub fn new(table_ctx: Arc<dyn TableContext>, metadata: MetadataRef) -> Self {
        Self {
            table_ctx,
            metadata,

            enable_distributed_optimization: false,
            enable_join_reorder: true,
            enable_dphyp: true,
        }
    }

    pub fn with_enable_distributed_optimization(mut self, enable: bool) -> Self {
        self.enable_distributed_optimization = enable;
        self
    }

    pub fn with_enable_join_reorder(mut self, enable: bool) -> Self {
        self.enable_join_reorder = enable;
        self
    }

    pub fn with_enable_dphyp(mut self, enable: bool) -> Self {
        self.enable_dphyp = enable;
        self
    }
}

/// A recursive optimizer that will apply the given rules recursively.
/// It will keep applying the rules on the substituted expression
/// until no more rules can be applied.
pub struct RecursiveOptimizer<'a> {
    ctx: &'a OptimizerContext,
    rules: &'static [RuleID],
}

impl<'a> RecursiveOptimizer<'a> {
    pub fn new(rules: &'static [RuleID], ctx: &'a OptimizerContext) -> Self {
        Self { ctx, rules }
    }

    /// Run the optimizer on the given expression.
    pub fn run(&self, s_expr: &SExpr) -> Result<SExpr> {
        self.optimize_expression(s_expr)
    }

    fn optimize_expression(&self, s_expr: &SExpr) -> Result<SExpr> {
        let mut optimized_children = Vec::with_capacity(s_expr.arity());
        for expr in s_expr.children() {
            optimized_children.push(Arc::new(self.run(expr)?));
        }
        let optimized_expr = s_expr.replace_children(optimized_children);
        let result = self.apply_transform_rules(&optimized_expr, self.rules)?;

        Ok(result)
    }

    fn apply_transform_rules(&self, s_expr: &SExpr, rules: &[RuleID]) -> Result<SExpr> {
        let mut s_expr = s_expr.clone();
        for rule_id in rules {
            let rule = RuleFactory::create_rule(*rule_id, self.ctx.metadata.clone())?;
            let mut state = TransformResult::new();
            if rule
                .matchers()
                .iter()
                .any(|matcher| matcher.matches(&s_expr))
                && !s_expr.applied_rule(&rule.id())
            {
                s_expr.set_applied_rule(&rule.id());
                rule.apply(&s_expr, &mut state)?;
                if !state.results().is_empty() {
                    // Recursive optimize the result
                    let result = &state.results()[0];
                    let optimized_result = self.optimize_expression(result)?;
                    return Ok(optimized_result);
                }
            }
        }

        Ok(s_expr.clone())
    }
}

#[minitrace::trace]
#[async_backtrace::framed]
pub async fn optimize(opt_ctx: OptimizerContext, plan: Plan) -> Result<Plan> {
    match plan {
        Plan::Query {
            s_expr,
            bind_context,
            metadata,
            rewrite_kind,
            formatted_ast,
            ignore_result,
        } => Ok(Plan::Query {
            s_expr: Box::new(optimize_query(opt_ctx, *s_expr).await?),
            bind_context,
            metadata,
            rewrite_kind,
            formatted_ast,
            ignore_result,
        }),
        Plan::Explain { kind, config, plan } => match kind {
            ExplainKind::Ast(_) | ExplainKind::Syntax(_) => {
                Ok(Plan::Explain { config, kind, plan })
            }
            ExplainKind::Memo(_) => {
                if let box Plan::Query { ref s_expr, .. } = plan {
                    let memo = get_optimized_memo(opt_ctx, *s_expr.clone())?;
                    Ok(Plan::Explain {
                        config,
                        kind: ExplainKind::Memo(display_memo(&memo)?),
                        plan,
                    })
                } else {
                    Err(ErrorCode::BadArguments(
                        "Cannot use EXPLAIN MEMO with a non-query statement",
                    ))
                }
            }
            _ => {
                if config.optimized || !config.logical {
                    let optimized_plan = Box::pin(optimize(opt_ctx.clone(), *plan)).await?;
                    Ok(Plan::Explain {
                        kind,
                        config,
                        plan: Box::new(optimized_plan),
                    })
                } else {
                    Ok(Plan::Explain { kind, config, plan })
                }
            }
        },
        Plan::ExplainAnalyze { plan } => Ok(Plan::ExplainAnalyze {
            plan: Box::new(Box::pin(optimize(opt_ctx, *plan)).await?),
        }),
        Plan::CopyIntoLocation(CopyIntoLocationPlan { stage, path, from }) => {
            Ok(Plan::CopyIntoLocation(CopyIntoLocationPlan {
                stage,
                path,
                from: Box::new(Box::pin(optimize(opt_ctx, *from)).await?),
            }))
        }
        Plan::CopyIntoTable(mut plan) if !plan.no_file_to_copy => {
            plan.enable_distributed = opt_ctx.enable_distributed_optimization
                && opt_ctx
                    .table_ctx
                    .get_settings()
                    .get_enable_distributed_copy()?;
            info!(
                "after optimization enable_distributed_copy? : {}",
                plan.enable_distributed
            );
            Ok(Plan::CopyIntoTable(plan))
        }
        Plan::MergeInto(plan) => optimize_merge_into(opt_ctx.clone(), plan).await,

        // Pass through statements.
        _ => Ok(plan),
    }
}

#[async_backtrace::framed]
pub async fn optimize_query(opt_ctx: OptimizerContext, mut s_expr: SExpr) -> Result<SExpr> {
    let enable_distributed_query = opt_ctx.enable_distributed_optimization
        && !contains_local_table_scan(&s_expr, &opt_ctx.metadata);

    // Decorrelate subqueries, after this step, there should be no subquery in the expression.
    if s_expr.contain_subquery() {
        s_expr = decorrelate_subquery(
            opt_ctx.table_ctx.clone(),
            opt_ctx.metadata.clone(),
            s_expr.clone(),
        )?;
    }

    // Normalize aggregate, it should be executed before RuleSplitAggregate.
    s_expr = RuleNormalizeAggregateOptimizer::new().run(&s_expr)?;

    // Pull up and infer filter.
    s_expr = PullUpFilterOptimizer::new(opt_ctx.metadata.clone()).run(&s_expr)?;

    // Run default rewrite rules
    s_expr = RecursiveOptimizer::new(&DEFAULT_REWRITE_RULES, &opt_ctx).run(&s_expr)?;

    // Cost based optimization
    let mut dphyp_optimized = false;
    if opt_ctx.enable_dphyp && opt_ctx.enable_join_reorder {
        let (dp_res, optimized) =
            DPhpy::new(opt_ctx.table_ctx.clone(), opt_ctx.metadata.clone()).optimize(&s_expr)?;
        if optimized {
            s_expr = (*dp_res).clone();
            dphyp_optimized = true;
        }
    }

    // After join reorder, Convert some single join to inner join.
    s_expr = SingleToInnerOptimizer::new().run(&s_expr)?;

    // Deduplicate join conditions.
    s_expr = DeduplicateJoinConditionOptimizer::new().run(&s_expr)?;

    let mut cascades = CascadesOptimizer::new(
        opt_ctx.table_ctx.clone(),
        opt_ctx.metadata.clone(),
        dphyp_optimized,
        enable_distributed_query,
    )?;

    if opt_ctx.enable_join_reorder {
        s_expr =
            RecursiveOptimizer::new([RuleID::CommuteJoin].as_slice(), &opt_ctx).run(&s_expr)?;
    }

    // Cascades optimizer may fail due to timeout, fallback to heuristic optimizer in this case.
    s_expr = match cascades.optimize(s_expr.clone()) {
        Ok(mut s_expr) => {
            // Push down sort and limit
            // TODO(leiysky): do this optimization in cascades optimizer
            if enable_distributed_query {
                let sort_and_limit_optimizer = SortAndLimitPushDownOptimizer::create();
                s_expr = sort_and_limit_optimizer.optimize(&s_expr)?;
            }
            s_expr
        }

        Err(e) => {
            info!(
                "CascadesOptimizer failed, fallback to heuristic optimizer: {}",
                e
            );
            if enable_distributed_query {
                s_expr = optimize_distributed_query(opt_ctx.table_ctx.clone(), &s_expr)?;
            }

            s_expr
        }
    };

    s_expr =
        RecursiveOptimizer::new([RuleID::EliminateEvalScalar].as_slice(), &opt_ctx).run(&s_expr)?;

    Ok(s_expr)
}

// TODO(leiysky): reuse the optimization logic with `optimize_query`
fn get_optimized_memo(opt_ctx: OptimizerContext, mut s_expr: SExpr) -> Result<Memo> {
    let enable_distributed_query = opt_ctx.enable_distributed_optimization
        && !contains_local_table_scan(&s_expr, &opt_ctx.metadata);

    // Decorrelate subqueries, after this step, there should be no subquery in the expression.
    if s_expr.contain_subquery() {
        s_expr = decorrelate_subquery(
            opt_ctx.table_ctx.clone(),
            opt_ctx.metadata.clone(),
            s_expr.clone(),
        )?;
    }

    // Run default rewrite rules
    s_expr = RecursiveOptimizer::new(&DEFAULT_REWRITE_RULES, &opt_ctx).run(&s_expr)?;

    // Cost based optimization
    let mut dphyp_optimized = false;
    if opt_ctx.enable_dphyp && opt_ctx.enable_join_reorder {
        let (dp_res, optimized) =
            DPhpy::new(opt_ctx.table_ctx.clone(), opt_ctx.metadata.clone()).optimize(&s_expr)?;
        if optimized {
            s_expr = (*dp_res).clone();
            dphyp_optimized = true;
        }
    }
    let mut cascades = CascadesOptimizer::new(
        opt_ctx.table_ctx.clone(),
        opt_ctx.metadata.clone(),
        dphyp_optimized,
        enable_distributed_query,
    )?;
    cascades.optimize(s_expr)?;

    Ok(cascades.memo)
}

#[async_backtrace::framed]
async fn optimize_merge_into(opt_ctx: OptimizerContext, plan: Box<MergeInto>) -> Result<Plan> {
    // optimize source :fix issue #13733
    // reason: if there is subquery,windowfunc exprs etc. see
    // src/planner/semantic/lowering.rs `as_raw_expr()`, we will
    // get dummy index. So we need to use optimizer to solve this.
    let mut right_source = optimize_query(opt_ctx.clone(), plan.input.child(1)?.clone()).await?;

    // if it's not distributed execution, we should reserve
    // exchange to merge source data.
    if opt_ctx.enable_distributed_optimization
        && opt_ctx
            .table_ctx
            .get_settings()
            .get_enable_distributed_merge_into()?
    {
        // we need to remove exchange of right_source, because it's
        // not an end query.
        if let RelOperator::Exchange(_) = right_source.plan.as_ref() {
            right_source = right_source.child(0)?.clone();
        }
    }
    // replace right source
    let mut join_sexpr = plan.input.clone();
    join_sexpr = Box::new(join_sexpr.replace_children(vec![
        Arc::new(join_sexpr.child(0)?.clone()),
        Arc::new(right_source),
    ]));

    let join_op = Join::try_from(join_sexpr.plan().clone())?;
    let non_equal_join = join_op.right_conditions.is_empty() && join_op.left_conditions.is_empty();
    // before, we think source table is always the small table.
    // 1. for matched only, we use inner join
    // 2. for insert only, we use right anti join
    // 3. for full merge into, we use right outer join
    // for now, let's import the statistic info to determine left join or right join
    // we just do optimization for the top join (target and source),won't do recursive optimization.
    let rule = RuleFactory::create_rule(RuleID::CommuteJoin, plan.meta_data.clone())?;
    let mut state = TransformResult::new();
    // we will reorder the join order according to the cardinality of target and source.
    rule.apply(&join_sexpr, &mut state)?;
    assert!(state.results().len() <= 1);
    // we need to check whether we do swap left and right.
    let change_join_order = if state.results().len() == 1 {
        join_sexpr = Box::new(state.results()[0].clone());
        true
    } else {
        false
    };

    // we just support left join to use MergeIntoBlockInfoHashTable, we
    // don't support spill for now, and we need the macthed clauses' count
    // is one, just support `merge into t using source when matched then
    // update xx when not matched then insert xx`.
    let flag = plan.matched_evaluators.len() == 1
        && plan.matched_evaluators[0].condition.is_none()
        && plan.matched_evaluators[0].update.is_some()
        && !opt_ctx
            .table_ctx
            .get_settings()
            .get_enable_distributed_merge_into()?;
    let mut new_columns_set = plan.columns_set.clone();
    if change_join_order
        && matches!(plan.merge_type, MergeIntoType::FullOperation)
        && opt_ctx
            .table_ctx
            .get_settings()
            .get_join_spilling_memory_ratio()?
            == 0
        && flag
    {
        new_columns_set.remove(&plan.row_id_index);
        opt_ctx.table_ctx.set_merge_into_join(MergeIntoJoin {
            merge_into_join_type: MergeIntoJoinType::Left,
            is_distributed: false,
            target_tbl_idx: plan.target_table_idx,
        })
    }

    // try to optimize distributed join, only if
    // - distributed optimization is enabled
    // - no local table scan
    // - distributed merge-into is enabled
    if opt_ctx.enable_distributed_optimization
        && !contains_local_table_scan(&join_sexpr, &opt_ctx.metadata)
        && opt_ctx
            .table_ctx
            .get_settings()
            .get_enable_distributed_merge_into()?
    {
        // distributed execution stargeties:
        // I. change join order is true, we use the `optimize_distributed_query`'s result.
        // II. change join order is false and match_pattern and not enable spill and not non-equal-join, we use right outer join with rownumber distributed strategies.
        // III otherwise, use `merge_into_join_sexpr` as standalone execution(so if change join order is false,but doesn't match_pattern, we don't support distributed,in fact. case I
        // can take this at most time, if that's a hash shuffle, the I can take it. We think source is always very small).
        // input is a Join_SExpr
        let mut merge_into_join_sexpr =
            optimize_distributed_query(opt_ctx.table_ctx.clone(), &join_sexpr)?;
        let merge_source_optimizer = MergeSourceOptimizer::create();
        // II.
        // - join spilling is disabled
        let (optimized_distributed_merge_into_join_sexpr, distributed) = if opt_ctx
            .table_ctx
            .get_settings()
            .get_join_spilling_memory_ratio()?
            == 0
            && !change_join_order
            && merge_source_optimizer
                .merge_source_matcher
                .matches(&merge_into_join_sexpr)
            && !non_equal_join
        {
            (
                merge_source_optimizer.optimize(&merge_into_join_sexpr)?,
                true,
            )
        } else if change_join_order {
            // I
            // we need to judge whether it'a broadcast join to support runtime filter.
            merge_into_join_sexpr = try_to_change_as_broadcast_join(
                merge_into_join_sexpr,
                change_join_order,
                opt_ctx.table_ctx.clone(),
                plan.as_ref(),
                false, // we will open it, but for now we don't support distributed
                new_columns_set.as_mut(),
            )?;
            (
                merge_into_join_sexpr.clone(),
                matches!(
                    merge_into_join_sexpr.plan.as_ref(),
                    RelOperator::Exchange(_)
                ),
            )
        } else {
            // III.
            (merge_into_join_sexpr.clone(), false)
        };

        Ok(Plan::MergeInto(Box::new(MergeInto {
            input: Box::new(optimized_distributed_merge_into_join_sexpr),
            distributed,
            change_join_order,
            columns_set: new_columns_set.clone(),
            ..*plan
        })))
    } else {
        Ok(Plan::MergeInto(Box::new(MergeInto {
            input: join_sexpr,
            change_join_order,
            columns_set: new_columns_set,
            ..*plan
        })))
    }
}

fn try_to_change_as_broadcast_join(
    merge_into_join_sexpr: SExpr,
    _change_join_order: bool,
    _table_ctx: Arc<dyn TableContext>,
    _plan: &MergeInto,
    _only_one_matched_clause: bool,
    _new_columns_set: &mut HashSet<usize>,
) -> Result<SExpr> {
    if let RelOperator::Exchange(Exchange::Merge) = merge_into_join_sexpr.plan.as_ref() {
        let right_exchange = merge_into_join_sexpr.child(0)?.child(1)?;
        if let RelOperator::Exchange(Exchange::Broadcast) = right_exchange.plan.as_ref() {
            let join: Join = merge_into_join_sexpr.child(0)?.plan().clone().try_into()?;
            let join_s_expr = merge_into_join_sexpr
                .child(0)?
                .replace_plan(Arc::new(RelOperator::Join(join)));
            // for now, when we use target table as build side and it's a broadcast join,
            // we will use merge_into_block_info_hashtable to reduce i/o operations.
            // Todo(JackTan25): we don't support in distributed mod for target build optimization for now. we will enable in next pr.
            // if change_join_order
            //     && matches!(plan.merge_type, MergeIntoType::FullOperation)
            //     && only_one_matched_clause
            // {
            // remove rowid
            // new_columns_set.remove(&plan.row_id_index);
            // table_ctx.set_merge_into_join(MergeIntoJoin {
            //     merge_into_join_type: MergeIntoJoinType::Left,
            //     is_distributed: true,
            //     target_tbl_idx: plan.target_table_idx,
            // })
            // }
            return Ok(merge_into_join_sexpr.replace_children(vec![Arc::new(join_s_expr)]));
        }
    }
    Ok(merge_into_join_sexpr)
}
