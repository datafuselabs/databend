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

use async_recursion::async_recursion;
use databend_common_ast::ast::ExplainKind;
use databend_common_catalog::merge_into_join::MergeIntoJoin;
use databend_common_catalog::merge_into_join::MergeIntoJoinType;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use educe::Educe;
use log::info;

use super::distributed::MergeOptimizer;
use super::format::display_memo;
use super::Memo;
use crate::binder::target_table_position;
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
use crate::optimizer::statistics::CollectStatisticsOptimizer;
use crate::optimizer::util::contains_local_table_scan;
use crate::optimizer::RuleFactory;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::optimizer::DEFAULT_REWRITE_RULES;
use crate::plans::CopyIntoLocationPlan;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::MergeInto;
use crate::plans::Plan;
use crate::plans::RelOperator;
use crate::InsertInputSource;
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
    enable_merge_into_join_reorder: bool,
}

impl OptimizerContext {
    pub fn new(table_ctx: Arc<dyn TableContext>, metadata: MetadataRef) -> Self {
        Self {
            table_ctx,
            metadata,

            enable_distributed_optimization: false,
            enable_join_reorder: true,
            enable_dphyp: true,
            enable_merge_into_join_reorder: true,
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

    pub fn with_enable_merge_into_join_reorder(mut self, enable: bool) -> Self {
        self.enable_merge_into_join_reorder = enable;
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
    #[recursive::recursive]
    pub fn run(&self, s_expr: &SExpr) -> Result<SExpr> {
        self.optimize_expression(s_expr)
    }

    #[recursive::recursive]
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
#[async_recursion(#[recursive::recursive])]
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
                    let memo = get_optimized_memo(opt_ctx, *s_expr.clone()).await?;
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

            if let Some(p) = &plan.query {
                let optimized_plan = optimize(opt_ctx.clone(), *p.clone()).await?;
                plan.query = Some(Box::new(optimized_plan));
            }
            Ok(Plan::CopyIntoTable(plan))
        }
        Plan::MergeInto { s_expr, .. } => optimize_merge_into(opt_ctx, *s_expr).await,

        // distributed insert will be optimized in `physical_plan_builder`
        Plan::Insert(mut plan) => {
            match plan.source {
                InsertInputSource::SelectPlan(p) => {
                    let optimized_plan = optimize(opt_ctx.clone(), *p.clone()).await?;
                    plan.source = InsertInputSource::SelectPlan(Box::new(optimized_plan));
                }
                InsertInputSource::Stage(p) => {
                    let optimized_plan = optimize(opt_ctx.clone(), *p.clone()).await?;
                    plan.source = InsertInputSource::Stage(Box::new(optimized_plan));
                }
                _ => {}
            }
            Ok(Plan::Insert(plan))
        }
        Plan::InsertMultiTable(mut plan) => {
            plan.input_source = optimize(opt_ctx.clone(), plan.input_source.clone()).await?;
            Ok(Plan::InsertMultiTable(plan))
        }
        Plan::Replace(mut plan) => {
            match plan.source {
                InsertInputSource::SelectPlan(p) => {
                    let optimized_plan = optimize(opt_ctx.clone(), *p.clone()).await?;
                    plan.source = InsertInputSource::SelectPlan(Box::new(optimized_plan));
                }
                InsertInputSource::Stage(p) => {
                    let optimized_plan = optimize(opt_ctx.clone(), *p.clone()).await?;
                    plan.source = InsertInputSource::Stage(Box::new(optimized_plan));
                }
                _ => {}
            }
            Ok(Plan::Replace(plan))
        }

        Plan::CreateTable(mut plan) => {
            if let Some(p) = &plan.as_select {
                let optimized_plan = optimize(opt_ctx.clone(), *p.clone()).await?;
                plan.as_select = Some(Box::new(optimized_plan));
            }

            Ok(Plan::CreateTable(plan))
        }
        // Already done in binder
        // Plan::RefreshIndex(mut plan) => {
        //     // use fresh index
        //     let opt_ctx =
        //         OptimizerContext::new(opt_ctx.table_ctx.clone(), opt_ctx.metadata.clone());
        //     plan.query_plan = Box::new(optimize(opt_ctx.clone(), *plan.query_plan.clone()).await?);
        //     Ok(Plan::RefreshIndex(plan))
        // }
        // Pass through statements.
        _ => Ok(plan),
    }
}

pub async fn optimize_query(opt_ctx: OptimizerContext, mut s_expr: SExpr) -> Result<SExpr> {
    let mut enable_distributed_query = opt_ctx.enable_distributed_optimization;
    if contains_local_table_scan(&s_expr, &opt_ctx.metadata) {
        enable_distributed_query = false;
        info!("Disable distributed optimization due to local table scan.");
    }

    // Decorrelate subqueries, after this step, there should be no subquery in the expression.
    if s_expr.contain_subquery() {
        s_expr = decorrelate_subquery(
            opt_ctx.table_ctx.clone(),
            opt_ctx.metadata.clone(),
            s_expr.clone(),
        )?;
    }

    // Collect statistics for each leaf node in SExpr.
    s_expr = CollectStatisticsOptimizer::new(opt_ctx.table_ctx.clone(), opt_ctx.metadata.clone())
        .run(&s_expr)
        .await?;

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
async fn get_optimized_memo(opt_ctx: OptimizerContext, mut s_expr: SExpr) -> Result<Memo> {
    let mut enable_distributed_query = opt_ctx.enable_distributed_optimization;
    if contains_local_table_scan(&s_expr, &opt_ctx.metadata) {
        enable_distributed_query = false;
        info!("Disable distributed optimization due to local table scan.");
    }

    // Decorrelate subqueries, after this step, there should be no subquery in the expression.
    if s_expr.contain_subquery() {
        s_expr = decorrelate_subquery(
            opt_ctx.table_ctx.clone(),
            opt_ctx.metadata.clone(),
            s_expr.clone(),
        )?;
    }

    // Collect statistics for each leaf node in SExpr.
    s_expr = CollectStatisticsOptimizer::new(opt_ctx.table_ctx.clone(), opt_ctx.metadata.clone())
        .run(&s_expr)
        .await?;

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

async fn optimize_merge_into(mut opt_ctx: OptimizerContext, s_expr: SExpr) -> Result<Plan> {
    let mut plan: MergeInto = s_expr.plan().clone().try_into()?;
    let enable_distributed_merge_into = opt_ctx
        .table_ctx
        .get_settings()
        .get_enable_distributed_merge_into()?;
    if opt_ctx.enable_distributed_optimization {
        opt_ctx = opt_ctx.with_enable_distributed_optimization(enable_distributed_merge_into);
    }
    let original_target_table_position =
        target_table_position(s_expr.child(0)?, plan.target_table_index)?;
    let mut join_s_expr = optimize_query(opt_ctx.clone(), s_expr.child(0)?.clone()).await?;
    if let &RelOperator::Exchange(_) = join_s_expr.plan() {
        join_s_expr = join_s_expr.child(0)?.clone();
    }
    let new_target_table_position = target_table_position(&join_s_expr, plan.target_table_index)?;
    let join_order_changed = original_target_table_position != new_target_table_position;
    let join_op = Join::try_from(join_s_expr.plan().clone())?;

    // we just support left join to use MergeIntoBlockInfoHashTable, we
    // don't support spill for now, and we need the matched clauses' count
    // is one, just support `merge into t using source when matched then
    // update xx when not matched then insert xx`.
    let flag = plan.matched_evaluators.len() == 1
        && plan.matched_evaluators[0].condition.is_none()
        && plan.matched_evaluators[0].update.is_some()
        && !opt_ctx
            .table_ctx
            .get_settings()
            .get_enable_distributed_merge_into()?;
    let new_columns_set = plan.columns_set.clone();
    if join_order_changed
        && matches!(plan.merge_type, MergeIntoType::FullOperation)
        && opt_ctx
            .table_ctx
            .get_settings()
            .get_join_spilling_memory_ratio()?
            == 0
        && flag
    {
        // due to issue https://github.com/datafuselabs/databend/issues/15643,
        // target build optimization of merge-into is disabled: here row_id column should be kept

        // new_columns_set.remove(&plan.row_id_index);
        opt_ctx.table_ctx.set_merge_into_join(MergeIntoJoin {
            merge_into_join_type: MergeIntoJoinType::Left,
            is_distributed: false,
            target_tbl_idx: plan.target_table_index,
        })
    }

    plan.change_join_order = join_order_changed;
    let distributed = !join_s_expr.has_merge_exchange();
    if opt_ctx.enable_distributed_optimization && distributed {
        let merge_optimizer = MergeOptimizer::create();
        // Left join changes to shuffle.
        if matches!(join_op.join_type, JoinType::Left | JoinType::LeftAnti)
            && merge_optimizer.merge_matcher.matches(&join_s_expr)
        {
            join_s_expr = merge_optimizer.optimize(&join_s_expr)?;
        };

        plan.distributed = true;
        plan.columns_set = new_columns_set;
        Ok(Plan::MergeInto {
            schema: plan.schema(),
            s_expr: Box::new(SExpr::create_unary(
                Arc::new(RelOperator::MergeInto(plan)),
                Arc::new(join_s_expr),
            )),
            metadata: opt_ctx.metadata.clone(),
        })
    } else {
        plan.columns_set = new_columns_set;
        Ok(Plan::MergeInto {
            schema: plan.schema(),
            s_expr: Box::new(SExpr::create_unary(
                Arc::new(RelOperator::MergeInto(plan)),
                Arc::new(join_s_expr),
            )),
            metadata: opt_ctx.metadata.clone(),
        })
    }
}
