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
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use educe::Educe;
use log::info;

use super::aggregate::RuleStatsAggregateOptimizer;
use super::distributed::BroadcastToShuffleOptimizer;
use super::format::display_memo;
use super::Memo;
use crate::binder::target_probe;
use crate::binder::MutationStrategy;
use crate::binder::MutationType;
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
use crate::optimizer::QuerySampleExecutor;
use crate::optimizer::RuleFactory;
use crate::optimizer::RuleID;
use crate::optimizer::SExpr;
use crate::optimizer::DEFAULT_REWRITE_RULES;
use crate::plans::CopyIntoLocationPlan;
use crate::plans::Join;
use crate::plans::JoinType;
use crate::plans::Mutation;
use crate::plans::Plan;
use crate::plans::RelOperator;
use crate::plans::SetScalarsOrQuery;
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
    planning_agg_index: bool,
    #[educe(Debug(ignore))]
    sample_executor: Option<Arc<dyn QuerySampleExecutor>>,
}

impl OptimizerContext {
    pub fn new(table_ctx: Arc<dyn TableContext>, metadata: MetadataRef) -> Self {
        Self {
            table_ctx,
            metadata,

            enable_distributed_optimization: false,
            enable_join_reorder: true,
            enable_dphyp: true,
            sample_executor: None,
            planning_agg_index: false,
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

    pub fn with_sample_executor(
        mut self,
        sample_executor: Option<Arc<dyn QuerySampleExecutor>>,
    ) -> Self {
        self.sample_executor = sample_executor;
        self
    }

    pub fn with_planning_agg_index(mut self) -> Self {
        self.planning_agg_index = true;
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
        let mut children_changed = false;
        for expr in s_expr.children() {
            let optimized_child = self.run(expr)?;
            if !optimized_child.eq(expr) {
                children_changed = true;
            }
            optimized_children.push(Arc::new(optimized_child));
        }
        let mut optimized_expr = s_expr.clone();
        if children_changed {
            optimized_expr = s_expr.replace_children(optimized_children);
        }

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

#[fastrace::trace]
#[async_recursion(#[recursive::recursive])]
pub async fn optimize(mut opt_ctx: OptimizerContext, plan: Plan) -> Result<Plan> {
    match plan {
        Plan::Query {
            s_expr,
            bind_context,
            metadata,
            rewrite_kind,
            formatted_ast,
            ignore_result,
        } => Ok(Plan::Query {
            s_expr: Box::new(optimize_query(&mut opt_ctx, *s_expr).await?),
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
        Plan::ExplainAnalyze {
            plan,
            partial,
            graphical,
        } => Ok(Plan::ExplainAnalyze {
            partial,
            graphical,
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
        Plan::DataMutation { s_expr, .. } => optimize_mutation(opt_ctx, *s_expr).await,

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

        Plan::Set(mut plan) => {
            if let SetScalarsOrQuery::Query(q) = plan.values {
                let optimized_plan = optimize(opt_ctx.clone(), *q.clone()).await?;
                plan.values = SetScalarsOrQuery::Query(Box::new(optimized_plan))
            }

            Ok(Plan::Set(plan))
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

pub async fn optimize_query(opt_ctx: &mut OptimizerContext, mut s_expr: SExpr) -> Result<SExpr> {
    if contains_local_table_scan(&s_expr, &opt_ctx.metadata) {
        opt_ctx.enable_distributed_optimization = false;
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

    s_expr = RuleStatsAggregateOptimizer::new(opt_ctx.table_ctx.clone(), opt_ctx.metadata.clone())
        .run(&s_expr)
        .await?;

    // Collect statistics for each leaf node in SExpr.
    s_expr = CollectStatisticsOptimizer::new(opt_ctx.table_ctx.clone(), opt_ctx.metadata.clone())
        .run(&s_expr)
        .await?;

    // Normalize aggregate, it should be executed before RuleSplitAggregate.
    s_expr = RuleNormalizeAggregateOptimizer::new().run(&s_expr)?;

    // Pull up and infer filter.
    s_expr = PullUpFilterOptimizer::new(opt_ctx.metadata.clone()).run(&s_expr)?;

    // Run default rewrite rules
    s_expr = RecursiveOptimizer::new(&DEFAULT_REWRITE_RULES, opt_ctx).run(&s_expr)?;

    // Run post rewrite rules
    s_expr = RecursiveOptimizer::new(&[RuleID::SplitAggregate], opt_ctx).run(&s_expr)?;

    // Cost based optimization
    let mut dphyp_optimized = false;
    if opt_ctx.enable_dphyp && opt_ctx.enable_join_reorder {
        let (dp_res, optimized) = DPhpy::new(
            opt_ctx.table_ctx.clone(),
            opt_ctx.metadata.clone(),
            opt_ctx.sample_executor.clone(),
        )
        .optimize(&s_expr)
        .await?;
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
        opt_ctx.enable_distributed_optimization,
    )?;

    if opt_ctx.enable_join_reorder {
        s_expr = RecursiveOptimizer::new([RuleID::CommuteJoin].as_slice(), opt_ctx).run(&s_expr)?;
    }

    // Cascades optimizer may fail due to timeout, fallback to heuristic optimizer in this case.
    s_expr = match cascades.optimize(s_expr.clone()) {
        Ok(mut s_expr) => {
            // Push down sort and limit
            // TODO(leiysky): do this optimization in cascades optimizer
            if opt_ctx.enable_distributed_optimization {
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
            if opt_ctx.enable_distributed_optimization {
                s_expr = optimize_distributed_query(opt_ctx.table_ctx.clone(), &s_expr)?;
            }

            s_expr
        }
    };

    if !opt_ctx.planning_agg_index {
        s_expr = RecursiveOptimizer::new([RuleID::EliminateEvalScalar].as_slice(), opt_ctx)
            .run(&s_expr)?;
    }

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

    s_expr = RuleStatsAggregateOptimizer::new(opt_ctx.table_ctx.clone(), opt_ctx.metadata.clone())
        .run(&s_expr)
        .await?;

    // Collect statistics for each leaf node in SExpr.
    s_expr = CollectStatisticsOptimizer::new(opt_ctx.table_ctx.clone(), opt_ctx.metadata.clone())
        .run(&s_expr)
        .await?;

    // Pull up and infer filter.
    s_expr = PullUpFilterOptimizer::new(opt_ctx.metadata.clone()).run(&s_expr)?;
    // Run default rewrite rules
    s_expr = RecursiveOptimizer::new(&DEFAULT_REWRITE_RULES, &opt_ctx).run(&s_expr)?;
    // Run post rewrite rules
    s_expr = RecursiveOptimizer::new(&[RuleID::SplitAggregate], &opt_ctx).run(&s_expr)?;

    // Cost based optimization
    let mut dphyp_optimized = false;
    if opt_ctx.enable_dphyp && opt_ctx.enable_join_reorder {
        let (dp_res, optimized) = DPhpy::new(
            opt_ctx.table_ctx.clone(),
            opt_ctx.metadata.clone(),
            opt_ctx.sample_executor.clone(),
        )
        .optimize(&s_expr)
        .await?;
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

async fn optimize_mutation(mut opt_ctx: OptimizerContext, s_expr: SExpr) -> Result<Plan> {
    // Optimize the input plan.
    let mut input_s_expr = optimize_query(&mut opt_ctx, s_expr.child(0)?.clone()).await?;

    // For distributed query optimization, we need to remove the Exchange operator at the top of the plan.
    if let &RelOperator::Exchange(_) = input_s_expr.plan() {
        input_s_expr = input_s_expr.child(0)?.clone();
    }
    // If there still exists a Exchange::Merge operator, we should disable distributed optimization and
    // optimize the input plan again.
    if input_s_expr.has_merge_exchange() {
        opt_ctx = opt_ctx.with_enable_distributed_optimization(false);
        input_s_expr = optimize_query(&mut opt_ctx, s_expr.child(0)?.clone()).await?;
    }

    let mut mutation: Mutation = s_expr.plan().clone().try_into()?;
    mutation.distributed = opt_ctx.enable_distributed_optimization;

    input_s_expr = match mutation.mutation_type {
        MutationType::Merge => {
            if mutation.distributed {
                let join = Join::try_from(input_s_expr.plan().clone())?;
                let broadcast_to_shuffle = BroadcastToShuffleOptimizer::create();
                let is_broadcast = broadcast_to_shuffle.matcher.matches(&input_s_expr)
                    && broadcast_to_shuffle.is_broadcast(&input_s_expr)?;

                // If the mutation strategy is matched only, the join type is inner join, if it is a broadcast
                // join and the target table on the probe side, we can avoid row id shuffle after the join.
                let target_probe = target_probe(&input_s_expr, mutation.target_table_index)?;
                if is_broadcast
                    && target_probe
                    && mutation.strategy == MutationStrategy::MatchedOnly
                {
                    mutation.row_id_shuffle = false;
                }

                // Change broadcast join to shuffle join if the join type is left or left-anti join, because
                // broadcast join can not deduplicate row ids.
                if is_broadcast && matches!(join.join_type, JoinType::Left | JoinType::LeftAnti) {
                    broadcast_to_shuffle.optimize(&input_s_expr)?
                } else {
                    input_s_expr
                }
            } else {
                input_s_expr
            }
        }
        MutationType::Update | MutationType::Delete => input_s_expr,
    };

    Ok(Plan::DataMutation {
        schema: mutation.schema(),
        s_expr: Box::new(SExpr::create_unary(
            Arc::new(RelOperator::Mutation(mutation)),
            Arc::new(input_s_expr),
        )),
        metadata: opt_ctx.metadata.clone(),
    })
}
