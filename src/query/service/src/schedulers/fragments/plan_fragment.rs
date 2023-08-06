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

use std::collections::HashMap;
use std::sync::Arc;

use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::Partitions;
use common_exception::ErrorCode;
use common_exception::Result;
use common_sql::executor::CopyIntoTable;
use common_sql::executor::CopyIntoTableSource;
use common_sql::executor::Deduplicate;
use common_sql::executor::DeletePartial;
use common_sql::executor::QueryCtx;
use common_sql::executor::ReplaceInto;
use common_storages_fuse::TableContext;
use storages_common_table_meta::meta::Location;

use crate::api::DataExchange;
use crate::schedulers::Fragmenter;
use crate::schedulers::QueryFragmentAction;
use crate::schedulers::QueryFragmentActions;
use crate::schedulers::QueryFragmentsActions;
use crate::sessions::QueryContext;
use crate::sql::executor::PhysicalPlan;
use crate::sql::executor::PhysicalPlanReplacer;
use crate::sql::executor::TableScan;

/// Type of plan fragment
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FragmentType {
    /// Root fragment of a query plan
    Root,

    /// Intermediate fragment of a query plan,
    /// doesn't contain any `TableScan` operator.
    Intermediate,

    /// Leaf fragment of a query plan, which contains
    /// a `TableScan` operator.
    Source,
    /// Leaf fragment of a delete plan, which contains
    /// a `DeletePartial` operator.
    DeleteLeaf,
    /// Intermediate fragment of a replace into plan, which contains a `ReplaceInto` operator.
    ReplaceInto,
}

#[derive(Clone)]
pub struct PlanFragment {
    pub plan: PhysicalPlan,
    pub fragment_type: FragmentType,
    pub fragment_id: usize,
    pub exchange: Option<DataExchange>,
    pub query_id: String,

    // The fragments to ask data from.
    pub source_fragments: Vec<PlanFragment>,
}

impl PlanFragment {
    pub fn get_actions(
        &self,
        ctx: Arc<QueryContext>,
        actions: &mut QueryFragmentsActions,
    ) -> Result<()> {
        for input in self.source_fragments.iter() {
            input.get_actions(ctx.clone(), actions)?;
        }

        let mut fragment_actions = QueryFragmentActions::create(self.fragment_id);

        match &self.fragment_type {
            FragmentType::Root => {
                let action = QueryFragmentAction::create(
                    Fragmenter::get_local_executor(ctx),
                    self.plan.clone(),
                );
                fragment_actions.add_action(action);
                if let Some(ref exchange) = self.exchange {
                    fragment_actions.set_exchange(exchange.clone());
                }
                actions.add_fragment_actions(fragment_actions)?;
            }
            FragmentType::Intermediate => {
                if self
                    .source_fragments
                    .iter()
                    .any(|fragment| matches!(&fragment.exchange, Some(DataExchange::Merge(_))))
                {
                    // If this is a intermediate fragment with merge input,
                    // we will only send it to coordinator node.
                    let action = QueryFragmentAction::create(
                        Fragmenter::get_local_executor(ctx),
                        self.plan.clone(),
                    );
                    fragment_actions.add_action(action);
                } else {
                    // Otherwise distribute the fragment to all the executors.
                    for executor in Fragmenter::get_executors(ctx) {
                        let action = QueryFragmentAction::create(executor, self.plan.clone());
                        fragment_actions.add_action(action);
                    }
                }
                if let Some(ref exchange) = self.exchange {
                    fragment_actions.set_exchange(exchange.clone());
                }
                actions.add_fragment_actions(fragment_actions)?;
            }
            FragmentType::Source => {
                // Redistribute partitions
                let mut fragment_actions = self.redistribute_source_fragment(ctx)?;
                if let Some(ref exchange) = self.exchange {
                    fragment_actions.set_exchange(exchange.clone());
                }
                actions.add_fragment_actions(fragment_actions)?;
            }
            FragmentType::DeleteLeaf => {
                let mut fragment_actions = self.redistribute_delete_leaf(ctx)?;
                if let Some(ref exchange) = self.exchange {
                    fragment_actions.set_exchange(exchange.clone());
                }
                actions.add_fragment_actions(fragment_actions)?;
            }
            FragmentType::ReplaceInto => {
                // Redistribute partitions
                let mut fragment_actions = self.redistribute_replace_into(ctx)?;
                if let Some(ref exchange) = self.exchange {
                    fragment_actions.set_exchange(exchange.clone());
                }
                actions.add_fragment_actions(fragment_actions)?;
            }
        }

        Ok(())
    }

    /// Redistribute partitions of current source fragment to executors.
    fn redistribute_source_fragment(&self, ctx: Arc<QueryContext>) -> Result<QueryFragmentActions> {
        if self.fragment_type != FragmentType::Source {
            return Err(ErrorCode::Internal(
                "Cannot redistribute a non-source fragment".to_string(),
            ));
        }

        let read_source = self.get_read_source()?;

        let executors = Fragmenter::get_executors(ctx);
        // Redistribute partitions of ReadDataSourcePlan.
        let mut fragment_actions = QueryFragmentActions::create(self.fragment_id);

        let partitions = &read_source.parts;
        let partition_reshuffle = partitions.reshuffle(executors)?;

        for (executor, parts) in partition_reshuffle.iter() {
            let mut new_read_source = read_source.clone();
            new_read_source.parts = parts.clone();
            let mut plan = self.plan.clone();

            // Replace `ReadDataSourcePlan` with rewritten one and generate new fragment for it.
            let mut replace_read_source = ReplaceReadSource {
                source: new_read_source,
            };
            plan = replace_read_source.replace(&plan)?;

            fragment_actions
                .add_action(QueryFragmentAction::create(executor.clone(), plan.clone()));
        }

        Ok(fragment_actions)
    }

    fn redistribute_delete_leaf(&self, ctx: Arc<QueryContext>) -> Result<QueryFragmentActions> {
        let plan = match &self.plan {
            PhysicalPlan::ExchangeSink(plan) => plan,
            _ => unreachable!("logic error"),
        };
        let plan = match plan.input.as_ref() {
            PhysicalPlan::DeletePartial(plan) => plan,
            _ => unreachable!("logic error"),
        };
        let partitions = &plan.parts;
        let executors = Fragmenter::get_executors(ctx);
        let mut fragment_actions = QueryFragmentActions::create(self.fragment_id);
        let partition_reshuffle = partitions.reshuffle(executors)?;

        for (executor, parts) in partition_reshuffle.iter() {
            let mut plan = self.plan.clone();

            let mut replace_delete_partial = ReplaceDeletePartial {
                partitions: parts.clone(),
            };
            plan = replace_delete_partial.replace(&plan)?;

            fragment_actions
                .add_action(QueryFragmentAction::create(executor.clone(), plan.clone()));
        }

        Ok(fragment_actions)
    }

    fn reshuffle<T: Clone>(
        executors: Vec<String>,
        partitions: Vec<T>,
    ) -> Result<HashMap<String, Vec<T>>> {
        let num_parts = partitions.len();
        let num_executors = executors.len();
        let mut executors_sorted = executors;
        executors_sorted.sort();
        let mut executor_part = HashMap::default();
        // the first num_parts % num_executors get parts_per_node parts
        // the remaining get parts_per_node - 1 parts
        let parts_per_node = (num_parts + num_executors - 1) / num_executors;
        for (idx, executor) in executors_sorted.iter().enumerate() {
            let begin = parts_per_node * idx;
            let end = num_parts.min(parts_per_node * (idx + 1));
            let parts = partitions[begin..end].to_vec();
            executor_part.insert(executor.clone(), parts);
            if end == num_parts && idx < num_executors - 1 {
                // reach here only when num_executors > num_parts
                executors_sorted[(idx + 1)..].iter().for_each(|executor| {
                    executor_part.insert(executor.clone(), vec![]);
                });
                break;
            }
        }

        Ok(executor_part)
    }

    fn redistribute_replace_into(&self, ctx: Arc<QueryContext>) -> Result<QueryFragmentActions> {
        let plan = match &self.plan {
            PhysicalPlan::ExchangeSink(plan) => plan,
            _ => unreachable!("logic error"),
        };
        let plan = match plan.input.as_ref() {
            PhysicalPlan::ReplaceInto(plan) => plan,
            _ => unreachable!("logic error"),
        };
        let partitions = &plan.segments;
        let executors = Fragmenter::get_executors(ctx.clone());
        let mut fragment_actions = QueryFragmentActions::create(self.fragment_id);
        let partition_reshuffle = Self::reshuffle(executors, partitions.clone())?;

        let local_id = &ctx.get_cluster().local_id;

        for (executor, parts) in partition_reshuffle.iter() {
            let mut plan = self.plan.clone();
            let need_insert = executor == local_id;

            let mut replace_replace_into = ReplaceReplaceInto {
                partitions: parts.clone(),
                need_insert,
            };
            plan = replace_replace_into.replace(&plan)?;

            fragment_actions
                .add_action(QueryFragmentAction::create(executor.clone(), plan.clone()));
        }

        Ok(fragment_actions)
    }

    fn get_read_source(&self) -> Result<DataSourcePlan> {
        if self.fragment_type != FragmentType::Source {
            return Err(ErrorCode::Internal(
                "Cannot get read source from a non-source fragment".to_string(),
            ));
        }

        let mut source = vec![];

        let mut collect_read_source = |plan: &PhysicalPlan| match plan {
            PhysicalPlan::TableScan(scan) => source.push(*scan.source.clone()),
            PhysicalPlan::CopyIntoTable(copy) => {
                if let Some(stage) = copy.source.as_stage().cloned() {
                    source.push(*stage);
                }
            }
            _ => {}
        };

        PhysicalPlan::traverse(
            &self.plan,
            &mut |_| true,
            &mut collect_read_source,
            &mut |_| {},
        );

        if source.len() != 1 {
            Err(ErrorCode::Internal(
                "Invalid source fragment with multiple table scan".to_string(),
            ))
        } else {
            Ok(source.remove(0))
        }
    }
}

pub struct ReplaceReadSource {
    pub source: DataSourcePlan,
}

impl PhysicalPlanReplacer for ReplaceReadSource {
    fn replace_table_scan(&mut self, plan: &TableScan) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::TableScan(TableScan {
            plan_id: plan.plan_id,
            source: Box::new(self.source.clone()),
            name_mapping: plan.name_mapping.clone(),
            table_index: plan.table_index,
            stat_info: plan.stat_info.clone(),
            internal_column: plan.internal_column.clone(),
        }))
    }

    fn replace_copy_into_table(&mut self, plan: &CopyIntoTable) -> Result<PhysicalPlan> {
        match &plan.source {
            CopyIntoTableSource::Query(query_ctx) => {
                let input = self.replace(&query_ctx.plan)?;
                Ok(PhysicalPlan::CopyIntoTable(Box::new(CopyIntoTable {
                    source: CopyIntoTableSource::Query(Box::new(QueryCtx {
                        plan: input,
                        ..*query_ctx.clone()
                    })),
                    ..plan.clone()
                })))
            }
            CopyIntoTableSource::Stage(_) => {
                Ok(PhysicalPlan::CopyIntoTable(Box::new(CopyIntoTable {
                    source: CopyIntoTableSource::Stage(Box::new(self.source.clone())),
                    ..plan.clone()
                })))
            }
        }
    }
}

struct ReplaceDeletePartial {
    pub partitions: Partitions,
}

impl PhysicalPlanReplacer for ReplaceDeletePartial {
    fn replace_delete_partial(&mut self, plan: &DeletePartial) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::DeletePartial(Box::new(DeletePartial {
            parts: self.partitions.clone(),
            ..plan.clone()
        })))
    }
}

struct ReplaceReplaceInto {
    pub partitions: Vec<(usize, Location)>,
    pub need_insert: bool,
}

impl PhysicalPlanReplacer for ReplaceReplaceInto {
    fn replace_replace_into(&mut self, plan: &ReplaceInto) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::ReplaceInto(ReplaceInto {
            input: Box::new(input),
            need_insert: self.need_insert,
            segments: self.partitions.clone(),
            ..plan.clone()
        }))
    }

    fn replace_deduplicate(&mut self, plan: &Deduplicate) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::Deduplicate(Deduplicate {
            input: Box::new(input),
            need_insert: self.need_insert,
            ..plan.clone()
        }))
    }
}
