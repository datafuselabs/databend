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

use databend_common_exception::Result;

use crate::executor::physical_plan::PhysicalPlan;
use crate::executor::physical_plans::AggregateExpand;
use crate::executor::physical_plans::AggregateFinal;
use crate::executor::physical_plans::AggregatePartial;
use crate::executor::physical_plans::ChunkAppendData;
use crate::executor::physical_plans::ChunkCastSchema;
use crate::executor::physical_plans::ChunkCommitInsert;
use crate::executor::physical_plans::ChunkFillAndReorder;
use crate::executor::physical_plans::ChunkFilter;
use crate::executor::physical_plans::ChunkMerge;
use crate::executor::physical_plans::ChunkProject;
use crate::executor::physical_plans::CommitSink;
use crate::executor::physical_plans::CompactSource;
use crate::executor::physical_plans::ConstantTableScan;
use crate::executor::physical_plans::CopyIntoLocation;
use crate::executor::physical_plans::CopyIntoTable;
use crate::executor::physical_plans::CopyIntoTableSource;
use crate::executor::physical_plans::CteScan;
use crate::executor::physical_plans::DeleteSource;
use crate::executor::physical_plans::DistributedInsertSelect;
use crate::executor::physical_plans::Duplicate;
use crate::executor::physical_plans::EvalScalar;
use crate::executor::physical_plans::Exchange;
use crate::executor::physical_plans::ExchangeSink;
use crate::executor::physical_plans::ExchangeSource;
use crate::executor::physical_plans::Filter;
use crate::executor::physical_plans::HashJoin;
use crate::executor::physical_plans::Limit;
use crate::executor::physical_plans::MaterializedCte;
use crate::executor::physical_plans::MergeInto;
use crate::executor::physical_plans::MergeIntoAddRowNumber;
use crate::executor::physical_plans::MergeIntoAppendNotMatched;
use crate::executor::physical_plans::MergeIntoSource;
use crate::executor::physical_plans::Project;
use crate::executor::physical_plans::ProjectSet;
use crate::executor::physical_plans::RangeJoin;
use crate::executor::physical_plans::ReclusterSink;
use crate::executor::physical_plans::ReclusterSource;
use crate::executor::physical_plans::ReplaceAsyncSourcer;
use crate::executor::physical_plans::ReplaceDeduplicate;
use crate::executor::physical_plans::ReplaceInto;
use crate::executor::physical_plans::RowFetch;
use crate::executor::physical_plans::Shuffle;
use crate::executor::physical_plans::Sort;
use crate::executor::physical_plans::TableScan;
use crate::executor::physical_plans::Udf;
use crate::executor::physical_plans::UnionAll;
use crate::executor::physical_plans::UpdateSource;
use crate::executor::physical_plans::Window;

pub trait PhysicalPlanReplacer {
    fn replace(&mut self, plan: &PhysicalPlan) -> Result<PhysicalPlan> {
        match plan {
            PhysicalPlan::TableScan(plan) => self.replace_table_scan(plan),
            PhysicalPlan::CteScan(plan) => self.replace_cte_scan(plan),
            PhysicalPlan::Filter(plan) => self.replace_filter(plan),
            PhysicalPlan::Project(plan) => self.replace_project(plan),
            PhysicalPlan::EvalScalar(plan) => self.replace_eval_scalar(plan),
            PhysicalPlan::AggregateExpand(plan) => self.replace_aggregate_expand(plan),
            PhysicalPlan::AggregatePartial(plan) => self.replace_aggregate_partial(plan),
            PhysicalPlan::AggregateFinal(plan) => self.replace_aggregate_final(plan),
            PhysicalPlan::Window(plan) => self.replace_window(plan),
            PhysicalPlan::Sort(plan) => self.replace_sort(plan),
            PhysicalPlan::Limit(plan) => self.replace_limit(plan),
            PhysicalPlan::RowFetch(plan) => self.replace_row_fetch(plan),
            PhysicalPlan::HashJoin(plan) => self.replace_hash_join(plan),
            PhysicalPlan::Exchange(plan) => self.replace_exchange(plan),
            PhysicalPlan::ExchangeSource(plan) => self.replace_exchange_source(plan),
            PhysicalPlan::ExchangeSink(plan) => self.replace_exchange_sink(plan),
            PhysicalPlan::UnionAll(plan) => self.replace_union(plan),
            PhysicalPlan::DistributedInsertSelect(plan) => self.replace_insert_select(plan),
            PhysicalPlan::ProjectSet(plan) => self.replace_project_set(plan),
            PhysicalPlan::CompactSource(plan) => self.replace_compact_source(plan),
            PhysicalPlan::DeleteSource(plan) => self.replace_delete_source(plan),
            PhysicalPlan::CommitSink(plan) => self.replace_commit_sink(plan),
            PhysicalPlan::RangeJoin(plan) => self.replace_range_join(plan),
            PhysicalPlan::CopyIntoTable(plan) => self.replace_copy_into_table(plan),
            PhysicalPlan::CopyIntoLocation(plan) => self.replace_copy_into_location(plan),
            PhysicalPlan::ReplaceAsyncSourcer(plan) => self.replace_async_sourcer(plan),
            PhysicalPlan::ReplaceDeduplicate(plan) => self.replace_deduplicate(plan),
            PhysicalPlan::ReplaceInto(plan) => self.replace_replace_into(plan),
            PhysicalPlan::MergeInto(plan) => self.replace_merge_into(plan),
            PhysicalPlan::MergeIntoAddRowNumber(plan) => self.replace_add_row_number(plan),
            PhysicalPlan::MergeIntoSource(plan) => self.replace_merge_into_source(plan),
            PhysicalPlan::MergeIntoAppendNotMatched(plan) => {
                self.replace_merge_into_row_id_apply(plan)
            }
            PhysicalPlan::MaterializedCte(plan) => self.replace_materialized_cte(plan),
            PhysicalPlan::ConstantTableScan(plan) => self.replace_constant_table_scan(plan),
            PhysicalPlan::ReclusterSource(plan) => self.replace_recluster_source(plan),
            PhysicalPlan::ReclusterSink(plan) => self.replace_recluster_sink(plan),
            PhysicalPlan::UpdateSource(plan) => self.replace_update_source(plan),
            PhysicalPlan::Udf(plan) => self.replace_udf(plan),
            PhysicalPlan::Duplicate(plan) => self.replace_duplicate(plan),
            PhysicalPlan::Shuffle(plan) => self.replace_shuffle(plan),
            PhysicalPlan::ChunkFilter(plan) => self.replace_chunk_filter(plan),
            PhysicalPlan::ChunkProject(plan) => self.replace_chunk_project(plan),
            PhysicalPlan::ChunkCastSchema(plan) => self.replace_chunk_cast_schema(plan),
            PhysicalPlan::ChunkFillAndReorder(plan) => self.replace_chunk_fill_and_reorder(plan),
            PhysicalPlan::ChunkAppendData(plan) => self.replace_chunk_append_data(plan),
            PhysicalPlan::ChunkMerge(plan) => self.replace_chunk_merge(plan),
            PhysicalPlan::ChunkCommitInsert(plan) => self.replace_chunk_commit_insert(plan),
        }
    }

    fn replace_recluster_source(&mut self, plan: &ReclusterSource) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::ReclusterSource(Box::new(plan.clone())))
    }

    fn replace_recluster_sink(&mut self, plan: &ReclusterSink) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::ReclusterSink(Box::new(ReclusterSink {
            input: Box::new(input),
            ..plan.clone()
        })))
    }

    fn replace_table_scan(&mut self, plan: &TableScan) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::TableScan(plan.clone()))
    }

    fn replace_cte_scan(&mut self, plan: &CteScan) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::CteScan(plan.clone()))
    }

    fn replace_constant_table_scan(&mut self, plan: &ConstantTableScan) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::ConstantTableScan(plan.clone()))
    }

    fn replace_filter(&mut self, plan: &Filter) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::Filter(Filter {
            plan_id: plan.plan_id,
            projections: plan.projections.clone(),
            input: Box::new(input),
            predicates: plan.predicates.clone(),
            stat_info: plan.stat_info.clone(),
        }))
    }

    fn replace_project(&mut self, plan: &Project) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::Project(Project {
            plan_id: plan.plan_id,
            input: Box::new(input),
            projections: plan.projections.clone(),
            ignore_result: plan.ignore_result,
            columns: plan.columns.clone(),
            stat_info: plan.stat_info.clone(),
        }))
    }

    fn replace_eval_scalar(&mut self, plan: &EvalScalar) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::EvalScalar(EvalScalar {
            plan_id: plan.plan_id,
            projections: plan.projections.clone(),
            input: Box::new(input),
            exprs: plan.exprs.clone(),
            stat_info: plan.stat_info.clone(),
        }))
    }

    fn replace_aggregate_expand(&mut self, plan: &AggregateExpand) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::AggregateExpand(AggregateExpand {
            plan_id: plan.plan_id,
            input: Box::new(input),
            group_bys: plan.group_bys.clone(),
            grouping_sets: plan.grouping_sets.clone(),
            stat_info: plan.stat_info.clone(),
        }))
    }

    fn replace_aggregate_partial(&mut self, plan: &AggregatePartial) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::AggregatePartial(AggregatePartial {
            plan_id: plan.plan_id,
            input: Box::new(input),
            enable_experimental_aggregate_hashtable: plan.enable_experimental_aggregate_hashtable,
            group_by: plan.group_by.clone(),
            group_by_display: plan.group_by_display.clone(),
            agg_funcs: plan.agg_funcs.clone(),
            stat_info: plan.stat_info.clone(),
        }))
    }

    fn replace_aggregate_final(&mut self, plan: &AggregateFinal) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::AggregateFinal(AggregateFinal {
            plan_id: plan.plan_id,
            input: Box::new(input),
            before_group_by_schema: plan.before_group_by_schema.clone(),
            group_by: plan.group_by.clone(),
            agg_funcs: plan.agg_funcs.clone(),
            group_by_display: plan.group_by_display.clone(),
            stat_info: plan.stat_info.clone(),
            limit: plan.limit,
        }))
    }

    fn replace_window(&mut self, plan: &Window) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::Window(Window {
            plan_id: plan.plan_id,
            index: plan.index,
            input: Box::new(input),
            func: plan.func.clone(),
            partition_by: plan.partition_by.clone(),
            order_by: plan.order_by.clone(),
            window_frame: plan.window_frame.clone(),
            limit: plan.limit,
        }))
    }

    fn replace_hash_join(&mut self, plan: &HashJoin) -> Result<PhysicalPlan> {
        let build = self.replace(&plan.build)?;
        let probe = self.replace(&plan.probe)?;

        Ok(PhysicalPlan::HashJoin(HashJoin {
            plan_id: plan.plan_id,
            projections: plan.projections.clone(),
            probe_projections: plan.probe_projections.clone(),
            build_projections: plan.build_projections.clone(),
            build: Box::new(build),
            probe: Box::new(probe),
            build_keys: plan.build_keys.clone(),
            probe_keys: plan.probe_keys.clone(),
            non_equi_conditions: plan.non_equi_conditions.clone(),
            join_type: plan.join_type.clone(),
            marker_index: plan.marker_index,
            from_correlated_subquery: plan.from_correlated_subquery,
            probe_to_build: plan.probe_to_build.clone(),
            output_schema: plan.output_schema.clone(),
            need_hold_hash_table: plan.need_hold_hash_table,
            stat_info: plan.stat_info.clone(),
            probe_keys_rt: plan.probe_keys_rt.clone(),
            enable_bloom_runtime_filter: plan.enable_bloom_runtime_filter,
            broadcast: plan.broadcast,
            single_to_inner: plan.single_to_inner.clone(),
        }))
    }

    fn replace_materialized_cte(&mut self, plan: &MaterializedCte) -> Result<PhysicalPlan> {
        let left = self.replace(&plan.left)?;
        let right = self.replace(&plan.right)?;

        Ok(PhysicalPlan::MaterializedCte(MaterializedCte {
            plan_id: plan.plan_id,
            left: Box::new(left),
            right: Box::new(right),
            cte_idx: plan.cte_idx,
            left_output_columns: plan.left_output_columns.clone(),
        }))
    }

    fn replace_range_join(&mut self, plan: &RangeJoin) -> Result<PhysicalPlan> {
        let left = self.replace(&plan.left)?;
        let right = self.replace(&plan.right)?;

        Ok(PhysicalPlan::RangeJoin(RangeJoin {
            plan_id: plan.plan_id,
            left: Box::new(left),
            right: Box::new(right),
            conditions: plan.conditions.clone(),
            other_conditions: plan.other_conditions.clone(),
            join_type: plan.join_type.clone(),
            range_join_type: plan.range_join_type.clone(),
            stat_info: plan.stat_info.clone(),
        }))
    }

    fn replace_sort(&mut self, plan: &Sort) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::Sort(Sort {
            plan_id: plan.plan_id,
            input: Box::new(input),
            order_by: plan.order_by.clone(),
            limit: plan.limit,
            after_exchange: plan.after_exchange,
            pre_projection: plan.pre_projection.clone(),
            stat_info: plan.stat_info.clone(),
        }))
    }

    fn replace_limit(&mut self, plan: &Limit) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::Limit(Limit {
            plan_id: plan.plan_id,
            input: Box::new(input),
            limit: plan.limit,
            offset: plan.offset,
            stat_info: plan.stat_info.clone(),
        }))
    }

    fn replace_row_fetch(&mut self, plan: &RowFetch) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::RowFetch(RowFetch {
            plan_id: plan.plan_id,
            input: Box::new(input),
            source: plan.source.clone(),
            row_id_col_offset: plan.row_id_col_offset,
            cols_to_fetch: plan.cols_to_fetch.clone(),
            fetched_fields: plan.fetched_fields.clone(),
            stat_info: plan.stat_info.clone(),
        }))
    }

    fn replace_exchange(&mut self, plan: &Exchange) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::Exchange(Exchange {
            plan_id: plan.plan_id,
            input: Box::new(input),
            kind: plan.kind.clone(),
            keys: plan.keys.clone(),
            ignore_exchange: plan.ignore_exchange,
            allow_adjust_parallelism: plan.allow_adjust_parallelism,
        }))
    }

    fn replace_exchange_source(&mut self, plan: &ExchangeSource) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::ExchangeSource(plan.clone()))
    }

    fn replace_exchange_sink(&mut self, plan: &ExchangeSink) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::ExchangeSink(ExchangeSink {
            // TODO(leiysky): we reuse the plan id of the Exchange node here,
            // should generate a new one.
            plan_id: plan.plan_id,

            input: Box::new(input),
            schema: plan.schema.clone(),
            kind: plan.kind.clone(),
            keys: plan.keys.clone(),
            destination_fragment_id: plan.destination_fragment_id,
            query_id: plan.query_id.clone(),
            ignore_exchange: plan.ignore_exchange,
            allow_adjust_parallelism: plan.allow_adjust_parallelism,
        }))
    }

    fn replace_union(&mut self, plan: &UnionAll) -> Result<PhysicalPlan> {
        let left = self.replace(&plan.left)?;
        let right = self.replace(&plan.right)?;
        Ok(PhysicalPlan::UnionAll(UnionAll {
            plan_id: plan.plan_id,
            left: Box::new(left),
            right: Box::new(right),
            schema: plan.schema.clone(),
            pairs: plan.pairs.clone(),
            stat_info: plan.stat_info.clone(),
        }))
    }

    fn replace_copy_into_table(&mut self, plan: &CopyIntoTable) -> Result<PhysicalPlan> {
        match &plan.source {
            CopyIntoTableSource::Stage(_) => {
                Ok(PhysicalPlan::CopyIntoTable(Box::new(plan.clone())))
            }
            CopyIntoTableSource::Query(query_physical_plan) => {
                let input = self.replace(query_physical_plan)?;
                Ok(PhysicalPlan::CopyIntoTable(Box::new(CopyIntoTable {
                    source: CopyIntoTableSource::Query(Box::new(input)),
                    ..plan.clone()
                })))
            }
        }
    }

    fn replace_copy_into_location(&mut self, plan: &CopyIntoLocation) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::CopyIntoLocation(Box::new(CopyIntoLocation {
            plan_id: plan.plan_id,
            input: Box::new(input),
            input_schema: plan.input_schema.clone(),
            to_stage_info: plan.to_stage_info.clone(),
        })))
    }

    fn replace_insert_select(&mut self, plan: &DistributedInsertSelect) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;

        Ok(PhysicalPlan::DistributedInsertSelect(Box::new(
            DistributedInsertSelect {
                plan_id: plan.plan_id,
                input: Box::new(input),
                catalog_info: plan.catalog_info.clone(),
                table_info: plan.table_info.clone(),
                select_schema: plan.select_schema.clone(),
                insert_schema: plan.insert_schema.clone(),
                select_column_bindings: plan.select_column_bindings.clone(),
                cast_needed: plan.cast_needed,
            },
        )))
    }

    fn replace_compact_source(&mut self, plan: &CompactSource) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::CompactSource(Box::new(plan.clone())))
    }

    fn replace_delete_source(&mut self, plan: &DeleteSource) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::DeleteSource(Box::new(plan.clone())))
    }

    fn replace_update_source(&mut self, plan: &UpdateSource) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::UpdateSource(Box::new(plan.clone())))
    }

    fn replace_commit_sink(&mut self, plan: &CommitSink) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::CommitSink(Box::new(CommitSink {
            input: Box::new(input),
            ..plan.clone()
        })))
    }

    fn replace_async_sourcer(&mut self, plan: &ReplaceAsyncSourcer) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::ReplaceAsyncSourcer(plan.clone()))
    }

    fn replace_deduplicate(&mut self, plan: &ReplaceDeduplicate) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::ReplaceDeduplicate(Box::new(
            ReplaceDeduplicate {
                input: Box::new(input),
                ..plan.clone()
            },
        )))
    }

    fn replace_replace_into(&mut self, plan: &ReplaceInto) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::ReplaceInto(Box::new(ReplaceInto {
            input: Box::new(input),
            ..plan.clone()
        })))
    }

    fn replace_merge_into(&mut self, plan: &MergeInto) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::MergeInto(Box::new(MergeInto {
            input: Box::new(input),
            ..plan.clone()
        })))
    }

    fn replace_add_row_number(&mut self, plan: &MergeIntoAddRowNumber) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::MergeIntoAddRowNumber(Box::new(
            MergeIntoAddRowNumber {
                input: Box::new(input),
                ..plan.clone()
            },
        )))
    }

    fn replace_merge_into_source(&mut self, plan: &MergeIntoSource) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::MergeIntoSource(MergeIntoSource {
            input: Box::new(input),
            ..plan.clone()
        }))
    }

    fn replace_merge_into_row_id_apply(
        &mut self,
        plan: &MergeIntoAppendNotMatched,
    ) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::MergeIntoAppendNotMatched(Box::new(
            MergeIntoAppendNotMatched {
                input: Box::new(input),
                ..plan.clone()
            },
        )))
    }

    fn replace_project_set(&mut self, plan: &ProjectSet) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::ProjectSet(ProjectSet {
            plan_id: plan.plan_id,
            input: Box::new(input),
            srf_exprs: plan.srf_exprs.clone(),
            projections: plan.projections.clone(),
            stat_info: plan.stat_info.clone(),
        }))
    }

    fn replace_udf(&mut self, plan: &Udf) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::Udf(Udf {
            plan_id: plan.plan_id,
            input: Box::new(input),
            udf_funcs: plan.udf_funcs.clone(),
            stat_info: plan.stat_info.clone(),
            script_udf: plan.script_udf,
        }))
    }

    fn replace_duplicate(&mut self, plan: &Duplicate) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::Duplicate(Box::new(Duplicate {
            input: Box::new(input),
            ..plan.clone()
        })))
    }

    fn replace_shuffle(&mut self, plan: &Shuffle) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::Shuffle(Box::new(Shuffle {
            input: Box::new(input),
            ..plan.clone()
        })))
    }

    fn replace_chunk_filter(&mut self, plan: &ChunkFilter) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::ChunkFilter(Box::new(ChunkFilter {
            input: Box::new(input),
            predicates: plan.predicates.clone(),
            ..plan.clone()
        })))
    }

    fn replace_chunk_project(&mut self, plan: &ChunkProject) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::ChunkProject(Box::new(ChunkProject {
            input: Box::new(input),
            projections: plan.projections.clone(),
            ..plan.clone()
        })))
    }

    fn replace_chunk_cast_schema(&mut self, plan: &ChunkCastSchema) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::ChunkCastSchema(Box::new(ChunkCastSchema {
            input: Box::new(input),
            cast_schemas: plan.cast_schemas.clone(),
            ..plan.clone()
        })))
    }

    fn replace_chunk_fill_and_reorder(
        &mut self,
        plan: &ChunkFillAndReorder,
    ) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::ChunkFillAndReorder(Box::new(
            ChunkFillAndReorder {
                input: Box::new(input),
                ..plan.clone()
            },
        )))
    }

    fn replace_chunk_append_data(&mut self, plan: &ChunkAppendData) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::ChunkAppendData(Box::new(ChunkAppendData {
            input: Box::new(input),
            ..plan.clone()
        })))
    }

    fn replace_chunk_merge(&mut self, plan: &ChunkMerge) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::ChunkMerge(Box::new(ChunkMerge {
            input: Box::new(input),
            ..plan.clone()
        })))
    }

    fn replace_chunk_commit_insert(&mut self, plan: &ChunkCommitInsert) -> Result<PhysicalPlan> {
        let input = self.replace(&plan.input)?;
        Ok(PhysicalPlan::ChunkCommitInsert(Box::new(
            ChunkCommitInsert {
                input: Box::new(input),
                ..plan.clone()
            },
        )))
    }
}

impl PhysicalPlan {
    pub fn traverse<'a, 'b>(
        plan: &'a PhysicalPlan,
        pre_visit: &'b mut dyn FnMut(&'a PhysicalPlan) -> bool,
        visit: &'b mut dyn FnMut(&'a PhysicalPlan),
        post_visit: &'b mut dyn FnMut(&'a PhysicalPlan),
    ) {
        if pre_visit(plan) {
            visit(plan);
            match plan {
                PhysicalPlan::TableScan(_)
                | PhysicalPlan::ReplaceAsyncSourcer(_)
                | PhysicalPlan::CteScan(_)
                | PhysicalPlan::ConstantTableScan(_)
                | PhysicalPlan::ReclusterSource(_)
                | PhysicalPlan::ExchangeSource(_)
                | PhysicalPlan::CompactSource(_)
                | PhysicalPlan::DeleteSource(_)
                | PhysicalPlan::UpdateSource(_) => {}
                PhysicalPlan::Filter(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::Project(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::EvalScalar(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::AggregateExpand(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::AggregatePartial(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::AggregateFinal(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::Window(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::Sort(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::Limit(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::RowFetch(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::HashJoin(plan) => {
                    Self::traverse(&plan.build, pre_visit, visit, post_visit);
                    Self::traverse(&plan.probe, pre_visit, visit, post_visit);
                }
                PhysicalPlan::Exchange(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::ExchangeSink(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::UnionAll(plan) => {
                    Self::traverse(&plan.left, pre_visit, visit, post_visit);
                    Self::traverse(&plan.right, pre_visit, visit, post_visit);
                }
                PhysicalPlan::DistributedInsertSelect(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::ProjectSet(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit)
                }
                PhysicalPlan::CopyIntoTable(plan) => match &plan.source {
                    CopyIntoTableSource::Query(input) => {
                        Self::traverse(input, pre_visit, visit, post_visit);
                    }
                    CopyIntoTableSource::Stage(input) => {
                        Self::traverse(input, pre_visit, visit, post_visit);
                    }
                },
                PhysicalPlan::CopyIntoLocation(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit)
                }
                PhysicalPlan::RangeJoin(plan) => {
                    Self::traverse(&plan.left, pre_visit, visit, post_visit);
                    Self::traverse(&plan.right, pre_visit, visit, post_visit);
                }
                PhysicalPlan::ReclusterSink(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::CommitSink(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::ReplaceDeduplicate(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::ReplaceInto(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::MergeIntoSource(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::MergeInto(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::MergeIntoAddRowNumber(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::MergeIntoAppendNotMatched(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::MaterializedCte(plan) => {
                    Self::traverse(&plan.left, pre_visit, visit, post_visit);
                    Self::traverse(&plan.right, pre_visit, visit, post_visit);
                }
                PhysicalPlan::Udf(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::Duplicate(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::Shuffle(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::ChunkFilter(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::ChunkProject(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::ChunkCastSchema(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::ChunkFillAndReorder(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::ChunkAppendData(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::ChunkMerge(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
                PhysicalPlan::ChunkCommitInsert(plan) => {
                    Self::traverse(&plan.input, pre_visit, visit, post_visit);
                }
            }
            post_visit(plan);
        }
    }
}
