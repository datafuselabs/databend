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

use databend_common_base::base::ProgressValues;
use databend_common_catalog::plan::Filters;
use databend_common_catalog::plan::PartInfoType;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ROW_ID_COLUMN_ID;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::TableInfo;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_transforms::processors::AsyncAccumulatingTransformer;
use databend_common_sql::executor::physical_plans::CommitSink;
use databend_common_sql::executor::physical_plans::DeleteSource;
use databend_common_sql::executor::physical_plans::Exchange;
use databend_common_sql::executor::physical_plans::FragmentKind;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_sql::executor::PhysicalPlanBuilder;
use databend_common_storages_factory::Table;
use databend_common_storages_fuse::operations::MutationGenerator;
use databend_common_storages_fuse::operations::SubqueryMutation;
use databend_common_storages_fuse::operations::TableMutationAggregator;
use databend_common_storages_fuse::operations::TransformMutationSubquery;
use databend_common_storages_fuse::operations::TransformSerializeBlock;
use databend_common_storages_fuse::operations::TransformSerializeSegment;
use databend_common_storages_fuse::operations::TruncateMode;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_table_meta::meta::TableSnapshot;
use log::debug;

use crate::interpreters::common::create_push_down_filters;
use crate::interpreters::HookOperator;
use crate::interpreters::Interpreter;
use crate::locks::LockManager;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::PipelineBuilder;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::DeletePlan;

/// interprets DeletePlan
pub struct DeleteInterpreter {
    ctx: Arc<QueryContext>,
    plan: DeletePlan,
}

impl DeleteInterpreter {
    /// Create the DeleteInterpreter from DeletePlan
    pub fn try_create(ctx: Arc<QueryContext>, plan: DeletePlan) -> Result<Self> {
        Ok(DeleteInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DeleteInterpreter {
    /// Get the name of current interpreter
    fn name(&self) -> &str {
        "DeleteInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[minitrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "delete_interpreter_execute");

        let is_distributed = !self.ctx.get_cluster().is_empty();

        let catalog_name = self.plan.catalog_name.as_str();
        let catalog = self.ctx.get_catalog(catalog_name).await?;
        let catalog_info = catalog.info();

        let db_name = self.plan.database_name.as_str();
        let tbl_name = self.plan.table_name.as_str();
        let tbl = catalog
            .get_table(self.ctx.get_tenant().name(), db_name, tbl_name)
            .await?;

        // Add table lock.
        let table_lock = LockManager::create_table_lock(tbl.get_table_info().clone())?;
        let lock_guard = table_lock.try_lock(self.ctx.clone()).await?;

        // refresh table.
        let tbl = tbl.refresh(self.ctx.as_ref()).await?;

        // check mutability
        tbl.check_mutable()?;

        if !self.plan.subquery_desc.is_empty() {
            let support_row_id = tbl.supported_internal_column(ROW_ID_COLUMN_ID);
            if !support_row_id {
                return Err(ErrorCode::from_string(format!(
                    "Delete with subquery is not supported for the table '{}', which lacks row_id support.",
                    tbl.name(),
                )));
            }

            let mut build_res = self.delete_by_subquery(tbl.clone()).await?;
            build_res.main_pipeline.add_lock_guard(lock_guard);

            return Ok(build_res);
        }

        let selection = self.plan.selection.clone();

        let (filters, col_indices) = if let Some(scalar) = selection {
            // prepare the filter expression
            let filters = create_push_down_filters(&scalar)?;

            let expr = filters.filter.as_expr(&BUILTIN_FUNCTIONS);
            if !expr.is_deterministic(&BUILTIN_FUNCTIONS) {
                return Err(ErrorCode::Unimplemented(
                    "Delete must have deterministic predicate",
                ));
            }

            let col_indices: Vec<usize> = scalar.used_columns().into_iter().collect();
            (Some(filters), col_indices)
        } else {
            (None, vec![])
        };

        let fuse_table = FuseTable::try_from_table(tbl.as_ref()).map_err(|_| {
            ErrorCode::Unimplemented(format!(
                "table {}, engine type {}, does not support DELETE FROM",
                tbl.name(),
                tbl.get_table_info().engine(),
            ))
        })?;

        let mut build_res = PipelineBuildResult::create();

        // check if table is empty
        let Some(snapshot) = fuse_table.read_table_snapshot().await? else {
            // no snapshot, no deletion
            return Ok(build_res);
        };
        if snapshot.summary.row_count == 0 {
            // empty snapshot, no deletion
            return Ok(build_res);
        }

        build_res.main_pipeline.add_lock_guard(lock_guard);
        // check if unconditional deletion
        let Some(filters) = filters else {
            let progress_values = ProgressValues {
                rows: snapshot.summary.row_count as usize,
                bytes: snapshot.summary.uncompressed_byte_size as usize,
            };
            self.ctx.get_write_progress().incr(&progress_values);
            // deleting the whole table... just a truncate
            fuse_table
                .do_truncate(
                    self.ctx.clone(),
                    &mut build_res.main_pipeline,
                    TruncateMode::Delete,
                )
                .await?;
            return Ok(build_res);
        };

        if col_indices.is_empty() {
            // here the situation: filter_expr is not null, but col_indices in empty, which
            // indicates the expr being evaluated is unrelated to the value of rows:
            //   e.g.
            //       `delete from t where 1 = 1`, `delete from t where now()`,
            //       or `delete from t where RANDOM()::INT::BOOLEAN`
            // if the `filter_expr` is of "constant" nullary :
            //   for the whole block, whether all of the rows should be kept or dropped,
            //   we can just return from here, without accessing the block data
            if fuse_table.try_eval_const(self.ctx.clone(), &fuse_table.schema(), &filters.filter)? {
                let progress_values = ProgressValues {
                    rows: snapshot.summary.row_count as usize,
                    bytes: snapshot.summary.uncompressed_byte_size as usize,
                };
                self.ctx.get_write_progress().incr(&progress_values);

                // deleting the whole table... just a truncate
                fuse_table
                    .do_truncate(
                        self.ctx.clone(),
                        &mut build_res.main_pipeline,
                        TruncateMode::Delete,
                    )
                    .await?;
                return Ok(build_res);
            }
        }

        let cluster = self.ctx.get_cluster();
        let is_lazy = !cluster.is_empty() && snapshot.segments.len() >= cluster.nodes.len();
        let partitions = fuse_table
            .mutation_read_partitions(
                self.ctx.clone(),
                snapshot.clone(),
                col_indices.clone(),
                Some(filters.clone()),
                is_lazy,
                true,
            )
            .await?;

        let physical_plan = self.build_physical_plan(
            filters,
            partitions,
            fuse_table.get_table_info().clone(),
            col_indices,
            snapshot,
            catalog_info,
            is_distributed,
            false,
        )?;

        build_res =
            build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan).await?;
        {
            let hook_operator = HookOperator::create(
                self.ctx.clone(),
                catalog_name.to_string(),
                db_name.to_string(),
                tbl_name.to_string(),
                "delete".to_string(),
                // table lock has been added, no need to check.
                false,
            );
            hook_operator
                .execute_refresh(&mut build_res.main_pipeline)
                .await;
        }

        Ok(build_res)
    }
}

impl DeleteInterpreter {
    async fn delete_by_subquery(&self, table: Arc<dyn Table>) -> Result<PipelineBuildResult> {
        let subquery_desc = &self.plan.subquery_desc[0];
        let input_expr = &subquery_desc.input_expr;
        let mut outer_columns = subquery_desc.outer_columns.clone();
        outer_columns.extend(subquery_desc.predicate_columns.iter());
        let ctx = &self.ctx;
        let table = FuseTable::try_from_table(table.as_ref())?;
        let snapshot = if let Some(snapshot) = table.read_table_snapshot().await? {
            snapshot
        } else {
            return Err(ErrorCode::from_string(format!(
                "read table {:?} snapshot failed",
                table.name()
            )));
        };

        // 1. build sub query join input
        let mut builder =
            PhysicalPlanBuilder::new(self.plan.metadata.clone(), self.ctx.clone(), false);
        let join_input = builder.build(input_expr, outer_columns).await?;

        let pipeline_builder = PipelineBuilder::create(
            ctx.get_function_context()?,
            ctx.get_settings(),
            ctx.clone(),
            vec![],
        );
        let mut build_res = pipeline_builder.finalize(&join_input)?;

        // 2. add TransformMutationSubquery
        build_res.main_pipeline.add_transform(|input, output| {
            TransformMutationSubquery::try_create(
                ctx.get_function_context()?,
                input,
                output,
                SubqueryMutation::Delete,
                subquery_desc.compare_op,
            )?
            .into_processor()
        })?;

        // 3. add TransformSerializeBlock
        let block_thresholds = table.get_block_thresholds();
        let cluster_stats_gen =
            table.get_cluster_stats_gen(self.ctx.clone(), 0, block_thresholds, None)?;
        build_res.main_pipeline.add_transform(|input, output| {
            let proc = TransformSerializeBlock::try_create(
                ctx.clone(),
                input,
                output,
                table,
                cluster_stats_gen.clone(),
                MutationKind::Replace,
            )?;
            proc.into_processor()
        })?;

        // 4. add TransformSerializeSegment
        build_res.main_pipeline.try_resize(1)?;
        build_res.main_pipeline.add_transform(|input, output| {
            let proc =
                TransformSerializeSegment::new(ctx.clone(), input, output, table, block_thresholds);
            proc.into_processor()
        })?;

        // 5. add TableMutationAggregator
        build_res.main_pipeline.add_transform(|input, output| {
            let aggregator =
                TableMutationAggregator::new(table, ctx.clone(), vec![], MutationKind::Replace);
            Ok(ProcessorPtr::create(AsyncAccumulatingTransformer::create(
                input, output, aggregator,
            )))
        })?;

        // 6. add CommitSink
        let snapshot_gen = MutationGenerator::new(snapshot, true);
        let lock = None;
        build_res.main_pipeline.add_sink(|input| {
            databend_common_storages_fuse::operations::CommitSink::try_create(
                table,
                ctx.clone(),
                None,
                vec![],
                snapshot_gen.clone(),
                input,
                None,
                lock.clone(),
                None,
                None,
            )
        })?;

        Ok(build_res)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn build_physical_plan(
        &self,
        filters: Filters,
        partitions: Partitions,
        table_info: TableInfo,
        col_indices: Vec<usize>,
        snapshot: Arc<TableSnapshot>,
        catalog_info: CatalogInfo,
        is_distributed: bool,
        query_row_id_col: bool,
    ) -> Result<PhysicalPlan> {
        debug_assert!(self.plan.subquery_desc.is_empty());
        let merge_meta = partitions.partitions_type() == PartInfoType::LazyLevel;
        let mut root = PhysicalPlan::DeleteSource(Box::new(DeleteSource {
            parts: partitions,
            filters,
            table_info: table_info.clone(),
            catalog_info: catalog_info.clone(),
            col_indices,
            query_row_id_col,
            snapshot: snapshot.clone(),
            plan_id: u32::MAX,
        }));

        if is_distributed {
            root = PhysicalPlan::Exchange(Exchange {
                plan_id: 0,
                input: Box::new(root),
                kind: FragmentKind::Merge,
                keys: vec![],
                allow_adjust_parallelism: true,
                ignore_exchange: false,
            });
        }
        let mut plan = PhysicalPlan::CommitSink(Box::new(CommitSink {
            input: Box::new(root),
            snapshot,
            table_info,
            catalog_info,
            mutation_kind: MutationKind::Delete,
            update_stream_meta: vec![],
            merge_meta,
            need_lock: false,
            deduplicated_label: None,
            plan_id: u32::MAX,
        }));
        plan.adjust_plan_id(&mut 0);
        Ok(plan)
    }
}
