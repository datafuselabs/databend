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

use std::collections::BTreeMap;
use std::sync::Arc;

use databend_common_catalog::lock::LockTableOption;
use databend_common_exception::Result;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::StringType;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::SendableDataBlockStream;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_common_pipeline_core::Pipeline;
use databend_common_sql::executor::physical_plans::CopyIntoTable;
use databend_common_sql::executor::physical_plans::CopyIntoTableSource;
use databend_common_sql::executor::physical_plans::Exchange;
use databend_common_sql::executor::physical_plans::FragmentKind;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::executor::physical_plans::TableScan;
use databend_common_sql::executor::table_read_plan::ToReadDataSourcePlan;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_storage::StageFileInfo;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_stage::StageTable;
use log::debug;
use log::info;

use crate::interpreters::common::check_deduplicate_label;
use crate::interpreters::common::dml_build_update_stream_req;
use crate::interpreters::HookOperator;
use crate::interpreters::Interpreter;
use crate::interpreters::SelectInterpreter;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::PipelineBuilder;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::CopyIntoTablePlan;
use crate::sql::plans::Plan;
use crate::stream::DataBlockStream;

pub struct CopyIntoTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: CopyIntoTablePlan,
}

impl CopyIntoTableInterpreter {
    /// Create a CopyInterpreter with context and [`CopyIntoTablePlan`].
    pub fn try_create(ctx: Arc<QueryContext>, plan: CopyIntoTablePlan) -> Result<Self> {
        Ok(CopyIntoTableInterpreter { ctx, plan })
    }

    #[async_backtrace::framed]
    async fn build_query(
        &self,
        query: &Plan,
    ) -> Result<(SelectInterpreter, Vec<UpdateStreamMetaReq>)> {
        let (s_expr, metadata, bind_context, formatted_ast) = match query {
            Plan::Query {
                s_expr,
                metadata,
                bind_context,
                formatted_ast,
                ..
            } => (s_expr, metadata, bind_context, formatted_ast),
            v => unreachable!("Input plan must be Query, but it's {}", v),
        };

        let update_stream_meta = dml_build_update_stream_req(self.ctx.clone(), metadata).await?;

        let select_interpreter = SelectInterpreter::try_create(
            self.ctx.clone(),
            *(bind_context.clone()),
            *s_expr.clone(),
            metadata.clone(),
            formatted_ast.clone(),
            false,
        )?;

        Ok((select_interpreter, update_stream_meta))
    }

    #[async_backtrace::framed]
    pub async fn build_physical_plan(
        &self,
        plan: &CopyIntoTablePlan,
    ) -> Result<(PhysicalPlan, Vec<UpdateStreamMetaReq>)> {
        let to_table = self
            .ctx
            .get_table(
                plan.catalog_info.catalog_name(),
                &plan.database_name,
                &plan.table_name,
            )
            .await?;
        let snapshot = FuseTable::try_from_table(to_table.as_ref())?
            .read_table_snapshot()
            .await?;
        let table_meta_timestamps = self
            .ctx
            .get_table_meta_timestamps(to_table.get_id(), snapshot)?;
        let mut update_stream_meta_reqs = vec![];
        let (source, project_columns) = if let Some(ref query) = plan.query {
            let query = if plan.enable_distributed {
                query.remove_exchange_for_select()
            } else {
                *query.clone()
            };

            let (query_interpreter, update_stream_meta) = self.build_query(&query).await?;
            update_stream_meta_reqs = update_stream_meta;
            let query_physical_plan = Box::new(query_interpreter.build_physical_plan().await?);

            let result_columns = query_interpreter.get_result_columns();
            (
                CopyIntoTableSource::Query(query_physical_plan),
                Some(result_columns),
            )
        } else {
            let stage_table = StageTable::try_create(plan.stage_table_info.clone())?;

            let data_source_plan = stage_table
                .read_plan(self.ctx.clone(), None, None, false, false)
                .await?;

            let mut name_mapping = BTreeMap::new();
            for (idx, field) in data_source_plan.schema().fields.iter().enumerate() {
                name_mapping.insert(field.name.clone(), idx);
            }

            (
                CopyIntoTableSource::Stage(Box::new(PhysicalPlan::TableScan(TableScan {
                    plan_id: 0,
                    name_mapping,
                    stat_info: None,
                    table_index: None,
                    internal_column: None,
                    source: Box::new(data_source_plan),
                }))),
                None,
            )
        };

        let mut root = PhysicalPlan::CopyIntoTable(Box::new(CopyIntoTable {
            plan_id: 0,
            required_values_schema: plan.required_values_schema.clone(),
            values_consts: plan.values_consts.clone(),
            required_source_schema: plan.required_source_schema.clone(),
            stage_table_info: plan.stage_table_info.clone(),
            table_info: to_table.get_table_info().clone(),
            force: plan.force,
            write_mode: plan.write_mode,
            validation_mode: plan.validation_mode.clone(),
            project_columns,
            source,
            is_transform: plan.is_transform,
            table_meta_timestamps,
        }));

        if plan.enable_distributed {
            root = PhysicalPlan::Exchange(Exchange {
                plan_id: 0,
                input: Box::new(root),
                kind: FragmentKind::Merge,
                keys: Vec::new(),
                allow_adjust_parallelism: true,
                ignore_exchange: false,
            });
        }

        let mut next_plan_id = 0;
        root.adjust_plan_id(&mut next_plan_id);

        Ok((root, update_stream_meta_reqs))
    }

    fn get_copy_into_table_result(&self) -> Result<Vec<DataBlock>> {
        let return_all = !self
            .plan
            .stage_table_info
            .stage_info
            .copy_options
            .return_failed_only;
        let cs = self.ctx.get_copy_status();

        let mut results = cs.files.iter().collect::<Vec<_>>();
        results.sort_by(|a, b| a.key().cmp(b.key()));

        let n = cs.files.len();
        let mut files = Vec::with_capacity(n);
        let mut rows_loaded = Vec::with_capacity(n);
        let mut errors_seen = Vec::with_capacity(n);
        let mut first_error = Vec::with_capacity(n);
        let mut first_error_line = Vec::with_capacity(n);

        for entry in results {
            let status = entry.value();
            if let Some(err) = &status.error {
                files.push(entry.key().clone());
                rows_loaded.push(status.num_rows_loaded as i32);
                errors_seen.push(err.num_errors as i32);
                first_error.push(Some(err.first_error.error.to_string().clone()));
                first_error_line.push(Some(err.first_error.line as i32 + 1));
            } else if return_all {
                files.push(entry.key().clone());
                rows_loaded.push(status.num_rows_loaded as i32);
                errors_seen.push(0);
                first_error.push(None);
                first_error_line.push(None);
            }
        }
        let blocks = vec![DataBlock::new_from_columns(vec![
            StringType::from_data(files),
            Int32Type::from_data(rows_loaded),
            Int32Type::from_data(errors_seen),
            StringType::from_opt_data(first_error),
            Int32Type::from_opt_data(first_error_line),
        ])];
        Ok(blocks)
    }

    /// Build commit insertion pipeline.
    async fn commit_insertion(
        &self,
        main_pipeline: &mut Pipeline,
        plan: &CopyIntoTablePlan,
        files_to_copy: Vec<StageFileInfo>,
        duplicated_files_detected: Vec<String>,
        update_stream_meta: Vec<UpdateStreamMetaReq>,
        deduplicated_label: Option<String>,
    ) -> Result<()> {
        let ctx = self.ctx.clone();
        let to_table = ctx
            .get_table(
                plan.catalog_info.catalog_name(),
                &plan.database_name,
                &plan.table_name,
            )
            .await?;

        // Commit.
        {
            let copied_files_meta_req = PipelineBuilder::build_upsert_copied_files_to_meta_req(
                ctx.clone(),
                to_table.as_ref(),
                &plan.stage_table_info.stage_info,
                &files_to_copy,
                plan.force,
            )?;

            let fuse_table = FuseTable::try_from_table(to_table.as_ref())?;
            let table_meta_timestamps = ctx.get_table_meta_timestamps(
                to_table.get_id(),
                fuse_table.read_table_snapshot().await?,
            )?;
            to_table.commit_insertion(
                ctx.clone(),
                main_pipeline,
                copied_files_meta_req,
                update_stream_meta,
                plan.write_mode.is_overwrite(),
                None,
                deduplicated_label,
                table_meta_timestamps,
            )?;
        }

        // Purge files.
        {
            info!(
                "set files to be purged, # of copied files: {}, # of duplicated files: {}",
                files_to_copy.len(),
                duplicated_files_detected.len()
            );

            let files_to_be_deleted = files_to_copy
                .into_iter()
                .map(|v| v.path)
                .chain(duplicated_files_detected)
                .collect::<Vec<_>>();
            // set on_finished callback.
            PipelineBuilder::set_purge_files_on_finished(
                ctx.clone(),
                files_to_be_deleted,
                plan.stage_table_info.stage_info.copy_options.purge,
                plan.stage_table_info.stage_info.clone(),
                main_pipeline,
            )?;
        }
        Ok(())
    }

    async fn on_no_files_to_copy(&self) -> Result<PipelineBuildResult> {
        // currently, there is only one thing that we care about:
        //
        // if `purge_duplicated_files_in_copy` and `purge` are all enabled,
        // and there are duplicated files detected, we should clean them up immediately.

        // it might be better to reuse the PipelineBuilder::set_purge_files_on_finished,
        // unfortunately, hooking the on_finished callback of a "blank" pipeline,
        // e.g. `PipelineBuildResult::create` leads to runtime error (during pipeline execution).

        if self.plan.stage_table_info.stage_info.copy_options.purge
            && !self
                .plan
                .stage_table_info
                .duplicated_files_detected
                .is_empty()
            && self
                .ctx
                .get_settings()
                .get_enable_purge_duplicated_files_in_copy()?
        {
            info!(
                "purge_duplicated_files_in_copy enabled, number of duplicated files: {}",
                self.plan.stage_table_info.duplicated_files_detected.len()
            );

            PipelineBuilder::purge_files_immediately(
                self.ctx.clone(),
                self.plan.stage_table_info.duplicated_files_detected.clone(),
                self.plan.stage_table_info.stage_info.clone(),
            )
            .await?;
        }
        Ok(PipelineBuildResult::create())
    }
}

#[async_trait::async_trait]
impl Interpreter for CopyIntoTableInterpreter {
    fn name(&self) -> &str {
        "CopyIntoTableInterpreterV2"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "copy_into_table_interpreter_execute_v2");

        if check_deduplicate_label(self.ctx.clone()).await? {
            return Ok(PipelineBuildResult::create());
        }

        if self.plan.no_file_to_copy {
            info!("no file to copy");
            return self.on_no_files_to_copy().await;
        }

        let (physical_plan, update_stream_meta) = self.build_physical_plan(&self.plan).await?;
        let mut build_res =
            build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan).await?;

        // Build commit insertion pipeline.
        {
            let files_to_copy = self
                .plan
                .stage_table_info
                .files_to_copy
                .clone()
                .unwrap_or_default();

            let duplicated_files_detected =
                self.plan.stage_table_info.duplicated_files_detected.clone();

            self.commit_insertion(
                &mut build_res.main_pipeline,
                &self.plan,
                files_to_copy,
                duplicated_files_detected,
                update_stream_meta,
                unsafe { self.ctx.get_settings().get_deduplicate_label()? },
            )
            .await?;
        }

        // Execute hook.
        {
            let hook_operator = HookOperator::create(
                self.ctx.clone(),
                self.plan.catalog_info.catalog_name().to_string(),
                self.plan.database_name.to_string(),
                self.plan.table_name.to_string(),
                MutationKind::Insert,
                LockTableOption::LockNoRetry,
            );
            hook_operator.execute(&mut build_res.main_pipeline).await;
        }

        Ok(build_res)
    }

    fn inject_result(&self) -> Result<SendableDataBlockStream> {
        let blocks = if self.plan.no_file_to_copy {
            vec![DataBlock::empty_with_schema(self.plan.schema())]
        } else {
            self.get_copy_into_table_result()?
        };

        Ok(Box::pin(DataBlockStream::create(None, blocks)))
    }
}
