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
use std::time::Duration;
use std::time::SystemTime;

use databend_common_catalog::plan::Filters;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check::check_function;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::TableInfo;
use databend_common_sql::executor::physical_plans::Exchange;
use databend_common_sql::executor::physical_plans::FragmentKind;
use databend_common_sql::executor::physical_plans::ReclusterSink;
use databend_common_sql::executor::physical_plans::ReclusterSource;
use databend_common_sql::executor::physical_plans::ReclusterTask;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::TableSnapshot;
use log::error;
use log::warn;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterClusteringHistory;
use crate::locks::LockExt;
use crate::locks::LockManager;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::executor::cast_expr_to_non_null_boolean;
use crate::sql::plans::ReclusterTablePlan;

pub struct ReclusterTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: ReclusterTablePlan,
}

impl ReclusterTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ReclusterTablePlan) -> Result<Self> {
        Ok(Self { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for ReclusterTableInterpreter {
    fn name(&self) -> &str {
        "ReclusterTableInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let plan = &self.plan;
        let ctx = self.ctx.clone();
        let settings = ctx.get_settings();
        let max_threads = settings.get_max_threads()? as usize;
        let recluster_timeout_secs = settings.get_recluster_timeout_secs()?;

        // Build extras via push down scalar
        let extras = if let Some(scalar) = &plan.push_downs {
            // prepare the filter expression
            let filter = cast_expr_to_non_null_boolean(
                scalar
                    .as_expr()?
                    .project_column_ref(|col| col.column_name.clone()),
            )?;
            // prepare the inverse filter expression
            let inverted_filter =
                check_function(None, "not", &[], &[filter.clone()], &BUILTIN_FUNCTIONS)?;

            let filters = Filters {
                filter: filter.as_remote_expr(),
                inverted_filter: inverted_filter.as_remote_expr(),
            };

            Some(PushDownInfo {
                filters: Some(filters),
                ..PushDownInfo::default()
            })
        } else {
            None
        };

        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;
        let tenant = self.ctx.get_tenant();
        let mut table = catalog
            .get_table(tenant.name(), &self.plan.database, &self.plan.table)
            .await?;

        // check mutability
        table.check_mutable()?;

        let mut times = 0;
        let mut block_count = 0;
        let start = SystemTime::now();
        let timeout = Duration::from_secs(recluster_timeout_secs);
        loop {
            if let Err(err) = ctx.check_aborting() {
                error!(
                    "execution of replace into statement aborted. server is shutting down or the query was killed. table: {}",
                    plan.table,
                );
                return Err(err);
            }

            let table_info = table.get_table_info().clone();

            // check if the table is locked.
            let table_lock = LockManager::create_table_lock(table_info.clone())?;
            if !table_lock.wait_lock_expired(catalog.clone()).await? {
                return Err(ErrorCode::TableAlreadyLocked(format!(
                    "table '{}' is locked, please retry recluster later",
                    self.plan.table
                )));
            }

            let fuse_table = FuseTable::try_from_table(table.as_ref())?;
            let mutator = fuse_table
                .build_recluster_mutator(ctx.clone(), extras.clone(), plan.limit)
                .await?;
            if mutator.is_none() {
                break;
            };

            let mutator = mutator.unwrap();
            if mutator.tasks.is_empty() {
                break;
            };
            block_count += mutator.recluster_blocks_count;
            let physical_plan = build_recluster_physical_plan(
                mutator.tasks,
                table_info,
                catalog.info(),
                mutator.snapshot,
                mutator.remained_blocks,
                mutator.removed_segment_indexes,
                mutator.removed_segment_summary,
            )?;

            let mut build_res =
                build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan).await?;
            assert!(build_res.main_pipeline.is_complete_pipeline()?);
            build_res.set_max_threads(max_threads);

            let executor_settings = ExecutorSettings::try_create(ctx.clone())?;

            let mut pipelines = build_res.sources_pipelines;
            pipelines.push(build_res.main_pipeline);

            let complete_executor =
                PipelineCompleteExecutor::from_pipelines(pipelines, executor_settings)?;
            ctx.set_executor(complete_executor.get_inner())?;
            complete_executor.execute()?;
            // make sure the executor is dropped before the next loop.
            drop(complete_executor);

            let elapsed_time = SystemTime::now().duration_since(start).unwrap();
            times += 1;
            // Status.
            {
                let status = format!(
                    "recluster: run recluster tasks:{} times, cost:{} sec",
                    times,
                    elapsed_time.as_secs()
                );
                ctx.set_status_info(&status);
            }

            if !plan.is_final {
                break;
            }

            if elapsed_time >= timeout {
                warn!(
                    "Recluster stopped because the runtime was over {} secs",
                    timeout.as_secs()
                );
                break;
            }

            // refresh table.
            table = catalog
                .get_table(tenant.name(), &self.plan.database, &self.plan.table)
                .await?;
        }

        if block_count != 0 {
            InterpreterClusteringHistory::write_log(
                &ctx,
                start,
                &plan.database,
                &plan.table,
                block_count,
            )?;
        }

        Ok(PipelineBuildResult::create())
    }
}

pub fn build_recluster_physical_plan(
    tasks: Vec<ReclusterTask>,
    table_info: TableInfo,
    catalog_info: CatalogInfo,
    snapshot: Arc<TableSnapshot>,
    remained_blocks: Vec<Arc<BlockMeta>>,
    removed_segment_indexes: Vec<usize>,
    removed_segment_summary: Statistics,
) -> Result<PhysicalPlan> {
    let is_distributed = tasks.len() > 1;
    let mut root = PhysicalPlan::ReclusterSource(Box::new(ReclusterSource {
        tasks,
        table_info: table_info.clone(),
        catalog_info: catalog_info.clone(),
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
    let mut plan = PhysicalPlan::ReclusterSink(Box::new(ReclusterSink {
        input: Box::new(root),
        table_info,
        catalog_info,
        snapshot,
        remained_blocks,
        removed_segment_indexes,
        removed_segment_summary,
        plan_id: u32::MAX,
    }));
    plan.adjust_plan_id(&mut 0);
    Ok(plan)
}
