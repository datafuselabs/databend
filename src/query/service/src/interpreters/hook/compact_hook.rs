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
use std::time::Instant;

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_pipeline_core::Pipeline;
use databend_common_sql::plans::OptimizeTableAction;
use databend_common_sql::plans::OptimizeTablePlan;
use log::info;

use crate::interpreters::common::metrics_inc_compact_hook_compact_time_ms;
use crate::interpreters::common::metrics_inc_compact_hook_main_operation_time_ms;
use crate::interpreters::Interpreter;
use crate::interpreters::OptimizeTableInterpreter;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::sessions::QueryContext;

pub struct CompactTargetTableDescription {
    pub catalog: String,
    pub database: String,
    pub table: String,
}

pub struct CompactHookTraceCtx {
    pub start: Instant,
    pub operation_name: String,
}

/// Hook compact action with a on-finished callback.
/// errors (if any) are ignored.
pub async fn hook_compact(
    ctx: Arc<QueryContext>,
    pipeline: &mut Pipeline,
    compact_target: CompactTargetTableDescription,
    trace_ctx: CompactHookTraceCtx,
    need_lock: bool,
) {
    let op_name = trace_ctx.operation_name.clone();
    if let Err(e) = do_hook_compact(ctx, pipeline, compact_target, trace_ctx, need_lock).await {
        info!("compact hook ({}) with error (ignored): {}", op_name, e);
    }
}

/// hook the compact action with a on-finished callback.
async fn do_hook_compact(
    ctx: Arc<QueryContext>,
    pipeline: &mut Pipeline,
    compact_target: CompactTargetTableDescription,
    trace_ctx: CompactHookTraceCtx,
    need_lock: bool,
) -> Result<()> {
    if pipeline.is_empty() {
        return Ok(());
    }

    if ctx.get_settings().get_enable_compact_after_write()? {
        {
            pipeline.set_on_finished(move |err| {
                if !ctx.get_need_compact_after_write() {
                    return Ok(());
                }

                let op_name = &trace_ctx.operation_name;
                metrics_inc_compact_hook_main_operation_time_ms(op_name, trace_ctx.start.elapsed().as_millis() as u64);

                let compact_start_at = Instant::now();
                if err.is_ok() {
                    info!("execute {op_name} finished successfully. running table optimization job.");
                    match GlobalIORuntime::instance().block_on({
                        compact_table(ctx, compact_target, need_lock)
                    }) {
                        Ok(_) => {
                            info!("execute {op_name} finished successfully. table optimization job finished.");
                        }
                        Err(e) => { info!("execute {op_name} finished successfully. table optimization job failed. {:?}", e) }
                    }
                }
                metrics_inc_compact_hook_compact_time_ms(&trace_ctx.operation_name, compact_start_at.elapsed().as_millis() as u64);

                Ok(())
            });
        }
    }
    Ok(())
}

/// compact the target table, will do optimize table actions, including:
///  - compact blocks
///  - re-cluster if the cluster keys are defined
async fn compact_table(
    ctx: Arc<QueryContext>,
    compact_target: CompactTargetTableDescription,
    need_lock: bool,
) -> Result<()> {
    // evict the table from cache
    ctx.evict_table_from_cache(
        &compact_target.catalog,
        &compact_target.database,
        &compact_target.table,
    )?;

    // build the optimize table pipeline with compact action.
    let optimize_interpreter =
        OptimizeTableInterpreter::try_create(ctx.clone(), OptimizeTablePlan {
            catalog: compact_target.catalog,
            database: compact_target.database,
            table: compact_target.table,
            action: OptimizeTableAction::CompactBlocks,
            limit: Some(3),
            need_lock,
        })?;

    let mut build_res = optimize_interpreter.execute2().await?;

    if build_res.main_pipeline.is_empty() {
        return Ok(());
    }

    // execute the compact pipeline (for table with cluster keys, re-cluster will also be executed)
    let settings = ctx.get_settings();
    build_res.set_max_threads(settings.get_max_threads()? as usize);
    let settings = ExecutorSettings::try_create(ctx.clone())?;

    if build_res.main_pipeline.is_complete_pipeline()? {
        let mut pipelines = build_res.sources_pipelines;
        pipelines.push(build_res.main_pipeline);

        let complete_executor = PipelineCompleteExecutor::from_pipelines(pipelines, settings)?;

        // keep the original progress value
        let progress_value = ctx.get_write_progress_value();
        // Clears previously generated segment locations to avoid duplicate data in the refresh phase
        ctx.clear_segment_locations()?;
        ctx.set_executor(complete_executor.get_inner())?;
        complete_executor.execute()?;

        // reset the progress value
        ctx.get_write_progress().set(&progress_value);
    }
    Ok(())
}
