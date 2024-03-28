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
use std::time::SystemTime;

use databend_common_base::runtime::profile::get_statistics_desc;
use databend_common_base::runtime::profile::ProfileDesc;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::SendableDataBlockStream;
use databend_common_pipeline_core::processors::PlanProfile;
use databend_common_pipeline_core::SourcePipeBuilder;
use databend_common_sql::plans::Plan;
use databend_common_sql::PlanExtras;
use databend_common_sql::Planner;
use log::error;
use log::info;

use crate::interpreters::interpreter_txn_commit::CommitInterpreter;
use crate::interpreters::InterpreterMetrics;
use crate::interpreters::InterpreterQueryLog;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::pipelines::executor::PipelinePullingExecutor;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::SessionManager;
use crate::stream::DataBlockStream;
use crate::stream::ProgressStream;
use crate::stream::PullingExecutorStream;

#[async_trait::async_trait]
/// Interpreter is a trait for different PlanNode
/// Each type of planNode has its own corresponding interpreter
pub trait Interpreter: Sync + Send {
    /// Return the name of Interpreter, such as "CreateDatabaseInterpreter"
    fn name(&self) -> &str;

    fn is_txn_command(&self) -> bool {
        false
    }

    fn is_ddl(&self) -> bool;

    /// The core of the databend processor which will execute the logical plan and get the DataBlock
    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn execute(&self, ctx: Arc<QueryContext>) -> Result<SendableDataBlockStream> {
        ctx.set_status_info("building pipeline");
        InterpreterMetrics::record_query_start(&ctx);
        log_query_start(&ctx);

        if let Err(err) = ctx.check_aborting() {
            log_query_finished(&ctx, Some(err.clone()), false);
            return Err(err);
        }
        if self.is_ddl() {
            CommitInterpreter::try_create(ctx.clone())?
                .execute2()
                .await?;
            ctx.clear_tables_cache();
        }
        if !self.is_txn_command() && ctx.txn_mgr().lock().is_fail() {
            let error = ErrorCode::CurrentTransactionIsAborted(
                "current transaction is aborted, commands ignored until end of transaction block",
            );
            log_query_finished(&ctx, Some(error.clone()), false);
            return Err(error);
        }
        let mut build_res = match self.execute2().await {
            Ok(build_res) => build_res,
            Err(build_error) => {
                InterpreterMetrics::record_query_error(&ctx);
                log_query_finished(&ctx, Some(build_error.clone()), false);
                return Err(build_error);
            }
        };

        if build_res.main_pipeline.is_empty() {
            InterpreterMetrics::record_query_finished(&ctx, None);
            log_query_finished(&ctx, None, false);

            return Ok(Box::pin(DataBlockStream::create(None, vec![])));
        }

        let query_ctx = ctx.clone();
        build_res.main_pipeline.set_on_finished(move |may_error| {
            let mut has_profiles = false;
            if let Ok(plans_profile) = may_error {
                query_ctx.add_query_profiles(plans_profile);

                let query_profiles = query_ctx.get_query_profiles();

                if !query_profiles.is_empty() {
                    has_profiles = true;
                    #[derive(serde::Serialize)]
                    struct QueryProfiles {
                        query_id: String,
                        profiles: Vec<PlanProfile>,
                        statistics_desc: Arc<BTreeMap<ProfileStatisticsName, ProfileDesc>>,
                    }

                    info!(
                        target: "databend::log::profile",
                        "{}",
                        serde_json::to_string(&QueryProfiles {
                            query_id: query_ctx.get_id(),
                            profiles: query_profiles,
                            statistics_desc: get_statistics_desc(),
                        })?
                    );
                }
            }

            let err_opt = match may_error {
                Ok(_) => None,
                Err(e) => Some(e.clone()),
            };

            InterpreterMetrics::record_query_finished(&query_ctx, err_opt.clone());
            log_query_finished(&query_ctx, err_opt, has_profiles);

            match may_error {
                Ok(_) => Ok(()),
                Err(error) => Err(error.clone()),
            }
        });

        ctx.set_status_info("executing pipeline");

        let settings = ctx.get_settings();
        build_res.set_max_threads(settings.get_max_threads()? as usize);
        let settings = ExecutorSettings::try_create(ctx.clone())?;

        if build_res.main_pipeline.is_complete_pipeline()? {
            let mut pipelines = build_res.sources_pipelines;
            pipelines.push(build_res.main_pipeline);

            let complete_executor = PipelineCompleteExecutor::from_pipelines(pipelines, settings)?;

            ctx.set_executor(complete_executor.get_inner())?;
            complete_executor.execute()?;
            self.inject_result()
        } else {
            let pulling_executor = PipelinePullingExecutor::from_pipelines(build_res, settings)?;

            ctx.set_executor(pulling_executor.get_inner())?;
            Ok(Box::pin(ProgressStream::try_create(
                Box::pin(PullingExecutorStream::create(pulling_executor)?),
                ctx.get_result_progress(),
            )?))
        }
    }

    /// The core of the databend processor which will execute the logical plan and build the pipeline
    async fn execute2(&self) -> Result<PipelineBuildResult>;

    fn set_source_pipe_builder(&self, _builder: Option<SourcePipeBuilder>) -> Result<()> {
        Err(ErrorCode::Unimplemented(format!(
            "UnImplement set_source_pipe_builder method for {:?}",
            self.name()
        )))
    }

    fn inject_result(&self) -> Result<SendableDataBlockStream> {
        Ok(Box::pin(DataBlockStream::create(None, vec![])))
    }
}

pub type InterpreterPtr = Arc<dyn Interpreter>;

fn log_query_start(ctx: &QueryContext) {
    let now = SystemTime::now();
    let session = ctx.get_current_session();

    if session.get_type().is_user_session() {
        SessionManager::instance().status.write().query_start(now);
    }

    if let Err(error) = InterpreterQueryLog::log_start(ctx, now, None) {
        error!("interpreter.start.error: {:?}", error)
    }
}

fn log_query_finished(ctx: &QueryContext, error: Option<ErrorCode>, has_profiles: bool) {
    let now = SystemTime::now();
    let session = ctx.get_current_session();

    session.get_status().write().query_finish();
    if session.get_type().is_user_session() {
        SessionManager::instance().status.write().query_finish(now)
    }

    if let Err(error) = InterpreterQueryLog::log_finish(ctx, now, error, has_profiles) {
        error!("interpreter.finish.error: {:?}", error)
    }
}

/// There are two steps to execute a query:
/// 1. Plan the SQL
/// 2. Execute the plan -- interpreter
///
/// This function is used to plan the SQL. If an error occurs, we will log the query start and finished.
pub async fn interpreter_plan_sql(ctx: Arc<QueryContext>, sql: &str) -> Result<(Plan, PlanExtras)> {
    let mut planner = Planner::new(ctx.clone());
    let result = planner.plan_sql(sql).await;

    if result.is_err() {
        // Only log if there's an error
        log_query_start(&ctx);
        log_query_finished(&ctx, result.as_ref().err().cloned(), false);
    }

    result
}
