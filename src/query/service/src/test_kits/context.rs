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

use std::str;
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::SendableDataBlockStream;
use futures::TryStreamExt;

use crate::interpreters::InterpreterFactory;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sql::Planner;

/// If you no need to care the ctx please use TestFixture.execute_query().
pub async fn execute_query(ctx: Arc<QueryContext>, query: &str) -> Result<SendableDataBlockStream> {
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(query).await?;
    let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
    executor.execute(ctx.clone()).await
}

/// If you no need to care the ctx please use TestFixture.execute_command().
pub async fn execute_command(ctx: Arc<QueryContext>, query: &str) -> Result<()> {
    let res = execute_query(ctx, query).await?;
    res.try_collect::<Vec<DataBlock>>().await?;
    Ok(())
}

pub fn execute_pipeline(ctx: Arc<QueryContext>, mut res: PipelineBuildResult) -> Result<()> {
    let executor_settings = ExecutorSettings::try_create(ctx.clone())?;
    res.set_max_threads(ctx.get_settings().get_max_threads()? as usize);
    let mut pipelines = res.sources_pipelines;
    pipelines.push(res.main_pipeline);
    let executor = PipelineCompleteExecutor::from_pipelines(pipelines, executor_settings)?;
    ctx.set_executor(executor.get_inner())?;
    executor.execute()
}
