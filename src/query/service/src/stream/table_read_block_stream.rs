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

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_exception::Result;
use databend_common_expression::SendableDataBlockStream;
use databend_common_pipeline_core::Pipeline;

use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelinePullingExecutor;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::storages::Table;
use crate::stream::PullingExecutorStream;

#[async_trait::async_trait]
pub trait ReadDataBlockStream: Send + Sync {
    async fn read_data_block_stream(
        &self,
        _ctx: Arc<QueryContext>,
        _plan: &DataSourcePlan,
    ) -> Result<SendableDataBlockStream>;
}

#[async_trait::async_trait]
impl<T: ?Sized + Table> ReadDataBlockStream for T {
    #[async_backtrace::framed]
    async fn read_data_block_stream(
        &self,
        ctx: Arc<QueryContext>,
        plan: &DataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let mut pipeline = Pipeline::create();
        ctx.set_partitions(plan.parts.clone())?;
        self.read_data(ctx.clone(), plan, &mut pipeline, true)?;

        let settings = ctx.get_settings();
        pipeline.set_max_threads(settings.get_max_threads()? as usize);
        let executor_settings = ExecutorSettings::try_create(ctx.clone())?;
        let executor = PipelinePullingExecutor::try_create(pipeline, executor_settings)?;
        ctx.set_executor(executor.get_inner())?;
        Ok(Box::pin(PullingExecutorStream::create(executor)?))
    }
}
