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

use common_exception::Result;
use common_sql::executor::MergeIntoSource;
use common_sql::executor::PhysicalPlan;
use common_sql::plans::MergeInto;

use super::Interpreter;
use super::InterpreterPtr;
use super::SelectInterpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct MergeIntoInterpreter {
    ctx: Arc<QueryContext>,
    plan: MergeInto,
}

impl MergeIntoInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: MergeInto) -> Result<InterpreterPtr> {
        Ok(Arc::new(MergeIntoInterpreter { ctx, plan }))
    }
}

#[async_trait::async_trait]
impl Interpreter for MergeIntoInterpreter {
    fn name(&self) -> &str {
        "MergeIntoInterpreter"
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        todo!()
    }
}

impl MergeIntoInterpreter {
    async fn build_physical_plan(&self) -> Result<PhysicalPlan> {
        let MergeInto {
            bind_context,
            input,
            meta_data,
            ..
        } = &self.plan;
        // build interpreter_select
        let select_intepreter = SelectInterpreter::try_create(
            self.ctx.clone(),
            *bind_context.clone(),
            *input.clone(),
            meta_data.clone(),
            None,
            false,
        )?;
        let join_input = select_intepreter.build_physical_plan().await?;
        // let merge_into_source = PhysicalPlan::MergeInt

        // }
        todo!()
    }
}
