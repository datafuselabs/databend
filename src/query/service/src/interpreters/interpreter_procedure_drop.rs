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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::DropProcedureReq;
use databend_common_sql::plans::DropProcedurePlan;
use databend_common_users::UserApiProvider;
use log::debug;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[derive(Debug)]
pub struct DropProcedureInterpreter {
    ctx: Arc<QueryContext>,
    pub(crate) plan: DropProcedurePlan,
}

impl DropProcedureInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropProcedurePlan) -> Result<Self> {
        Ok(DropProcedureInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropProcedureInterpreter {
    fn name(&self) -> &str {
        "DropProcedureInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "drop_procedure_execute");

        let tenant = self.plan.tenant.clone();

        let drop_procedure_req: DropProcedureReq = self.plan.clone().into();

        let dropped = UserApiProvider::instance()
            .procedure_api(&tenant)
            .drop_procedure(&drop_procedure_req.name_ident)
            .await?;
        if dropped.is_none() && !self.plan.if_exists {
            return Err(ErrorCode::UnknownProcedure(format!(
                "Unknown procedure '{}' while drop procedure",
                drop_procedure_req.name_ident
            )));
        }

        Ok(PipelineBuildResult::create())
    }
}
