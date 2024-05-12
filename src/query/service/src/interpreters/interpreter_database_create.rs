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
use databend_common_management::RoleApi;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::schema::CreateDatabaseReq;
use databend_common_meta_types::MatchSeq;
use databend_common_sql::plans::CreateDatabasePlan;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;
use log::debug;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

#[derive(Debug)]
pub struct CreateDatabaseInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateDatabasePlan,
}

impl CreateDatabaseInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateDatabasePlan) -> Result<Self> {
        Ok(CreateDatabaseInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateDatabaseInterpreter {
    fn name(&self) -> &str {
        "CreateDatabaseInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[minitrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "create_database_execute");

        let tenant = self.plan.tenant.clone();

        let quota_api = UserApiProvider::instance().tenant_quota_api(&tenant);
        let quota = quota_api.get_quota(MatchSeq::GE(0)).await?.data;
        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;
        let databases = catalog.list_databases(&tenant).await?;
        if quota.max_databases != 0 && databases.len() >= quota.max_databases as usize {
            return Err(ErrorCode::TenantQuotaExceeded(format!(
                "Max databases quota exceeded {}",
                quota.max_databases
            )));
        };

        let create_db_req: CreateDatabaseReq = self.plan.clone().into();
        let reply = catalog.create_database(create_db_req).await?;

        // Grant ownership as the current role. The above create_db_req.meta.owner could be removed in
        // the future.
        let role_api = UserApiProvider::instance().role_api(&tenant);
        if let Some(current_role) = self.ctx.get_current_role() {
            role_api
                .grant_ownership(
                    &OwnershipObject::Database {
                        catalog_name: self.plan.catalog.clone(),
                        db_id: reply.db_id,
                    },
                    &current_role.name,
                )
                .await?;
            RoleCacheManager::instance().invalidate_cache(&tenant);
        }

        Ok(PipelineBuildResult::create())
    }
}
