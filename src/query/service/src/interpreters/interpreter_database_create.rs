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
use databend_common_meta_app::share::ShareGrantObjectPrivilege;
use databend_common_meta_app::share::ShareNameIdent;
use databend_common_meta_types::MatchSeq;
use databend_common_sharing::ShareEndpointManager;
use databend_common_sql::plans::CreateDatabasePlan;
use databend_common_storages_share::save_share_spec;
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

    async fn check_create_database_from_share(
        &self,
        tenant: &String,
        share_name: &ShareNameIdent,
    ) -> Result<()> {
        let share_specs = ShareEndpointManager::instance()
            .get_inbound_shares(
                tenant,
                Some(share_name.tenant.name().to_string()),
                Some(share_name.clone()),
            )
            .await?;
        match share_specs.first() {
            Some((_, share_spec)) => {
                if !share_spec.tenants.contains(tenant) {
                    return Err(ErrorCode::UnknownShareAccounts(format!(
                        "share {} has not granted privilege to {}",
                        share_name, tenant
                    )));
                }
                match share_spec.db_privileges {
                    Some(db_privileges) => {
                        if !db_privileges.contains(ShareGrantObjectPrivilege::Usage) {
                            return Err(ErrorCode::ShareHasNoGrantedPrivilege(format!(
                                "share {} has not granted privilege to {}",
                                share_name, tenant
                            )));
                        }
                    }
                    None => {
                        return Err(ErrorCode::ShareHasNoGrantedPrivilege(format!(
                            "share {} has not granted privilege to {}",
                            share_name, tenant
                        )));
                    }
                }
            }
            None => {
                return Err(ErrorCode::UnknownShare(format!(
                    "UnknownShare {:?}",
                    share_name
                )));
            }
        }
        Ok(())
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
        // if create from other tenant, check from share endpoint
        if let Some(ref share_name) = self.plan.meta.from_share {
            self.check_create_database_from_share(&tenant.name().to_string(), share_name)
                .await?;
        }

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

        // handle share cleanups with the DropDatabaseReply
        if let Some(spec_vec) = reply.spec_vec {
            let mut share_table_into = Vec::with_capacity(spec_vec.len());
            for share_spec in &spec_vec {
                share_table_into.push((share_spec.name.clone(), None));
            }

            save_share_spec(
                &self.ctx.get_tenant().name().to_string(),
                self.ctx.get_data_operator()?.operator(),
                Some(spec_vec),
                Some(share_table_into),
            )
            .await?;
        }

        Ok(PipelineBuildResult::create())
    }
}
