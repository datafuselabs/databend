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

use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_management::RoleApi;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::schema::DropTableByIdReq;
use databend_common_sql::plans::DropTablePlan;
use databend_common_storages_fuse::operations::TruncateMode;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_stream::stream_table::STREAM_ENGINE;
use databend_common_storages_view::view_table::VIEW_ENGINE;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct DropTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropTablePlan,
}

impl DropTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropTablePlan) -> Result<Self> {
        Ok(DropTableInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropTableInterpreter {
    fn name(&self) -> &str {
        "DropTableInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog_name = self.plan.catalog.as_str();
        let db_name = self.plan.database.as_str();
        let tbl_name = self.plan.table.as_str();
        let tbl = match self.ctx.get_table(catalog_name, db_name, tbl_name).await {
            Ok(table) => table,
            Err(error) => {
                if (error.code() == ErrorCode::UNKNOWN_TABLE
                    || error.code() == ErrorCode::UNKNOWN_CATALOG
                    || error.code() == ErrorCode::UNKNOWN_DATABASE)
                    && self.plan.if_exists
                {
                    return Ok(PipelineBuildResult::create());
                } else {
                    return Err(error);
                }
            }
        };

        let engine = tbl.get_table_info().engine();
        if matches!(engine, VIEW_ENGINE | STREAM_ENGINE) {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "{}.{} engine is {} that doesn't support drop, use `DROP {} {}.{}` instead",
                &self.plan.database,
                &self.plan.table,
                engine,
                engine,
                &self.plan.database,
                &self.plan.table
            )));
        }
        let catalog = self.ctx.get_catalog(catalog_name).await?;

        // Although even if data is in READ_ONLY mode,
        // as a catalog object, the table itself is allowed to be dropped (and undropped later),
        // `drop table ALL` is NOT allowed, which implies that the table data need to be truncated.
        if self.plan.all {
            // check mutability, if the table is read only, we cannot truncate the data
            tbl.check_mutable().map_err(|e| {
                    e.add_message(" drop table ALL is not allowed for read only table, please consider remove the option ALL")
                })?
        }

        let tenant = self.ctx.get_tenant();
        let db = catalog.get_database(&tenant, &self.plan.database).await?;
        // actually drop table
        let _resp = catalog
            .drop_table_by_id(DropTableByIdReq {
                if_exists: self.plan.if_exists,
                tenant: tenant.clone(),
                table_name: tbl_name.to_string(),
                tb_id: tbl.get_table_info().ident.table_id,
                db_id: db.get_db_info().ident.db_id,
            })
            .await?;

        // we should do `drop ownership` after actually drop table, otherwise when we drop the ownership,
        // but the table still exists, in the interval maybe some unexpected things will happen.
        // drop the ownership
        let role_api = UserApiProvider::instance().role_api(&self.plan.tenant);
        let owner_object = OwnershipObject::Table {
            catalog_name: self.plan.catalog.clone(),
            db_id: db.get_db_info().ident.db_id,
            table_id: tbl.get_table_info().ident.table_id,
        };

        role_api.revoke_ownership(&owner_object).await?;
        RoleCacheManager::instance().invalidate_cache(&tenant);

        let mut build_res = PipelineBuildResult::create();
        // if `plan.all`, truncate, then purge the historical data
        if self.plan.all {
            // the above `catalog.drop_table` operation changed the table meta version,
            // thus if we do not refresh the table instance, `truncate` will fail
            let latest = tbl.as_ref().refresh(self.ctx.as_ref()).await?;
            let maybe_fuse_table = FuseTable::try_from_table(latest.as_ref());
            // if target table if of type FuseTable, purge its historical data
            // otherwise, plain truncate
            if let Ok(fuse_table) = maybe_fuse_table {
                fuse_table
                    .do_truncate(
                        self.ctx.clone(),
                        &mut build_res.main_pipeline,
                        TruncateMode::Purge,
                    )
                    .await?
            } else {
                latest
                    .truncate(self.ctx.clone(), &mut build_res.main_pipeline)
                    .await?
            }
        }

        Ok(build_res)
    }
}
