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
use databend_common_meta_app::schema::DropTableByIdReq;
use databend_common_sql::plans::DropViewPlan;
use databend_common_storages_stream::stream_table::STREAM_ENGINE;
use databend_common_storages_view::view_table::VIEW_ENGINE;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct DropViewInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropViewPlan,
}

impl DropViewInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropViewPlan) -> Result<Self> {
        Ok(DropViewInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropViewInterpreter {
    fn name(&self) -> &str {
        "DropViewInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog_name = self.plan.catalog.clone();
        let db_name = self.plan.database.clone();
        let view_name = self.plan.view_name.clone();
        let tbl = self
            .ctx
            .get_table(&catalog_name, &db_name, &view_name)
            .await
            .ok();

        if tbl.is_none() && !self.plan.if_exists {
            return Err(ErrorCode::UnknownTable(format!(
                "unknown view `{}`.`{}` in catalog '{}'",
                db_name, view_name, &catalog_name
            )));
        }

        if let Some(table) = &tbl {
            let engine = table.get_table_info().engine();
            if engine != VIEW_ENGINE {
                return Err(ErrorCode::TableEngineNotSupported(format!(
                    "{}.{} is not VIEW, please use `DROP {} {}.{}`",
                    &self.plan.database,
                    &self.plan.view_name,
                    if engine == STREAM_ENGINE {
                        "STREAM"
                    } else {
                        "TABLE"
                    },
                    &self.plan.database,
                    &self.plan.view_name
                )));
            }

            let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;
            let db = catalog
                .get_database(self.plan.tenant.name(), &self.plan.database)
                .await?;
            catalog
                .drop_table_by_id(DropTableByIdReq {
                    if_exists: self.plan.if_exists,
                    tenant: self.plan.tenant.clone(),
                    table_name: self.plan.view_name.clone(),
                    tb_id: table.get_id(),
                    db_id: db.get_db_info().ident.db_id,
                })
                .await?;
        };

        Ok(PipelineBuildResult::create())
    }
}
