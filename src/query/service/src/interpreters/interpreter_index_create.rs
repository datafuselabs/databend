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

use chrono::Utc;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::schema::CreateIndexReq;
use databend_common_meta_app::schema::IndexMeta;
use databend_common_meta_app::schema::IndexNameIdent;
use databend_common_meta_app::schema::IndexType;
use databend_common_sql::plans::CreateIndexPlan;
use databend_enterprise_aggregating_index::get_agg_index_handler;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct CreateIndexInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateIndexPlan,
}

impl CreateIndexInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateIndexPlan) -> Result<Self> {
        Ok(CreateIndexInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateIndexInterpreter {
    fn name(&self) -> &str {
        "CreateIndexInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let tenant = self.ctx.get_tenant();

        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::AggregateIndex)?;

        let index_name = self.plan.index_name.clone();
        let catalog = self.ctx.get_current_catalog();
        if catalog != "default" {
            return Err(ErrorCode::CatalogNotSupported(
                "Only allow creating aggregating index in default catalog",
            ));
        }

        let catalog = self.ctx.get_catalog(&catalog).await?;

        let create_index_req = CreateIndexReq {
            create_option: self.plan.create_option,
            name_ident: IndexNameIdent::new(tenant, &index_name),
            meta: IndexMeta {
                table_id: self.plan.table_id,
                index_type: IndexType::AGGREGATING,
                created_on: Utc::now(),
                dropped_on: None,
                updated_on: None,
                original_query: self.plan.original_query.clone(),
                query: self.plan.query.clone(),
                sync_creation: self.plan.sync_creation,
            },
        };

        let handler = get_agg_index_handler();
        let _ = handler.do_create_index(catalog, create_index_req).await?;

        Ok(PipelineBuildResult::create())
    }
}
