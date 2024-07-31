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

use databend_common_exception::Result;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::schema::CreateTableIndexReq;
use databend_common_sql::plans::CreateTableIndexPlan;
use databend_common_storages_fuse::TableContext;
use databend_enterprise_inverted_index::get_inverted_index_handler;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct CreateTableIndexInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateTableIndexPlan,
}

impl CreateTableIndexInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateTableIndexPlan) -> Result<Self> {
        Ok(CreateTableIndexInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateTableIndexInterpreter {
    fn name(&self) -> &str {
        "CreateTableIndexInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::InvertedIndex)?;

        let index_name = self.plan.index_name.clone();
        let column_ids = self.plan.column_ids.clone();
        let sync_creation = self.plan.sync_creation;
        let table_id = self.plan.table_id;
        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;

        let create_index_req = CreateTableIndexReq {
            create_option: self.plan.create_option,
            table_id,
            name: index_name,
            column_ids,
            sync_creation,
            options: self.plan.index_options.clone(),
        };

        let handler = get_inverted_index_handler();
        let _ = handler
            .do_create_table_index(catalog, create_index_req)
            .await?;

        Ok(PipelineBuildResult::create())
    }
}
