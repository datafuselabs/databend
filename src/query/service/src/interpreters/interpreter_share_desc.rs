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
use databend_common_expression::types::StringType;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_meta_api::ShareApi;
use databend_common_meta_app::share::GetShareGrantObjectReq;
use databend_common_meta_app::share::ShareGrantObjectName;
use databend_common_meta_app::share::ShareNameIdent;
use databend_common_users::UserApiProvider;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::share::DescSharePlan;

pub struct DescShareInterpreter {
    ctx: Arc<QueryContext>,
    plan: DescSharePlan,
}

impl DescShareInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DescSharePlan) -> Result<Self> {
        Ok(DescShareInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DescShareInterpreter {
    fn name(&self) -> &str {
        "DescShareInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let req = GetShareGrantObjectReq {
            share_name: ShareNameIdent {
                tenant: self.ctx.get_tenant(),
                share_name: self.plan.share.clone(),
            },
        };
        let resp = meta_api.get_share_grant_objects(req).await?;
        if resp.objects.is_empty() {
            return Ok(PipelineBuildResult::create());
        }

        let mut names: Vec<String> = vec![];
        let mut kinds: Vec<String> = vec![];
        let mut shared_owns: Vec<String> = vec![];
        for entry in resp.objects.iter() {
            match &entry.object {
                ShareGrantObjectName::Database(db) => {
                    kinds.push("DATABASE".to_string());
                    names.push(db.clone());
                }
                ShareGrantObjectName::Table(db, table_name) => {
                    kinds.push("TABLE".to_string());
                    names.push(format!("{}.{}", db, table_name));
                }
            }
            shared_owns.push(entry.grant_on.to_string());
        }

        PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
            StringType::from_data(kinds),
            StringType::from_data(names),
            StringType::from_data(shared_owns),
        ])])
    }
}
