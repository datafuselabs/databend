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
use databend_common_meta_app::share::ShowSharesReq;
use databend_common_sharing::ShareEndpointManager;
use databend_common_users::UserApiProvider;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct ShowSharesInterpreter {
    ctx: Arc<QueryContext>,
}

impl ShowSharesInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>) -> Result<Self> {
        Ok(ShowSharesInterpreter { ctx })
    }
}

#[async_trait::async_trait]
impl Interpreter for ShowSharesInterpreter {
    fn name(&self) -> &str {
        "ShowSharesInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let tenant = self.ctx.get_tenant();
        let mut names: Vec<String> = vec![];
        let mut kinds: Vec<String> = vec![];
        let mut created_owns: Vec<String> = vec![];
        let mut database_names: Vec<String> = vec![];
        let mut from: Vec<String> = vec![];
        let mut to: Vec<String> = vec![];
        let mut comments: Vec<String> = vec![];

        // query all share endpoint for other tenant inbound shares
        let share_specs = ShareEndpointManager::instance()
            .get_inbound_shares(tenant.name(), None, None)
            .await?;
        for (from_tenant, share_spec) in share_specs {
            names.push(share_spec.name.clone());
            kinds.push("INBOUND".to_string());
            created_owns.push(share_spec.share_on.unwrap_or_default().to_string());
            database_names.push(share_spec.database.unwrap_or_default().name);
            from.push(from_tenant);
            to.push(tenant.name().to_string());
            comments.push(share_spec.comment.unwrap_or_default());
        }

        let req = ShowSharesReq {
            tenant: tenant.name().to_string(),
        };
        let resp = meta_api.show_shares(req).await?;

        for entry in resp.outbound_accounts {
            names.push(entry.share_name.share_name.clone());
            kinds.push("OUTBOUND".to_string());
            created_owns.push(entry.create_on.to_string());
            database_names.push(entry.database_name.unwrap_or_default());
            from.push(entry.share_name.tenant.name().to_string());
            to.push(
                entry
                    .accounts
                    .map_or("".to_string(), |accounts| accounts.join(",")),
            );
            comments.push(entry.comment.unwrap_or_default());
        }

        PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
            StringType::from_data(created_owns),
            StringType::from_data(kinds),
            StringType::from_data(names),
            StringType::from_data(database_names),
            StringType::from_data(from),
            StringType::from_data(to),
            StringType::from_data(comments),
        ])])
    }
}
