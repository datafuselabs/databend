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

use databend_common_exception::Result;
use databend_common_meta_app::principal::user_token::QueryTokenInfo;
use databend_common_meta_app::tenant::Tenant;

use crate::UserApiProvider;

impl UserApiProvider {
    #[async_backtrace::framed]
    pub async fn upsert_token(
        &self,
        tenant: &Tenant,
        token_hash: &str,
        token_info: QueryTokenInfo,
        ttl_in_secs: u64,
        update_only: bool,
    ) -> Result<bool> {
        let token_api_provider = self.token_api(tenant);
        token_api_provider
            .upsert_token(token_hash, token_info, ttl_in_secs, update_only)
            .await
    }

    #[async_backtrace::framed]
    pub async fn get_token(
        &self,
        tenant: &Tenant,
        token_hash: &str,
    ) -> Result<Option<QueryTokenInfo>> {
        let token_api_provider = self.token_api(tenant);
        token_api_provider.get_token(token_hash).await
    }

    #[async_backtrace::framed]
    pub async fn drop_token(&self, tenant: &Tenant, token_hash: &str) -> Result<()> {
        let token_api_provider = self.token_api(tenant);
        token_api_provider.drop_token(token_hash).await
    }
}
