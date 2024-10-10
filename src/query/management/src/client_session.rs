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
use std::time::Duration;

use databend_common_exception::Result;
use databend_common_meta_api::kv_pb_api::KVPbApi;
use databend_common_meta_api::kv_pb_api::UpsertPB;
use databend_common_meta_app::principal::client_session::ClientSession;
use databend_common_meta_app::principal::client_session_ident::ClientSessionIdent;
use databend_common_meta_app::principal::client_session_ident::UserSessionId;
use databend_common_meta_app::principal::user_token::QueryTokenInfo;
use databend_common_meta_app::principal::user_token_ident::TokenIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::Key;
use databend_common_meta_kvapi::kvapi::UpsertKVReq;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::Operation;
use databend_common_meta_types::With;

pub struct ClientSessionMgr {
    kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>,
    tenant: Tenant,
}

impl ClientSessionMgr {
    pub fn create(kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>, tenant: &Tenant) -> Self {
        ClientSessionMgr {
            kv_api,
            tenant: tenant.clone(),
        }
    }

    fn token_ident(&self, token_hash: &str) -> TokenIdent {
        TokenIdent::new(self.tenant.clone(), token_hash)
    }
    fn session_ident(&self, session_id: &str, user_name: &str) -> ClientSessionIdent {
        let id = UserSessionId {
            session_id: session_id.to_string(),
            user_name: user_name.to_string(),
        };
        ClientSessionIdent::new_generic(self.tenant.clone(), id)
    }
}

impl ClientSessionMgr {
    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn upsert_token(
        &self,
        token_hash: &str,
        token_info: QueryTokenInfo,
        ttl: Duration,
        update_only: bool,
    ) -> Result<bool> {
        let ident = self.token_ident(token_hash);
        let seq = MatchSeq::GE(if update_only { 1 } else { 0 });
        let upsert = UpsertPB::update(ident, token_info)
            .with(seq)
            .with_ttl(Duration::from_secs(ttl.as_secs()));

        let res = self.kv_api.upsert_pb(&upsert).await?;

        Ok(res.prev.is_none())
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn get_token(&self, token_hash: &str) -> Result<Option<QueryTokenInfo>> {
        let ident = self.token_ident(token_hash);
        let res = self.kv_api.get_pb(&ident).await?;

        Ok(res.map(|r| r.data))
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn drop_token(&self, token_hash: &str) -> Result<()> {
        let key = self.token_ident(token_hash).to_string_key();

        // simply ignore the result
        self.kv_api
            .upsert_kv(UpsertKVReq::new(
                &key,
                MatchSeq::GE(0),
                Operation::Delete,
                None,
            ))
            .await?;

        Ok(())
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn upsert_client_session_id(
        &self,
        client_session_id: &str,
        user_name: &str,
        ttl: Duration,
    ) -> Result<bool> {
        let ident = self.session_ident(client_session_id, user_name);
        let seq = MatchSeq::GE(0);
        let upsert = UpsertPB::update(ident, ClientSession {})
            .with(seq)
            .with_ttl(ttl);

        let res = self.kv_api.upsert_pb(&upsert).await?;

        Ok(res.prev.is_none())
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn get_client_session(
        &self,
        client_session_id: &str,
        user_name: &str,
    ) -> Result<Option<ClientSession>> {
        let ident = self.session_ident(client_session_id, user_name);
        let res = self.kv_api.get_pb(&ident).await?;

        Ok(res.map(|r| r.data))
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn drop_client_session_id(
        &self,
        client_session_id: &str,
        user_name: &str,
    ) -> Result<()> {
        let key = self
            .session_ident(client_session_id, user_name)
            .to_string_key();

        // simply ignore the result
        self.kv_api
            .upsert_kv(UpsertKVReq::new(
                &key,
                MatchSeq::GE(0),
                Operation::Delete,
                None,
            ))
            .await?;

        Ok(())
    }
}
