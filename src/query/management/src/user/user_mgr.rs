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
use databend_common_meta_app::principal::TenantUserIdent;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::Key;
use databend_common_meta_kvapi::kvapi::ListKVReply;
use databend_common_meta_kvapi::kvapi::UpsertKVReq;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MatchSeqExt;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::Operation;
use databend_common_meta_types::SeqV;

use crate::serde::deserialize_struct;
use crate::serde::serialize_struct;
use crate::user::user_api::UserApi;

pub struct UserMgr {
    kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>,
    tenant: Tenant,
}

impl UserMgr {
    pub fn create(kv_api: Arc<dyn kvapi::KVApi<Error = MetaError>>, tenant: &Tenant) -> Self {
        UserMgr {
            kv_api,
            tenant: tenant.clone(),
        }
    }

    /// Create a string key to store a user@host into meta-service.
    fn user_key(&self, username: &str) -> String {
        let ident = TenantUserIdent::new_user_host(self.tenant.clone(), username);
        ident.to_string_key()
    }

    /// Create a prefix `<PREFIX>/<tenant>/` for listing
    fn user_prefix(&self) -> String {
        let ident = TenantUserIdent::new_user_host(self.tenant.clone(), "dummy");
        ident.tenant_prefix()
    }

    #[async_backtrace::framed]
    async fn upsert_user_info(
        &self,
        user_info: &UserInfo,
        seq: MatchSeq,
    ) -> databend_common_exception::Result<u64> {
        let key = self.user_key(&user_info.name);

        let value = serialize_struct(user_info, ErrorCode::IllegalUserInfoFormat, || "")?;

        let kv_api = self.kv_api.clone();
        let res = kv_api
            .upsert_kv(UpsertKVReq::new(&key, seq, Operation::Update(value), None))
            .await?;

        match res.result {
            Some(SeqV { seq: s, .. }) => Ok(s),
            None => Err(ErrorCode::UnknownUser(format!(
                "User '{}' update failed: User does not exist or invalid request.",
                user_info.name
            ))),
        }
    }
}

#[async_trait::async_trait]
impl UserApi for UserMgr {
    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn add_user(
        &self,
        user_info: UserInfo,
        create_option: &CreateOption,
    ) -> databend_common_exception::Result<()> {
        let key = self.user_key(&user_info.name);
        let value = serialize_struct(&user_info, ErrorCode::IllegalUserInfoFormat, || "")?;

        let kv_api = &self.kv_api;
        let seq = MatchSeq::from(*create_option);
        let res = kv_api
            .upsert_kv(UpsertKVReq::new(&key, seq, Operation::Update(value), None))
            .await?;

        if let CreateOption::Create = create_option {
            if res.prev.is_some() {
                return Err(ErrorCode::UserAlreadyExists(format!(
                    "User {} already exists.",
                    user_info.identity().identity()
                )));
            }
        }

        Ok(())
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn get_user(&self, user: UserIdentity, seq: MatchSeq) -> Result<SeqV<UserInfo>> {
        let key = self.user_key(&user.username);

        let res = self.kv_api.get_kv(&key).await?;
        let seq_value = res.ok_or_else(|| {
            ErrorCode::UnknownUser(format!("User {} does not exist.", user.identity()))
        })?;

        match seq.match_seq(&seq_value) {
            Ok(_) => Ok(SeqV::new(
                seq_value.seq,
                deserialize_struct(&seq_value.data, ErrorCode::IllegalUserInfoFormat, || "")?,
            )),
            Err(_) => Err(ErrorCode::UnknownUser(format!(
                "User {} does not exist.",
                user.identity()
            ))),
        }
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn get_users(&self) -> Result<Vec<SeqV<UserInfo>>> {
        let values = self.get_raw_users().await?;
        let mut r = vec![];
        for (_key, val) in values {
            let u = deserialize_struct(&val.data, ErrorCode::IllegalUserInfoFormat, || "")?;
            r.push(SeqV::new(val.seq, u));
        }

        Ok(r)
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn get_raw_users(&self) -> Result<ListKVReply> {
        let user_prefix = self.user_prefix();
        Ok(self.kv_api.prefix_list_kv(user_prefix.as_str()).await?)
    }

    #[async_backtrace::framed]
    async fn update_user_with<F>(
        &self,
        user: UserIdentity,
        seq: MatchSeq,
        f: F,
    ) -> Result<Option<u64>>
    where
        F: FnOnce(&mut UserInfo) + Send,
    {
        let SeqV {
            seq,
            data: mut user_info,
            ..
        } = self.get_user(user, seq).await?;

        f(&mut user_info);

        let seq = self
            .upsert_user_info(&user_info, MatchSeq::Exact(seq))
            .await?;
        Ok(Some(seq))
    }

    #[async_backtrace::framed]
    async fn drop_user(&self, user: UserIdentity, seq: MatchSeq) -> Result<()> {
        let key = self.user_key(&user.username);
        let res = self
            .kv_api
            .upsert_kv(UpsertKVReq::new(&key, seq, Operation::Delete, None))
            .await?;
        if res.prev.is_some() && res.result.is_none() {
            Ok(())
        } else {
            Err(ErrorCode::UnknownUser(format!(
                "Cannot delete user {}. User does not exist or invalid operation.",
                user.identity()
            )))
        }
    }
}
