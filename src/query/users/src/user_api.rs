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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_common_exception::Result;
use databend_common_grpc::RpcClientConf;
use databend_common_management::udf::UdfMgr;
use databend_common_management::ConnectionMgr;
use databend_common_management::FileFormatMgr;
use databend_common_management::NetworkPolicyMgr;
use databend_common_management::PasswordPolicyMgr;
use databend_common_management::QuotaApi;
use databend_common_management::QuotaMgr;
use databend_common_management::RoleApi;
use databend_common_management::RoleMgr;
use databend_common_management::SettingApi;
use databend_common_management::SettingMgr;
use databend_common_management::StageApi;
use databend_common_management::StageMgr;
use databend_common_management::TokenMgr;
use databend_common_management::UserApi;
use databend_common_management::UserMgr;
use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::UserDefinedFunction;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::tenant::TenantQuota;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_store::MetaStore;
use databend_common_meta_store::MetaStoreProvider;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MetaError;

use crate::builtin::BuiltIn;
use crate::BUILTIN_ROLE_PUBLIC;

pub struct UserApiProvider {
    meta: MetaStore,
    client: Arc<dyn kvapi::KVApi<Error = MetaError> + Send + Sync>,
    builtin: BuiltIn,
}

impl UserApiProvider {
    #[async_backtrace::framed]
    pub async fn init(
        conf: RpcClientConf,
        builtin: BuiltIn,
        tenant: &Tenant,
        quota: Option<TenantQuota>,
    ) -> Result<()> {
        GlobalInstance::set(Self::try_create(conf, builtin, tenant).await?);
        let user_mgr = UserApiProvider::instance();
        if let Some(q) = quota {
            let i = user_mgr.tenant_quota_api(tenant);
            let res = i.get_quota(MatchSeq::GE(0)).await?;
            i.set_quota(&q, MatchSeq::Exact(res.seq)).await?;
        }
        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn try_create(
        conf: RpcClientConf,
        builtin: BuiltIn,
        tenant: &Tenant,
    ) -> Result<Arc<UserApiProvider>> {
        let client = MetaStoreProvider::new(conf).create_meta_store().await?;
        let user_mgr = UserApiProvider {
            meta: client.clone(),
            client: client.arc(),
            builtin,
        };

        // init built-in role
        // Currently we have two builtin roles:
        // 1. ACCOUNT_ADMIN, which has the equivalent privileges of `GRANT ALL ON *.* TO ROLE account_admin`,
        //    it also contains all roles. ACCOUNT_ADMIN can access the data objects which owned by any role.
        // 2. PUBLIC, on the other side only includes the public accessible privileges, but every role
        //    contains the PUBLIC role. The data objects which owned by PUBLIC can be accessed by any role.
        // But we only can set PUBLIC role into meta.
        // Because the previous deserialization using from_bits caused forward compatibility issues after adding new privilege type
        // Until we can confirm all product use https://github.com/datafuselabs/databend/releases/tag/v1.2.321-nightly or later,
        // We can add account_admin into meta.
        {
            let public = RoleInfo::new(BUILTIN_ROLE_PUBLIC);
            user_mgr.add_role(tenant, public, true).await?;
        }

        Ok(Arc::new(user_mgr))
    }

    #[async_backtrace::framed]
    pub async fn try_create_simple(
        conf: RpcClientConf,
        tenant: &Tenant,
    ) -> Result<Arc<UserApiProvider>> {
        Self::try_create(conf, BuiltIn::default(), tenant).await
    }

    pub fn instance() -> Arc<UserApiProvider> {
        GlobalInstance::get()
    }

    pub fn udf_api(&self, tenant: &Tenant) -> UdfMgr {
        UdfMgr::create(self.client.clone(), tenant)
    }

    pub fn user_api(&self, tenant: &Tenant) -> Arc<impl UserApi> {
        let user_mgr = UserMgr::create(self.client.clone(), tenant);
        Arc::new(user_mgr)
    }

    pub fn role_api(&self, tenant: &Tenant) -> Arc<impl RoleApi> {
        let role_mgr = RoleMgr::create(self.client.clone(), tenant);
        Arc::new(role_mgr)
    }

    pub fn stage_api(&self, tenant: &Tenant) -> Arc<dyn StageApi> {
        Arc::new(StageMgr::create(self.client.clone(), tenant))
    }

    pub fn file_format_api(&self, tenant: &Tenant) -> FileFormatMgr {
        FileFormatMgr::create(self.client.clone(), tenant)
    }

    pub fn connection_api(&self, tenant: &Tenant) -> ConnectionMgr {
        ConnectionMgr::create(self.client.clone(), tenant)
    }

    pub fn tenant_quota_api(&self, tenant: &Tenant) -> Arc<dyn QuotaApi> {
        const WRITE_PB: bool = false;
        Arc::new(QuotaMgr::<WRITE_PB>::create(self.client.clone(), tenant))
    }

    pub fn setting_api(&self, tenant: &Tenant) -> Arc<dyn SettingApi> {
        Arc::new(SettingMgr::create(self.client.clone(), tenant))
    }

    pub fn network_policy_api(&self, tenant: &Tenant) -> NetworkPolicyMgr {
        NetworkPolicyMgr::create(self.client.clone(), tenant)
    }

    pub fn password_policy_api(&self, tenant: &Tenant) -> PasswordPolicyMgr {
        PasswordPolicyMgr::create(self.client.clone(), tenant)
    }

    pub fn token_api(&self, tenant: &Tenant) -> TokenMgr {
        TokenMgr::create(self.client.clone(), tenant)
    }

    pub fn get_meta_store_client(&self) -> Arc<MetaStore> {
        Arc::new(self.meta.clone())
    }

    pub fn get_configured_user(&self, user_name: &str) -> Option<&AuthInfo> {
        self.builtin.users.get(user_name)
    }

    pub fn get_configured_users(&self) -> HashMap<String, AuthInfo> {
        self.builtin.users.clone()
    }

    pub fn get_configured_udf(&self, udf_name: &str) -> Option<UserDefinedFunction> {
        self.builtin.udfs.get(udf_name).cloned()
    }

    pub fn get_configured_udfs(&self) -> HashMap<String, UserDefinedFunction> {
        self.builtin.udfs.clone()
    }
}
