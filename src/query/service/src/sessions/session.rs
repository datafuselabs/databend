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

use std::net::SocketAddr;
use std::sync::Arc;

use databend_common_base::runtime::drop_guard;
use databend_common_catalog::cluster_info::Cluster;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_io::prelude::FormatSettings;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_meta_app::tenant::Tenant;
use databend_common_pipeline_core::PlanProfile;
use databend_common_settings::Settings;
use databend_common_users::GrantObjectVisibilityChecker;
use databend_storages_common_txn::TxnManagerRef;
use log::debug;
use parking_lot::RwLock;

use crate::clusters::ClusterDiscovery;
use crate::sessions::session_privilege_mgr::SessionPrivilegeManager;
use crate::sessions::session_privilege_mgr::SessionPrivilegeManagerImpl;
use crate::sessions::QueryContext;
use crate::sessions::QueryContextShared;
use crate::sessions::SessionContext;
use crate::sessions::SessionManager;
use crate::sessions::SessionStatus;
use crate::sessions::SessionType;

pub struct Session {
    pub(in crate::sessions) id: String,
    pub(in crate::sessions) typ: RwLock<SessionType>,
    pub(in crate::sessions) session_ctx: Box<SessionContext>,
    status: Arc<RwLock<SessionStatus>>,
    pub(in crate::sessions) mysql_connection_id: Option<u32>,
    format_settings: FormatSettings,
}

impl Session {
    pub fn try_create(
        id: String,
        typ: SessionType,
        session_ctx: Box<SessionContext>,
        mysql_connection_id: Option<u32>,
    ) -> Result<Session> {
        let status = Arc::new(Default::default());
        Ok(Session {
            id,
            typ: RwLock::new(typ),
            status,
            session_ctx,
            mysql_connection_id,
            format_settings: FormatSettings::default(),
        })
    }

    pub fn to_fastrace_properties(&self) -> Vec<(String, String)> {
        let mut properties = vec![
            ("session_id".to_string(), self.id.clone()),
            ("session_database".to_string(), self.get_current_database()),
            (
                "session_tenant".to_string(),
                self.get_current_tenant().tenant_name().to_string(),
            ),
        ];
        if let Some(query_id) = self.get_current_query_id() {
            properties.push(("query_id".to_string(), query_id));
        }
        if let Some(connection_id) = self.get_mysql_conn_id() {
            properties.push(("connection_id".to_string(), connection_id.to_string()));
        }
        properties
    }

    pub fn get_mysql_conn_id(&self) -> Option<u32> {
        self.mysql_connection_id
    }

    pub fn get_id(&self) -> String {
        self.id.clone()
    }

    pub fn get_type(&self) -> SessionType {
        let lock = self.typ.read();
        lock.clone()
    }

    pub fn set_type(&self, typ: SessionType) {
        let mut lock = self.typ.write();
        *lock = typ;
    }

    pub fn is_aborting(&self) -> bool {
        self.session_ctx.get_abort()
    }

    pub fn quit(&self) {
        let session_ctx = self.session_ctx.as_ref();

        if session_ctx.get_current_query_id().is_some() {
            if let Some(shutdown_fun) = session_ctx.take_io_shutdown_tx() {
                shutdown_fun();
            }
        }
    }

    pub fn kill(&self) {
        self.session_ctx.set_abort(true);
        self.quit();
    }

    pub fn force_kill_session(&self) {
        self.force_kill_query(ErrorCode::AbortedQuery(
            "Aborted query, because the server is shutting down or the query was killed",
        ));
        self.kill(/* shutdown io stream */);
    }

    pub fn force_kill_query(&self, cause: ErrorCode) {
        if let Some(context_shared) = self.session_ctx.get_query_context_shared() {
            context_shared.kill(cause);
        }
    }

    /// Create a query context for query.
    /// For a query, execution environment(e.g cluster) should be immutable.
    /// We can bind the environment to the context in create_context method.
    #[async_backtrace::framed]
    pub async fn create_query_context(self: &Arc<Self>) -> Result<Arc<QueryContext>> {
        let config = GlobalConfig::instance();
        let cluster = ClusterDiscovery::instance().discover(&config).await?;
        self.create_query_context_with_cluster(cluster)
    }

    pub fn create_query_context_with_cluster(
        self: &Arc<Self>,
        cluster: Arc<Cluster>,
    ) -> Result<Arc<QueryContext>> {
        let session = self.clone();
        let shared = QueryContextShared::try_create(session, cluster)?;

        self.session_ctx
            .set_query_context_shared(Arc::downgrade(&shared));
        Ok(QueryContext::create_from_shared(shared))
    }

    // only used for values and mysql output
    pub fn set_format_settings(&mut self, other: FormatSettings) {
        self.format_settings = other
    }

    pub fn get_format_settings(&self) -> FormatSettings {
        self.format_settings.clone()
    }

    pub fn get_current_query_id(&self) -> Option<String> {
        self.session_ctx.get_current_query_id()
    }

    pub fn attach<F>(&self, host: Option<SocketAddr>, io_shutdown: F)
    where F: FnOnce() + Send + Sync + 'static {
        self.session_ctx
            .set_client_host(host.map(|host| host.ip().to_string()));
        self.session_ctx.set_io_shutdown_tx(io_shutdown);
    }

    pub fn set_client_host(&self, host: Option<String>) {
        self.session_ctx.set_client_host(host);
    }

    pub fn set_current_database(&self, database_name: String) {
        self.session_ctx.set_current_database(database_name);
    }

    pub fn get_current_database(&self) -> String {
        self.session_ctx.get_current_database()
    }

    pub fn get_current_catalog(&self) -> String {
        self.session_ctx.get_current_catalog()
    }

    pub fn get_current_tenant(&self) -> Tenant {
        self.session_ctx.get_current_tenant()
    }

    pub fn set_current_tenant(&mut self, tenant: Tenant) {
        self.session_ctx.set_current_tenant(tenant);
    }

    pub fn get_current_user(&self) -> Result<UserInfo> {
        self.privilege_mgr().get_current_user()
    }

    pub fn privilege_mgr(&self) -> SessionPrivilegeManagerImpl<'_> {
        SessionPrivilegeManagerImpl::new(self.session_ctx.as_ref())
    }

    // set_authed_user() is called after authentication is passed in various protocol handlers, like
    // HTTP handler, clickhouse query handler, mysql query handler. restricted_role represents the role
    // granted by external authenticator, it will overwrite the current user's granted roles, and
    // becomes the CURRENT ROLE if not set X-DATABEND-ROLE.
    #[async_backtrace::framed]
    pub async fn set_authed_user(
        &self,
        user: UserInfo,
        restricted_role: Option<String>,
    ) -> Result<()> {
        self.privilege_mgr()
            .set_authed_user(user, restricted_role)
            .await
    }

    #[async_backtrace::framed]
    pub async fn validate_available_role(&self, role_name: &str) -> Result<RoleInfo> {
        self.privilege_mgr()
            .validate_available_role(role_name)
            .await
    }

    // Only the available role can be set as current role. The current role can be set by the SET
    // ROLE statement, or by the `session.role` field in the HTTP query request body.
    #[async_backtrace::framed]
    pub async fn set_current_role_checked(&self, role_name: &str) -> Result<()> {
        self.privilege_mgr()
            .set_current_role(Some(role_name.to_string()))
            .await
    }

    #[async_backtrace::framed]
    pub async fn set_secondary_roles_checked(&self, role_names: Option<Vec<String>>) -> Result<()> {
        self.privilege_mgr().set_secondary_roles(role_names).await
    }

    pub fn get_current_role(&self) -> Option<RoleInfo> {
        self.privilege_mgr().get_current_role()
    }

    pub fn get_secondary_roles(&self) -> Option<Vec<String>> {
        self.privilege_mgr().get_secondary_roles()
    }

    #[async_backtrace::framed]
    pub async fn unset_current_role(&self) -> Result<()> {
        self.privilege_mgr()
            .set_current_role(Some("public".to_string()))
            .await
    }

    // Returns all the roles the current session has. If the user have been granted restricted_role,
    // the other roles will be ignored.
    // On executing SET ROLE, the role have to be one of the available roles.
    #[async_backtrace::framed]
    pub async fn get_all_available_roles(&self) -> Result<Vec<RoleInfo>> {
        self.privilege_mgr().get_all_available_roles().await
    }

    #[async_backtrace::framed]
    pub async fn get_all_effective_roles(&self) -> Result<Vec<RoleInfo>> {
        self.privilege_mgr().get_all_effective_roles().await
    }

    #[async_backtrace::framed]
    pub async fn validate_privilege(
        &self,
        object: &GrantObject,
        privilege: UserPrivilegeType,
        check_current_role_only: bool,
    ) -> Result<()> {
        if matches!(self.get_type(), SessionType::Local) {
            return Ok(());
        }
        self.privilege_mgr()
            .validate_privilege(object, privilege, check_current_role_only)
            .await
    }

    #[async_backtrace::framed]
    pub async fn has_ownership(
        &self,
        object: &OwnershipObject,
        check_current_role_only: bool,
    ) -> Result<bool> {
        if matches!(self.get_type(), SessionType::Local) {
            return Ok(true);
        }
        self.privilege_mgr()
            .has_ownership(object, check_current_role_only)
            .await
    }

    #[async_backtrace::framed]
    pub async fn get_visibility_checker(&self) -> Result<GrantObjectVisibilityChecker> {
        self.privilege_mgr().get_visibility_checker().await
    }

    pub fn get_settings(&self) -> Arc<Settings> {
        self.session_ctx.get_settings()
    }

    pub fn get_memory_usage(&self) -> usize {
        // TODO(winter): use thread memory tracker
        0
    }

    pub fn get_status(&self) -> Arc<RwLock<SessionStatus>> {
        self.status.clone()
    }

    pub fn get_query_result_cache_key(&self, query_id: &str) -> Option<String> {
        self.session_ctx.get_query_result_cache_key(query_id)
    }

    pub fn update_query_ids_results(&self, query_id: String, result_cache_key: String) {
        self.session_ctx
            .update_query_ids_results(query_id, Some(result_cache_key))
    }

    pub fn txn_mgr(&self) -> TxnManagerRef {
        self.session_ctx.txn_mgr()
    }
    pub fn set_txn_mgr(&self, txn_mgr: TxnManagerRef) {
        self.session_ctx.set_txn_mgr(txn_mgr)
    }

    pub fn set_query_priority(&self, priority: u8) {
        if let Some(context_shared) = self.session_ctx.get_query_context_shared() {
            context_shared.set_priority(priority);
        }
    }

    pub fn get_query_profiles(&self) -> Vec<PlanProfile> {
        match self.session_ctx.get_query_context_shared() {
            None => vec![],
            Some(x) => x.get_query_profiles(),
        }
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        drop_guard(move || {
            debug!("Drop session {}", self.id.clone());
            SessionManager::instance().destroy_session(&self.id.clone());
        })
    }
}
