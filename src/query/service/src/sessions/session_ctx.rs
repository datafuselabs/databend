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
use std::collections::HashSet;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Weak;

use databend_common_config::GlobalConfig;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::tenant::Tenant;
use databend_common_settings::Settings;
use databend_storages_common_txn::TxnManager;
use databend_storages_common_txn::TxnManagerRef;
use parking_lot::Mutex;
use parking_lot::RwLock;

use super::SessionType;
use crate::sessions::QueryContextShared;

pub struct SessionContext {
    abort: AtomicBool,
    settings: Arc<Settings>,
    current_catalog: RwLock<String>,
    current_database: RwLock<String>,

    /// The current tenant can be determined by databend-query's config file, or by X-DATABEND-TENANT
    /// if it's in management mode.
    /// If databend-query is not in management mode, the current tenant can **NOT** be modified at runtime.
    current_tenant: Option<Tenant>,

    /// The current user is determined by the authentication phase on each connection. It will not be
    /// changed during a session.
    current_user: RwLock<Option<UserInfo>>,
    /// Each session has a current role, by default all the users' granted roles will take effect,
    /// and the current role will become the owner of the database/table that the user created.
    /// The user can switch to another available role by `SET ROLE`. If the current_role is not set,
    /// it takes the user's default role.
    current_role: RwLock<Option<RoleInfo>>,
    /// When a user comes from an external authenticator, the session is usually mapped to a single role.
    auth_role: RwLock<Option<String>>,
    /// To SET SECONDARY ROLES ALL, the session will have all the roles take effect. On the other hand,
    /// SET SECONDARY ROLES NONE will disable all the roles except the current role.
    /// By default, the SECONDARY ROLES is ALL, which is None here. There are a few cases that the SECONDARY
    /// ROLES is preferred to be empty, which is Some([]) here:
    /// 1. The user comes from an external authenticator, which maps to a single role.
    /// 2. The role is intentionally restricted by the sql client, to run SQLs with a restricted privileges.
    secondary_roles: RwLock<Option<Vec<String>>>,
    /// The client IP from the client.
    client_host: RwLock<Option<String>>,
    io_shutdown_tx: RwLock<Option<Box<dyn FnOnce() + Send + Sync + 'static>>>,
    query_context_shared: RwLock<Weak<QueryContextShared>>,
    /// We store `query_id -> query_result_cache_key` to session context, so that we can fetch
    /// query result through previous query_id easily.
    query_ids_results: RwLock<Vec<(String, Option<String>)>>,
    // Used in set variables inside session
    variables: Arc<RwLock<HashMap<String, Scalar>>>,
    typ: SessionType,
    txn_mgr: Mutex<TxnManagerRef>,
}

impl SessionContext {
    pub fn try_create(settings: Arc<Settings>, typ: SessionType) -> Result<Self> {
        Ok(SessionContext {
            settings,
            abort: Default::default(),
            current_user: Default::default(),
            current_role: Default::default(),
            auth_role: Default::default(),
            secondary_roles: Default::default(),
            current_tenant: Default::default(),
            client_host: Default::default(),
            current_catalog: RwLock::new("default".to_string()),
            current_database: RwLock::new("default".to_string()),
            io_shutdown_tx: Default::default(),
            query_context_shared: Default::default(),
            query_ids_results: Default::default(),
            variables: Default::default(),
            typ,
            txn_mgr: Mutex::new(TxnManager::init()),
        })
    }

    // Get abort status.
    pub fn get_abort(&self) -> bool {
        self.abort.load(Ordering::Relaxed)
    }

    // Set abort status.
    pub fn set_abort(&self, v: bool) {
        self.abort.store(v, Ordering::Relaxed);
    }

    pub fn get_settings(&self) -> Arc<Settings> {
        self.settings.clone()
    }

    // Get current catalog name.
    pub fn get_current_catalog(&self) -> String {
        let lock = self.current_catalog.read();
        lock.clone()
    }

    // Set current catalog.
    pub fn set_current_catalog(&self, catalog_name: String) {
        let mut lock = self.current_catalog.write();
        *lock = catalog_name
    }

    // Get current database.
    pub fn get_current_database(&self) -> String {
        let lock = self.current_database.read();
        lock.clone()
    }

    // Set current database.
    pub fn set_current_database(&self, db: String) {
        let mut lock = self.current_database.write();
        *lock = db
    }

    // Return the current role if it's set. If the current role is not set, it'll take the user's
    // default role.
    pub fn get_current_role(&self) -> Option<RoleInfo> {
        let lock = self.current_role.read();
        lock.clone()
    }

    pub fn set_current_role(&self, role: Option<RoleInfo>) {
        let mut lock = self.current_role.write();
        *lock = role
    }

    pub fn get_auth_role(&self) -> Option<String> {
        let lock = self.auth_role.read();
        lock.clone()
    }

    pub fn set_auth_role(&self, role: Option<String>) {
        let mut lock = self.auth_role.write();
        *lock = role
    }

    pub fn get_current_tenant(&self) -> Tenant {
        let conf = GlobalConfig::instance();

        if conf.query.internal_enable_sandbox_tenant {
            let sandbox_tenant = self.settings.get_sandbox_tenant().unwrap_or_default();
            if !sandbox_tenant.is_empty() {
                return Tenant::new_or_err(sandbox_tenant, "create from sandbox_tenant").unwrap();
            }
        }

        if conf.query.management_mode || self.typ == SessionType::Local {
            if let Some(tenant) = &self.current_tenant {
                return tenant.clone();
            }
        }

        conf.query.tenant_id.clone()
    }

    pub(in crate::sessions) fn set_current_tenant(&mut self, tenant: Tenant) {
        self.current_tenant = Some(tenant);
    }

    // Get current user
    pub fn get_current_user(&self) -> Option<UserInfo> {
        let lock = self.current_user.read();
        lock.clone()
    }

    // Set the current user after authentication
    pub fn set_current_user(&self, user: UserInfo) {
        let mut lock = self.current_user.write();
        *lock = Some(user);
    }

    // Get restricted role. Restricted role is the role granted by authenticator, or set
    // by sql client to restrict its privilege.
    pub fn get_secondary_roles(&self) -> Option<Vec<String>> {
        let lock = self.secondary_roles.read();
        lock.clone()
    }

    pub fn set_secondary_roles(&self, secondary_roles: Option<Vec<String>>) {
        let mut lock = self.secondary_roles.write();
        *lock = secondary_roles;
    }

    pub fn get_client_host(&self) -> Option<String> {
        let lock = self.client_host.read();
        lock.clone()
    }

    pub fn set_client_host(&self, sock: Option<String>) {
        let mut lock = self.client_host.write();
        *lock = sock
    }

    pub fn set_io_shutdown_tx<F: FnOnce() + Send + Sync + 'static>(&self, f: F) {
        let mut lock = self.io_shutdown_tx.write();

        if let Some(old_fun) = lock.take() {
            *lock = Some(Box::new(move || {
                old_fun();
                f();
            }));

            return;
        }
        *lock = Some(Box::new(f));
    }

    //  Take the io_shutdown_tx and the self.io_shutdown_tx is None.
    pub fn take_io_shutdown_tx(&self) -> Option<Box<dyn FnOnce() + Send + Sync + 'static>> {
        let mut lock = self.io_shutdown_tx.write();
        lock.take()
    }

    pub fn get_current_query_id(&self) -> Option<String> {
        self.query_context_shared
            .read()
            .upgrade()
            .map(|shared| shared.init_query_id.read().clone())
    }

    pub fn get_query_context_shared(&self) -> Option<Arc<QueryContextShared>> {
        let lock = self.query_context_shared.read();
        lock.upgrade()
    }

    pub fn set_query_context_shared(&self, ctx: Weak<QueryContextShared>) {
        let mut lock = self.query_context_shared.write();
        *lock = ctx
    }

    pub fn get_query_result_cache_key(&self, query_id: &str) -> Option<String> {
        let lock = self.query_ids_results.read();
        for (qid, result_cache_key) in (*lock).iter().rev() {
            if qid.eq_ignore_ascii_case(query_id) {
                return result_cache_key.clone();
            }
        }
        None
    }

    pub fn update_query_ids_results(&self, query_id: String, value: Option<String>) {
        let mut lock = self.query_ids_results.write();
        // Here we use reverse iteration, as it is not common to modify elements from earlier.
        for (idx, (qid, _)) in (*lock).iter().rev().enumerate() {
            if qid.eq_ignore_ascii_case(&query_id) {
                // update value iff value is some.
                if let Some(v) = value {
                    (*lock)[idx] = (query_id, Some(v))
                }
                return;
            }
        }
        lock.push((query_id, value))
    }

    pub fn get_last_query_id(&self, index: i32) -> String {
        let lock = self.query_ids_results.read();
        let query_ids_len = lock.len();
        let idx = if index < 0 {
            query_ids_len as i32 + index
        } else {
            index
        };

        if idx < 0 || idx > (query_ids_len - 1) as i32 {
            return "".to_string();
        }

        (*lock)[idx as usize].0.clone()
    }

    pub fn get_query_id_history(&self) -> HashSet<String> {
        let lock = self.query_ids_results.read();
        HashSet::from_iter(lock.iter().map(|result| result.clone().0))
    }

    pub fn txn_mgr(&self) -> TxnManagerRef {
        self.txn_mgr.lock().clone()
    }

    pub fn set_txn_mgr(&self, txn_mgr: TxnManagerRef) {
        *self.txn_mgr.lock() = txn_mgr;
    }

    pub fn set_variable(&self, key: String, value: Scalar) {
        self.variables.write().insert(key, value);
    }

    pub fn unset_variable(&self, key: &str) {
        self.variables.write().remove(key);
    }

    pub fn get_variable(&self, key: &str) -> Option<Scalar> {
        self.variables.read().get(key).cloned()
    }
}
