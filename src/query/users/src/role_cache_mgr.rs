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
use std::time::Duration;
use std::time::Instant;

use common_base::base::tokio;
use common_base::base::tokio::task::JoinHandle;
use common_base::base::GlobalInstance;
use common_base::runtime::Runtime;
use common_base::GLOBAL_TASK;
use common_exception::Result;
use common_meta_app::principal::GrantObjectByID;
use common_meta_app::principal::RoleInfo;
use log::warn;
use parking_lot::RwLock;

use crate::role_util::find_all_related_roles;
use crate::UserApiProvider;

struct CachedRoles {
    roles: HashMap<String, RoleInfo>,
    cached_at: Instant,
}

pub struct RoleCacheManager {
    user_manager: Arc<UserApiProvider>,
    cache: Arc<RwLock<HashMap<String, CachedRoles>>>,
    polling_interval: Duration,
    polling_join_handle: Option<JoinHandle<()>>,
}

impl RoleCacheManager {
    pub fn init() -> Result<()> {
        // Check that the user API has been initialized.
        let instance = UserApiProvider::instance();

        GlobalInstance::set(Self::try_create(instance)?);
        Ok(())
    }

    pub fn try_create(user_manager: Arc<UserApiProvider>) -> Result<Arc<RoleCacheManager>> {
        let mut role_cache_manager = Self {
            user_manager,
            polling_join_handle: None,
            cache: Arc::new(RwLock::new(HashMap::new())),
            polling_interval: Duration::new(15, 0),
        };

        role_cache_manager.background_polling();
        Ok(Arc::new(role_cache_manager))
    }

    pub fn instance() -> Arc<RoleCacheManager> {
        GlobalInstance::get()
    }

    pub fn background_polling(&mut self) {
        let cache = self.cache.clone();
        let polling_interval = self.polling_interval;
        let user_manager = self.user_manager.clone();
        self.polling_join_handle = Some(Runtime::spawn_current_runtime(GLOBAL_TASK, async move {
            loop {
                let tenants: Vec<String> = {
                    let cached = cache.read();
                    cached.keys().cloned().collect()
                };
                for tenant in tenants {
                    match load_roles_data(&user_manager, &tenant).await {
                        Err(err) => {
                            warn!(
                                "role_cache_mgr load roles data of tenant {} failed: {}",
                                tenant, err,
                            )
                        }
                        Ok(data) => {
                            let mut cached = cache.write();
                            cached.insert(tenant.to_string(), data);
                        }
                    }
                }
                tokio::time::sleep(polling_interval).await
            }
        }));
    }

    pub fn invalidate_cache(&self, tenant: &str) {
        let mut cached = self.cache.write();
        cached.remove(tenant);
    }

    #[async_backtrace::framed]
    pub async fn find_role(&self, tenant: &str, role: &str) -> Result<Option<RoleInfo>> {
        let cached = self.cache.read();
        let cached_roles = match cached.get(tenant) {
            None => return Ok(None),
            Some(cached_roles) => cached_roles,
        };
        Ok(cached_roles.roles.get(role).cloned())
    }

    // TODO(liyz): really cache it if got any performance issue. IMHO the ownership data won't become a big memory.
    #[async_backtrace::framed]
    pub async fn find_object_owner(
        &self,
        tenant: &str,
        object: &GrantObjectByID,
    ) -> Result<Option<RoleInfo>> {
        let owner = match self.user_manager.get_ownership(tenant, object).await? {
            None => return Ok(None),
            Some(owner) => owner,
        };

        // cache manager would not look into built-in roles.
        let role = self.user_manager.get_role(tenant, owner.role).await?;
        Ok(Some(role))
    }

    // find_related_roles is called on validating an user's privileges.
    #[async_backtrace::framed]
    pub async fn find_related_roles(
        &self,
        tenant: &str,
        roles: &[String],
    ) -> Result<Vec<RoleInfo>> {
        self.maybe_reload(tenant).await?;
        let cached = self.cache.read();
        let cached_roles = match cached.get(tenant) {
            None => return Ok(vec![]),
            Some(cached_roles) => cached_roles,
        };
        Ok(find_all_related_roles(&cached_roles.roles, roles))
    }

    #[async_backtrace::framed]
    pub async fn force_reload(&self, tenant: &str) -> Result<()> {
        let data = load_roles_data(&self.user_manager, tenant).await?;
        let mut cached = self.cache.write();
        cached.insert(tenant.to_string(), data);
        Ok(())
    }

    // Load roles data if not found in cache. Watch this tenant's role data in background if
    // once it loads successfully.
    #[async_backtrace::framed]
    async fn maybe_reload(&self, tenant: &str) -> Result<()> {
        let need_reload = {
            let cached = self.cache.read();
            match cached.get(tenant) {
                None => true,
                Some(cached_roles) => {
                    // force reload the data when:
                    // - if the cache is too old (the background polling task
                    //   may got some network errors, leaves the cache outdated)
                    // - if the cache is empty
                    cached_roles.cached_at.elapsed() >= self.polling_interval * 2
                        || cached_roles.roles.is_empty()
                }
            }
        };
        if need_reload {
            self.force_reload(tenant).await?;
        }
        Ok(())
    }
}

async fn load_roles_data(user_api: &Arc<UserApiProvider>, tenant: &str) -> Result<CachedRoles> {
    let roles = user_api.get_roles(tenant).await?;
    let roles_map = roles
        .into_iter()
        .map(|r| (r.identity().to_string(), r))
        .collect::<HashMap<_, _>>();
    Ok(CachedRoles {
        roles: roles_map,
        cached_at: Instant::now(),
    })
}
