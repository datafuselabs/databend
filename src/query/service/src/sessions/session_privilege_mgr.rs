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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_management::RoleApi;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_users::GrantObjectVisibilityChecker;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;
use databend_common_users::BUILTIN_ROLE_ACCOUNT_ADMIN;
use databend_common_users::BUILTIN_ROLE_PUBLIC;

use crate::sessions::SessionContext;

/// SessionPrivilegeManager handles all the things related to privieges in a session. On validating a privilege,
/// we have the following requirements:
///
/// - An user can have multiple privileges & roles assigned. Each privilege is related to a grant object,
///   which might be a database, table, stage, task, warehouse, etc.
/// - An role is a collection of the privileges, and each role can have multiple roles granted, which forms
///   an role hierarchy. The higher level role has all the privileges inherited from the lower level roles.
/// - There're two special roles in the role role hierarchy: PUBLIC and ACCOUNT_ADMIN. PUBLIC is by default
///   granted to every role, and ACCOUNT_ADMIN is by default have all the roles granted.
/// - Each grant object has an owner, which is a role. The owner role has all the privileges on the grant
///   object, and can grant the privileges to other roles. Each session have a CURRENT ROLE, in this session,
///   all the newly created objects (databases/tables) will have the CURRENT ROLE as the owner.
#[async_trait::async_trait]
pub trait SessionPrivilegeManager {
    fn get_current_user(&self) -> Result<UserInfo>;

    fn get_current_role(&self) -> Option<RoleInfo>;

    fn get_auth_role(&self) -> Option<String>;

    fn get_secondary_roles(&self) -> Option<Vec<String>>;

    async fn set_authed_user(&self, user: UserInfo, auth_role: Option<String>) -> Result<()>;

    async fn set_current_role(&self, role: Option<String>) -> Result<()>;

    async fn set_secondary_roles(&self, secondary_roles: Option<Vec<String>>) -> Result<()>;

    async fn get_all_effective_roles(&self) -> Result<Vec<RoleInfo>>;

    async fn get_all_available_roles(&self) -> Result<Vec<RoleInfo>>;

    async fn validate_privilege(
        &self,
        object: &GrantObject,
        privilege: UserPrivilegeType,
        check_current_role_only: bool,
    ) -> Result<()>;

    async fn has_ownership(
        &self,
        object: &OwnershipObject,
        check_current_role_only: bool,
    ) -> Result<bool>;

    async fn validate_available_role(&self, role_name: &str) -> Result<RoleInfo>;

    async fn get_visibility_checker(&self) -> Result<GrantObjectVisibilityChecker>;

    // fn show_grants(&self);
}

pub struct SessionPrivilegeManagerImpl<'a> {
    session_ctx: &'a SessionContext,
}

impl<'a> SessionPrivilegeManagerImpl<'a> {
    pub fn new(session_ctx: &'a SessionContext) -> Self {
        Self { session_ctx }
    }

    #[async_backtrace::framed]
    async fn ensure_current_role(&self) -> Result<()> {
        let tenant = self.session_ctx.get_current_tenant();
        let public_role = RoleCacheManager::instance()
            .find_role(&tenant, BUILTIN_ROLE_PUBLIC)
            .await?
            .unwrap_or_else(|| RoleInfo::new(BUILTIN_ROLE_PUBLIC));

        // if CURRENT ROLE is not set, take current session's AUTH ROLE
        let mut current_role_name = self.get_current_role().map(|r| r.name);
        if current_role_name.is_none() {
            current_role_name = self.session_ctx.get_auth_role();
        }

        // if CURRENT ROLE and AUTH ROLE are not set, take current user's DEFAULT ROLE
        if current_role_name.is_none() {
            current_role_name = self
                .session_ctx
                .get_current_user()
                .map(|u| u.option.default_role().cloned())
                .unwrap_or(None)
        };

        // if CURRENT ROLE, AUTH ROLE and DEFAULT ROLE are not set, take PUBLIC role
        let current_role_name =
            current_role_name.unwrap_or_else(|| BUILTIN_ROLE_PUBLIC.to_string());

        // I can not use the CURRENT ROLE, reset to PUBLIC role
        let role = self
            .validate_available_role(&current_role_name)
            .await
            .or_else(|e| {
                if e.code() == ErrorCode::INVALID_ROLE {
                    Ok(public_role)
                } else {
                    Err(e)
                }
            })?;
        self.session_ctx.set_current_role(Some(role));
        Ok(())
    }
}

#[async_trait::async_trait]
impl<'a> SessionPrivilegeManager for SessionPrivilegeManagerImpl<'a> {
    // set_authed_user() is called after authentication is passed in various protocol handlers, like
    // HTTP handler, clickhouse query handler, mysql query handler. auth_role represents the role
    // granted by external authenticator, it will overwrite the current user's granted roles, and
    // becomes the CURRENT ROLE if not set session.role in the HTTP query.
    #[async_backtrace::framed]
    async fn set_authed_user(&self, user: UserInfo, auth_role: Option<String>) -> Result<()> {
        self.session_ctx.set_current_user(user);
        self.session_ctx.set_auth_role(auth_role);
        self.ensure_current_role().await?;
        Ok(())
    }

    #[async_backtrace::framed]
    async fn set_current_role(&self, role_name: Option<String>) -> Result<()> {
        let role = match &role_name {
            Some(role_name) => Some(self.validate_available_role(role_name).await?),
            None => None,
        };
        self.session_ctx.set_current_role(role);
        Ok(())
    }

    /// If secondary_roles is set, it must be ALL or NONE:
    /// 1. ALL: all the roles granted to the current user will have effects on validate_privilege,
    ///    `secondary_roles` will be set to None, which is default.
    /// 2. NONE: only the current_role has effects on validate_privilge, `secondary_roles`
    ///    will be set to Some([]).
    /// We can release this restriction in the future if any user needed. If an user want to set
    /// secondary_roles to be a subset of the roles granted to the current user, he can pass
    /// `secondary_roles` as Some([role1, role2, .. etc.]).
    #[async_backtrace::framed]
    async fn set_secondary_roles(&self, secondary_roles: Option<Vec<String>>) -> Result<()> {
        if secondary_roles.is_some() && !secondary_roles.as_ref().unwrap().is_empty() {
            return Err(ErrorCode::InvalidArgument(
                "only ALL or NONE is allowed on setting secondary roles",
            ));
        }
        self.session_ctx.set_secondary_roles(secondary_roles);
        Ok(())
    }

    fn get_secondary_roles(&self) -> Option<Vec<String>> {
        self.session_ctx.get_secondary_roles()
    }

    fn get_current_user(&self) -> Result<UserInfo> {
        self.session_ctx
            .get_current_user()
            .ok_or_else(|| ErrorCode::AuthenticateFailure("unauthenticated"))
    }

    fn get_current_role(&self) -> Option<RoleInfo> {
        self.session_ctx.get_current_role()
    }
    fn get_auth_role(&self) -> Option<String> {
        self.session_ctx.get_auth_role()
    }

    #[async_backtrace::framed]
    async fn get_all_effective_roles(&self) -> Result<Vec<RoleInfo>> {
        let secondary_roles = self.session_ctx.get_secondary_roles();

        // if secondary_roles is not set, return all the available roles
        if secondary_roles.is_none() {
            let available_roles = self.get_all_available_roles().await?;
            return Ok(available_roles);
        }

        // the current role is always included in the effective roles.
        self.ensure_current_role().await?;
        let mut role_names = self
            .get_current_role()
            .map(|r| vec![r.name])
            .unwrap_or_default();

        // if secondary_roles is set to be Some([]), only return the current role and its related roles.
        // if the secondary_roles is set to be non-empty, return both current_role and the secondary_roles
        // and their related roles.
        let secondary_roles = secondary_roles.unwrap_or_default();
        role_names.extend(secondary_roles);

        // find their related roles as the final effective roles
        let role_cache = RoleCacheManager::instance();
        let tenant = self.session_ctx.get_current_tenant();
        let effective_roles = role_cache.find_related_roles(&tenant, &role_names).await?;
        Ok(effective_roles)
    }

    // Returns all the roles the current session has. If the user have been granted auth_role,
    // the other roles will be ignored.
    // On executing SET ROLE, the role have to be one of the available roles.
    #[async_backtrace::framed]
    async fn get_all_available_roles(&self) -> Result<Vec<RoleInfo>> {
        let roles = match self.session_ctx.get_auth_role() {
            Some(auth_role) => vec![auth_role],
            None => {
                let current_user = self.get_current_user()?;
                let mut roles = current_user.grants.roles();
                if let Some(current_role) = self.get_current_role() {
                    roles.push(current_role.name);
                }
                roles
            }
        };

        let tenant = self.session_ctx.get_current_tenant();
        let mut related_roles = RoleCacheManager::instance()
            .find_related_roles(&tenant, &roles)
            .await?;
        related_roles.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(related_roles)
    }

    #[async_backtrace::framed]
    async fn validate_privilege(
        &self,
        object: &GrantObject,
        privilege: UserPrivilegeType,
        check_current_role_only: bool,
    ) -> Result<()> {
        // 1. check user's privilege set
        let current_user = self.get_current_user()?;
        let user_verified = current_user.grants.verify_privilege(object, privilege);
        if user_verified {
            return Ok(());
        }

        // 2. check the user's roles' privilege set
        self.ensure_current_role().await?;
        let effective_roles = if check_current_role_only {
            if let Some(role) = self.get_current_role() {
                vec![role]
            } else {
                return Err(ErrorCode::InvalidRole(format!(
                    "Validate object {} privilege {} failed. Current role is None for current session.",
                    object, privilege
                )));
            }
        } else {
            self.get_all_effective_roles().await?
        };

        let role_verified = &effective_roles
            .iter()
            .any(|r| r.grants.verify_privilege(object, privilege));
        if *role_verified {
            return Ok(());
        }

        Err(ErrorCode::PermissionDenied("Permission denied"))
    }

    #[async_backtrace::framed]
    async fn has_ownership(
        &self,
        object: &OwnershipObject,
        check_current_role_only: bool,
    ) -> Result<bool> {
        let role_mgr = RoleCacheManager::instance();
        let tenant = self.session_ctx.get_current_tenant();
        let owner_role_name = role_mgr
            .find_object_owner(&tenant, object)
            .await?
            .unwrap_or_else(|| BUILTIN_ROLE_ACCOUNT_ADMIN.to_string());

        let effective_roles = if check_current_role_only {
            if let Some(role) = self.get_current_role() {
                vec![role]
            } else {
                return Err(ErrorCode::InvalidRole(format!(
                    "Validate object {} ownership. Current role is None for current session.",
                    object
                )));
            }
        } else {
            self.get_all_effective_roles().await?
        };
        let exists = effective_roles.iter().any(|r| r.name == owner_role_name);
        return Ok(exists);
    }

    #[async_backtrace::framed]
    async fn validate_available_role(&self, role_name: &str) -> Result<RoleInfo> {
        let available_roles = self.get_all_available_roles().await?;
        let role = available_roles.iter().find(|r| r.name == role_name);
        match role {
            Some(role) => Ok(role.clone()),
            None => {
                let available_role_names = available_roles
                    .iter()
                    .map(|r| r.name.clone())
                    .collect::<Vec<_>>()
                    .join(",");
                Err(ErrorCode::InvalidRole(format!(
                    "Invalid role {} for current session, available: {}",
                    role_name, available_role_names,
                )))
            }
        }
    }

    #[async_backtrace::framed]
    async fn get_visibility_checker(&self) -> Result<GrantObjectVisibilityChecker> {
        // TODO(liyz): is it check the visibility according onwerships?
        let user_api = UserApiProvider::instance();
        let ownerships = user_api
            .role_api(&self.session_ctx.get_current_tenant())
            .get_ownerships()
            .await?;
        let roles = self.get_all_effective_roles().await?;
        let roles_name: Vec<String> = roles.iter().map(|role| role.name.to_string()).collect();

        let ownership_objects = if roles_name.contains(&"account_admin".to_string()) {
            vec![]
        } else {
            let mut ownership_objects = vec![];
            for ownership in ownerships {
                if roles_name.contains(&ownership.data.role) {
                    ownership_objects.push(ownership.data.object);
                }
            }
            ownership_objects
        };

        Ok(GrantObjectVisibilityChecker::new(
            &self.get_current_user()?,
            &roles,
            &ownership_objects,
        ))
    }
}
