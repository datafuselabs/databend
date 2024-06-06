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

use databend_common_base::base::GlobalInstance;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;
use databend_common_users::JwtAuthenticator;
use databend_common_users::UserApiProvider;
use minitrace::func_name;

use crate::sessions::Session;

pub struct AuthMgr {
    jwt_auth: Option<JwtAuthenticator>,
}

pub enum Credential {
    Jwt {
        token: String,
        client_ip: Option<String>,
    },
    Password {
        name: String,
        password: Option<Vec<u8>>,
        client_ip: Option<String>,
    },
}

impl AuthMgr {
    pub fn init(cfg: &InnerConfig) -> Result<()> {
        GlobalInstance::set(AuthMgr::create(cfg));
        Ok(())
    }

    pub fn instance() -> Arc<AuthMgr> {
        GlobalInstance::get()
    }

    fn create(cfg: &InnerConfig) -> Arc<AuthMgr> {
        Arc::new(AuthMgr {
            jwt_auth: JwtAuthenticator::create(
                cfg.query.jwt_key_file.clone(),
                cfg.query.jwt_key_files.clone(),
            ),
        })
    }

    #[async_backtrace::framed]
    pub async fn auth(&self, session: &mut Session, credential: &Credential) -> Result<()> {
        let user_api = UserApiProvider::instance();
        match credential {
            Credential::Jwt {
                token: t,
                client_ip,
            } => {
                let jwt_auth = self
                    .jwt_auth
                    .as_ref()
                    .ok_or_else(|| ErrorCode::AuthenticateFailure("jwt auth not configured."))?;
                let jwt = jwt_auth.parse_jwt_claims(t.as_str()).await?;
                let user_name = jwt.subject.ok_or_else(|| {
                    ErrorCode::AuthenticateFailure(
                        "jwt auth not configured correctly, user name is missing.",
                    )
                })?;

                // setup tenant if the JWT claims contain extra.tenant_id
                if let Some(tenant) = jwt.custom.tenant_id {
                    let tenant = Tenant::new_or_err(tenant, func_name!())?;
                    session.set_current_tenant(tenant);
                };

                let tenant = session.get_current_tenant();
                let identity = UserIdentity::new(&user_name);

                // create a new user for this identity if not exists
                let user = match user_api
                    .get_user_with_client_ip(&tenant, identity.clone(), client_ip.as_deref())
                    .await
                {
                    Ok(user_info) => match user_info.auth_info {
                        AuthInfo::JWT => user_info,
                        _ => return Err(ErrorCode::AuthenticateFailure("wrong auth type")),
                    },
                    Err(e) => {
                        match e.code() {
                            ErrorCode::UNKNOWN_USER => {}
                            ErrorCode::META_SERVICE_ERROR => {
                                return Err(e);
                            }
                            _ => return Err(ErrorCode::AuthenticateFailure(e.message())),
                        }
                        let ensure_user = jwt
                            .custom
                            .ensure_user
                            .ok_or_else(|| ErrorCode::AuthenticateFailure(e.message()))?;
                        // create a new user if not exists
                        let mut user_info = UserInfo::new(&user_name, "%", AuthInfo::JWT);
                        if let Some(ref roles) = ensure_user.roles {
                            for role in roles.clone().into_iter() {
                                user_info.grants.grant_role(role);
                            }
                        }
                        user_api
                            .add_user(&tenant, user_info.clone(), &CreateOption::CreateIfNotExists)
                            .await?;
                        user_info
                    }
                };

                session.set_authed_user(user, jwt.custom.role).await?;
            }
            Credential::Password {
                name: n,
                password: p,
                client_ip,
            } => {
                let tenant = session.get_current_tenant();
                let identity = UserIdentity::new(n);
                let user = user_api
                    .get_user_with_client_ip(&tenant, identity.clone(), client_ip.as_deref())
                    .await?;
                // Check password policy for login
                UserApiProvider::instance()
                    .check_login_password(&tenant, identity.clone(), &user)
                    .await?;

                let authed = match &user.auth_info {
                    AuthInfo::None => Ok(()),
                    AuthInfo::Password {
                        hash_value: h,
                        hash_method: t,
                    } => match p {
                        None => Err(ErrorCode::AuthenticateFailure("password required")),
                        Some(p) => {
                            if *h == t.hash(p) {
                                Ok(())
                            } else {
                                Err(ErrorCode::AuthenticateFailure("wrong password"))
                            }
                        }
                    },
                    _ => Err(ErrorCode::AuthenticateFailure("wrong auth type")),
                };
                UserApiProvider::instance()
                    .update_user_login_result(tenant, identity, authed.is_ok(), &user)
                    .await?;

                authed?;

                session.set_authed_user(user, None).await?;
            }
        };
        Ok(())
    }
}
