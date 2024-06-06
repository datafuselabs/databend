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

use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_users::UserApiProvider;
use tonic::metadata::MetadataMap;
use tonic::Request;
use tonic::Status;

use super::status;
use crate::servers::flight_sql::flight_sql_service::FlightSqlServiceImpl;
use crate::sessions::Session;
use crate::sessions::SessionManager;
use crate::sessions::SessionType;

impl FlightSqlServiceImpl {
    pub(super) fn get_session<T>(&self, req: &Request<T>) -> Result<Arc<Session>, Status> {
        let auth = req
            .metadata()
            .get("authorization")
            .ok_or_else(|| Status::unauthenticated("No authorization header!"))?;
        let str = auth
            .to_str()
            .map_err(|e| Status::unauthenticated(format!("Error parsing header: {e}")))?;
        let authorization = str.to_string();
        let bearer = "Bearer ";
        if !authorization.starts_with(bearer) {
            Err(Status::unauthenticated("Invalid auth header!"))?;
        }
        let session_id = authorization[bearer.len()..].to_string();

        if let Some(session) = self.sessions.lock().get(&session_id) {
            Ok(session)
        } else {
            Err(Status::unauthenticated(format!(
                "session_id not found: {session_id}"
            )))?
        }
    }

    pub(super) fn get_header_value(metadata: &MetadataMap, key: &str) -> Option<String> {
        metadata
            .get(key)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.to_string())
    }

    pub(super) fn get_user_password(metadata: &MetadataMap) -> Result<(String, String), String> {
        let basic = "Basic ";
        let authorization = Self::get_header_value(metadata, "authorization")
            .ok_or("authorization not parsable".to_string())?;

        if !authorization.starts_with(basic) {
            return Err(format!("Auth type not implemented: {authorization}"));
        }
        let base64 = &authorization[basic.len()..];
        let bytes = BASE64_STANDARD
            .decode(base64)
            .map_err(|e| format!("authorization not decodable: {}", e))?;
        let str =
            String::from_utf8(bytes).map_err(|e| format!("authorization not parsable: {}", e))?;
        let parts: Vec<_> = str.split(':').collect();
        let (user, pass) = match parts.as_slice() {
            [user, pass] => (user, pass),
            _ => return Err("Invalid authorization header".to_string()),
        };
        Ok((user.to_string(), pass.to_string()))
    }

    #[async_backtrace::framed]
    pub(super) async fn auth_user_password(
        user: String,
        password: String,
        client_ip: Option<&str>,
    ) -> Result<Arc<Session>, Status> {
        let session_manager = SessionManager::instance();
        let session = session_manager
            .create_session(SessionType::FlightSQL)
            .await
            .map_err(|e| status!("Could not create session", e))?;

        let session = session_manager.register_session(session)?;

        let tenant = session.get_current_tenant();

        let identity = UserIdentity::new(&user);
        let user = UserApiProvider::instance()
            .get_user_with_client_ip(&tenant, identity.clone(), client_ip)
            .await
            .map_err(|e| status!("get_user fail {}", e))?;
        // Check password policy for login
        UserApiProvider::instance()
            .check_login_password(&tenant, identity.clone(), &user)
            .await
            .map_err(|e| status!("not compliant with password policy {}", e))?;

        let password = password.as_bytes().to_vec();
        let password = (!password.is_empty()).then_some(password);

        let authed = match &user.auth_info {
            AuthInfo::None => Ok(()),
            AuthInfo::Password {
                hash_value: h,
                hash_method: t,
            } => match password {
                None => Err(Status::unauthenticated("password required")),
                Some(p) => {
                    if *h == t.hash(&p) {
                        Ok(())
                    } else {
                        Err(Status::unauthenticated("wrong password"))
                    }
                }
            },
            _ => Err(Status::unauthenticated("wrong auth type")),
        };

        UserApiProvider::instance()
            .update_user_login_result(tenant, identity, authed.is_ok(), &user)
            .await?;
        authed?;

        session
            .set_authed_user(user, None)
            .await
            .map_err(|e| status!("set_authed_user fail {}", e))?;
        Ok(session)
    }
}
