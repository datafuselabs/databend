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
use databend_common_cache::Cache;
use databend_common_cache::LruCache;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::user_token::QueryTokenInfo;
use databend_common_meta_app::principal::user_token::TokenType;
use databend_common_meta_app::tenant::Tenant;
use databend_common_users::UserApiProvider;
use parking_lot::RwLock;
use sha2::Digest;
use sha2::Sha256;
use time::Duration;

use crate::servers::http::v1::session::token::unix_ts;
use crate::servers::http::v1::SessionClaim;
use crate::sessions::Session;
use crate::sessions::SessionPrivilegeManager;

/// target TTL
pub const REFRESH_TOKEN_VALIDITY: Duration = Duration::hours(4);
pub const SESSION_TOKEN_VALIDITY: Duration = Duration::hours(1);

/// to cove network latency, retry and time skew
const TOKEN_TTL_DELAY: Duration = Duration::seconds(300);
/// in case of client retry, shorted the ttl instead of drop at once
/// only required for refresh token.
const TOKEN_DROP_DELAY: Duration = Duration::seconds(90);

pub struct TokenPair {
    pub refresh: String,
    pub session: String,
}

fn hash_token(token: &[u8]) -> String {
    hex::encode_upper(Sha256::digest(token))
}
pub struct TokenManager {
    /// store hash only for hit ratio with limited memory, feasible because:
    /// - token contain all info in itself.
    /// - for eviction, LRU itself is enough, no need to check expired tokens specifically.
    session_tokens: RwLock<LruCache<String, ()>>,
    refresh_tokens: RwLock<LruCache<String, ()>>,
}

impl TokenManager {
    pub fn instance() -> Arc<TokenManager> {
        GlobalInstance::get()
    }

    #[async_backtrace::framed]
    pub async fn init(_cfg: &InnerConfig) -> Result<()> {
        GlobalInstance::set(Arc::new(Self {
            session_tokens: RwLock::new(LruCache::new(1024)),
            refresh_tokens: RwLock::new(LruCache::new(1024)),
        }));

        Ok(())
    }

    /// used for both issue token for new session and renew token for existing session.
    /// currently, when renewing, always return a new refresh token instead of update the TTL,
    /// since now we include expire time in token, which can not be updated.
    pub async fn new_token_pair(
        &self,
        session: &Arc<Session>,
        old_token_pair: Option<TokenPair>,
    ) -> Result<(String, TokenPair)> {
        // those infos are set when the request is authed
        let tenant = session.get_current_tenant();
        let tenant_name = tenant.tenant_name().to_string();
        let user = session.get_current_user()?.name;
        let auth_role = session.privilege_mgr().get_auth_role();

        let client_session_id = if let Some(old) = &old_token_pair {
            let claim = SessionClaim::decode(&old.refresh)?;
            assert_eq!(tenant_name, claim.tenant);
            assert_eq!(user, claim.user);
            assert_eq!(auth_role, claim.auth_role);
            claim.session_id
        } else {
            uuid::Uuid::new_v4().to_string()
        };

        let token_api = UserApiProvider::instance().token_api(&tenant);

        let now = unix_ts();
        let mut claim = SessionClaim {
            tenant: tenant_name.clone(),
            user: user.to_owned(),
            auth_role: auth_role.clone(),
            session_id: client_session_id.to_string(),
            nonce: uuid::Uuid::new_v4().to_string(),
            expire_at_in_secs: (now + REFRESH_TOKEN_VALIDITY + SESSION_TOKEN_VALIDITY)
                .whole_seconds() as u64,
        };
        let refresh_token = if let Some(old) = &old_token_pair {
            old.refresh.clone()
        } else {
            claim.encode()
        };
        let refresh_token_hash = hash_token(refresh_token.as_bytes());
        let token_info = QueryTokenInfo {
            token_type: TokenType::Refresh,
            parent: None,
        };

        // by adding SESSION_TOKEN_VALIDITY_IN_SECS, avoid touch refresh token for each request.
        // note the ttl is not accurate, the TTL is in fact longer (by 0 to 1 hour) then expected.
        token_api
            .upsert_token(
                &refresh_token_hash,
                token_info.clone(),
                (REFRESH_TOKEN_VALIDITY + SESSION_TOKEN_VALIDITY + TOKEN_TTL_DELAY).whole_seconds()
                    as u64,
                false,
            )
            .await?;
        if let Some(old) = old_token_pair {
            token_api
                .upsert_token(
                    &old.refresh,
                    token_info,
                    (REFRESH_TOKEN_VALIDITY + TOKEN_DROP_DELAY).whole_seconds() as u64,
                    true,
                )
                .await?;
        };
        self.refresh_tokens
            .write()
            .put(refresh_token_hash.clone(), ());

        claim.expire_at_in_secs = (now + SESSION_TOKEN_VALIDITY).whole_seconds() as u64;
        claim.nonce = uuid::Uuid::new_v4().to_string();

        let session_token = claim.encode();
        let session_token_hash = hash_token(session_token.as_bytes());
        let token_info = QueryTokenInfo {
            token_type: TokenType::Session,
            parent: Some(refresh_token_hash.clone()),
        };
        token_api
            .upsert_token(
                &session_token_hash,
                token_info,
                (REFRESH_TOKEN_VALIDITY + TOKEN_TTL_DELAY).whole_seconds() as u64,
                false,
            )
            .await?;
        self.session_tokens.write().put(session_token_hash, ());

        Ok((client_session_id, TokenPair {
            refresh: refresh_token,
            session: session_token,
        }))
    }

    pub(crate) async fn verify_token(
        self: &Arc<Self>,
        token: &str,
        token_type: TokenType,
    ) -> Result<SessionClaim> {
        let claim = SessionClaim::decode(token)?;
        let now = unix_ts().as_secs();
        if now > claim.expire_at_in_secs {
            return match token_type {
                TokenType::Refresh => Err(ErrorCode::RefreshTokenExpired("refresh token expired")),
                TokenType::Session => Err(ErrorCode::SessionTokenExpired("session token expired")),
            };
        }

        let hash = hash_token(token.as_bytes());
        let cache = match token_type {
            TokenType::Refresh => &self.refresh_tokens,
            TokenType::Session => &self.session_tokens,
        };

        if !cache.read().contains(&hash) {
            let tenant = Tenant::new_literal(&claim.tenant);
            match UserApiProvider::instance()
                .token_api(&tenant)
                .get_token(&hash)
                .await?
            {
                Some(info) if info.token_type == token_type => cache.write().put(hash, ()),
                _ => {
                    return match token_type {
                        TokenType::Refresh => {
                            Err(ErrorCode::RefreshTokenNotFound("refresh token not found"))
                        }
                        TokenType::Session => {
                            Err(ErrorCode::SessionTokenNotFound("session token not found"))
                        }
                    };
                }
            };
        };
        Ok(claim)
    }
}
