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

use std::time::Duration;

use base64::prelude::*;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use serde::Deserialize;
use serde::Serialize;

const TOKEN_PREFIX: &str = "bend-v1-";

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SessionClaim {
    pub tenant: String,
    pub user: String,
    pub auth_role: Option<String>,
    #[serde(rename = "sid")]
    pub session_id: String,
    pub(crate) nonce: String,
    #[serde(rename = "exp")]
    pub expire_at_in_secs: u64,
}

pub fn unix_ts() -> Duration {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("The system clock is not properly set")
}

impl SessionClaim {
    pub fn is_databend_token(token: &str) -> bool {
        token.starts_with(TOKEN_PREFIX)
    }

    pub(crate) fn encode(&self) -> String {
        let token = BASE64_STANDARD.encode(serde_json::to_vec(&self).unwrap());
        format!("{TOKEN_PREFIX}{token}")
    }

    pub fn decode(token: &str) -> Result<Self> {
        let fmt_err = |reason: String| {
            ErrorCode::Internal(format!("fail to decode token({reason}): {token}"))
        };
        if token.len() < TOKEN_PREFIX.len() {
            return Err(fmt_err("too short".to_string()));
        }
        let token = &token.as_bytes()[TOKEN_PREFIX.len()..];
        let json = BASE64_STANDARD
            .decode(token)
            .map_err(|e| fmt_err(format!("base64 decode error: {e}")))?;
        serde_json::from_slice::<SessionClaim>(&json)
            .map_err(|e| fmt_err(format!("json decode error: {e}")))
    }
}
