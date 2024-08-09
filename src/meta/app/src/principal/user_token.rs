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

use serde::Deserialize;
use serde::Serialize;

/// A client starts with /session/login to get the initial refresh_token and session_token pair.
/// - Use session_token for computing.
/// - Use refresh_token to auth /session/renew and get new pair when session_token expires.
#[derive(
    serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, num_derive::FromPrimitive,
)]
pub enum TokenType {
    Refresh = 1,
    Session = 2,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct QueryTokenInfo {
    pub token_type: TokenType,
    /// used to delete refresh token when close session, which authed by session_token too.
    /// None for Refresh token.
    pub parent: Option<String>,
}
