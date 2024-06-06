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

use std::fmt;

use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::KeyBuilder;
use databend_common_meta_kvapi::kvapi::KeyCodec;
use databend_common_meta_kvapi::kvapi::KeyError;
use databend_common_meta_kvapi::kvapi::KeyParser;

/// Uniquely identifies a user with a username and a hostname.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Hash, Default)]
pub struct UserIdentity {
    pub username: String,
    pub hostname: String,
}

impl UserIdentity {
    const ESCAPE_CHARS: [u8; 2] = [b'\'', b'@'];

    pub fn new(name: impl ToString) -> Self {
        Self {
            username: name.to_string(),
            hostname: "%".to_string(),
        }
    }

    pub fn parse(s: &str) -> Result<Self, kvapi::KeyError> {
        let parts = s.splitn(2, '@').collect::<Vec<&str>>();
        if parts.len() != 2 {
            return Err(kvapi::KeyError::WrongNumberOfSegments {
                expect: 2,
                got: s.to_string(),
            });
        }

        // trim single quotes
        let username =
            kvapi::KeyParser::unescape_specified(parts[0].trim_matches('\''), &Self::ESCAPE_CHARS)?;
        let hostname =
            kvapi::KeyParser::unescape_specified(parts[1].trim_matches('\''), &Self::ESCAPE_CHARS)?;

        Ok(UserIdentity { username, hostname })
    }

    /// Encode the user identity into a string for building a meta-service key.
    pub fn encode(&self) -> String {
        format!(
            "'{}'@'{}'",
            kvapi::KeyBuilder::escape_specified(&self.username, &Self::ESCAPE_CHARS),
            kvapi::KeyBuilder::escape_specified(&self.hostname, &Self::ESCAPE_CHARS),
        )
    }

    /// Display should have a different implementation from encode(), one for human readable and the other for machine readable.
    pub fn display(&self) -> impl fmt::Display {
        format!(
            "'{}'@'{}'",
            kvapi::KeyBuilder::escape_specified(&self.username, &Self::ESCAPE_CHARS),
            kvapi::KeyBuilder::escape_specified(&self.hostname, &Self::ESCAPE_CHARS),
        )
    }

    /// Identity is human readable, used to record log or err message.
    pub fn identity(&self) -> impl fmt::Display {
        format!("'{}'@'{}'", &self.username, &self.hostname)
    }
}

impl KeyCodec for UserIdentity {
    fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
        b.push_str(&self.encode())
    }

    fn decode_key(parser: &mut KeyParser) -> Result<Self, KeyError>
    where Self: Sized {
        let s = parser.next_str()?;
        Self::parse(&s)
    }
}

impl From<databend_common_ast::ast::UserIdentity> for UserIdentity {
    fn from(user: databend_common_ast::ast::UserIdentity) -> Self {
        UserIdentity::new(user.username)
    }
}
