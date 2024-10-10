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

use databend_common_meta_kvapi::kvapi::KeyBuilder;
use databend_common_meta_kvapi::kvapi::KeyCodec;
use databend_common_meta_kvapi::kvapi::KeyError;
use databend_common_meta_kvapi::kvapi::KeyParser;

use crate::tenant_key::ident::TIdent;

#[derive(PartialEq, Debug)]
pub struct UserSessionId {
    pub session_id: String,
    pub user_name: String,
}

impl UserSessionId {
    const ESCAPE_CHARS: [u8; 1] = [b':'];

    pub fn parse(s: &str) -> Result<Self, KeyError> {
        let parts = s.splitn(2, ':').collect::<Vec<&str>>();
        if parts.len() != 2 {
            return Err(KeyError::WrongNumberOfSegments {
                expect: 2,
                got: s.to_string(),
            });
        }

        let session_id = KeyParser::unescape_specified(parts[0], &Self::ESCAPE_CHARS)?;

        Ok(UserSessionId {
            session_id,
            user_name: parts[1].to_string(),
        })
    }

    /// Encode the user identity into a string for constructing a meta-service key.
    /// note this key is also used as a segment in S3 path.
    ///
    /// Similar to meta server, Opendal handles URL percent_encoding automatically, we do not need
    /// to care about it here.
    ///
    /// Since the session ID is expected to be a UUID, only minimal escaping is needed.
    /// This preserves readability without altering the UUID, making the result more human-friendly.
    pub fn encode(&self) -> String {
        format!(
            "{}:{}",
            KeyBuilder::escape_specified(&self.session_id, &Self::ESCAPE_CHARS),
            &self.user_name,
        )
    }
}

impl KeyCodec for UserSessionId {
    fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
        b.push_str(&self.encode())
    }

    fn decode_key(parser: &mut KeyParser) -> Result<Self, KeyError>
    where Self: Sized {
        let s = parser.next_str()?;
        Self::parse(&s)
    }
}

/// Define the meta-service key for a user setting.
pub type ClientSessionIdent = TIdent<Resource, UserSessionId>;

pub use kvapi_impl::Resource;

mod kvapi_impl {

    use databend_common_meta_kvapi::kvapi;

    use crate::principal::client_session::ClientSession;
    use crate::principal::client_session_ident::ClientSessionIdent;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_session";
        const TYPE: &'static str = "ClientSession";
        const HAS_TENANT: bool = true;
        type ValueType = ClientSession;
    }

    impl kvapi::Value for ClientSession {
        type KeyType = ClientSessionIdent;
        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_meta_kvapi::kvapi::Key;

    use crate::principal::client_session_ident::ClientSessionIdent;
    use crate::principal::client_session_ident::UserSessionId;
    use crate::tenant::Tenant;

    #[test]
    fn test_setting_ident() {
        let tenant = Tenant::new_literal("tenant1");
        let id = UserSessionId {
            session_id: "x:y".to_string(),
            user_name: "m:n".to_string(),
        };
        let ident = ClientSessionIdent::new_generic(tenant.clone(), id);
        // encode to x%3a:y:m:n first
        assert_eq!(
            "__fd_session/tenant1/x%253ay%3am%3an",
            ident.to_string_key()
        );

        let got = ClientSessionIdent::from_str_key(&ident.to_string_key()).unwrap();
        assert_eq!(ident, got);
    }
}
