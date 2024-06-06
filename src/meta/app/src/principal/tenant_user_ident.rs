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

use crate::tenant::ToTenant;
use crate::tenant_key::ident::TIdent;

/// A user identity belonging to a tenant.
pub type TenantUserIdent = TIdent<Resource, UserIdentity>;
pub type TenantUserIdentRaw = TIdentRaw<Resource, UserIdentity>;

pub use kvapi_impl::Resource;

use crate::principal::UserIdentity;
use crate::tenant_key::raw::TIdentRaw;

impl TenantUserIdent {
    pub fn new(tenant: impl ToTenant, user: UserIdentity) -> Self {
        Self::new_generic(tenant, user)
    }

    pub fn new_user_host(tenant: impl ToTenant, user: impl ToString) -> Self {
        Self::new(tenant, UserIdentity::new(user))
    }
}

mod kvapi_impl {

    use databend_common_meta_kvapi::kvapi;

    use crate::principal::UserInfo;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_users";
        const TYPE: &'static str = "TenantUserIdent";
        const HAS_TENANT: bool = true;
        type ValueType = UserInfo;
    }

    impl kvapi::Value for UserInfo {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }

    // // Use these error types to replace usage of ErrorCode if possible.
    // impl From<ExistError<Resource>> for ErrorCode {
    // impl From<UnknownError<Resource>> for ErrorCode {
}

#[cfg(test)]
mod tests {
    use databend_common_meta_kvapi::kvapi::Key;

    use crate::principal::TenantUserIdent;
    use crate::principal::UserIdentity;
    use crate::tenant::Tenant;

    fn test_format_parse(user: &str, host: &str, expect: &str) {
        let tenant = Tenant::new_literal("test_tenant");
        let user_ident = UserIdentity {
            username: user.to_string(),
            hostname: host.to_string(),
        };

        let tenant_user_ident = TenantUserIdent::new(tenant, user_ident);

        let key = tenant_user_ident.to_string_key();
        assert_eq!(key, expect, "'{user}' '{host}' '{expect}'");

        let tenant_user_ident_parsed = TenantUserIdent::from_str_key(&key).unwrap();
        assert_eq!(
            tenant_user_ident, tenant_user_ident_parsed,
            "'{user}' '{host}' '{expect}'"
        );
    }

    #[test]
    fn test_tenant_user_ident_as_kvapi_key() {
        test_format_parse("user", "%", "__fd_users/test_tenant/%27user%27%40%27%25%27");
        test_format_parse(
            "user'",
            "%",
            "__fd_users/test_tenant/%27user%2527%27%40%27%25%27",
        );

        // With correct encoding the following two pair should not be encoded into the same string.

        test_format_parse(
            "u'@'h",
            "h",
            "__fd_users/test_tenant/%27u%2527%2540%2527h%27%40%27h%27",
        );
        test_format_parse(
            "u",
            "h'@'h",
            "__fd_users/test_tenant/%27u%27%40%27h%2527%2540%2527h%27",
        );
    }
}
