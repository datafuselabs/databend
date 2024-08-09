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

//! Principal is a user or role that accesses an entity.

mod connection;
mod file_format;
mod network_policy;
mod ownership_info;
mod password_policy;
mod principal_identity;
pub mod role_ident;
mod role_info;
mod stage_file_path;
pub mod udf_ident;
mod user_auth;
mod user_defined_file_format;
mod user_defined_function;
mod user_grant;
mod user_identity;
mod user_info;
mod user_privilege;
mod user_quota;
mod user_setting;
mod user_stage;

mod ownership_object;

pub mod connection_ident;
pub mod network_policy_ident;
pub mod password_policy_ident;
pub mod stage_file_ident;
pub mod tenant_ownership_object_ident;
pub mod tenant_user_ident;
pub mod user_defined_file_format_ident;
pub mod user_setting_ident;
pub mod user_stage_ident;
pub mod user_token;
pub mod user_token_ident;

pub use connection::*;
pub use file_format::*;
pub use network_policy::NetworkPolicy;
pub use network_policy_ident::NetworkPolicyIdent;
pub use ownership_info::OwnershipInfo;
pub use ownership_object::OwnershipObject;
pub use password_policy::PasswordPolicy;
pub use password_policy_ident::PasswordPolicyIdent;
pub use principal_identity::PrincipalIdentity;
pub use role_ident::RoleIdent;
pub use role_ident::RoleIdentRaw;
pub use role_info::RoleInfo;
pub use role_info::RoleInfoSerdeError;
pub use stage_file_ident::StageFileIdent;
pub use stage_file_path::StageFilePath;
pub use tenant_ownership_object_ident::TenantOwnershipObjectIdent;
pub use tenant_user_ident::TenantUserIdent;
pub use udf_ident::UdfIdent;
pub use udf_ident::UdfIdentRaw;
pub use user_auth::AuthInfo;
pub use user_auth::AuthType;
pub use user_auth::PasswordHashMethod;
pub use user_defined_file_format::UserDefinedFileFormat;
pub use user_defined_function::LambdaUDF;
pub use user_defined_function::UDFDefinition;
pub use user_defined_function::UDFScript;
pub use user_defined_function::UDFServer;
pub use user_defined_function::UserDefinedFunction;
pub use user_grant::GrantEntry;
pub use user_grant::GrantObject;
pub use user_grant::UserGrantSet;
pub use user_identity::UserIdentity;
pub use user_info::UserInfo;
pub use user_info::UserOption;
pub use user_info::UserOptionFlag;
pub use user_privilege::UserPrivilegeSet;
pub use user_privilege::UserPrivilegeType;
pub use user_quota::UserQuota;
pub use user_setting::UserSetting;
pub use user_setting::UserSettingValue;
pub use user_setting_ident::SettingIdent;
pub use user_stage::*;
pub use user_stage_ident::StageIdent;
