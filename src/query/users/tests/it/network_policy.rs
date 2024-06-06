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

use chrono::TimeZone;
use chrono::Utc;
use databend_common_base::base::tokio;
use databend_common_exception::Result;
use databend_common_grpc::RpcClientConf;
use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::NetworkPolicy;
use databend_common_meta_app::principal::PasswordHashMethod;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserOption;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;
use databend_common_users::UserApiProvider;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_network_policy() -> Result<()> {
    let conf = RpcClientConf::default();
    let tenant = Tenant::new_literal("test");

    let user_mgr = UserApiProvider::try_create_simple(conf, &tenant).await?;
    let username = "test-user1";
    let hostname = "%";
    let pwd = "test-pwd";

    let policy_name = "test_policy".to_string();
    let allowed_ip_list = vec!["192.168.0.0/24".to_string()];
    let blocked_ip_list = vec!["192.168.0.10".to_string(), "192.168.0.20".to_string()];

    // add network policy
    let network_policy = NetworkPolicy {
        name: policy_name.clone(),
        allowed_ip_list,
        blocked_ip_list,
        comment: "".to_string(),
        create_on: Utc.with_ymd_and_hms(2023, 7, 10, 12, 0, 9).unwrap(),
        update_on: None,
    };
    user_mgr
        .add_network_policy(&tenant, network_policy, &CreateOption::Create)
        .await?;

    // add user
    let auth_info = AuthInfo::Password {
        hash_value: Vec::from(pwd),
        hash_method: PasswordHashMethod::Sha256,
    };

    let mut user_info = UserInfo::new(username, hostname, auth_info.clone());
    let mut option = UserOption::empty();
    option = option.with_network_policy(Some(policy_name.clone()));
    user_info.update_auth_option(None, Some(option));
    user_mgr
        .add_user(&tenant, user_info, &CreateOption::Create)
        .await?;

    let user = UserIdentity::new(username);

    // check get user with client ip
    let res = user_mgr
        .get_user_with_client_ip(&tenant, user.clone(), Some("192.168.0.1"))
        .await;
    assert!(res.is_ok());

    let res = user_mgr
        .get_user_with_client_ip(&tenant, user.clone(), Some("192.168.0.10"))
        .await;
    assert!(res.is_err());

    let res = user_mgr
        .get_user_with_client_ip(&tenant, user.clone(), Some("192.168.0.20"))
        .await;
    assert!(res.is_err());

    let res = user_mgr
        .get_user_with_client_ip(&tenant, user.clone(), Some("127.0.0.1"))
        .await;
    assert!(res.is_err());

    // update network policy
    let new_allowed_ip_list = vec!["127.0.0.0/24".to_string()];
    let new_blocked_ip_list = vec!["127.0.0.10".to_string()];
    user_mgr
        .update_network_policy(
            &tenant,
            policy_name.as_ref(),
            Some(new_allowed_ip_list),
            Some(new_blocked_ip_list),
            None,
            false,
        )
        .await?;

    // check get user with client ip
    let res = user_mgr
        .get_user_with_client_ip(&tenant, user.clone(), Some("192.168.0.1"))
        .await;
    assert!(res.is_err());

    let res = user_mgr
        .get_user_with_client_ip(&tenant, user.clone(), Some("127.0.0.1"))
        .await;
    assert!(res.is_ok());

    let res = user_mgr
        .get_user_with_client_ip(&tenant, user.clone(), Some("127.0.0.10"))
        .await;
    assert!(res.is_err());

    // drop network policy
    let res = user_mgr
        .drop_network_policy(&tenant, policy_name.as_ref(), false)
        .await;
    assert!(res.is_err());

    user_mgr.drop_user(&tenant, user.clone(), false).await?;

    let res = user_mgr
        .drop_network_policy(&tenant, policy_name.as_ref(), false)
        .await;
    assert!(res.is_ok());

    Ok(())
}
