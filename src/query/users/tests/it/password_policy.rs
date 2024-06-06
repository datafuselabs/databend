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

use chrono::Duration;
use chrono::TimeZone;
use chrono::Utc;
use databend_common_ast::ast::AuthOption;
use databend_common_ast::ast::AuthType;
use databend_common_base::base::tokio;
use databend_common_exception::Result;
use databend_common_grpc::RpcClientConf;
use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::PasswordPolicy;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserOption;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::tenant::Tenant;
use databend_common_users::UserApiProvider;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_password_policy() -> Result<()> {
    let conf = RpcClientConf::default();
    let tenant_name = "test";
    let tenant = Tenant::new_literal(tenant_name);

    let user_mgr = UserApiProvider::try_create_simple(conf, &tenant).await?;
    let username = "test-user1";
    let hostname = "%";
    let pwd1 = "123456abcDEF!@#1";
    let pwd2 = "123456abcDEF!@#2";
    let pwd3 = "123456abcDEF!@#3";
    let pwd4 = "123456abcDEF!@#4";
    let pwd5 = "123456abcDEF!@#5";
    let pwd6 = "123456abcDEF!@#6";
    let invalid_pwd = "123";

    let policy_name = "test_policy".to_string();

    // add password policy
    let password_policy = PasswordPolicy {
        name: policy_name.clone(),
        min_length: 12,
        max_length: 24,
        min_upper_case_chars: 2,
        min_lower_case_chars: 2,
        min_numeric_chars: 2,
        min_special_chars: 2,
        min_age_days: 1,
        max_age_days: 30,
        max_retries: 3,
        lockout_time_mins: 30,
        history: 5,
        comment: "".to_string(),
        create_on: Utc.with_ymd_and_hms(2023, 12, 19, 12, 0, 0).unwrap(),
        update_on: None,
    };

    let res = user_mgr
        .add_password_policy(
            &tenant,
            password_policy.clone(),
            &CreateOption::CreateIfNotExists,
        )
        .await;
    assert!(res.is_ok());

    // invalid min length
    let mut invalid_password_policy1 = password_policy.clone();
    invalid_password_policy1.min_length = 0;
    let res = user_mgr
        .add_password_policy(
            &tenant,
            invalid_password_policy1,
            &CreateOption::CreateIfNotExists,
        )
        .await;
    assert!(res.is_err());

    // invalid max length
    let mut invalid_password_policy2 = password_policy.clone();
    invalid_password_policy2.max_length = 260;
    let res = user_mgr
        .add_password_policy(
            &tenant,
            invalid_password_policy2,
            &CreateOption::CreateIfNotExists,
        )
        .await;
    assert!(res.is_err());

    // invalid min length greater than max length
    let mut invalid_password_policy3 = password_policy.clone();
    invalid_password_policy3.min_length = 30;
    invalid_password_policy3.max_length = 20;
    let res = user_mgr
        .add_password_policy(
            &tenant,
            invalid_password_policy3,
            &CreateOption::CreateIfNotExists,
        )
        .await;
    assert!(res.is_err());

    // invalid min chars
    let mut invalid_password_policy4 = password_policy.clone();
    invalid_password_policy4.min_upper_case_chars = 270;
    invalid_password_policy4.min_lower_case_chars = 271;
    invalid_password_policy4.min_numeric_chars = 272;
    invalid_password_policy4.min_special_chars = 273;
    let res = user_mgr
        .add_password_policy(
            &tenant,
            invalid_password_policy4,
            &CreateOption::CreateIfNotExists,
        )
        .await;
    assert!(res.is_err());

    // invalid sum of upper chars, lower chars, numeric chars and special chars greater than max length
    let mut invalid_password_policy5 = password_policy.clone();
    invalid_password_policy5.max_length = 30;
    invalid_password_policy5.min_upper_case_chars = 10;
    invalid_password_policy5.min_lower_case_chars = 11;
    invalid_password_policy5.min_numeric_chars = 12;
    invalid_password_policy5.min_special_chars = 13;
    let res = user_mgr
        .add_password_policy(
            &tenant,
            invalid_password_policy5,
            &CreateOption::CreateIfNotExists,
        )
        .await;
    assert!(res.is_err());

    // invalid min age days greater than max age days
    let mut invalid_password_policy6 = password_policy.clone();
    invalid_password_policy6.min_age_days = 20;
    invalid_password_policy6.max_age_days = 10;
    let res = user_mgr
        .add_password_policy(
            &tenant,
            invalid_password_policy6,
            &CreateOption::CreateIfNotExists,
        )
        .await;
    assert!(res.is_err());

    // invalid max retries
    let mut invalid_password_policy7 = password_policy.clone();
    invalid_password_policy7.max_retries = 20;
    let res = user_mgr
        .add_password_policy(
            &tenant,
            invalid_password_policy7,
            &CreateOption::CreateIfNotExists,
        )
        .await;
    assert!(res.is_err());

    // invalid lockout time mins
    let mut invalid_password_policy8 = password_policy.clone();
    invalid_password_policy8.lockout_time_mins = 2000;
    let res = user_mgr
        .add_password_policy(
            &tenant,
            invalid_password_policy8,
            &CreateOption::CreateIfNotExists,
        )
        .await;
    assert!(res.is_err());

    // invalid history
    let mut invalid_password_policy9 = password_policy.clone();
    invalid_password_policy9.history = 50;
    let res = user_mgr
        .add_password_policy(
            &tenant,
            invalid_password_policy9,
            &CreateOption::CreateIfNotExists,
        )
        .await;
    assert!(res.is_err());

    // verify new user add password
    let user_option = UserOption::empty().with_password_policy(Some(policy_name.clone()));
    let auth_option = AuthOption {
        auth_type: Some(AuthType::DoubleSha1Password),
        password: Some(pwd1.to_string()),
    };
    let res = user_mgr
        .verify_password(&tenant, &user_option, &auth_option, None, None)
        .await;
    assert!(res.is_ok());

    // simple password, invalid complexity
    let mut auth_option1 = auth_option.clone();
    auth_option1.password = Some(invalid_pwd.to_string());
    let res = user_mgr
        .verify_password(&tenant, &user_option, &auth_option1, None, None)
        .await;
    assert!(res.is_err());

    // verify user change password
    let auth_info1 = AuthInfo::create2(&None, &Some(pwd1.to_string())).unwrap();
    let mut user_info = UserInfo::new(username, hostname, auth_info1.clone());
    let mut option = UserOption::empty();
    option = option.with_password_policy(Some(policy_name.clone()));
    user_info.update_auth_option(None, Some(option));
    let res = user_mgr
        .verify_password(
            &tenant,
            &user_option,
            &auth_option,
            Some(&user_info),
            Some(&auth_info1),
        )
        .await;
    assert!(res.is_ok());

    // change the password before the `min_age_days` reached.
    user_info.update_auth_option(Some(auth_info1.clone()), None);
    user_info.update_auth_history(Some(auth_info1));
    let auth_info2 = AuthInfo::create2(&None, &Some(pwd2.to_string())).unwrap();
    let res = user_mgr
        .verify_password(
            &tenant,
            &user_option,
            &auth_option,
            Some(&user_info),
            Some(&auth_info2),
        )
        .await;
    assert!(res.is_err());

    // modify the last password update time to 3 days ago, just for test
    user_info.password_update_on = Some(Utc::now().checked_sub_signed(Duration::days(3)).unwrap());
    let res = user_mgr
        .verify_password(
            &tenant,
            &user_option,
            &auth_option,
            Some(&user_info),
            Some(&auth_info2),
        )
        .await;
    assert!(res.is_ok());

    // password cannot repeat
    user_info.update_auth_option(Some(auth_info2.clone()), None);
    user_info.update_auth_history(Some(auth_info2));
    let new_auth_info1 = AuthInfo::create2(&None, &Some(pwd1.to_string())).unwrap();
    let res = user_mgr
        .verify_password(
            &tenant,
            &user_option,
            &auth_option,
            Some(&user_info),
            Some(&new_auth_info1),
        )
        .await;
    assert!(res.is_err());

    // change the password 6 times with different values
    let auth_info3 = AuthInfo::create2(&None, &Some(pwd3.to_string())).unwrap();
    user_info.update_auth_option(Some(auth_info3.clone()), None);
    user_info.update_auth_history(Some(auth_info3));
    let auth_info4 = AuthInfo::create2(&None, &Some(pwd4.to_string())).unwrap();
    user_info.update_auth_option(Some(auth_info4.clone()), None);
    user_info.update_auth_history(Some(auth_info4));
    let auth_info5 = AuthInfo::create2(&None, &Some(pwd5.to_string())).unwrap();
    user_info.update_auth_option(Some(auth_info5.clone()), None);
    user_info.update_auth_history(Some(auth_info5));
    let auth_info6 = AuthInfo::create2(&None, &Some(pwd6.to_string())).unwrap();
    user_info.update_auth_option(Some(auth_info6.clone()), None);
    user_info.update_auth_history(Some(auth_info6));
    // the first password can use again
    user_info.password_update_on = Some(Utc::now().checked_sub_signed(Duration::days(3)).unwrap());
    let res = user_mgr
        .verify_password(
            &tenant,
            &user_option,
            &auth_option,
            Some(&user_info),
            Some(&new_auth_info1),
        )
        .await;
    assert!(res.is_ok());

    // check login password success
    let identity = UserIdentity::new(username);
    let res = user_mgr
        .check_login_password(&tenant, identity.clone(), &user_info)
        .await;
    assert!(res.is_ok());

    // login fail 3 times
    for _ in 0..3 {
        user_info.update_login_fail_history()
    }
    // user lockout because of too many login fails
    let res = user_mgr
        .check_login_password(&tenant, identity.clone(), &user_info)
        .await;
    assert!(res.is_err());

    // set lockout time for test
    user_info.lockout_time = Some(
        Utc::now()
            .checked_add_signed(Duration::minutes(30))
            .unwrap(),
    );
    // user can't log in because of locked out
    let res = user_mgr
        .check_login_password(&tenant, identity.clone(), &user_info)
        .await;
    assert!(res.is_err());

    // clear login fail for test
    user_info.clear_login_fail_history();
    // set last change password time as 31 days ago for test
    user_info.password_update_on = Some(Utc::now().checked_sub_signed(Duration::days(31)).unwrap());
    // user can't log in because of not change password more than max allowed days
    let res = user_mgr
        .check_login_password(&tenant, identity.clone(), &user_info)
        .await;
    assert!(res.is_err());

    // update password policy
    let res = user_mgr
        .update_password_policy(
            &tenant,
            &policy_name,
            Some(10),
            Some(20),
            Some(3),
            Some(3),
            Some(3),
            Some(3),
            Some(2),
            Some(50),
            Some(10),
            Some(20),
            Some(10),
            None,
            false,
        )
        .await;
    assert!(res.is_ok());

    // add user
    user_mgr
        .add_user(&tenant, user_info, &CreateOption::CreateIfNotExists)
        .await?;

    // drop password policy
    let res = user_mgr
        .drop_password_policy(&tenant, policy_name.as_ref(), false)
        .await;
    assert!(res.is_err());

    user_mgr.drop_user(&tenant, identity, false).await?;

    let res = user_mgr
        .drop_password_policy(&tenant, policy_name.as_ref(), false)
        .await;
    assert!(res.is_ok());

    Ok(())
}
