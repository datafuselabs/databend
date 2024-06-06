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

use async_trait::async_trait;
use databend_common_base::base::escape_for_key;
use databend_common_base::base::tokio;
use databend_common_exception::ErrorCode;
use databend_common_management::*;
use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::PasswordHashMethod;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::GetKVReply;
use databend_common_meta_kvapi::kvapi::KVStream;
use databend_common_meta_kvapi::kvapi::ListKVReply;
use databend_common_meta_kvapi::kvapi::MGetKVReply;
use databend_common_meta_kvapi::kvapi::UpsertKVReply;
use databend_common_meta_kvapi::kvapi::UpsertKVReq;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::Operation;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::TxnReply;
use databend_common_meta_types::TxnRequest;
use mockall::predicate::*;
use mockall::*;

// and mock!
mock! {
    pub KV {}
    #[async_trait]
    impl kvapi::KVApi for KV {
        type Error = MetaError;

        async fn upsert_kv(
            &self,
            act: UpsertKVReq,
        ) -> Result<UpsertKVReply, MetaError>;

        async fn get_kv(&self, key: &str) -> Result<GetKVReply,MetaError>;

        async fn mget_kv(
            &self,
            key: &[String],
        ) -> Result<MGetKVReply,MetaError>;

        async fn get_kv_stream(&self, key: &[String]) -> Result<KVStream<MetaError>, MetaError>;

        async fn prefix_list_kv(&self, prefix: &str) -> Result<ListKVReply, MetaError>;

        async fn list_kv(&self, prefix: &str) -> Result<KVStream<MetaError>, MetaError>;

        async fn transaction(&self, txn: TxnRequest) -> Result<TxnReply, MetaError>;

        }
}

fn format_user_key(username: &str, hostname: &str) -> String {
    format!("'{}'@'{}'", username, hostname)
}

fn default_test_auth_info() -> AuthInfo {
    AuthInfo::Password {
        hash_value: Vec::from("test_password"),
        hash_method: PasswordHashMethod::DoubleSha1,
    }
}

mod add {
    use databend_common_meta_app::principal::UserInfo;
    use databend_common_meta_app::schema::CreateOption;
    use databend_common_meta_app::tenant::Tenant;
    use databend_common_meta_types::Operation;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_add_user() -> databend_common_exception::Result<()> {
        let test_user_name = "test_user";
        let test_hostname = "%";
        let user_info = UserInfo::new(test_user_name, test_hostname, default_test_auth_info());

        let v = serialize_struct(&user_info, ErrorCode::IllegalUserInfoFormat, || "")?;
        let value = Operation::Update(serialize_struct(
            &user_info,
            ErrorCode::IllegalUserInfoFormat,
            || "",
        )?);

        let test_key = format!(
            "__fd_users/tenant1/{}",
            escape_for_key(&format_user_key(test_user_name, test_hostname))?
        );
        let test_seq = MatchSeq::Exact(0);

        // normal
        {
            let test_key = test_key.clone();
            let mut api = MockKV::new();
            api.expect_upsert_kv()
                .with(predicate::eq(UpsertKVReq::new(
                    &test_key,
                    test_seq,
                    value.clone(),
                    None,
                )))
                .times(1)
                .return_once(|_u| Ok(UpsertKVReply::new(None, Some(SeqV::new(1, v)))));
            let api = Arc::new(api);
            let user_mgr = UserMgr::create(api, &Tenant::new_literal("tenant1"));
            let res = user_mgr.add_user(user_info.clone(), &CreateOption::Create);

            assert!(res.await.is_ok());
        }

        // already exists
        {
            let test_key = test_key.clone();
            let mut api = MockKV::new();
            api.expect_upsert_kv()
                .with(predicate::eq(UpsertKVReq::new(
                    &test_key,
                    test_seq,
                    value.clone(),
                    None,
                )))
                .times(1)
                .returning(|_u| {
                    Ok(UpsertKVReply::new(
                        Some(SeqV::new(1, vec![])),
                        Some(SeqV::new(1, vec![])),
                    ))
                });

            let api = Arc::new(api);
            let user_mgr = UserMgr::create(api, &Tenant::new_literal("tenant1"));
            let res = user_mgr
                .add_user(user_info.clone(), &CreateOption::Create)
                .await;

            assert_eq!(res.unwrap_err().code(), ErrorCode::USER_ALREADY_EXISTS);
        }

        Ok(())
    }
}

mod get {
    use databend_common_meta_app::principal::UserInfo;
    use databend_common_meta_app::tenant::Tenant;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_get_user_seq_match() -> databend_common_exception::Result<()> {
        let test_user_name = "test";
        let test_hostname = "%";
        let test_key = format!(
            "__fd_users/tenant1/{}",
            escape_for_key(&format_user_key(test_user_name, test_hostname))?
        );

        let user_info = UserInfo::new(test_user_name, test_hostname, default_test_auth_info());
        let value = serialize_struct(&user_info, ErrorCode::IllegalUserInfoFormat, || "")?;

        let mut kv = MockKV::new();
        kv.expect_get_kv()
            .with(predicate::function(move |v| v == test_key.as_str()))
            .times(1)
            .return_once(move |_k| Ok(Some(SeqV::new(1, value))));

        let kv = Arc::new(kv);
        let user_mgr = UserMgr::create(kv, &Tenant::new_literal("tenant1"));
        let res = user_mgr.get_user(user_info.identity(), MatchSeq::Exact(1));
        assert!(res.await.is_ok());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_get_user_do_not_care_seq() -> databend_common_exception::Result<()> {
        let test_user_name = "test";
        let test_hostname = "%";
        let test_key = format!(
            "__fd_users/tenant1/{}",
            escape_for_key(&format_user_key(test_user_name, test_hostname))?
        );

        let user_info = UserInfo::new(test_user_name, test_hostname, default_test_auth_info());
        let value = serialize_struct(&user_info, ErrorCode::IllegalUserInfoFormat, || "")?;

        let mut kv = MockKV::new();
        kv.expect_get_kv()
            .with(predicate::function(move |v| v == test_key.as_str()))
            .times(1)
            .return_once(move |_k| Ok(Some(SeqV::new(100, value))));

        let kv = Arc::new(kv);
        let user_mgr = UserMgr::create(kv, &Tenant::new_literal("tenant1"));
        let res = user_mgr.get_user(user_info.identity(), MatchSeq::GE(0));
        assert!(res.await.is_ok());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_get_user_not_exist() -> databend_common_exception::Result<()> {
        let test_user_name = "test";
        let test_hostname = "%";
        let test_key = format!(
            "__fd_users/tenant1/{}",
            escape_for_key(&format_user_key(test_user_name, test_hostname))?
        );

        let mut kv = MockKV::new();
        kv.expect_get_kv()
            .with(predicate::function(move |v| v == test_key.as_str()))
            .times(1)
            .return_once(move |_k| Ok(None));

        let kv = Arc::new(kv);
        let user_mgr = UserMgr::create(kv, &Tenant::new_literal("tenant1"));
        let res = user_mgr
            .get_user(UserIdentity::new(test_user_name), MatchSeq::GE(0))
            .await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().code(), ErrorCode::UNKNOWN_USER);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_get_user_not_exist_seq_mismatch() -> databend_common_exception::Result<()> {
        let test_user_name = "test";
        let test_hostname = "%";
        let test_key = format!(
            "__fd_users/tenant1/{}",
            escape_for_key(&format_user_key(test_user_name, test_hostname))?
        );

        let mut kv = MockKV::new();
        kv.expect_get_kv()
            .with(predicate::function(move |v| v == test_key.as_str()))
            .times(1)
            .return_once(move |_k| Ok(Some(SeqV::new(1, vec![]))));

        let kv = Arc::new(kv);
        let user_mgr = UserMgr::create(kv, &Tenant::new_literal("tenant1"));
        let res = user_mgr
            .get_user(UserIdentity::new(test_user_name), MatchSeq::Exact(2))
            .await;
        assert!(res.is_err());
        assert_eq!(res.unwrap_err().code(), ErrorCode::UNKNOWN_USER);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_get_user_invalid_user_info_encoding() -> databend_common_exception::Result<()> {
        let test_user_name = "test";
        let test_hostname = "%";
        let test_key = format!(
            "__fd_users/tenant1/{}",
            escape_for_key(&format_user_key(test_user_name, test_hostname))?
        );

        let mut kv = MockKV::new();
        kv.expect_get_kv()
            .with(predicate::function(move |v| v == test_key.as_str()))
            .times(1)
            .return_once(move |_k| Ok(Some(SeqV::new(1, vec![]))));

        let kv = Arc::new(kv);
        let user_mgr = UserMgr::create(kv, &Tenant::new_literal("tenant1"));
        let res = user_mgr.get_user(UserIdentity::new(test_user_name), MatchSeq::GE(0));
        assert_eq!(
            res.await.unwrap_err().code(),
            ErrorCode::ILLEGAL_USER_INFO_FORMAT
        );

        Ok(())
    }
}

mod get_users {
    use databend_common_meta_app::principal::UserInfo;
    use databend_common_meta_app::tenant::Tenant;

    use super::*;

    type FakeKeys = Vec<(String, SeqV<Vec<u8>>)>;
    type UserInfos = Vec<SeqV<UserInfo>>;

    fn prepare() -> databend_common_exception::Result<(FakeKeys, UserInfos)> {
        let mut names = vec![];
        let mut hostnames = vec![];
        let mut keys = vec![];
        let mut res = vec![];
        let mut user_infos = vec![];

        for i in 0..9 {
            let name = format!("test_user_{}", i);
            names.push(name.clone());
            let hostname = format!("test_hostname_{}", i);
            hostnames.push(hostname.clone());

            let key = format!("tenant1/{}", format_user_key(&name, &hostname));
            keys.push(key);

            let user_info = UserInfo::new(&name, &hostname, default_test_auth_info());
            res.push((
                "fake_key".to_string(),
                SeqV::new(
                    i,
                    serialize_struct(&user_info, ErrorCode::IllegalUserInfoFormat, || "")?,
                ),
            ));
            user_infos.push(SeqV::new(i, user_info));
        }
        Ok((res, user_infos))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_get_users_normal() -> databend_common_exception::Result<()> {
        let (res, user_infos) = prepare()?;
        let mut kv = MockKV::new();
        {
            let k = "__fd_users/tenant1/";
            kv.expect_prefix_list_kv()
                .with(predicate::eq(k))
                .times(1)
                .return_once(|_p| Ok(res));
        }

        let kv = Arc::new(kv);
        let user_mgr = UserMgr::create(kv, &Tenant::new_literal("tenant1"));
        let res = user_mgr.get_users();
        assert_eq!(res.await?, user_infos);

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_get_all_users_invalid_user_info_encoding() -> databend_common_exception::Result<()>
    {
        let (mut res, _user_infos) = prepare()?;
        res.insert(
            8,
            (
                "fake_key".to_string(),
                SeqV::new(0, b"some arbitrary str".to_vec()),
            ),
        );

        let mut kv = MockKV::new();
        {
            let k = "__fd_users/tenant1/";
            kv.expect_prefix_list_kv()
                .with(predicate::eq(k))
                .times(1)
                .return_once(|_p| Ok(res));
        }

        let kv = Arc::new(kv);
        let user_mgr = UserMgr::create(kv, &Tenant::new_literal("tenant1"));
        let res = user_mgr.get_users();
        assert_eq!(
            res.await.unwrap_err().code(),
            ErrorCode::ILLEGAL_USER_INFO_FORMAT
        );

        Ok(())
    }
}

mod drop {
    use databend_common_meta_app::tenant::Tenant;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_drop_user_normal_case() -> databend_common_exception::Result<()> {
        let mut kv = MockKV::new();
        let test_user = "test";
        let test_hostname = "%";
        let test_key = format!(
            "__fd_users/tenant1/{}",
            escape_for_key(&format_user_key(test_user, test_hostname))?
        );
        kv.expect_upsert_kv()
            .with(predicate::eq(UpsertKVReq::new(
                &test_key,
                MatchSeq::GE(1),
                Operation::Delete,
                None,
            )))
            .times(1)
            .returning(|_k| Ok(UpsertKVReply::new(Some(SeqV::new(1, vec![])), None)));
        let kv = Arc::new(kv);
        let user_mgr = UserMgr::create(kv, &Tenant::new_literal("tenant1"));
        let res = user_mgr.drop_user(UserIdentity::new(test_user), MatchSeq::GE(1));
        assert!(res.await.is_ok());

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_drop_user_unknown() -> databend_common_exception::Result<()> {
        let mut kv = MockKV::new();
        let test_user = "test";
        let test_hostname = "%";
        let test_key = format!(
            "__fd_users/tenant1/{}",
            escape_for_key(&format_user_key(test_user, test_hostname))?
        );
        kv.expect_upsert_kv()
            .with(predicate::eq(UpsertKVReq::new(
                &test_key,
                MatchSeq::GE(1),
                Operation::Delete,
                None,
            )))
            .times(1)
            .returning(|_k| Ok(UpsertKVReply::new(None, None)));
        let kv = Arc::new(kv);
        let user_mgr = UserMgr::create(kv, &Tenant::new_literal("tenant1"));
        let res = user_mgr.drop_user(UserIdentity::new(test_user), MatchSeq::GE(1));
        assert_eq!(res.await.unwrap_err().code(), ErrorCode::UNKNOWN_USER);
        Ok(())
    }
}

mod update {
    use databend_common_meta_app::principal::AuthInfo;
    use databend_common_meta_app::principal::UserInfo;
    use databend_common_meta_app::tenant::Tenant;

    use super::*;

    fn new_test_auth_info(full: bool) -> AuthInfo {
        AuthInfo::Password {
            hash_value: Vec::from("test_password_new"),
            hash_method: if full {
                PasswordHashMethod::Sha256
            } else {
                PasswordHashMethod::DoubleSha1
            },
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_update_user_normal_update_full() -> databend_common_exception::Result<()> {
        test_update_user_normal(true).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_update_user_normal_update_partial() -> databend_common_exception::Result<()> {
        test_update_user_normal(false).await
    }

    async fn test_update_user_normal(full: bool) -> databend_common_exception::Result<()> {
        let test_user_name = "name";
        let test_hostname = "%";

        let test_key = format!(
            "__fd_users/tenant1/{}",
            escape_for_key(&format_user_key(test_user_name, test_hostname))?
        );
        let test_seq = MatchSeq::GE(1);

        let user_info = UserInfo::new(test_user_name, test_hostname, default_test_auth_info());
        let prev_value = serialize_struct(&user_info, ErrorCode::IllegalUserInfoFormat, || "")?;

        // get_kv should be called
        let mut kv = MockKV::new();
        {
            let test_key = test_key.clone();
            kv.expect_get_kv()
                .with(predicate::function(move |v| v == test_key.as_str()))
                .times(1)
                .return_once(move |_k| Ok(Some(SeqV::new(1, prev_value))));
        }

        // and then, update_kv should be called
        let mut new_user_info = user_info.clone();
        new_user_info.auth_info = new_test_auth_info(full);
        let new_value_with_old_salt =
            serialize_struct(&new_user_info, ErrorCode::IllegalUserInfoFormat, || "")?;

        kv.expect_upsert_kv()
            .with(predicate::eq(UpsertKVReq::new(
                &test_key,
                MatchSeq::Exact(1),
                Operation::Update(new_value_with_old_salt.clone()),
                None,
            )))
            .times(1)
            .return_once(|_| Ok(UpsertKVReply::new(None, Some(SeqV::new(1, vec![])))));

        let kv = Arc::new(kv);
        let user_mgr = UserMgr::create(kv, &Tenant::new_literal("tenant1"));

        let res = user_mgr.update_user_with(user_info.identity(), test_seq, |ui: &mut UserInfo| {
            ui.update_auth_option(Some(new_test_auth_info(full)), None)
        });

        assert!(res.await.is_ok());
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_update_user_with_conflict_when_writing_back()
    -> databend_common_exception::Result<()> {
        let test_user_name = "name";
        let test_hostname = "%";
        let test_key = format!(
            "__fd_users/tenant1/{}",
            escape_for_key(&format_user_key(test_user_name, test_hostname))?
        );

        // if partial update, and get_kv returns None
        // update_kv should NOT be called
        let mut kv = MockKV::new();
        kv.expect_get_kv()
            .with(predicate::function(move |v| v == test_key.as_str()))
            .times(1)
            .return_once(move |_k| Ok(None));

        let kv = Arc::new(kv);
        let user_mgr = UserMgr::create(kv, &Tenant::new_literal("tenant1"));

        let res = user_mgr.update_user_with(
            UserIdentity::new(test_user_name),
            MatchSeq::GE(0),
            |ui: &mut UserInfo| ui.update_auth_option(Some(new_test_auth_info(false)), None),
        );
        assert_eq!(res.await.unwrap_err().code(), ErrorCode::UNKNOWN_USER);
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_update_user_with_complete() -> databend_common_exception::Result<()> {
        let test_user_name = "name";
        let test_hostname = "%";
        let test_key = format!(
            "__fd_users/tenant1/{}",
            escape_for_key(&format_user_key(test_user_name, test_hostname))?
        );

        let user_info = UserInfo::new(test_user_name, test_hostname, default_test_auth_info());
        let prev_value = serialize_struct(&user_info, ErrorCode::IllegalUserInfoFormat, || "")?;

        // - get_kv should be called
        let mut kv = MockKV::new();
        {
            let test_key = test_key.clone();
            kv.expect_get_kv()
                .with(predicate::function(move |v| v == test_key.as_str()))
                .times(1)
                .return_once(move |_k| Ok(Some(SeqV::new(2, prev_value))));
        }

        // upsert should be called
        kv.expect_upsert_kv()
            .with(predicate::function(move |act: &UpsertKVReq| {
                act.key == test_key.as_str() && act.seq == MatchSeq::Exact(2)
            }))
            .times(1)
            .returning(|_| Ok(UpsertKVReply::new(None, None)));

        let kv = Arc::new(kv);
        let user_mgr = UserMgr::create(kv, &Tenant::new_literal("tenant1"));

        let _ = user_mgr
            .update_user_with(user_info.identity(), MatchSeq::GE(1), |_x| {})
            .await;
        Ok(())
    }
}

mod set_user_privileges {
    use databend_common_meta_app::principal::GrantObject;
    use databend_common_meta_app::principal::UserInfo;
    use databend_common_meta_app::principal::UserPrivilegeSet;
    use databend_common_meta_app::principal::UserPrivilegeType;
    use databend_common_meta_app::tenant::Tenant;

    use super::*;

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_grant_user_privileges() -> databend_common_exception::Result<()> {
        let test_user_name = "name";
        let test_hostname = "%";
        let test_key = format!(
            "__fd_users/tenant1/{}",
            escape_for_key(&format_user_key(test_user_name, test_hostname))?
        );

        let mut user_info = UserInfo::new(test_user_name, test_hostname, default_test_auth_info());
        let prev_value = serialize_struct(&user_info, ErrorCode::IllegalUserInfoFormat, || "")?;

        // - get_kv should be called
        let mut kv = MockKV::new();
        {
            let test_key = test_key.clone();
            kv.expect_get_kv()
                .with(predicate::function(move |v| v == test_key.as_str()))
                .times(1)
                .return_once(move |_k| Ok(Some(SeqV::new(1, prev_value))));
        }
        // - update_kv should be called
        let mut privileges = UserPrivilegeSet::empty();
        privileges.set_privilege(UserPrivilegeType::Select);
        user_info
            .grants
            .grant_privileges(&GrantObject::Global, privileges);
        let new_value = serialize_struct(&user_info, ErrorCode::IllegalUserInfoFormat, || "")?;

        kv.expect_upsert_kv()
            .with(predicate::eq(UpsertKVReq::new(
                &test_key,
                MatchSeq::Exact(1),
                Operation::Update(new_value),
                None,
            )))
            .times(1)
            .return_once(|_| Ok(UpsertKVReply::new(None, Some(SeqV::new(1, vec![])))));

        let kv = Arc::new(kv);
        let user_mgr = UserMgr::create(kv, &Tenant::new_literal("tenant1"));

        let res = user_mgr.update_user_with(
            user_info.identity(),
            MatchSeq::GE(1),
            |ui: &mut UserInfo| ui.grants.grant_privileges(&GrantObject::Global, privileges),
        );
        assert!(res.await.is_ok());
        Ok(())
    }
}
