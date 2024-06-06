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

use std::collections::HashSet;

use chrono::DateTime;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use chrono::NaiveTime;
use chrono::TimeZone;
use chrono::Utc;
use databend_common_meta_app as mt;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_meta_app::storage::StorageCosConfig;
use databend_common_meta_app::storage::StorageFsConfig;
use databend_common_meta_app::storage::StorageGcsConfig;
use databend_common_meta_app::storage::StorageObsConfig;
use databend_common_meta_app::storage::StorageOssConfig;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_app::storage::StorageS3Config;
use databend_common_meta_app::storage::StorageWebhdfsConfig;
use databend_common_proto_conv::FromToProto;
use databend_common_proto_conv::Incompatible;
use databend_common_proto_conv::VER;
use databend_common_protos::pb;
use enumflags2::make_bitflags;
use pretty_assertions::assert_eq;

use crate::common::print_err;

fn test_user_info() -> mt::principal::UserInfo {
    let option = mt::principal::UserOption::default()
        .with_set_flag(mt::principal::UserOptionFlag::TenantSetting)
        .with_default_role(Some("role1".into()))
        .with_disabled(None);

    mt::principal::UserInfo {
        name: "test_user".to_string(),
        hostname: "localhost".to_string(),
        auth_info: mt::principal::AuthInfo::Password {
            hash_value: [
                116, 101, 115, 116, 95, 112, 97, 115, 115, 119, 111, 114, 100,
            ]
            .to_vec(),
            hash_method: mt::principal::PasswordHashMethod::DoubleSha1,
        },
        grants: mt::principal::UserGrantSet::new(
            vec![mt::principal::GrantEntry::new(
                mt::principal::GrantObject::Global,
                make_bitflags!(UserPrivilegeType::{Create}),
            )],
            HashSet::new(),
        ),
        quota: mt::principal::UserQuota {
            max_cpu: 10,
            max_memory_in_bytes: 10240,
            max_storage_in_bytes: 20480,
        },
        option,
        history_auth_infos: vec![],
        password_fails: vec![],
        password_update_on: None,
        lockout_time: None,
        created_on: DateTime::<Utc>::default(),
        update_on: DateTime::<Utc>::default(),
    }
}

pub(crate) fn test_fs_stage_info() -> mt::principal::StageInfo {
    mt::principal::StageInfo {
        stage_name: "fs://dir/to/files".to_string(),
        stage_type: mt::principal::StageType::LegacyInternal,
        stage_params: mt::principal::StageParams {
            storage: mt::storage::StorageParams::Fs(mt::storage::StorageFsConfig {
                root: "/dir/to/files".to_string(),
            }),
        },
        is_temporary: false,
        file_format_params: mt::principal::FileFormatParams::Json(
            mt::principal::JsonFileFormatParams {
                compression: mt::principal::StageFileCompression::Bz2,
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::AbortNum(2),
            size_limit: 1038,
            max_files: 0,
            split_size: 0,
            purge: true,
            single: false,
            max_file_size: 0,
            disable_variant_check: false,
            return_failed_only: false,
            detailed_output: false,
        },
        comment: "test".to_string(),

        number_of_files: 100,
        creator: Some(UserIdentity {
            username: "databend".to_string(),
            hostname: "databend.rs".to_string(),
        }),
        created_on: Utc::now(),
    }
}

pub(crate) fn test_s3_stage_info() -> mt::principal::StageInfo {
    mt::principal::StageInfo {
        stage_name: "s3://mybucket/data/files".to_string(),
        stage_type: mt::principal::StageType::External,
        stage_params: mt::principal::StageParams {
            storage: StorageParams::S3(StorageS3Config {
                bucket: "mybucket".to_string(),
                root: "/data/files".to_string(),
                access_key_id: "my_key_id".to_string(),
                secret_access_key: "my_secret_key".to_string(),
                security_token: "my_security_token".to_string(),
                master_key: "my_master_key".to_string(),
                ..Default::default()
            }),
        },
        is_temporary: false,
        file_format_params: mt::principal::FileFormatParams::Json(
            mt::principal::JsonFileFormatParams {
                compression: mt::principal::StageFileCompression::Bz2,
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::SkipFileNum(666),
            size_limit: 1038,
            max_files: 0,
            split_size: 0,
            purge: true,
            single: false,
            max_file_size: 0,
            disable_variant_check: false,
            return_failed_only: false,
            detailed_output: false,
        },
        comment: "test".to_string(),
        ..Default::default()
    }
}

pub(crate) fn test_s3_stage_info_v16() -> mt::principal::StageInfo {
    mt::principal::StageInfo {
        stage_name: "s3://mybucket/data/files".to_string(),
        stage_type: mt::principal::StageType::External,
        stage_params: mt::principal::StageParams {
            storage: StorageParams::S3(StorageS3Config {
                bucket: "mybucket".to_string(),
                root: "/data/files".to_string(),
                access_key_id: "my_key_id".to_string(),
                secret_access_key: "my_secret_key".to_string(),
                security_token: "my_security_token".to_string(),
                master_key: "my_master_key".to_string(),
                role_arn: "aws::iam::xxx".to_string(),
                external_id: "hello,world".to_string(),
                ..Default::default()
            }),
        },
        is_temporary: false,
        file_format_params: mt::principal::FileFormatParams::Json(
            mt::principal::JsonFileFormatParams {
                compression: mt::principal::StageFileCompression::Bz2,
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::SkipFileNum(666),
            size_limit: 1038,
            max_files: 0,
            split_size: 1024,
            purge: true,
            single: false,
            max_file_size: 0,
            disable_variant_check: false,
            return_failed_only: false,
            detailed_output: false,
        },
        comment: "test".to_string(),
        ..Default::default()
    }
}

pub(crate) fn test_s3_stage_info_v14() -> mt::principal::StageInfo {
    mt::principal::StageInfo {
        stage_name: "s3://mybucket/data/files".to_string(),
        stage_type: mt::principal::StageType::External,
        stage_params: mt::principal::StageParams {
            storage: StorageParams::S3(StorageS3Config {
                bucket: "mybucket".to_string(),
                root: "/data/files".to_string(),
                access_key_id: "my_key_id".to_string(),
                secret_access_key: "my_secret_key".to_string(),
                security_token: "my_security_token".to_string(),
                master_key: "my_master_key".to_string(),
                role_arn: "aws::iam::xxx".to_string(),
                external_id: "hello,world".to_string(),
                ..Default::default()
            }),
        },
        is_temporary: false,
        file_format_params: mt::principal::FileFormatParams::Json(
            mt::principal::JsonFileFormatParams {
                compression: mt::principal::StageFileCompression::Bz2,
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::SkipFileNum(666),
            size_limit: 1038,
            max_files: 0,
            split_size: 0,
            purge: true,
            single: false,
            max_file_size: 0,
            disable_variant_check: false,
            return_failed_only: false,
            detailed_output: false,
        },
        comment: "test".to_string(),
        ..Default::default()
    }
}

// Version 4 added Google Cloud Storage as a stage backend, should be tested
pub(crate) fn test_gcs_stage_info() -> mt::principal::StageInfo {
    mt::principal::StageInfo {
        stage_name: "gcs://my_bucket/data/files".to_string(),
        stage_type: mt::principal::StageType::External,
        stage_params: mt::principal::StageParams {
            storage: StorageParams::Gcs(StorageGcsConfig {
                endpoint_url: "https://storage.googleapis.com".to_string(),
                bucket: "my_bucket".to_string(),
                root: "/data/files".to_string(),
                credential: "my_credential".to_string(),
            }),
        },
        is_temporary: false,
        file_format_params: mt::principal::FileFormatParams::Json(
            mt::principal::JsonFileFormatParams {
                compression: mt::principal::StageFileCompression::Bz2,
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::SkipFileNum(666),
            size_limit: 1038,
            max_files: 0,
            split_size: 0,
            purge: true,
            single: false,
            max_file_size: 0,
            disable_variant_check: false,
            return_failed_only: false,
            detailed_output: false,
        },
        comment: "test".to_string(),
        ..Default::default()
    }
}

// Version 13 added OSS as a stage backend, should be tested
pub(crate) fn test_oss_stage_info() -> mt::principal::StageInfo {
    mt::principal::StageInfo {
        stage_name: "oss://my_bucket/data/files".to_string(),
        stage_type: mt::principal::StageType::External,
        stage_params: mt::principal::StageParams {
            storage: StorageParams::Oss(StorageOssConfig {
                endpoint_url: "https://oss-cn-litang.example.com".to_string(),
                bucket: "my_bucket".to_string(),
                root: "/data/files".to_string(),
                server_side_encryption: "".to_string(),
                access_key_id: "access_key_id".to_string(),
                access_key_secret: "access_key_secret".to_string(),
                presign_endpoint_url: "".to_string(),
                server_side_encryption_key_id: "".to_string(),
            }),
        },
        is_temporary: false,
        file_format_params: mt::principal::FileFormatParams::Json(
            mt::principal::JsonFileFormatParams {
                compression: mt::principal::StageFileCompression::Bz2,
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::SkipFileNum(666),
            size_limit: 1038,
            max_files: 0,
            split_size: 0,
            purge: true,
            single: false,
            max_file_size: 0,
            disable_variant_check: false,
            return_failed_only: false,
            detailed_output: false,
        },
        comment: "test".to_string(),
        ..Default::default()
    }
}

// version 29 added WebHDFS as a stage backend, should be tested
pub(crate) fn test_webhdfs_stage_info() -> mt::principal::StageInfo {
    mt::principal::StageInfo {
        stage_name: "webhdfs://path/to/stage/files".to_string(),
        stage_type: mt::principal::StageType::External,
        stage_params: mt::principal::StageParams {
            storage: StorageParams::Webhdfs(StorageWebhdfsConfig {
                endpoint_url: "https://webhdfs.example.com".to_string(),
                root: "/path/to/stage/files".to_string(),
                delegation: "<delegation_token>".to_string(),
            }),
        },
        is_temporary: false,
        file_format_params: mt::principal::FileFormatParams::Json(
            mt::principal::JsonFileFormatParams {
                compression: mt::principal::StageFileCompression::Bz2,
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::SkipFileNum(3141),
            size_limit: 1038,
            max_files: 0,
            split_size: 0,
            purge: true,
            single: false,
            max_file_size: 0,
            disable_variant_check: false,
            return_failed_only: false,
            detailed_output: false,
        },
        comment: "test".to_string(),
        ..Default::default()
    }
}

pub(crate) fn test_obs_stage_info() -> mt::principal::StageInfo {
    mt::principal::StageInfo {
        stage_name: "obs://bucket/to/stage/files".to_string(),
        stage_type: mt::principal::StageType::External,
        stage_params: mt::principal::StageParams {
            storage: StorageParams::Obs(StorageObsConfig {
                endpoint_url: "https://obs.example.com".to_string(),
                root: "/path/to/stage/files".to_string(),
                access_key_id: "access_key_id".to_string(),
                secret_access_key: "secret_access_key".to_string(),
                bucket: "bucket".to_string(),
            }),
        },
        is_temporary: false,
        file_format_params: mt::principal::FileFormatParams::Json(
            mt::principal::JsonFileFormatParams {
                compression: mt::principal::StageFileCompression::Bz2,
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::SkipFileNum(3141),
            size_limit: 1038,
            max_files: 0,
            split_size: 0,
            purge: true,
            single: false,
            max_file_size: 0,
            disable_variant_check: false,
            return_failed_only: false,
            detailed_output: false,
        },
        comment: "test".to_string(),
        ..Default::default()
    }
}

pub(crate) fn test_cos_stage_info() -> mt::principal::StageInfo {
    mt::principal::StageInfo {
        stage_name: "cos://bucket/to/stage/files".to_string(),
        stage_type: mt::principal::StageType::External,
        stage_params: mt::principal::StageParams {
            storage: StorageParams::Cos(StorageCosConfig {
                endpoint_url: "https://cos.example.com".to_string(),
                root: "/path/to/stage/files".to_string(),
                secret_id: "secret_id".to_string(),
                secret_key: "secret_key".to_string(),
                bucket: "bucket".to_string(),
            }),
        },
        is_temporary: false,
        file_format_params: mt::principal::FileFormatParams::Json(
            mt::principal::JsonFileFormatParams {
                compression: mt::principal::StageFileCompression::Bz2,
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::SkipFileNum(3141),
            size_limit: 1038,
            max_files: 0,
            split_size: 0,
            purge: true,
            single: false,
            max_file_size: 0,
            disable_variant_check: false,
            return_failed_only: false,
            detailed_output: false,
        },
        comment: "test".to_string(),
        ..Default::default()
    }
}

pub(crate) fn test_stage_file() -> mt::principal::StageFile {
    let dt = NaiveDateTime::new(
        NaiveDate::from_ymd_opt(2022, 9, 16).unwrap(),
        NaiveTime::from_hms_opt(0, 1, 2).unwrap(),
    );
    let user_id = mt::principal::UserIdentity {
        username: "datafuselabs".to_string(),
        hostname: "datafuselabs.rs".to_string(),
    };
    mt::principal::StageFile {
        path: "/path/to/stage".to_string(),
        size: 233,
        md5: None,
        last_modified: Utc.from_utc_datetime(&dt),
        creator: Some(user_id),
        etag: None,
    }
}

#[test]
fn test_user_pb_from_to() -> anyhow::Result<()> {
    let test_user_info = test_user_info();
    let test_user_info_pb = test_user_info.to_pb()?;
    let got = mt::principal::UserInfo::from_pb(test_user_info_pb)?;
    assert_eq!(got, test_user_info);

    Ok(())
}

#[test]
fn test_stage_file_pb_from_to() -> anyhow::Result<()> {
    let test_stage_file = test_stage_file();
    let test_stage_file_pb = test_stage_file.to_pb()?;
    let got = mt::principal::StageFile::from_pb(test_stage_file_pb)?;
    assert_eq!(got, test_stage_file);

    Ok(())
}

#[test]
fn test_user_incompatible() -> anyhow::Result<()> {
    {
        let user_info = test_user_info();
        let mut p = user_info.to_pb()?;
        p.ver = VER + 1;
        p.min_reader_ver = VER + 1;

        let res = mt::principal::UserInfo::from_pb(p);
        assert_eq!(
            Incompatible {
                reason: format!(
                    "executable ver={} is smaller than the min reader version({}) that can read this message",
                    VER,
                    VER + 1
                )
            },
            res.unwrap_err()
        );
    }

    {
        let stage_file = test_stage_file();
        let mut p = stage_file.to_pb()?;
        p.ver = VER + 1;
        p.min_reader_ver = VER + 1;

        let res = mt::principal::StageFile::from_pb(p);
        assert_eq!(
            Incompatible {
                reason: format!(
                    "executable ver={} is smaller than the min reader version({}) that can read this message",
                    VER,
                    VER + 1
                )
            },
            res.unwrap_err()
        )
    }

    {
        let fs_stage_info = test_fs_stage_info();
        let mut p = fs_stage_info.to_pb()?;
        p.ver = VER + 1;
        p.min_reader_ver = VER + 1;

        let res = mt::principal::StageInfo::from_pb(p);
        assert_eq!(
            Incompatible {
                reason: format!(
                    "executable ver={} is smaller than the min reader version({}) that can read this message",
                    VER,
                    VER + 1
                )
            },
            res.unwrap_err()
        )
    }

    {
        let s3_stage_info = test_s3_stage_info();
        let mut p = s3_stage_info.to_pb()?;
        p.ver = VER + 1;
        p.min_reader_ver = VER + 1;

        let res = mt::principal::StageInfo::from_pb(p);
        assert_eq!(
            Incompatible {
                reason: format!(
                    "executable ver={} is smaller than the min reader version({}) that can read this message",
                    VER,
                    VER + 1
                )
            },
            res.unwrap_err()
        );
    }

    {
        let s3_stage_info = test_s3_stage_info_v14();
        let mut p = s3_stage_info.to_pb()?;
        p.ver = VER + 1;
        p.min_reader_ver = VER + 1;

        let res = mt::principal::StageInfo::from_pb(p);
        assert_eq!(
            Incompatible {
                reason: format!(
                    "executable ver={} is smaller than the min reader version({}) that can read this message",
                    VER,
                    VER + 1
                )
            },
            res.unwrap_err()
        );
    }

    {
        let gcs_stage_info = test_gcs_stage_info();
        let mut p = gcs_stage_info.to_pb()?;
        p.ver = VER + 1;
        p.min_reader_ver = VER + 1;

        let res = mt::principal::StageInfo::from_pb(p);
        assert_eq!(
            Incompatible {
                reason: format!(
                    "executable ver={} is smaller than the min reader version({}) that can read this message",
                    VER,
                    VER + 1
                )
            },
            res.unwrap_err()
        );
    }

    {
        let oss_stage_info = test_oss_stage_info();
        let mut p = oss_stage_info.to_pb()?;
        p.ver = VER + 1;
        p.min_reader_ver = VER + 1;

        let res = mt::principal::StageInfo::from_pb(p);
        assert_eq!(
            Incompatible {
                reason: format!(
                    "executable ver={} is smaller than the min reader version({}) that can read this message",
                    VER,
                    VER + 1
                )
            },
            res.unwrap_err()
        )
    }

    {
        let webhdfs_stage_info = test_webhdfs_stage_info();
        let mut p = webhdfs_stage_info.to_pb()?;
        p.ver = VER + 1;
        p.min_reader_ver = VER + 1;

        let res = mt::principal::StageInfo::from_pb(p);
        assert_eq!(
            Incompatible {
                reason: format!(
                    "executable ver={} is smaller than the min reader version({}) that can read this message",
                    VER,
                    VER + 1
                )
            },
            res.unwrap_err()
        )
    }

    Ok(())
}

#[test]
fn test_build_user_pb_buf() -> anyhow::Result<()> {
    // build serialized buf of protobuf data, for backward compatibility test with a new version binary.

    // UserInfo
    {
        let user_info = test_user_info();
        let p = user_info.to_pb()?;

        let mut buf = vec![];
        prost::Message::encode(&p, &mut buf)?;
        println!("user_info: {:?}", buf);
    }

    // StageFile
    {
        let stage_file = test_stage_file();
        let p = stage_file.to_pb()?;
        let mut buf = vec![];
        prost::Message::encode(&p, &mut buf)?;
        println!("stage_file: {:?}", buf);
    }

    // Stage on local file system
    {
        let fs_stage_info = test_fs_stage_info();

        let p = fs_stage_info.to_pb()?;

        let mut buf = vec![];
        prost::Message::encode(&p, &mut buf)?;
        println!("fs_stage_info: {:?}", buf);
    }

    // Stage on S3
    {
        let s3_stage_info = test_s3_stage_info();

        let p = s3_stage_info.to_pb()?;

        let mut buf = vec![];
        prost::Message::encode(&p, &mut buf)?;
        println!("s3_stage_info: {:?}", buf);
    }

    // Stage on S3 v16
    {
        let s3_stage_info = test_s3_stage_info_v16();

        let p = s3_stage_info.to_pb()?;

        let mut buf = vec![];
        prost::Message::encode(&p, &mut buf)?;
        println!("s3_stage_info_v16: {:?}", buf);
    }

    // Stage on S3 v14
    {
        let s3_stage_info = test_s3_stage_info_v14();

        let p = s3_stage_info.to_pb()?;

        let mut buf = vec![];
        prost::Message::encode(&p, &mut buf)?;
        println!("s3_stage_info_v14: {:?}", buf);
    }

    // Stage on GCS, supported in version >=4.
    {
        let gcs_stage_info = test_gcs_stage_info();
        let p = gcs_stage_info.to_pb()?;
        let mut buf = vec![];
        prost::Message::encode(&p, &mut buf)?;
        println!("gcs_stage_info: {:?}", buf);
    }

    // Stage on OSS, supported in version >= 13.
    {
        let oss_stage_info = test_oss_stage_info();
        let p = oss_stage_info.to_pb()?;
        let mut buf = vec![];
        prost::Message::encode(&p, &mut buf)?;
        println!("oss_stage_info: {:?}", buf);
    }

    Ok(())
}

#[test]
fn test_load_old_user() -> anyhow::Result<()> {
    // built with `test_build_user_pb_buf()`

    {
        // User information generated by test_build_user_pb_buf()
        let user_info_v4: Vec<u8> = vec![
            10, 9, 116, 101, 115, 116, 95, 117, 115, 101, 114, 18, 9, 108, 111, 99, 97, 108, 104,
            111, 115, 116, 26, 25, 18, 17, 10, 13, 116, 101, 115, 116, 95, 112, 97, 115, 115, 119,
            111, 114, 100, 16, 1, 160, 6, 4, 168, 6, 1, 34, 26, 10, 18, 10, 8, 10, 0, 160, 6, 4,
            168, 6, 1, 16, 2, 160, 6, 4, 168, 6, 1, 160, 6, 4, 168, 6, 1, 42, 15, 8, 10, 16, 128,
            80, 24, 128, 160, 1, 160, 6, 4, 168, 6, 1, 50, 15, 8, 1, 18, 5, 114, 111, 108, 101, 49,
            160, 6, 4, 168, 6, 1, 160, 6, 4, 168, 6, 1,
        ];
        let p: pb::UserInfo = prost::Message::decode(user_info_v4.as_slice()).map_err(print_err)?;
        let got = mt::principal::UserInfo::from_pb(p).map_err(print_err)?;
        let want = test_user_info();

        assert_eq!(want, got);
    }

    // UserInfo is loadable
    {
        let user_info_v1: Vec<u8> = vec![
            10, 9, 116, 101, 115, 116, 95, 117, 115, 101, 114, 18, 9, 108, 111, 99, 97, 108, 104,
            111, 115, 116, 26, 22, 18, 17, 10, 13, 116, 101, 115, 116, 95, 112, 97, 115, 115, 119,
            111, 114, 100, 16, 1, 160, 6, 1, 34, 17, 10, 12, 10, 5, 10, 0, 160, 6, 1, 16, 2, 160,
            6, 1, 160, 6, 1, 42, 12, 8, 10, 16, 128, 80, 24, 128, 160, 1, 160, 6, 1, 50, 5, 8, 1,
            160, 6, 1, 160, 6, 1,
        ];
        let p: pb::UserInfo = prost::Message::decode(user_info_v1.as_slice()).map_err(print_err)?;
        let got = mt::principal::UserInfo::from_pb(p).map_err(print_err)?;
        assert_eq!(got.name, "test_user".to_string());
        assert_eq!(got.option.default_role().clone(), None);
        assert_eq!(got.option.disabled().clone(), None);
    }

    {
        let user_info_v3: Vec<u8> = vec![
            10, 9, 116, 101, 115, 116, 95, 117, 115, 101, 114, 18, 9, 108, 111, 99, 97, 108, 104,
            111, 115, 116, 26, 25, 18, 17, 10, 13, 116, 101, 115, 116, 95, 112, 97, 115, 115, 119,
            111, 114, 100, 16, 1, 160, 6, 3, 168, 6, 1, 34, 26, 10, 18, 10, 8, 10, 0, 160, 6, 3,
            168, 6, 1, 16, 2, 160, 6, 3, 168, 6, 1, 160, 6, 3, 168, 6, 1, 42, 15, 8, 10, 16, 128,
            80, 24, 128, 160, 1, 160, 6, 3, 168, 6, 1, 50, 15, 8, 1, 18, 5, 114, 111, 108, 101, 49,
            160, 6, 3, 168, 6, 1, 160, 6, 3, 168, 6, 1,
        ];
        let p: pb::UserInfo = prost::Message::decode(user_info_v3.as_slice()).map_err(print_err)?;
        let got = mt::principal::UserInfo::from_pb(p).map_err(print_err)?;
        let want = test_user_info();
        assert_eq!(want, got);
    }

    {
        // a legacy UserInfo with ConfigReload flag set, running on S3 service
        let user_info_v3: Vec<u8> = vec![
            10, 9, 116, 101, 115, 116, 95, 117, 115, 101, 114, 18, 9, 108, 111, 99, 97, 108, 104,
            111, 115, 116, 26, 25, 18, 17, 10, 13, 116, 101, 115, 116, 95, 112, 97, 115, 115, 119,
            111, 114, 100, 16, 1, 160, 6, 3, 168, 6, 1, 34, 26, 10, 18, 10, 8, 10, 0, 160, 6, 3,
            168, 6, 1, 16, 2, 160, 6, 3, 168, 6, 1, 160, 6, 3, 168, 6, 1, 42, 15, 8, 10, 16, 128,
            80, 24, 128, 160, 1, 160, 6, 3, 168, 6, 1, 50, 15, 8, 2, 18, 5, 114, 111, 108, 101, 49,
            160, 6, 3, 168, 6, 1, 160, 6, 3, 168, 6, 1,
        ];
        let p: pb::UserInfo = prost::Message::decode(user_info_v3.as_slice()).map_err(print_err)?;
        let got = mt::principal::UserInfo::from_pb(p).map_err(print_err)?;
        assert!(got.option.flags().is_empty());
    }

    {
        // a legacy UserInfo with NetworkPolicy
        let user_info_v5: Vec<u8> = vec![
            10, 9, 116, 101, 115, 116, 95, 117, 115, 101, 114, 18, 9, 108, 111, 99, 97, 108, 104,
            111, 115, 116, 26, 25, 18, 17, 10, 13, 116, 101, 115, 116, 95, 112, 97, 115, 115, 119,
            111, 114, 100, 16, 1, 160, 6, 49, 168, 6, 24, 34, 26, 10, 18, 10, 8, 10, 0, 160, 6, 49,
            168, 6, 24, 16, 2, 160, 6, 49, 168, 6, 24, 160, 6, 49, 168, 6, 24, 42, 15, 8, 10, 16,
            128, 80, 24, 128, 160, 1, 160, 6, 49, 168, 6, 24, 50, 25, 8, 1, 18, 5, 114, 111, 108,
            101, 49, 26, 8, 109, 121, 112, 111, 108, 105, 99, 121, 160, 6, 49, 168, 6, 24, 160, 6,
            49, 168, 6, 24,
        ];

        let p: pb::UserInfo = prost::Message::decode(user_info_v5.as_slice()).map_err(print_err)?;
        let got = mt::principal::UserInfo::from_pb(p).map_err(print_err)?;
        let mut want = test_user_info();
        want.option = want
            .option
            .with_network_policy(Some("mypolicy".to_string()));
        assert_eq!(want, got);
    }

    Ok(())
}

#[test]
fn test_old_stage_file() -> anyhow::Result<()> {
    // Encoded data of version 7 of StageFile:
    // Generated with `test_build_user_pb_buf`
    {
        let stage_file_v7 = vec![
            10, 14, 47, 112, 97, 116, 104, 47, 116, 111, 47, 115, 116, 97, 103, 101, 16, 233, 1,
            34, 23, 50, 48, 50, 50, 45, 48, 57, 45, 49, 54, 32, 48, 48, 58, 48, 49, 58, 48, 50, 32,
            85, 84, 67, 42, 37, 10, 12, 100, 97, 116, 97, 102, 117, 115, 101, 108, 97, 98, 115, 18,
            15, 100, 97, 116, 97, 102, 117, 115, 101, 108, 97, 98, 115, 46, 114, 115, 160, 6, 8,
            168, 6, 1, 160, 6, 8, 168, 6, 1,
        ];
        let p: pb::StageFile =
            prost::Message::decode(stage_file_v7.as_slice()).map_err(print_err)?;
        let got = mt::principal::StageFile::from_pb(p).map_err(print_err)?;

        let dt = NaiveDateTime::new(
            NaiveDate::from_ymd_opt(2022, 9, 16).unwrap(),
            NaiveTime::from_hms_opt(0, 1, 2).unwrap(),
        );
        let user_id = mt::principal::UserIdentity {
            username: "datafuselabs".to_string(),
            hostname: "datafuselabs.rs".to_string(),
        };
        let want = mt::principal::StageFile {
            path: "/path/to/stage".to_string(),
            size: 233,
            md5: None,
            last_modified: Utc.from_utc_datetime(&dt),
            creator: Some(user_id),
            ..Default::default()
        };

        assert_eq!(got, want);
    }

    Ok(())
}

pub(crate) fn test_internal_stage_info_v17() -> mt::principal::StageInfo {
    mt::principal::StageInfo {
        stage_name: "fs://dir/to/files".to_string(),
        stage_type: mt::principal::StageType::Internal,
        stage_params: mt::principal::StageParams {
            storage: StorageParams::Fs(StorageFsConfig {
                root: "/dir/to/files".to_string(),
            }),
        },
        is_temporary: false,
        file_format_params: mt::principal::FileFormatParams::Json(
            mt::principal::JsonFileFormatParams {
                compression: mt::principal::StageFileCompression::Bz2,
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::SkipFileNum(666),
            size_limit: 1038,
            max_files: 0,
            split_size: 0,
            purge: true,
            single: false,
            max_file_size: 0,
            disable_variant_check: false,
            return_failed_only: false,
            detailed_output: false,
        },
        comment: "test".to_string(),
        ..Default::default()
    }
}

pub(crate) fn test_stage_info_v18() -> mt::principal::StageInfo {
    mt::principal::StageInfo {
        stage_name: "root".to_string(),
        stage_type: mt::principal::StageType::User,
        stage_params: mt::principal::StageParams {
            storage: StorageParams::Fs(StorageFsConfig {
                root: "/dir/to/files".to_string(),
            }),
        },
        is_temporary: false,
        file_format_params: mt::principal::FileFormatParams::Json(
            mt::principal::JsonFileFormatParams {
                compression: mt::principal::StageFileCompression::Bz2,
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::SkipFileNum(666),
            size_limit: 1038,
            max_files: 0,
            split_size: 0,
            purge: true,
            single: false,
            max_file_size: 0,
            disable_variant_check: false,
            return_failed_only: false,
            detailed_output: false,
        },
        comment: "test".to_string(),
        ..Default::default()
    }
}
