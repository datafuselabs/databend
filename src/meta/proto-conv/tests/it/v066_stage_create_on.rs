// Copyright 2023 Datafuse Labs.
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

use chrono::DateTime;
use chrono::Utc;
use databend_common_meta_app as mt;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_app::storage::StorageS3Config;
use minitrace::func_name;

use crate::common;

// These bytes are built when a new version in introduced,
// and are kept for backward compatibility test.
//
// *************************************************************
// * These messages should never be updated,                   *
// * only be added when a new version is added,                *
// * or be removed when an old version is no longer supported. *
// *************************************************************
#[test]
fn test_decode_v66_stage() -> anyhow::Result<()> {
    let stage_info_v66 = vec![
        10, 10, 115, 116, 97, 103, 101, 95, 110, 97, 109, 101, 16, 2, 26, 50, 10, 48, 10, 46, 10,
        4, 116, 101, 115, 116, 18, 24, 104, 116, 116, 112, 115, 58, 47, 47, 115, 51, 46, 97, 109,
        97, 122, 111, 110, 97, 119, 115, 46, 99, 111, 109, 42, 4, 116, 101, 115, 116, 104, 1, 160,
        6, 66, 168, 6, 24, 42, 11, 10, 2, 48, 2, 16, 231, 7, 24, 1, 56, 1, 50, 3, 99, 99, 99, 56,
        100, 66, 19, 10, 8, 100, 97, 116, 97, 98, 101, 110, 100, 18, 1, 37, 160, 6, 66, 168, 6, 24,
        74, 8, 10, 6, 160, 6, 66, 168, 6, 24, 82, 23, 50, 48, 50, 51, 45, 49, 50, 45, 49, 53, 32,
        48, 49, 58, 50, 54, 58, 48, 57, 32, 85, 84, 67, 160, 6, 66, 168, 6, 24,
    ];

    let want = || mt::principal::StageInfo {
        stage_name: "stage_name".to_string(),
        stage_type: mt::principal::StageType::Internal,
        stage_params: mt::principal::StageParams {
            storage: StorageParams::S3(StorageS3Config {
                bucket: "test".to_string(),
                region: "test".to_string(),
                ..Default::default()
            }),
        },
        is_temporary: false,
        file_format_params: mt::principal::FileFormatParams::Parquet(
            mt::principal::ParquetFileFormatParams {
                missing_field_as: Default::default(),
            },
        ),
        copy_options: mt::principal::CopyOptions {
            on_error: mt::principal::OnErrorMode::AbortNum(2),
            size_limit: 999,
            max_files: 0,
            split_size: 0,
            purge: true,
            single: false,
            max_file_size: 0,
            disable_variant_check: true,
            return_failed_only: false,
            detailed_output: false,
        },
        comment: "ccc".to_string(),
        number_of_files: 100,
        creator: Some(UserIdentity::new("databend")),
        created_on: DateTime::<Utc>::from_timestamp(1702603569, 0).unwrap(),
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), stage_info_v66.as_slice(), 66, want())?;

    Ok(())
}
