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

use crate::Incompatible;

/// Describes metadata changes.
///
/// This is a list of every `VER` and the corresponding change it introduces.
///
/// ## For developers
///
/// Every time fields are added/removed into/from data types in this crate:
/// - Add a new line to this list to describe what changed.
/// - Add a test case to ensure protobuf message serialized by this version can be loaded,
///   similar to: test_user_stage_fs_v6() in tests/it/user_stage.rs;
///
/// `VER` is the current metadata version and is automatically set to the last version.
/// `MIN_READER_VER` is the oldest compatible version.
#[rustfmt::skip]
const META_CHANGE_LOG: &[(u64, &str)] = &[
    //
    (1, "----------: Initial", ),
    (2, "2022-07-13: Add: share.proto", ),
    (3, "2022-07-29: Add: user.proto/UserOption::default_role", ),
    (4, "2022-08-22: Add: config.proto/GcsStorageConfig", ),
    (5, "2022-08-25: Add: ShareMetaV1::share_from_db_ids; DatabaseMeta::from_share", ),
    (6, "2022-09-08: Add: users.proto/CopyOptions::purge", ),
    (7, "2022-09-09: Add: table.proto/{TableCopiedFileInfo,TableCopiedFileLock} type", ),
    (8, "2022-09-16: Add: users.proto/StageFile::entity_tag", ),
    (9, "2022-09-20: Add: config.proto/S3StorageConfig::security_token", ),
    (10, "2022-09-23: Add: table.proto/TableMeta::catalog", ),
    (11, "2022-09-29: Add: users.proto/CopyOptions::single and CopyOptions::max_file_size", ),
    (12, "2022-09-29: Add: table.proto/TableMeta::storage_params", ),
    (13, "2022-10-09: Add: config.proto/OssStorageConfig and user.proto/StageStorage::oss", ),
    (14, "2022-10-11: Add: role_arn and external_id in config.proto/OssStorageConfig, Remove role_arn and oidc_token from config.proto/OssStorageConfig", ),
    (15, "2022-10-12: Remove: precision in TimestampType", ),
    (16, "2022-09-29: Add: CopyOptions::split_size", ),
    (17, "2022-10-28: Add: StageType::LegacyInternal", ),
    (18, "2022-10-28: Add: FILEFormatOptions::escape", ),
    (19, "2022-10-31: Add: StageType::UserStage", ),
    (20, "2022-11-02: Add: users.proto/FileFormatOptions::row_tag", ),
    (21, "2022-11-24: Add: users.proto/FileFormatOptions::nan_display", ),
    (22, "2022-12-13: Add: users.proto/FileFormatOptions::quote", ),
    (23, "2022-12-28: Add: table.proto/TableMeta::part_prefix", ),
    (24, "2023-01-07: Add: new-schema pb::DataType to/from TableDataType", ),
    (25, "2023-01-05: Add: user.proto/OnErrorMode::AbortNum", ),
    (26, "2023-01-16: Add: metadata.proto/DataSchema::next_column_id", ),
    (27, "2023-02-10: Add: metadata.proto/DataType Decimal types", ),
    (28, "2023-02-13: Add: user.proto/UserDefinedFileFormat", ),
    (29, "2023-02-23: Add: metadata.proto/DataType EmptyMap types", ),
    (30, "2023-02-21: Add: config.proto/WebhdfsStorageConfig; Modify: user.proto/UserStageInfo::StageStorage", ),
    (31, "2023-02-21: Add: CopyOptions::max_files", ),
    (32, "2023-04-05: Add: file_format.proto/FileFormatParams", ),
    (33, "2023-04-13: Update: add `shared_by` field into TableMeta", ),
    (34, "2023-04-23: Add: metadata.proto/DataType Bitmap type", ),
    (35, "2023-05-08: Add: CopyOptions::disable_variant_check", ),
    (36, "2023-05-12: Add: metadata.proto/ComputedExpr (reverted)", ),
    (37, "2023-05-05: Add: index.proto", ),
    (38, "2023-05-19: Rename: table.proto/TableCopiedFileLock to EmptyProto", ),
    (39, "2023-05-22: Add: data_mask.proto", ),
    (40, "2023-05-26: Add: TableMeta add column_mask_policy field", ),
    (41, "2023-05-29: Add: virtual_column.proto", ),
    (42, "2023-06-03: Add allow_anonymous in S3 Config", ),
    (43, "2023-06-05: Add fields `number_of_segments` and `number_of_blocks` to TableStatistics", ),
    (44, "2023-06-07: Add: metadata.proto/ComputedExpr", ),
    (45, "2023-06-06: Add: background_tasks.proto and background_jobs.proto", ),
    (46, "2023-06-28: Add: index.proto/IndexMeta::updated_on", ),
    (47, "2023-07-03: Add: catalog.proto/CatalogMeta", ),
    (48, "2023-07-04: Add: ManualTriggerParams on background_job", ),
    (49, "2023-07-14: Add: user.proto/NetworkPolicy", ),
    (50, "2023-07-20: Add: user.proto/UserOption::network_policy", ),
    (51, "2023-08-15: Add: config.proto/ObsStorageConfig add CosStorageConfig", ),
    (52, "2023-08-15: Add: catalog.proto/HiveCatalogConfig add storage params", ),
    (53, "2023-08-17: Add: user.proto/CsvFileFormatParams add field `null_display`", ),
    (54, "2023-08-17: Add: index.proto/IndexMeta::sync_creation", ),
    (55, "2023-07-31: Add: TableMeta and DatabaseMeta add Ownership", ),
    (56, "2023-08-31: (Depressed, see 65 below)Add: Least Visible Time", ),
    (57, "2023-09-05: Add: catalog.proto add hdfs config", ),
    (58, "2023-09-06: Add: udf.proto/UserDefinedFunction", ),
    (59, "2023-08-17: Add: user.proto/CsvFileFormatParams add field `allow_column_count_mismatch`", ),
    (60, "2023-08-17: Add: user.proto/CopyOptions add field `return_failed_only`", ),
    (61, "2023-10-19: Add: config.proto/OssStorageConfig add SSE options", ),
    (62, "2023-10-30: Add: lock.proto"),
    (63, "2023-10-30: Add: connection.proto"),
    (64, "2023-11-16: Add: user.proto/NDJsonFileFormatParams add field `missing_field_as` and `null_field_as`", ),
    (65, "2023-11-16: Retype: use Datetime<Utc> instead of u64 to in lvt.time", ),
    (66, "2023-12-15: Add: stage.proto/StageInfo::created_on", ),
    (67, "2023-12-19: Add: user.proto/PasswordPolicy and UserOption::password_policy", ),
    (68, "2023-12-19: Add: index.proto/IndexMeta add field `original_query` and `user_defined_block_name`"),
    (69, "2023-12-21: Add: user.proto/GrantTableIdObject and GrantDatabaseIdObject", ),
    (70, "2023-12-25: Add: datatype.proto Binary type", ),
    (71, "2024-01-02: Add: user.proto/password options", ),
    (72, "2024-01-09: Add: user.proto/CSVFileFormatParams add field `empty_field_as`", ),
    (73, "2024-01-11: Add: config.proto/StorageConfig add HuggingfaceConfig", ),
    (74, "2024-01-12: Remove: owner in DatabaseMeta and TableMeta", ),
    (75, "2024-01-15: ADD: user.proto/CsvFileFormatParams add field `binary_format` and `output_header`", ),
    (76, "2024-01-18: ADD: ownership.proto and role.proto", ),
    (77, "2024-01-22: Remove: allow_anonymous in S3 Config", ),
    (78, "2024-01-29: Refactor: GrantEntry::UserPrivilegeType and ShareGrantEntry::ShareGrantObjectPrivilege use from_bits_truncate deserialize", ),
    (79, "2024-01-31: Add: udf.proto/UserDefinedFunction add created_on field", ),
    (80, "2024-02-01: Add: datatype.proto/DataType Geometry type"),
    (81, "2024-03-04: Add: udf.udf_script"),
    (82, "2024-03-08: Add: table.inverted_index"),
    (83, "2024-03-14: Add: null_if in user.proto/NDJSONFileFormatParams"),
    (84, "2024-03-21: Rename: background.proto/BackgroundJobIdent to BackgroundTaskCreator"),
    (85, "2024-03-26: Add: table.inverted_index sync_creation"),
    (86, "2024-04-01: Add: table.inverted_index version, options"),
    (87, "2024-04-17: Add: UserOption::disabled"),
    (88, "2024-04-17: Add: SequenceMeta"),
    (89, "2024-04-19: Add: geometry_output_format settings"),
    (90, "2024-05-13: Refactor: After reader_check_msg success, RoleInfo::from_pb should not return err"),
    (91, "2024-05-28: Add: user/role.proto and User/RoleInfo add update/created_on field"),
    (92, "2024-06-03: Add: user.proto/OrcFileFormatParams", ),
    (93, "2024-06-06: Add: null_if in user.proto/ParquetFileFormatParams"),
    (94, "2024-06-21: Remove: catalog in table meta"),
    (95, "2024-07-01: Add: add credential into share endpoint meta"),
    (96, "2024-07-02: Add: add using_share_endpoint field into DatabaseMeta"),
    (97, "2024-07-04: Add: missing_field_as in user.proto/OrcFileFormatParams"),
    (98, "2024-07-04: Add: add iceberg catalog option in catalog option"),
    (99, "2024-07-08: Add: missing_field_as in user.proto/ParquetFileFormatParams"),
    (100, "2024-06-21: Add: tenant.proto/TenantQuota"),
    (101, "2024-07-06: Add: add from_share_db_id field into DatabaseMeta"),
    (102, "2024-07-11: Add: UserOption add must_change_password, AuthInfo.Password add need_change"),
    (103, "2024-07-31: Add: ShareMetaV2"),
    (104, "2024-08-02: Add: add share catalog into Catalog meta"),
    (105, "2024-08-05: Add: add Dictionary meta"),
    // Dear developer:
    //      If you're gonna add a new metadata version, you'll have to add a test for it.
    //      You could just copy an existing test file(e.g., `../tests/it/v024_table_meta.rs`)
    //      and replace two of the variable `bytes` and `want`.
];

/// Attribute of both a reader and a message:
/// The version to write into a message and it is also the version of the message reader.
pub const VER: u64 = META_CHANGE_LOG.last().unwrap().0;

/// Attribute of a message:
/// The minimal reader version that can read message of version `VER`, i.e. `message.ver=VER`.
///
/// This is written to every message that needs to be serialized independently.
pub const MIN_READER_VER: u64 = 24;

/// Attribute of a reader:
/// The minimal message version(`message.ver`) that a reader can read.
pub const MIN_MSG_VER: u64 = 1;

pub fn reader_check_msg(msg_ver: u64, msg_min_reader_ver: u64) -> Result<(), Incompatible> {
    // The reader version must be big enough
    if VER < msg_min_reader_ver {
        return Err(Incompatible {
            reason: format!(
                "executable ver={} is smaller than the min reader version({}) that can read this message",
                VER, msg_min_reader_ver
            ),
        });
    }

    // The message version must be big enough
    if msg_ver < MIN_MSG_VER {
        return Err(Incompatible {
            reason: format!(
                "message ver={} is smaller than executable MIN_MSG_VER({}) that this program can read",
                msg_ver, MIN_MSG_VER
            ),
        });
    }
    Ok(())
}

pub fn missing(reason: impl ToString) -> impl FnOnce() -> Incompatible {
    let s = reason.to_string();
    move || Incompatible { reason: s }
}
