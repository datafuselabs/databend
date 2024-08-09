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

#![allow(clippy::uninlined_format_args)]

#[macro_use]
pub(crate) mod common;
pub(crate) mod proto_conv;
mod user_proto_conv;
mod user_stage;
mod v002_database_meta;
mod v002_share_account_meta;
mod v002_share_meta;
mod v002_table_meta;
mod v005_database_meta;
mod v005_share_meta;
mod v006_copied_file_info;
mod v010_table_meta;
mod v012_table_meta;
mod v023_table_meta;
mod v024_table_meta;
mod v025_user_stage;
mod v026_schema;
mod v027_schema;
mod v028_schema;
mod v029_schema;
mod v030_user_stage;
mod v031_copy_max_file;
mod v032_file_format_params;
mod v033_table_meta;
mod v034_schema;
mod v035_user_stage;
mod v037_index_meta;
mod v038_empty_proto;
mod v039_data_mask;
mod v040_table_meta;
mod v041_virtual_column;
mod v042_s3_stage_new_field;
mod v043_table_statistics;
mod v044_table_meta;
mod v045_background;
mod v046_index_meta;
mod v047_catalog_meta;
mod v048_background;
mod v049_network_policy;
mod v050_user_info;
mod v051_obs_and_cos_storage;
mod v052_hive_catalog_config;
mod v053_csv_format_params;
mod v054_index_meta;
mod v055_table_meta;
mod v057_hdfs_storage;
mod v058_udf;
mod v059_csv_format_params;
mod v060_copy_options;
mod v061_oss_sse_options;
mod v062_table_lock_meta;
mod v063_connection;
mod v064_ndjson_format_params;
mod v065_least_visible_time;
mod v066_stage_create_on;
mod v067_password_policy;
mod v068_index_meta;
mod v069_user_grant_id;
mod v070_binary_type;
mod v071_user_password;
mod v072_csv_format_params;
mod v073_huggingface_config;
mod v074_table_db_meta;
mod v075_csv_format_params;
mod v076_role_ownership_info;
mod v077_s3_remove_allow_anonymous;
mod v078_grantentry;
mod v079_udf_created_on;
mod v080_geometry_datatype;
mod v081_udf_script;
mod v082_table_index;
mod v083_ndjson_format_params;
mod v084_background_task_creator;
mod v085_table_index;
mod v086_table_index;
mod v087_user_option_disabled;
mod v088_sequence_meta;
mod v089_geometry_output_format;
mod v090_role_info;
mod v091_role_user_create_time_info;
mod v092_orc_format_params;
mod v093_parquet_format_params;
mod v094_table_meta;
mod v095_share_endpoint_meta;
mod v096_database_meta;
mod v097_orc_format_params;
mod v098_catalog_option;
mod v099_parquet_format_params;
mod v100_tenant_quota;
mod v101_database_meta;
mod v102_user_must_change_password;
mod v103_share_meta_v2;
mod v104_share_catalog;
mod v105_query_token;
