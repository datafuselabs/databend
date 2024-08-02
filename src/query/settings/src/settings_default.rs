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

use std::collections::HashMap;
use std::fmt::Display;
use std::ops::RangeInclusive;
use std::sync::Arc;

use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::UserSettingValue;
use once_cell::sync::OnceCell;

static DEFAULT_SETTINGS: OnceCell<Arc<DefaultSettings>> = OnceCell::new();

// Default value of cost factor settings
#[allow(dead_code)]
static COST_FACTOR_COMPUTE_PER_ROW: u64 = 1;
static COST_FACTOR_HASH_TABLE_PER_ROW: u64 = 10;
static COST_FACTOR_AGGREGATE_PER_ROW: u64 = 5;
static COST_FACTOR_NETWORK_PER_ROW: u64 = 50;

// Settings for readability and writability of tags.
// we will not be able to safely get its value when set to only write.
// we will not be able to safely set its value when set to only read.
#[derive(Copy, Clone, Debug)]
pub enum SettingMode {
    // they can be set, unset, or select
    Both,
    // they only can be select
    Read,
    // they only can be set or unset
    Write,
}

#[derive(Clone, Debug)]
pub enum SettingRange {
    Numeric(RangeInclusive<u64>),
    String(Vec<String>),
}

impl Display for SettingRange {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            SettingRange::Numeric(range) => write!(f, "[{}, {}]", range.start(), range.end()),
            SettingRange::String(values) => write!(f, "{:?}", values),
        }
    }
}

impl SettingRange {
    /// Checks if an integer value is within the numeric range.
    pub fn is_within_numeric_range(&self, value: u64) -> Result<()> {
        match self {
            SettingRange::Numeric(range) => {
                if range.contains(&value) {
                    Ok(())
                } else {
                    Err(ErrorCode::WrongValueForVariable(format!(
                        "Value {} is not within the range {}",
                        value, self
                    )))
                }
            }
            _ => Err(ErrorCode::BadArguments(
                "Expected numeric range".to_string(),
            )),
        }
    }

    /// Checks if a string value is within the string range.
    pub fn is_within_string_range(&self, value: &str) -> Result<String> {
        match self {
            SettingRange::String(values) => {
                match values.iter().find(|&s| s.eq_ignore_ascii_case(value)) {
                    Some(s) => Ok(s.to_string()),
                    None => Err(ErrorCode::WrongValueForVariable(format!(
                        "Value {} is not within the allowed values {:}",
                        value, self
                    ))),
                }
            }
            _ => Err(ErrorCode::BadArguments("Expected string range".to_string())),
        }
    }
}

#[derive(Clone, Debug)]
pub struct DefaultSettingValue {
    pub(crate) value: UserSettingValue,
    pub(crate) desc: &'static str,
    pub(crate) mode: SettingMode,
    pub(crate) range: Option<SettingRange>,
}

#[derive(Clone)]
pub struct DefaultSettings {
    pub(crate) settings: HashMap<String, DefaultSettingValue>,
}

impl DefaultSettings {
    pub fn instance() -> Result<Arc<DefaultSettings>> {
        Ok(Arc::clone(DEFAULT_SETTINGS.get_or_try_init(|| -> Result<Arc<DefaultSettings>> {
            let num_cpus = Self::num_cpus();
            let max_memory_usage = Self::max_memory_usage()?;
            let recluster_block_size = Self::recluster_block_size()?;
            let default_max_storage_io_requests = Self::storage_io_requests(num_cpus);
            let data_retention_time_in_days_max = Self::data_retention_time_in_days_max();
            let global_conf = GlobalConfig::try_get_instance();
            let all_timezones: Vec<String> = chrono_tz::TZ_VARIANTS.iter().map(|tz| tz.to_string()).collect();

            let default_settings = HashMap::from([
                ("enable_clickhouse_handler", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Enables clickhouse handler.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("max_block_size", DefaultSettingValue {
                    value: UserSettingValue::UInt64(65536),
                    desc: "Sets the maximum byte size of a single data block that can be read.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(1..=u64::MAX)),
                }),
                ("parquet_max_block_size", DefaultSettingValue {
                    value: UserSettingValue::UInt64(8192),
                    desc: "Max block size for parquet reader",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(1..=u64::MAX)),
                }),
                ("max_threads", DefaultSettingValue {
                    value: UserSettingValue::UInt64(num_cpus),
                    desc: "Sets the maximum number of threads to execute a request.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(1..=1024)),
                }),
                ("max_memory_usage", DefaultSettingValue {
                    value: UserSettingValue::UInt64(max_memory_usage),
                    desc: "Sets the maximum memory usage in bytes for processing a single query.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("data_retention_time_in_days", DefaultSettingValue {
                    // unit of retention_period is day
                    value: UserSettingValue::UInt64(1),
                    desc: "Sets the data retention time in days.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=data_retention_time_in_days_max)),
                }),
                ("max_storage_io_requests", DefaultSettingValue {
                    value: UserSettingValue::UInt64(default_max_storage_io_requests),
                    desc: "Sets the maximum number of concurrent I/O requests.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(1..=1024)),
                }),
                ("storage_io_min_bytes_for_seek", DefaultSettingValue {
                    value: UserSettingValue::UInt64(48),
                    desc: "Sets the minimum byte size of data that must be read from storage in a single I/O operation \
                when seeking a new location in the data file.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("storage_io_max_page_bytes_for_read", DefaultSettingValue {
                    value: UserSettingValue::UInt64(512 * 1024),
                    desc: "Sets the maximum byte size of data pages that can be read from storage in a single I/O operation.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("flight_client_timeout", DefaultSettingValue {
                    value: UserSettingValue::UInt64(60),
                    desc: "Sets the maximum time in seconds that a flight client request can be processed.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("http_handler_result_timeout_secs", DefaultSettingValue {
                    value: {
                        let result_timeout_secs = global_conf.map(|conf| conf.query.http_handler_result_timeout_secs)
                            .unwrap_or(60);
                        UserSettingValue::UInt64(result_timeout_secs)
                    },
                    desc: "Set the timeout in seconds that a http query session expires without any polls.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("storage_read_buffer_size", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1024 * 1024),
                    desc: "Sets the byte size of the buffer used for reading data into memory.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("input_read_buffer_size", DefaultSettingValue {
                    value: UserSettingValue::UInt64(4 * 1024 * 1024),
                    desc: "Sets the memory size in bytes allocated to the buffer used by the buffered reader to read data from storage.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("enable_new_copy_for_text_formats", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Use new implementation for loading CSV files.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("purge_duplicated_files_in_copy", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Purge duplicated files detected during execution of copy into table.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("timezone", DefaultSettingValue {
                    value: UserSettingValue::String("UTC".to_owned()),
                    desc: "Sets the timezone.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::String(all_timezones)),
                }),
                ("group_by_two_level_threshold", DefaultSettingValue {
                    value: UserSettingValue::UInt64(20000),
                    desc: "Sets the number of keys in a GROUP BY operation that will trigger a two-level aggregation.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("max_inlist_to_or", DefaultSettingValue {
                    value: UserSettingValue::UInt64(3),
                    desc: "Sets the maximum number of values that can be included in an IN expression to be converted to an OR operator.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("unquoted_ident_case_sensitive", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Set to 1 to make unquoted names (like table or column names) case-sensitive, or 0 for case-insensitive.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("quoted_ident_case_sensitive", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Set to 1 for case-sensitive treatment of quoted names (like \"TableName\"), or 0 for case-insensitive.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("sql_dialect", DefaultSettingValue {
                    value: UserSettingValue::String("PostgreSQL".to_owned()),
                    desc: "Sets the SQL dialect. Available values include \"PostgreSQL\", \"MySQL\",  \"Experimental\", \"Prql\", and \"Hive\".",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::String(vec!["PostgreSQL".into(), "MySQL".into(), "Experimental".into(), "Hive".into(), "Prql".into()])),
                }),
                ("enable_dphyp", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Enables dphyp join order algorithm.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("enable_cbo", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Enables cost-based optimization.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("disable_join_reorder", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Disable join reorder optimization.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("join_spilling_memory_ratio", DefaultSettingValue {
                    value: UserSettingValue::UInt64(60),
                    desc: "Sets the maximum memory ratio in bytes that hash join can use before spilling data to storage during query execution, 0 is unlimited",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=100)),
                }),
                ("join_spilling_bytes_threshold_per_proc", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Sets the maximum amount of memory in bytes that one join processor can use before spilling data to storage during query execution, 0 is unlimited.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("join_spilling_partition_bits", DefaultSettingValue {
                    value: UserSettingValue::UInt64(4),
                    desc: "Set the number of partitions for join spilling. Default value is 4, it means 2^4 partitions.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("join_spilling_buffer_threshold_per_proc_mb", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1024),
                    desc: "Set the spilling buffer threshold (MB) for each join processor.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("disable_merge_into_join_reorder", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Disable merge into join reorder optimization.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("enable_merge_into_row_fetch", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Enable merge into row fetch optimization.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("max_cte_recursive_depth", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1000),
                    desc: "Max recursive depth for recursive cte",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("inlist_to_join_threshold", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1024),
                    desc: "Set the threshold for converting IN list to JOIN.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("enable_bloom_runtime_filter", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Enables runtime filter optimization for JOIN.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("max_execute_time_in_seconds", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Sets the maximum query execution time in seconds. Setting it to 0 means no limit.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("collation", DefaultSettingValue {
                    value: UserSettingValue::String("utf8".to_owned()),
                    desc: "Sets the character collation. Available values include \"utf8\".",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::String(vec!["utf8".into()])),
                }),
                ("max_result_rows", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Sets the maximum number of rows that can be returned in a query result when no specific row count is specified. Setting it to 0 means no limit.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("prefer_broadcast_join", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Enables broadcast join.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("enforce_broadcast_join", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Enforce broadcast join.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("storage_fetch_part_num", DefaultSettingValue {
                    value: UserSettingValue::UInt64(2),
                    desc: "Sets the number of partitions that are fetched in parallel from storage during query execution.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("load_file_metadata_expire_hours", DefaultSettingValue {
                    value: UserSettingValue::UInt64(24),
                    desc: "Sets the hours that the metadata of files you load data from with COPY INTO will expire in.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("hide_options_in_show_create_table", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Hides table-relevant information, such as SNAPSHOT_LOCATION and STORAGE_FORMAT, at the end of the result of SHOW TABLE CREATE.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("sandbox_tenant", DefaultSettingValue {
                    value: UserSettingValue::String("".to_string()),
                    desc: "Injects a custom 'sandbox_tenant' into this session. This is only for testing purposes and will take effect only when 'internal_enable_sandbox_tenant' is turned on.",
                    mode: SettingMode::Both,
                    range: None,
                }),
                ("enable_query_result_cache", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Enables caching query results to improve performance for identical queries.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("query_result_cache_max_bytes", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1048576), // 1MB
                    desc: "Sets the maximum byte size of cache for a single query result.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("query_result_cache_min_execute_secs", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "For a query to be cached, it must take at least this many seconds to fetch the first block. It helps to avoid caching queries that are too fast to execute or queries with streaming scan.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("query_result_cache_ttl_secs", DefaultSettingValue {
                    value: UserSettingValue::UInt64(300), // seconds
                    desc: "Sets the time-to-live (TTL) in seconds for cached query results. \
                Once the TTL for a cached result has expired, the result is considered stale and will not be used for new queries.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("query_result_cache_allow_inconsistent", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Determines whether Databend will return cached query results that are inconsistent with the underlying data.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("enable_hive_parquet_predict_pushdown", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Enables hive parquet predict pushdown  by setting this variable to 1, default value: 1",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("hive_parquet_chunk_size", DefaultSettingValue {
                    value: UserSettingValue::UInt64(16384),
                    desc: "The max number of rows each read from parquet to databend processor",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("aggregate_spilling_bytes_threshold_per_proc", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Sets the maximum amount of memory in bytes that an aggregator can use before spilling data to storage during query execution.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("aggregate_spilling_memory_ratio", DefaultSettingValue {
                    value: UserSettingValue::UInt64(60),
                    desc: "Sets the maximum memory ratio in bytes that an aggregator can use before spilling data to storage during query execution.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=100)),
                }),
                ("window_partition_spilling_bytes_threshold_per_proc", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Sets the maximum amount of memory in bytes that a window partitioner can use before spilling data to storage during query execution.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("window_partition_spilling_memory_ratio", DefaultSettingValue {
                    value: UserSettingValue::UInt64(60),
                    desc: "Sets the maximum memory ratio in bytes that a window partitioner can use before spilling data to storage during query execution.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=100)),
                }),
                ("sort_spilling_bytes_threshold_per_proc", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Sets the maximum amount of memory in bytes that a sorter can use before spilling data to storage during query execution.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("sort_spilling_memory_ratio", DefaultSettingValue {
                    value: UserSettingValue::UInt64(60),
                    desc: "Sets the maximum memory ratio in bytes that a sorter can use before spilling data to storage during query execution.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=100)),
                }),
                ("sort_spilling_batch_bytes", DefaultSettingValue {
                    value: UserSettingValue::UInt64(8 * 1024 * 1024),
                    desc: "Sets the uncompressed size that merge sorter will spill to storage",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(4 * 1024..=u64::MAX)),
                }),
                ("group_by_shuffle_mode", DefaultSettingValue {
                    value: UserSettingValue::String(String::from("before_merge")),
                    desc: "Group by shuffle mode, 'before_partial' is more balanced, but more data needs to exchange.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::String(vec!["before_partial".into(), "before_merge".into()])),
                }),
                ("efficiently_memory_group_by", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Memory is used efficiently, but this may cause performance degradation.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("lazy_read_threshold", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1000),
                    desc: "Sets the maximum LIMIT in a query to enable lazy read optimization. Setting it to 0 disables the optimization.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("parquet_fast_read_bytes", DefaultSettingValue {
                    value: UserSettingValue::UInt64(16 * 1024 * 1024),
                    desc: "Parquet file with smaller size will be read as a whole file, instead of column by column. Default value: 16MB",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),

                // enterprise license related settings
                ("enterprise_license", DefaultSettingValue {
                    value: UserSettingValue::String("".to_owned()),
                    desc: "License key for use enterprise features",
                    // license key should not be reported
                    mode: SettingMode::Write,
                    range: None,
                }),
                ("enable_table_lock", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Enables table lock if necessary (enabled by default).",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("table_lock_expire_secs", DefaultSettingValue {
                    value: UserSettingValue::UInt64(30),
                    desc: "Sets the seconds that the table lock will expire in.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("acquire_lock_timeout", DefaultSettingValue {
                    value: UserSettingValue::UInt64(30),
                    desc: "Sets the maximum timeout in seconds for acquire a lock.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("deduplicate_label", DefaultSettingValue {
                    value: UserSettingValue::String("".to_owned()),
                    desc: "Sql duplicate label for deduplication.",
                    mode: SettingMode::Write,
                    range: None,
                }),
                ("enable_distributed_copy_into", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Enables distributed execution for the 'COPY INTO'.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("enable_experimental_merge_into", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Enables the experimental feature for 'MERGE INTO'.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("enable_distributed_merge_into", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Enables distributed execution for 'MERGE INTO'.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("enable_distributed_replace_into", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Enables distributed execution of 'REPLACE INTO'.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("enable_distributed_compact", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Enables distributed execution of table compaction.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("enable_analyze_histogram", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Enables analyze histogram for query optimization during analyzing table.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("enable_aggregating_index_scan", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Enables scanning aggregating index data while querying.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("enable_compact_after_write", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Enables compact after write(copy/insert/replace-into/merge-into), need more memory.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("auto_compaction_imperfect_blocks_threshold", DefaultSettingValue {
                    value: UserSettingValue::UInt64(25),
                    desc: "Threshold for triggering auto compaction. This occurs when the number of imperfect blocks in a snapshot exceeds this value after write operations.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("use_parquet2", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "This setting is deprecated",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("enable_replace_into_partitioning", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Enables partitioning for replace-into statement (if table has cluster keys).",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("replace_into_bloom_pruning_max_column_number", DefaultSettingValue {
                    value: UserSettingValue::UInt64(4),
                    desc: "Max number of columns used by bloom pruning for replace-into statement.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("replace_into_shuffle_strategy", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Choose shuffle strategy: 0 for Block, 1 for Segment level.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("recluster_timeout_secs", DefaultSettingValue {
                    value: UserSettingValue::UInt64(12 * 60 * 60),
                    desc: "Sets the seconds that recluster final will be timeout.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("ddl_column_type_nullable", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Sets new columns to be nullable (1) or not (0) by default in table operations.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("recluster_block_size", DefaultSettingValue {
                    value: UserSettingValue::UInt64(recluster_block_size),
                    desc: "Sets the maximum byte size of blocks for recluster",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("compact_max_block_selection", DefaultSettingValue {
                    value: UserSettingValue::UInt64(10000),
                    desc: "Limits the maximum number of blocks that can be selected during a compact operation.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(2..=u64::MAX)),
                }),
                ("enable_distributed_recluster", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Enable distributed execution of table recluster.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("enable_parquet_page_index", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Enables parquet page index",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("enable_parquet_rowgroup_pruning", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Enables parquet rowgroup pruning",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("external_server_connect_timeout_secs", DefaultSettingValue {
                    value: UserSettingValue::UInt64(10),
                    desc: "Connection timeout to external server",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("external_server_request_timeout_secs", DefaultSettingValue {
                    value: UserSettingValue::UInt64(180),
                    desc: "Request timeout to external server",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("external_server_request_batch_rows", DefaultSettingValue {
                    value: UserSettingValue::UInt64(65536),
                    desc: "Request batch rows to external server",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(1..=u64::MAX)),
                }),
                ("enable_parquet_prewhere", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Enables parquet prewhere",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("enable_experimental_aggregate_hashtable", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Enables experimental aggregate hashtable",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("numeric_cast_option", DefaultSettingValue {
                    value: UserSettingValue::String("rounding".to_string()),
                    desc: "Set numeric cast mode as \"rounding\" or \"truncating\".",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::String(vec!["rounding".into(), "truncating".into()])),
                }),
                ("enable_experimental_rbac_check", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "experiment setting disables stage and udf privilege check(enable by default).",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("create_query_flight_client_with_current_rt", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Turns on (1) or off (0) the use of the current runtime for query operations.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("query_flight_compression", DefaultSettingValue {
                    value: UserSettingValue::String(String::from("LZ4")),
                    desc: "flight compression method",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::String(vec!["None".into(), "LZ4".into(), "ZSTD".into()])),
                }),
                ("enable_refresh_virtual_column_after_write", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Refresh virtual column after new data written",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("enable_refresh_aggregating_index_after_write", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Refresh aggregating index after new data written",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("parse_datetime_ignore_remainder", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Ignore trailing chars when parse string to datetime",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("enable_dst_hour_fix", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Time conversion handles invalid DST by adding an hour. Accuracy not guaranteed.(disable by default)",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("enable_strict_datetime_parser", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Strict datetime parser. Only support ISO 8601 as Default format.The best practice is to turn this parameter on.(enable by default)",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("disable_variant_check", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Disable variant check to allow insert invalid JSON values",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("cost_factor_hash_table_per_row", DefaultSettingValue {
                    value: UserSettingValue::UInt64(COST_FACTOR_HASH_TABLE_PER_ROW),
                    desc: "Cost factor of building hash table for a data row",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("cost_factor_aggregate_per_row", DefaultSettingValue {
                    value: UserSettingValue::UInt64(COST_FACTOR_AGGREGATE_PER_ROW),
                    desc: "Cost factor of grouping operation for a data row",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("cost_factor_network_per_row", DefaultSettingValue {
                    value: UserSettingValue::UInt64(COST_FACTOR_NETWORK_PER_ROW),
                    desc: "Cost factor of transmit via network for a data row",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                // this setting will be removed when geometry type stable.
                ("enable_geo_create_table", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Create and alter table with geometry type",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("idle_transaction_timeout_secs", DefaultSettingValue {
                    value: UserSettingValue::UInt64(4 * 60 * 60),
                    desc: "Set the timeout in seconds for active session without any query",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(1..=u64::MAX)),
                }),
                ("enable_experimental_queries_executor", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Enables experimental new executor",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("statement_queued_timeout_in_seconds", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "The maximum waiting seconds in the queue. The default value is 0(no limit).",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("geometry_output_format", DefaultSettingValue {
                    value: UserSettingValue::String("GeoJSON".to_owned()),
                    desc: "Display format for GEOMETRY values.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::String(vec!["WKT".into(), "WKB".into(), "EWKT".into(), "EWKB".into(), "GeoJSON".into()])),
                }),
                ("script_max_steps", DefaultSettingValue {
                    value: UserSettingValue::UInt64(10000),
                    desc: "The maximum steps allowed in a single execution of script.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("enable_auto_fix_missing_bloom_index", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Enables auto fix missing bloom index",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("max_vacuum_temp_files_after_query", DefaultSettingValue {
                    value: UserSettingValue::UInt64(u64::MAX),
                    desc: "The maximum temp files will be removed after query. please enable vacuum feature. disable if 0",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("max_set_operator_count", DefaultSettingValue {
                    value: UserSettingValue::UInt64(u64::MAX),
                    desc: "The maximum count of set operator in a query. If your query stack overflow, you can reduce this value.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=u64::MAX)),
                }),
                ("enable_loser_tree_merge_sort", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "Enables loser tree merge sort",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("format_null_as_str", DefaultSettingValue {
                    value: UserSettingValue::UInt64(1),
                    desc: "Format NULL as str in query api response",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("distributed_connection_retry_count", DefaultSettingValue {
                    value: UserSettingValue::UInt64(60),
                    desc: "",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=1)),
                }),
                ("max_flight_connection_retry_times", DefaultSettingValue {
                    value: UserSettingValue::UInt64(0),
                    desc: "The maximum retry count for cluster flight. Disable if 0.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=30)),
                }),
                ("flight_connection_retry_interval", DefaultSettingValue {
                    value: UserSettingValue::UInt64(60),
                    desc: "The retry interval of cluster flight is in seconds.",
                    mode: SettingMode::Both,
                    range: Some(SettingRange::Numeric(0..=900)),
                }),
            ]);

            Ok(Arc::new(DefaultSettings {
                settings: default_settings.into_iter().map(|(k, v)| (k.to_string(), v))
                    .collect()
            }))
        })?))
    }

    fn storage_io_requests(num_cpus: u64) -> u64 {
        match GlobalConfig::try_get_instance() {
            None => std::cmp::min(num_cpus, 64),
            Some(conf) => match conf.storage.params.is_fs() {
                true => 48,
                false => std::cmp::min(num_cpus, 64),
            },
        }
    }

    /// The maximum number of days that data can be retained.
    /// The max is read from the global config:data_retention_time_in_days_max
    /// If the global config is not set, the default value is 90 days.
    fn data_retention_time_in_days_max() -> u64 {
        match GlobalConfig::try_get_instance() {
            None => 90,
            Some(conf) => conf.query.data_retention_time_in_days_max,
        }
    }

    fn num_cpus() -> u64 {
        match GlobalConfig::try_get_instance() {
            None => num_cpus::get() as u64,
            Some(conf) => {
                let mut num_cpus = num_cpus::get() as u64;

                if conf.storage.params.is_fs() {
                    if let Ok(n) = std::thread::available_parallelism() {
                        num_cpus = n.get() as u64;
                    }

                    // Most of x86_64 CPUs have 2-way Hyper-Threading
                    #[cfg(target_arch = "x86_64")]
                    {
                        if num_cpus >= 32 {
                            num_cpus /= 2;
                        }
                    }
                    // Detect CGROUPS ?
                }

                if conf.query.num_cpus != 0 {
                    num_cpus = conf.query.num_cpus;
                }

                num_cpus.clamp(1, 96)
            }
        }
    }

    fn max_memory_usage() -> Result<u64> {
        let memory_info = sys_info::mem_info().map_err(ErrorCode::from_std_error)?;

        Ok(match GlobalConfig::try_get_instance() {
            None => 1024 * memory_info.total * 80 / 100,
            Some(conf) => match conf.query.max_server_memory_usage {
                0 => 1024 * memory_info.total * 80 / 100,
                max_server_memory_usage => max_server_memory_usage,
            },
        })
    }

    fn recluster_block_size() -> Result<u64> {
        let max_memory_usage = Self::max_memory_usage()?;
        // The sort merge consumes more than twice as much memory,
        // so the block size is set relatively conservatively here.
        let recluster_block_size = max_memory_usage * 32 / 100;
        Ok(recluster_block_size)
    }

    /// Converts and validates a setting value based on its key.
    pub fn convert_value(k: String, v: String) -> Result<(String, UserSettingValue)> {
        // Retrieve the default settings instance
        let default_settings = DefaultSettings::instance()?;

        let setting_value = default_settings
            .settings
            .get(&k)
            .ok_or_else(|| ErrorCode::UnknownVariable(format!("Unknown variable: {:?}", k)))?;

        match &setting_value.range {
            None => {
                match setting_value.value {
                    // Numeric value.
                    UserSettingValue::UInt64(_) => {
                        let u64_val = Self::parse_to_u64(&v)?;
                        Ok((k, UserSettingValue::UInt64(u64_val)))
                    }
                    // String value.
                    UserSettingValue::String(_) => Ok((k, UserSettingValue::String(v))),
                }
            }
            Some(range) => {
                match range {
                    // Numeric range.
                    SettingRange::Numeric(_) => {
                        let u64_val = Self::parse_to_u64(&v)?;
                        range.is_within_numeric_range(u64_val)?;

                        Ok((k, UserSettingValue::UInt64(u64_val)))
                    }
                    // String range.
                    SettingRange::String(_) => {
                        // value is the standard value of the setting.
                        let value = range.is_within_string_range(&v)?;

                        Ok((k, UserSettingValue::String(value)))
                    }
                }
            }
        }
    }

    /// Parses a string value to u64.
    /// If the value is not a valid u64, it will be parsed as f64.
    /// Used for:
    /// set max_memory_usage = 1024*1024*1024*1.5;
    fn parse_to_u64(v: &str) -> Result<u64, ErrorCode> {
        match v.parse::<u64>() {
            Ok(val) => Ok(val),
            Err(_) => {
                // If not a valid u64, try parsing as f64
                match v.parse::<f64>() {
                    Ok(f) if f.fract() == 0.0 && f >= 0.0 && f <= u64::MAX as f64 => {
                        Ok(f.trunc() as u64) /* Convert to u64 if no fractional part, non-negative, and within u64 range */
                    }
                    _ => Err(ErrorCode::WrongValueForVariable(format!(
                        "{} is not a valid integer value",
                        v
                    ))),
                }
            }
        }
    }

    pub fn try_get_u64(key: &str) -> Result<u64> {
        match DefaultSettings::instance()?.settings.get(key) {
            Some(v) => v.value.as_u64(),
            None => Err(ErrorCode::UnknownVariable(format!(
                "Unknown variable: {:?}",
                key
            ))),
        }
    }

    pub fn try_get_string(key: &str) -> Result<String> {
        match DefaultSettings::instance()?.settings.get(key) {
            Some(v) => Ok(v.value.as_string()),
            None => Err(ErrorCode::UnknownVariable(format!(
                "Unknown variable: {:?}",
                key
            ))),
        }
    }

    pub fn check_setting_mode(key: &str, expect: SettingMode) -> Result<()> {
        let default_settings = DefaultSettings::instance()?;
        let setting_mode = default_settings
            .settings
            .get(key)
            .map(|x| x.mode)
            .ok_or_else(|| ErrorCode::UnknownVariable(format!("Unknown variable: {:?}", key)))?;

        let matched_mode = match expect {
            SettingMode::Both => matches!(setting_mode, SettingMode::Both),
            SettingMode::Read => matches!(setting_mode, SettingMode::Both | SettingMode::Read),
            SettingMode::Write => matches!(setting_mode, SettingMode::Both | SettingMode::Write),
        };

        match matched_mode {
            true => Ok(()),
            false => Err(ErrorCode::Internal(format!(
                "Variable mode don't matched, expect: {:?}, actual: {:?}",
                expect, setting_mode
            ))),
        }
    }
}

pub enum ReplaceIntoShuffleStrategy {
    SegmentLevelShuffling,
    BlockLevelShuffling,
}

impl TryFrom<u64> for ReplaceIntoShuffleStrategy {
    type Error = ErrorCode;

    fn try_from(value: u64) -> std::result::Result<Self, Self::Error> {
        match value {
            0 => Ok(ReplaceIntoShuffleStrategy::BlockLevelShuffling),
            1 => Ok(ReplaceIntoShuffleStrategy::SegmentLevelShuffling),
            _ => Err(ErrorCode::InvalidConfig(
                "value of replace_into_shuffle_strategy should be one of {0,1}, 0 for block level shuffling, 1 for segment level shuffling",
            )),
        }
    }
}
