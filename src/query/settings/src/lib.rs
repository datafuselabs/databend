// Copyright 2022 Datafuse Labs.
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
#![deny(unused_crate_dependencies)]

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::str;
use std::sync::Arc;

use common_ast::Dialect;
use common_base::block_on;
use common_base::runtime::GlobalIORuntime;
use common_base::runtime::TrySpawn;
use common_config::GlobalConfig;
use common_config::InnerConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::principal::UserSetting;
use common_meta_app::principal::UserSettingValue;
use common_meta_types::MatchSeq;
use common_users::UserApiProvider;
use dashmap::DashMap;
use itertools::Itertools;

#[derive(Clone)]
pub enum ScopeLevel {
    #[allow(dead_code)]
    Global,
    Session,
}

impl Debug for ScopeLevel {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        match self {
            ScopeLevel::Global => {
                write!(f, "GLOBAL")
            }
            ScopeLevel::Session => {
                write!(f, "SESSION")
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct SettingValue {
    // Default value of this setting.
    default_value: UserSettingValue,
    user_setting: UserSetting,
    level: ScopeLevel,
    desc: &'static str,
    possible_values: Option<Vec<&'static str>>,
}

#[derive(Clone, Debug)]
pub struct Settings {
    settings: Arc<DashMap<String, SettingValue>>,
    // TODO verify this, will tenant change during the lifetime of a given session?
    //#[allow(dead_code)]
    // session_ctx: Arc<T>,
    tenant: String,
}

impl Settings {
    pub async fn try_create(
        user_api: Arc<UserApiProvider>,
        tenant: String,
    ) -> Result<Arc<Settings>> {
        let config = GlobalConfig::instance();
        let settings = Self::default_settings(&tenant, config)?;

        let ret = {
            // Overwrite settings from metasrv
            let global_settings = user_api
                .get_setting_api_client(&tenant)?
                .get_settings()
                .await?;

            for global_setting in global_settings {
                let name = global_setting.name;
                let val = global_setting.value.as_string()?;

                // the settings may be deprecated
                if !settings.has_setting(&name) {
                    tracing::warn!("Ignore deprecated global setting {} = {}", name, val);
                    continue;
                }
                settings.set_settings(name.clone(), val, false)?;
                settings.set_setting_level(&name, true)?;
            }
            settings
        };
        Ok(ret)
    }

    pub fn default_settings(tenant: &str, conf: Arc<InnerConfig>) -> Result<Arc<Settings>> {
        let memory_info = sys_info::mem_info().map_err(ErrorCode::from_std_error)?;
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
        num_cpus = num_cpus.clamp(1, 96);

        let mut default_max_memory_usage = 1024 * memory_info.total * 80 / 100;
        if conf.query.max_server_memory_usage != 0 {
            default_max_memory_usage = conf.query.max_server_memory_usage;
        }

        let default_max_storage_io_requests = if conf.storage.params.is_fs() { 48 } else { 64 };

        let values = vec![
            // max_block_size
            SettingValue {
                default_value: UserSettingValue::UInt64(65536),
                user_setting: UserSetting::create(
                    "max_block_size",
                    UserSettingValue::UInt64(65536),
                ),
                level: ScopeLevel::Session,
                desc: "Maximum block size for reading, default value: 65536.",
                possible_values: None,
            },
            // max_threads
            SettingValue {
                default_value: UserSettingValue::UInt64(num_cpus),
                user_setting: UserSetting::create(
                    "max_threads",
                    UserSettingValue::UInt64(num_cpus),
                ),
                level: ScopeLevel::Session,
                desc: "The maximum number of threads to execute the request. By default the value is determined automatically.",
                possible_values: None,
            },
            // max_memory_usage
            SettingValue {
                // unit of memory_info.total is kB
                default_value: UserSettingValue::UInt64(default_max_memory_usage),
                user_setting: UserSetting::create(
                    "max_memory_usage",
                    UserSettingValue::UInt64(default_max_memory_usage),
                ),
                level: ScopeLevel::Session,
                desc: "The maximum memory usage for processing single query, in bytes. By default the value is determined automatically.",
                possible_values: None,
            },
            // retention_period
            SettingValue {
                // unit of retention_period is hour
                default_value: UserSettingValue::UInt64(12),
                user_setting: UserSetting::create("retention_period", UserSettingValue::UInt64(12)),
                level: ScopeLevel::Session,
                desc: "The retention_period in hours. By default the value is 12 hours.",
                possible_values: None,
            },
            // max_storage_io_requests
            SettingValue {
                default_value: UserSettingValue::UInt64(default_max_storage_io_requests),
                user_setting: UserSetting::create(
                    "max_storage_io_requests",
                    UserSettingValue::UInt64(default_max_storage_io_requests),
                ),
                level: ScopeLevel::Session,
                desc: "The maximum number of concurrent IO requests. By default the value is determined automatically.",
                possible_values: None,
            },
            // storage_io_min_bytes_for_seek
            SettingValue {
                default_value: UserSettingValue::UInt64(48),
                user_setting: UserSetting::create(
                    "storage_io_min_bytes_for_seek",
                    UserSettingValue::UInt64(48),
                ),
                level: ScopeLevel::Session,
                desc: "If the distance between two IO ranges to be read in one file is less than storage_io_min_bytes_for_seek, then Databend sequentially reads a range of file that contains both ranges, thus avoiding extra seek. \
                Default value is 48Bytes",
                possible_values: None,
            },
            // storage_io_max_page_bytes_for_read
            SettingValue {
                default_value: UserSettingValue::UInt64(512 * 1024),
                user_setting: UserSetting::create(
                    "storage_io_max_page_bytes_for_read",
                    UserSettingValue::UInt64(512 * 1024),
                ),
                level: ScopeLevel::Session,
                desc: "The maximum bytes of one IO request to read. Default the value is 512KB",
                possible_values: None,
            },
            // flight_client_timeout
            SettingValue {
                default_value: UserSettingValue::UInt64(60),
                user_setting: UserSetting::create(
                    "flight_client_timeout",
                    UserSettingValue::UInt64(60),
                ),
                level: ScopeLevel::Session,
                desc: "Max duration the flight client request is allowed to take in seconds. By default, it is 60 seconds.",
                possible_values: None,
            },
            // storage_read_buffer_size
            SettingValue {
                default_value: UserSettingValue::UInt64(1024 * 1024),
                user_setting: UserSetting::create(
                    "storage_read_buffer_size",
                    UserSettingValue::UInt64(1024 * 1024),
                ),
                level: ScopeLevel::Session,
                desc: "The size of buffer in bytes for buffered reader of dal. By default, it is 1MB.",
                possible_values: None,
            },
            SettingValue {
                default_value: UserSettingValue::UInt64(1024 * 1024),
                user_setting: UserSetting::create(
                    "input_read_buffer_size",
                    UserSettingValue::UInt64(1024 * 1024),
                ),
                level: ScopeLevel::Session,
                desc: "The size of buffer in bytes for input with format. By default, it is 1MB.",
                possible_values: None,
            },
            SettingValue {
                default_value: UserSettingValue::String("UTC".to_owned()),
                user_setting: UserSetting::create(
                    "timezone",
                    UserSettingValue::String("UTC".to_owned()),
                ),
                level: ScopeLevel::Session,
                desc: "Timezone, default value: \"UTC\".",
                possible_values: None,
            },
            SettingValue {
                default_value: UserSettingValue::UInt64(20000),
                user_setting: UserSetting::create(
                    "group_by_two_level_threshold",
                    UserSettingValue::UInt64(20000),
                ),
                level: ScopeLevel::Session,
                desc: "The threshold of keys to open two-level aggregation, default value: 20000.",
                possible_values: None,
            },
            SettingValue {
                default_value: UserSettingValue::UInt64(3),
                user_setting: UserSetting::create("max_inlist_to_or", UserSettingValue::UInt64(3)),
                level: ScopeLevel::Session,
                desc: "Max size in inlist expression that will convert to or combinator, default value: 3.",
                possible_values: None,
            },
            SettingValue {
                default_value: UserSettingValue::UInt64(0),
                user_setting: UserSetting::create(
                    "unquoted_ident_case_sensitive",
                    UserSettingValue::UInt64(0),
                ),
                level: ScopeLevel::Session,
                desc: "Case sensitivity of unquoted identifiers, default value: 0 (aka case-insensitive).",
                possible_values: None,
            },
            SettingValue {
                default_value: UserSettingValue::UInt64(1),
                user_setting: UserSetting::create(
                    "quoted_ident_case_sensitive",
                    UserSettingValue::UInt64(1),
                ),
                level: ScopeLevel::Session,
                desc: "Case sensitivity of quoted identifiers, default value: 1 (aka case-sensitive).",
                possible_values: None,
            },
            SettingValue {
                default_value: UserSettingValue::String("PostgreSQL".to_owned()),
                user_setting: UserSetting::create(
                    "sql_dialect",
                    UserSettingValue::String("PostgreSQL".to_owned()),
                ),
                level: ScopeLevel::Session,
                desc: "SQL dialect, support \"PostgreSQL\" \"MySQL\" and \"Hive\", default value: \"PostgreSQL\".",
                possible_values: Some(vec!["PostgreSQL", "MySQL", "Hive"]),
            },
            SettingValue {
                default_value: UserSettingValue::UInt64(1),
                user_setting: UserSetting::create("enable_cbo", UserSettingValue::UInt64(1)),
                level: ScopeLevel::Session,
                desc: "If enable cost based optimization, default value: 1.",
                possible_values: None,
            },
            SettingValue {
                default_value: UserSettingValue::UInt64(0),
                user_setting: UserSetting::create(
                    "enable_runtime_filter",
                    UserSettingValue::UInt64(0),
                ),
                level: ScopeLevel::Session,
                desc: "If enable runtime filter optimization for join, default value: 0.",
                possible_values: None,
            },
            // max_execute_time
            SettingValue {
                default_value: UserSettingValue::UInt64(0),
                user_setting: UserSetting::create("max_execute_time", UserSettingValue::UInt64(0)),
                level: ScopeLevel::Session,
                desc: "The maximum query execution time. it means no limit if the value is zero. default value: 0.",
                possible_values: None,
            },
            SettingValue {
                default_value: UserSettingValue::String("binary".to_owned()),
                user_setting: UserSetting::create(
                    "collation",
                    UserSettingValue::String("binary".to_owned()),
                ),
                level: ScopeLevel::Session,
                desc: "Char collation, support \"binary\" \"utf8\" default value: binary",
                possible_values: Some(vec!["binary", "utf8"]),
            },
            #[cfg(feature = "hive")]
            SettingValue {
                default_value: UserSettingValue::UInt64(1),
                user_setting: UserSetting::create(
                    "enable_hive_parquet_predict_pushdown",
                    UserSettingValue::UInt64(1),
                ),
                level: ScopeLevel::Session,
                desc: "Enable hive parquet predict pushdown  by setting this variable to 1, default value: 1",
                possible_values: None,
            },
            #[cfg(feature = "hive")]
            SettingValue {
                default_value: UserSettingValue::UInt64(16384),
                user_setting: UserSetting::create(
                    "hive_parquet_chunk_size",
                    UserSettingValue::UInt64(16384),
                ),
                level: ScopeLevel::Session,
                desc: "the max number of rows each read from parquet to databend processor",
                possible_values: None,
            },
            SettingValue {
                default_value: UserSettingValue::UInt64(0),
                user_setting: UserSetting::create("max_result_rows", UserSettingValue::UInt64(0)),
                level: ScopeLevel::Session,
                desc: "Auto limit max result rows if user not specify the limit, default is 0 means no limit",
                possible_values: None,
            },
            SettingValue {
                default_value: UserSettingValue::UInt64(1),
                user_setting: UserSetting::create(
                    "enable_distributed_eval_index",
                    UserSettingValue::UInt64(1),
                ),
                level: ScopeLevel::Session,
                desc: "If enable distributed eval index, default value: 1",
                possible_values: None,
            },
            SettingValue {
                default_value: UserSettingValue::UInt64(1),
                user_setting: UserSetting::create(
                    "prefer_broadcast_join",
                    UserSettingValue::UInt64(1),
                ),
                level: ScopeLevel::Session,
                desc: "If enable broadcast join, default value: 1",
                possible_values: None,
            },
            SettingValue {
                default_value: UserSettingValue::UInt64(2),
                user_setting: UserSetting::create(
                    "storage_fetch_part_num",
                    UserSettingValue::UInt64(2),
                ),
                level: ScopeLevel::Session,
                desc: "The max number of part each read cycle.",
                possible_values: None,
            },
            SettingValue {
                default_value: UserSettingValue::UInt64(24 * 7),
                user_setting: UserSetting::create(
                    "load_file_metadata_expire_hours",
                    UserSettingValue::UInt64(24 * 7),
                ),
                level: ScopeLevel::Session,
                desc: "How many hours will the COPY file metadata expired in the metasrv, default value: 24*7=7days",
                possible_values: None,
            },
            SettingValue {
                default_value: UserSettingValue::UInt64(1),
                user_setting: UserSetting::create(
                    "hide_options_in_show_create_table",
                    UserSettingValue::UInt64(1),
                ),
                level: ScopeLevel::Session,
                desc: "Ignore options while rendering the result of show create table.",
                possible_values: None,
            },
            SettingValue {
                default_value: UserSettingValue::String("".to_string()),
                user_setting: UserSetting::create(
                    "sandbox_tenant",
                    UserSettingValue::String("".to_string()),
                ),
                level: ScopeLevel::Session,
                desc: "Inject a custom sandbox_tenant into this session, it's only for testing purpose and take effect when the internal_enable_sandbox_tenant is on",
                possible_values: None,
            },
            SettingValue {
                default_value: UserSettingValue::UInt64(2 * 1024 * 1024),
                user_setting: UserSetting::create(
                    "parquet_uncompressed_buffer_size",
                    UserSettingValue::UInt64(2 * 1024 * 1024),
                ),
                level: ScopeLevel::Session,
                desc: "Parquet decompresses buffer size. default: 2MB",
                possible_values: None,
            },
            SettingValue {
                default_value: UserSettingValue::UInt64(0),
                user_setting: UserSetting::create("enable_bushy_join", UserSettingValue::UInt64(0)),
                level: ScopeLevel::Session,
                desc: "Enable generating bushy join plan in optimizer",
                possible_values: None,
            },
            SettingValue {
                default_value: UserSettingValue::UInt64(0),
                user_setting: UserSetting::create(
                    "enable_query_result_cache",
                    UserSettingValue::UInt64(0),
                ),
                level: ScopeLevel::Session,
                desc: "Enable the query result caching of SQL queries in Databend. \
                When this setting is enabled, Databend will store the results of queries in storage. \
                This can improve query performance by reducing the amount of time required to re-execute the same query multiple times. \
                Default is disabled.",
                possible_values: None,
            },
            SettingValue {
                default_value: UserSettingValue::UInt64(1048576), // 1MB
                user_setting: UserSetting::create(
                    "query_result_cache_max_bytes",
                    UserSettingValue::UInt64(1048576),
                ),
                level: ScopeLevel::Session,
                desc: "The maximum bytes of the query result cache for one query, default: 1048576 bytes (1MB).",
                possible_values: None,
            },
            SettingValue {
                default_value: UserSettingValue::UInt64(300), // seconds
                user_setting: UserSetting::create(
                    "query_result_cache_ttl_secs",
                    UserSettingValue::UInt64(300),
                ),
                level: ScopeLevel::Session,
                desc: "The time-to-live (TTL) for cached query results, in seconds. \
                 Once the TTL for a cached result has expired, the result is considered stale and will not be used for new queries. \
                 Default: 300 seconds (5 minutes).",
                possible_values: None,
            },
            SettingValue {
                default_value: UserSettingValue::UInt64(0),
                user_setting: UserSetting::create(
                    "query_result_cache_allow_inconsistent",
                    UserSettingValue::UInt64(0),
                ),
                level: ScopeLevel::Session,
                desc: "Controls whether inconsistent cached results can be used for queries. \
                When this setting is set to TRUE, Databend will use cached results even if they may be inconsistent due to changes in the underlying data. \
                Default is FALSE (disabled).",
                possible_values: None,
            },
            SettingValue {
                default_value: UserSettingValue::UInt64(0),
                user_setting: UserSetting::create(
                    "spilling_bytes_threshold_per_proc",
                    UserSettingValue::UInt64(0),
                ),
                level: ScopeLevel::Session,
                desc: "When the memory used by the aggregator exceeds the set value, the data will overflow into the storage. disable if it's 0.",
                possible_values: None,
            },
        ];

        let settings: Arc<DashMap<String, SettingValue>> = Arc::new(DashMap::default());

        // Initial settings.
        {
            for value in values {
                let name = value.user_setting.name.clone();
                settings.insert(name, value);
            }
        }

        Ok(Arc::new(Settings {
            tenant: tenant.to_string(),
            settings,
        }))
    }

    // Only used for testings
    pub fn default_test_settings() -> Result<Arc<Settings>> {
        Self::default_settings("default", Arc::new(InnerConfig::default()))
    }

    // Get max_block_size.
    pub fn get_max_block_size(&self) -> Result<u64> {
        let key = "max_block_size";
        self.try_get_u64(key)
    }

    // Get max_threads.
    pub fn get_max_threads(&self) -> Result<u64> {
        let key = "max_threads";
        let value = self.try_get_u64(key)?;
        if value == 0 { Ok(16) } else { Ok(value) }
    }

    // Set max_threads.
    pub fn set_max_threads(&self, val: u64) -> Result<()> {
        let key = "max_threads";
        self.try_set_u64(key, val, false)
    }

    // Get storage_fetch_part_num.
    pub fn get_storage_fetch_part_num(&self) -> Result<u64> {
        let key = "storage_fetch_part_num";
        let value = self.try_get_u64(key)?;
        if value == 0 { Ok(16) } else { Ok(value) }
    }

    // Set storage_fetch_part_num.
    pub fn set_storage_fetch_part_num(&self, val: u64) -> Result<()> {
        let key = "storage_fetch_part_num";
        self.try_set_u64(key, val, false)
    }

    // Get parquet_uncompressed_buffer_size.
    pub fn get_parquet_uncompressed_buffer_size(&self) -> Result<u64> {
        let key = "parquet_uncompressed_buffer_size";
        self.try_get_u64(key)
    }

    // Set parquet_uncompressed_buffer_size.
    pub fn set_parquet_uncompressed_buffer_size(&self, val: u64) -> Result<()> {
        let key = "parquet_uncompressed_buffer_size";
        self.try_set_u64(key, val, false)
    }

    pub fn get_max_memory_usage(&self) -> Result<u64> {
        let key = "max_memory_usage";
        self.try_get_u64(key)
    }

    pub fn set_max_memory_usage(&self, val: u64) -> Result<()> {
        let key = "max_memory_usage";
        self.try_set_u64(key, val, false)
    }

    pub fn set_retention_period(&self, hours: u64) -> Result<()> {
        let key = "retention_period";
        self.try_set_u64(key, hours, false)
    }

    pub fn get_retention_period(&self) -> Result<u64> {
        let key = "retention_period";
        self.try_get_u64(key)
    }

    pub fn get_max_storage_io_requests(&self) -> Result<u64> {
        let key = "max_storage_io_requests";
        self.try_get_u64(key)
    }

    pub fn set_max_storage_io_requests(&self, val: u64) -> Result<()> {
        let key = "max_storage_io_requests";
        self.try_set_u64(key, val, false)
    }

    pub fn get_storage_io_min_bytes_for_seek(&self) -> Result<u64> {
        let key = "storage_io_min_bytes_for_seek";
        self.try_get_u64(key)
    }

    pub fn set_storage_io_min_bytes_for_seek(&self, val: u64) -> Result<()> {
        let key = "storage_io_min_bytes_for_seek";
        self.try_set_u64(key, val, false)
    }

    pub fn get_storage_io_max_page_bytes_for_read(&self) -> Result<u64> {
        let key = "storage_io_max_page_bytes_for_read";
        self.try_get_u64(key)
    }

    pub fn set_storage_io_max_page_bytes_for_read(&self, val: u64) -> Result<()> {
        let key = "storage_io_max_page_bytes_for_read";
        self.try_set_u64(key, val, false)
    }

    // Get max_execute_time.
    pub fn get_max_execute_time(&self) -> Result<u64> {
        self.try_get_u64("max_execute_time")
    }

    // Set max_execute_time.
    pub fn set_max_execute_time(&self, val: u64) -> Result<()> {
        self.try_set_u64("max_execute_time", val, false)
    }

    // Get flight client timeout.
    pub fn get_flight_client_timeout(&self) -> Result<u64> {
        let key = "flight_client_timeout";
        self.try_get_u64(key)
    }

    // Get storage read buffer size.
    pub fn get_storage_read_buffer_size(&self) -> Result<u64> {
        let key = "storage_read_buffer_size";
        self.try_get_u64(key)
    }

    pub fn get_input_read_buffer_size(&self) -> Result<u64> {
        let key = "input_read_buffer_size";
        self.try_get_u64(key)
    }

    pub fn get_enable_bushy_join(&self) -> Result<u64> {
        static KEY: &str = "enable_bushy_join";
        self.try_get_u64(KEY)
    }

    pub fn get_timezone(&self) -> Result<String> {
        let key = "timezone";
        self.check_and_get_setting_value(key)
            .and_then(|v| v.user_setting.value.as_string())
    }

    // Get group by two level threshold
    pub fn get_group_by_two_level_threshold(&self) -> Result<u64> {
        let key = "group_by_two_level_threshold";
        self.try_get_u64(key)
    }

    // Set group by two level threshold
    pub fn set_group_by_two_level_threshold(&self, val: u64) -> Result<()> {
        let key = "group_by_two_level_threshold";
        self.try_set_u64(key, val, false)
    }

    pub fn get_max_inlist_to_or(&self) -> Result<u64> {
        let key = "max_inlist_to_or";
        self.try_get_u64(key)
    }

    pub fn get_unquoted_ident_case_sensitive(&self) -> Result<bool> {
        static KEY: &str = "unquoted_ident_case_sensitive";
        let v = self.try_get_u64(KEY)?;
        Ok(v != 0)
    }

    pub fn set_unquoted_ident_case_sensitive(&self, val: bool) -> Result<()> {
        static KEY: &str = "unquoted_ident_case_sensitive";
        let v = u64::from(val);
        self.try_set_u64(KEY, v, false)
    }

    pub fn get_quoted_ident_case_sensitive(&self) -> Result<bool> {
        static KEY: &str = "quoted_ident_case_sensitive";
        let v = self.try_get_u64(KEY)?;
        Ok(v != 0)
    }

    pub fn set_quoted_ident_case_sensitive(&self, val: bool) -> Result<()> {
        static KEY: &str = "quoted_ident_case_sensitive";
        let v = u64::from(val);
        self.try_set_u64(KEY, v, false)
    }

    pub fn get_enable_distributed_eval_index(&self) -> Result<bool> {
        static KEY: &str = "enable_distributed_eval_index";
        let v = self.try_get_u64(KEY)?;
        Ok(v != 0)
    }

    pub fn get_max_result_rows(&self) -> Result<u64> {
        static KEY: &str = "max_result_rows";
        let v = self.try_get_u64(KEY)?;
        Ok(v)
    }

    pub fn set_enable_distributed_eval_index(&self, val: bool) -> Result<()> {
        static KEY: &str = "enable_distributed_eval_index";
        let v = u64::from(val);
        self.try_set_u64(KEY, v, false)
    }

    pub fn get_enable_cbo(&self) -> Result<bool> {
        static KEY: &str = "enable_cbo";
        let v = self.try_get_u64(KEY)?;
        Ok(v != 0)
    }

    pub fn set_enable_cbo(&self, val: bool) -> Result<()> {
        static KEY: &str = "enable_cbo";
        let v = u64::from(val);
        self.try_set_u64(KEY, v, false)
    }

    pub fn get_runtime_filter(&self) -> Result<bool> {
        static KEY: &str = "enable_runtime_filter";
        let v = self.try_get_u64(KEY)?;
        Ok(v != 0)
    }

    pub fn set_runtime_filter(&self, val: bool) -> Result<()> {
        static KEY: &str = "enable_runtime_filter";
        let v = u64::from(val);
        self.try_set_u64(KEY, v, false)
    }

    pub fn get_prefer_broadcast_join(&self) -> Result<bool> {
        static KEY: &str = "prefer_broadcast_join";
        let v = self.try_get_u64(KEY)?;
        Ok(v != 0)
    }

    pub fn set_prefer_broadcast_join(&self, val: bool) -> Result<()> {
        static KEY: &str = "join_distribution_type";
        let v = u64::from(val);
        self.try_set_u64(KEY, v, false)
    }

    pub fn get_sql_dialect(&self) -> Result<Dialect> {
        let key = "sql_dialect";
        self.check_and_get_setting_value(key)
            .and_then(|v| v.user_setting.value.as_string())
            .map(|v| match &*v.to_lowercase() {
                "mysql" => Dialect::MySQL,
                "hive" => Dialect::Hive,
                _ => Dialect::PostgreSQL,
            })
    }

    pub fn get_collation(&self) -> Result<&str> {
        let key = "collation";
        self.check_and_get_setting_value(key)
            .and_then(|v| v.user_setting.value.as_string())
            .map(|v| match &*v.to_lowercase() {
                "utf8" => "utf8",
                _ => "binary",
            })
    }

    pub fn get_enable_hive_parquet_predict_pushdown(&self) -> Result<u64> {
        static KEY: &str = "enable_hive_parquet_predict_pushdown";
        self.try_get_u64(KEY)
    }

    pub fn get_hive_parquet_chunk_size(&self) -> Result<u64> {
        static KEY: &str = "hive_parquet_chunk_size";
        self.try_get_u64(KEY)
    }

    pub fn set_load_file_metadata_expire_hours(&self, val: u64) -> Result<()> {
        let key = "load_file_metadata_expire_hours";
        self.try_set_u64(key, val, false)
    }

    pub fn get_load_file_metadata_expire_hours(&self) -> Result<u64> {
        let key = "load_file_metadata_expire_hours";
        self.try_get_u64(key)
    }

    pub fn get_sandbox_tenant(&self) -> Result<String> {
        let key = "sandbox_tenant";
        self.check_and_get_setting_value(key)
            .and_then(|v| v.user_setting.value.as_string())
    }

    pub fn get_hide_options_in_show_create_table(&self) -> Result<bool> {
        let key = "hide_options_in_show_create_table";
        let v = self.try_get_u64(key)?;
        Ok(v != 0)
    }

    pub fn get_enable_query_result_cache(&self) -> Result<bool> {
        let key = "enable_query_result_cache";
        self.try_get_u64(key).map(|v| v != 0)
    }

    pub fn get_query_result_cache_max_bytes(&self) -> Result<usize> {
        let key = "query_result_cache_max_bytes";
        self.try_get_u64(key).map(|v| v as usize)
    }

    pub fn get_query_result_cache_ttl_secs(&self) -> Result<u64> {
        let key = "query_result_cache_ttl_secs";
        self.try_get_u64(key)
    }

    pub fn get_query_result_cache_allow_inconsistent(&self) -> Result<bool> {
        let key = "query_result_cache_allow_inconsistent";
        self.try_get_u64(key).map(|v| v != 0)
    }

    pub fn get_spilling_bytes_threshold_per_proc(&self) -> Result<usize> {
        let key = "spilling_bytes_threshold_per_proc";
        self.try_get_u64(key).map(|v| v as usize)
    }

    pub fn set_spilling_bytes_threshold_per_proc(&self, value: usize) -> Result<()> {
        let key = "spilling_bytes_threshold_per_proc";
        self.try_set_u64(key, value as u64, false)
    }

    pub fn has_setting(&self, key: &str) -> bool {
        self.settings.get(key).is_some()
    }

    pub fn check_and_get_setting_value(&self, key: &str) -> Result<SettingValue> {
        let setting = self
            .settings
            .get(key)
            .map(|e| e.value().clone())
            .ok_or_else(|| ErrorCode::UnknownVariable(format!("Unknown variable: {:?}", key)))?;
        Ok(setting)
    }

    pub fn check_and_get_default_value(&self, key: &str) -> Result<UserSettingValue> {
        let setting = self
            .settings
            .get(key)
            .map(|e| e.value().default_value.clone())
            .ok_or_else(|| ErrorCode::UnknownVariable(format!("Unknown variable: {:?}", key)))?;
        Ok(setting)
    }

    fn check_possible_values(&self, setting: &SettingValue, val: String) -> Result<String> {
        if let Some(possible_values) = &setting.possible_values {
            for possible_value in possible_values {
                if possible_value.to_lowercase() == val.to_lowercase() {
                    return Ok(possible_value.to_string());
                }
            }
            return Err(ErrorCode::WrongValueForVariable(format!(
                "Variable {:?} can't be set to the value of {:?}",
                setting.user_setting.name, val
            )));
        }
        Ok(val)
    }

    // Get u64 value, we don't get from the metasrv.
    fn try_get_u64(&self, key: &str) -> Result<u64> {
        let setting = self.check_and_get_setting_value(key)?;
        setting.user_setting.value.as_u64()
    }

    // Set u64 value to settings map, if is_global will write to metasrv.
    fn try_set_u64(&self, key: &str, val: u64, is_global: bool) -> Result<()> {
        let mut setting = self
            .settings
            .get_mut(key)
            .ok_or_else(|| ErrorCode::UnknownVariable(format!("Unknown variable: {:?}", key)))?;
        setting.user_setting.value = UserSettingValue::UInt64(val);

        if is_global {
            let tenant = self.tenant.clone();
            let user_setting = setting.user_setting.clone();
            let set_handle = GlobalIORuntime::instance().spawn(async move {
                UserApiProvider::instance()
                    .get_setting_api_client(&tenant)?
                    .set_setting(user_setting)
                    .await
            });

            let _ = block_on(set_handle).unwrap()?;
            setting.level = ScopeLevel::Global;
        }

        Ok(())
    }

    fn try_set_string(&self, key: &str, val: String, is_global: bool) -> Result<()> {
        let mut setting = self
            .settings
            .get_mut(key)
            .ok_or_else(|| ErrorCode::UnknownVariable(format!("Unknown variable: {:?}", key)))?;
        setting.user_setting.value = UserSettingValue::String(val);

        if is_global {
            let tenant = self.tenant.clone();
            let user_setting = setting.user_setting.clone();
            let set_handle = GlobalIORuntime::instance().spawn(async move {
                UserApiProvider::instance()
                    .get_setting_api_client(&tenant)?
                    .set_setting(user_setting)
                    .await
            });
            let _ = block_on(set_handle).unwrap()?;
            setting.level = ScopeLevel::Global;
        }

        Ok(())
    }

    pub async fn try_drop_setting(&self, key: &str) -> Result<()> {
        let mut setting = self
            .settings
            .get_mut(key)
            .ok_or_else(|| ErrorCode::UnknownVariable(format!("Unknown variable: {:?}", key)))?;

        let tenant = self.tenant.clone();
        let key = key.to_string();

        UserApiProvider::instance()
            .get_setting_api_client(&tenant)?
            .drop_setting(key.as_str(), MatchSeq::GE(1))
            .await?;

        setting.level = ScopeLevel::Session;
        Ok(())
    }

    fn set_setting_level(&self, key: &str, is_global: bool) -> Result<()> {
        let mut setting = self
            .settings
            .get_mut(key)
            .ok_or_else(|| ErrorCode::UnknownVariable(format!("Unknown variable: {:?}", key)))?;

        if is_global {
            setting.level = ScopeLevel::Global;
        }
        Ok(())
    }

    pub fn get_setting_level(&self, key: &str) -> Result<ScopeLevel> {
        let setting = self
            .settings
            .get_mut(key)
            .ok_or_else(|| ErrorCode::UnknownVariable(format!("Unknown variable: {:?}", key)))?;

        Ok(setting.level.clone())
    }

    pub fn get_setting_values(
        &self,
    ) -> Vec<(String, UserSettingValue, UserSettingValue, String, String)> {
        let mut v = self
            .settings
            .iter()
            .map(|e| {
                let (k, v) = e.pair();
                (
                    // Name.
                    k.to_owned(),
                    // Value.
                    v.user_setting.value.clone(),
                    // Default Value.
                    v.default_value.clone(),
                    // Scope level.
                    format!("{:?}", v.level),
                    // Desc.
                    v.desc.to_owned(),
                )
            })
            .collect_vec();
        v.sort_by(|a, b| a.0.cmp(&b.0));
        v
    }

    pub fn get_changed_settings(&self) -> Settings {
        let mut values = vec![];
        for v in self
            .settings
            .iter()
            .sorted_by(|a, b| a.key().cmp(b.key()))
            .map(|e| e.value().clone())
        {
            if v.user_setting.value != v.default_value {
                values.push(v);
            }
        }
        let new_settings = Arc::new(DashMap::new());
        {
            for value in values {
                let name = value.user_setting.name.clone();
                new_settings.insert(name, value.clone());
            }
        }
        Settings {
            settings: new_settings,
            tenant: self.tenant.clone(),
        }
    }

    pub fn apply_changed_settings(&self, changed_settings: Arc<Settings>) -> Result<()> {
        let values = changed_settings.get_setting_values();
        for value in values.into_iter() {
            let key = value.0;
            let mut val = self.settings.get_mut(&key).ok_or_else(|| {
                ErrorCode::UnknownVariable(format!("Unknown variable: {:?}", key))
            })?;
            val.user_setting.value = value.1.clone();
        }
        Ok(())
    }

    pub fn get_setting_values_short(&self) -> BTreeMap<String, UserSettingValue> {
        let mut result = BTreeMap::new();
        for e in self.settings.iter().sorted_by(|a, b| a.key().cmp(b.key())) {
            let (k, v) = e.pair();
            result.insert(k.to_owned(), v.user_setting.value.clone());
        }
        result
    }

    pub fn set_settings(&self, key: String, val: String, is_global: bool) -> Result<()> {
        let setting = self.check_and_get_setting_value(&key)?;
        let val = self.check_possible_values(&setting, val)?;

        match setting.user_setting.value {
            UserSettingValue::UInt64(_) => {
                // decimal 10 * 1.5 to string may result in string like "15.0"
                let val = if let Some(p) = val.find('.') {
                    if val[(p + 1)..].chars().all(|x| x == '0') {
                        &val[..p]
                    } else {
                        return Err(ErrorCode::BadArguments("not a integer"));
                    }
                } else {
                    &val[..]
                };
                let u64_val = val.parse::<u64>()?;
                self.try_set_u64(&key, u64_val, is_global)?
            }
            UserSettingValue::String(_) => {
                self.try_set_string(&key, val, is_global)?;
            }
        }
        Ok(())
    }

    pub fn set_batch_settings(
        &self,
        settings: &HashMap<String, String>,
        is_global: bool,
    ) -> Result<()> {
        for (k, v) in settings.iter() {
            if self.has_setting(k.as_str()) {
                self.set_settings(k.to_string(), v.to_string(), is_global)?
            }
        }
        Ok(())
    }
}
