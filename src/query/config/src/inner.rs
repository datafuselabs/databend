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
use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::str::FromStr;
use std::time::Duration;

use databend_common_base::base::mask_string;
use databend_common_base::base::GlobalUniqName;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_grpc::RpcClientConf;
use databend_common_grpc::RpcClientTlsConfig;
use databend_common_meta_app::principal::UserSettingValue;
use databend_common_meta_app::tenant::TenantQuota;
use databend_common_storage::StorageConfig;
use databend_common_tracing::Config as LogConfig;
use databend_common_users::idm_config::IDMConfig;

use super::config::Commands;
use super::config::Config;
use crate::background_config::InnerBackgroundConfig;

/// Inner config for query.
///
/// All function should implement based on this Config.
#[derive(Clone, Default, PartialEq, Eq)]
pub struct InnerConfig {
    pub subcommand: Option<Commands>,
    pub config_file: String,

    // Query engine config.
    pub query: QueryConfig,

    pub log: LogConfig,

    // Meta Service config.
    pub meta: MetaConfig,

    // Storage backend config.
    pub storage: StorageConfig,

    // external catalog config.
    // - Later, catalog information SHOULD be kept in KV Service
    // - currently only supports HIVE (via hive meta store)
    pub catalogs: HashMap<String, CatalogConfig>,

    // Cache Config
    pub cache: CacheConfig,

    // Background Config
    pub background: InnerBackgroundConfig,
}

impl InnerConfig {
    /// As requires by [RFC: Config Backward Compatibility](https://github.com/datafuselabs/databend/pull/5324), we will load user's config via wrapper [`ConfigV0`] and then convert from [`ConfigV0`] to [`InnerConfig`].
    ///
    /// In the future, we could have `ConfigV1` and `ConfigV2`.
    pub async fn load() -> Result<Self> {
        let mut cfg: Self = Config::load(true)?.try_into()?;

        // Handle the node_id for query node.
        cfg.query.node_id = GlobalUniqName::unique();
        Ok(cfg)
    }

    /// Check if current config is valid.
    pub async fn check(mut self) -> Result<Self> {
        // Handle auto detect for storage params.
        self.storage.params = self.storage.params.auto_detect().await?;

        // Only check meta config when cmd is empty.
        if self.subcommand.is_none() {
            self.meta.check_valid()?;
        }
        Ok(self)
    }

    /// # NOTE
    ///
    /// This function is served for tests only.
    pub fn load_for_test() -> Result<Self> {
        let cfg: Self = Config::load(false)?.try_into()?;
        Ok(cfg)
    }

    pub fn tls_query_cli_enabled(&self) -> bool {
        !self.query.rpc_tls_query_server_root_ca_cert.is_empty()
            && !self.query.rpc_tls_query_service_domain_name.is_empty()
    }

    pub fn tls_meta_cli_enabled(&self) -> bool {
        !self.meta.rpc_tls_meta_server_root_ca_cert.is_empty()
            && !self.meta.rpc_tls_meta_service_domain_name.is_empty()
    }

    pub fn flight_sql_tls_server_enabled(&self) -> bool {
        !self.query.flight_sql_tls_server_key.is_empty()
            && !self.query.flight_sql_tls_server_cert.is_empty()
    }

    pub fn tls_rpc_server_enabled(&self) -> bool {
        !self.query.rpc_tls_server_key.is_empty() && !self.query.rpc_tls_server_cert.is_empty()
    }

    /// Transform inner::Config into the Config.
    ///
    /// This function should only be used for end-users.
    ///
    /// For examples:
    ///
    /// - system config table
    /// - HTTP Handler
    /// - tests
    pub fn into_config(self) -> Config {
        Config::from(self)
    }
}

impl Debug for InnerConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("InnerConfig")
            .field("subcommand", &self.subcommand)
            .field("config_file", &self.config_file)
            .field("query", &self.query.sanitize())
            .field("log", &self.log)
            .field("meta", &self.meta)
            .field("storage", &self.storage)
            .field("catalogs", &self.catalogs)
            .field("cache", &self.cache)
            .field("background", &self.background)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct QueryConfig {
    /// Tenant id for get the information from the MetaSrv.
    pub tenant_id: String,
    /// ID for construct the cluster.
    pub cluster_id: String,
    // ID for the query node.
    // This only initialized when InnerConfig::load().
    pub node_id: String,
    pub num_cpus: u64,
    pub mysql_handler_host: String,
    pub mysql_handler_port: u16,
    pub mysql_handler_tcp_keepalive_timeout_secs: u64,
    pub mysql_tls_server_cert: String,
    pub mysql_tls_server_key: String,
    pub max_active_sessions: u64,
    pub max_server_memory_usage: u64,
    pub max_memory_limit_enabled: bool,
    pub clickhouse_http_handler_host: String,
    pub clickhouse_http_handler_port: u16,
    pub http_handler_host: String,
    pub http_handler_port: u16,
    pub http_handler_result_timeout_secs: u64,
    pub flight_api_address: String,
    pub flight_sql_handler_host: String,
    pub flight_sql_handler_port: u16,
    pub admin_api_address: String,
    pub metric_api_address: String,
    pub http_handler_tls_server_cert: String,
    pub http_handler_tls_server_key: String,
    pub http_handler_tls_server_root_ca_cert: String,
    pub api_tls_server_cert: String,
    pub api_tls_server_key: String,
    pub api_tls_server_root_ca_cert: String,
    pub flight_sql_tls_server_cert: String,
    pub flight_sql_tls_server_key: String,
    /// rpc server cert
    pub rpc_tls_server_cert: String,
    /// key for rpc server cert
    pub rpc_tls_server_key: String,
    /// Certificate for client to identify query rpc server
    pub rpc_tls_query_server_root_ca_cert: String,
    pub rpc_tls_query_service_domain_name: String,
    pub rpc_client_timeout_secs: u64,
    /// Table engine memory enabled
    pub table_engine_memory_enabled: bool,
    /// Graceful shutdown timeout
    pub shutdown_wait_timeout_ms: u64,
    pub max_query_log_size: usize,
    pub databend_enterprise_license: Option<String>,
    /// If in management mode, only can do some meta level operations(database/table/user/stage etc.) with metasrv.
    pub management_mode: bool,

    pub parquet_fast_read_bytes: Option<u64>,
    pub max_storage_io_requests: Option<u64>,

    pub jwt_key_file: String,
    pub jwt_key_files: Vec<String>,
    pub default_storage_format: String,
    pub default_compression: String,
    pub idm: IDMConfig,
    pub share_endpoint_address: String,
    pub share_endpoint_auth_token_file: String,
    pub tenant_quota: Option<TenantQuota>,
    pub internal_enable_sandbox_tenant: bool,
    pub internal_merge_on_read_mutation: bool,
    /// Disable some system load(For example system.configs) for cloud security.
    pub disable_system_table_load: bool,

    /// Max data retention time in days.
    pub data_retention_time_in_days_max: u64,

    /// (azure) openai
    pub openai_api_key: String,
    pub openai_api_version: String,
    pub openai_api_chat_base_url: String,
    pub openai_api_embedding_base_url: String,
    pub openai_api_embedding_model: String,
    pub openai_api_completion_model: String,

    pub enable_udf_server: bool,
    pub udf_server_allow_list: Vec<String>,

    pub cloud_control_grpc_server_address: Option<String>,
    pub cloud_control_grpc_timeout: u64,
    pub settings: HashMap<String, UserSettingValue>,
}

impl Default for QueryConfig {
    fn default() -> Self {
        Self {
            tenant_id: "admin".to_string(),
            cluster_id: "".to_string(),
            node_id: "".to_string(),
            num_cpus: 0,
            mysql_handler_host: "127.0.0.1".to_string(),
            mysql_handler_port: 3307,
            mysql_handler_tcp_keepalive_timeout_secs: 120,
            mysql_tls_server_cert: "".to_string(),
            mysql_tls_server_key: "".to_string(),
            max_active_sessions: 256,
            max_server_memory_usage: 0,
            max_memory_limit_enabled: false,
            clickhouse_http_handler_host: "127.0.0.1".to_string(),
            clickhouse_http_handler_port: 8124,
            http_handler_host: "127.0.0.1".to_string(),
            http_handler_port: 8000,
            http_handler_result_timeout_secs: 60,
            flight_api_address: "127.0.0.1:9090".to_string(),
            flight_sql_handler_host: "127.0.0.1".to_string(),
            flight_sql_handler_port: 8900,
            admin_api_address: "127.0.0.1:8080".to_string(),
            metric_api_address: "127.0.0.1:7070".to_string(),
            api_tls_server_cert: "".to_string(),
            api_tls_server_key: "".to_string(),
            api_tls_server_root_ca_cert: "".to_string(),
            flight_sql_tls_server_cert: "".to_string(),
            http_handler_tls_server_cert: "".to_string(),
            http_handler_tls_server_key: "".to_string(),
            http_handler_tls_server_root_ca_cert: "".to_string(),
            rpc_tls_server_cert: "".to_string(),
            rpc_tls_server_key: "".to_string(),
            rpc_tls_query_server_root_ca_cert: "".to_string(),
            rpc_tls_query_service_domain_name: "localhost".to_string(),
            rpc_client_timeout_secs: 0,
            table_engine_memory_enabled: true,
            shutdown_wait_timeout_ms: 5000,
            max_query_log_size: 10_000,
            databend_enterprise_license: None,
            management_mode: false,
            parquet_fast_read_bytes: None,
            max_storage_io_requests: None,
            jwt_key_file: "".to_string(),
            jwt_key_files: Vec::new(),
            default_storage_format: "auto".to_string(),
            default_compression: "auto".to_string(),
            idm: IDMConfig::default(),
            share_endpoint_address: "".to_string(),
            share_endpoint_auth_token_file: "".to_string(),
            tenant_quota: None,
            internal_enable_sandbox_tenant: false,
            internal_merge_on_read_mutation: false,
            disable_system_table_load: false,
            flight_sql_tls_server_key: "".to_string(),
            openai_api_chat_base_url: "https://api.openai.com/v1/".to_string(),
            openai_api_embedding_base_url: "https://api.openai.com/v1/".to_string(),
            openai_api_key: "".to_string(),
            openai_api_version: "".to_string(),
            openai_api_completion_model: "gpt-3.5-turbo".to_string(),
            openai_api_embedding_model: "text-embedding-ada-002".to_string(),
            enable_udf_server: false,
            udf_server_allow_list: Vec::new(),
            cloud_control_grpc_server_address: None,
            cloud_control_grpc_timeout: 0,
            data_retention_time_in_days_max: 90,
            settings: HashMap::new(),
        }
    }
}

impl QueryConfig {
    pub fn to_rpc_client_tls_config(&self) -> RpcClientTlsConfig {
        RpcClientTlsConfig {
            rpc_tls_server_root_ca_cert: self.rpc_tls_query_server_root_ca_cert.clone(),
            domain_name: self.rpc_tls_query_service_domain_name.clone(),
        }
    }

    pub fn sanitize(&self) -> Self {
        let mut sanitized = self.clone();
        sanitized.databend_enterprise_license = self
            .databend_enterprise_license
            .clone()
            .map(|s| mask_string(&s, 3));
        sanitized.openai_api_key = mask_string(&self.openai_api_key, 3);
        sanitized
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct MetaConfig {
    /// The dir to store persisted meta state for a embedded meta store
    pub embedded_dir: String,
    /// MetaStore endpoint address
    pub endpoints: Vec<String>,
    /// MetaStore backend user name
    pub username: String,
    /// MetaStore backend user password
    pub password: String,
    /// Timeout for each client request, in seconds
    pub client_timeout_in_second: u64,
    /// AutoSyncInterval is the interval to update endpoints with its latest members.
    /// 0 disables auto-sync. By default auto-sync is disabled.
    pub auto_sync_interval: u64,
    pub unhealth_endpoint_evict_time: u64,
    /// Certificate for client to identify meta rpc serve
    pub rpc_tls_meta_server_root_ca_cert: String,
    pub rpc_tls_meta_service_domain_name: String,
}

impl Default for MetaConfig {
    fn default() -> Self {
        Self {
            embedded_dir: "".to_string(),
            endpoints: vec![],
            username: "root".to_string(),
            password: "".to_string(),
            client_timeout_in_second: 10,
            auto_sync_interval: 0,
            unhealth_endpoint_evict_time: 120,
            rpc_tls_meta_server_root_ca_cert: "".to_string(),
            rpc_tls_meta_service_domain_name: "localhost".to_string(),
        }
    }
}

impl MetaConfig {
    pub fn is_embedded_meta(&self) -> Result<bool> {
        Ok(!self.embedded_dir.is_empty())
    }

    pub fn check_valid(&self) -> Result<()> {
        let has_embedded_dir = !self.embedded_dir.is_empty();
        let has_remote = !self.endpoints.is_empty();
        if has_embedded_dir && has_remote {
            return Err(ErrorCode::InvalidConfig(
                "Can not set embedded_dir and endpoints at the same time, embedded_dir is only for testing, please remove this config".to_string(),
            ));
        }

        if !has_embedded_dir && !has_remote {
            return Err(ErrorCode::InvalidConfig(
                "Please set your meta endpoints config: endpoints = [<your-meta-service-endpoints>]".to_string(),
            ));
        }

        Ok(())
    }

    pub fn is_tls_enabled(&self) -> bool {
        !self.rpc_tls_meta_server_root_ca_cert.is_empty()
            && !self.rpc_tls_meta_service_domain_name.is_empty()
    }

    pub fn to_rpc_client_tls_config(&self) -> RpcClientTlsConfig {
        RpcClientTlsConfig {
            rpc_tls_server_root_ca_cert: self.rpc_tls_meta_server_root_ca_cert.to_string(),
            domain_name: self.rpc_tls_meta_service_domain_name.to_string(),
        }
    }

    pub fn to_meta_grpc_client_conf(&self) -> RpcClientConf {
        RpcClientConf {
            endpoints: self.endpoints.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            tls_conf: if self.is_tls_enabled() {
                Some(self.to_rpc_client_tls_config())
            } else {
                None
            },

            timeout: Some(Duration::from_secs(self.client_timeout_in_second)),
            auto_sync_interval: if self.auto_sync_interval > 0 {
                Some(Duration::from_secs(self.auto_sync_interval))
            } else {
                None
            },
            unhealthy_endpoint_evict_time: Duration::from_secs(self.unhealth_endpoint_evict_time),
        }
    }
}

impl Debug for MetaConfig {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("MetaConfig")
            .field("endpoints", &self.endpoints)
            .field("username", &self.username)
            .field("password", &mask_string(&self.password, 3))
            .field("embedded_dir", &self.embedded_dir)
            .field("client_timeout_in_second", &self.client_timeout_in_second)
            .field("auto_sync_interval", &self.auto_sync_interval)
            .field(
                "unhealth_endpoint_evict_time",
                &self.unhealth_endpoint_evict_time,
            )
            .field(
                "rpc_tls_meta_server_root_ca_cert",
                &self.rpc_tls_meta_server_root_ca_cert,
            )
            .field(
                "rpc_tls_meta_service_domain_name",
                &self.rpc_tls_meta_service_domain_name,
            )
            .finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CatalogConfig {
    Hive(CatalogHiveConfig),
}

// TODO: add compat protocol support
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ThriftProtocol {
    Binary,
}

impl FromStr for ThriftProtocol {
    type Err = ErrorCode;

    fn from_str(s: &str) -> Result<ThriftProtocol> {
        let s = s.to_lowercase();
        match s.as_str() {
            "binary" => Ok(ThriftProtocol::Binary),
            _ => Err(ErrorCode::StorageOther("invalid thrift protocol spec")),
        }
    }
}

impl Display for ThriftProtocol {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Binary => write!(f, "binary"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CatalogHiveConfig {
    pub metastore_address: String,
    pub protocol: ThriftProtocol,
}

impl Default for CatalogHiveConfig {
    fn default() -> Self {
        Self {
            metastore_address: "127.0.0.1:9083".to_string(),
            protocol: ThriftProtocol::Binary,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalConfig {
    pub sql: String,
    // name1=filepath1,name2=filepath2
    pub table: String,
}

impl Default for LocalConfig {
    fn default() -> Self {
        Self {
            sql: "SELECT 1".to_string(),
            table: "".to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CacheConfig {
    /// Enable table meta cache. Default is enabled. Set it to false to disable all the table meta caches
    pub enable_table_meta_cache: bool,

    /// Max number of cached table snapshot
    pub table_meta_snapshot_count: u64,

    /// Max size(in bytes) of cached table segment
    pub table_meta_segment_bytes: u64,

    /// Max number of cached table segment
    pub table_meta_statistic_count: u64,

    /// Enable bloom index cache. Default is enabled. Set it to false to disable all the bloom index caches
    pub enable_table_index_bloom: bool,

    /// Max number of cached bloom index meta objects. Set it to 0 to disable it.
    pub table_bloom_index_meta_count: u64,

    /// Max number of cached prune partitions objects. Set it to 0 to disable it.
    pub table_prune_partitions_count: u64,

    /// Max number of cached bloom index filters. Set it to 0 to disable it.
    // One bloom index filter per column of data block being indexed will be generated if necessary.
    //
    // For example, a table of 1024 columns, with 800 data blocks, a query that triggers a full
    // table filter on 2 columns, might populate 2 * 800 bloom index filter cache items (at most)
    pub table_bloom_index_filter_count: u64,

    /// Max bytes of cached bloom index filters used. Set it to 0 to disable it.
    // One bloom index filter per column of data block being indexed will be generated if necessary.
    pub table_bloom_index_filter_size: u64,

    pub data_cache_storage: CacheStorageTypeConfig,

    /// Max size of external cache population queue length
    ///
    /// the items being queued reference table column raw data, which are
    /// un-deserialized and usually compressed (depends on table compression options).
    ///
    /// - please monitor the 'table_data_cache_population_pending_count' metric
    ///   if it is too high, and takes too much memory, please consider decrease this value
    ///
    /// - please monitor the 'population_overflow_count' metric
    ///   if it keeps increasing, and disk cache hits rate is not as expected. please consider
    ///   increase this value.
    pub table_data_cache_population_queue_size: u32,

    /// Storage that hold the raw data caches
    pub disk_cache_config: DiskCacheConfig,

    /// Max size of in memory table column object cache. By default it is 0 (disabled)
    ///
    /// CAUTION: The cache items are deserialized table column objects, may take a lot of memory.
    ///
    /// Only if query nodes have plenty of un-utilized memory, the working set can be fitted into,
    /// and the access pattern will benefit from caching, consider enabled this cache.
    pub table_data_deserialized_data_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CacheStorageTypeConfig {
    None,
    Disk,
    // Redis,
}

impl Default for CacheStorageTypeConfig {
    fn default() -> Self {
        Self::None
    }
}

impl ToString for CacheStorageTypeConfig {
    fn to_string(&self) -> String {
        match self {
            CacheStorageTypeConfig::None => "none".to_string(),
            CacheStorageTypeConfig::Disk => "disk".to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DiskCacheConfig {
    /// Max bytes of cached raw table data. Default 20GB, set it to 0 to disable it.
    pub max_bytes: u64,

    /// Table disk cache root path
    pub path: String,
}

impl Default for DiskCacheConfig {
    fn default() -> Self {
        Self {
            max_bytes: 21474836480,
            path: "./.databend/_cache".to_owned(),
        }
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enable_table_meta_cache: true,
            table_meta_snapshot_count: 256,
            table_meta_segment_bytes: 1073741824,
            table_meta_statistic_count: 256,
            enable_table_index_bloom: true,
            table_bloom_index_meta_count: 3000,
            table_bloom_index_filter_count: 0,
            table_bloom_index_filter_size: 2147483648,
            table_prune_partitions_count: 256,
            data_cache_storage: Default::default(),
            table_data_cache_population_queue_size: 0,
            disk_cache_config: Default::default(),
            table_data_deserialized_data_bytes: 0,
        }
    }
}
