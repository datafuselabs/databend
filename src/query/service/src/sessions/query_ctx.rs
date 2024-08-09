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

use std::any::Any;
use std::cmp::min;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::future::Future;
use std::str::FromStr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

use chrono::Utc;
use chrono_tz::Tz;
use dashmap::mapref::multiple::RefMulti;
use dashmap::DashMap;
use databend_common_base::base::tokio::task::JoinHandle;
use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_base::runtime::TrySpawn;
use databend_common_catalog::lock::LockTableOption;
use databend_common_catalog::merge_into_join::MergeIntoJoin;
use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::ParquetReadOptions;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::StageTableInfo;
use databend_common_catalog::query_kind::QueryKind;
use databend_common_catalog::runtime_filter_info::RuntimeFilterInfo;
use databend_common_catalog::statistics::data_cache_statistics::DataCacheMetrics;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::FilteredCopyFiles;
use databend_common_catalog::table_context::MaterializedCtesBlocks;
use databend_common_catalog::table_context::StageAttachment;
use databend_common_config::GlobalConfig;
use databend_common_config::DATABEND_COMMIT_VERSION;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::date_helper::TzFactory;
use databend_common_expression::BlockThresholds;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_io::prelude::FormatSettings;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::OnErrorMode;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::StageFileFormatType;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::principal::UserDefinedConnection;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_meta_app::principal::COPY_MAX_FILES_COMMIT_MSG;
use databend_common_meta_app::principal::COPY_MAX_FILES_PER_COMMIT;
use databend_common_meta_app::schema::GetTableCopiedFileReq;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_app::tenant::Tenant;
use databend_common_metrics::storage::*;
use databend_common_pipeline_core::processors::PlanProfile;
use databend_common_pipeline_core::InputError;
use databend_common_pipeline_core::LockGuard;
use databend_common_settings::Settings;
use databend_common_sql::IndexType;
use databend_common_storage::CopyStatus;
use databend_common_storage::DataOperator;
use databend_common_storage::FileStatus;
use databend_common_storage::MultiTableInsertStatus;
use databend_common_storage::MutationStatus;
use databend_common_storage::StageFileInfo;
use databend_common_storage::StageFilesInfo;
use databend_common_storage::StorageMetrics;
use databend_common_storages_delta::DeltaTable;
use databend_common_storages_fuse::TableContext;
use databend_common_storages_iceberg::IcebergTable;
use databend_common_storages_orc::OrcTable;
use databend_common_storages_parquet::ParquetRSTable;
use databend_common_storages_result_cache::ResultScan;
use databend_common_storages_stage::StageTable;
use databend_common_users::GrantObjectVisibilityChecker;
use databend_common_users::UserApiProvider;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_txn::TxnManagerRef;
use log::debug;
use log::info;
use parking_lot::Mutex;
use parking_lot::RwLock;
use xorf::BinaryFuse16;

use crate::catalogs::Catalog;
use crate::clusters::Cluster;
use crate::locks::LockManager;
use crate::pipelines::executor::PipelineExecutor;
use crate::servers::flight::v1::exchange::DataExchangeManager;
use crate::sessions::query_affect::QueryAffect;
use crate::sessions::ProcessInfo;
use crate::sessions::QueriesQueueManager;
use crate::sessions::QueryContextShared;
use crate::sessions::Session;
use crate::sessions::SessionManager;
use crate::sessions::SessionType;
use crate::sql::binder::get_storage_params_from_options;
use crate::storages::Table;

const MYSQL_VERSION: &str = "8.0.26";
const CLICKHOUSE_VERSION: &str = "8.12.14";
const COPIED_FILES_FILTER_BATCH_SIZE: usize = 1000;

#[derive(Clone)]
pub struct QueryContext {
    version: String,
    mysql_version: String,
    clickhouse_version: String,
    block_threshold: Arc<RwLock<BlockThresholds>>,
    partition_queue: Arc<RwLock<VecDeque<PartInfoPtr>>>,
    shared: Arc<QueryContextShared>,
    query_settings: Arc<Settings>,
    fragment_id: Arc<AtomicUsize>,
    // Used by synchronized generate aggregating indexes when new data written.
    inserted_segment_locs: Arc<RwLock<HashSet<Location>>>,
    snapshot: Arc<RwLock<Option<Arc<TableSnapshot>>>>,
    lazy_mutaion_delete: Arc<RwLock<bool>>,
}

impl QueryContext {
    pub fn create_from(other: Arc<QueryContext>) -> Arc<QueryContext> {
        QueryContext::create_from_shared(other.shared.clone())
    }

    pub fn create_from_shared(shared: Arc<QueryContextShared>) -> Arc<QueryContext> {
        debug!("Create QueryContext");

        let tenant = GlobalConfig::instance().query.tenant_id.clone();
        let query_settings = Settings::create(tenant);
        Arc::new(QueryContext {
            partition_queue: Arc::new(RwLock::new(VecDeque::new())),
            version: format!("Databend Query {}", *DATABEND_COMMIT_VERSION),
            mysql_version: format!("{}-{}", MYSQL_VERSION, *DATABEND_COMMIT_VERSION),
            clickhouse_version: CLICKHOUSE_VERSION.to_string(),
            shared,
            query_settings,
            fragment_id: Arc::new(AtomicUsize::new(0)),
            inserted_segment_locs: Arc::new(RwLock::new(HashSet::new())),
            block_threshold: Arc::new(RwLock::new(BlockThresholds::default())),
            snapshot: Arc::new(RwLock::new(None)),
            lazy_mutaion_delete: Arc::new(RwLock::new(false)),
        })
    }

    /// Build fuse/system normal table by table info.
    pub fn build_table_by_table_info(
        &self,
        table_info: &TableInfo,
        table_args: Option<TableArgs>,
    ) -> Result<Arc<dyn Table>> {
        let catalog = self
            .shared
            .catalog_manager
            .build_catalog(table_info.catalog_info.clone(), self.txn_mgr())?;
        match table_args {
            None => {
                let table = catalog.get_table_by_info(table_info);
                if table.is_err() {
                    let table_function = catalog
                        .get_table_function(&table_info.name, TableArgs::new_positioned(vec![]));

                    if table_function.is_err() {
                        table
                    } else {
                        Ok(table_function?.as_table())
                    }
                } else {
                    table
                }
            }

            Some(table_args) => Ok(catalog
                .get_table_function(&table_info.name, table_args)?
                .as_table()),
        }
    }

    // Build external table by stage info, this is used in:
    // COPY INTO t1 FROM 's3://'
    // 's3://' here is a s3 external stage, and build it to the external table.
    fn build_external_by_table_info(
        &self,
        table_info: &StageTableInfo,
        _table_args: Option<TableArgs>,
    ) -> Result<Arc<dyn Table>> {
        StageTable::try_create(table_info.clone())
    }

    #[async_backtrace::framed]
    pub async fn set_current_database(&self, new_database_name: String) -> Result<()> {
        let tenant_id = self.get_tenant();
        let catalog = self
            .get_catalog(self.get_current_catalog().as_str())
            .await?;
        match catalog.get_database(&tenant_id, &new_database_name).await {
            Ok(_) => self.shared.set_current_database(new_database_name),
            Err(_) => {
                return Err(ErrorCode::UnknownDatabase(format!(
                    "Cannot use database '{}': It does not exist.",
                    new_database_name
                )));
            }
        };

        Ok(())
    }

    pub fn attach_table(&self, catalog: &str, database: &str, name: &str, table: Arc<dyn Table>) {
        self.shared.attach_table(catalog, database, name, table)
    }

    pub fn get_exchange_manager(&self) -> Arc<DataExchangeManager> {
        DataExchangeManager::instance()
    }

    // Get the current session.
    pub fn get_current_session(&self) -> Arc<Session> {
        self.shared.session.clone()
    }

    // Get one session by session id.
    pub fn get_session_by_id(self: &Arc<Self>, id: &str) -> Option<Arc<Session>> {
        SessionManager::instance().get_session_by_id(id)
    }

    // Get session id by mysql connection id.
    pub fn get_id_by_mysql_conn_id(self: &Arc<Self>, conn_id: &Option<u32>) -> Option<String> {
        SessionManager::instance().get_id_by_mysql_conn_id(conn_id)
    }

    // Get all the processes list info.
    pub fn get_processes_info(self: &Arc<Self>) -> Vec<ProcessInfo> {
        SessionManager::instance().processes_info()
    }

    /// Get the client socket address.
    pub fn get_client_address(&self) -> Option<String> {
        self.shared.session.session_ctx.get_client_host()
    }

    pub fn get_affect(self: &Arc<Self>) -> Option<QueryAffect> {
        self.shared.get_affect()
    }

    pub fn pop_warnings(&self) -> Vec<String> {
        self.shared.pop_warnings()
    }

    pub fn get_data_metrics(&self) -> StorageMetrics {
        self.shared.get_data_metrics()
    }

    pub fn set_affect(self: &Arc<Self>, affect: QueryAffect) {
        self.shared.set_affect(affect)
    }

    pub fn set_id(&self, id: String) {
        *self.shared.init_query_id.write() = id;
    }

    pub fn set_executor(&self, weak_ptr: Arc<PipelineExecutor>) -> Result<()> {
        self.shared.set_executor(weak_ptr)
    }

    pub fn attach_stage(&self, attachment: StageAttachment) {
        self.shared.attach_stage(attachment);
    }

    pub fn set_ua(&self, ua: String) {
        *self.shared.user_agent.write() = ua;
    }

    pub fn get_ua(&self) -> String {
        let ua = self.shared.user_agent.read();
        ua.clone()
    }

    pub fn get_query_duration_ms(&self) -> i64 {
        let query_start_time = convert_query_log_timestamp(self.shared.created_time);
        let finish_time = *self.shared.finish_time.read();
        let finish_time = finish_time.unwrap_or_else(SystemTime::now);
        let finish_time = convert_query_log_timestamp(finish_time);
        (finish_time - query_start_time) / 1_000
    }

    pub fn get_created_time(&self) -> SystemTime {
        self.shared.created_time
    }

    pub fn set_finish_time(&self, time: SystemTime) {
        *self.shared.finish_time.write() = Some(time)
    }

    pub fn evict_table_from_cache(&self, catalog: &str, database: &str, table: &str) -> Result<()> {
        self.shared.evict_table_from_cache(catalog, database, table)
    }

    pub fn clear_tables_cache(&self) {
        self.shared.clear_tables_cache()
    }
}

#[async_trait::async_trait]
impl TableContext for QueryContext {
    fn as_any(&self) -> &dyn Any {
        self
    }
    /// Build a table instance the plan wants to operate on.
    ///
    /// A plan just contains raw information about a table or table function.
    /// This method builds a `dyn Table`, which provides table specific io methods the plan needs.
    fn build_table_from_source_plan(&self, plan: &DataSourcePlan) -> Result<Arc<dyn Table>> {
        match &plan.source_info {
            DataSourceInfo::TableSource(table_info) => {
                self.build_table_by_table_info(table_info, plan.tbl_args.clone())
            }
            DataSourceInfo::StageSource(stage_info) => {
                self.build_external_by_table_info(stage_info, plan.tbl_args.clone())
            }
            DataSourceInfo::ParquetSource(table_info) => ParquetRSTable::from_info(table_info),
            DataSourceInfo::ResultScanSource(table_info) => ResultScan::from_info(table_info),
            DataSourceInfo::ORCSource(table_info) => OrcTable::from_info(table_info),
        }
    }

    fn incr_total_scan_value(&self, value: ProgressValues) {
        self.shared.total_scan_values.as_ref().incr(&value);
    }

    fn get_total_scan_value(&self) -> ProgressValues {
        self.shared.total_scan_values.as_ref().get_values()
    }

    fn get_scan_progress(&self) -> Arc<Progress> {
        self.shared.scan_progress.clone()
    }

    fn get_scan_progress_value(&self) -> ProgressValues {
        self.shared.scan_progress.as_ref().get_values()
    }

    fn get_write_progress(&self) -> Arc<Progress> {
        self.shared.write_progress.clone()
    }

    fn get_join_spill_progress(&self) -> Arc<Progress> {
        self.shared.join_spill_progress.clone()
    }

    fn get_aggregate_spill_progress(&self) -> Arc<Progress> {
        self.shared.agg_spill_progress.clone()
    }

    fn get_group_by_spill_progress(&self) -> Arc<Progress> {
        self.shared.group_by_spill_progress.clone()
    }

    fn get_window_partition_spill_progress(&self) -> Arc<Progress> {
        self.shared.window_partition_spill_progress.clone()
    }

    fn get_write_progress_value(&self) -> ProgressValues {
        self.shared.write_progress.as_ref().get_values()
    }

    fn get_join_spill_progress_value(&self) -> ProgressValues {
        self.shared.join_spill_progress.as_ref().get_values()
    }

    fn get_aggregate_spill_progress_value(&self) -> ProgressValues {
        self.shared.agg_spill_progress.as_ref().get_values()
    }

    fn get_group_by_spill_progress_value(&self) -> ProgressValues {
        self.shared.group_by_spill_progress.as_ref().get_values()
    }

    fn get_window_partition_spill_progress_value(&self) -> ProgressValues {
        self.shared
            .window_partition_spill_progress
            .as_ref()
            .get_values()
    }

    fn get_result_progress(&self) -> Arc<Progress> {
        self.shared.result_progress.clone()
    }

    fn get_result_progress_value(&self) -> ProgressValues {
        self.shared.result_progress.as_ref().get_values()
    }

    fn get_status_info(&self) -> String {
        let status = self.shared.status.read();
        status.clone()
    }

    fn set_status_info(&self, info: &str) {
        // set_status_info is not called frequently, so we can use info! here.
        // make it easier to match the status to the log.
        info!("{}", info);
        let mut status = self.shared.status.write();
        *status = info.to_string();
    }

    fn get_data_cache_metrics(&self) -> &DataCacheMetrics {
        self.shared.get_query_cache_metrics()
    }

    fn get_partition(&self) -> Option<PartInfoPtr> {
        if let Some(part) = self.partition_queue.write().pop_front() {
            Profile::record_usize_profile(ProfileStatisticsName::ScanPartitions, 1);
            return Some(part);
        }

        None
    }

    fn get_partitions(&self, num: usize) -> Vec<PartInfoPtr> {
        let mut res = Vec::with_capacity(num);
        let mut queue_guard = self.partition_queue.write();

        for _index in 0..num {
            match queue_guard.pop_front() {
                None => {
                    break;
                }
                Some(part) => {
                    res.push(part);
                }
            };
        }

        Profile::record_usize_profile(ProfileStatisticsName::ScanPartitions, res.len());

        res
    }

    // Update the context partition pool from the pipeline builder.
    fn set_partitions(&self, partitions: Partitions) -> Result<()> {
        let mut partition_queue = self.partition_queue.write();

        partition_queue.clear();
        for part in partitions.partitions {
            partition_queue.push_back(part);
        }
        Ok(())
    }

    fn set_table_snapshot(&self, snapshot: Arc<TableSnapshot>) {
        *self.snapshot.write() = Some(snapshot);
    }

    fn get_table_snapshot(&self) -> Option<Arc<TableSnapshot>> {
        self.snapshot.read().clone()
    }

    fn set_lazy_mutation_delete(&self, lazy: bool) {
        *self.lazy_mutaion_delete.write() = lazy;
    }

    fn get_lazy_mutation_delete(&self) -> bool {
        *self.lazy_mutaion_delete.read()
    }

    fn partition_num(&self) -> usize {
        self.partition_queue.read().len()
    }

    fn add_partitions_sha(&self, s: String) {
        let mut shas = self.shared.partitions_shas.write();
        shas.push(s);
    }

    fn get_partitions_shas(&self) -> Vec<String> {
        let mut sha = self.shared.partitions_shas.read().clone();
        // Sort to make sure the SHAs are stable for the same query.
        sha.sort();
        sha
    }

    fn get_cacheable(&self) -> bool {
        self.shared.cacheable.load(Ordering::Acquire)
    }

    fn set_cacheable(&self, cacheable: bool) {
        self.shared.cacheable.store(cacheable, Ordering::Release);
    }

    fn get_can_scan_from_agg_index(&self) -> bool {
        self.shared.can_scan_from_agg_index.load(Ordering::Acquire)
    }

    fn set_can_scan_from_agg_index(&self, enable: bool) {
        self.shared
            .can_scan_from_agg_index
            .store(enable, Ordering::Release);
    }

    fn get_enable_sort_spill(&self) -> bool {
        self.shared.enable_sort_spill.load(Ordering::Acquire)
    }

    fn set_enable_sort_spill(&self, enable: bool) {
        self.shared
            .enable_sort_spill
            .store(enable, Ordering::Release);
    }

    // get a hint at the number of blocks that need to be compacted.
    fn get_compaction_num_block_hint(&self) -> u64 {
        self.shared
            .num_fragmented_block_hint
            .load(Ordering::Acquire)
    }

    // set a hint at the number of blocks that need to be compacted.
    fn set_compaction_num_block_hint(&self, hint: u64) {
        self.shared
            .num_fragmented_block_hint
            .store(hint, Ordering::Release);
    }

    fn attach_query_str(&self, kind: QueryKind, query: String) {
        self.shared.attach_query_str(kind, query);
    }

    fn attach_query_hash(&self, text_hash: String, parameterized_hash: String) {
        self.shared.attach_query_hash(text_hash, parameterized_hash);
    }

    /// Get the session running query.
    fn get_query_str(&self) -> String {
        self.shared.get_query_str()
    }

    fn get_query_parameterized_hash(&self) -> String {
        self.shared.get_query_parameterized_hash()
    }

    fn get_query_text_hash(&self) -> String {
        self.shared.get_query_text_hash()
    }

    fn get_fragment_id(&self) -> usize {
        self.fragment_id.fetch_add(1, Ordering::Release)
    }

    #[async_backtrace::framed]
    async fn get_catalog(&self, catalog_name: &str) -> Result<Arc<dyn Catalog>> {
        self.shared
            .catalog_manager
            .get_catalog(
                self.get_tenant().tenant_name(),
                catalog_name.as_ref(),
                self.txn_mgr(),
            )
            .await
    }

    fn get_default_catalog(&self) -> Result<Arc<dyn Catalog>> {
        self.shared
            .catalog_manager
            .get_default_catalog(self.txn_mgr())
    }

    fn get_id(&self) -> String {
        self.shared.init_query_id.as_ref().read().clone()
    }

    fn get_current_catalog(&self) -> String {
        self.shared.get_current_catalog()
    }

    fn check_aborting(&self) -> Result<()> {
        self.shared.check_aborting()
    }

    fn get_error(&self) -> Option<ErrorCode> {
        self.shared.get_error()
    }

    fn push_warning(&self, warn: String) {
        self.shared.push_warning(warn)
    }

    fn get_current_database(&self) -> String {
        self.shared.get_current_database()
    }

    fn get_current_user(&self) -> Result<UserInfo> {
        self.shared.get_current_user()
    }

    fn get_current_role(&self) -> Option<RoleInfo> {
        self.shared.get_current_role()
    }
    async fn get_available_roles(&self) -> Result<Vec<RoleInfo>> {
        self.get_current_session().get_all_available_roles().await
    }

    async fn get_all_effective_roles(&self) -> Result<Vec<RoleInfo>> {
        self.get_current_session().get_all_effective_roles().await
    }

    async fn validate_privilege(
        &self,
        object: &GrantObject,
        privilege: UserPrivilegeType,
        check_current_role_only: bool,
    ) -> Result<()> {
        self.get_current_session()
            .validate_privilege(object, privilege, check_current_role_only)
            .await
    }

    fn get_current_session_id(&self) -> String {
        self.get_current_session().get_id()
    }

    async fn get_visibility_checker(&self) -> Result<GrantObjectVisibilityChecker> {
        self.shared.session.get_visibility_checker().await
    }

    fn get_fuse_version(&self) -> String {
        let session = self.get_current_session();
        match session.get_type() {
            SessionType::ClickHouseHttpHandler => self.clickhouse_version.clone(),
            SessionType::MySQL => self.mysql_version.clone(),
            _ => self.version.clone(),
        }
    }

    fn get_format_settings(&self) -> Result<FormatSettings> {
        let tz = self.get_settings().get_timezone()?;
        let timezone = tz.parse::<Tz>().map_err(|_| {
            ErrorCode::InvalidTimezone("Timezone has been checked and should be valid")
        })?;
        let geometry_format = self.get_settings().get_geometry_output_format()?;
        let format_null_as_str = self.get_settings().get_format_null_as_str()?;
        let enable_dst_hour_fix = self.get_settings().get_enable_dst_hour_fix()?;
        let format = FormatSettings {
            timezone,
            geometry_format,
            enable_dst_hour_fix,
            format_null_as_str,
        };
        Ok(format)
    }

    fn get_tenant(&self) -> Tenant {
        self.shared.get_tenant()
    }

    fn get_query_kind(&self) -> QueryKind {
        self.shared.get_query_kind()
    }

    fn get_function_context(&self) -> Result<FunctionContext> {
        let settings = self.get_settings();
        let external_server_connect_timeout_secs =
            settings.get_external_server_connect_timeout_secs()?;
        let external_server_request_timeout_secs =
            settings.get_external_server_request_timeout_secs()?;
        let external_server_request_batch_rows =
            settings.get_external_server_request_batch_rows()?;

        let tz = settings.get_timezone()?;
        let tz = TzFactory::instance().get_by_name(&tz)?;
        let now = Utc::now();
        let numeric_cast_option = settings.get_numeric_cast_option()?;
        let rounding_mode = numeric_cast_option.as_str() == "rounding";
        let disable_variant_check = settings.get_disable_variant_check()?;
        let geometry_output_format = settings.get_geometry_output_format()?;
        let parse_datetime_ignore_remainder = settings.get_parse_datetime_ignore_remainder()?;
        let enable_dst_hour_fix = settings.get_enable_dst_hour_fix()?;
        let enable_strict_datetime_parser = settings.get_enable_strict_datetime_parser()?;
        let query_config = &GlobalConfig::instance().query;

        Ok(FunctionContext {
            tz,
            now,
            rounding_mode,
            disable_variant_check,

            openai_api_key: query_config.openai_api_key.clone(),
            openai_api_version: query_config.openai_api_version.clone(),
            openai_api_chat_base_url: query_config.openai_api_chat_base_url.clone(),
            openai_api_embedding_base_url: query_config.openai_api_embedding_base_url.clone(),
            openai_api_embedding_model: query_config.openai_api_embedding_model.clone(),
            openai_api_completion_model: query_config.openai_api_completion_model.clone(),

            external_server_connect_timeout_secs,
            external_server_request_timeout_secs,
            external_server_request_batch_rows,
            geometry_output_format,
            parse_datetime_ignore_remainder,
            enable_dst_hour_fix,
            enable_strict_datetime_parser,
        })
    }

    fn get_connection_id(&self) -> String {
        self.shared.get_connection_id()
    }

    fn get_settings(&self) -> Arc<Settings> {
        if !self.query_settings.is_changed() {
            unsafe {
                self.query_settings
                    .unchecked_apply_changes(self.shared.get_settings().changes());
            }
        }

        self.query_settings.clone()
    }

    fn get_shared_settings(&self) -> Arc<Settings> {
        self.shared.get_settings()
    }

    fn get_cluster(&self) -> Arc<Cluster> {
        self.shared.get_cluster()
    }

    // Get all the processes list info.
    fn get_processes_info(&self) -> Vec<ProcessInfo> {
        SessionManager::instance().processes_info()
    }

    fn get_queued_queries(&self) -> Vec<ProcessInfo> {
        let queries = QueriesQueueManager::instance()
            .list()
            .iter()
            .map(|x| x.query_id.clone())
            .collect::<HashSet<_>>();

        SessionManager::instance()
            .processes_info()
            .into_iter()
            .filter(|x| match &x.current_query_id {
                None => false,
                Some(query_id) => queries.contains(query_id),
            })
            .collect::<Vec<_>>()
    }

    // Get Stage Attachment.
    fn get_stage_attachment(&self) -> Option<StageAttachment> {
        self.shared.get_stage_attachment()
    }

    fn get_last_query_id(&self, index: i32) -> String {
        self.shared.session.session_ctx.get_last_query_id(index)
    }

    fn get_query_id_history(&self) -> HashSet<String> {
        self.shared.session.session_ctx.get_query_id_history()
    }

    fn get_result_cache_key(&self, query_id: &str) -> Option<String> {
        self.shared
            .session
            .session_ctx
            .get_query_result_cache_key(query_id)
    }

    fn set_query_id_result_cache(&self, query_id: String, result_cache_key: String) {
        self.shared
            .session
            .session_ctx
            .update_query_ids_results(query_id, Some(result_cache_key))
    }

    fn get_on_error_map(&self) -> Option<Arc<DashMap<String, HashMap<u16, InputError>>>> {
        self.shared.get_on_error_map()
    }

    fn set_on_error_map(&self, map: Arc<DashMap<String, HashMap<u16, InputError>>>) {
        self.shared.set_on_error_map(map);
    }

    fn get_on_error_mode(&self) -> Option<OnErrorMode> {
        self.shared.get_on_error_mode()
    }
    fn set_on_error_mode(&self, mode: OnErrorMode) {
        self.shared.set_on_error_mode(mode)
    }

    fn get_maximum_error_per_file(&self) -> Option<HashMap<String, ErrorCode>> {
        if let Some(on_error_map) = self.get_on_error_map() {
            if on_error_map.is_empty() {
                return None;
            }
            let mut m = HashMap::<String, ErrorCode>::new();
            on_error_map
                .iter()
                .for_each(|x: RefMulti<String, HashMap<u16, InputError>>| {
                    if let Some(max_v) = x.value().iter().max_by_key(|entry| entry.1.num) {
                        m.insert(x.key().to_string(), max_v.1.err.clone());
                    }
                });
            return Some(m);
        }
        None
    }

    /// Get the storage data accessor operator from the session manager.
    /// Note that this is the application level data accessor, which may be different from
    /// the table level data accessor (e.g., table with customized storage parameters).
    fn get_application_level_data_operator(&self) -> Result<DataOperator> {
        Ok(self.shared.data_operator.clone())
    }

    #[async_backtrace::framed]
    async fn get_file_format(&self, name: &str) -> Result<FileFormatParams> {
        match StageFileFormatType::from_str(name) {
            Ok(typ) => FileFormatParams::default_by_type(typ),
            Err(_) => {
                let user_mgr = UserApiProvider::instance();
                let tenant = self.get_tenant();
                Ok(user_mgr
                    .get_file_format(&tenant, name)
                    .await?
                    .file_format_params)
            }
        }
    }
    async fn get_connection(&self, name: &str) -> Result<UserDefinedConnection> {
        self.shared.get_connection(name).await
    }

    /// Fetch a Table by db and table name.
    ///
    /// It guaranteed to return a consistent result for multiple calls, in a same query.
    /// E.g.:
    /// ```sql
    /// SELECT * FROM (SELECT * FROM db.table_name) as subquery_1, (SELECT * FROM db.table_name) AS subquery_2
    /// ```
    #[async_backtrace::framed]
    async fn get_table(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Arc<dyn Table>> {
        let table = self.shared.get_table(catalog, database, table).await?;
        // the better place to do this is in the QueryContextShared::get_table_to_cache() method,
        // but there is no way to access dyn TableContext.
        let table: Arc<dyn Table> = match table.engine() {
            "ICEBERG" => {
                let sp = get_storage_params_from_options(self, table.options()).await?;
                let mut info = table.get_table_info().to_owned();
                info.meta.storage_params = Some(sp);
                IcebergTable::try_create(info.to_owned())?.into()
            }
            "DELTA" => {
                let sp = get_storage_params_from_options(self, table.options()).await?;
                let mut info = table.get_table_info().to_owned();
                info.meta.storage_params = Some(sp);
                DeltaTable::try_create(info.to_owned())?.into()
            }
            _ => table,
        };
        Ok(table)
    }

    #[async_backtrace::framed]
    async fn filter_out_copied_files(
        &self,
        catalog_name: &str,
        database_name: &str,
        table_name: &str,
        files: &[StageFileInfo],
        max_files: Option<usize>,
    ) -> Result<FilteredCopyFiles> {
        if files.is_empty() {
            info!("no files to filter");
            return Ok(FilteredCopyFiles::default());
        }

        let collect_duplicated_files = self
            .get_settings()
            .get_enable_purge_duplicated_files_in_copy()?;

        let tenant = self.get_tenant();
        let catalog = self.get_catalog(catalog_name).await?;
        let table = catalog
            .get_table(&tenant, database_name, table_name)
            .await?;
        let table_id = table.get_id();

        let mut result_size: usize = 0;
        let max_files = max_files.unwrap_or(usize::MAX);
        let batch_size = min(COPIED_FILES_FILTER_BATCH_SIZE, max_files);

        let mut files_to_copy = Vec::with_capacity(files.len());
        let mut duplicated_files = Vec::with_capacity(files.len());

        for chunk in files.chunks(batch_size) {
            let files = chunk.iter().map(|v| v.path.clone()).collect::<Vec<_>>();
            let req = GetTableCopiedFileReq { table_id, files };
            let start_request = Instant::now();
            let copied_files = catalog
                .get_table_copied_file_info(&tenant, database_name, req)
                .await?
                .file_info;

            metrics_inc_copy_filter_out_copied_files_request_milliseconds(
                Instant::now().duration_since(start_request).as_millis() as u64,
            );
            // Colored
            for file in chunk {
                if !copied_files.contains_key(&file.path) {
                    files_to_copy.push(file.clone());
                    result_size += 1;
                    if result_size == max_files {
                        return Ok(FilteredCopyFiles {
                            files_to_copy,
                            duplicated_files,
                        });
                    }
                    if result_size > COPY_MAX_FILES_PER_COMMIT {
                        return Err(ErrorCode::Internal(COPY_MAX_FILES_COMMIT_MSG));
                    }
                } else if collect_duplicated_files && duplicated_files.len() < max_files {
                    duplicated_files.push(file.path.clone());
                }
            }
        }
        Ok(FilteredCopyFiles {
            files_to_copy,
            duplicated_files,
        })
    }

    fn set_materialized_cte(
        &self,
        idx: (IndexType, IndexType),
        blocks: Arc<RwLock<Vec<DataBlock>>>,
    ) -> Result<()> {
        let mut ctes = self.shared.materialized_cte_tables.write();
        ctes.insert(idx, blocks);
        Ok(())
    }

    fn get_materialized_cte(
        &self,
        idx: (IndexType, IndexType),
    ) -> Result<Option<Arc<RwLock<Vec<DataBlock>>>>> {
        let ctes = self.shared.materialized_cte_tables.read();
        Ok(ctes.get(&idx).cloned())
    }

    fn get_materialized_ctes(&self) -> MaterializedCtesBlocks {
        self.shared.materialized_cte_tables.clone()
    }

    fn add_segment_location(&self, segment_loc: Location) -> Result<()> {
        let mut segment_locations = self.inserted_segment_locs.write();
        segment_locations.insert(segment_loc);
        Ok(())
    }

    fn clear_segment_locations(&self) -> Result<()> {
        let mut segment_locations = self.inserted_segment_locs.write();
        segment_locations.clear();
        Ok(())
    }

    fn get_segment_locations(&self) -> Result<Vec<Location>> {
        Ok(self
            .inserted_segment_locs
            .read()
            .iter()
            .cloned()
            .collect::<Vec<_>>())
    }

    fn add_file_status(&self, file_path: &str, file_status: FileStatus) -> Result<()> {
        if matches!(self.get_query_kind(), QueryKind::CopyIntoTable) {
            self.shared.copy_status.add_chunk(file_path, file_status);
        }
        Ok(())
    }

    fn get_copy_status(&self) -> Arc<CopyStatus> {
        self.shared.copy_status.clone()
    }

    fn add_mutation_status(&self, mutation_status: MutationStatus) {
        self.shared
            .mutation_status
            .write()
            .merge_mutation_status(mutation_status)
    }

    fn get_mutation_status(&self) -> Arc<RwLock<MutationStatus>> {
        self.shared.mutation_status.clone()
    }

    fn update_multi_table_insert_status(&self, table_id: u64, num_rows: u64) {
        let mut multi_table_insert_status = self.shared.multi_table_insert_status.lock();
        match multi_table_insert_status.insert_rows.get_mut(&table_id) {
            Some(v) => {
                *v += num_rows;
            }
            None => {
                multi_table_insert_status
                    .insert_rows
                    .insert(table_id, num_rows);
            }
        }
    }

    fn get_multi_table_insert_status(&self) -> Arc<Mutex<MultiTableInsertStatus>> {
        self.shared.multi_table_insert_status.clone()
    }

    fn get_license_key(&self) -> String {
        unsafe {
            self.get_settings()
                .get_enterprise_license()
                .unwrap_or_default()
        }
    }

    fn get_query_profiles(&self) -> Vec<PlanProfile> {
        self.shared.get_query_profiles()
    }

    fn add_query_profiles(&self, profiles: &HashMap<u32, PlanProfile>) {
        self.shared.add_query_profiles(profiles)
    }

    fn get_queries_profile(&self) -> HashMap<String, Vec<PlanProfile>> {
        SessionManager::instance().get_queries_profiles()
    }

    fn set_merge_into_join(&self, join: MergeIntoJoin) {
        let mut merge_into_join = self.shared.merge_into_join.write();
        *merge_into_join = join;
    }

    fn clear_runtime_filter(&self) {
        let mut runtime_filters = self.shared.runtime_filters.write();
        runtime_filters.clear();
    }

    fn set_runtime_filter(&self, filters: (IndexType, RuntimeFilterInfo)) {
        let mut runtime_filters = self.shared.runtime_filters.write();
        match runtime_filters.entry(filters.0) {
            Entry::Vacant(v) => {
                v.insert(filters.1);
            }
            Entry::Occupied(mut v) => {
                for filter in filters.1.get_inlist() {
                    v.get_mut().add_inlist(filter.clone());
                }
                for filter in filters.1.get_min_max() {
                    v.get_mut().add_min_max(filter.clone());
                }
                for filter in filters.1.blooms() {
                    v.get_mut().add_bloom(filter);
                }
            }
        }
    }

    fn get_merge_into_join(&self) -> MergeIntoJoin {
        let merge_into_join = self.shared.merge_into_join.read();
        MergeIntoJoin {
            merge_into_join_type: merge_into_join.merge_into_join_type.clone(),
            is_distributed: merge_into_join.is_distributed,
            target_tbl_idx: merge_into_join.target_tbl_idx,
        }
    }

    fn get_bloom_runtime_filter_with_id(&self, id: IndexType) -> Vec<(String, BinaryFuse16)> {
        let runtime_filters = self.shared.runtime_filters.read();
        match runtime_filters.get(&id) {
            Some(v) => (v.get_bloom()).clone(),
            None => vec![],
        }
    }

    fn get_inlist_runtime_filter_with_id(&self, id: IndexType) -> Vec<Expr<String>> {
        let runtime_filters = self.shared.runtime_filters.read();
        match runtime_filters.get(&id) {
            Some(v) => (v.get_inlist()).clone(),
            None => vec![],
        }
    }

    fn get_min_max_runtime_filter_with_id(&self, id: IndexType) -> Vec<Expr<String>> {
        let runtime_filters = self.shared.runtime_filters.read();
        match runtime_filters.get(&id) {
            Some(v) => (v.get_min_max()).clone(),
            None => vec![],
        }
    }

    fn has_bloom_runtime_filters(&self, id: usize) -> bool {
        if let Some(runtime_filter) = self.shared.runtime_filters.read().get(&id) {
            return !runtime_filter.get_bloom().is_empty();
        }
        false
    }

    fn txn_mgr(&self) -> TxnManagerRef {
        self.shared.session.session_ctx.txn_mgr()
    }

    fn get_table_meta_timestamps(
        &self,
        table_id: u64,
        previous_snapshot: Option<Arc<TableSnapshot>>,
    ) -> Result<TableMetaTimestamps> {
        let cache = self.shared.get_table_meta_timestamps();
        let cached_item = cache.lock().get(&table_id).copied();
        match cached_item {
            Some(ts) => Ok(ts),
            None => {
                let ts = self.txn_mgr().lock().get_table_meta_timestamps(
                    table_id,
                    previous_snapshot,
                    self.get_settings().get_data_retention_time_in_days()? as i64,
                );
                cache.lock().insert(table_id, ts);
                Ok(ts)
            }
        }
    }

    fn get_read_block_thresholds(&self) -> BlockThresholds {
        *self.block_threshold.read()
    }

    fn set_read_block_thresholds(&self, thresholds: BlockThresholds) {
        *self.block_threshold.write() = thresholds;
    }

    fn get_query_queued_duration(&self) -> Duration {
        *self.shared.query_queued_duration.read()
    }

    fn set_query_queued_duration(&self, queued_duration: Duration) {
        *self.shared.query_queued_duration.write() = queued_duration;
    }

    fn set_variable(&self, key: String, value: Scalar) {
        self.shared.session.session_ctx.set_variable(key, value)
    }

    fn unset_variable(&self, key: &str) {
        self.shared.session.session_ctx.unset_variable(key)
    }

    fn get_variable(&self, key: &str) -> Option<Scalar> {
        self.shared.session.session_ctx.get_variable(key)
    }

    #[async_backtrace::framed]
    async fn load_datalake_schema(
        &self,
        kind: &str,
        sp: &StorageParams,
    ) -> Result<(TableSchema, String)> {
        match kind {
            "delta" => {
                let table = DeltaTable::load(sp).await?;
                DeltaTable::get_meta(&table).await
            }
            // TODO: iceberg doesn't support load from storage directly.
            _ => Err(ErrorCode::Internal("unsupported datalake type {}")),
        }
    }

    async fn create_stage_table(
        &self,
        stage_info: StageInfo,
        files_info: StageFilesInfo,
        files_to_copy: Option<Vec<StageFileInfo>>,
        max_column_position: usize,
    ) -> Result<Arc<dyn Table>> {
        match stage_info.file_format_params {
            FileFormatParams::Parquet(..) => {
                let mut read_options = ParquetReadOptions::default();

                if !self.get_settings().get_enable_parquet_page_index()? {
                    read_options = read_options.with_prune_pages(false);
                }

                if !self.get_settings().get_enable_parquet_rowgroup_pruning()? {
                    read_options = read_options.with_prune_row_groups(false);
                }

                if !self.get_settings().get_enable_parquet_prewhere()? {
                    read_options = read_options.with_do_prewhere(false);
                }

                ParquetRSTable::create(
                    stage_info.clone(),
                    files_info,
                    read_options,
                    files_to_copy,
                    self.get_settings(),
                    self.get_query_kind(),
                )
                .await
            }
            FileFormatParams::Orc(..) => {
                let schema = Arc::new(TableSchema::empty());
                let info = StageTableInfo {
                    schema,
                    stage_info,
                    files_info,
                    files_to_copy,
                    duplicated_files_detected: vec![],
                    is_select: true,
                    default_values: None,
                };
                OrcTable::try_create(info).await
            }
            FileFormatParams::NdJson(..) => {
                let schema = Arc::new(TableSchema::new(vec![TableField::new(
                    "_$1", // TODO: this name should be in visible
                    TableDataType::Variant,
                )]));
                let info = StageTableInfo {
                    schema,
                    stage_info,
                    files_info,
                    files_to_copy,
                    duplicated_files_detected: vec![],
                    is_select: true,
                    default_values: None,
                };
                StageTable::try_create(info)
            }
            FileFormatParams::Csv(..) | FileFormatParams::Tsv(..) => {
                if max_column_position == 0 {
                    let file_type = match stage_info.file_format_params {
                        FileFormatParams::Csv(..) => "CSV",
                        FileFormatParams::Tsv(..) => "TSV",
                        _ => unreachable!(), // This branch should never be reached
                    };

                    return Err(ErrorCode::SemanticError(format!(
                        "Query from {} file lacks column positions. Specify as $1, $2, etc.",
                        file_type
                    )));
                }

                let mut fields = vec![];
                for i in 1..(max_column_position + 1) {
                    fields.push(TableField::new(
                        &format!("_${}", i),
                        TableDataType::Nullable(Box::new(TableDataType::String)),
                    ));
                }

                let schema = Arc::new(TableSchema::new(fields));
                let info = StageTableInfo {
                    schema,
                    stage_info,
                    files_info,
                    files_to_copy,
                    duplicated_files_detected: vec![],
                    is_select: true,
                    default_values: None,
                };
                StageTable::try_create(info)
            }
            _ => {
                return Err(ErrorCode::Unimplemented(format!(
                    "The file format in the query stage is not supported. Currently supported formats are: Parquet, NDJson, CSV, and TSV. Provided format: '{}'.",
                    stage_info.file_format_params
                )));
            }
        }
    }

    async fn acquire_table_lock(
        self: Arc<Self>,
        catalog_name: &str,
        db_name: &str,
        tbl_name: &str,
        lock_opt: &LockTableOption,
    ) -> Result<Option<Arc<LockGuard>>> {
        let enabled_table_lock = self.get_settings().get_enable_table_lock().unwrap_or(false);
        if !enabled_table_lock {
            return Ok(None);
        }

        let catalog = self.get_catalog(catalog_name).await?;
        let tbl = catalog
            .get_table(&self.get_tenant(), db_name, tbl_name)
            .await?;
        if tbl.engine() != "FUSE" {
            return Ok(None);
        }

        // Add table lock.
        let table_lock = LockManager::create_table_lock(tbl.get_table_info().clone())?;
        match lock_opt {
            LockTableOption::LockNoRetry => table_lock.try_lock(self, false).await,
            LockTableOption::LockWithRetry => table_lock.try_lock(self, true).await,
            LockTableOption::NoLock => Ok(None),
        }
    }
}

impl TrySpawn for QueryContext {
    /// Spawns a new asynchronous task, returning a tokio::JoinHandle for it.
    /// The task will run in the current context thread_pool not the global.
    fn try_spawn<T>(&self, task: T) -> Result<JoinHandle<T::Output>>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        Ok(self.shared.try_get_runtime()?.spawn(task))
    }
}

impl std::fmt::Debug for QueryContext {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.get_current_user())
    }
}

pub fn convert_query_log_timestamp(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::new(0, 0))
        .as_micros() as i64
}
