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
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;

use dashmap::DashMap;
use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AbortChecker;
use databend_common_expression::BlockThresholds;
use databend_common_expression::CheckAbort;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchema;
use databend_common_io::prelude::FormatSettings;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::OnErrorMode;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::principal::UserDefinedConnection;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_app::tenant::Tenant;
use databend_common_pipeline_core::processors::PlanProfile;
use databend_common_pipeline_core::InputError;
use databend_common_pipeline_core::LockGuard;
use databend_common_settings::Settings;
use databend_common_storage::CopyStatus;
use databend_common_storage::DataOperator;
use databend_common_storage::FileStatus;
use databend_common_storage::MultiTableInsertStatus;
use databend_common_storage::MutationStatus;
use databend_common_storage::StageFileInfo;
use databend_common_storage::StageFilesInfo;
use databend_common_storage::StorageMetrics;
use databend_common_users::GrantObjectVisibilityChecker;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_txn::TxnManagerRef;
use parking_lot::Mutex;
use parking_lot::RwLock;
use xorf::BinaryFuse16;

use crate::catalog::Catalog;
use crate::cluster_info::Cluster;
use crate::lock::LockTableOption;
use crate::merge_into_join::MergeIntoJoin;
use crate::plan::DataSourcePlan;
use crate::plan::PartInfoPtr;
use crate::plan::Partitions;
use crate::query_kind::QueryKind;
use crate::runtime_filter_info::RuntimeFilterInfo;
use crate::statistics::data_cache_statistics::DataCacheMetrics;
use crate::table::Table;

pub type MaterializedCtesBlocks = Arc<RwLock<HashMap<(usize, usize), Arc<RwLock<Vec<DataBlock>>>>>>;

#[derive(Debug)]
pub struct ProcessInfo {
    pub id: String,
    pub typ: String,
    pub state: ProcessInfoState,
    pub database: String,
    pub user: Option<UserInfo>,
    pub settings: Arc<Settings>,
    pub client_address: Option<String>,
    pub session_extra_info: Option<String>,
    pub memory_usage: i64,
    /// storage metrics for persisted data reading.
    pub data_metrics: Option<StorageMetrics>,
    pub scan_progress_value: Option<ProgressValues>,
    pub mysql_connection_id: Option<u32>,
    pub created_time: SystemTime,
    pub status_info: Option<String>,
    pub current_query_id: Option<String>,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum ProcessInfoState {
    Query,
    Aborting,
    Idle,
}

impl Display for ProcessInfoState {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ProcessInfoState::Query => write!(f, "Query"),
            ProcessInfoState::Aborting => write!(f, "Aborting"),
            ProcessInfoState::Idle => write!(f, "Idle"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StageAttachment {
    pub location: String,
    pub file_format_options: Option<BTreeMap<String, String>>,
    pub copy_options: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Default)]
pub struct FilteredCopyFiles {
    pub files_to_copy: Vec<StageFileInfo>,
    pub duplicated_files: Vec<String>,
}

#[async_trait::async_trait]
pub trait TableContext: Send + Sync {
    fn as_any(&self) -> &dyn Any;
    /// Build a table instance the plan wants to operate on.
    ///
    /// A plan just contains raw information about a table or table function.
    /// This method builds a `dyn Table`, which provides table specific io methods the plan needs.
    fn build_table_from_source_plan(&self, plan: &DataSourcePlan) -> Result<Arc<dyn Table>>;

    fn incr_total_scan_value(&self, value: ProgressValues);
    fn get_total_scan_value(&self) -> ProgressValues;

    fn get_scan_progress(&self) -> Arc<Progress>;
    fn get_scan_progress_value(&self) -> ProgressValues;
    fn get_write_progress(&self) -> Arc<Progress>;
    fn get_join_spill_progress(&self) -> Arc<Progress>;
    fn get_group_by_spill_progress(&self) -> Arc<Progress>;
    fn get_aggregate_spill_progress(&self) -> Arc<Progress>;
    fn get_window_partition_spill_progress(&self) -> Arc<Progress>;
    fn get_write_progress_value(&self) -> ProgressValues;
    fn get_join_spill_progress_value(&self) -> ProgressValues;
    fn get_group_by_spill_progress_value(&self) -> ProgressValues;
    fn get_aggregate_spill_progress_value(&self) -> ProgressValues;
    fn get_window_partition_spill_progress_value(&self) -> ProgressValues;
    fn get_result_progress(&self) -> Arc<Progress>;
    fn get_result_progress_value(&self) -> ProgressValues;
    fn get_status_info(&self) -> String;
    fn set_status_info(&self, info: &str);
    fn get_data_cache_metrics(&self) -> &DataCacheMetrics;
    fn get_partition(&self) -> Option<PartInfoPtr>;
    fn get_partitions(&self, num: usize) -> Vec<PartInfoPtr>;
    fn partition_num(&self) -> usize {
        unimplemented!()
    }
    fn set_partitions(&self, partitions: Partitions) -> Result<()>;
    fn add_partitions_sha(&self, sha: String);
    fn get_partitions_shas(&self) -> Vec<String>;
    fn get_cacheable(&self) -> bool;
    fn set_cacheable(&self, cacheable: bool);
    fn get_can_scan_from_agg_index(&self) -> bool;
    fn set_can_scan_from_agg_index(&self, enable: bool);
    fn get_enable_sort_spill(&self) -> bool;
    fn set_enable_sort_spill(&self, enable: bool);
    fn set_compaction_num_block_hint(&self, hint: u64);
    fn get_compaction_num_block_hint(&self) -> u64;
    fn set_table_snapshot(&self, snapshot: Arc<TableSnapshot>);
    fn get_table_snapshot(&self) -> Option<Arc<TableSnapshot>>;
    fn set_lazy_mutation_delete(&self, lazy: bool);
    fn get_lazy_mutation_delete(&self) -> bool;

    fn attach_query_str(&self, kind: QueryKind, query: String);
    fn attach_query_hash(&self, text_hash: String, parameterized_hash: String);
    fn get_query_str(&self) -> String;

    fn get_query_parameterized_hash(&self) -> String;
    fn get_query_text_hash(&self) -> String;

    fn get_fragment_id(&self) -> usize;
    async fn get_catalog(&self, catalog_name: &str) -> Result<Arc<dyn Catalog>>;
    fn get_default_catalog(&self) -> Result<Arc<dyn Catalog>>;
    fn get_id(&self) -> String;
    fn get_current_catalog(&self) -> String;
    fn check_aborting(&self) -> Result<()>;
    fn get_abort_checker(self: Arc<Self>) -> AbortChecker
    where Self: 'static {
        struct Checker<S> {
            this: S,
        }
        impl<S: TableContext + ?Sized> CheckAbort for Checker<Arc<S>> {
            fn is_aborting(&self) -> bool {
                self.this.as_ref().check_aborting().is_err()
            }

            fn try_check_aborting(&self) -> Result<()> {
                self.this.check_aborting()
            }
        }
        Arc::new(Checker { this: self })
    }
    fn get_error(&self) -> Option<ErrorCode>;
    fn push_warning(&self, warning: String);
    fn get_current_database(&self) -> String;
    fn get_current_user(&self) -> Result<UserInfo>;
    fn get_current_role(&self) -> Option<RoleInfo>;
    fn get_current_session_id(&self) -> String {
        unimplemented!()
    }
    async fn get_all_effective_roles(&self) -> Result<Vec<RoleInfo>>;

    async fn validate_privilege(
        &self,
        object: &GrantObject,
        privilege: UserPrivilegeType,
        check_current_role_only: bool,
    ) -> Result<()>;
    async fn get_available_roles(&self) -> Result<Vec<RoleInfo>>;
    async fn get_visibility_checker(&self) -> Result<GrantObjectVisibilityChecker>;
    fn get_fuse_version(&self) -> String;
    fn get_format_settings(&self) -> Result<FormatSettings>;
    fn get_tenant(&self) -> Tenant;
    /// Get the kind of session running query.
    fn get_query_kind(&self) -> QueryKind;
    fn get_function_context(&self) -> Result<FunctionContext>;
    fn get_connection_id(&self) -> String;
    fn get_settings(&self) -> Arc<Settings>;
    fn get_shared_settings(&self) -> Arc<Settings>;
    fn get_cluster(&self) -> Arc<Cluster>;
    fn get_processes_info(&self) -> Vec<ProcessInfo>;
    fn get_queued_queries(&self) -> Vec<ProcessInfo>;
    fn get_queries_profile(&self) -> HashMap<String, Vec<PlanProfile>>;
    fn get_stage_attachment(&self) -> Option<StageAttachment>;
    fn get_last_query_id(&self, index: i32) -> String;
    fn get_query_id_history(&self) -> HashSet<String>;
    fn get_result_cache_key(&self, query_id: &str) -> Option<String>;
    fn set_query_id_result_cache(&self, query_id: String, result_cache_key: String);
    fn get_on_error_map(&self) -> Option<Arc<DashMap<String, HashMap<u16, InputError>>>>;
    fn set_on_error_map(&self, map: Arc<DashMap<String, HashMap<u16, InputError>>>);
    fn get_on_error_mode(&self) -> Option<OnErrorMode>;
    fn set_on_error_mode(&self, mode: OnErrorMode);
    fn get_maximum_error_per_file(&self) -> Option<HashMap<String, ErrorCode>>;

    /// Get the storage data accessor operator from the session manager.
    /// Note that this is the application level data accessor, which may be different from
    /// the table level data accessor (e.g., table with customized storage parameters).
    fn get_application_level_data_operator(&self) -> Result<DataOperator>;

    async fn get_file_format(&self, name: &str) -> Result<FileFormatParams>;

    async fn get_connection(&self, name: &str) -> Result<UserDefinedConnection>;

    async fn get_table(&self, catalog: &str, database: &str, table: &str)
    -> Result<Arc<dyn Table>>;

    async fn filter_out_copied_files(
        &self,
        catalog_name: &str,
        database_name: &str,
        table_name: &str,
        files: &[StageFileInfo],
        max_files: Option<usize>,
    ) -> Result<FilteredCopyFiles>;

    fn set_materialized_cte(
        &self,
        idx: (usize, usize),
        mem_table: Arc<RwLock<Vec<DataBlock>>>,
    ) -> Result<()>;

    fn get_materialized_cte(
        &self,
        idx: (usize, usize),
    ) -> Result<Option<Arc<RwLock<Vec<DataBlock>>>>>;

    fn get_materialized_ctes(&self) -> MaterializedCtesBlocks;

    fn add_segment_location(&self, segment_loc: Location) -> Result<()>;

    fn clear_segment_locations(&self) -> Result<()>;

    fn get_segment_locations(&self) -> Result<Vec<Location>>;

    fn add_file_status(&self, file_path: &str, file_status: FileStatus) -> Result<()>;

    fn get_copy_status(&self) -> Arc<CopyStatus>;

    fn add_mutation_status(&self, mutation_status: MutationStatus);

    fn get_mutation_status(&self) -> Arc<RwLock<MutationStatus>>;

    fn update_multi_table_insert_status(&self, table_id: u64, num_rows: u64);

    fn get_multi_table_insert_status(&self) -> Arc<Mutex<MultiTableInsertStatus>>;

    /// Get license key from context, return empty if license is not found or error happened.
    fn get_license_key(&self) -> String;

    fn add_query_profiles(&self, profiles: &HashMap<u32, PlanProfile>);

    fn get_query_profiles(&self) -> Vec<PlanProfile>;

    fn set_runtime_filter(&self, filters: (usize, RuntimeFilterInfo));

    fn clear_runtime_filter(&self);

    fn set_merge_into_join(&self, join: MergeIntoJoin);

    fn get_merge_into_join(&self) -> MergeIntoJoin;

    fn get_bloom_runtime_filter_with_id(&self, id: usize) -> Vec<(String, BinaryFuse16)>;

    fn get_inlist_runtime_filter_with_id(&self, id: usize) -> Vec<Expr<String>>;

    fn get_min_max_runtime_filter_with_id(&self, id: usize) -> Vec<Expr<String>>;

    fn has_bloom_runtime_filters(&self, id: usize) -> bool;
    fn txn_mgr(&self) -> TxnManagerRef;
    fn get_table_meta_timestamps(
        &self,
        table_id: u64,
        previous_snapshot: Option<Arc<TableSnapshot>>,
    ) -> Result<TableMetaTimestamps>;

    fn get_read_block_thresholds(&self) -> BlockThresholds;
    fn set_read_block_thresholds(&self, _thresholds: BlockThresholds);

    fn get_query_queued_duration(&self) -> Duration;
    fn set_query_queued_duration(&self, queued_duration: Duration);

    fn set_variable(&self, key: String, value: Scalar);
    fn unset_variable(&self, key: &str);
    fn get_variable(&self, key: &str) -> Option<Scalar>;

    async fn load_datalake_schema(
        &self,
        _kind: &str,
        _sp: &StorageParams,
    ) -> Result<(TableSchema, String)> {
        unimplemented!()
    }
    async fn create_stage_table(
        &self,
        _stage_info: StageInfo,
        _files_info: StageFilesInfo,
        _files_to_copy: Option<Vec<StageFileInfo>>,
        _max_column_position: usize,
    ) -> Result<Arc<dyn Table>> {
        unimplemented!()
    }

    async fn acquire_table_lock(
        self: Arc<Self>,
        catalog_name: &str,
        db_name: &str,
        tbl_name: &str,
        lock_opt: &LockTableOption,
    ) -> Result<Option<Arc<LockGuard>>>;
}
