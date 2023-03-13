// Copyright 2021 Datafuse Labs.
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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Weak;
use std::time::SystemTime;

use common_base::base::Progress;
use common_base::runtime::Runtime;
use common_catalog::table_context::StageAttachment;
use common_config::InnerConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_meta_app::principal::RoleInfo;
use common_meta_app::principal::UserInfo;
use common_settings::Settings;
use common_storage::DataOperator;
use common_storage::StorageMetrics;
use parking_lot::Mutex;
use parking_lot::RwLock;
use uuid::Uuid;

use crate::auth::AuthMgr;
use crate::catalogs::CatalogManager;
use crate::clusters::Cluster;
use crate::pipelines::executor::PipelineExecutor;
use crate::sessions::query_affect::QueryAffect;
use crate::sessions::Session;
use crate::storages::Table;

type DatabaseAndTable = (String, String, String);

/// Data that needs to be shared in a query context.
/// This is very useful, for example, for queries:
///     USE database_1;
///     SELECT
///         (SELECT scalar FROM table_name_1) AS scalar_1,
///         (SELECT scalar FROM table_name_2) AS scalar_2,
///         (SELECT scalar FROM table_name_3) AS scalar_3
///     FROM table_name_4;
/// For each subquery, they will share a runtime, session, progress, init_query_id
pub struct QueryContextShared {
    /// scan_progress for scan metrics of datablocks (uncompressed)
    pub(in crate::sessions) scan_progress: Arc<Progress>,
    /// write_progress for write/commit metrics of datablocks (uncompressed)
    pub(in crate::sessions) write_progress: Arc<Progress>,
    /// result_progress for metrics of result datablocks (uncompressed)
    pub(in crate::sessions) result_progress: Arc<Progress>,
    pub(in crate::sessions) error: Arc<Mutex<Option<ErrorCode>>>,
    pub(in crate::sessions) session: Arc<Session>,
    pub(in crate::sessions) runtime: Arc<RwLock<Option<Arc<Runtime>>>>,
    pub(in crate::sessions) init_query_id: Arc<RwLock<String>>,
    pub(in crate::sessions) cluster_cache: Arc<Cluster>,
    pub(in crate::sessions) running_query: Arc<RwLock<Option<String>>>,
    pub(in crate::sessions) running_query_kind: Arc<RwLock<Option<String>>>,
    pub(in crate::sessions) aborting: Arc<AtomicBool>,
    pub(in crate::sessions) tables_refs: Arc<Mutex<HashMap<DatabaseAndTable, Arc<dyn Table>>>>,
    pub(in crate::sessions) auth_manager: Arc<AuthMgr>,
    pub(in crate::sessions) affect: Arc<Mutex<Option<QueryAffect>>>,
    pub(in crate::sessions) catalog_manager: Arc<CatalogManager>,
    pub(in crate::sessions) data_operator: DataOperator,
    pub(in crate::sessions) executor: Arc<RwLock<Weak<PipelineExecutor>>>,
    pub(in crate::sessions) precommit_blocks: Arc<RwLock<Vec<DataBlock>>>,
    pub(in crate::sessions) stage_attachment: Arc<RwLock<Option<StageAttachment>>>,
    pub(in crate::sessions) created_time: SystemTime,
    pub(in crate::sessions) on_error_map: Arc<RwLock<Option<HashMap<String, ErrorCode>>>>,
    /// partitions_sha for each table in the query. Not empty only when enabling query result cache.
    pub(in crate::sessions) partitions_shas: Arc<RwLock<Vec<String>>>,
    pub(in crate::sessions) cacheable: Arc<AtomicBool>,
    // Status info.
    pub(in crate::sessions) status: Arc<RwLock<String>>,
}

impl QueryContextShared {
    pub fn try_create(
        config: &InnerConfig,
        session: Arc<Session>,
        cluster_cache: Arc<Cluster>,
    ) -> Result<Arc<QueryContextShared>> {
        Ok(Arc::new(QueryContextShared {
            session,
            cluster_cache,
            catalog_manager: CatalogManager::instance(),
            data_operator: DataOperator::instance(),
            init_query_id: Arc::new(RwLock::new(Uuid::new_v4().to_string())),
            scan_progress: Arc::new(Progress::create()),
            result_progress: Arc::new(Progress::create()),
            write_progress: Arc::new(Progress::create()),
            error: Arc::new(Mutex::new(None)),
            runtime: Arc::new(RwLock::new(None)),
            running_query: Arc::new(RwLock::new(None)),
            running_query_kind: Arc::new(RwLock::new(None)),
            aborting: Arc::new(AtomicBool::new(false)),
            tables_refs: Arc::new(Mutex::new(HashMap::new())),
            auth_manager: AuthMgr::create(config),
            affect: Arc::new(Mutex::new(None)),
            executor: Arc::new(RwLock::new(Weak::new())),
            precommit_blocks: Arc::new(RwLock::new(vec![])),
            stage_attachment: Arc::new(RwLock::new(None)),
            created_time: SystemTime::now(),
            on_error_map: Arc::new(RwLock::new(None)),
            partitions_shas: Arc::new(RwLock::new(vec![])),
            cacheable: Arc::new(AtomicBool::new(true)),
            status: Arc::new(RwLock::new("null".to_string())),
        }))
    }

    pub fn set_error(&self, err: ErrorCode) {
        let mut guard = self.error.lock();
        *guard = Some(err);
    }

    pub fn set_on_error_map(&self, map: Option<HashMap<String, ErrorCode>>) {
        let mut guard = self.on_error_map.write();
        *guard = map;
    }

    pub fn get_on_error_map(&self) -> Option<HashMap<String, ErrorCode>> {
        self.on_error_map.read().as_ref().cloned()
    }

    pub fn kill(&self, cause: ErrorCode) {
        self.set_error(cause.clone());
        self.aborting.store(true, Ordering::Release);

        if let Some(executor) = self.executor.read().upgrade() {
            executor.finish(Some(cause));
        }

        // TODO: Wait for the query to be processed (write out the last error)
    }

    pub fn get_cluster(&self) -> Arc<Cluster> {
        self.cluster_cache.clone()
    }

    pub fn get_current_catalog(&self) -> String {
        self.session.get_current_catalog()
    }

    pub fn get_aborting(&self) -> Arc<AtomicBool> {
        self.aborting.clone()
    }

    pub fn get_current_database(&self) -> String {
        self.session.get_current_database()
    }

    pub fn set_current_database(&self, new_database_name: String) {
        self.session.set_current_database(new_database_name);
    }

    pub fn get_current_user(&self) -> Result<UserInfo> {
        self.session.get_current_user()
    }

    pub fn get_current_role(&self) -> Option<RoleInfo> {
        self.session.get_current_role()
    }

    pub fn set_current_tenant(&self, tenant: String) {
        self.session.set_current_tenant(tenant);
    }

    /// Get all tables that already attached in this query.
    pub fn get_tables_refs(&self) -> Vec<Arc<dyn Table>> {
        let tables = self.tables_refs.lock();
        tables.values().cloned().collect()
    }

    pub fn get_data_metrics(&self) -> StorageMetrics {
        let tables = self.get_tables_refs();
        let metrics: Vec<Arc<StorageMetrics>> =
            tables.iter().filter_map(|v| v.get_data_metrics()).collect();
        StorageMetrics::merge(&metrics)
    }

    pub fn get_tenant(&self) -> String {
        self.session.get_current_tenant()
    }

    pub fn get_auth_manager(&self) -> Arc<AuthMgr> {
        self.auth_manager.clone()
    }

    pub fn get_settings(&self) -> Arc<Settings> {
        self.session.get_settings()
    }

    pub fn get_changed_settings(&self) -> Arc<Settings> {
        self.session.get_changed_settings()
    }

    pub fn apply_changed_settings(&self, changed_settings: Arc<Settings>) -> Result<()> {
        self.session.apply_changed_settings(changed_settings)
    }

    pub async fn get_table(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Arc<dyn Table>> {
        // Always get same table metadata in the same query

        let table_meta_key = (catalog.to_string(), database.to_string(), table.to_string());

        let already_in_cache = { self.tables_refs.lock().contains_key(&table_meta_key) };
        match already_in_cache {
            false => self.get_table_to_cache(catalog, database, table).await,
            true => Ok(self
                .tables_refs
                .lock()
                .get(&table_meta_key)
                .ok_or_else(|| ErrorCode::Internal("Logical error, it's a bug."))?
                .clone()),
        }
    }

    async fn get_table_to_cache(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
    ) -> Result<Arc<dyn Table>> {
        let tenant = self.get_tenant();
        let table_meta_key = (catalog.to_string(), database.to_string(), table.to_string());
        let catalog = self.catalog_manager.get_catalog(catalog)?;
        let cache_table = catalog.get_table(tenant.as_str(), database, table).await?;

        let mut tables_refs = self.tables_refs.lock();

        match tables_refs.entry(table_meta_key) {
            Entry::Occupied(v) => Ok(v.get().clone()),
            Entry::Vacant(v) => Ok(v.insert(cache_table).clone()),
        }
    }

    /// Init runtime when first get
    pub fn try_get_runtime(&self) -> Result<Arc<Runtime>> {
        let mut query_runtime = self.runtime.write();

        match &*query_runtime {
            Some(query_runtime) => Ok(query_runtime.clone()),
            None => {
                // To avoid possible deadlock, we should keep at least two threads.
                let runtime = Arc::new(Runtime::with_worker_threads(
                    2,
                    Some("query-ctx".to_string()),
                    false,
                )?);
                *query_runtime = Some(runtime.clone());
                Ok(runtime)
            }
        }
    }

    pub fn attach_query_str(&self, kind: String, query: &str) {
        {
            let mut running_query = self.running_query.write();
            *running_query = Some(short_sql(query));
        }

        {
            let mut running_query_kind = self.running_query_kind.write();
            *running_query_kind = Some(kind);
        }
    }

    pub fn get_query_str(&self) -> String {
        let running_query = self.running_query.read();
        running_query.as_ref().unwrap_or(&"".to_string()).clone()
    }

    pub fn get_query_kind(&self) -> String {
        let running_query_kind = self.running_query_kind.read();
        running_query_kind
            .as_ref()
            .cloned()
            .unwrap_or_else(|| "Unknown".to_string())
    }

    pub fn get_connection_id(&self) -> String {
        self.session.get_id()
    }

    pub fn get_affect(&self) -> Option<QueryAffect> {
        let guard = self.affect.lock();
        (*guard).clone()
    }

    pub fn set_affect(&self, affect: QueryAffect) {
        let mut guard = self.affect.lock();
        *guard = Some(affect);
    }

    pub fn set_executor(&self, weak_ptr: Weak<PipelineExecutor>) {
        let mut executor = self.executor.write();
        *executor = weak_ptr;
    }

    pub fn push_precommit_block(&self, block: DataBlock) {
        let mut blocks = self.precommit_blocks.write();
        blocks.push(block);
    }

    pub fn consume_precommit_blocks(&self) -> Vec<DataBlock> {
        let mut blocks = self.precommit_blocks.write();

        let mut swapped_precommit_blocks = vec![];
        std::mem::swap(&mut *blocks, &mut swapped_precommit_blocks);
        swapped_precommit_blocks
    }

    pub fn get_stage_attachment(&self) -> Option<StageAttachment> {
        self.stage_attachment.read().clone()
    }

    pub fn attach_stage(&self, attachment: StageAttachment) {
        let mut stage_attachment = self.stage_attachment.write();
        *stage_attachment = Some(attachment);
    }

    pub fn get_created_time(&self) -> SystemTime {
        self.created_time
    }
}

impl Drop for QueryContextShared {
    fn drop(&mut self) {
        // last_query_id() should return the query_id of the last executed statement,
        // so we set it when the current context drops
        // to avoid returning the query_id of the current statement.
        self.session
            .session_ctx
            .update_query_ids_results(self.init_query_id.read().clone(), None)
    }
}

pub fn short_sql(query: &str) -> String {
    use unicode_segmentation::UnicodeSegmentation;
    let query = query.trim_start();
    if query.len() >= 64 && query[..6].eq_ignore_ascii_case("INSERT") {
        // keep first 64 graphemes
        String::from_utf8(
            query
                .graphemes(true)
                .take(64)
                .flat_map(|g| g.as_bytes().iter())
                .copied() // copied converts &u8 into u8
                .chain(b"...".iter().copied())
                .collect::<Vec<u8>>(),
        )
        .unwrap() // by construction, this cannot panic as we extracted unicode grapheme
    } else {
        query.to_string()
    }
}
