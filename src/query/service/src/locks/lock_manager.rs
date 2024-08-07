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
use std::sync::Arc;
use std::time::Duration;

use databend_common_base::base::tokio::sync::mpsc;
use databend_common_base::base::tokio::time::timeout;
use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::TrySpawn;
use databend_common_catalog::lock::Lock;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::CreateLockRevReq;
use databend_common_meta_app::schema::DeleteLockRevReq;
use databend_common_meta_app::schema::ExtendLockRevReq;
use databend_common_meta_app::schema::ListLockRevReq;
use databend_common_meta_app::schema::LockKey;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableLockIdent;
use databend_common_meta_kvapi::kvapi::Key;
use databend_common_meta_types::protobuf::watch_request::FilterType;
use databend_common_meta_types::protobuf::WatchRequest;
use databend_common_metrics::lock::metrics_inc_shutdown_lock_holder_nums;
use databend_common_metrics::lock::metrics_inc_start_lock_holder_nums;
use databend_common_metrics::lock::record_acquired_lock_nums;
use databend_common_pipeline_core::LockGuard;
use databend_common_pipeline_core::UnlockApi;
use databend_common_users::UserApiProvider;
use futures_util::StreamExt;
use parking_lot::RwLock;

use crate::locks::lock_holder::LockHolder;
use crate::locks::table_lock::TableLock;

pub struct LockManager {
    active_locks: Arc<RwLock<HashMap<u64, Arc<LockHolder>>>>,
    tx: mpsc::UnboundedSender<u64>,
}

impl LockManager {
    pub fn init() -> Result<()> {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let active_locks = Arc::new(RwLock::new(HashMap::new()));
        let lock_manager = Self { active_locks, tx };
        GlobalIORuntime::instance().spawn({
            let active_locks = lock_manager.active_locks.clone();
            async move {
                while let Some(revision) = rx.recv().await {
                    metrics_inc_shutdown_lock_holder_nums();
                    if let Some(lock) = active_locks.write().remove(&revision) {
                        lock.shutdown();
                    }
                }
            }
        });
        GlobalInstance::set(Arc::new(lock_manager));
        Ok(())
    }

    pub fn instance() -> Arc<LockManager> {
        GlobalInstance::get()
    }

    pub fn create_table_lock(table_info: TableInfo) -> Result<Arc<dyn Lock>> {
        let lock_mgr = LockManager::instance();
        Ok(TableLock::create(lock_mgr, table_info))
    }

    /// The requested lock returns a global incremental revision, listing all existing revisions,
    /// and if the current revision is the smallest, the lock is acquired successfully.
    /// Otherwise, listen to the deletion event of the previous revision in a loop until get lock success.
    ///
    /// NOTICE: the lock holder is not 100% reliable.
    /// E.g., there is a very small probability of failure in extending or deleting the lock.
    #[async_backtrace::framed]
    pub async fn try_lock(
        self: &Arc<Self>,
        ctx: Arc<dyn TableContext>,
        lock_key: LockKey,
        catalog_name: &str,
        should_retry: bool,
    ) -> Result<Option<Arc<LockGuard>>> {
        let lock_type = lock_key.lock_type().to_string();
        let table_id = lock_key.get_table_id();
        let tenant = lock_key.get_tenant();
        let expire_secs = ctx.get_settings().get_table_lock_expire_secs()?;
        let query_id = ctx.get_id();
        let req = CreateLockRevReq::new(
            lock_key.clone(),
            ctx.get_current_user()?.name,       // user
            ctx.get_cluster().local_id.clone(), // node
            query_id.clone(),                   // query_id
            expire_secs,
        );

        let catalog = ctx.get_catalog(catalog_name).await?;

        let lock_holder = Arc::new(LockHolder::default());
        let revision = lock_holder.start(query_id, catalog.clone(), req).await?;

        self.insert_lock(revision, lock_holder);
        let guard = LockGuard::new(self.clone(), revision);

        let acquire_lock_timeout = ctx.get_settings().get_acquire_lock_timeout()?;
        let duration = Duration::from_secs(acquire_lock_timeout);
        let meta_api = UserApiProvider::instance().get_meta_store_client();

        let list_table_lock_req = ListLockRevReq::new(lock_key.clone());

        let delete_table_lock_req = DeleteLockRevReq::new(lock_key.clone(), revision);

        loop {
            // List all revisions and check if the current is the minimum.
            let reply = catalog
                .list_lock_revisions(list_table_lock_req.clone())
                .await?;
            let position = reply.iter().position(|(x, _)| *x == revision).ok_or_else(||
                // If the current is not found in list,  it means that the current has been expired.
                ErrorCode::TableLockExpired("the acquired table lock has been expired".to_string()),
            )?;

            if position == 0 {
                // The lock is acquired by current session.
                let extend_table_lock_req =
                    ExtendLockRevReq::new(lock_key.clone(), revision, expire_secs, true);

                catalog.extend_lock_revision(extend_table_lock_req).await?;
                // metrics.
                record_acquired_lock_nums(lock_type, table_id, 1);
                break;
            }

            // if no need retry, return error directly.
            if !should_retry {
                catalog
                    .delete_lock_revision(delete_table_lock_req.clone())
                    .await?;
                return Err(ErrorCode::TableAlreadyLocked(
                    "table is locked by other session, please retry later".to_string(),
                ));
            }

            let watch_delete_ident = TableLockIdent::new(tenant, table_id, reply[position - 1].0);

            // Get the previous revision, watch the delete event.
            let req = WatchRequest {
                key: watch_delete_ident.to_string_key(),
                key_end: None,
                filter_type: FilterType::Delete.into(),
            };
            let mut watch_stream = meta_api.watch(req).await?;
            // Add a timeout period for watch.
            match timeout(duration, async move {
                while let Some(Ok(resp)) = watch_stream.next().await {
                    if let Some(event) = resp.event {
                        if event.current.is_none() {
                            break;
                        }
                    }
                }
            })
            .await
            {
                Ok(_) => Ok(()),
                Err(_) => {
                    catalog
                        .delete_lock_revision(delete_table_lock_req.clone())
                        .await?;
                    Err(ErrorCode::TableAlreadyLocked(
                        "table is locked by other session, please retry later".to_string(),
                    ))
                }
            }?;
        }

        Ok(Some(Arc::new(guard)))
    }

    fn insert_lock(&self, revision: u64, lock_holder: Arc<LockHolder>) {
        let mut active_locks = self.active_locks.write();
        let prev = active_locks.insert(revision, lock_holder);
        assert!(prev.is_none());

        // metrics.
        metrics_inc_start_lock_holder_nums();
    }
}

impl UnlockApi for LockManager {
    fn unlock(&self, revision: u64) {
        let _ = self.tx.send(revision);
    }
}
