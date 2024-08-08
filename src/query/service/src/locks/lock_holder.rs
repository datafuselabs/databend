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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use backoff::backoff::Backoff;
use databend_common_base::base::tokio::sync::Notify;
use databend_common_base::base::tokio::time::sleep;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::TrySpawn;
use databend_common_catalog::catalog::Catalog;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::CreateLockRevReq;
use databend_common_meta_app::schema::DeleteLockRevReq;
use databend_common_meta_app::schema::ExtendLockRevReq;
use databend_common_metrics::lock::record_created_lock_nums;
use databend_common_storages_fuse::operations::set_backoff;
use futures::future::select;
use futures::future::Either;
use rand::thread_rng;
use rand::Rng;

use crate::sessions::SessionManager;

#[derive(Default)]
pub struct LockHolder {
    shutdown_flag: AtomicBool,
    shutdown_notify: Notify,
}

impl LockHolder {
    #[async_backtrace::framed]
    pub async fn start(
        self: &Arc<Self>,
        query_id: String,
        catalog: Arc<dyn Catalog>,
        req: CreateLockRevReq,
    ) -> Result<u64> {
        let lock_key = req.lock_key.clone();
        let expire_secs = req.expire_secs;

        // get a new table lock revision.
        let res = catalog.create_lock_revision(req).await?;
        if res.elapsed_time >= expire_secs {
            return Err(ErrorCode::TableLockExpired(format!(
                "lock creation took {} secs, exceeding the set expiration time of {} secs",
                res.elapsed_time, expire_secs
            )));
        }
        let revision = res.revision;
        // metrics.
        record_created_lock_nums(lock_key.lock_type().to_string(), lock_key.get_table_id(), 1);

        let mut interval_secs = expire_secs - res.elapsed_time;
        let delete_table_lock_req = DeleteLockRevReq::new(lock_key.clone(), revision);
        let extend_table_lock_req =
            ExtendLockRevReq::new(lock_key.clone(), revision, expire_secs, false);
        GlobalIORuntime::instance().spawn({
            let self_clone = self.clone();
            async move {
                let mut notified = Box::pin(self_clone.shutdown_notify.notified());
                while !self_clone.shutdown_flag.load(Ordering::SeqCst) {
                    let mills = {
                        let sleep_range =
                            (interval_secs * 1000 / 3)..=(interval_secs * 1000 * 2 / 3);
                        let mut rng = thread_rng();
                        rng.gen_range(sleep_range.clone())
                    };
                    let sleep_range = Box::pin(sleep(Duration::from_millis(mills)));
                    match select(notified, sleep_range).await {
                        Either::Left((_, _)) => {
                            // shutdown.
                            break;
                        }
                        Either::Right((_, new_notified)) => {
                            notified = new_notified;
                            match self_clone
                                .try_extend_lock(
                                    catalog.clone(),
                                    extend_table_lock_req.clone(),
                                    Some(Duration::from_millis(expire_secs * 1000 - mills)),
                                )
                                .await
                            {
                                Ok(v) => interval_secs = v,
                                Err(e) => {
                                    // Force kill the query if extend lock failure.
                                    if let Some(session) =
                                        SessionManager::instance().get_session_by_id(&query_id)
                                    {
                                        session.force_kill_query(e.clone());
                                    }
                                    return Err(e);
                                }
                            }
                        }
                    }
                }

                Self::try_delete_lock(
                    catalog,
                    delete_table_lock_req,
                    Some(Duration::from_millis(expire_secs * 1000)),
                )
                .await
            }
        });

        Ok(revision)
    }

    pub fn shutdown(&self) {
        self.shutdown_flag.store(true, Ordering::SeqCst);
        self.shutdown_notify.notify_one();
    }
}

impl LockHolder {
    async fn try_extend_lock(
        self: &Arc<Self>,
        catalog: Arc<dyn Catalog>,
        req: ExtendLockRevReq,
        max_retry_elapsed: Option<Duration>,
    ) -> Result<u64> {
        let mut elapsed_time = 0;
        let mut backoff = set_backoff(Some(Duration::from_millis(2)), None, max_retry_elapsed);
        let mut extend_notified = Box::pin(self.shutdown_notify.notified());
        while !self.shutdown_flag.load(Ordering::SeqCst) {
            match catalog.extend_lock_revision(req.clone()).await {
                Ok(res) => {
                    elapsed_time = res.elapsed_time;
                    break;
                }
                Err(e) if e.code() == ErrorCode::TABLE_LOCK_EXPIRED => {
                    log::error!("failed to extend the lock. cause {:?}", e);
                    return Err(e);
                }
                Err(e) => match backoff.next_backoff() {
                    Some(duration) => {
                        log::debug!(
                            "failed to extend the lock, tx will be retried {} ms later. table id {}, revision {}",
                            duration.as_millis(),
                            req.lock_key.get_table_id(),
                            req.revision,
                        );
                        let sleep_gap = Box::pin(sleep(duration));
                        match select(extend_notified, sleep_gap).await {
                            Either::Left((_, _)) => {
                                // shutdown.
                                break;
                            }
                            Either::Right((_, new_notified)) => {
                                extend_notified = new_notified;
                            }
                        }
                    }
                    None => {
                        let error_info = format!(
                            "failed to extend the lock after retries {} ms, aborted. cause {:?}",
                            Instant::now()
                                .duration_since(backoff.start_time)
                                .as_millis(),
                            e,
                        );
                        log::error!("{}", error_info);
                        return Err(ErrorCode::OCCRetryFailure(error_info));
                    }
                },
            }
        }

        if elapsed_time >= req.expire_secs {
            return Err(ErrorCode::TableLockExpired(format!(
                "lock extend took {} secs, exceeding the set expiration time of {} secs",
                elapsed_time, req.expire_secs
            )));
        }
        Ok(req.expire_secs - elapsed_time)
    }

    async fn try_delete_lock(
        catalog: Arc<dyn Catalog>,
        req: DeleteLockRevReq,
        max_retry_elapsed: Option<Duration>,
    ) -> Result<()> {
        let mut backoff = set_backoff(Some(Duration::from_millis(2)), None, max_retry_elapsed);
        loop {
            match catalog.delete_lock_revision(req.clone()).await {
                Ok(_) => break,
                Err(e) => match backoff.next_backoff() {
                    Some(duration) => {
                        log::debug!(
                            "failed to delete the lock, tx will be retried {} ms later. table id {}, revision {}",
                            duration.as_millis(),
                            req.lock_key.get_table_id(),
                            req.revision,
                        );
                        sleep(duration).await;
                    }
                    None => {
                        let error_info = format!(
                            "failed to delete the lock after retries {} ms, aborted. cause {:?}",
                            Instant::now()
                                .duration_since(backoff.start_time)
                                .as_millis(),
                            e,
                        );
                        log::error!("{}", error_info);
                        return Err(ErrorCode::OCCRetryFailure(error_info));
                    }
                },
            }
        }
        Ok(())
    }
}
