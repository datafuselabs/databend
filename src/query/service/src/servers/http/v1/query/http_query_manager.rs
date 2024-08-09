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
use std::collections::VecDeque;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use chrono::SecondsFormat;
use dashmap::DashMap;
use databend_common_base::base::tokio::time::sleep;
use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::TrySpawn;
use databend_common_base::JoinHandle;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_storages_common_txn::TxnManagerRef;
use parking_lot::Mutex;

use super::expiring_map::ExpiringMap;
use super::HttpQueryContext;
use crate::servers::http::v1::query::http_query::ExpireResult;
use crate::servers::http::v1::query::http_query::HttpQuery;
use crate::servers::http::v1::query::http_query::ServerInfo;
use crate::servers::http::v1::query::HttpQueryRequest;
use crate::sessions::Session;

#[derive(Clone, Debug, Copy)]
pub(crate) enum RemoveReason {
    Timeout,
    Canceled,
    Finished,
}

impl Display for RemoveReason {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", format!("{self:?}").to_lowercase())
    }
}

pub struct LimitedQueue<T> {
    deque: VecDeque<T>,
    max_size: usize,
}

impl<T> LimitedQueue<T> {
    fn new(max_size: usize) -> Self {
        LimitedQueue {
            deque: VecDeque::new(),
            max_size,
        }
    }

    fn push(&mut self, item: T) -> Option<T> {
        self.deque.push_back(item);
        if self.deque.len() > self.max_size {
            self.deque.pop_front()
        } else {
            None
        }
    }
}

pub struct HttpQueryManager {
    pub(crate) start_instant: Instant,
    pub(crate) server_info: ServerInfo,
    #[allow(clippy::type_complexity)]
    pub(crate) queries: Arc<DashMap<String, Arc<HttpQuery>>>,
    pub(crate) removed_queries: Arc<parking_lot::Mutex<LimitedQueue<String>>>,
    #[allow(clippy::type_complexity)]
    pub(crate) txn_managers: Arc<Mutex<HashMap<String, (TxnManagerRef, JoinHandle<()>)>>>,
    pub(crate) sessions: Mutex<ExpiringMap<String, Arc<Session>>>,
}

impl HttpQueryManager {
    #[async_backtrace::framed]
    pub async fn init(cfg: &InnerConfig) -> Result<()> {
        GlobalInstance::set(Arc::new(HttpQueryManager {
            start_instant: Instant::now(),
            server_info: ServerInfo {
                id: cfg.query.node_id.clone(),
                start_time: chrono::Local::now().to_rfc3339_opts(SecondsFormat::Millis, false),
            },
            queries: Arc::new(DashMap::new()),
            sessions: Mutex::new(ExpiringMap::default()),
            removed_queries: Arc::new(parking_lot::Mutex::new(LimitedQueue::new(1000))),
            txn_managers: Arc::new(Mutex::new(HashMap::new())),
        }));

        Ok(())
    }

    pub fn instance() -> Arc<HttpQueryManager> {
        GlobalInstance::get()
    }

    #[async_backtrace::framed]
    pub(crate) async fn try_create_query(
        self: &Arc<Self>,
        ctx: &HttpQueryContext,
        request: HttpQueryRequest,
    ) -> Result<Arc<HttpQuery>> {
        let query = HttpQuery::try_create(ctx, request).await?;
        self.add_query(&query.id, query.clone()).await;
        Ok(query)
    }

    pub(crate) fn get_query(self: &Arc<Self>, query_id: &str) -> Option<Arc<HttpQuery>> {
        self.queries.get(query_id).map(|q| q.to_owned())
    }

    #[async_backtrace::framed]
    async fn add_query(self: &Arc<Self>, query_id: &str, query: Arc<HttpQuery>) {
        self.queries.insert(query_id.to_string(), query.clone());

        let self_clone = self.clone();
        let query_id_clone = query_id.to_string();
        let query_result_timeout_secs = query.result_timeout_secs;

        // downgrade to weak reference
        // it may cannot destroy with final or kill when we hold ref of Arc<HttpQuery>
        let http_query_weak = Arc::downgrade(&query);

        GlobalIORuntime::instance().spawn(async move {
            loop {
                let expire_res = match http_query_weak.upgrade() {
                    None => {
                        break;
                    }
                    Some(query) => query.check_expire().await,
                };

                match expire_res {
                    ExpireResult::Expired => {
                        let msg = format!(
                            "http query {} timeout after {} s",
                            &query_id_clone, query_result_timeout_secs
                        );
                        _ = self_clone
                            .remove_query(
                                &query_id_clone,
                                RemoveReason::Timeout,
                                ErrorCode::AbortedQuery(&msg),
                            )
                            .await;
                        break;
                    }
                    ExpireResult::Sleep(t) => {
                        sleep(t).await;
                    }
                    ExpireResult::Removed => {
                        break;
                    }
                }
            }
        });
    }

    #[async_backtrace::framed]
    pub(crate) async fn remove_query(
        self: &Arc<Self>,
        query_id: &str,
        reason: RemoveReason,
        error: ErrorCode,
    ) -> Option<Arc<HttpQuery>> {
        // deref at once to avoid holding DashMap shard guard for too long.
        let query = self.queries.get(query_id).map(|q| q.clone());
        if let Some(q) = &query {
            if q.mark_removed(reason) {
                q.kill(error).await;
                let mut queue = self.removed_queries.lock();
                if let Some(to_evict) = queue.push(q.id.to_string()) {
                    self.queries.remove(&to_evict);
                };
            }
        }
        query
    }

    #[async_backtrace::framed]
    pub(crate) async fn add_txn(
        self: &Arc<Self>,
        last_query_id: String,
        txn_mgr: TxnManagerRef,
        timeout_secs: u64,
    ) {
        let mut txn_managers = self.txn_managers.lock();
        let deleter = {
            let self_clone = self.clone();
            let last_query_id_clone = last_query_id.clone();
            GlobalIORuntime::instance().spawn(async move {
                sleep(Duration::from_secs(timeout_secs)).await;
                if self_clone.get_txn(&last_query_id_clone).is_some() {
                    log::info!(
                        "transaction timeout after {} secs, last_query_id = {}.",
                        timeout_secs,
                        last_query_id_clone
                    );
                }
            })
        };
        txn_managers.insert(last_query_id, (txn_mgr, deleter));
    }

    #[async_backtrace::framed]
    pub(crate) fn get_txn(self: &Arc<Self>, last_query_id: &str) -> Option<TxnManagerRef> {
        let mut txn_managers = self.txn_managers.lock();
        if let Some((txn_mgr, task_handle)) = txn_managers.remove(last_query_id) {
            task_handle.abort();
            Some(txn_mgr)
        } else {
            None
        }
    }

    #[async_backtrace::framed]
    pub(crate) async fn get_session(self: &Arc<Self>, session_id: &str) -> Option<Arc<Session>> {
        let sessions = self.sessions.lock();
        sessions.get(session_id)
    }

    #[async_backtrace::framed]
    pub(crate) async fn add_session(self: &Arc<Self>, session: Arc<Session>, timeout: Duration) {
        let mut sessions = self.sessions.lock();
        sessions.insert(session.get_id(), session, Some(timeout));
    }

    pub(crate) fn kill_session(self: &Arc<Self>, session_id: &str) {
        let mut sessions = self.sessions.lock();
        sessions.remove(session_id);
    }
}
