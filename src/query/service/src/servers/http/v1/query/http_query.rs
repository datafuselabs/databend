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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use databend_common_base::base::short_sql;
use databend_common_base::base::tokio::sync::Mutex as TokioMutex;
use databend_common_base::base::tokio::sync::RwLock;
use databend_common_base::runtime::CatchUnwindFuture;
use databend_common_base::runtime::GlobalQueryRuntime;
use databend_common_base::runtime::TrySpawn;
use databend_common_catalog::table_context::StageAttachment;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::ResultExt;
use databend_common_expression::Scalar;
use databend_common_io::prelude::FormatSettings;
use databend_common_meta_app::tenant::Tenant;
use databend_common_metrics::http::metrics_incr_http_response_errors_count;
use databend_common_settings::ScopeLevel;
use databend_storages_common_session::TxnState;
use fastrace::prelude::*;
use http::StatusCode;
use log::info;
use log::warn;
use parking_lot::Mutex;
use poem::web::Json;
use poem::IntoResponse;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

use super::execute_state::ExecutionError;
use super::HttpQueryContext;
use super::RemoveReason;
use crate::servers::http::error::QueryError;
use crate::servers::http::v1::http_query_handlers::QueryResponseField;
use crate::servers::http::v1::query::execute_state::ExecuteStarting;
use crate::servers::http::v1::query::execute_state::ExecuteStopped;
use crate::servers::http::v1::query::execute_state::ExecutorSessionState;
use crate::servers::http::v1::query::execute_state::Progresses;
use crate::servers::http::v1::query::sized_spsc::sized_spsc;
use crate::servers::http::v1::query::ExecuteState;
use crate::servers::http::v1::query::ExecuteStateKind;
use crate::servers::http::v1::query::Executor;
use crate::servers::http::v1::query::PageManager;
use crate::servers::http::v1::query::ResponseData;
use crate::servers::http::v1::query::Wait;
use crate::servers::http::v1::ClientSessionManager;
use crate::servers::http::v1::HttpQueryManager;
use crate::servers::http::v1::QueryResponse;
use crate::servers::http::v1::QueryStats;
use crate::sessions::QueryAffect;
use crate::sessions::Session;
use crate::sessions::SessionType;
use crate::sessions::TableContext;

fn default_as_true() -> bool {
    true
}

#[derive(Deserialize, Clone)]
pub struct HttpQueryRequest {
    pub session_id: Option<String>,
    pub session: Option<HttpSessionConf>,
    pub sql: String,
    #[serde(default)]
    pub pagination: PaginationConf,
    #[serde(default = "default_as_true")]
    pub string_fields: bool,
    pub stage_attachment: Option<StageAttachmentConf>,
}

impl HttpQueryRequest {
    pub(crate) fn fail_to_start_sql(&self, err: ErrorCode) -> impl IntoResponse {
        metrics_incr_http_response_errors_count(err.name(), err.code());
        let session = self.session.as_ref().map(|s| {
            let txn_state = if matches!(s.txn_state, Some(TxnState::Active)) {
                Some(TxnState::Fail)
            } else {
                s.txn_state.clone()
            };
            HttpSessionConf {
                txn_state,
                ..s.clone()
            }
        });
        Json(QueryResponse {
            id: "".to_string(),
            stats: QueryStats::default(),
            state: ExecuteStateKind::Failed,
            affect: None,
            data: vec![],
            schema: vec![],
            session_id: None,
            warnings: vec![],
            node_id: "".to_string(),
            session,
            next_uri: None,
            stats_uri: None,
            final_uri: None,
            kill_uri: None,
            error: Some(QueryError::from_error_code(err)),
            has_result_set: None,
        })
    }
}

impl Debug for HttpQueryRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("HttpQueryRequest")
            .field("session_id", &self.session_id)
            .field("session", &self.session)
            .field("sql", &short_sql(self.sql.clone(), 1000))
            .field("pagination", &self.pagination)
            .field("string_fields", &self.string_fields)
            .field("stage_attachment", &self.stage_attachment)
            .finish()
    }
}

const DEFAULT_MAX_ROWS_IN_BUFFER: usize = 5 * 1000 * 1000;
const DEFAULT_MAX_ROWS_PER_PAGE: usize = 10000;
const DEFAULT_WAIT_TIME_SECS: u32 = 10;

fn default_max_rows_in_buffer() -> usize {
    DEFAULT_MAX_ROWS_IN_BUFFER
}

fn default_max_rows_per_page() -> usize {
    DEFAULT_MAX_ROWS_PER_PAGE
}

fn default_wait_time_secs() -> u32 {
    DEFAULT_WAIT_TIME_SECS
}

#[derive(Deserialize, Debug, Clone)]
pub struct PaginationConf {
    #[serde(default = "default_wait_time_secs")]
    pub(crate) wait_time_secs: u32,
    #[serde(default = "default_max_rows_in_buffer")]
    pub(crate) max_rows_in_buffer: usize,
    #[serde(default = "default_max_rows_per_page")]
    pub(crate) max_rows_per_page: usize,
}

impl Default for PaginationConf {
    fn default() -> Self {
        PaginationConf {
            wait_time_secs: DEFAULT_WAIT_TIME_SECS,
            max_rows_in_buffer: DEFAULT_MAX_ROWS_IN_BUFFER,
            max_rows_per_page: DEFAULT_MAX_ROWS_PER_PAGE,
        }
    }
}

impl PaginationConf {
    pub(crate) fn get_wait_type(&self) -> Wait {
        let t = self.wait_time_secs;
        if t > 0 {
            Wait::Deadline(Instant::now() + Duration::from_secs(t as u64))
        } else {
            Wait::Async
        }
    }
}

#[derive(Deserialize, Serialize, Debug, Default, Clone, Eq, PartialEq)]
pub struct ServerInfo {
    pub id: String,
    pub start_time: String,
}

#[derive(Deserialize, Serialize, Debug, Default, Clone, Eq, PartialEq)]
pub struct HttpSessionStateInternal {
    /// value is JSON of Scalar
    variables: Vec<(String, String)>,
}

impl HttpSessionStateInternal {
    fn new(variables: &HashMap<String, Scalar>) -> Self {
        let variables = variables
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    serde_json::to_string(&v).expect("fail to serialize Scalar"),
                )
            })
            .collect();
        Self { variables }
    }

    pub fn get_variables(&self) -> Result<HashMap<String, Scalar>> {
        let mut vars = HashMap::with_capacity(self.variables.len());
        for (k, v) in self.variables.iter() {
            match serde_json::from_str::<Scalar>(v) {
                Ok(s) => {
                    vars.insert(k.to_string(), s);
                }
                Err(e) => {
                    return Err(ErrorCode::BadBytes(format!(
                        "fail decode scalar from string '{v}', error: {e}"
                    )));
                }
            }
        }
        Ok(vars)
    }
}

fn serialize_as_json_string<S>(
    value: &Option<HttpSessionStateInternal>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(complex_value) => {
            let json_string =
                serde_json::to_string(complex_value).map_err(serde::ser::Error::custom)?;
            serializer.serialize_some(&json_string)
        }
        None => serializer.serialize_none(),
    }
}

fn deserialize_from_json_string<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<HttpSessionStateInternal>, D::Error>
where D: Deserializer<'de> {
    let json_string: Option<String> = Option::deserialize(deserializer)?;
    match json_string {
        Some(s) => {
            let complex_value = serde_json::from_str(&s).map_err(serde::de::Error::custom)?;
            Ok(Some(complex_value))
        }
        None => Ok(None),
    }
}

#[derive(Deserialize, Serialize, Debug, Default, Clone, Eq, PartialEq)]
pub struct HttpSessionConf {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub secondary_roles: Option<Vec<String>>,
    // todo: remove this later
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keep_server_session_secs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub settings: Option<BTreeMap<String, String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub txn_state: Option<TxnState>,
    #[serde(default)]
    pub need_sticky: bool,
    #[serde(default)]
    pub need_refresh: bool,
    // used to check if the session is still on the same server
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_server_info: Option<ServerInfo>,
    /// last_query_ids[0] is the last query id, last_query_ids[1] is the second last query id, etc.
    #[serde(default)]
    pub last_query_ids: Vec<String>,
    /// hide state not useful to clients
    /// so client only need to know there is a String field `internal`,
    /// which need to carry with session/conn
    #[serde(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(
        serialize_with = "serialize_as_json_string",
        deserialize_with = "deserialize_from_json_string"
    )]
    pub internal: Option<HttpSessionStateInternal>,
}

impl HttpSessionConf {}

#[derive(Deserialize, Debug, Clone)]
pub struct StageAttachmentConf {
    /// location of the stage
    /// for example: @stage_name/path/to/file, @~/path/to/file
    pub(crate) location: String,
    pub(crate) file_format_options: Option<BTreeMap<String, String>>,
    pub(crate) copy_options: Option<BTreeMap<String, String>>,
}

#[derive(Debug, Clone)]
pub struct ResponseState {
    pub has_result_set: Option<bool>,
    pub schema: Vec<QueryResponseField>,
    pub running_time_ms: i64,
    pub progresses: Progresses,
    pub state: ExecuteStateKind,
    pub affect: Option<QueryAffect>,
    pub error: Option<ErrorCode<ExecutionError>>,
    pub warnings: Vec<String>,
}

pub struct HttpQueryResponseInternal {
    pub data: Option<ResponseData>,
    pub session_id: String,
    pub session: Option<HttpSessionConf>,
    pub state: ResponseState,
    pub node_id: String,
}

#[derive(Debug, Clone, Copy)]
pub enum ExpireState {
    Working,
    ExpireAt(Instant),
    Removed(RemoveReason),
}

pub enum ExpireResult {
    Expired,
    Sleep(Duration),
    Removed,
}

pub struct HttpQuery {
    pub(crate) id: String,
    pub(crate) tenant: Tenant,
    pub(crate) user_name: String,
    pub(crate) client_session_id: Option<String>,
    pub(crate) session_id: String,
    pub(crate) node_id: String,
    request: HttpQueryRequest,
    state: Arc<RwLock<Executor>>,
    page_manager: Arc<TokioMutex<PageManager>>,
    expire_state: Arc<parking_lot::Mutex<ExpireState>>,
    /// The timeout for the query result polling. In the normal case, the client driver
    /// should fetch the paginated result in a timely manner, and the interval should not
    /// exceed this result_timeout_secs.
    pub(crate) result_timeout_secs: u64,

    pub(crate) need_refresh: bool,
    pub(crate) is_txn_mgr_saved: AtomicBool,

    pub(crate) has_temp_table_before_run: bool,
    pub(crate) has_temp_table_after_run: Mutex<Option<bool>>,
    pub(crate) is_session_handle_refreshed: AtomicBool,
}

fn try_set_txn(
    query_id: &str,
    session: &Arc<Session>,
    session_conf: &HttpSessionConf,
    http_query_manager: &Arc<HttpQueryManager>,
) -> Result<()> {
    match &session_conf.txn_state {
        Some(TxnState::Active) => {
            http_query_manager.check_sticky_for_txn(&session_conf.last_server_info)?;
            let last_query_id = session_conf.last_query_ids.first().ok_or_else(|| {
                ErrorCode::InvalidSessionState(
                    "transaction is active but last_query_ids is empty".to_string(),
                )
            })?;
            if let Some(txn_mgr) = http_query_manager.get_txn(last_query_id) {
                session.set_txn_mgr(txn_mgr);
                info!(
                    "{}: continue transaction from last query {}",
                    query_id, last_query_id
                );
            } else {
                // the returned TxnState should be Fail
                return Err(ErrorCode::TransactionTimeout(format!(
                    "transaction timeout: last_query_id {} not found",
                    last_query_id
                )));
            }
        }
        Some(TxnState::Fail) => session.txn_mgr().lock().force_set_fail(),
        _ => {}
    }
    Ok(())
}

impl HttpQuery {
    #[async_backtrace::framed]
    #[fastrace::trace]
    pub(crate) async fn try_create(
        ctx: &HttpQueryContext,
        request: HttpQueryRequest,
    ) -> Result<Arc<HttpQuery>> {
        let http_query_manager = HttpQueryManager::instance();
        let need_refresh = ctx.credential.need_refresh();
        let session = ctx
            .upgrade_session(SessionType::HTTPQuery)
            .map_err(|err| ErrorCode::Internal(format!("{err}")))?;

        // Read the session variables in the request, and set them to the current session.
        // the session variables includes:
        // - the current database
        // - the current role
        // - the session-level settings, like max_threads, http_handler_result_timeout_secs, etc.
        if let Some(session_conf) = &request.session {
            if let Some(db) = &session_conf.database {
                session.set_current_database(db.clone());
            }
            if let Some(role) = &session_conf.role {
                session.set_current_role_checked(role).await?;
            }

            // if the secondary_roles are None (which is the common case), it will not send any rpc on validation.
            session
                .set_secondary_roles_checked(session_conf.secondary_roles.clone())
                .await?;
            // TODO(liyz): pass secondary roles here
            if let Some(conf_settings) = &session_conf.settings {
                let settings = session.get_settings();
                for (k, v) in conf_settings {
                    settings
                        .set_setting(k.to_string(), v.to_string())
                        .or_else(|e| {
                            if e.code() == ErrorCode::UNKNOWN_VARIABLE {
                                warn!("http query unknown session setting: {}", k);
                                Ok(())
                            } else {
                                Err(e)
                            }
                        })?;
                }
            }
            if let Some(state) = &session_conf.internal {
                if !state.variables.is_empty() {
                    session.set_all_variables(state.get_variables()?)
                }
            }
            try_set_txn(&ctx.query_id, &session, session_conf, &http_query_manager)?;
            if session_conf.need_sticky
                && matches!(session_conf.txn_state, None | Some(TxnState::AutoCommit))
            {
                http_query_manager.check_sticky_for_temp_table(&session_conf.last_server_info)?;
            }
        };

        let settings = session.get_settings();
        let result_timeout_secs = settings.get_http_handler_result_timeout_secs()?;
        let deduplicate_label = &ctx.deduplicate_label;
        let user_agent = &ctx.user_agent;
        let query_id = ctx.query_id.clone();

        session.set_client_host(ctx.client_host.clone());

        let http_ctx = ctx;
        let ctx = session.create_query_context().await?;

        // Deduplicate label is used on the DML queries which may be retried by the client.
        // It can be used to avoid the duplicated execution of the DML queries.
        if let Some(label) = deduplicate_label {
            unsafe {
                ctx.get_settings().set_deduplicate_label(label.clone())?;
            }
        }
        if let Some(ua) = user_agent {
            ctx.set_ua(ua.clone());
        }

        // TODO: validate the query_id to be uuid format
        ctx.set_id(query_id.clone());

        let session_id = session.get_id().clone();
        let node_id = ctx.get_cluster().local_id.clone();
        let sql = &request.sql;
        info!(query_id = query_id, session_id = session_id, node_id = node_id, sql = sql; "create query");

        // Stage attachment is used to carry the data payload to the INSERT/REPLACE statements.
        // When stage attachment is specified, the query may looks like `INSERT INTO mytbl VALUES;`,
        // and the data in the stage attachment (which is mostly a s3 path) will be inserted into
        // the table.
        match &request.stage_attachment {
            Some(attachment) => ctx.attach_stage(StageAttachment {
                location: attachment.location.clone(),
                file_format_options: attachment.file_format_options.as_ref().map(|v| {
                    v.iter()
                        .map(|(k, v)| (k.to_lowercase(), v.to_owned()))
                        .collect::<BTreeMap<_, _>>()
                }),
                copy_options: attachment.copy_options.clone(),
            }),
            None => {}
        };

        let (block_sender, block_receiver) = sized_spsc(request.pagination.max_rows_in_buffer);

        let state = Arc::new(RwLock::new(Executor {
            query_id: query_id.clone(),
            state: ExecuteState::Starting(ExecuteStarting { ctx: ctx.clone() }),
        }));
        let block_sender_closer = block_sender.closer();
        let state_clone = state.clone();
        let ctx_clone = ctx.clone();
        let sql = request.sql.clone();

        let http_query_runtime_instance = GlobalQueryRuntime::instance();
        let span = if let Some(parent) = SpanContext::current_local_parent() {
            Span::root(std::any::type_name::<ExecuteState>(), parent)
                .with_properties(|| http_ctx.to_fastrace_properties())
        } else {
            Span::noop()
        };
        let format_settings: Arc<parking_lot::RwLock<Option<FormatSettings>>> = Default::default();
        let format_settings_clone = format_settings.clone();
        let tenant = session.get_current_tenant();
        let user_name = session.get_current_user()?.name;

        let has_temp_table_before_run = if let Some(cid) = session.get_client_session_id() {
            ClientSessionManager::instance().on_query_start(&cid, &user_name, &session);
            true
        } else {
            false
        };
        http_query_runtime_instance.runtime().try_spawn(
            async move {
                let state = state_clone.clone();
                if let Err(e) = CatchUnwindFuture::create(ExecuteState::try_start_query(
                    state,
                    sql,
                    session,
                    ctx_clone.clone(),
                    block_sender,
                    format_settings_clone,
                ))
                .await
                .with_context(|| "failed to start query")
                .flatten()
                {
                    let state = ExecuteStopped {
                        stats: Progresses::default(),
                        schema: vec![],
                        has_result_set: None,
                        reason: Err(e.clone()),
                        session_state: ExecutorSessionState::new(ctx_clone.get_current_session()),
                        query_duration_ms: ctx_clone.get_query_duration_ms(),
                        affect: ctx_clone.get_affect(),
                        warnings: ctx_clone.pop_warnings(),
                    };
                    info!("http query change state to Stopped, fail to start {:?}", e);
                    Executor::start_to_stop(&state_clone, ExecuteState::Stopped(Box::new(state)))
                        .await;
                    block_sender_closer.close();
                }
            }
            .in_span(span),
        )?;

        let data = Arc::new(TokioMutex::new(PageManager::new(
            request.pagination.max_rows_per_page,
            block_receiver,
            format_settings,
        )));

        let query = HttpQuery {
            id: query_id,
            tenant,
            user_name,
            client_session_id: http_ctx.client_session_id.clone(),
            session_id,
            node_id,
            request,
            state,
            page_manager: data,
            result_timeout_secs,

            expire_state: Arc::new(Mutex::new(ExpireState::Working)),

            need_refresh,
            has_temp_table_before_run,

            is_txn_mgr_saved: Default::default(),
            has_temp_table_after_run: Default::default(),
            is_session_handle_refreshed: Default::default(),
        };

        Ok(Arc::new(query))
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn get_response_page(&self, page_no: usize) -> Result<HttpQueryResponseInternal> {
        let data = Some(self.get_page(page_no).await?);
        let state = self.get_state().await;
        let session = self.get_response_session().await?;

        Ok(HttpQueryResponseInternal {
            data,
            state,
            session: Some(session),
            node_id: self.node_id.clone(),
            session_id: self.session_id.clone(),
        })
    }

    #[async_backtrace::framed]
    pub async fn get_response_state_only(&self) -> Result<HttpQueryResponseInternal> {
        let state = self.get_state().await;

        Ok(HttpQueryResponseInternal {
            data: None,
            session_id: self.session_id.clone(),
            node_id: self.node_id.clone(),
            state,
            session: None,
        })
    }

    #[async_backtrace::framed]
    async fn get_state(&self) -> ResponseState {
        let state = self.state.read().await;
        state.get_response_state()
    }

    #[async_backtrace::framed]
    async fn get_response_session(&self) -> Result<HttpSessionConf> {
        // reply the updated session state, includes:
        // - current_database: updated by USE XXX;
        // - role: updated by SET ROLE;
        // - secondary_roles: updated by SET SECONDARY ROLES ALL|NONE;
        // - settings: updated by SET XXX = YYY;
        let executor = self.state.read().await;
        let session_state = executor.get_session_state();

        let settings = session_state
            .settings
            .as_ref()
            .into_iter()
            .filter(|item| matches!(item.level, ScopeLevel::Session))
            .map(|item| (item.name.to_string(), item.user_value.as_string()))
            .collect::<BTreeMap<_, _>>();
        let database = session_state.current_database.clone();
        let role = session_state.current_role.clone();
        let secondary_roles = session_state.secondary_roles.clone();
        let txn_state = session_state.txn_manager.lock().state();
        let internal = if !session_state.variables.is_empty() {
            Some(HttpSessionStateInternal::new(&session_state.variables))
        } else {
            None
        };

        if matches!(executor.state, ExecuteState::Stopped(_)) {
            if let Some(cid) = &self.client_session_id {
                let (has_temp_table_after_run, just_changed) = {
                    let mut guard = self.has_temp_table_after_run.lock();
                    match *guard {
                        None => {
                            let not_empty = !session_state.temp_tbl_mgr.lock().is_empty().0;
                            *guard = Some(not_empty);
                            ClientSessionManager::instance().on_query_finish(
                                cid,
                                &self.user_name,
                                session_state.temp_tbl_mgr,
                                !not_empty,
                                not_empty != self.has_temp_table_before_run,
                            );
                            (not_empty, true)
                        }
                        Some(v) => (v, false),
                    }
                };

                if !self.has_temp_table_before_run
                    && has_temp_table_after_run
                    && just_changed
                    && self
                        .is_session_handle_refreshed
                        .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
                        .is_ok()
                {
                    ClientSessionManager::instance()
                        .refresh_session_handle(
                            self.tenant.clone(),
                            self.user_name.to_string(),
                            cid,
                        )
                        .await?;
                }
            }

            if txn_state != TxnState::AutoCommit
                && !self.is_txn_mgr_saved.load(Ordering::Relaxed)
                && self
                    .is_txn_mgr_saved
                    .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
                    .is_ok()
            {
                let timeout = session_state
                    .settings
                    .get_idle_transaction_timeout_secs()
                    .unwrap();
                HttpQueryManager::instance()
                    .add_txn(self.id.clone(), session_state.txn_manager.clone(), timeout)
                    .await;
            }
        }
        let has_temp_table = (*self.has_temp_table_after_run.lock()).unwrap_or(false);

        let need_sticky = txn_state != TxnState::AutoCommit || has_temp_table;
        let need_refresh = self.need_refresh || has_temp_table;

        Ok(HttpSessionConf {
            database: Some(database),
            role,
            secondary_roles,
            keep_server_session_secs: None,
            settings: Some(settings),
            txn_state: Some(txn_state),
            need_sticky,
            need_refresh,
            last_server_info: Some(HttpQueryManager::instance().server_info.clone()),
            last_query_ids: vec![self.id.clone()],
            internal,
        })
    }

    #[async_backtrace::framed]
    async fn get_page(&self, page_no: usize) -> Result<ResponseData> {
        let mut page_manager = self.page_manager.lock().await;
        let page = page_manager
            .get_a_page(page_no, &self.request.pagination.get_wait_type())
            .await?;
        let response = ResponseData {
            page,
            next_page_no: page_manager.next_page_no(),
        };
        Ok(response)
    }

    #[async_backtrace::framed]
    pub async fn kill(&self, reason: ErrorCode) {
        // the query will be removed from the query manager before the session is dropped.
        self.detach().await;

        Executor::stop(&self.state, Err(reason)).await;
    }

    #[async_backtrace::framed]
    async fn detach(&self) {
        let mut data = self.page_manager.lock().await;
        data.detach().await
    }

    #[async_backtrace::framed]
    pub async fn update_expire_time(&self, before_wait: bool) {
        let duration = Duration::from_secs(self.result_timeout_secs)
            + if before_wait {
                Duration::from_secs(self.request.pagination.wait_time_secs as u64)
            } else {
                Duration::new(0, 0)
            };
        let deadline = Instant::now() + duration;
        let mut t = self.expire_state.lock();
        *t = ExpireState::ExpireAt(deadline);
    }

    pub fn mark_removed(&self, remove_reason: RemoveReason) -> bool {
        let mut t = self.expire_state.lock();
        if !matches!(*t, ExpireState::Removed(_)) {
            *t = ExpireState::Removed(remove_reason);
            true
        } else {
            false
        }
    }

    pub fn check_removed(&self) -> Option<RemoveReason> {
        let t = self.expire_state.lock();
        if let ExpireState::Removed(r) = *t {
            Some(r)
        } else {
            None
        }
    }

    // return Duration to sleep
    #[async_backtrace::framed]
    pub async fn check_expire(&self) -> ExpireResult {
        let expire_state = self.expire_state.lock();
        match *expire_state {
            ExpireState::ExpireAt(expire_at) => {
                let now = Instant::now();
                if now >= expire_at {
                    ExpireResult::Expired
                } else {
                    ExpireResult::Sleep(expire_at - now)
                }
            }
            ExpireState::Removed(_) => ExpireResult::Removed,
            ExpireState::Working => {
                ExpireResult::Sleep(Duration::from_secs(self.result_timeout_secs))
            }
        }
    }

    pub fn check_client_session_id(&self, id: &Option<String>) -> poem::error::Result<()> {
        if *id != self.client_session_id {
            return Err(poem::error::Error::from_string(
                format!(
                    "wrong client_session_id, expect {:?}, got {id:?}",
                    &self.client_session_id
                ),
                StatusCode::UNAUTHORIZED,
            ));
        }
        Ok(())
    }
}
