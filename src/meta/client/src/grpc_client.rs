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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use common_arrow::arrow_format::flight::data::BasicAuth;
use common_base::base::tokio::select;
use common_base::base::tokio::sync::mpsc;
use common_base::base::tokio::sync::mpsc::Receiver;
use common_base::base::tokio::sync::mpsc::Sender;
use common_base::base::tokio::sync::oneshot;
use common_base::base::tokio::sync::oneshot::Receiver as OneRecv;
use common_base::base::tokio::sync::oneshot::Sender as OneSend;
use common_base::base::tokio::sync::RwLock;
use common_base::base::tokio::time::sleep;
use common_base::containers::ItemManager;
use common_base::containers::Pool;
use common_base::containers::TtlHashMap;
use common_base::runtime::Runtime;
use common_base::runtime::TrySpawn;
use common_base::runtime::UnlimitedFuture;
use common_grpc::ConnectionFactory;
use common_grpc::GrpcConnectionError;
use common_grpc::RpcClientConf;
use common_grpc::RpcClientTlsConfig;
use common_meta_api::reply::reply_to_api_result;
use common_meta_types::anyerror::AnyError;
use common_meta_types::protobuf::meta_service_client::MetaServiceClient;
use common_meta_types::protobuf::ClientInfo;
use common_meta_types::protobuf::Empty;
use common_meta_types::protobuf::ExportedChunk;
use common_meta_types::protobuf::HandshakeRequest;
use common_meta_types::protobuf::MemberListReply;
use common_meta_types::protobuf::MemberListRequest;
use common_meta_types::protobuf::RaftReply;
use common_meta_types::protobuf::RaftRequest;
use common_meta_types::protobuf::WatchRequest;
use common_meta_types::protobuf::WatchResponse;
use common_meta_types::ConnectionError;
use common_meta_types::InvalidArgument;
use common_meta_types::MetaClientError;
use common_meta_types::MetaError;
use common_meta_types::MetaHandshakeError;
use common_meta_types::MetaNetworkError;
use common_meta_types::TxnReply;
use common_meta_types::TxnRequest;
use common_metrics::label_counter_with_val_and_labels;
use common_metrics::label_decrement_gauge_with_val_and_labels;
use common_metrics::label_histogram_with_val;
use common_metrics::label_increment_gauge_with_val_and_labels;
use futures::stream::StreamExt;
use parking_lot::Mutex;
use prost::Message;
use semver::Version;
use serde::de::DeserializeOwned;
use tonic::async_trait;
use tonic::client::GrpcService;
use tonic::codegen::InterceptedService;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::transport::Channel;
use tonic::Code;
use tonic::Request;
use tonic::Status;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;

use crate::from_digit_ver;
use crate::grpc_action::RequestFor;
use crate::message;
use crate::to_digit_ver;
use crate::MetaGrpcReq;
use crate::METACLI_COMMIT_SEMVER;
use crate::MIN_METASRV_SEMVER;

const AUTH_TOKEN_KEY: &str = "auth-token-bin";
const META_GRPC_CLIENT_REQUEST_DURATION_MS: &str = "meta_grpc_client_request_duration_ms";
const META_GRPC_CLIENT_REQUEST_INFLIGHT: &str = "meta_grpc_client_request_inflight";
const META_GRPC_CLIENT_REQUEST_SUCCESS: &str = "meta_grpc_client_request_success";
const META_GRPC_CLIENT_REQUEST_FAILED: &str = "meta_grpc_client_request_fail";
const META_GRPC_MAKE_CLIENT_FAILED: &str = "meta_grpc_make_client_fail";
const LABEL_ENDPOINT: &str = "endpoint";
const LABEL_REQUEST: &str = "request";
const LABEL_ERROR: &str = "error";

#[derive(Debug)]
struct MetaChannelManager {
    timeout: Option<Duration>,
    conf: Option<RpcClientTlsConfig>,
}

impl MetaChannelManager {
    async fn build_channel(&self, addr: &String) -> Result<Channel, MetaNetworkError> {
        let ch = ConnectionFactory::create_rpc_channel(addr, self.timeout, self.conf.clone())
            .await
            .map_err(|e| match e {
                GrpcConnectionError::InvalidUri { .. } => MetaNetworkError::BadAddressFormat(
                    AnyError::new(&e).add_context(|| "while creating rpc channel"),
                ),
                GrpcConnectionError::TLSConfigError { .. } => MetaNetworkError::TLSConfigError(
                    AnyError::new(&e).add_context(|| "while creating rpc channel"),
                ),
                GrpcConnectionError::CannotConnect { .. } => MetaNetworkError::ConnectionError(
                    ConnectionError::new(e, "while creating rpc channel"),
                ),
            })?;
        Ok(ch)
    }
}

#[async_trait]
impl ItemManager for MetaChannelManager {
    type Key = String;
    type Item = Channel;
    type Error = MetaNetworkError;

    #[tracing::instrument(level = "debug", err(Debug))]
    async fn build(&self, addr: &Self::Key) -> Result<Self::Item, Self::Error> {
        self.build_channel(addr).await
    }

    #[tracing::instrument(level = "debug", err(Debug))]
    async fn check(&self, mut ch: Self::Item) -> Result<Self::Item, Self::Error> {
        futures::future::poll_fn(|cx| ch.poll_ready(cx))
            .await
            .map_err(|e| {
                MetaNetworkError::ConnectionError(ConnectionError::new(e, "while check item"))
            })?;
        Ok(ch)
    }
}

/// A handle to access meta-client worker.
/// The worker will be actually running in a dedicated runtime: `MetaGrpcClient.rt`.
pub struct ClientHandle {
    /// For sending request to meta-client worker.
    pub(crate) req_tx: Sender<message::ClientWorkerRequest>,
    /// Notify auto sync to stop.
    /// `oneshot::Receiver` impl `Drop` by sending a closed notification to the `Sender` half.
    #[allow(dead_code)]
    cancel_auto_sync_rx: OneRecv<()>,
}

impl ClientHandle {
    /// Send a request to the internal worker task, which may be running in another runtime.
    pub async fn request<Req, Resp, E>(&self, req: Req) -> Result<Resp, E>
    where
        Req: RequestFor<Reply = Resp>,
        Req: Into<message::Request>,
        Result<Resp, E>: TryFrom<message::Response>,
        <Result<Resp, E> as TryFrom<message::Response>>::Error: std::fmt::Display,
        E: From<MetaClientError>,
    {
        let request_future = async move {
            let (tx, rx) = oneshot::channel();
            let req = message::ClientWorkerRequest {
                resp_tx: tx,
                req: req.into(),
            };

            label_increment_gauge_with_val_and_labels(
                META_GRPC_CLIENT_REQUEST_INFLIGHT,
                &vec![],
                1.0,
            );

            let res = self.req_tx.send(req).await.map_err(|e| {
                let cli_err = MetaClientError::ClientRuntimeError(
                    AnyError::new(&e).add_context(|| "when sending req to MetaGrpcClient worker"),
                );
                cli_err.into()
            });

            if let Err(err) = res {
                label_decrement_gauge_with_val_and_labels(
                    META_GRPC_CLIENT_REQUEST_INFLIGHT,
                    &vec![],
                    1.0,
                );

                return Err(err);
            }

            let res = rx.await.map_err(|e| {
                label_decrement_gauge_with_val_and_labels(
                    META_GRPC_CLIENT_REQUEST_INFLIGHT,
                    &vec![],
                    1.0,
                );

                MetaClientError::ClientRuntimeError(
                    AnyError::new(&e).add_context(|| "when recv resp from MetaGrpcClient worker"),
                )
            })?;

            label_decrement_gauge_with_val_and_labels(
                META_GRPC_CLIENT_REQUEST_INFLIGHT,
                &vec![],
                1.0,
            );
            let res: Result<Resp, E> = res
                .try_into()
                .map_err(|e| format!("expect: {}, got: {}", std::any::type_name::<Resp>(), e))
                .unwrap();

            res
        };

        UnlimitedFuture::create(request_future).await
    }

    pub async fn get_client_info(&self) -> Result<ClientInfo, MetaError> {
        self.request(message::GetClientInfo {}).await
    }

    pub async fn make_client(
        &self,
    ) -> Result<MetaServiceClient<InterceptedService<Channel, AuthInterceptor>>, MetaClientError>
    {
        self.request(message::MakeClient {}).await
    }

    /// Return the endpoints list cached on this client.
    pub async fn get_cached_endpoints(&self) -> Result<Vec<String>, MetaError> {
        self.request(message::GetEndpoints {}).await
    }
}

// TODO: maybe it just needs a runtime, not a MetaGrpcClientWorker.
//
/// Meta grpc client has a internal worker task that deals with all traffic to remote meta service.
///
/// We expect meta-client should be cloneable.
/// But the underlying hyper client has a worker that runs in its creating tokio-runtime.
/// Thus a cloned meta client may try to talk to a destroyed hyper worker if the creating tokio-runtime is dropped.
/// Thus we have to guarantee that as long as there is a meta-client, the huper worker runtime must not be dropped.
/// Thus a meta client creates a runtime then spawn a MetaGrpcClientWorker.
pub struct MetaGrpcClient {
    conn_pool: Pool<MetaChannelManager>,
    endpoints: RwLock<Vec<String>>,
    username: String,
    password: String,
    current_endpoint: Arc<Mutex<Option<String>>>,
    unhealthy_endpoints: Mutex<TtlHashMap<String, ()>>,
    auto_sync_interval: Option<Duration>,

    /// Dedicated runtime to support meta client background tasks.
    ///
    /// In order not to let a blocking operation(such as calling the new PipelinePullingExecutor) in a tokio runtime block meta-client background tasks.
    /// If a background task is blocked, no meta-client will be able to proceed if meta-client is reused.
    ///
    /// Note that a thread_pool tokio runtime does not help: a scheduled tokio-task resides in `filo_slot` won't be stolen by other tokio-workers.
    #[allow(dead_code)]
    rt: Arc<Runtime>,
}

impl Debug for MetaGrpcClient {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut de = f.debug_struct("MetaGrpcClient");
        de.field("endpoints", &self.endpoints);
        de.field("current_endpoints", &self.current_endpoint);
        de.field("unhealthy_endpoints", &self.unhealthy_endpoints);
        de.field("auto_sync_interval", &self.auto_sync_interval);
        de.finish()
    }
}

impl MetaGrpcClient {
    /// Create a new client of metasrv.
    ///
    /// It creates a new `Runtime` and spawn a background worker task in it that do all the RPC job.
    /// A client-handle is returned to communicate with the worker.
    ///
    /// Thus the real work is done in the dedicated runtime to avoid the client spawning tasks in the caller's runtime, which potentially leads to a deadlock if the caller has blocking calls to other components
    /// Because `tower` and `hyper` will spawn tasks when handling RPCs.
    ///
    /// The worker is a singleton and the returned handle is cheap to clone.
    /// When all handles are dropped the worker will quit, then the runtime will be destroyed.
    pub fn try_new(conf: &RpcClientConf) -> Result<Arc<ClientHandle>, MetaClientError> {
        Self::try_create(
            conf.get_endpoints(),
            &conf.username,
            &conf.password,
            conf.timeout,
            conf.auto_sync_interval,
            conf.tls_conf.clone(),
        )
    }

    #[tracing::instrument(level = "debug", skip(password))]
    pub fn try_create(
        endpoints: Vec<String>,
        username: &str,
        password: &str,
        timeout: Option<Duration>,
        auto_sync_interval: Option<Duration>,
        conf: Option<RpcClientTlsConfig>,
    ) -> Result<Arc<ClientHandle>, MetaClientError> {
        Self::endpoints_non_empty(&endpoints)?;

        let mgr = MetaChannelManager { timeout, conf };

        let rt = Runtime::with_worker_threads(1, Some("meta-client-rt".to_string()), true)
            .map_err(|e| {
                MetaClientError::ClientRuntimeError(
                    AnyError::new(&e).add_context(|| "when creating meta-client"),
                )
            })?;
        let rt = Arc::new(rt);

        // Build the handle-worker pair

        let (tx, rx) = mpsc::channel(256);
        let (one_tx, one_rx) = oneshot::channel::<()>();

        let handle = Arc::new(ClientHandle {
            req_tx: tx,
            cancel_auto_sync_rx: one_rx,
        });

        let worker = Arc::new(Self {
            conn_pool: Pool::new(mgr, Duration::from_millis(50)),
            endpoints: RwLock::new(endpoints),
            current_endpoint: Arc::new(Mutex::new(None)),
            unhealthy_endpoints: Mutex::new(TtlHashMap::new(Duration::from_secs(120))),
            auto_sync_interval,
            username: username.to_string(),
            password: password.to_string(),
            rt: rt.clone(),
        });

        rt.spawn(UnlimitedFuture::create(Self::worker_loop(
            worker.clone(),
            rx,
        )));
        rt.spawn(UnlimitedFuture::create(Self::auto_sync_endpoints(
            worker, one_tx,
        )));

        Ok(handle)
    }

    /// A worker runs a receiving-loop to accept user-request to metasrv and deals with request in the dedicated runtime.
    #[tracing::instrument(level = "info", skip_all)]
    async fn worker_loop(self: Arc<Self>, mut req_rx: Receiver<message::ClientWorkerRequest>) {
        info!("MetaGrpcClient::worker spawned");

        loop {
            let t = req_rx.recv().await;
            let req = match t {
                None => {
                    info!("MetaGrpcClient handle closed. worker quit");
                    return;
                }
                Some(x) => x,
            };

            debug!(req = debug(&req), "MetaGrpcClient recv request");

            if req.resp_tx.is_closed() {
                debug!(
                    req = debug(&req),
                    "MetaGrpcClient request.resp_tx is closed, cancel handling this request"
                );
                continue;
            }

            let resp_tx = req.resp_tx;
            let req = req.req;
            let req_name = req.name();
            let req_str = format!("{:?}", req);

            let start = Instant::now();

            let resp = match req {
                message::Request::Get(r) => {
                    let resp = self.kv_api(r).await;
                    message::Response::Get(resp)
                }
                message::Request::MGet(r) => {
                    let resp = self.kv_api(r).await;
                    message::Response::MGet(resp)
                }
                message::Request::PrefixList(r) => {
                    let resp = self.kv_api(r).await;
                    message::Response::PrefixList(resp)
                }
                message::Request::Upsert(r) => {
                    let resp = self.kv_api(r).await;
                    message::Response::Upsert(resp)
                }
                message::Request::Txn(r) => {
                    let resp = self.transaction(r).await;
                    message::Response::Txn(resp)
                }
                message::Request::Watch(r) => {
                    let resp = self.watch(r).await;
                    message::Response::Watch(resp)
                }
                message::Request::Export(r) => {
                    let resp = self.export(r).await;
                    message::Response::Export(resp)
                }
                message::Request::MakeClient(_) => {
                    let resp = self.make_client().await;
                    message::Response::MakeClient(resp)
                }
                message::Request::GetEndpoints(_) => {
                    let resp = self.get_cached_endpoints().await;
                    message::Response::GetEndpoints(Ok(resp))
                }
                message::Request::GetClientInfo(_) => {
                    let resp = self.get_client_info().await;
                    message::Response::GetClientInfo(resp)
                }
            };

            debug!(
                resp = debug(&resp),
                "MetaGrpcClient send response to the handle"
            );

            let current_endpoint = {
                let current_endpoint = self.current_endpoint.lock();
                current_endpoint.clone()
            };

            if let Some(current_endpoint) = current_endpoint {
                let elapsed = start.elapsed().as_millis() as f64;
                label_histogram_with_val(
                    META_GRPC_CLIENT_REQUEST_DURATION_MS,
                    &vec![
                        (LABEL_ENDPOINT, current_endpoint.to_string()),
                        (LABEL_REQUEST, req_name.to_string()),
                    ],
                    elapsed,
                );
                if elapsed > 1000_f64 {
                    warn!(
                        "MetaGrpcClient slow request {} to {} takes {} ms: {}",
                        req_name, current_endpoint, elapsed, req_str,
                    );
                }

                if let Some(err) = resp.err() {
                    label_counter_with_val_and_labels(
                        META_GRPC_CLIENT_REQUEST_FAILED,
                        &vec![
                            (LABEL_ENDPOINT, current_endpoint.to_string()),
                            (LABEL_ERROR, err.to_string()),
                            (LABEL_REQUEST, req_name.to_string()),
                        ],
                        1,
                    );
                    error!("MetaGrpcClient error: {:?}", err);
                } else {
                    label_counter_with_val_and_labels(
                        META_GRPC_CLIENT_REQUEST_SUCCESS,
                        &vec![
                            (LABEL_ENDPOINT, current_endpoint.to_string()),
                            (LABEL_REQUEST, req_name.to_string()),
                        ],
                        1,
                    );
                }
            }

            let send_res = resp_tx.send(resp);
            if let Err(err) = send_res {
                error!(
                    err = debug(err),
                    "MetaGrpcClient failed to send response to the handle. recv-end closed"
                );
            }
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn make_client(
        &self,
    ) -> Result<MetaServiceClient<InterceptedService<Channel, AuthInterceptor>>, MetaClientError>
    {
        let mut eps = self.get_cached_endpoints().await;
        debug!("service endpoints: {:?}", eps);

        debug_assert!(!eps.is_empty());

        if eps.len() > 1 {
            // remove unhealthy endpoints
            let ues = self.unhealthy_endpoints.lock();
            eps.retain(|e| !ues.contains_key(e));
        }

        for (addr, is_last) in eps.iter().enumerate().map(|(i, a)| (a, i == eps.len() - 1)) {
            let channel = self.make_channel(Some(addr)).await;
            match channel {
                Ok(c) => {
                    let mut client = MetaServiceClient::new(c.clone());

                    let new_token = Self::handshake(
                        &mut client,
                        &METACLI_COMMIT_SEMVER,
                        &MIN_METASRV_SEMVER,
                        &self.username,
                        &self.password,
                    )
                    .await;
                    match new_token {
                        Ok(token) => {
                            return Ok(MetaServiceClient::with_interceptor(c, AuthInterceptor {
                                token,
                            }));
                        }
                        Err(handshake_err) => {
                            warn!("handshake error when make client: {:?}", handshake_err);
                            {
                                let mut ue = self.unhealthy_endpoints.lock();
                                ue.insert(addr.to_string(), ());
                            }
                            if is_last {
                                // reach to last addr
                                let cli_err = MetaClientError::HandshakeError(handshake_err);
                                return Err(cli_err);
                            }
                            continue;
                        }
                    };
                }

                Err(net_err) => {
                    {
                        let mut ue = self.unhealthy_endpoints.lock();
                        ue.insert(addr.to_string(), ());
                    }
                    if is_last {
                        let cli_err = MetaClientError::NetworkError(net_err);
                        return Err(cli_err);
                    }
                    continue;
                }
            }
        }
        Err(MetaClientError::ConfigError(AnyError::error(
            "endpoints is empty",
        )))
    }

    #[tracing::instrument(level = "debug", skip(self), err(Debug))]
    async fn make_channel(&self, addr: Option<&String>) -> Result<Channel, MetaNetworkError> {
        let addr = if let Some(addr) = addr {
            addr.clone()
        } else {
            let eps = self.endpoints.read().await;
            eps.first().unwrap().clone()
        };
        let ch = self.conn_pool.get(&addr).await;
        {
            let mut current_endpoint = self.current_endpoint.lock();
            *current_endpoint = Some(addr.clone());
        }
        match ch {
            Ok(c) => Ok(c),
            Err(e) => {
                warn!(
                    "grpc_client create channel with {} failed, err: {:?}",
                    addr, e
                );
                label_counter_with_val_and_labels(
                    META_GRPC_MAKE_CLIENT_FAILED,
                    &vec![(LABEL_ENDPOINT, addr.to_string())],
                    1,
                );
                Err(e)
            }
        }
    }

    pub fn endpoints_non_empty(endpoints: &[String]) -> Result<(), MetaClientError> {
        if endpoints.is_empty() {
            return Err(MetaClientError::ConfigError(AnyError::error(
                "endpoints is empty",
            )));
        }
        Ok(())
    }

    async fn get_cached_endpoints(&self) -> Vec<String> {
        let eps = self.endpoints.read().await;
        (*eps).clone()
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn set_endpoints(&self, endpoints: Vec<String>) -> Result<(), MetaError> {
        Self::endpoints_non_empty(&endpoints)?;

        // Older meta nodes may not store endpoint information and need to be filtered out.
        let distinct_cnt = endpoints.iter().filter(|n| !(*n).is_empty()).count();

        // If the fetched endpoints are less than the majority of the current cluster, no replacement should occur.
        if distinct_cnt < endpoints.len() / 2 + 1 {
            warn!(
                "distinct endpoints small than majority of meta cluster nodes {}<{}, endpoints: {:?}",
                distinct_cnt,
                endpoints.len(),
                endpoints
            );
            return Ok(());
        }

        let mut eps = self.endpoints.write().await;
        *eps = endpoints;
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub async fn sync_endpoints(&self) -> Result<(), MetaError> {
        let mut client = self.make_client().await?;
        let result = client
            .member_list(Request::new(MemberListRequest {
                data: "".to_string(),
            }))
            .await;
        let endpoints: Result<MemberListReply, Status> = match result {
            Ok(r) => Ok(r.into_inner()),
            Err(s) => {
                if status_is_retryable(&s) {
                    self.mark_as_unhealthy().await;
                    let mut client = self.make_client().await?;
                    let req = Request::new(MemberListRequest {
                        data: "".to_string(),
                    });
                    Ok(client.member_list(req).await?.into_inner())
                } else {
                    Err(s)
                }
            }
        };
        let result: Vec<String> = endpoints?.data;
        debug!("received meta endpoints: {:?}", result);

        self.set_endpoints(result).await?;
        Ok(())
    }

    async fn auto_sync_endpoints(self: Arc<Self>, mut cancel_tx: OneSend<()>) {
        info!(
            "start auto sync endpoints: interval: {:?}",
            self.auto_sync_interval
        );
        if let Some(interval) = self.auto_sync_interval {
            loop {
                select! {
                    _ = cancel_tx.closed() => {
                        return;
                    }
                    _ = sleep(interval) => {
                        let r = self.sync_endpoints().await;
                        if let Err(e) = r {
                            warn!("auto sync endpoints failed: {:?}", e);
                        }
                    }
                }
            }
        }
    }

    /// Handshake with metasrv.
    ///
    /// - Check whether the versions of this client(`C`) and the remote metasrv(`S`) are compatible.
    /// - Authorize this client.
    ///
    /// ## Check compatibility
    ///
    /// Both client `C` and  server `S` maintains two semantic-version:
    /// - `C` maintains the its own semver(`C.ver`) and the minimal compatible `S` semver(`C.min_srv_ver`).
    /// - `S` maintains the its own semver(`S.ver`) and the minimal compatible `S` semver(`S.min_cli_ver`).
    ///
    /// When handshaking:
    /// - `C` sends its ver `C.ver` to `S`,
    /// - When `S` receives handshake request, `S` asserts that `C.ver >= S.min_cli_ver`.
    /// - Then `S` replies handshake-reply with its `S.ver`.
    /// - When `C` receives the reply, `C` asserts that `S.ver >= C.min_srv_ver`.
    ///
    /// Handshake succeeds if both of these two assertions hold.
    ///
    /// E.g.:
    /// - `S: (ver=3, min_cli_ver=1)` is compatible with `C: (ver=3, min_srv_ver=2)`.
    /// - `S: (ver=4, min_cli_ver=4)` is **NOT** compatible with `C: (ver=3, min_srv_ver=2)`.
    ///   Because although `S.ver(4) >= C.min_srv_ver(3)` holds,
    ///   but `C.ver(3) >= S.min_cli_ver(4)` does not hold.
    ///
    /// ```text
    /// C.ver:    1             3      4
    /// C --------+-------------+------+------------>
    ///           ^      .------'      ^
    ///           |      |             |
    ///           '-------------.      |
    ///                  |      |      |
    ///                  v      |      |
    /// S ---------------+------+------+------------>
    /// S.ver:           2      3      4
    /// ```
    #[tracing::instrument(level = "debug", skip(client, password, client_ver, min_metasrv_ver))]
    pub async fn handshake(
        client: &mut MetaServiceClient<Channel>,
        client_ver: &Version,
        min_metasrv_ver: &Version,
        username: &str,
        password: &str,
    ) -> Result<Vec<u8>, MetaHandshakeError> {
        debug!(
            client_ver = display(client_ver),
            min_metasrv_ver = display(min_metasrv_ver),
            "client version"
        );

        let auth = BasicAuth {
            username: username.to_string(),
            password: password.to_string(),
        };
        let mut payload = vec![];
        auth.encode(&mut payload)
            .map_err(|e| MetaHandshakeError::new("encode auth payload", &e))?;

        let my_ver = to_digit_ver(client_ver);
        let req = Request::new(futures::stream::once(async move {
            HandshakeRequest {
                protocol_version: my_ver,
                payload,
            }
        }));

        let rx = client
            .handshake(req)
            .await
            .map_err(|e| MetaHandshakeError::new("when sending handshake rpc", &e))?;
        let mut rx = rx.into_inner();

        let res = rx.next().await.ok_or_else(|| {
            MetaHandshakeError::new(
                "when recv from handshake stream",
                &AnyError::error("handshake returns nothing"),
            )
        })?;

        let resp =
            res.map_err(|status| MetaHandshakeError::new("handshake is refused", &status))?;

        assert!(
            resp.protocol_version > 0,
            "talking to a very old databend-meta: upgrade databend-meta to at least 0.8"
        );

        let min_compatible = to_digit_ver(min_metasrv_ver);
        if resp.protocol_version < min_compatible {
            let invalid_err = AnyError::error(format!(
                "metasrv protocol_version({}) < meta-client min-compatible({})",
                from_digit_ver(resp.protocol_version),
                min_metasrv_ver,
            ));
            return Err(MetaHandshakeError::new(
                "incompatible protocol version",
                &invalid_err,
            ));
        }

        let token = resp.payload;
        Ok(token)
    }

    /// Create a watching stream that receives KV change events.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn watch(
        &self,
        watch_request: WatchRequest,
    ) -> Result<tonic::codec::Streaming<WatchResponse>, MetaError> {
        debug!(
            watch_request = debug(&watch_request),
            "MetaGrpcClient worker: handle watch request"
        );

        let mut client = self.make_client().await?;
        let res = client.watch(watch_request).await?;
        Ok(res.into_inner())
    }

    /// Export all data in json from metasrv.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn export(
        &self,
        export_request: message::ExportReq,
    ) -> Result<tonic::codec::Streaming<ExportedChunk>, MetaError> {
        debug!(
            export_request = debug(&export_request),
            "MetaGrpcClient worker: handle export request"
        );

        let mut client = self.make_client().await?;
        let res = client.export(Empty {}).await?;
        Ok(res.into_inner())
    }

    /// Export all data in json from metasrv.
    #[tracing::instrument(level = "debug", skip_all)]
    pub(crate) async fn get_client_info(&self) -> Result<ClientInfo, MetaError> {
        debug!("MetaGrpcClient::get_client_info");

        let mut client = self.make_client().await?;
        let res = client.get_client_info(Empty {}).await?;
        Ok(res.into_inner())
    }

    #[tracing::instrument(level = "debug", skip(self, v))]
    pub(crate) async fn kv_api<T, R>(&self, v: T) -> Result<R, MetaError>
    where
        T: RequestFor<Reply = R>,
        T: Into<MetaGrpcReq>,
        R: DeserializeOwned,
    {
        let read_req: MetaGrpcReq = v.into();

        debug!(req = debug(&read_req), "MetaGrpcClient::kv_api request");

        let req: Request<RaftRequest> = read_req.clone().try_into().map_err(|e| {
            MetaNetworkError::InvalidArgument(InvalidArgument::new(e, "fail to encode request"))
        })?;

        debug!(
            req = debug(&req),
            "MetaGrpcClient::kv_api serialized request"
        );

        let req = common_tracing::inject_span_to_tonic_request(req);

        let mut client = self.make_client().await?;
        let result = client.kv_api(req).await;

        debug!(reply = debug(&result), "MetaGrpcClient::kv_api reply");

        let rpc_res: Result<RaftReply, Status> = match result {
            Ok(r) => Ok(r.into_inner()),
            Err(s) => {
                if status_is_retryable(&s) {
                    self.mark_as_unhealthy().await;
                    let mut client = self.make_client().await?;
                    let req: Request<RaftRequest> = read_req.try_into().map_err(|e| {
                        MetaNetworkError::InvalidArgument(InvalidArgument::new(
                            e,
                            "fail to encode request",
                        ))
                    })?;
                    let req = common_tracing::inject_span_to_tonic_request(req);
                    Ok(client.kv_api(req).await?.into_inner())
                } else {
                    Err(s)
                }
            }
        };
        let raft_reply = rpc_res?;

        let resp: R = reply_to_api_result(raft_reply)?;
        Ok(resp)
    }

    #[tracing::instrument(level = "debug", skip(self, req))]
    pub(crate) async fn transaction(&self, req: TxnRequest) -> Result<TxnReply, MetaError> {
        let txn: TxnRequest = req;

        debug!(req = display(&txn), "MetaGrpcClient::transaction request");

        let req: Request<TxnRequest> = Request::new(txn.clone());
        let req = common_tracing::inject_span_to_tonic_request(req);

        let mut client = self.make_client().await?;
        let result = client.transaction(req).await;

        let result: Result<TxnReply, Status> = match result {
            Ok(r) => return Ok(r.into_inner()),
            Err(s) => {
                if status_is_retryable(&s) {
                    self.mark_as_unhealthy().await;
                    let mut client = self.make_client().await?;
                    let req: Request<TxnRequest> = Request::new(txn);
                    let req = common_tracing::inject_span_to_tonic_request(req);
                    let ret = client.transaction(req).await?.into_inner();
                    return Ok(ret);
                } else {
                    Err(s)
                }
            }
        };

        let reply = result?;

        debug!(reply = display(&reply), "MetaGrpcClient::transaction reply");

        Ok(reply)
    }
    async fn mark_as_unhealthy(&self) {
        let ca = self.current_endpoint.lock();
        let mut ue = self.unhealthy_endpoints.lock();
        ue.insert((*ca).as_ref().unwrap().clone(), ());
    }
}

fn status_is_retryable(status: &Status) -> bool {
    matches!(
        status.code(),
        Code::Unauthenticated | Code::Unavailable | Code::Internal
    )
}

#[derive(Clone)]
pub struct AuthInterceptor {
    pub token: Vec<u8>,
}

impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        let metadata = req.metadata_mut();
        metadata.insert_bin(AUTH_TOKEN_KEY, MetadataValue::from_bytes(&self.token));
        Ok(req)
    }
}
