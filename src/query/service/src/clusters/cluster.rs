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

use std::net::SocketAddr;
use std::ops::RangeInclusive;
use std::str::FromStr;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use common_arrow::arrow_format::flight::service::flight_service_client::FlightServiceClient;
use common_base::base::tokio;
use common_base::base::tokio::sync::Mutex;
use common_base::base::tokio::sync::Notify;
use common_base::base::tokio::task::JoinHandle;
use common_base::base::tokio::time::sleep as tokio_async_sleep;
use common_base::base::DummySignalStream;
use common_base::base::GlobalInstance;
use common_base::base::SignalStream;
use common_base::base::SignalType;
pub use common_catalog::cluster_info::Cluster;
use common_config::InnerConfig;
use common_config::DATABEND_COMMIT_VERSION;
use common_exception::ErrorCode;
use common_exception::Result;
use common_grpc::ConnectionFactory;
use common_management::ClusterApi;
use common_management::ClusterMgr;
use common_meta_store::MetaStore;
use common_meta_store::MetaStoreProvider;
use common_meta_types::MatchSeq;
use common_meta_types::NodeInfo;
use common_metrics::cluster::*;
use futures::future::select;
use futures::future::Either;
use futures::Future;
use futures::StreamExt;
use log::error;
use log::warn;
use rand::thread_rng;
use rand::Rng;

use crate::api::FlightClient;

pub struct ClusterDiscovery {
    local_id: String,
    heartbeat: Mutex<ClusterHeartbeat>,
    api_provider: Arc<dyn ClusterApi>,
    cluster_id: String,
    tenant_id: String,
    flight_address: String,
}

// avoid leak FlightClient to common-xxx
#[async_trait::async_trait]
pub trait ClusterHelper {
    fn create(nodes: Vec<Arc<NodeInfo>>, local_id: String) -> Arc<Cluster>;
    fn empty() -> Arc<Cluster>;
    fn is_empty(&self) -> bool;
    fn is_local(&self, node: &NodeInfo) -> bool;
    fn local_id(&self) -> String;
    async fn create_node_conn(&self, name: &str, config: &InnerConfig) -> Result<FlightClient>;
    fn get_nodes(&self) -> Vec<Arc<NodeInfo>>;
}

#[async_trait::async_trait]
impl ClusterHelper for Cluster {
    fn create(nodes: Vec<Arc<NodeInfo>>, local_id: String) -> Arc<Cluster> {
        Arc::new(Cluster { local_id, nodes })
    }

    fn empty() -> Arc<Cluster> {
        Arc::new(Cluster {
            local_id: String::from(""),
            nodes: Vec::new(),
        })
    }

    fn is_empty(&self) -> bool {
        self.nodes.len() <= 1
    }

    fn is_local(&self, node: &NodeInfo) -> bool {
        node.id == self.local_id
    }

    fn local_id(&self) -> String {
        self.local_id.clone()
    }

    #[async_backtrace::framed]
    async fn create_node_conn(&self, name: &str, config: &InnerConfig) -> Result<FlightClient> {
        for node in &self.nodes {
            if node.id == name {
                return match config.tls_query_cli_enabled() {
                    true => Ok(FlightClient::new(FlightServiceClient::new(
                        ConnectionFactory::create_rpc_channel(
                            node.flight_address.clone(),
                            None,
                            Some(config.query.to_rpc_client_tls_config()),
                        )
                        .await?,
                    ))),
                    false => Ok(FlightClient::new(FlightServiceClient::new(
                        ConnectionFactory::create_rpc_channel(
                            node.flight_address.clone(),
                            None,
                            None,
                        )
                        .await?,
                    ))),
                };
            }
        }

        Err(ErrorCode::NotFoundClusterNode(format!(
            "The node \"{}\" not found in the cluster",
            name
        )))
    }

    fn get_nodes(&self) -> Vec<Arc<NodeInfo>> {
        self.nodes.to_vec()
    }
}

impl ClusterDiscovery {
    #[async_backtrace::framed]
    pub async fn create_meta_client(cfg: &InnerConfig) -> Result<MetaStore> {
        let meta_api_provider = MetaStoreProvider::new(cfg.meta.to_meta_grpc_client_conf());
        match meta_api_provider.create_meta_store().await {
            Ok(meta_store) => Ok(meta_store),
            Err(cause) => {
                Err(ErrorCode::from(cause).add_message_back("(while create cluster api)."))
            }
        }
    }

    #[async_backtrace::framed]
    pub async fn init(cfg: &InnerConfig) -> Result<()> {
        let metastore = ClusterDiscovery::create_meta_client(cfg).await?;
        GlobalInstance::set(Self::try_create(cfg, metastore).await?);

        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn try_create(
        cfg: &InnerConfig,
        metastore: MetaStore,
    ) -> Result<Arc<ClusterDiscovery>> {
        let (lift_time, provider) = Self::create_provider(cfg, metastore)?;

        Ok(Arc::new(ClusterDiscovery {
            local_id: cfg.query.node_id.clone(),
            api_provider: provider.clone(),
            heartbeat: Mutex::new(ClusterHeartbeat::create(
                lift_time,
                provider,
                cfg.query.cluster_id.clone(),
                cfg.query.tenant_id.clone(),
            )),
            cluster_id: cfg.query.cluster_id.clone(),
            tenant_id: cfg.query.tenant_id.clone(),
            flight_address: cfg.query.flight_api_address.clone(),
        }))
    }

    pub fn instance() -> Arc<ClusterDiscovery> {
        GlobalInstance::get()
    }

    fn create_provider(
        cfg: &InnerConfig,
        metastore: MetaStore,
    ) -> Result<(Duration, Arc<dyn ClusterApi>)> {
        // TODO: generate if tenant or cluster id is empty
        let tenant_id = &cfg.query.tenant_id;
        let cluster_id = &cfg.query.cluster_id;
        let lift_time = Duration::from_secs(60);
        let cluster_manager = ClusterMgr::create(metastore, tenant_id, cluster_id, lift_time)?;

        Ok((lift_time, Arc::new(cluster_manager)))
    }

    #[async_backtrace::framed]
    pub async fn discover(&self, config: &InnerConfig) -> Result<Arc<Cluster>> {
        match self.api_provider.get_nodes().await {
            Err(cause) => {
                metric_incr_cluster_error_count(
                    &self.local_id,
                    "discover",
                    &self.cluster_id,
                    &self.tenant_id,
                    &self.flight_address,
                );
                Err(cause.add_message_back("(while cluster api get_nodes)."))
            }
            Ok(cluster_nodes) => {
                let mut res = Vec::with_capacity(cluster_nodes.len());
                for node in &cluster_nodes {
                    if node.id != self.local_id {
                        let start_at = Instant::now();
                        if let Err(cause) = create_client(config, &node.flight_address).await {
                            warn!(
                                "{} cannot connect node [{:?}] after {:?}s, remove it in query, cluster nodes:{:?}. cause: {:?}",
                                self.local_id,
                                node.flight_address,
                                start_at.elapsed().as_secs_f32(),
                                cluster_nodes,
                                cause
                            );

                            continue;
                        }
                    }

                    res.push(Arc::new(node.clone()));
                }

                metrics_gauge_discovered_nodes(
                    &self.local_id,
                    &self.cluster_id,
                    &self.tenant_id,
                    &self.flight_address,
                    cluster_nodes.len() as f64,
                );
                Ok(Cluster::create(res, self.local_id.clone()))
            }
        }
    }

    #[async_backtrace::framed]
    async fn drop_invalid_nodes(self: &Arc<Self>, node_info: &NodeInfo) -> Result<()> {
        let current_nodes_info = match self.api_provider.get_nodes().await {
            Ok(nodes) => nodes,
            Err(cause) => {
                metric_incr_cluster_error_count(
                    &self.local_id,
                    "drop_invalid_ndes.get_nodes",
                    &self.cluster_id,
                    &self.tenant_id,
                    &self.flight_address,
                );
                return Err(cause.add_message_back("(while drop_invalid_nodes)"));
            }
        };

        for before_node in current_nodes_info {
            // Restart in a very short time(< heartbeat timeout) after abnormal shutdown, Which will
            // lead to some invalid information
            if before_node.flight_address.eq(&node_info.flight_address) {
                let drop_invalid_node =
                    self.api_provider.drop_node(before_node.id, MatchSeq::GE(1));
                if let Err(cause) = drop_invalid_node.await {
                    warn!("Drop invalid node failure: {:?}", cause);
                }
            }
        }

        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn unregister_to_metastore(self: &Arc<Self>, signal: &mut SignalStream) {
        let mut heartbeat = self.heartbeat.lock().await;

        if let Err(shutdown_failure) = heartbeat.shutdown().await {
            warn!(
                "Cannot shutdown cluster heartbeat, cause {:?}",
                shutdown_failure
            );
        }

        let mut mut_signal_pin = signal.as_mut();
        let signal_future = Box::pin(mut_signal_pin.next());
        let drop_node = Box::pin(
            self.api_provider
                .drop_node(self.local_id.clone(), MatchSeq::GE(1)),
        );
        match futures::future::select(drop_node, signal_future).await {
            Either::Left((drop_node_result, _)) => {
                if let Err(drop_node_failure) = drop_node_result {
                    warn!(
                        "Cannot drop cluster node(while shutdown), cause {:?}",
                        drop_node_failure
                    );
                }
            }
            Either::Right((signal_type, _)) => {
                match signal_type {
                    None => *signal = DummySignalStream::create(SignalType::Exit),
                    Some(signal_type) => *signal = DummySignalStream::create(signal_type),
                };
            }
        };
    }

    #[async_backtrace::framed]
    pub async fn register_to_metastore(self: &Arc<Self>, cfg: &InnerConfig) -> Result<()> {
        let cpus = cfg.query.num_cpus;
        let mut address = cfg.query.flight_api_address.clone();

        if let Ok(socket_addr) = SocketAddr::from_str(&address) {
            let ip_addr = socket_addr.ip();
            if ip_addr.is_loopback() || ip_addr.is_unspecified() {
                if let Some(local_addr) = self.api_provider.get_local_addr().await? {
                    let local_socket_addr = SocketAddr::from_str(&local_addr)?;
                    let new_addr = format!("{}:{}", local_socket_addr.ip(), socket_addr.port());
                    warn!(
                        "Detected loopback or unspecified address as cluster flight endpoint. \
                        We rewrite it(\"{}\" -> \"{}\") for advertising to other nodes. \
                        If there are proxies between nodes, you can specify endpoint with --flight-api-address.",
                        address, new_addr
                    );

                    address = new_addr;
                }
            }
        }

        let node_info = NodeInfo::create(
            self.local_id.clone(),
            cpus,
            address,
            DATABEND_COMMIT_VERSION.to_string(),
        );

        self.drop_invalid_nodes(&node_info).await?;
        match self.api_provider.add_node(node_info.clone()).await {
            Ok(_) => self.start_heartbeat(node_info).await,
            Err(cause) => Err(cause.add_message_back("(while cluster api add_node).")),
        }
    }

    #[async_backtrace::framed]
    async fn start_heartbeat(self: &Arc<Self>, node_info: NodeInfo) -> Result<()> {
        let mut heartbeat = self.heartbeat.lock().await;
        heartbeat.start(node_info);
        Ok(())
    }
}

struct ClusterHeartbeat {
    timeout: Duration,
    shutdown: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
    cluster_api: Arc<dyn ClusterApi>,
    shutdown_handler: Option<JoinHandle<()>>,
    cluster_id: String,
    tenant_id: String,
}

impl ClusterHeartbeat {
    pub fn create(
        timeout: Duration,
        cluster_api: Arc<dyn ClusterApi>,
        cluster_id: String,
        tenant_id: String,
    ) -> ClusterHeartbeat {
        ClusterHeartbeat {
            timeout,
            cluster_api,
            shutdown: Arc::new(AtomicBool::new(false)),
            shutdown_notify: Arc::new(Notify::new()),
            shutdown_handler: None,
            cluster_id,
            tenant_id,
        }
    }

    fn heartbeat_loop(&self, node: NodeInfo) -> impl Future<Output = ()> + 'static {
        let shutdown = self.shutdown.clone();
        let shutdown_notify = self.shutdown_notify.clone();
        let cluster_api = self.cluster_api.clone();
        let sleep_range = self.heartbeat_interval(self.timeout);
        let cluster_id = self.cluster_id.clone();
        let tenant_id = self.tenant_id.clone();

        async move {
            let mut shutdown_notified = Box::pin(shutdown_notify.notified());

            while !shutdown.load(Ordering::Relaxed) {
                let mills = {
                    let mut rng = thread_rng();
                    rng.gen_range(sleep_range.clone())
                };

                let sleep = tokio_async_sleep(Duration::from_millis(mills as u64));

                match select(shutdown_notified, Box::pin(sleep)).await {
                    Either::Left((_, _)) => {
                        break;
                    }
                    Either::Right((_, new_shutdown_notified)) => {
                        shutdown_notified = new_shutdown_notified;
                        let heartbeat = cluster_api.heartbeat(&node, MatchSeq::GE(1));
                        if let Err(failure) = heartbeat.await {
                            metric_incr_cluster_heartbeat_count(
                                &node.id,
                                &node.flight_address,
                                &cluster_id,
                                &tenant_id,
                                "failure",
                            );
                            error!("Cluster cluster api heartbeat failure: {:?}", failure);
                        }
                    }
                }
            }
        }
    }

    fn heartbeat_interval(&self, duration: Duration) -> RangeInclusive<u128> {
        (duration / 3).as_millis()..=((duration / 3) * 2).as_millis()
    }

    pub fn start(&mut self, node_info: NodeInfo) {
        self.shutdown_handler = Some(tokio::spawn(
            async_backtrace::location!().frame(self.heartbeat_loop(node_info)),
        ));
    }

    #[async_backtrace::framed]
    pub async fn shutdown(&mut self) -> Result<()> {
        if let Some(shutdown_handler) = self.shutdown_handler.take() {
            self.shutdown.store(true, Ordering::Relaxed);
            self.shutdown_notify.notify_waiters();
            if let Err(shutdown_failure) = shutdown_handler.await {
                return Err(ErrorCode::TokioError(format!(
                    "Cannot shutdown cluster heartbeat, cause {:?}",
                    shutdown_failure
                )));
            }
        }
        Ok(())
    }
}

#[async_backtrace::framed]
pub async fn create_client(config: &InnerConfig, address: &str) -> Result<FlightClient> {
    let timeout = if config.query.rpc_client_timeout_secs > 0 {
        Some(Duration::from_secs(config.query.rpc_client_timeout_secs))
    } else {
        None
    };

    let rpc_tls_config = if config.tls_query_cli_enabled() {
        Some(config.query.to_rpc_client_tls_config())
    } else {
        None
    };

    Ok(FlightClient::new(FlightServiceClient::new(
        ConnectionFactory::create_rpc_channel(address.to_owned(), timeout, rpc_tls_config).await?,
    )))
}
