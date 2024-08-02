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

use std::cell::SyncUnsafeCell;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::ops::Deref;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use databend_common_arrow::arrow_format::flight::service::flight_service_client::FlightServiceClient;
use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::Thread;
use databend_common_base::runtime::TrySpawn;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_grpc::ConnectionFactory;
use databend_common_pipeline_core::ExecutionInfo;
use databend_common_sql::executor::PhysicalPlan;
use fastrace::prelude::*;
use log::warn;
use parking_lot::Mutex;
use parking_lot::ReentrantMutex;
use petgraph::prelude::EdgeRef;
use petgraph::Direction;
use tokio::task::JoinHandle;

use super::exchange_params::ExchangeParams;
use super::exchange_params::MergeExchangeParams;
use super::exchange_params::ShuffleExchangeParams;
use super::exchange_sink::ExchangeSink;
use super::exchange_transform::ExchangeTransform;
use super::statistics_receiver::StatisticsReceiver;
use super::statistics_sender::StatisticsSender;
use crate::clusters::ClusterHelper;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::PipelineBuilder;
use crate::schedulers::QueryFragmentActions;
use crate::schedulers::QueryFragmentsActions;
use crate::servers::flight::flight_client::FlightDataAckState;
use crate::servers::flight::flight_client::FlightDataAckStream;
use crate::servers::flight::v1::actions::init_query_fragments;
use crate::servers::flight::v1::actions::INIT_QUERY_FRAGMENTS;
use crate::servers::flight::v1::actions::START_PREPARED_QUERY;
use crate::servers::flight::v1::exchange::DataExchange;
use crate::servers::flight::v1::exchange::DefaultExchangeInjector;
use crate::servers::flight::v1::exchange::ExchangeInjector;
use crate::servers::flight::v1::packets::Edge;
use crate::servers::flight::v1::packets::QueryEnv;
use crate::servers::flight::v1::packets::QueryFragment;
use crate::servers::flight::v1::packets::QueryFragments;
use crate::servers::flight::FlightClient;
use crate::servers::flight::FlightExchange;
use crate::servers::flight::FlightSender;
use crate::servers::flight::RetryableFlightReceiver;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct DataExchangeManager {
    queries_coordinator: ReentrantMutex<SyncUnsafeCell<HashMap<String, QueryCoordinator>>>,
}

impl DataExchangeManager {
    pub fn init() -> Result<()> {
        GlobalInstance::set(Arc::new(DataExchangeManager {
            queries_coordinator: ReentrantMutex::new(SyncUnsafeCell::new(HashMap::new())),
        }));

        Ok(())
    }

    pub fn instance() -> Arc<DataExchangeManager> {
        GlobalInstance::get()
    }

    pub fn get_query_ctx(&self, query_id: &str) -> Result<Arc<QueryContext>> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        if let Some(coordinator) = queries_coordinator.get_mut(query_id) {
            if let Some(coordinator) = &coordinator.info {
                return Ok(coordinator.query_ctx.clone());
            }
        }

        Err(ErrorCode::Internal(format!(
            "Query {} not found in cluster.",
            query_id
        )))
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn init_query_env(
        &self,
        env: &QueryEnv,
        ctx: Option<Arc<QueryContext>>,
    ) -> Result<()> {
        enum QueryExchange {
            Fragment {
                source: String,
                fragment: usize,
                exchange: FlightExchange,
            },
            Statistics {
                source: String,
                exchange: FlightExchange,
            },
        }

        let config = GlobalConfig::instance();
        let with_cur_rt = env.create_rpc_clint_with_current_rt;

        let flight_retry_times = env.settings.get_max_flight_retry_times()?;
        let flight_retry_interval = env.settings.get_flight_retry_interval()?;

        let mut request_exchanges = HashMap::new();
        let mut targets_exchanges = HashMap::new();

        for index in env.dataflow_diagram.node_indices() {
            if env.dataflow_diagram[index].id == config.query.node_id {
                let edges = env
                    .dataflow_diagram
                    .edges_directed(index, Direction::Incoming);

                let mut flight_exchanges = vec![];
                for edge in edges {
                    let source = env.dataflow_diagram[edge.source()].clone();
                    let target = env.dataflow_diagram[edge.target()].clone();
                    let edge = edge.weight().clone();

                    let query_id = env.query_id.clone();
                    let address = source.flight_address.clone();

                    flight_exchanges.push(async move {
                        let mut flight_client = Self::create_client(&address, with_cur_rt).await?;

                        Ok::<QueryExchange, ErrorCode>(match edge {
                            Edge::Fragment(v) => QueryExchange::Fragment {
                                source: source.id.clone(),
                                fragment: v,
                                exchange: flight_client
                                    .do_get(
                                        &query_id,
                                        &target.id,
                                        v,
                                        &address,
                                        flight_retry_times,
                                        flight_retry_interval,
                                    )
                                    .await?,
                            },
                            Edge::Statistics => QueryExchange::Statistics {
                                source: source.id.clone(),
                                exchange: flight_client
                                    .request_server_exchange(
                                        &query_id,
                                        &target.id,
                                        &address,
                                        flight_retry_times,
                                        flight_retry_interval,
                                    )
                                    .await?,
                            },
                        })
                    });
                }

                let flight_exchanges = futures::future::try_join_all(flight_exchanges).await?;
                for flight_exchange in flight_exchanges {
                    match flight_exchange {
                        QueryExchange::Fragment {
                            source,
                            fragment,
                            exchange,
                        } => {
                            targets_exchanges.insert((source, fragment), exchange);
                        }
                        QueryExchange::Statistics { source, exchange } => {
                            request_exchanges.insert(source, exchange);
                        }
                    };
                }

                let mut query_info = Self::create_info(ctx)?;

                if let Some(query_info) = query_info.as_mut() {
                    let query_id = env.query_id.clone();
                    query_info.remove_leak_query_worker =
                        Some(GlobalIORuntime::instance().spawn(async move {
                            let _ = tokio::time::sleep(Duration::from_secs(180)).await;
                            DataExchangeManager::instance().remove_if_leak_query(query_id);
                        }));
                }

                let queries_coordinator_guard = self.queries_coordinator.lock();
                let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

                match queries_coordinator.entry(env.query_id.clone()) {
                    Entry::Occupied(mut v) => {
                        let query_coordinator = v.get_mut();
                        query_coordinator.info = query_info;
                        query_coordinator.add_fragment_exchanges(targets_exchanges)?;
                        query_coordinator.add_statistics_exchanges(request_exchanges)?;
                    }
                    Entry::Vacant(v) => {
                        let query_coordinator = v.insert(QueryCoordinator::create());
                        query_coordinator.info = query_info;
                        query_coordinator.add_fragment_exchanges(targets_exchanges)?;
                        query_coordinator.add_statistics_exchanges(request_exchanges)?;
                    }
                };

                return Ok(());
            }
        }

        // do nothing
        Ok(())
    }

    fn remove_if_leak_query(&self, query_id: String) {
        let leak_query_id = {
            let queries_coordinator_guard = self.queries_coordinator.lock();
            let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

            match queries_coordinator.get(&query_id) {
                None => None,
                Some(may_leak_query) => {
                    let info = may_leak_query.info.as_ref().expect("expect query info");
                    match info.started.load(Ordering::SeqCst) {
                        true => None,
                        false => Some(query_id),
                    }
                }
            }
        };

        if let Some(query_id) = leak_query_id {
            warn!(
                "Query {} cannot start command while in 180 seconds",
                query_id
            );
            self.on_finished_query(&query_id);
        }
    }

    fn create_info(query_ctx: Option<Arc<QueryContext>>) -> Result<Option<QueryInfo>> {
        match query_ctx {
            None => Ok(None),
            Some(query_ctx) => {
                let query_id = query_ctx.get_id();

                Ok(Some(QueryInfo {
                    query_ctx,
                    query_executor: None,
                    query_id: query_id.clone(),
                    started: AtomicBool::new(false),
                    current_executor: GlobalConfig::instance().query.node_id.clone(),
                    remove_leak_query_worker: None,
                }))
            }
        }
    }

    #[async_backtrace::framed]
    pub async fn create_client(address: &str, use_current_rt: bool) -> Result<FlightClient> {
        let config = GlobalConfig::instance();
        let address = address.to_string();
        let task = async move {
            match config.tls_query_cli_enabled() {
                true => Ok(FlightClient::new(FlightServiceClient::new(
                    ConnectionFactory::create_rpc_channel(
                        address.to_owned(),
                        None,
                        Some(config.query.to_rpc_client_tls_config()),
                    )
                    .await?,
                ))),
                false => Ok(FlightClient::new(FlightServiceClient::new(
                    ConnectionFactory::create_rpc_channel(address.to_owned(), None, None).await?,
                ))),
            }
        };
        if use_current_rt {
            task.await
        } else {
            GlobalIORuntime::instance()
                .spawn(task)
                .await
                .expect("create client future must be joined successfully")
        }
    }

    pub fn set_ctx(&self, query_id: &str, ctx: Arc<QueryContext>) -> Result<()> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };
        match queries_coordinator.get_mut(query_id) {
            None => Err(ErrorCode::Internal(format!(
                "Query {} not found in cluster.",
                query_id
            ))),
            Some(coordinator) => {
                if let Some(info) = coordinator.info.as_mut() {
                    info.query_ctx = ctx;
                    return Ok(());
                }

                coordinator.info = Self::create_info(Some(ctx))?;
                Ok(())
            }
        }
    }

    // Execute query in background
    #[fastrace::trace]
    pub fn execute_partial_query(&self, query_id: &str) -> Result<()> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.get_mut(query_id) {
            None => Err(ErrorCode::Internal(format!(
                "Query {} not found in cluster.",
                query_id
            ))),
            Some(coordinator) => coordinator.execute_pipeline(),
        }
    }

    // Create a pipeline based on query plan
    #[fastrace::trace]
    pub fn init_query_fragments_plan(&self, fragments: &QueryFragments) -> Result<()> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        // TODO: When the query is not executed for a long time after submission, we need to remove it
        match queries_coordinator.get_mut(&fragments.query_id) {
            None => Err(ErrorCode::Internal(format!(
                "Query {} not found in cluster.",
                fragments.query_id
            ))),
            Some(query_coordinator) => query_coordinator.prepare_pipeline(fragments),
        }
    }

    #[fastrace::trace]
    pub fn handle_statistics_exchange(
        &self,
        id: String,
        target: String,
        continue_from: usize,
    ) -> Result<FlightDataAckStream> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.entry(id) {
            Entry::Occupied(mut v) => v.get_mut().add_statistics_exchange(target, continue_from),
            Entry::Vacant(v) => match continue_from == 0 {
                true => v
                    .insert(QueryCoordinator::create())
                    .add_statistics_exchange(target, 0),
                false => Err(ErrorCode::Timeout(
                    "Reconnection timeout, the state has been cleared.",
                )),
            },
        }
    }

    #[fastrace::trace]
    pub fn handle_exchange_fragment(
        &self,
        query: String,
        target: String,
        fragment: usize,
        continue_from: usize,
    ) -> Result<FlightDataAckStream> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.entry(query) {
            Entry::Occupied(mut v) => {
                v.get_mut()
                    .add_fragment_exchange(target, fragment, continue_from)
            }
            Entry::Vacant(v) => match continue_from == 0 {
                true => v
                    .insert(QueryCoordinator::create())
                    .add_fragment_exchange(target, fragment, 0),
                false => Err(ErrorCode::Timeout(
                    "Reconnection timeout, the state has been cleared.",
                )),
            },
        }
    }

    pub fn shutdown_query(&self, query_id: &str) {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        if let Some(query_coordinator) = queries_coordinator.get_mut(query_id) {
            query_coordinator.shutdown_query();
        }
    }

    #[fastrace::trace]
    pub fn on_finished_query(&self, query_id: &str) {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        if let Some(mut query_coordinator) = queries_coordinator.remove(query_id) {
            // Drop mutex guard to avoid deadlock during shutdown,
            drop(queries_coordinator_guard);

            query_coordinator.shutdown_query();
            query_coordinator.on_finished();
        }
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn commit_actions(
        &self,
        ctx: Arc<QueryContext>,
        actions: QueryFragmentsActions,
    ) -> Result<PipelineBuildResult> {
        let settings = ctx.get_settings();
        let timeout = settings.get_flight_client_timeout()?;
        let root_actions = actions.get_root_actions()?;
        let conf = GlobalConfig::instance();

        // Initialize query env between cluster nodes
        let query_env = actions.get_query_env()?;
        query_env.init(&ctx, timeout).await?;

        // Submit distributed tasks to all nodes.
        let cluster = ctx.get_cluster();
        let mut query_fragments = actions.get_query_fragments()?;

        let local_fragments = query_fragments.remove(&conf.query.node_id);

        let _: HashMap<String, ()> = cluster
            .do_action(INIT_QUERY_FRAGMENTS, query_fragments, timeout)
            .await?;

        self.set_ctx(&ctx.get_id(), ctx.clone())?;
        if let Some(query_fragments) = local_fragments {
            init_query_fragments(query_fragments).await?;
        }

        // Get local pipeline of local task
        let build_res = self.get_root_pipeline(ctx, root_actions)?;

        let prepared_query = actions.prepared_query()?;
        let _: HashMap<String, ()> = cluster
            .do_action(START_PREPARED_QUERY, prepared_query, timeout)
            .await?;

        Ok(build_res)
    }

    fn get_root_pipeline(
        &self,
        ctx: Arc<QueryContext>,
        root_actions: &QueryFragmentActions,
    ) -> Result<PipelineBuildResult> {
        let query_id = ctx.get_id();
        let fragment_id = root_actions.fragment_id;

        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.get_mut(&query_id) {
            None => Err(ErrorCode::Internal("Query not exists.")),
            Some(query_coordinator) => {
                query_coordinator.assert_leak_fragment_exchanges();
                let injector = DefaultExchangeInjector::create();
                let mut build_res =
                    query_coordinator.subscribe_fragment(&ctx, fragment_id, injector)?;

                // query_coordinator.exchanges
                let exchanges = query_coordinator.take_statistics_receivers();
                let statistics_receiver = StatisticsReceiver::spawn_receiver(&ctx, exchanges)?;

                let statistics_receiver: Mutex<StatisticsReceiver> =
                    Mutex::new(statistics_receiver);

                // Interrupting the execution of finished callback if network error
                build_res
                    .main_pipeline
                    .lift_on_finished(move |info: &ExecutionInfo| {
                        let query_id = ctx.get_id();
                        let mut statistics_receiver = statistics_receiver.lock();

                        statistics_receiver.shutdown(info.res.is_err());
                        ctx.get_exchange_manager().on_finished_query(&query_id);
                        statistics_receiver.wait_shutdown()
                    });

                // Return if it‘s an error returned by another query node
                build_res
                    .main_pipeline
                    .set_on_finished(move |info: &ExecutionInfo| match &info.res {
                        Ok(_) => Ok(()),
                        Err(error_code) => Err(error_code.clone()),
                    });

                Ok(build_res)
            }
        }
    }

    pub fn get_flight_sender(&self, params: &ExchangeParams) -> Result<Vec<FlightSender>> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.get_mut(&params.get_query_id()) {
            None => Err(ErrorCode::Internal("Query not exists.")),
            Some(coordinator) => coordinator.get_flight_senders(params),
        }
    }

    pub fn get_flight_receiver(
        &self,
        params: &ExchangeParams,
    ) -> Result<Vec<(String, RetryableFlightReceiver)>> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.get_mut(&params.get_query_id()) {
            None => Err(ErrorCode::Internal("Query not exists.")),
            Some(coordinator) => coordinator.take_flight_receiver(params),
        }
    }

    pub fn get_fragment_source(
        &self,
        query_id: &str,
        fragment_id: usize,
        injector: Arc<dyn ExchangeInjector>,
    ) -> Result<PipelineBuildResult> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.get_mut(query_id) {
            None => Err(ErrorCode::Internal("Query not exists.")),
            Some(query_coordinator) => {
                let query_ctx = query_coordinator
                    .info
                    .as_ref()
                    .expect("QueryInfo is none")
                    .query_ctx
                    .clone();

                query_coordinator.subscribe_fragment(&query_ctx, fragment_id, injector)
            }
        }
    }
}

struct QueryInfo {
    query_id: String,
    started: AtomicBool,
    current_executor: String,
    query_ctx: Arc<QueryContext>,
    remove_leak_query_worker: Option<JoinHandle<()>>,
    query_executor: Option<Arc<PipelineCompleteExecutor>>,
}

#[derive(Hash, Eq, PartialEq)]
pub struct FragmentExchangeIdentifier {
    target: String,
    fragment: usize,
}

#[derive(Hash, Eq, PartialEq)]
pub enum ExchangeIdentifier {
    Statistics(String),
    DataSender(FragmentExchangeIdentifier),
    DataReceiver(FragmentExchangeIdentifier),
}

impl ExchangeIdentifier {
    pub fn fragment_sender(target: String, fragment: usize) -> Self {
        ExchangeIdentifier::DataSender(FragmentExchangeIdentifier { target, fragment })
    }

    pub fn fragment_receiver(target: String, fragment: usize) -> Self {
        ExchangeIdentifier::DataReceiver(FragmentExchangeIdentifier { target, fragment })
    }
}

struct QueryCoordinator {
    info: Option<QueryInfo>,
    exchanges: HashMap<ExchangeIdentifier, FlightExchange>,
    fragments_coordinator: HashMap<usize, Box<FragmentCoordinator>>,
}

impl QueryCoordinator {
    pub fn create() -> QueryCoordinator {
        QueryCoordinator {
            info: None,
            fragments_coordinator: HashMap::new(),
            exchanges: HashMap::new(),
        }
    }

    pub fn take_statistics_senders(&mut self) -> Vec<FlightSender> {
        let mut statistics_senders = Vec::with_capacity(1);

        for (identifier, exchange) in &mut self.exchanges {
            if let ExchangeIdentifier::Statistics(_) = identifier {
                statistics_senders.push(exchange.take_as_sender());
            }
        }

        statistics_senders
    }

    pub fn take_statistics_receivers(&mut self) -> Vec<RetryableFlightReceiver> {
        let mut statistics_receivers = Vec::with_capacity(self.exchanges.len());

        for (identifier, exchange) in &mut self.exchanges {
            if let ExchangeIdentifier::Statistics(_) = identifier {
                statistics_receivers.push(exchange.take_as_receiver());
            }
        }

        statistics_receivers
    }

    pub fn assert_leak_fragment_exchanges(&self) {
        for (identifier, exchange) in &self.exchanges {
            if !matches!(identifier, ExchangeIdentifier::Statistics(_)) {
                assert!(matches!(
                    exchange,
                    FlightExchange::MovedSender(_) | FlightExchange::MovedReceiver(_)
                ));
            }
        }
    }

    pub fn add_statistics_exchange(
        &mut self,
        target: String,
        begin: usize,
    ) -> Result<FlightDataAckStream> {
        let (tx, rx) = async_channel::bounded(8);
        let identifier = ExchangeIdentifier::Statistics(target);

        match self.exchanges.entry(identifier) {
            Entry::Vacant(v) => {
                let state = FlightDataAckState::create(10, rx);
                v.insert(FlightExchange::create_sender(state.clone(), tx));
                FlightDataAckStream::create(state, begin)
            }
            Entry::Occupied(mut v) => match v.get_mut() {
                FlightExchange::MovedSender(v) => {
                    FlightDataAckStream::create(v.state.clone(), begin)
                }
                _ => Err(ErrorCode::Internal(
                    "statistics exchanges can only have one",
                )),
            },
        }
    }

    pub fn add_statistics_exchanges(
        &mut self,
        exchanges: HashMap<String, FlightExchange>,
    ) -> Result<()> {
        for (source, exchange) in exchanges.into_iter() {
            let identifier = ExchangeIdentifier::Statistics(source);
            if self.exchanges.insert(identifier, exchange).is_some() {
                return Err(ErrorCode::Internal(
                    "Internal error, statistics exchange can only have one.",
                ));
            }
        }

        Ok(())
    }

    pub fn add_fragment_exchange(
        &mut self,
        target: String,
        fragment: usize,
        begin: usize,
    ) -> Result<FlightDataAckStream> {
        let (tx, rx) = async_channel::bounded(8);
        let identifier = ExchangeIdentifier::fragment_sender(target, fragment);

        match self.exchanges.entry(identifier) {
            Entry::Vacant(v) => {
                let state = FlightDataAckState::create(10, rx);
                v.insert(FlightExchange::create_sender(state.clone(), tx));
                FlightDataAckStream::create(state, begin)
            }
            Entry::Occupied(mut v) => match v.get_mut() {
                FlightExchange::MovedSender(v) => {
                    FlightDataAckStream::create(v.state.clone(), begin)
                }
                _ => Err(ErrorCode::Internal("fragment exchange can only have one")),
            },
        }
    }

    pub fn add_fragment_exchanges(
        &mut self,
        exchanges: HashMap<(String, usize), FlightExchange>,
    ) -> Result<()> {
        for ((source, fragment), exchange) in exchanges.into_iter() {
            let identifier = ExchangeIdentifier::fragment_receiver(source, fragment);

            self.exchanges.insert(identifier, exchange);
        }

        Ok(())
    }

    pub fn get_flight_senders(&mut self, params: &ExchangeParams) -> Result<Vec<FlightSender>> {
        let mut fragments_exchanges = Vec::with_capacity(self.exchanges.len());

        match params {
            ExchangeParams::MergeExchange(params) => {
                for (identifier, exchange) in &mut self.exchanges {
                    if let ExchangeIdentifier::DataSender(v) = identifier {
                        if v.fragment != params.fragment_id {
                            continue;
                        }

                        fragments_exchanges.push(exchange.take_as_sender());
                    }
                }
            }
            ExchangeParams::ShuffleExchange(params) => {
                for destination in &params.destination_ids {
                    if destination == &params.executor_id {
                        let dummy = FlightSender::create(async_channel::bounded(1).0);
                        fragments_exchanges.push(dummy);
                        continue;
                    }

                    let target = destination.clone();
                    let fragment = params.fragment_id;
                    let identifier = ExchangeIdentifier::fragment_sender(target, fragment);
                    if let Some(v) = self.exchanges.get_mut(&identifier) {
                        fragments_exchanges.push(v.take_as_sender());
                        continue;
                    }

                    return Err(ErrorCode::UnknownFragmentExchange(format!(
                        "Unknown fragment exchange channel, {}, {}",
                        destination, params.fragment_id
                    )));
                }
            }
        };

        Ok(fragments_exchanges)
    }

    pub fn take_flight_receiver(
        &mut self,
        params: &ExchangeParams,
    ) -> Result<Vec<(String, RetryableFlightReceiver)>> {
        let mut fragments_exchanges = Vec::with_capacity(self.exchanges.len());

        match params {
            ExchangeParams::MergeExchange(params) => {
                for (identifier, exchange) in &mut self.exchanges {
                    if let ExchangeIdentifier::DataReceiver(v) = identifier {
                        if v.fragment != params.fragment_id {
                            continue;
                        }

                        fragments_exchanges.push((v.target.clone(), exchange.take_as_receiver()));
                    }
                }
            }
            ExchangeParams::ShuffleExchange(params) => {
                for destination in &params.destination_ids {
                    if destination == &params.executor_id {
                        let dummy = RetryableFlightReceiver::dummy();
                        fragments_exchanges.push((destination.clone(), dummy));
                        continue;
                    }

                    let source = destination.clone();
                    let fragment = params.fragment_id;
                    let identifier = ExchangeIdentifier::fragment_receiver(source, fragment);
                    if let Some(v) = self.exchanges.get_mut(&identifier) {
                        let receiver = v.take_as_receiver();
                        fragments_exchanges.push((destination.clone(), receiver));
                        continue;
                    }

                    return Err(ErrorCode::UnknownFragmentExchange(format!(
                        "Unknown fragment flight receiver, {}, {}",
                        destination, params.fragment_id
                    )));
                }
            }
        };

        Ok(fragments_exchanges)
    }

    pub fn prepare_pipeline(&mut self, fragments: &QueryFragments) -> Result<()> {
        let query_info = self.info.as_ref().expect("expect query info");
        let query_context = query_info.query_ctx.clone();

        for fragment in &fragments.fragments {
            self.fragments_coordinator.insert(
                fragment.fragment_id.to_owned(),
                FragmentCoordinator::create(fragment),
            );
        }

        for fragment in &fragments.fragments {
            let fragment_id = fragment.fragment_id;
            if let Some(coordinator) = self.fragments_coordinator.get_mut(&fragment_id) {
                coordinator.prepare_pipeline(query_context.clone())?;
            }
        }

        Ok(())
    }

    pub fn subscribe_fragment(
        &mut self,
        ctx: &Arc<QueryContext>,
        fragment_id: usize,
        injector: Arc<dyn ExchangeInjector>,
    ) -> Result<PipelineBuildResult> {
        // Merge pipelines if exist locally pipeline
        if let Some(mut fragment_coordinator) = self.fragments_coordinator.remove(&fragment_id) {
            let info = self.info.as_ref().expect("QueryInfo is none");
            fragment_coordinator.prepare_pipeline(ctx.clone())?;

            if fragment_coordinator.pipeline_build_res.is_none() {
                return Err(ErrorCode::Internal(
                    "Pipeline is none, maybe query fragment circular dependency.",
                ));
            }

            if fragment_coordinator.data_exchange.is_none() {
                // When the root fragment and the data has been send to the coordination node,
                // we do not need to wait for the data of other nodes.
                return Ok(fragment_coordinator.pipeline_build_res.unwrap());
            }

            let exchange_params = fragment_coordinator.create_exchange_params(
                info,
                fragment_coordinator
                    .pipeline_build_res
                    .as_ref()
                    .map(|x| x.exchange_injector.clone())
                    .ok_or_else(|| {
                        ErrorCode::Internal("Pipeline build result is none, It's a bug")
                    })?,
            )?;
            let mut build_res = fragment_coordinator.pipeline_build_res.unwrap();

            // Add exchange data transform.

            ExchangeTransform::via(
                ctx,
                &exchange_params,
                &mut build_res.main_pipeline,
                injector,
            )?;

            return Ok(build_res);
        }
        Err(ErrorCode::Unimplemented("ExchangeSource is unimplemented"))
    }

    pub fn shutdown_query(&mut self) {
        if let Some(query_info) = &mut self.info {
            if let Some(query_executor) = &query_info.query_executor {
                query_executor.finish(None);
            }

            if let Some(worker) = query_info.remove_leak_query_worker.take() {
                worker.abort();
            }
        }
    }

    pub fn on_finished(self) {
        // Do something when query finished.
    }

    pub fn execute_pipeline(&mut self) -> Result<()> {
        let info = self.info.as_mut().expect("Query info is None");

        if !info.started.swap(true, Ordering::SeqCst) {
            if let Some(leak_worker) = info.remove_leak_query_worker.take() {
                leak_worker.abort();
            }
        }

        if self.fragments_coordinator.is_empty() {
            // Empty fragments if it is a request server, because the pipelines may have been linked.
            return Ok(());
        }

        let max_threads = info.query_ctx.get_settings().get_max_threads()?;
        let mut pipelines = Vec::with_capacity(self.fragments_coordinator.len());

        let mut params = Vec::with_capacity(self.fragments_coordinator.len());
        for coordinator in self.fragments_coordinator.values() {
            params.push(
                coordinator.create_exchange_params(
                    info,
                    coordinator
                        .pipeline_build_res
                        .as_ref()
                        .map(|x| x.exchange_injector.clone())
                        .ok_or_else(|| {
                            ErrorCode::Internal("Pipeline build result is none, It's a bug")
                        })?,
                )?,
            );
        }

        for ((_, coordinator), params) in self.fragments_coordinator.iter_mut().zip(params) {
            if let Some(mut build_res) = coordinator.pipeline_build_res.take() {
                build_res.set_max_threads(max_threads as usize);

                if !build_res.main_pipeline.is_pulling_pipeline()? {
                    return Err(ErrorCode::Internal("Logical error, It's a bug"));
                }

                // Add exchange data publisher.
                ExchangeSink::via(&info.query_ctx, &params, &mut build_res.main_pipeline)?;

                if !build_res.main_pipeline.is_complete_pipeline()? {
                    return Err(ErrorCode::Internal("Logical error, It's a bug"));
                }

                pipelines.push(build_res.main_pipeline);
                pipelines.extend(build_res.sources_pipelines.into_iter());
            }
        }

        let settings = ExecutorSettings::try_create(info.query_ctx.clone())?;
        let executor = PipelineCompleteExecutor::from_pipelines(pipelines, settings)?;

        self.assert_leak_fragment_exchanges();
        let info_mut = self.info.as_mut().expect("Query info is None");
        info_mut.query_executor = Some(executor.clone());

        let query_id = info_mut.query_id.clone();
        let query_ctx = info_mut.query_ctx.clone();

        let ctx = query_ctx.clone();
        let mut statistics_senders = self.take_statistics_senders();

        let Some(statistics_sender) = statistics_senders.pop() else {
            return Err(ErrorCode::Internal(
                "Request server must less than 1 if is not request server.",
            ));
        };

        if !statistics_senders.is_empty() {
            return Err(ErrorCode::Internal(
                "Request server must less than 1 if is not request server.",
            ));
        }

        let mut statistics_sender =
            StatisticsSender::spawn(&query_id, ctx, statistics_sender, executor.get_inner());

        let span = if let Some(parent) = SpanContext::current_local_parent() {
            Span::root("Distributed-Executor", parent)
        } else {
            Span::noop()
        };

        Thread::named_spawn(Some(String::from("Distributed-Executor")), move || {
            let _g = span.set_local_parent();
            statistics_sender.shutdown(executor.execute().err());
            query_ctx
                .get_exchange_manager()
                .on_finished_query(&query_id);
        });

        Ok(())
    }
}

struct FragmentCoordinator {
    initialized: bool,
    fragment_id: usize,
    physical_plan: PhysicalPlan,
    data_exchange: Option<DataExchange>,
    pipeline_build_res: Option<PipelineBuildResult>,
}

impl FragmentCoordinator {
    pub fn create(packet: &QueryFragment) -> Box<FragmentCoordinator> {
        Box::new(FragmentCoordinator {
            initialized: false,
            physical_plan: packet.physical_plan.clone(),
            fragment_id: packet.fragment_id,
            data_exchange: packet.data_exchange.clone(),
            pipeline_build_res: None,
        })
    }

    pub fn create_exchange_params(
        &self,
        info: &QueryInfo,
        exchange_injector: Arc<dyn ExchangeInjector>,
    ) -> Result<ExchangeParams> {
        if let Some(data_exchange) = &self.data_exchange {
            return match data_exchange {
                DataExchange::Merge(exchange) => {
                    Ok(ExchangeParams::MergeExchange(MergeExchangeParams {
                        exchange_injector: exchange_injector.clone(),
                        schema: self.physical_plan.output_schema()?,
                        fragment_id: self.fragment_id,
                        query_id: info.query_id.to_string(),
                        destination_id: exchange.destination_id.clone(),
                        allow_adjust_parallelism: exchange.allow_adjust_parallelism,
                        ignore_exchange: exchange.ignore_exchange,
                    }))
                }
                DataExchange::Broadcast(exchange) => {
                    Ok(ExchangeParams::ShuffleExchange(ShuffleExchangeParams {
                        exchange_injector: exchange_injector.clone(),
                        schema: self.physical_plan.output_schema()?,
                        fragment_id: self.fragment_id,
                        query_id: info.query_id.to_string(),
                        executor_id: info.current_executor.to_string(),
                        destination_ids: exchange.destination_ids.to_owned(),
                        shuffle_scatter: exchange_injector
                            .flight_scatter(&info.query_ctx, data_exchange)?,
                    }))
                }
                DataExchange::ShuffleDataExchange(exchange) => {
                    Ok(ExchangeParams::ShuffleExchange(ShuffleExchangeParams {
                        exchange_injector: exchange_injector.clone(),
                        schema: self.physical_plan.output_schema()?,
                        fragment_id: self.fragment_id,
                        query_id: info.query_id.to_string(),
                        executor_id: info.current_executor.to_string(),
                        destination_ids: exchange.destination_ids.to_owned(),
                        shuffle_scatter: exchange_injector
                            .flight_scatter(&info.query_ctx, data_exchange)?,
                    }))
                }
            };
        }

        Err(ErrorCode::Internal("Cannot find data exchange."))
    }

    pub fn prepare_pipeline(&mut self, ctx: Arc<QueryContext>) -> Result<()> {
        if !self.initialized {
            self.initialized = true;

            let pipeline_ctx = QueryContext::create_from(ctx);

            let pipeline_builder = PipelineBuilder::create(
                pipeline_ctx.get_function_context()?,
                pipeline_ctx.get_settings(),
                pipeline_ctx,
                vec![],
            );

            let res = pipeline_builder.finalize(&self.physical_plan)?;

            self.pipeline_build_res = Some(res);
        }

        Ok(())
    }
}
