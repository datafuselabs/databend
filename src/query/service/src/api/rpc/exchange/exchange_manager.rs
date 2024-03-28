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
use std::sync::Arc;

use async_channel::Receiver;
use databend_common_arrow::arrow_format::flight::data::FlightData;
use databend_common_arrow::arrow_format::flight::service::flight_service_client::FlightServiceClient;
use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::Thread;
use databend_common_base::runtime::TrySpawn;
use databend_common_base::GLOBAL_TASK;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_grpc::ConnectionFactory;
use databend_common_sql::executor::PhysicalPlan;
use minitrace::prelude::*;
use parking_lot::Mutex;
use parking_lot::ReentrantMutex;
use tonic::Status;

use crate::api::rpc::exchange::exchange_params::ExchangeParams;
use crate::api::rpc::exchange::exchange_params::MergeExchangeParams;
use crate::api::rpc::exchange::exchange_params::ShuffleExchangeParams;
use crate::api::rpc::exchange::exchange_sink::ExchangeSink;
use crate::api::rpc::exchange::exchange_transform::ExchangeTransform;
use crate::api::rpc::exchange::statistics_receiver::StatisticsReceiver;
use crate::api::rpc::exchange::statistics_sender::StatisticsSender;
use crate::api::rpc::flight_client::FlightExchange;
use crate::api::rpc::flight_client::FlightReceiver;
use crate::api::rpc::flight_client::FlightSender;
use crate::api::rpc::Packet;
use crate::api::DataExchange;
use crate::api::DefaultExchangeInjector;
use crate::api::ExchangeInjector;
use crate::api::FlightClient;
use crate::api::FragmentPlanPacket;
use crate::api::InitNodesChannelPacket;
use crate::api::QueryFragmentsPlanPacket;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::PipelineBuilder;
use crate::schedulers::QueryFragmentActions;
use crate::schedulers::QueryFragmentsActions;
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

    pub fn get_queries_profile(&self) -> HashMap<String, Vec<Arc<Profile>>> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        let mut queries_profiles = HashMap::new();
        for (query_id, coordinator) in queries_coordinator.iter() {
            if let Some(executor) = coordinator
                .info
                .as_ref()
                .and_then(|x| x.query_executor.as_ref())
            {
                queries_profiles.insert(query_id.clone(), executor.get_inner().get_profiles());
            }
        }

        queries_profiles
    }

    // Create connections for cluster all nodes. We will push data through this connection.
    #[async_backtrace::framed]
    #[minitrace::trace]
    pub async fn init_nodes_channel(&self, packet: &InitNodesChannelPacket) -> Result<()> {
        let mut request_exchanges = HashMap::new();
        let mut targets_exchanges = HashMap::new();

        let target = &packet.executor.id;

        let create_rpc_client_with_current_rt = packet.create_rpc_clint_with_current_rt;

        for connection_info in &packet.fragment_connections_info {
            for fragment in &connection_info.fragments {
                let address = &connection_info.source.flight_address;
                let mut flight_client =
                    Self::create_client(address, create_rpc_client_with_current_rt).await?;

                targets_exchanges.insert(
                    (connection_info.source.id.clone(), *fragment),
                    flight_client
                        .do_get(&packet.query_id, target, *fragment)
                        .await?,
                );
            }
        }

        for connection_info in &packet.statistics_connections_info {
            let address = &connection_info.source.flight_address;
            let mut flight_client =
                Self::create_client(address, create_rpc_client_with_current_rt).await?;
            request_exchanges.insert(
                connection_info.source.id.clone(),
                flight_client
                    .request_server_exchange(&packet.query_id, target)
                    .await?,
            );
        }

        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.entry(packet.query_id.clone()) {
            Entry::Occupied(mut v) => {
                let query_coordinator = v.get_mut();
                query_coordinator.add_fragment_exchanges(targets_exchanges)?;
                query_coordinator.add_statistics_exchanges(request_exchanges)
            }
            Entry::Vacant(v) => {
                let query_coordinator = v.insert(QueryCoordinator::create());
                query_coordinator.add_fragment_exchanges(targets_exchanges)?;
                query_coordinator.add_statistics_exchanges(request_exchanges)
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
                .spawn(GLOBAL_TASK, task)
                .await
                .expect("create client future must be joined successfully")
        }
    }

    // Execute query in background
    #[minitrace::trace]
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
    #[minitrace::trace]
    pub fn init_query_fragments_plan(
        &self,
        ctx: &Arc<QueryContext>,
        packet: &QueryFragmentsPlanPacket,
    ) -> Result<()> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        // TODO: When the query is not executed for a long time after submission, we need to remove it
        match queries_coordinator.get_mut(&packet.query_id) {
            None => Err(ErrorCode::Internal(format!(
                "Query {} not found in cluster.",
                packet.query_id
            ))),
            Some(query_coordinator) => query_coordinator.prepare_pipeline(ctx, packet),
        }
    }

    #[minitrace::trace]
    pub fn handle_statistics_exchange(
        &self,
        id: String,
        target: String,
    ) -> Result<Receiver<Result<FlightData, Status>>> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.entry(id) {
            Entry::Occupied(mut v) => v.get_mut().add_statistics_exchange(target),
            Entry::Vacant(v) => v
                .insert(QueryCoordinator::create())
                .add_statistics_exchange(target),
        }
    }

    #[minitrace::trace]
    pub fn handle_exchange_fragment(
        &self,
        query: String,
        target: String,
        fragment: usize,
    ) -> Result<Receiver<Result<FlightData, Status>>> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.entry(query) {
            Entry::Occupied(mut v) => v.get_mut().add_fragment_exchange(target, fragment),
            Entry::Vacant(v) => v
                .insert(QueryCoordinator::create())
                .add_fragment_exchange(target, fragment),
        }
    }

    pub fn shutdown_query(&self, query_id: &str) {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        if let Some(query_coordinator) = queries_coordinator.get_mut(query_id) {
            query_coordinator.shutdown_query();
        }
    }

    #[minitrace::trace]
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
    #[minitrace::trace]
    pub async fn commit_actions(
        &self,
        ctx: Arc<QueryContext>,
        actions: QueryFragmentsActions,
    ) -> Result<PipelineBuildResult> {
        let settings = ctx.get_settings();
        let timeout = settings.get_flight_client_timeout()?;
        let root_actions = actions.get_root_actions()?;
        let conf = GlobalConfig::instance();

        // Initialize channels between cluster nodes
        actions
            .get_init_nodes_channel_packets()?
            .commit(conf.as_ref(), timeout)
            .await?;

        // Submit distributed tasks to all nodes.
        let (local_query_fragments_plan_packet, query_fragments_plan_packets) =
            actions.get_query_fragments_plan_packets()?;

        // Submit tasks to other nodes
        query_fragments_plan_packets
            .commit(conf.as_ref(), timeout)
            .await?;

        // Submit tasks to localhost
        self.init_query_fragments_plan(&ctx, &local_query_fragments_plan_packet)?;

        // Get local pipeline of local task
        let build_res = self.get_root_pipeline(ctx, root_actions)?;

        actions
            .get_execute_partial_query_packets()?
            .commit(conf.as_ref(), timeout)
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
                assert!(query_coordinator.fragment_exchanges.is_empty());
                let injector = DefaultExchangeInjector::create();
                let mut build_res =
                    query_coordinator.subscribe_fragment(&ctx, fragment_id, injector)?;

                let exchanges = std::mem::take(&mut query_coordinator.statistics_exchanges);
                let statistics_receiver = StatisticsReceiver::spawn_receiver(&ctx, exchanges)?;

                let statistics_receiver: Mutex<StatisticsReceiver> =
                    Mutex::new(statistics_receiver);

                let on_finished = build_res.main_pipeline.take_on_finished();
                build_res.main_pipeline.set_on_finished(move |may_error| {
                    let query_id = ctx.get_id();
                    let mut statistics_receiver = statistics_receiver.lock();

                    statistics_receiver.shutdown(may_error.is_err());
                    ctx.get_exchange_manager().on_finished_query(&query_id);
                    statistics_receiver.wait_shutdown()?;

                    on_finished(may_error)?;

                    match may_error {
                        Ok(_) => Ok(()),
                        Err(error_code) => Err(error_code.clone()),
                    }
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
    ) -> Result<Vec<(String, FlightReceiver)>> {
        let queries_coordinator_guard = self.queries_coordinator.lock();
        let queries_coordinator = unsafe { &mut *queries_coordinator_guard.deref().get() };

        match queries_coordinator.get_mut(&params.get_query_id()) {
            None => Err(ErrorCode::Internal("Query not exists.")),
            Some(coordinator) => coordinator.get_flight_receiver(params),
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
    current_executor: String,
    query_ctx: Arc<QueryContext>,
    query_executor: Option<Arc<PipelineCompleteExecutor>>,
}

static FLIGHT_SENDER: u8 = 1;
static FLIGHT_RECEIVER: u8 = 2;

struct QueryCoordinator {
    info: Option<QueryInfo>,
    fragments_coordinator: HashMap<usize, Box<FragmentCoordinator>>,

    statistics_exchanges: HashMap<String, FlightExchange>,
    fragment_exchanges: HashMap<(String, usize, u8), FlightExchange>,
}

impl QueryCoordinator {
    pub fn create() -> QueryCoordinator {
        QueryCoordinator {
            info: None,
            fragments_coordinator: HashMap::new(),
            fragment_exchanges: HashMap::new(),
            statistics_exchanges: HashMap::new(),
        }
    }

    pub fn add_statistics_exchange(
        &mut self,
        target: String,
    ) -> Result<Receiver<Result<FlightData, Status>>> {
        let (tx, rx) = async_channel::bounded(8);
        match self
            .statistics_exchanges
            .insert(target, FlightExchange::create_sender(tx))
        {
            None => Ok(rx),
            Some(_) => Err(ErrorCode::Internal(
                "statistics exchanges can only have one",
            )),
        }
    }

    pub fn add_statistics_exchanges(
        &mut self,
        exchanges: HashMap<String, FlightExchange>,
    ) -> Result<()> {
        for (source, exchange) in exchanges.into_iter() {
            if self.statistics_exchanges.insert(source, exchange).is_some() {
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
    ) -> Result<Receiver<Result<FlightData, Status>>> {
        let (tx, rx) = async_channel::bounded(8);
        self.fragment_exchanges.insert(
            (target, fragment, FLIGHT_SENDER),
            FlightExchange::create_sender(tx),
        );
        Ok(rx)
    }

    pub fn add_fragment_exchanges(
        &mut self,
        exchanges: HashMap<(String, usize), FlightExchange>,
    ) -> Result<()> {
        for ((source, fragment), exchange) in exchanges.into_iter() {
            self.fragment_exchanges
                .insert((source, fragment, FLIGHT_RECEIVER), exchange);
        }

        Ok(())
    }

    pub fn get_flight_senders(&mut self, params: &ExchangeParams) -> Result<Vec<FlightSender>> {
        match params {
            ExchangeParams::MergeExchange(params) => Ok(self
                .fragment_exchanges
                .extract_if(|(_, f, r), _| f == &params.fragment_id && *r == FLIGHT_SENDER)
                .map(|(_, v)| v.convert_to_sender())
                .collect::<Vec<_>>()),
            ExchangeParams::ShuffleExchange(params) => {
                let mut exchanges = Vec::with_capacity(params.destination_ids.len());

                for destination in &params.destination_ids {
                    exchanges.push(match destination == &params.executor_id {
                        true => Ok(FlightSender::create(async_channel::bounded(1).0)),
                        false => match self.fragment_exchanges.remove(&(
                            destination.clone(),
                            params.fragment_id,
                            FLIGHT_SENDER,
                        )) {
                            Some(exchange_channel) => Ok(exchange_channel.convert_to_sender()),
                            None => Err(ErrorCode::UnknownFragmentExchange(format!(
                                "Unknown fragment exchange channel, {}, {}",
                                destination, params.fragment_id
                            ))),
                        },
                    }?);
                }

                Ok(exchanges)
            }
        }
    }

    pub fn get_flight_receiver(
        &mut self,
        params: &ExchangeParams,
    ) -> Result<Vec<(String, FlightReceiver)>> {
        match params {
            ExchangeParams::MergeExchange(params) => Ok(self
                .fragment_exchanges
                .extract_if(|(_, f, r), _| f == &params.fragment_id && *r == FLIGHT_RECEIVER)
                .map(|((source, _, _), v)| (source.clone(), v.convert_to_receiver()))
                .collect::<Vec<_>>()),
            ExchangeParams::ShuffleExchange(params) => {
                let mut exchanges = Vec::with_capacity(params.destination_ids.len());

                for destination in &params.destination_ids {
                    exchanges.push((
                        destination.clone(),
                        match destination == &params.executor_id {
                            true => Ok(FlightReceiver::create(async_channel::bounded(1).1)),
                            false => match self.fragment_exchanges.remove(&(
                                destination.clone(),
                                params.fragment_id,
                                FLIGHT_RECEIVER,
                            )) {
                                Some(v) => Ok(v.convert_to_receiver()),
                                _ => Err(ErrorCode::UnknownFragmentExchange(format!(
                                    "Unknown fragment flight receiver, {}, {}",
                                    destination, params.fragment_id
                                ))),
                            },
                        }?,
                    ));
                }

                Ok(exchanges)
            }
        }
    }

    pub fn prepare_pipeline(
        &mut self,
        ctx: &Arc<QueryContext>,
        packet: &QueryFragmentsPlanPacket,
    ) -> Result<()> {
        self.info = Some(QueryInfo {
            query_ctx: ctx.clone(),
            query_id: packet.query_id.clone(),
            current_executor: packet.executor.clone(),
            query_executor: None,
        });

        for fragment in &packet.fragments {
            self.fragments_coordinator.insert(
                fragment.fragment_id.to_owned(),
                FragmentCoordinator::create(fragment),
            );
        }

        for fragment in &packet.fragments {
            let fragment_id = fragment.fragment_id;
            if let Some(coordinator) = self.fragments_coordinator.get_mut(&fragment_id) {
                coordinator.prepare_pipeline(ctx.clone())?;
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
        if let Some(query_info) = &self.info {
            if let Some(query_executor) = &query_info.query_executor {
                query_executor.finish(None);
            }
        }
    }

    pub fn on_finished(self) {
        // Do something when query finished.
    }

    pub fn execute_pipeline(&mut self) -> Result<()> {
        if self.fragments_coordinator.is_empty() {
            // Empty fragments if it is a request server, because the pipelines may have been linked.
            return Ok(());
        }

        let info = self.info.as_ref().expect("Query info is None");

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

        let executor_settings = ExecutorSettings::try_create(info.query_ctx.clone())?;
        let executor = PipelineCompleteExecutor::from_pipelines(pipelines, executor_settings)?;

        assert!(self.fragment_exchanges.is_empty());
        let info_mut = self.info.as_mut().expect("Query info is None");
        info_mut.query_executor = Some(executor.clone());

        let query_id = info_mut.query_id.clone();
        let query_ctx = info_mut.query_ctx.clone();
        let request_server_exchanges = std::mem::take(&mut self.statistics_exchanges);

        if request_server_exchanges.len() != 1 {
            return Err(ErrorCode::Internal(
                "Request server must less than 1 if is not request server.",
            ));
        }

        let ctx = query_ctx.clone();
        let (_, request_server_exchange) = request_server_exchanges.into_iter().next().unwrap();
        let mut statistics_sender =
            StatisticsSender::spawn_sender(&query_id, ctx, request_server_exchange);

        let span = if let Some(parent) = SpanContext::current_local_parent() {
            Span::root("Distributed-Executor", parent)
        } else {
            Span::noop()
        };

        Thread::named_spawn(Some(String::from("Distributed-Executor")), move || {
            let _g = span.set_local_parent();
            let res = executor.execute().err();
            let profiles = executor.get_inner().get_plans_profile();
            statistics_sender.shutdown(res, profiles);
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
    pub fn create(packet: &FragmentPlanPacket) -> Box<FragmentCoordinator> {
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
