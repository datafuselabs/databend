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

use std::pin::Pin;
use std::sync::Arc;

use databend_common_arrow::arrow_format::flight::data::Action;
use databend_common_arrow::arrow_format::flight::data::ActionType;
use databend_common_arrow::arrow_format::flight::data::Criteria;
use databend_common_arrow::arrow_format::flight::data::Empty;
use databend_common_arrow::arrow_format::flight::data::FlightData;
use databend_common_arrow::arrow_format::flight::data::FlightDescriptor;
use databend_common_arrow::arrow_format::flight::data::FlightInfo;
use databend_common_arrow::arrow_format::flight::data::HandshakeRequest;
use databend_common_arrow::arrow_format::flight::data::HandshakeResponse;
use databend_common_arrow::arrow_format::flight::data::PutResult;
use databend_common_arrow::arrow_format::flight::data::Result as FlightResult;
use databend_common_arrow::arrow_format::flight::data::SchemaResult;
use databend_common_arrow::arrow_format::flight::data::Ticket;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use fastrace::func_path;
use fastrace::prelude::*;
use futures_util::stream;
use tokio_stream::Stream;
use tonic::Request;
use tonic::Response as RawResponse;
use tonic::Status;
use tonic::Streaming;

use crate::servers::flight::flight_service::FlightOperation;
use crate::servers::flight::request_builder::RequestGetter;
use crate::servers::flight::v1::actions::flight_actions;
use crate::servers::flight::v1::actions::FlightActions;
use crate::servers::flight::v1::exchange::DataExchangeManager;

pub type FlightStream<T> = Pin<Box<dyn Stream<Item = Result<T, Status>> + Send + Sync + 'static>>;

pub struct DatabendQueryFlightService {
    actions: FlightActions,
}

impl DatabendQueryFlightService {
    pub fn create() -> Self {
        DatabendQueryFlightService {
            actions: flight_actions(),
        }
    }
}

type Response<T> = Result<RawResponse<T>, Status>;
type StreamReq<T> = Request<Streaming<T>>;
#[async_trait::async_trait]
impl FlightOperation for DatabendQueryFlightService {
    type HandshakeStream = FlightStream<Arc<HandshakeResponse>>;

    #[async_backtrace::framed]
    async fn handshake(&self, _: StreamReq<HandshakeRequest>) -> Response<Self::HandshakeStream> {
        Err(Status::unimplemented(
            "DatabendQuery does not implement handshake.",
        ))
    }

    type ListFlightsStream = FlightStream<Arc<FlightInfo>>;

    #[async_backtrace::framed]
    async fn list_flights(&self, _: Request<Criteria>) -> Response<Self::ListFlightsStream> {
        Err(Status::unimplemented(
            "DatabendQuery does not implement list_flights.",
        ))
    }

    #[async_backtrace::framed]
    async fn get_flight_info(&self, _: Request<FlightDescriptor>) -> Response<Arc<FlightInfo>> {
        Err(Status::unimplemented(
            "DatabendQuery does not implement get_flight_info.",
        ))
    }

    #[async_backtrace::framed]
    async fn get_schema(&self, _: Request<FlightDescriptor>) -> Response<Arc<SchemaResult>> {
        Err(Status::unimplemented(
            "DatabendQuery does not implement get_schema.",
        ))
    }
    type DoExchangeStream = FlightStream<Arc<FlightData>>;

    #[async_backtrace::framed]
    async fn do_exchange(
        &self,
        request: StreamReq<FlightData>,
    ) -> Response<Self::DoExchangeStream> {
        let root = databend_common_tracing::start_trace_for_remote_request(func_path!(), &request);
        let _guard = root.set_local_parent();
        match request.get_metadata("x-type")?.as_str() {
            "request_server_exchange" => {
                let target = request.get_metadata("x-target")?;
                let query_id = request.get_metadata("x-query-id")?;
                let continue_from = request
                    .get_metadata("x-continue-from")?
                    .parse::<usize>()
                    .unwrap();
                // if x-enable-retry is not 0, set enable_retry to a bool value true, otherwise false
                let enable_retry: u64 = request
                    .get_metadata("x-enable-retry")?
                    .parse::<u64>()
                    .unwrap_or_default();
                let client_stream = request.into_inner();
                Ok(RawResponse::new(Box::pin(
                    DataExchangeManager::instance().handle_statistics_exchange(
                        query_id,
                        target,
                        continue_from,
                        client_stream,
                        enable_retry != 0,
                    )?,
                )))
            }
            "exchange_fragment" => {
                let target = request.get_metadata("x-target")?;
                let query_id = request.get_metadata("x-query-id")?;
                let fragment = request
                    .get_metadata("x-fragment-id")?
                    .parse::<usize>()
                    .unwrap();
                let continue_from = request
                    .get_metadata("x-continue-from")?
                    .parse::<usize>()
                    .unwrap();
                let enable_retry: u64 = request
                    .get_metadata("x-enable-retry")?
                    .parse::<u64>()
                    .unwrap_or_default();
                let client_stream = request.into_inner();
                Ok(RawResponse::new(Box::pin(
                    DataExchangeManager::instance().handle_exchange_fragment(
                        query_id,
                        target,
                        fragment,
                        continue_from,
                        client_stream,
                        enable_retry != 0,
                    )?,
                )))
            }
            "health" => Ok(RawResponse::new(build_health_response())),
            exchange_type => Err(Status::unimplemented(format!(
                "Unimplemented exchange type: {:?}",
                exchange_type
            ))),
        }
    }

    type DoGetStream = FlightStream<Arc<FlightData>>;

    #[async_backtrace::framed]
    async fn do_get(&self, _request: Request<Ticket>) -> Response<Self::DoGetStream> {
        Err(Status::unimplemented("unimplemented do_exchange"))
    }
    type DoPutStream = FlightStream<Arc<PutResult>>;

    #[async_backtrace::framed]
    async fn do_put(&self, _req: StreamReq<FlightData>) -> Response<Self::DoPutStream> {
        Err(Status::unimplemented("unimplemented do_put"))
    }

    type DoActionStream = FlightStream<Arc<FlightResult>>;

    #[async_backtrace::framed]
    async fn do_action(&self, request: Request<Action>) -> Response<Self::DoActionStream> {
        let root = databend_common_tracing::start_trace_for_remote_request(func_path!(), &request);

        let secret = request.get_metadata("secret")?;

        let config = GlobalConfig::instance();
        if secret != config.query.node_secret {
            return Err(Into::into(ErrorCode::AuthenticateFailure(format!(
                "authenticate failure while flight, node: {}",
                config.query.node_id,
            ))));
        }

        let action = request.into_inner();
        match self
            .actions
            .do_action(&action.r#type, &action.body)
            .in_span(root)
            .await
        {
            Err(cause) => Err(cause.into()),
            Ok(body) => Ok(RawResponse::new(Box::pin(tokio_stream::once(Ok(Arc::new(
                FlightResult { body },
            ))))
                as FlightStream<Arc<FlightResult>>)),
        }
    }

    type ListActionsStream = FlightStream<Arc<ActionType>>;

    #[async_backtrace::framed]
    async fn list_actions(&self, _: Request<Empty>) -> Response<Self::ListActionsStream> {
        Ok(RawResponse::new(Box::pin(stream::empty())))
    }
}

fn build_health_response() -> FlightStream<Arc<FlightData>> {
    Box::pin(stream::iter(vec![Ok(Arc::new(FlightData {
        flight_descriptor: None,
        data_header: vec![],
        data_body: Vec::from("ok"),
        app_metadata: vec![0x03],
    }))]))
}
