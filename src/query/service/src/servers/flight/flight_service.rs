// Copyright 2016-2019 The Apache Software Foundation
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

use std::convert::Infallible;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use databend_common_arrow::arrow_format::flight;
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
use databend_common_arrow::arrow_format::flight::data::SchemaResult;
use databend_common_arrow::arrow_format::flight::data::Ticket;
use tonic::async_trait;
use tonic::body::empty_body;
use tonic::body::BoxBody;
use tonic::codegen::http::request::Request as HTTPRequest;
use tonic::codegen::http::response::Response as HTTPResponse;
use tonic::codegen::Body;
use tonic::codegen::BoxFuture;
use tonic::codegen::Service;
use tonic::codegen::StdError;
use tonic::server::NamedService;
use tonic::server::ServerStreamingService;
use tonic::server::StreamingService;
use tonic::server::UnaryService;
use tonic::Status;

use crate::servers::flight::codec::MessageCodec;

/// This trait is derived from `FlightService`.
/// It has been modified the `do_get` method signatures to use `Arc<FlightData>` as `DoGetStream` type
#[async_trait]
pub trait FlightOperation: Send + Sync + 'static {
    type HandshakeStream: tokio_stream::Stream<Item = Result<Arc<HandshakeResponse>, Status>>
        + Send
        + 'static;
    async fn handshake(
        &self,
        request: tonic::Request<tonic::Streaming<HandshakeRequest>>,
    ) -> Result<tonic::Response<Self::HandshakeStream>, Status>;

    type ListFlightsStream: tokio_stream::Stream<Item = Result<Arc<FlightInfo>, Status>>
        + Send
        + 'static;
    async fn list_flights(
        &self,
        request: tonic::Request<Criteria>,
    ) -> Result<tonic::Response<Self::ListFlightsStream>, Status>;

    async fn get_flight_info(
        &self,
        request: tonic::Request<FlightDescriptor>,
    ) -> Result<tonic::Response<Arc<FlightInfo>>, Status>;

    async fn get_schema(
        &self,
        request: tonic::Request<FlightDescriptor>,
    ) -> Result<tonic::Response<Arc<SchemaResult>>, Status>;
    type DoExchangeStream: tokio_stream::Stream<Item = Result<Arc<FlightData>, Status>>
        + Send
        + 'static;
    async fn do_exchange(
        &self,
        request: tonic::Request<tonic::Streaming<FlightData>>,
    ) -> Result<tonic::Response<Self::DoExchangeStream>, Status>;

    type DoGetStream: tokio_stream::Stream<Item = Result<Arc<FlightData>, Status>> + Send + 'static;
    async fn do_get(
        &self,
        request: tonic::Request<Ticket>,
    ) -> Result<tonic::Response<Self::DoGetStream>, Status>;
    type DoPutStream: tokio_stream::Stream<Item = Result<Arc<PutResult>, Status>> + Send + 'static;
    async fn do_put(
        &self,
        request: tonic::Request<tonic::Streaming<FlightData>>,
    ) -> Result<tonic::Response<Self::DoPutStream>, Status>;
    type DoActionStream: tokio_stream::Stream<Item = Result<Arc<flight::data::Result>, Status>>
        + Send
        + 'static;
    async fn do_action(
        &self,
        request: tonic::Request<Action>,
    ) -> Result<tonic::Response<Self::DoActionStream>, Status>;
    type ListActionsStream: tokio_stream::Stream<Item = Result<Arc<ActionType>, Status>>
        + Send
        + 'static;
    async fn list_actions(
        &self,
        request: tonic::Request<Empty>,
    ) -> Result<tonic::Response<Self::ListActionsStream>, Status>;
}

struct _Inner<T>(Arc<T>);
impl<T: FlightOperation> Clone for _Inner<T> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

pub struct FlightServiceServer<T: FlightOperation> {
    inner: _Inner<T>,
    max_decoding_message_size: Option<usize>,
    max_encoding_message_size: Option<usize>,
}

impl<T: FlightOperation> FlightServiceServer<T> {
    pub(crate) fn new(
        service: T,
        max_decoding_message_size: usize,
        max_encoding_message_size: usize,
    ) -> Self {
        Self {
            inner: _Inner(Arc::new(service)),
            max_decoding_message_size: Some(max_decoding_message_size),
            max_encoding_message_size: Some(max_encoding_message_size),
        }
    }
}

impl<T, B> Service<HTTPRequest<B>> for FlightServiceServer<T>
where
    T: FlightOperation,
    B: Body + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = HTTPResponse<BoxBody>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<std::result::Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: HTTPRequest<B>) -> Self::Future {
        match req.uri().path() {
            "/arrow.flight.protocol.FlightService/Handshake" => {
                struct HandshakeSvc<T: FlightOperation>(pub Arc<T>);
                impl<T: FlightOperation> StreamingService<HandshakeRequest> for HandshakeSvc<T> {
                    type Response = Arc<HandshakeResponse>;
                    type ResponseStream = T::HandshakeStream;
                    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                    fn call(
                        &mut self,
                        request: tonic::Request<tonic::Streaming<HandshakeRequest>>,
                    ) -> Self::Future {
                        let inner = self.0.clone();
                        let fut = async move { (*inner).handshake(request).await };
                        Box::pin(fut)
                    }
                }
                let max_decoding_message_size = self.max_decoding_message_size;
                let max_encoding_message_size = self.max_encoding_message_size;
                let inner = self.inner.clone();
                let fut = async move {
                    let inner = inner.0;
                    let method = HandshakeSvc(inner);
                    let codec = MessageCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec).apply_max_message_size_config(
                        max_decoding_message_size,
                        max_encoding_message_size,
                    );
                    let res = grpc.streaming(method, req).await;
                    Ok(res)
                };
                Box::pin(fut)
            }
            "/arrow.flight.protocol.FlightService/ListFlights" => {
                struct ListFlightsSvc<T: FlightOperation>(pub Arc<T>);
                impl<T: FlightOperation> ServerStreamingService<Criteria> for ListFlightsSvc<T> {
                    type Response = Arc<FlightInfo>;
                    type ResponseStream = T::ListFlightsStream;
                    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                    fn call(&mut self, request: tonic::Request<Criteria>) -> Self::Future {
                        let inner = self.0.clone();
                        let fut = async move { (*inner).list_flights(request).await };
                        Box::pin(fut)
                    }
                }
                let max_decoding_message_size = self.max_decoding_message_size;
                let max_encoding_message_size = self.max_encoding_message_size;
                let inner = self.inner.clone();
                let fut = async move {
                    let inner = inner.0;
                    let method = ListFlightsSvc(inner);
                    let codec = MessageCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec).apply_max_message_size_config(
                        max_decoding_message_size,
                        max_encoding_message_size,
                    );
                    let res = grpc.server_streaming(method, req).await;
                    Ok(res)
                };
                Box::pin(fut)
            }
            "/arrow.flight.protocol.FlightService/GetFlightInfo" => {
                struct GetFlightInfoSvc<T: FlightOperation>(pub Arc<T>);
                impl<T: FlightOperation> UnaryService<FlightDescriptor> for GetFlightInfoSvc<T> {
                    type Response = Arc<FlightInfo>;
                    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                    fn call(&mut self, request: tonic::Request<FlightDescriptor>) -> Self::Future {
                        let inner = self.0.clone();
                        let fut = async move { (*inner).get_flight_info(request).await };
                        Box::pin(fut)
                    }
                }
                let max_decoding_message_size = self.max_decoding_message_size;
                let max_encoding_message_size = self.max_encoding_message_size;
                let inner = self.inner.clone();
                let fut = async move {
                    let inner = inner.0;
                    let method = GetFlightInfoSvc(inner);
                    let codec = MessageCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec).apply_max_message_size_config(
                        max_decoding_message_size,
                        max_encoding_message_size,
                    );
                    let res = grpc.unary(method, req).await;
                    Ok(res)
                };
                Box::pin(fut)
            }
            "/arrow.flight.protocol.FlightService/GetSchema" => {
                struct GetSchemaSvc<T: FlightOperation>(pub Arc<T>);
                impl<T: FlightOperation> UnaryService<FlightDescriptor> for GetSchemaSvc<T> {
                    type Response = Arc<SchemaResult>;
                    type Future = BoxFuture<tonic::Response<Self::Response>, tonic::Status>;
                    fn call(&mut self, request: tonic::Request<FlightDescriptor>) -> Self::Future {
                        let inner = self.0.clone();
                        let fut = async move { (*inner).get_schema(request).await };
                        Box::pin(fut)
                    }
                }
                let max_decoding_message_size = self.max_decoding_message_size;
                let max_encoding_message_size = self.max_encoding_message_size;
                let inner = self.inner.clone();
                let fut = async move {
                    let inner = inner.0;
                    let method = GetSchemaSvc(inner);
                    let codec = MessageCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec).apply_max_message_size_config(
                        max_decoding_message_size,
                        max_encoding_message_size,
                    );
                    let res = grpc.unary(method, req).await;
                    Ok(res)
                };
                Box::pin(fut)
            }
            "/arrow.flight.protocol.FlightService/DoGet" => {
                struct DoGetSvc<T: FlightOperation>(pub Arc<T>);
                impl<T: FlightOperation> ServerStreamingService<Ticket> for DoGetSvc<T> {
                    type Response = Arc<FlightData>;
                    type ResponseStream = T::DoGetStream;
                    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, Status>;
                    fn call(&mut self, request: tonic::Request<Ticket>) -> Self::Future {
                        let inner = Arc::clone(&self.0);
                        let fut =
                            async move { <T as FlightOperation>::do_get(&inner, request).await };
                        Box::pin(fut)
                    }
                }
                let max_decoding_message_size = self.max_decoding_message_size;
                let max_encoding_message_size = self.max_encoding_message_size;
                let inner = self.inner.0.clone();
                let fut = async move {
                    let method = DoGetSvc(inner);
                    let codec = MessageCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec).apply_max_message_size_config(
                        max_decoding_message_size,
                        max_encoding_message_size,
                    );
                    let res = grpc.server_streaming(method, req).await;
                    Ok(res)
                };
                Box::pin(fut)
            }
            "/arrow.flight.protocol.FlightService/DoPut" => {
                struct DoPutSvc<T: FlightOperation>(pub Arc<T>);
                impl<T: FlightOperation> StreamingService<FlightData> for DoPutSvc<T> {
                    type Response = Arc<PutResult>;
                    type ResponseStream = T::DoPutStream;
                    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                    fn call(
                        &mut self,
                        request: tonic::Request<tonic::Streaming<FlightData>>,
                    ) -> Self::Future {
                        let inner = self.0.clone();
                        let fut = async move { (*inner).do_put(request).await };
                        Box::pin(fut)
                    }
                }
                let max_decoding_message_size = self.max_decoding_message_size;
                let max_encoding_message_size = self.max_encoding_message_size;
                let inner = self.inner.clone();
                let fut = async move {
                    let inner = inner.0;
                    let method = DoPutSvc(inner);
                    let codec = MessageCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec).apply_max_message_size_config(
                        max_decoding_message_size,
                        max_encoding_message_size,
                    );
                    let res = grpc.streaming(method, req).await;
                    Ok(res)
                };
                Box::pin(fut)
            }
            "/arrow.flight.protocol.FlightService/DoExchange" => {
                struct DoExchangeSvc<T: FlightOperation>(pub Arc<T>);
                impl<T: FlightOperation> StreamingService<FlightData> for DoExchangeSvc<T> {
                    type Response = Arc<FlightData>;
                    type ResponseStream = T::DoExchangeStream;
                    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                    fn call(
                        &mut self,
                        request: tonic::Request<tonic::Streaming<FlightData>>,
                    ) -> Self::Future {
                        let inner = self.0.clone();
                        let fut = async move { (*inner).do_exchange(request).await };
                        Box::pin(fut)
                    }
                }
                let max_decoding_message_size = self.max_decoding_message_size;
                let max_encoding_message_size = self.max_encoding_message_size;
                let inner = self.inner.clone();
                let fut = async move {
                    let inner = inner.0;
                    let method = DoExchangeSvc(inner);
                    let codec = MessageCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec).apply_max_message_size_config(
                        max_decoding_message_size,
                        max_encoding_message_size,
                    );
                    let res = grpc.streaming(method, req).await;
                    Ok(res)
                };
                Box::pin(fut)
            }
            "/arrow.flight.protocol.FlightService/DoAction" => {
                struct DoActionSvc<T: FlightOperation>(pub Arc<T>);
                impl<T: FlightOperation> ServerStreamingService<Action> for DoActionSvc<T> {
                    type Response = Arc<flight::data::Result>;

                    type ResponseStream = T::DoActionStream;

                    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, Status>;

                    fn call(&mut self, request: tonic::Request<Action>) -> Self::Future {
                        let inner = self.0.clone();
                        let fut = async move { (*inner).do_action(request).await };
                        Box::pin(fut)
                    }
                }
                let max_decoding_message_size = self.max_decoding_message_size;
                let max_encoding_message_size = self.max_encoding_message_size;
                let inner = self.inner.0.clone();
                let fut = async move {
                    let method = DoActionSvc(inner);
                    let codec = MessageCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec).apply_max_message_size_config(
                        max_decoding_message_size,
                        max_encoding_message_size,
                    );
                    let res = grpc.server_streaming(method, req).await;
                    Ok(res)
                };
                Box::pin(fut)
            }
            "/arrow.flight.protocol.FlightService/ListActions" => {
                struct ListActionsSvc<T: FlightOperation>(pub Arc<T>);
                impl<T: FlightOperation> ServerStreamingService<Empty> for ListActionsSvc<T> {
                    type Response = Arc<ActionType>;
                    type ResponseStream = T::ListActionsStream;
                    type Future = BoxFuture<tonic::Response<Self::ResponseStream>, tonic::Status>;
                    fn call(&mut self, request: tonic::Request<Empty>) -> Self::Future {
                        let inner = self.0.clone();
                        let fut = async move { (*inner).list_actions(request).await };
                        Box::pin(fut)
                    }
                }
                let max_decoding_message_size = self.max_decoding_message_size;
                let max_encoding_message_size = self.max_encoding_message_size;
                let inner = self.inner.clone();
                let fut = async move {
                    let inner = inner.0;
                    let method = ListActionsSvc(inner);
                    let codec = MessageCodec::default();
                    let mut grpc = tonic::server::Grpc::new(codec).apply_max_message_size_config(
                        max_decoding_message_size,
                        max_encoding_message_size,
                    );
                    let res = grpc.server_streaming(method, req).await;
                    Ok(res)
                };
                Box::pin(fut)
            }
            _ => Box::pin(async move {
                Ok(HTTPResponse::builder()
                    .status(200)
                    .header("grpc-status", "12")
                    .header("content-type", "application/grpc")
                    .body(empty_body())
                    .unwrap())
            }),
        }
    }
}

impl<T: FlightOperation> Clone for FlightServiceServer<T> {
    fn clone(&self) -> Self {
        let inner = self.inner.clone();
        Self {
            inner,
            max_decoding_message_size: self.max_decoding_message_size,
            max_encoding_message_size: self.max_encoding_message_size,
        }
    }
}

// The client side still uses FlightClient to find the service by this name,
// so we keep the service name unchanged.
impl<T: FlightOperation> NamedService for FlightServiceServer<T> {
    const NAME: &'static str = "arrow.flight.protocol.FlightService";
}
