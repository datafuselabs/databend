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

use std::collections::VecDeque;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use async_channel::Receiver;
use async_channel::Sender;
use databend_common_arrow::arrow_format::flight::data::Action;
use databend_common_arrow::arrow_format::flight::data::FlightData;
use databend_common_arrow::arrow_format::flight::data::Ticket;
use databend_common_arrow::arrow_format::flight::service::flight_service_client::FlightServiceClient;
use databend_common_base::base::tokio::time::Duration;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use fastrace::full_name;
use fastrace::future::FutureExt;
use fastrace::Span;
use futures::Stream;
use futures::StreamExt;
use futures_util::future::Either;
use log::info;
use parking_lot::Mutex;
use serde::Deserialize;
use serde::Serialize;
use tokio::time::sleep;
use tonic::metadata::AsciiMetadataKey;
use tonic::metadata::AsciiMetadataValue;
use tonic::transport::channel::Channel;
use tonic::Request;
use tonic::Status;
use tonic::Streaming;

use crate::pipelines::executor::WatchNotify;
use crate::servers::flight::flight_client::impls::FlightRxInner;
use crate::servers::flight::request_builder::RequestBuilder;
use crate::servers::flight::v1::exchange::DataExchangeManager;
use crate::servers::flight::v1::packets::DataPacket;

pub struct FlightClient {
    inner: FlightServiceClient<Channel>,
}

// TODO: Integration testing required
impl FlightClient {
    pub fn new(mut inner: FlightServiceClient<Channel>) -> FlightClient {
        inner = inner.max_decoding_message_size(usize::MAX);
        inner = inner.max_encoding_message_size(usize::MAX);

        FlightClient { inner }
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn do_action<T, Res>(
        &mut self,
        path: &str,
        secret: String,
        message: T,
        timeout: u64,
    ) -> Result<Res>
    where
        T: Serialize,
        Res: for<'a> Deserialize<'a>,
    {
        let mut body = Vec::with_capacity(512);
        let mut serializer = serde_json::Serializer::new(&mut body);
        let serializer = serde_stacker::Serializer::new(&mut serializer);
        message.serialize(serializer).map_err(|cause| {
            ErrorCode::BadArguments(format!(
                "Request payload serialize error while in {:?}, cause: {}",
                path, cause
            ))
        })?;

        drop(message);
        let mut request =
            databend_common_tracing::inject_span_to_tonic_request(Request::new(Action {
                body,
                r#type: path.to_string(),
            }));

        request.set_timeout(Duration::from_secs(timeout));
        request.metadata_mut().insert(
            AsciiMetadataKey::from_str("secret").unwrap(),
            AsciiMetadataValue::from_str(&secret).unwrap(),
        );

        let response = self.inner.do_action(request).await?;

        match response.into_inner().message().await? {
            Some(response) => {
                let mut deserializer = serde_json::Deserializer::from_slice(&response.body);
                deserializer.disable_recursion_limit();
                let deserializer = serde_stacker::Deserializer::new(&mut deserializer);

                Res::deserialize(deserializer).map_err(|cause| {
                    ErrorCode::BadBytes(format!(
                        "Response payload deserialize error while in {:?}, cause: {}",
                        path, cause
                    ))
                })
            }
            None => Err(ErrorCode::EmptyDataFromServer(format!(
                "Can not receive data from flight server, action: {:?}",
                path
            ))),
        }
    }

    #[async_backtrace::framed]
    pub async fn request_server_exchange(
        &mut self,
        query_id: &str,
        target: &str,
        source_address: &str,
        retry_times: usize,
        retry_interval: usize,
    ) -> Result<FlightExchange> {
        let req = RequestBuilder::create(Ticket::default())
            .with_metadata("x-type", "request_server_exchange")?
            .with_metadata("x-target", target)?
            .with_metadata("x-query-id", query_id)?
            .with_metadata("x-continue-from", "0")?
            .build();
        let streaming = self.get_streaming(req).await?;

        let (notify, rx) = Self::streaming_receiver(streaming);
        Ok(FlightExchange::create_receiver(
            notify,
            rx,
            Some(ConnectionInfo {
                query_id: query_id.to_string(),
                target: target.to_string(),
                fragment: None,
                source_address: source_address.to_string(),
                retry_times,
                retry_interval: Duration::from_secs(retry_interval as u64),
            }),
        ))
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn do_get(
        &mut self,
        query_id: &str,
        target: &str,
        fragment: usize,
        source_address: &str,
        retry_times: usize,
        retry_interval: usize,
    ) -> Result<FlightExchange> {
        let request = RequestBuilder::create(Ticket::default())
            .with_metadata("x-type", "exchange_fragment")?
            .with_metadata("x-target", target)?
            .with_metadata("x-query-id", query_id)?
            .with_metadata("x-fragment-id", &fragment.to_string())?
            .with_metadata("x-continue-from", "0")?
            .build();
        let request = databend_common_tracing::inject_span_to_tonic_request(request);

        let streaming = self.get_streaming(request).await?;

        let (notify, rx) = Self::streaming_receiver(streaming);
        Ok(FlightExchange::create_receiver(
            notify,
            rx,
            Some(ConnectionInfo {
                query_id: query_id.to_string(),
                target: target.to_string(),
                fragment: Some(fragment),
                source_address: source_address.to_string(),
                retry_times,
                retry_interval: Duration::from_secs(retry_interval as u64),
            }),
        ))
    }

    fn streaming_receiver(
        mut streaming: Streaming<FlightData>,
    ) -> (Arc<WatchNotify>, Receiver<Result<FlightData>>) {
        let (tx, rx) = async_channel::bounded(1);
        let notify = Arc::new(WatchNotify::new());
        let fut = {
            let notify = notify.clone();
            async move {
                let mut notified = Box::pin(notify.notified());
                let mut streaming_next = streaming.next();

                loop {
                    match futures::future::select(notified, streaming_next).await {
                        Either::Left((_, _)) | Either::Right((None, _)) => {
                            break;
                        }
                        Either::Right((Some(message), next_notified)) => {
                            notified = next_notified;
                            streaming_next = streaming.next();

                            match message {
                                Ok(message) => {
                                    if tx.send(Ok(message)).await.is_err() {
                                        break;
                                    }
                                }
                                Err(status) => {
                                    let _ = tx.send(Err(ErrorCode::from(status))).await;
                                    break;
                                }
                            }
                        }
                    }
                }

                drop(streaming);
                tx.close();
            }
        }
        .in_span(Span::enter_with_local_parent(full_name!()));

        databend_common_base::runtime::spawn(fut);

        (notify, rx)
    }

    #[async_backtrace::framed]
    async fn get_streaming(&mut self, request: Request<Ticket>) -> Result<Streaming<FlightData>> {
        match self.inner.do_get(request).await {
            Ok(res) => Ok(res.into_inner()),
            Err(status) => Err(ErrorCode::from(status).add_message_back("(while in query flight)")),
        }
    }

    #[async_backtrace::framed]
    async fn reconnect(&mut self, info: &ConnectionInfo, seq: usize) -> Result<FlightRxInner> {
        let request = match info.fragment {
            Some(fragment_id) => RequestBuilder::create(Ticket::default())
                .with_metadata("x-type", "exchange_fragment")?
                .with_metadata("x-target", &info.target)?
                .with_metadata("x-query-id", &info.query_id)?
                .with_metadata("x-fragment-id", &fragment_id.to_string())?
                .with_metadata("x-continue-from", &seq.to_string())?
                .build(),
            None => RequestBuilder::create(Ticket::default())
                .with_metadata("x-type", "request_server_exchange")?
                .with_metadata("x-target", &info.target)?
                .with_metadata("x-query-id", &info.query_id)?
                .with_metadata("x-continue-from", &seq.to_string())?
                .build(),
        };
        let request = databend_common_tracing::inject_span_to_tonic_request(request);

        let streaming = self.get_streaming(request).await?;

        let (network_notify, recv) = Self::streaming_receiver(streaming);
        Ok(FlightRxInner::create(network_notify, recv))
    }
}

#[derive(Clone)]
pub struct ConnectionInfo {
    pub query_id: String,
    pub target: String,
    pub fragment: Option<usize>,
    pub source_address: String,
    pub retry_times: usize,
    pub retry_interval: Duration,
}

pub struct RetryableFlightReceiver {
    seq: Arc<AtomicUsize>,
    info: Option<ConnectionInfo>,
    inner: Arc<AtomicPtr<FlightRxInner>>,
}

impl Drop for RetryableFlightReceiver {
    fn drop(&mut self) {
        self.close();
    }
}

impl RetryableFlightReceiver {
    pub fn dummy() -> RetryableFlightReceiver {
        // dummy
        RetryableFlightReceiver {
            info: None,
            seq: Arc::new(AtomicUsize::new(0)),
            inner: Arc::new(Default::default()),
        }
    }

    #[async_backtrace::framed]
    pub async fn recv(&self) -> Result<Option<DataPacket>> {
        // Non thread safe, we only use atomic to implement mutable.
        loop {
            let inner = unsafe { &*self.inner.load(Ordering::SeqCst) };

            return match inner.recv().await {
                Ok(message) => {
                    self.seq.fetch_add(1, Ordering::SeqCst);
                    Ok(message)
                }
                Err(cause) => {
                    info!("Error while receiving data from flight : {:?}", cause);
                    if cause.code() == ErrorCode::CANNOT_CONNECT_NODE {
                        // only retry when error is network problem
                        let Err(cause) = self.retry().await else {
                            info!("Retry flight connection successfully!");
                            continue;
                        };

                        info!("Retry flight connection failure, cause: {:?}", cause);
                    }

                    Err(cause)
                }
            };
        }
    }

    #[async_backtrace::framed]
    async fn retry(&self) -> Result<()> {
        if let Some(connection_info) = &self.info {
            let mut flight_client =
                DataExchangeManager::create_client(&connection_info.source_address, true).await?;

            for attempts in 0..connection_info.retry_times {
                let Ok(recv) = flight_client
                    .reconnect(connection_info, self.seq.load(Ordering::Acquire))
                    .await
                else {
                    info!("Reconnect attempt {} failed", attempts);
                    sleep(connection_info.retry_interval).await;
                    continue;
                };

                let ptr = self
                    .inner
                    .swap(Box::into_raw(Box::new(recv)), Ordering::SeqCst);

                unsafe {
                    // We cannot determine the number of strong ref. so close it.
                    let broken_connection = Box::from_raw(ptr);
                    broken_connection.close();
                }

                return Ok(());
            }

            return Err(ErrorCode::Timeout("Exceed max retries time"));
        }

        Ok(())
    }

    pub fn close(&self) {
        unsafe {
            let inner = self.inner.load(Ordering::SeqCst);

            if !inner.is_null() {
                (*inner).close();
            }
        }
    }
}

pub struct FlightSender {
    tx: Sender<Result<FlightData, Status>>,
}

impl FlightSender {
    pub fn create(tx: Sender<Result<FlightData, Status>>) -> FlightSender {
        FlightSender { tx }
    }

    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    #[async_backtrace::framed]
    pub async fn send(&self, data: DataPacket) -> Result<()> {
        if let Err(_cause) = self.tx.send(Ok(FlightData::try_from(data)?)).await {
            return Err(ErrorCode::AbortedQuery(
                "Aborted query, because the remote flight channel is closed.",
            ));
        }

        Ok(())
    }

    pub fn close(&self) {
        self.tx.close();
    }
}

pub struct SenderPayload {
    pub state: Arc<Mutex<FlightDataAckState>>,
    pub sender: Option<Sender<Result<FlightData, Status>>>,
}

pub struct ReceiverPayload {
    seq: Arc<AtomicUsize>,
    info: Option<ConnectionInfo>,
    inner: Arc<AtomicPtr<impls::FlightRxInner>>,
}

pub enum FlightExchange {
    Dummy,
    Sender(SenderPayload),
    Receiver(ReceiverPayload),

    MovedSender(SenderPayload),
    MovedReceiver(ReceiverPayload),
}

impl FlightExchange {
    pub fn create_sender(
        state: Arc<Mutex<FlightDataAckState>>,
        sender: Sender<Result<FlightData, Status>>,
    ) -> FlightExchange {
        FlightExchange::Sender(SenderPayload {
            state,
            sender: Some(sender),
        })
    }

    pub fn create_receiver(
        notify: Arc<WatchNotify>,
        receiver: Receiver<Result<FlightData>>,
        connection_info: Option<ConnectionInfo>,
    ) -> FlightExchange {
        FlightExchange::Receiver(ReceiverPayload {
            seq: Arc::new(AtomicUsize::new(0)),
            info: connection_info,
            inner: Arc::new(AtomicPtr::new(Box::into_raw(Box::new(
                FlightRxInner::create(notify, receiver),
            )))),
        })
    }

    pub fn take_as_sender(&mut self) -> FlightSender {
        let mut flight_sender = FlightExchange::Dummy;
        std::mem::swap(self, &mut flight_sender);

        if let FlightExchange::Sender(mut v) = flight_sender {
            let flight_sender = FlightSender::create(v.sender.take().unwrap());
            *self = FlightExchange::MovedSender(v);
            return flight_sender;
        }

        unreachable!("take as sender miss match exchange type")
    }

    pub fn take_as_receiver(&mut self) -> RetryableFlightReceiver {
        let mut flight_receiver = FlightExchange::Dummy;
        std::mem::swap(self, &mut flight_receiver);

        if let FlightExchange::Receiver(v) = flight_receiver {
            let flight_receiver = RetryableFlightReceiver {
                seq: v.seq.clone(),
                info: v.info.clone(),
                inner: v.inner.clone(),
            };

            *self = FlightExchange::MovedReceiver(v);

            return flight_receiver;
        }

        unreachable!("take as receiver miss match exchange type")
    }
}

mod impls {
    use std::sync::Arc;

    use async_channel::Receiver;
    use databend_common_arrow::arrow_format::flight::data::FlightData;
    use databend_common_base::base::WatchNotify;
    use databend_common_exception::Result;

    use crate::servers::flight::v1::packets::DataPacket;

    pub struct FlightRxInner {
        notify: Arc<WatchNotify>,
        rx: Receiver<Result<FlightData>>,
    }

    impl FlightRxInner {
        pub fn create(notify: Arc<WatchNotify>, rx: Receiver<Result<FlightData>>) -> FlightRxInner {
            FlightRxInner { rx, notify }
        }

        #[async_backtrace::framed]
        pub async fn recv(&self) -> Result<Option<DataPacket>> {
            match self.rx.recv().await {
                Err(_) => Ok(None),
                Ok(Err(error)) => Err(error),
                Ok(Ok(message)) => Ok(Some(DataPacket::try_from(message)?)),
            }
        }

        pub fn close(&self) {
            self.rx.close();
            self.notify.notify_waiters();
        }
    }
}

pub struct FlightDataAckState {
    seq: AtomicUsize,
    auto_ack_window_size: usize,

    may_retry: bool,
    receiver: Receiver<Result<FlightData, Status>>,
    confirmation_queue: VecDeque<(usize, Result<FlightData, Status>)>,
}

impl FlightDataAckState {
    pub fn create(
        window_size: usize,
        receiver: Receiver<Result<FlightData, Status>>,
    ) -> Arc<Mutex<FlightDataAckState>> {
        Arc::new(Mutex::new(FlightDataAckState {
            receiver,
            may_retry: true,
            seq: AtomicUsize::new(0),
            auto_ack_window_size: window_size,
            confirmation_queue: VecDeque::with_capacity(window_size),
        }))
    }

    fn ack_message(&mut self, seq: usize) {
        while let Some((id, _)) = self.confirmation_queue.front() {
            if *id <= seq {
                self.confirmation_queue.pop_front();
            }
        }
    }

    fn end_of_stream(&mut self) -> Poll<Option<Result<FlightData, Status>>> {
        let message_seq = self.seq.fetch_add(1, Ordering::SeqCst);
        self.ack_message(message_seq);

        self.may_retry = false;
        Poll::Ready(None)
    }

    fn error_of_stream(&mut self, cause: Status) -> Poll<Option<Result<FlightData, Status>>> {
        let message_seq = self.seq.fetch_add(1, Ordering::SeqCst);

        // Automatically acknowledge messages outside the ACK window.
        // A better approach is for the client to send back an ACK.
        if message_seq >= self.auto_ack_window_size {
            self.ack_message(message_seq - self.auto_ack_window_size);
        }

        self.confirmation_queue
            .push_back((message_seq, Err(cause.clone())));
        Poll::Ready(Some(Err(cause)))
    }

    fn message(&mut self, data: FlightData) -> Poll<Option<Result<FlightData, Status>>> {
        let message_seq = self.seq.fetch_add(1, Ordering::SeqCst);

        let (data, duplicate) = duplicate_flight_data(data);

        // Automatically acknowledge messages outside the ACK window.
        // A better approach is for the client to send back an ACK.
        if message_seq >= self.auto_ack_window_size {
            self.ack_message(message_seq - self.auto_ack_window_size);
        }

        self.confirmation_queue.push_back((message_seq, Ok(data)));
        Poll::Ready(Some(Ok(duplicate)))
    }

    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<FlightData, Status>>> {
        match Pin::new(&mut self.receiver).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => self.end_of_stream(),
            Poll::Ready(Some(Err(status))) => self.error_of_stream(status),
            Poll::Ready(Some(Ok(flight_data))) => self.message(flight_data),
        }
    }
}

pub struct FlightDataAckStream {
    state: Arc<Mutex<FlightDataAckState>>,
}

impl FlightDataAckStream {
    pub fn create(state: Arc<Mutex<FlightDataAckState>>, _i: usize) -> Result<FlightDataAckStream> {
        // TODO: reset begin
        Ok(FlightDataAckStream { state })
    }
}

impl Drop for FlightDataAckStream {
    fn drop(&mut self) {
        let state = self.state.lock();

        state.receiver.close();

        // TODO:
        // if state.may_retry {
        //     drop(state);
        //     let weak = Arc::downgrade(&self.state);
        //     GlobalIORuntime::instance().spawn(async move {
        //         // todo: wait retry connection and add timer
        //         tokio::time::sleep(Duration::from_secs(60)).await;
        //         if let Some(ss) = weak.upgrade() {
        //             let ss = ss.lock();
        //             ss.receiver.close();
        //         }
        //     });
        // }
    }
}

impl Stream for FlightDataAckStream {
    type Item = Result<FlightData, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.state.lock().poll_next(cx)
    }
}

fn duplicate_flight_data(flight_data: FlightData) -> (FlightData, FlightData) {
    (flight_data.clone(), flight_data)
}
