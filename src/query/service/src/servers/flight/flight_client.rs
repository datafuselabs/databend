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
use databend_common_arrow::arrow_format::flight::service::flight_service_client::FlightServiceClient;
use databend_common_base::base::tokio::time::Duration;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::TrySpawn;
use databend_common_base::JoinHandle;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use fastrace::func_path;
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

use crate::clusters::FlightParams;
use crate::pipelines::executor::WatchNotify;
use crate::servers::flight::request_builder::RequestBuilder;
use crate::servers::flight::v1::exchange::DataExchangeManager;
use crate::servers::flight::v1::packets::DataPacket;
use crate::servers::flight::v1::packets::FlightControlCommand;

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
    pub async fn request_statistics_exchange(
        &mut self,
        query_id: &str,
        target: &str,
        source_address: &str,
        flight_params: FlightParams,
    ) -> Result<FlightExchange> {
        let (server_tx, server_rx) = async_channel::bounded(1);
        let req = RequestBuilder::create(Box::pin(server_rx))
            .with_metadata("x-type", "request_server_exchange")?
            .with_metadata("x-target", target)?
            .with_metadata("x-query-id", query_id)?
            .with_metadata("x-continue-from", "0")?
            .with_metadata("x-enable-retry", &flight_params.retry_times.to_string())?
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
                retry_times: flight_params.retry_times,
                retry_interval: Duration::from_secs(flight_params.retry_interval),
            }),
            server_tx,
        ))
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    pub async fn request_fragment_exchange(
        &mut self,
        query_id: &str,
        target: &str,
        fragment: usize,
        source_address: &str,
        flight_params: FlightParams,
    ) -> Result<FlightExchange> {
        let (server_tx, server_rx) = async_channel::bounded(1);

        let request = RequestBuilder::create(Box::pin(server_rx))
            .with_metadata("x-type", "exchange_fragment")?
            .with_metadata("x-target", target)?
            .with_metadata("x-query-id", query_id)?
            .with_metadata("x-fragment-id", &fragment.to_string())?
            .with_metadata("x-continue-from", "0")?
            .with_metadata("x-enable-retry", &flight_params.retry_times.to_string())?
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
                retry_times: flight_params.retry_times,
                retry_interval: Duration::from_secs(flight_params.retry_interval),
            }),
            server_tx,
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
        .in_span(Span::enter_with_local_parent(func_path!()));

        databend_common_base::runtime::spawn(fut);

        (notify, rx)
    }

    #[async_backtrace::framed]
    async fn get_streaming(
        &mut self,
        request: Request<Pin<Box<Receiver<FlightData>>>>,
    ) -> Result<Streaming<FlightData>> {
        match self.inner.do_exchange(request).await {
            Ok(res) => Ok(res.into_inner()),
            Err(status) => Err(ErrorCode::from(status).add_message_back("(while in query flight)")),
        }
    }

    #[async_backtrace::framed]
    async fn reconnect(&mut self, info: &ConnectionInfo, seq: usize) -> Result<FlightRxInner> {
        let (server_tx, server_rx) = async_channel::bounded(1);
        let request = match info.fragment {
            Some(fragment_id) => RequestBuilder::create(Box::pin(server_rx))
                .with_metadata("x-type", "exchange_fragment")?
                .with_metadata("x-target", &info.target)?
                .with_metadata("x-query-id", &info.query_id)?
                .with_metadata("x-fragment-id", &fragment_id.to_string())?
                .with_metadata("x-continue-from", &seq.to_string())?
                .with_metadata("x-enable-retry", &info.retry_times.to_string())?
                .build(),
            None => RequestBuilder::create(Box::pin(server_rx))
                .with_metadata("x-type", "request_server_exchange")?
                .with_metadata("x-target", &info.target)?
                .with_metadata("x-query-id", &info.query_id)?
                .with_metadata("x-continue-from", &seq.to_string())?
                .with_metadata("x-enable-retry", &info.retry_times.to_string())?
                .build(),
        };
        let request = databend_common_tracing::inject_span_to_tonic_request(request);

        let streaming = self.get_streaming(request).await?;

        let (network_notify, recv) = Self::streaming_receiver(streaming);
        Ok(FlightRxInner::create(network_notify, recv, server_tx))
    }
}

#[derive(Clone)]
pub struct ConnectionInfo {
    pub query_id: String,
    pub target: String,
    pub fragment: Option<usize>,
    pub source_address: String,
    pub retry_times: u64,
    pub retry_interval: Duration,
}

pub struct FlightRxInner {
    notify: Arc<WatchNotify>,
    rx: Receiver<Result<FlightData>>,
    server_tx: Sender<FlightData>,
}

impl FlightRxInner {
    pub fn create(
        notify: Arc<WatchNotify>,
        rx: Receiver<Result<FlightData>>,
        server_tx: Sender<FlightData>,
    ) -> FlightRxInner {
        FlightRxInner {
            rx,
            notify,
            server_tx,
        }
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

    pub fn stop_cluster(&mut self) {
        // ignore the error, because we cannot determine the state of server side
        let _ = self.server_tx.try_send(
            FlightData::try_from(DataPacket::FlightControl(FlightControlCommand::Close))
                .expect("convert to flight data error"),
        );
    }
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
        RetryableFlightReceiver {
            seq: Arc::new(AtomicUsize::new(0)),
            info: None,
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
                (*inner).stop_cluster();
                (*inner).close();
            }
        }
    }
}

pub struct FlightSender {
    tx: Sender<std::result::Result<FlightData, Status>>,
}

impl FlightSender {
    pub fn create(tx: Sender<std::result::Result<FlightData, Status>>) -> FlightSender {
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
    pub sender: Option<Sender<std::result::Result<FlightData, Status>>>,
}

pub struct ReceiverPayload {
    seq: Arc<AtomicUsize>,
    info: Option<ConnectionInfo>,
    inner: Arc<AtomicPtr<FlightRxInner>>,
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
        sender: Sender<std::result::Result<FlightData, Status>>,
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
        server_tx: Sender<FlightData>,
    ) -> FlightExchange {
        FlightExchange::Receiver(ReceiverPayload {
            seq: Arc::new(AtomicUsize::new(0)),
            info: connection_info,
            inner: Arc::new(AtomicPtr::new(Box::into_raw(Box::new(
                FlightRxInner::create(notify, receiver, server_tx),
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

pub struct FlightDataAckState {
    seq: usize,
    finish: bool,
    receiver: Receiver<std::result::Result<FlightData, Status>>,
    ack_window: VecDeque<(usize, std::result::Result<Arc<FlightData>, Status>)>,
    clean_up_handle: Option<JoinHandle<()>>,
    window_size: usize,
}

impl FlightDataAckState {
    pub fn create(
        receiver: Receiver<std::result::Result<FlightData, Status>>,
        window_size: usize,
    ) -> Arc<Mutex<FlightDataAckState>> {
        Arc::new(Mutex::new(FlightDataAckState {
            receiver,
            seq: 0,
            ack_window: VecDeque::with_capacity(window_size),
            finish: false,
            clean_up_handle: None,
            window_size,
        }))
    }

    fn error_of_stream(
        &mut self,
        cause: Status,
    ) -> Poll<Option<std::result::Result<Arc<FlightData>, Status>>> {
        let message_seq = self.seq;
        self.seq += 1;
        self.ack_window.push_back((message_seq, Err(cause.clone())));
        Poll::Ready(Some(Err(cause)))
    }

    fn end_of_stream(&mut self) -> Poll<Option<std::result::Result<Arc<FlightData>, Status>>> {
        self.seq += 1;
        self.finish = true;
        Poll::Ready(None)
    }

    fn message(
        &mut self,
        data: FlightData,
    ) -> Poll<Option<std::result::Result<Arc<FlightData>, Status>>> {
        let message_seq = self.seq;
        self.seq += 1;
        let data = Arc::new(data);
        let duplicate = data.clone();
        self.ack_window.push_back((message_seq, Ok(data)));
        Poll::Ready(Some(Ok(duplicate)))
    }

    fn check_resend(&mut self) -> Option<std::result::Result<Arc<FlightData>, Status>> {
        let current_seq = self.seq;

        if let Some((seq, _packet)) = self.ack_window.back() {
            if seq + 1 == current_seq {
                return None;
            }
        }
        // resend case, iterate the queue to find the message to resend
        for (id, res) in self.ack_window.iter() {
            if *id == current_seq {
                self.seq += 1;
                return Some(res.clone());
            }
        }

        None
    }

    pub fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<Arc<FlightData>, Status>>> {
        if self.finish {
            return Poll::Ready(None);
        }

        // check if seq has been reset, if so, resend last packet
        if let Some(res) = self.check_resend() {
            return Poll::Ready(Some(res));
        }

        // check if ack window is full, if so, pop the oldest packet
        if self.ack_window.len() == self.window_size {
            self.ack_window.pop_front();
        }

        match Pin::new(&mut self.receiver).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => self.end_of_stream(),
            Poll::Ready(Some(Err(status))) => self.error_of_stream(status),
            Poll::Ready(Some(Ok(flight_data))) => self.message(flight_data),
        }
    }
}

pub struct FlightDataAckStream {
    notify: Arc<WatchNotify>,
    state: Arc<Mutex<FlightDataAckState>>,
    enable_retry: bool,
}

impl FlightDataAckStream {
    pub fn create(
        state: Arc<Mutex<FlightDataAckState>>,
        begin: usize,
        client_stream: Streaming<FlightData>,
        enable_retry: bool,
    ) -> Result<FlightDataAckStream> {
        let notify = Self::streaming_receiver(state.clone(), client_stream);
        let mut state_guard = state.lock();
        state_guard.seq = begin;
        state_guard.finish = false;
        if let Some(handle) = state_guard.clean_up_handle.take() {
            handle.abort();
        }
        drop(state_guard);
        Ok(FlightDataAckStream {
            notify,
            state,
            enable_retry,
        })
    }

    fn streaming_receiver(
        state: Arc<Mutex<FlightDataAckState>>,
        mut streaming: Streaming<FlightData>,
    ) -> Arc<WatchNotify> {
        let notify = Arc::new(WatchNotify::new());
        let fut = {
            let notify = notify.clone();
            async move {
                let notified = Box::pin(notify.notified());
                let streaming_next = streaming.next();

                match futures::future::select(notified, streaming_next).await {
                    Either::Left((_, _)) | Either::Right((None, _)) => {}
                    Either::Right((Some(message), _next_notified)) => {
                        if let Ok(flight_data) = message {
                            let packet = DataPacket::try_from(flight_data).unwrap();
                            if let DataPacket::FlightControl(FlightControlCommand::Close) = packet {
                                let mut state_guard = state.lock();
                                state_guard.finish = true;
                                state_guard.receiver.close();
                                drop(state_guard);
                            }
                        }
                    }
                }
                drop(state);
                drop(streaming);
            }
        }
        .in_span(Span::enter_with_local_parent(func_path!()));

        databend_common_base::runtime::spawn(fut);

        notify
    }
}

impl Drop for FlightDataAckStream {
    fn drop(&mut self) {
        info!("Drop FlightDataAckStream enable: {:?}", self.enable_retry);
        let mut state = self.state.lock();
        // if not enable retry, fallback to close immediately
        if !self.enable_retry || state.finish {
            info!("{:?} {:?}", state.finish, self.enable_retry);
            self.notify.notify_waiters();
            state.receiver.close();
            return;
        }
        let weak_state = Arc::downgrade(&self.state);
        let notify = Arc::downgrade(&self.notify);
        let handle = GlobalIORuntime::instance().spawn(async move {
            tokio::time::sleep(Duration::from_secs(60)).await;
            if let Some(state) = weak_state.upgrade() {
                let state_guard = state.lock();
                state_guard.receiver.close();
            }
            if let Some(notify) = notify.upgrade() {
                notify.notify_waiters();
            }
        });
        state.clean_up_handle = Some(handle);
    }
}

impl Stream for FlightDataAckStream {
    type Item = std::result::Result<Arc<FlightData>, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.state.lock().poll_next(cx)
    }
}
