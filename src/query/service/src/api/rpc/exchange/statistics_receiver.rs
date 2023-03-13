// Copyright 2022 Datafuse Labs.
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

use std::future::Future;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use common_base::base::select3;
use common_base::base::tokio;
use common_base::base::tokio::sync::Notify;
use common_base::base::tokio::task::JoinHandle;
use common_base::base::Select3Output;
use common_base::block_on;
use common_base::runtime::Runtime;
use common_base::runtime::TrySpawn;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::api::rpc::flight_client::FlightExchange;
use crate::api::DataPacket;
use crate::sessions::QueryContext;

pub struct StatisticsReceiver {
    ctx: Arc<QueryContext>,
    exchanges: Vec<FlightExchange>,
    shutdown_flag: Arc<AtomicBool>,
    shutdown_notify: Arc<Notify>,
    exchange_handler: Vec<JoinHandle<Result<()>>>,
    runtime: Arc<Runtime>,
}

impl StatisticsReceiver {
    pub fn create(
        ctx: Arc<QueryContext>,
        exchanges: Vec<FlightExchange>,
    ) -> Result<StatisticsReceiver> {
        Ok(StatisticsReceiver {
            ctx,
            exchanges,
            shutdown_flag: Arc::new(AtomicBool::new(false)),
            shutdown_notify: Arc::new(Notify::new()),
            exchange_handler: vec![],
            runtime: Arc::new(Runtime::with_worker_threads(
                2,
                Some(String::from("StatisticsReceiver")),
                false,
            )?),
        })
    }

    pub fn start(&mut self) {
        while let Some(flight_exchange) = self.exchanges.pop() {
            let ctx = self.ctx.clone();
            let shutdown_flag = self.shutdown_flag.clone();
            let shutdown_notify = self.shutdown_notify.clone();

            self.exchange_handler.push(self.runtime.spawn(async move {
                let mut recv = Box::pin(flight_exchange.recv());
                let mut notified = Box::pin(shutdown_notify.notified());

                'worker_loop: while !shutdown_flag.load(Ordering::Relaxed) {
                    let interval = Box::pin(tokio::time::sleep(Duration::from_millis(500)));

                    match select3(interval, notified, recv).await {
                        Select3Output::Left((_res, middle, right)) => {
                            recv = right;
                            notified = middle;

                            if !shutdown_flag.load(Ordering::Relaxed) {
                                match Self::fetch(&ctx, &flight_exchange, recv).await {
                                    Ok(true) => {
                                        flight_exchange.close_input().await;
                                        flight_exchange.close_output().await;
                                        return Ok(());
                                    }
                                    Ok(false) => {
                                        recv = Box::pin(flight_exchange.recv());
                                    }
                                    Err(cause) => {
                                        ctx.get_current_session().force_kill_query(cause.clone());
                                        flight_exchange.close_input().await;
                                        flight_exchange.close_output().await;
                                        return Err(cause);
                                    }
                                };
                            }
                        }
                        Select3Output::Middle((_, _, right)) => {
                            recv = right;
                            break 'worker_loop;
                        }
                        Select3Output::Right((res, _, middle)) => {
                            notified = middle;
                            match Self::recv_data(&ctx, res) {
                                Ok(true) => {
                                    flight_exchange.close_input().await;
                                    flight_exchange.close_output().await;
                                    return Ok(());
                                }
                                Ok(false) => {
                                    recv = Box::pin(flight_exchange.recv());
                                }
                                Err(cause) => {
                                    ctx.get_current_session().force_kill_query(cause.clone());
                                    flight_exchange.close_input().await;
                                    flight_exchange.close_output().await;
                                    return Err(cause);
                                }
                            };
                        }
                    }
                }

                if let Err(cause) = Self::fetch(&ctx, &flight_exchange, recv).await {
                    ctx.get_current_session().force_kill_query(cause.clone());
                    flight_exchange.close_input().await;
                    flight_exchange.close_output().await;
                    return Err(cause);
                }

                flight_exchange.close_input().await;
                flight_exchange.close_output().await;
                Ok(())
            }));
        }
    }

    async fn fetch(
        ctx: &Arc<QueryContext>,
        flight_exchange: &FlightExchange,
        recv: impl Future<Output = Result<Option<DataPacket>>>,
    ) -> Result<bool> {
        if let Err(error) = flight_exchange
            .send(DataPacket::FetchProgressAndPrecommit)
            .await
        {
            // The query is done(in remote).
            return match error.code() == ErrorCode::ABORTED_QUERY {
                true => Ok(true),
                false => Err(error),
            };
        }

        Self::recv_data(ctx, recv.await)
    }

    fn recv_data(ctx: &Arc<QueryContext>, recv_data: Result<Option<DataPacket>>) -> Result<bool> {
        match recv_data {
            Ok(None) => Ok(true),
            Err(transport_error) => Err(transport_error),
            Ok(Some(DataPacket::ErrorCode(error))) => Err(error),
            Ok(Some(DataPacket::ClosingInput)) => unreachable!(),
            Ok(Some(DataPacket::ClosingOutput)) => unreachable!(),
            Ok(Some(DataPacket::FragmentData(_))) => unreachable!(),
            Ok(Some(DataPacket::FetchProgressAndPrecommit)) => unreachable!(),
            Ok(Some(DataPacket::ProgressAndPrecommit {
                progress,
                precommit,
            })) => {
                for progress_info in progress {
                    progress_info.inc(ctx);
                }

                for precommit_block in precommit {
                    precommit_block.precommit(ctx);
                }

                Ok(false)
            }
        }
    }

    pub fn shutdown(&mut self) {
        self.shutdown_flag.store(true, Ordering::Release);
        self.shutdown_notify.notify_waiters();
    }

    pub fn wait_shutdown(&mut self) -> Result<()> {
        let mut exchanges_handler = std::mem::take(&mut self.exchange_handler);
        block_on(async move {
            while let Some(exchange_handler) = exchanges_handler.pop() {
                match exchange_handler.await {
                    Ok(Ok(_)) => Ok(()),
                    Ok(Err(cause)) => Err(cause),
                    Err(join_error) => match join_error.is_cancelled() {
                        true => Err(ErrorCode::TokioError("Tokio error is cancelled.")),
                        false => {
                            let panic_error = join_error.into_panic();
                            match panic_error.downcast_ref::<&'static str>() {
                                None => match panic_error.downcast_ref::<String>() {
                                    None => {
                                        Err(ErrorCode::PanicError("Sorry, unknown panic message"))
                                    }
                                    Some(message) => {
                                        Err(ErrorCode::PanicError(message.to_string()))
                                    }
                                },
                                Some(message) => Err(ErrorCode::PanicError(message.to_string())),
                            }
                        }
                    },
                }?;
            }

            Ok(())
        })
    }
}
