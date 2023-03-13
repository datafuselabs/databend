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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use async_channel::Receiver;
use async_channel::Sender;
use common_base::block_on;
use common_base::runtime::TrySpawn;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use futures_util::future::Either;

use crate::api::rpc::flight_client::FlightExchange;
use crate::api::rpc::packets::PrecommitBlock;
use crate::api::rpc::packets::ProgressInfo;
use crate::api::DataPacket;
use crate::sessions::QueryContext;

pub struct StatisticsSender {
    query_id: String,
    ctx: Arc<QueryContext>,
    exchange: Option<FlightExchange>,
    shutdown_flag: Arc<AtomicBool>,
    shutdown_flag_sender: Sender<Option<ErrorCode>>,
    shutdown_flag_receiver: Receiver<Option<ErrorCode>>,
}

impl StatisticsSender {
    pub fn create(
        query_id: &str,
        ctx: Arc<QueryContext>,
        exchange: FlightExchange,
    ) -> StatisticsSender {
        let (shutdown_flag_sender, shutdown_flag_receiver) = async_channel::bounded(1);
        StatisticsSender {
            ctx,
            shutdown_flag_sender,
            shutdown_flag_receiver,
            exchange: Some(exchange),
            query_id: query_id.to_string(),
            shutdown_flag: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn start(&mut self) {
        let ctx = self.ctx.clone();
        let query_id = self.query_id.clone();
        let flight_exchange = self.exchange.take().unwrap();
        let shutdown_flag = self.shutdown_flag.clone();
        let shutdown_flag_receiver = self.shutdown_flag_receiver.clone();

        let spawner = ctx.clone();
        spawner.spawn(async move {
            let mut recv = Box::pin(flight_exchange.recv());
            let mut notified = Box::pin(shutdown_flag_receiver.recv());

            while !shutdown_flag.load(Ordering::Relaxed) {
                match futures::future::select(recv, notified).await {
                    Either::Right((Ok(None), _))
                    | Either::Right((Err(_), _))
                    | Either::Left((Ok(None), _))
                    | Either::Left((Err(_), _)) => {
                        break;
                    }
                    Either::Right((Ok(Some(error_code)), _recv)) => {
                        let data = DataPacket::ErrorCode(error_code);
                        if let Err(error_code) = flight_exchange.send(data).await {
                            tracing::warn!(
                                "Cannot send data via flight exchange, cause: {:?}",
                                error_code
                            );
                        }

                        flight_exchange.close_input().await;
                        flight_exchange.close_output().await;
                        return;
                    }
                    Either::Left((Ok(Some(command)), right)) => {
                        notified = right;
                        recv = Box::pin(flight_exchange.recv());

                        if let Err(_cause) = Self::on_command(&ctx, command, &flight_exchange).await
                        {
                            ctx.get_exchange_manager().shutdown_query(&query_id);
                            flight_exchange.close_input().await;
                            flight_exchange.close_output().await;
                            return;
                        }
                    }
                }
            }

            if let Ok(Some(command)) = flight_exchange.recv().await {
                if let Err(error) = Self::on_command(&ctx, command, &flight_exchange).await {
                    tracing::warn!("Statistics send has error, cause: {:?}.", error);
                }
            }

            flight_exchange.close_input().await;
            flight_exchange.close_output().await;
        });
    }

    pub fn shutdown(&mut self, error: Option<ErrorCode>) {
        self.shutdown_flag.store(true, Ordering::Release);
        let shutdown_flag_sender = self.shutdown_flag_sender.clone();

        block_on(async move {
            if let Err(error_code) = shutdown_flag_sender.send(error).await {
                tracing::warn!(
                    "Cannot send data via flight exchange, cause: {:?}",
                    error_code
                );
            }

            shutdown_flag_sender.close();
        });
    }

    async fn on_command(
        ctx: &Arc<QueryContext>,
        command: DataPacket,
        exchange_flight: &FlightExchange,
    ) -> Result<()> {
        match command {
            DataPacket::ErrorCode(_) => unreachable!(),
            DataPacket::FragmentData(_) => unreachable!(),
            DataPacket::ProgressAndPrecommit { .. } => unreachable!(),
            DataPacket::ClosingInput => unreachable!(),
            DataPacket::ClosingOutput => unreachable!(),
            DataPacket::FetchProgressAndPrecommit => {
                exchange_flight
                    .send(DataPacket::ProgressAndPrecommit {
                        progress: Self::fetch_progress(ctx).await?,
                        precommit: Self::fetch_precommit(ctx).await?,
                    })
                    .await
            }
        }
    }

    async fn fetch_progress(ctx: &Arc<QueryContext>) -> Result<Vec<ProgressInfo>> {
        let mut progress_info = vec![];

        let scan_progress = ctx.get_scan_progress();
        let scan_progress_values = scan_progress.fetch();

        if scan_progress_values.rows != 0 || scan_progress_values.bytes != 0 {
            progress_info.push(ProgressInfo::ScanProgress(scan_progress_values));
        }

        let write_progress = ctx.get_write_progress();
        let write_progress_values = write_progress.fetch();

        if write_progress_values.rows != 0 || write_progress_values.bytes != 0 {
            progress_info.push(ProgressInfo::WriteProgress(write_progress_values));
        }

        let result_progress = ctx.get_result_progress();
        let result_progress_values = result_progress.fetch();

        if result_progress_values.rows != 0 || result_progress_values.bytes != 0 {
            progress_info.push(ProgressInfo::ResultProgress(result_progress_values));
        }

        Ok(progress_info)
    }

    async fn fetch_precommit(ctx: &Arc<QueryContext>) -> Result<Vec<PrecommitBlock>> {
        Ok(ctx
            .consume_precommit_blocks()
            .into_iter()
            .map(PrecommitBlock)
            .collect())
    }
}
