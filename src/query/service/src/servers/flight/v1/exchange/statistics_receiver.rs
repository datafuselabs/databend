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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_base::base::tokio::sync::broadcast::channel;
use databend_common_base::base::tokio::sync::broadcast::Sender;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::TrySpawn;
use databend_common_base::JoinHandle;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use futures_util::future::select;
use futures_util::future::Either;

use crate::servers::flight::v1::packets::DataPacket;
use crate::servers::flight::FlightExchange;
use crate::sessions::QueryContext;

pub struct StatisticsReceiver {
    _runtime: Runtime,
    shutdown_tx: Option<Sender<bool>>,
    exchange_handler: Vec<JoinHandle<Result<()>>>,
}

impl StatisticsReceiver {
    pub fn spawn_receiver(
        ctx: &Arc<QueryContext>,
        statistics_exchanges: HashMap<String, FlightExchange>,
    ) -> Result<StatisticsReceiver> {
        let (shutdown_tx, _shutdown_rx) = channel(2);
        let mut exchange_handler = Vec::with_capacity(statistics_exchanges.len());
        let runtime = Runtime::with_worker_threads(2, Some(String::from("StatisticsReceiver")))?;

        for (_source, exchange) in statistics_exchanges.into_iter() {
            let rx = exchange.convert_to_receiver();
            exchange_handler.push(runtime.spawn({
                let ctx = ctx.clone();
                let shutdown_rx = shutdown_tx.subscribe();

                async move {
                    let mut shutdown_rx = shutdown_rx;
                    let mut recv = Box::pin(rx.recv());
                    let mut notified = Box::pin(shutdown_rx.recv());

                    loop {
                        match select(notified, recv).await {
                            Either::Left((Ok(true /* has error */), _))
                            | Either::Left((Err(_), _)) => {
                                return Ok(());
                            }
                            Either::Left((Ok(false), recv)) => {
                                match StatisticsReceiver::recv_data(&ctx, recv.await) {
                                    Ok(true) => {
                                        return Ok(());
                                    }
                                    Err(cause) => {
                                        ctx.get_current_session().force_kill_query(cause.clone());
                                        return Err(cause);
                                    }
                                    _ => loop {
                                        match StatisticsReceiver::recv_data(&ctx, rx.recv().await) {
                                            Ok(true) => {
                                                return Ok(());
                                            }
                                            Err(cause) => {
                                                ctx.get_current_session()
                                                    .force_kill_query(cause.clone());
                                                return Err(cause);
                                            }
                                            _ => {}
                                        }
                                    },
                                }
                            }
                            Either::Right((res, left)) => {
                                match StatisticsReceiver::recv_data(&ctx, res) {
                                    Ok(true) => {
                                        return Ok(());
                                    }
                                    Ok(false) => {
                                        notified = left;
                                        recv = Box::pin(rx.recv());
                                    }
                                    Err(cause) => {
                                        ctx.get_current_session().force_kill_query(cause.clone());
                                        return Err(cause);
                                    }
                                };
                            }
                        }
                    }
                }
            }));
        }

        Ok(StatisticsReceiver {
            exchange_handler,
            _runtime: runtime,
            shutdown_tx: Some(shutdown_tx),
        })
    }

    fn recv_data(ctx: &Arc<QueryContext>, recv_data: Result<Option<DataPacket>>) -> Result<bool> {
        match recv_data {
            Ok(None) => Ok(true),
            Err(transport_error) => Err(transport_error),
            Ok(Some(DataPacket::ErrorCode(error))) => Err(error),
            Ok(Some(DataPacket::Dictionary(_))) => unreachable!(),
            Ok(Some(DataPacket::FragmentData(_))) => unreachable!(),
            Ok(Some(DataPacket::SerializeProgress(progress))) => {
                for progress_info in progress {
                    progress_info.inc(ctx);
                }

                Ok(false)
            }
            Ok(Some(DataPacket::QueryProfiles(profiles))) => {
                ctx.add_query_profiles(&profiles);
                Ok(false)
            }
            Ok(Some(DataPacket::CopyStatus(status))) => {
                log::info!("Merge CopyStatus for {} files", status.files.len());
                ctx.get_copy_status().merge(status);
                Ok(false)
            }
            Ok(Some(DataPacket::MutationStatus(status))) => {
                log::info!("Merge MutationStatus");
                ctx.get_mutation_status()
                    .write()
                    .merge_mutation_status(status);
                Ok(false)
            }
            Ok(Some(DataPacket::DataCacheMetrics(metrics))) => {
                ctx.get_data_cache_metrics().merge(metrics);
                Ok(false)
            }
        }
    }

    pub fn shutdown(&mut self, has_err: bool) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(has_err);
        }
    }

    pub fn wait_shutdown(&mut self) -> Result<()> {
        let mut exchanges_handler = std::mem::take(&mut self.exchange_handler);
        futures::executor::block_on(async move {
            while let Some(exchange_handler) = exchanges_handler.pop() {
                exchange_handler.await.flatten()?;
            }

            Ok(())
        })
    }
}
