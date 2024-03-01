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

use std::future::Future;
use std::net::SocketAddr;
use std::sync::Arc;

use arrow_flight::flight_service_server::FlightServiceServer;
use databend_common_base::base::tokio;
use databend_common_base::base::tokio::sync::Notify;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use log::info;
use tonic::transport::server::TcpIncoming;
use tonic::transport::Identity;
use tonic::transport::Server;
use tonic::transport::ServerTlsConfig;

use crate::servers::flight_sql::flight_sql_service::FlightSqlServiceImpl;
use crate::servers::Server as DatabendQueryServer;

pub struct FlightSQLServer {
    pub config: InnerConfig,
    pub abort_notify: Arc<Notify>,
}

impl FlightSQLServer {
    pub fn create(config: &InnerConfig) -> Result<Box<dyn DatabendQueryServer>> {
        Ok(Box::new(Self {
            config: config.clone(),
            abort_notify: Arc::new(Notify::new()),
        }))
    }

    fn shutdown_notify(&self) -> impl Future<Output = ()> + 'static {
        let notified = self.abort_notify.clone();
        async move {
            notified.notified().await;
        }
    }

    #[allow(unused)]
    #[async_backtrace::framed]
    async fn server_tls_config(conf: &InnerConfig) -> Result<ServerTlsConfig> {
        let cert = tokio::fs::read(conf.query.flight_sql_tls_server_cert.as_str()).await?;
        let key = tokio::fs::read(conf.query.flight_sql_tls_server_key.as_str()).await?;
        let server_identity = Identity::from_pem(cert, key);
        let tls_conf = ServerTlsConfig::new().identity(server_identity);
        Ok(tls_conf)
    }

    #[async_backtrace::framed]
    pub async fn start_with_incoming(&mut self, addr: SocketAddr) -> Result<()> {
        let flight_sql_service = FlightSqlServiceImpl::create();
        let builder = Server::builder();
        let mut builder = if self.config.flight_sql_tls_server_enabled() {
            info!("databend query tls flight sql enabled");
            builder
                .tls_config(Self::server_tls_config(&self.config).await.map_err(|e| {
                    ErrorCode::TLSConfigurationFailure(format!(
                        "failed to load server tls config: {e}",
                    ))
                })?)
                .map_err(|e| {
                    ErrorCode::TLSConfigurationFailure(format!("failed to invoke tls_config: {e}",))
                })?
        } else {
            builder
        };

        let incoming = TcpIncoming::new(addr, true, None)
            .map_err(|e| ErrorCode::CannotListenerPort(format!("{e}")))?;

        let server = builder
            .add_service(FlightServiceServer::new(flight_sql_service))
            .serve_with_incoming_shutdown(incoming, self.shutdown_notify());

        databend_common_base::runtime::spawn(server);
        Ok(())
    }
}

#[async_trait::async_trait]
impl DatabendQueryServer for FlightSQLServer {
    #[async_backtrace::framed]
    async fn shutdown(&mut self, _graceful: bool) {}

    #[async_backtrace::framed]
    async fn start(&mut self, addr: SocketAddr) -> Result<SocketAddr> {
        self.start_with_incoming(addr).await?;
        Ok(addr)
    }
}
