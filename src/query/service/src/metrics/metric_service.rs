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

use std::net::SocketAddr;
use std::time::Duration;

use databend_common_base::runtime::metrics::GLOBAL_METRICS_REGISTRY;
use databend_common_exception::ErrorCode;
use databend_common_http::HttpError;
use databend_common_http::HttpShutdownHandler;
use poem::IntoResponse;

use crate::servers::Server;

pub struct MetricService {
    shutdown_handler: HttpShutdownHandler,
}

#[allow(clippy::let_with_type_underscore)]
#[poem::handler]
#[async_backtrace::framed]
pub async fn metrics_handler() -> impl IntoResponse {
    GLOBAL_METRICS_REGISTRY
        .render_metrics()
        .unwrap_or_else(|e| e.message())
}

impl MetricService {
    // TODO add session tls handler
    pub fn create() -> Box<MetricService> {
        Box::new(MetricService {
            shutdown_handler: HttpShutdownHandler::create("metric api".to_string()),
        })
    }

    #[async_backtrace::framed]
    async fn start_without_tls(&mut self, listening: SocketAddr) -> Result<SocketAddr, HttpError> {
        let app = poem::Route::new().at("/metrics", poem::get(metrics_handler));
        let addr = self
            .shutdown_handler
            .start_service(listening, None, app, Some(Duration::from_millis(100)))
            .await?;
        Ok(addr)
    }
}

#[async_trait::async_trait]
impl Server for MetricService {
    #[async_backtrace::framed]
    async fn shutdown(&mut self, graceful: bool) {
        self.shutdown_handler.shutdown(graceful).await;
    }

    #[async_backtrace::framed]
    async fn start(&mut self, listening: SocketAddr) -> Result<SocketAddr, ErrorCode> {
        let res = self.start_without_tls(listening).await;

        res.map_err(|e: HttpError| match e {
            HttpError::BadAddressFormat(any_err) => {
                ErrorCode::BadAddressFormat(any_err.to_string())
            }
            le @ HttpError::ListenError { .. } => ErrorCode::CannotListenerPort(le.to_string()),
            HttpError::TlsConfigError(any_err) => {
                ErrorCode::TLSConfigurationFailure(any_err.to_string())
            }
        })
    }
}
