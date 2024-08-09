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
use std::sync::Arc;

use anyerror::AnyError;
use databend_common_base::base::tokio::sync::broadcast;
use databend_common_base::base::Stoppable;
use databend_common_http::health_handler;
use databend_common_http::home::debug_home_handler;
#[cfg(feature = "memory-profiling")]
use databend_common_http::jeprof::debug_jeprof_dump_handler;
use databend_common_http::pprof::debug_pprof_handler;
use databend_common_http::HttpError;
use databend_common_http::HttpShutdownHandler;
use databend_common_meta_types::MetaNetworkError;
use log::info;
use log::warn;
use poem::get;
use poem::listener::OpensslTlsConfig;
use poem::Endpoint;
use poem::EndpointExt;
use poem::Route;

use crate::configs::Config;
use crate::meta_service::MetaNode;

pub struct HttpService {
    cfg: Config,
    shutdown_handler: HttpShutdownHandler,
    meta_node: Arc<MetaNode>,
}

impl HttpService {
    pub fn create(cfg: Config, meta_node: Arc<MetaNode>) -> Box<Self> {
        Box::new(HttpService {
            cfg,
            shutdown_handler: HttpShutdownHandler::create("http api".to_string()),
            meta_node,
        })
    }

    fn build_router(&self) -> impl Endpoint {
        #[cfg_attr(not(feature = "memory-profiling"), allow(unused_mut))]
        let mut route = Route::new()
            .at("/v1/health", get(health_handler))
            .at("/v1/config", get(super::http::v1::config::config_handler))
            .at(
                "/v1/ctrl/trigger_snapshot",
                get(super::http::v1::ctrl::trigger_snapshot),
            )
            .at(
                "/v1/ctrl/trigger_transfer_leader",
                get(super::http::v1::ctrl::trigger_transfer_leader),
            )
            .at(
                "/v1/ctrl/block_write_snapshot",
                get(super::http::v1::ctrl::block_write_snapshot),
            )
            .at(
                "/v1/ctrl/block_compact_snapshot",
                get(super::http::v1::ctrl::block_compact_snapshot),
            )
            .at(
                "/v1/cluster/nodes",
                get(super::http::v1::cluster_state::nodes_handler),
            )
            .at(
                "/v1/cluster/status",
                get(super::http::v1::cluster_state::status_handler),
            )
            .at(
                "/v1/metrics",
                get(super::http::v1::metrics::metrics_handler),
            )
            .at("/debug/home", get(debug_home_handler))
            .at("/debug/pprof/profile", get(debug_pprof_handler));

        #[cfg(feature = "memory-profiling")]
        {
            route = route.at(
                // to follow the conversions of jeprof, we arrange the path in
                // this way, so that jeprof could be invoked like:
                //   `jeprof ./target/debug/databend-meta http://localhost:28002/debug/mem`
                // and jeprof will translate the above url into sth like:
                //    "http://localhost:28002/debug/mem/pprof/profile?seconds=30"
                "/debug/mem/pprof/profile",
                get(debug_jeprof_dump_handler),
            );
        };
        route.data(self.meta_node.clone()).data(self.cfg.clone())
    }

    fn build_tls(config: &Config) -> Result<OpensslTlsConfig, MetaNetworkError> {
        let cfg = OpensslTlsConfig::new()
            .cert_from_file(config.admin_tls_server_cert.as_str())
            .key_from_file(config.admin_tls_server_key.as_str());
        Ok(cfg)
    }

    async fn start_with_tls(&mut self, listening: SocketAddr) -> Result<(), HttpError> {
        info!("Http API TLS enabled");

        let tls_config = Self::build_tls(&self.cfg.clone())
            .map_err(|e| HttpError::TlsConfigError(AnyError::new(&e)))?;
        self.shutdown_handler
            .start_service(listening, Some(tls_config), self.build_router(), None)
            .await?;
        Ok(())
    }

    async fn start_without_tls(&mut self, listening: SocketAddr) -> Result<(), HttpError> {
        warn!("Http API TLS not set");

        self.shutdown_handler
            .start_service(listening, None, self.build_router(), None)
            .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl Stoppable for HttpService {
    type Error = AnyError;

    async fn start(&mut self) -> Result<(), Self::Error> {
        let conf = self.cfg.clone();
        let listening = conf
            .admin_api_address
            .parse::<SocketAddr>()
            .map_err(|e| AnyError::new(&e))?;

        let res =
            match conf.admin_tls_server_key.is_empty() || conf.admin_tls_server_cert.is_empty() {
                true => self.start_without_tls(listening).await,
                false => self.start_with_tls(listening).await,
            };

        res.map_err(|e| AnyError::new(&e))
    }

    async fn stop(&mut self, force: Option<broadcast::Receiver<()>>) -> Result<(), Self::Error> {
        self.shutdown_handler.stop(force).await;
        Ok(())
    }
}
