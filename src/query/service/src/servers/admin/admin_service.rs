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

use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_http::health_handler;
use databend_common_http::home::debug_home_handler;
#[cfg(feature = "memory-profiling")]
use databend_common_http::jeprof::debug_jeprof_dump_handler;
use databend_common_http::pprof::debug_pprof_handler;
use databend_common_http::stack::debug_dump_stack;
use databend_common_http::HttpError;
use databend_common_http::HttpShutdownHandler;
use databend_common_meta_types::anyerror::AnyError;
use log::info;
use log::warn;
use poem::get;
use poem::listener::OpensslTlsConfig;
use poem::post;
use poem::Endpoint;
use poem::Route;

use crate::servers::Server;

pub struct AdminService {
    config: InnerConfig,
    shutdown_handler: HttpShutdownHandler,
}

impl AdminService {
    pub fn create(config: &InnerConfig) -> Box<AdminService> {
        Box::new(AdminService {
            config: config.clone(),
            shutdown_handler: HttpShutdownHandler::create("http api".to_string()),
        })
    }

    fn build_router(&self) -> impl Endpoint {
        #[cfg_attr(not(feature = "memory-profiling"), allow(unused_mut))]
        let mut route = Route::new()
            .at("/v1/health", get(health_handler))
            .at("/v1/config", get(super::v1::config::config_handler))
            .at("/v1/system", get(super::v1::system::system_handler))
            .at(
                "/v1/status",
                get(super::v1::instance_status::instance_status_handler),
            )
            .at(
                "/v1/processlist",
                get(super::v1::processes::processlist_handler),
            )
            .at(
                "/v1/tables",
                get(super::v1::tenant_tables::list_tables_handler),
            )
            .at(
                "/v1/cluster/list",
                get(super::v1::cluster::cluster_list_handler),
            )
            .at(
                "v1/queries/:query_id/profiling",
                get(super::v1::query_profiling::query_profiling_handler),
            )
            .at("/debug/home", get(debug_home_handler))
            .at("/debug/pprof/profile", get(debug_pprof_handler))
            .at("/debug/async_tasks/dump", get(debug_dump_stack));

        // Multiple tenants admin api
        if self.config.query.management_mode {
            route = route
                .at(
                    "/v1/tenants/:tenant/tables",
                    get(super::v1::tenant_tables::list_tenant_tables_handler),
                )
                .at(
                    "v1/tenants/:tenant/stream_status",
                    get(super::v1::stream_status::stream_status_handler),
                )
                .at(
                    "/v1/background/:tenant/background_tasks",
                    get(super::v1::background_tasks::list_background_tasks),
                )
                .at(
                    "/v1/tenants/:tenant/background_tasks",
                    get(super::v1::background_tasks::list_background_tasks),
                )
                .at(
                    "/v1/tenants/:tenant/settings",
                    get(super::v1::settings::list_settings),
                )
                .at(
                    "/v1/tenants/:tenant/settings/:key",
                    post(super::v1::settings::set_settings)
                        .delete(super::v1::settings::unset_settings),
                )
                .at(
                    "/v1/tenants/:tenant/user_functions",
                    get(super::v1::user_functions::user_functions),
                );
        }

        #[cfg(feature = "memory-profiling")]
        {
            route = route.at(
                // to follow the conversions of jeprof, we arrange the path in
                // this way, so that jeprof could be invoked like:
                //   `jeprof ./target/debug/databend-query http://localhost:8080/debug/mem`
                // and jeprof will translate the above url into sth like:
                //    "http://localhost:8080/debug/mem/pprof/profile?seconds=30"
                "/debug/mem/pprof/profile",
                get(debug_jeprof_dump_handler),
            );
        };

        route
    }

    fn build_tls(config: &InnerConfig) -> Result<OpensslTlsConfig, std::io::Error> {
        let cfg = OpensslTlsConfig::new()
            .cert_from_file(config.query.api_tls_server_cert.as_str())
            .key_from_file(config.query.api_tls_server_key.as_str());

        // if Path::new(&config.query.api_tls_server_root_ca_cert).exists() {
        //     cfg = cfg.client_auth_required(std::fs::read(
        //         config.query.api_tls_server_root_ca_cert.as_str(),
        //     )?);
        // }
        Ok(cfg)
    }

    #[async_backtrace::framed]
    async fn start_with_tls(&mut self, listening: SocketAddr) -> Result<SocketAddr, HttpError> {
        info!("Http API TLS enabled");

        let tls_config = Self::build_tls(&self.config)
            .map_err(|e| HttpError::TlsConfigError(AnyError::new(&e)))?;

        let addr = self
            .shutdown_handler
            .start_service(
                listening,
                Some(tls_config),
                self.build_router(),
                Some(Duration::from_millis(1000)),
            )
            .await?;
        Ok(addr)
    }

    #[async_backtrace::framed]
    async fn start_without_tls(&mut self, listening: SocketAddr) -> Result<SocketAddr, HttpError> {
        warn!("Http API TLS not set");

        let addr = self
            .shutdown_handler
            .start_service(
                listening,
                None,
                self.build_router(),
                Some(Duration::from_millis(1000)),
            )
            .await?;
        Ok(addr)
    }
}

#[async_trait::async_trait]
impl Server for AdminService {
    #[async_backtrace::framed]
    async fn shutdown(&mut self, _graceful: bool) {
        // intendfully do nothing: sometimes we hope to diagnose the backtraces or metrics after
        // the process got the sigterm signal, we can still leave the admin service port open until
        // the process exited. it's not an user facing service, it's allowed to shutdown forcely.
    }

    #[async_backtrace::framed]
    async fn start(&mut self, listening: SocketAddr) -> Result<SocketAddr, ErrorCode> {
        let config = &self.config.query;
        let res =
            match config.api_tls_server_key.is_empty() || config.api_tls_server_cert.is_empty() {
                true => self.start_without_tls(listening).await,
                false => self.start_with_tls(listening).await,
            };

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
