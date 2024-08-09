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

use anyerror::AnyError;
use databend_common_base::base::tokio;
use databend_common_base::base::tokio::sync::broadcast;
use databend_common_base::base::tokio::sync::oneshot;
use databend_common_base::base::tokio::task::JoinHandle;
use futures::future::Either;
use futures::FutureExt;
use log::error;
use log::info;
use poem::listener::Acceptor;
use poem::listener::AcceptorExt;
use poem::listener::IntoTlsConfigStream;
use poem::listener::Listener;
use poem::listener::OpensslTlsConfig;
use poem::listener::TcpListener;
use poem::Endpoint;

use crate::HttpError;

pub struct HttpShutdownHandler {
    service_name: String,
    join_handle: Option<JoinHandle<std::io::Result<()>>>,
    abort_handle: Option<oneshot::Sender<()>>,
}

impl HttpShutdownHandler {
    pub fn create(service_name: String) -> HttpShutdownHandler {
        HttpShutdownHandler {
            service_name,
            join_handle: None,
            abort_handle: None,
        }
    }

    pub async fn start_service(
        &mut self,
        listening: SocketAddr,
        tls_config: Option<OpensslTlsConfig>,
        ep: impl Endpoint + 'static,
        graceful_shutdown_timeout: Option<Duration>,
    ) -> Result<SocketAddr, HttpError> {
        assert!(self.join_handle.is_none());
        assert!(self.abort_handle.is_none());

        let mut acceptor = TcpListener::bind(listening)
            .into_acceptor()
            .await
            .map_err(|err| HttpError::listen_error(listening, err))?
            .boxed();

        let addr = acceptor
            .local_addr()
            .pop()
            .and_then(|addr| addr.0.as_socket_addr().cloned())
            .expect("socket addr");

        if let Some(tls_config) = tls_config {
            let conf_stream = tls_config
                .into_stream()
                .map_err(|err| HttpError::TlsConfigError(AnyError::new(&err)))?;

            acceptor = acceptor.openssl_tls(conf_stream).boxed();
        }

        let (tx, rx) = oneshot::channel();
        let join_handle = databend_common_base::runtime::spawn(
            poem::Server::new_with_acceptor(acceptor)
                .name(self.service_name.clone())
                .idle_timeout(Duration::from_secs(20))
                .run_with_graceful_shutdown(ep, rx.map(|_| ()), graceful_shutdown_timeout),
        );
        self.join_handle = Some(join_handle);
        self.abort_handle = Some(tx);
        Ok(addr)
    }

    /// Shutdown in graceful mode and returns a join handle.
    /// To force shutdown: call the `abort()` method of the returned handle.
    fn send_stop_signal(&mut self) -> JoinHandle<std::io::Result<()>> {
        info!("{}: graceful stop", self.service_name);

        if let Some(abort_handle) = self.abort_handle.take() {
            info!("{}: send signal to abort_handle", self.service_name);

            let res = abort_handle.send(());

            info!(
                "Done: {}: send signal to abort_handle, res: {:?}",
                self.service_name, res
            );
        }

        let join_handle = self.join_handle.take();
        join_handle.unwrap()
    }

    /// Stop service gracefully. If `force` is ready, force shutdown the service.
    pub async fn stop(&mut self, force: Option<broadcast::Receiver<()>>) {
        let join_handle = self.send_stop_signal();

        if let Some(mut force) = force {
            let h = Box::pin(join_handle);
            let f = Box::pin(force.recv());

            match futures::future::select(f, h).await {
                Either::Left((_x, h)) => {
                    info!("{}: received force shutdown signal", self.service_name);
                    h.abort();
                }
                Either::Right((_, _)) => {
                    info!("Done: {}: graceful shutdown", self.service_name);
                }
            }
        } else {
            info!(
                "{}: force is None, wait for join handle for ever",
                self.service_name
            );

            let res = join_handle.await;

            info!(
                "Done: {}: waiting for join handle for ever, res: {:?}",
                self.service_name, res
            );
        }
    }

    pub async fn shutdown(&mut self, graceful: bool) {
        if graceful {
            if let Some(abort_handle) = self.abort_handle.take() {
                let _ = abort_handle.send(());
            }
            if let Some(join_handle) = self.join_handle.take() {
                if let Err(error) = join_handle.await {
                    error!(
                        "Unexpected error during shutdown Http Server {}. cause {}",
                        self.service_name, error
                    );
                }
            }

            if let Some(join_handle) = self.join_handle.take() {
                if let Err(_err) = tokio::time::timeout(Duration::from_secs(5), join_handle).await {
                    error!("Timeout during shutdown Http Server {}", self.service_name);
                }
            }
        } else if let Some(join_handle) = self.join_handle.take() {
            join_handle.abort();
        }
    }
}
