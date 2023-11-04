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

use common_base::base::tokio;
use common_base::base::tokio::net::TcpStream;
use common_base::base::tokio::task::JoinHandle;
use common_base::runtime::Runtime;
use common_base::runtime::TrySpawn;
use common_base::GLOBAL_TASK;
use common_exception::ErrorCode;
use common_exception::Result;
use futures::future::AbortHandle;
use futures::future::AbortRegistration;
use futures::future::Abortable;
use futures::StreamExt;
use log::error;
use log::info;
use log::warn;
use opensrv_mysql::*;
use rustls::ServerConfig;
use socket2::SockRef;
use socket2::TcpKeepalive;
use tokio_stream::wrappers::TcpListenerStream;

use crate::servers::mysql::mysql_session::MySQLConnection;
use crate::servers::mysql::reject_connection::RejectConnection;
use crate::servers::mysql::tls::MySQLTlsConfig;
use crate::servers::server::ListeningStream;
use crate::servers::server::Server;
use crate::sessions::SessionManager;
use crate::sessions::SessionType;

pub struct MySQLHandler {
    abort_handle: AbortHandle,
    abort_registration: Option<AbortRegistration>,
    join_handle: Option<JoinHandle<()>>,
    keepalive: TcpKeepalive,
    tls: Option<Arc<ServerConfig>>,
}

impl MySQLHandler {
    pub fn create(
        tcp_keepalive_timeout_secs: u64,
        tls_config: MySQLTlsConfig,
    ) -> Result<Box<dyn Server>> {
        let (abort_handle, registration) = AbortHandle::new_pair();
        let keepalive = TcpKeepalive::new()
            .with_time(std::time::Duration::from_secs(tcp_keepalive_timeout_secs));
        let tls = tls_config.setup()?.map(Arc::new);

        Ok(Box::new(MySQLHandler {
            abort_handle,
            abort_registration: Some(registration),
            join_handle: None,
            keepalive,
            tls,
        }))
    }

    #[async_backtrace::framed]
    async fn listener_tcp(listening: SocketAddr) -> Result<(TcpListenerStream, SocketAddr)> {
        let listener = tokio::net::TcpListener::bind(listening)
            .await
            .map_err(|e| {
                ErrorCode::TokioError(format!("{{{}:{}}} {}", listening.ip(), listening.port(), e))
            })?;
        let listener_addr = listener.local_addr()?;
        Ok((TcpListenerStream::new(listener), listener_addr))
    }

    fn listen_loop(&self, stream: ListeningStream, rt: Arc<Runtime>) -> impl Future<Output = ()> {
        let keepalive = self.keepalive.clone();
        let tls = self.tls.clone();

        stream.for_each(move |accept_socket| {
            let tls = tls.clone();
            let keepalive = keepalive.clone();
            let executor = rt.clone();
            let sessions = SessionManager::instance();
            async move {
                match accept_socket {
                    Err(error) => error!("Broken session connection: {}", error),
                    Ok(socket) => {
                        MySQLHandler::accept_socket(sessions, executor, socket, keepalive, tls)
                    }
                };
            }
        })
    }

    fn accept_socket(
        sessions: Arc<SessionManager>,
        executor: Arc<Runtime>,
        socket: TcpStream,
        keepalive: TcpKeepalive,
        tls: Option<Arc<ServerConfig>>,
    ) {
        executor.spawn(GLOBAL_TASK, async move {
            match sessions.create_session(SessionType::MySQL).await {
                Err(error) => {
                    warn!("create session failed, {:?}", error);
                    Self::reject_session(socket, error).await
                }
                Ok(session) => {
                    info!("MySQL connection coming: {:?}", socket.peer_addr());

                    // TcpStream must implement AsFd for socket2 0.5, wait https://github.com/tokio-rs/tokio/pull/5514
                    if let Err(e) = SockRef::from(&socket).set_tcp_keepalive(&keepalive) {
                        warn!("failed to set socket option keepalive {}", e);
                    }

                    if let Err(error) = MySQLConnection::run_on_stream(session, socket, tls) {
                        error!("Unexpected error occurred during query: {:?}", error);
                    };
                }
            }
        });
    }

    #[async_backtrace::framed]
    async fn reject_session(stream: TcpStream, error: ErrorCode) {
        let (kind, message) = match error.code() {
            41 => (ErrorKind::ER_TOO_MANY_USER_CONNECTIONS, error.message()),
            _ => (ErrorKind::ER_INTERNAL_ERROR, error.message()),
        };

        if let Err(error) = RejectConnection::reject_mysql_connection(stream, kind, message).await {
            error!(
                "Unexpected error occurred during reject connection: {:?}",
                error
            );
        }
    }
}

#[async_trait::async_trait]
impl Server for MySQLHandler {
    #[async_backtrace::framed]
    async fn shutdown(&mut self, graceful: bool) {
        if !graceful {
            return;
        }

        self.abort_handle.abort();

        if let Some(join_handle) = self.join_handle.take() {
            if let Err(error) = join_handle.await {
                error!(
                    "Unexpected error during shutdown MySQLHandler. cause {}",
                    error
                );
            }
        }
    }

    #[async_backtrace::framed]
    async fn start(&mut self, listening: SocketAddr) -> Result<SocketAddr> {
        match self.abort_registration.take() {
            None => Err(ErrorCode::Internal("MySQLHandler already running.")),
            Some(registration) => {
                let rejected_rt = Arc::new(Runtime::with_worker_threads(
                    1,
                    Some("mysql-handler".to_string()),
                )?);
                let (stream, listener) = Self::listener_tcp(listening).await?;
                let stream = Abortable::new(stream, registration);
                self.join_handle = Some(Runtime::spawn_current_runtime(
                    GLOBAL_TASK,
                    async_backtrace::location!().frame(self.listen_loop(stream, rejected_rt)),
                ));
                Ok(listener)
            }
        }
    }
}
