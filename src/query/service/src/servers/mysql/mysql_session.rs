// Copyright 2021 Datafuse Labs.
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

use std::net::Shutdown;
use std::sync::Arc;

use common_base::base::tokio::io::BufWriter;
use common_base::base::tokio::net::TcpStream;
use common_base::block_on;
use common_base::runtime::Runtime;
use common_base::runtime::Thread;
use common_base::runtime::TrySpawn;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::ToErrorCode;
use opensrv_mysql::AsyncMysqlIntermediary;
use opensrv_mysql::IntermediaryOptions;
use tracing::error;
use tracing::warn;

use crate::servers::mysql::mysql_interactive_worker::InteractiveWorker;
use crate::sessions::Session;

// default size of resultset write buffer: 100KB
const DEFAULT_RESULT_SET_WRITE_BUFFER_SIZE: usize = 100 * 1024;

pub struct MySQLConnection;

impl MySQLConnection {
    pub fn run_on_stream(session: Arc<Session>, stream: TcpStream) -> Result<()> {
        let blocking_stream = Self::convert_stream(stream)?;
        MySQLConnection::attach_session(&session, &blocking_stream)?;

        let non_blocking_stream = TcpStream::from_std(blocking_stream)?;
        let query_executor =
            Runtime::with_worker_threads(1, Some("mysql-query-executor".to_string()), false)?;
        Thread::spawn(move || {
            let join_handle = query_executor.spawn(async move {
                let client_addr = match non_blocking_stream.peer_addr() {
                    Ok(addr) => addr.to_string(),
                    Err(e) => {
                        warn!(
                            "Failed to get mysql conn peer address for {:?}: {}",
                            non_blocking_stream, e
                        );
                        return Ok(());
                    }
                };

                let interactive_worker = InteractiveWorker::create(session, client_addr);
                let opts = IntermediaryOptions {
                    process_use_statement_on_query: true,
                };
                let (r, w) = non_blocking_stream.into_split();
                let w = BufWriter::with_capacity(DEFAULT_RESULT_SET_WRITE_BUFFER_SIZE, w);
                AsyncMysqlIntermediary::run_with_options(interactive_worker, r, w, &opts).await
            });

            let _ = block_on(join_handle);
        });
        Ok(())
    }

    fn attach_session(session: &Arc<Session>, blocking_stream: &std::net::TcpStream) -> Result<()> {
        let host = blocking_stream.peer_addr().ok();
        let blocking_stream_ref = blocking_stream.try_clone()?;
        session.attach(host, move || {
            if let Err(error) = blocking_stream_ref.shutdown(Shutdown::Both) {
                error!("Cannot shutdown MySQL session io {}", error);
            }
        });

        Ok(())
    }

    // TODO: move to ToBlockingStream trait
    fn convert_stream(stream: TcpStream) -> Result<std::net::TcpStream> {
        let stream = stream.into_std().map_err_to_code(
            ErrorCode::TokioError,
            || "Cannot to convert Tokio TcpStream to Std TcpStream",
        )?;
        stream.set_nonblocking(false).map_err_to_code(
            ErrorCode::TokioError,
            || "Cannot to convert Tokio TcpStream to Std TcpStream",
        )?;

        Ok(stream)
    }
}
