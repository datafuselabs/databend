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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_sql::plans::KillPlan;

use crate::clusters::ClusterHelper;
use crate::clusters::FlightParams;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::servers::flight::v1::actions::KILL_QUERY;
use crate::sessions::QueriesQueueManager;
use crate::sessions::QueryContext;

pub struct KillInterpreter {
    ctx: Arc<QueryContext>,
    plan: KillPlan,
    proxy_to_cluster: bool,
}

impl KillInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: KillPlan) -> Result<Self> {
        Ok(KillInterpreter {
            ctx,
            plan,
            proxy_to_cluster: true,
        })
    }

    pub fn from_flight(ctx: Arc<QueryContext>, plan: KillPlan) -> Result<Self> {
        Ok(KillInterpreter {
            ctx,
            plan,
            proxy_to_cluster: false,
        })
    }

    #[async_backtrace::framed]
    async fn kill_cluster_query(&self) -> Result<PipelineBuildResult> {
        let cluster = self.ctx.get_cluster();
        let settings = self.ctx.get_settings();

        let mut message = HashMap::with_capacity(cluster.nodes.len());

        for node_info in &cluster.nodes {
            if node_info.id != cluster.local_id {
                message.insert(node_info.id.clone(), self.plan.clone());
            }
        }
        let flight_params = FlightParams {
            timeout: settings.get_flight_client_timeout()?,
            retry_times: settings.get_max_flight_retry_times()?,
            retry_interval: settings.get_flight_retry_interval()?,
        };

        let res = cluster
            .do_action::<_, bool>(KILL_QUERY, message, flight_params)
            .await?;

        match res.values().any(|x| *x) {
            true => Ok(PipelineBuildResult::create()),
            false => Err(ErrorCode::UnknownSession(format!(
                "Not found session id {}",
                self.plan.id
            ))),
        }
    }

    #[async_backtrace::framed]
    async fn execute_kill(&self, session_id: &String) -> Result<PipelineBuildResult> {
        match self.ctx.get_session_by_id(session_id) {
            None => match self.proxy_to_cluster {
                true => self.kill_cluster_query().await,
                false => Err(ErrorCode::UnknownSession(format!(
                    "Not found session id {}",
                    session_id
                ))),
            },
            Some(kill_session) if self.plan.kill_connection => {
                if let Some(query_id) = kill_session.get_current_query_id() {
                    if QueriesQueueManager::instance().remove(query_id) {
                        return Ok(PipelineBuildResult::create());
                    }
                }

                kill_session.force_kill_session();
                Ok(PipelineBuildResult::create())
            }
            Some(kill_session) => {
                if let Some(query_id) = kill_session.get_current_query_id() {
                    if QueriesQueueManager::instance().remove(query_id) {
                        return Ok(PipelineBuildResult::create());
                    }
                }

                kill_session.force_kill_query(ErrorCode::AbortedQuery(
                    "Aborted query, because the server is shutting down or the query was killed",
                ));
                Ok(PipelineBuildResult::create())
            }
        }
    }
}

#[async_trait::async_trait]
impl Interpreter for KillInterpreter {
    fn name(&self) -> &str {
        "KillInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let id = &self.plan.id;
        // If press Ctrl + C, MySQL Client will create a new session and send query
        // `kill query mysql_connection_id` to server.
        // the type of connection_id is u32, if parse success get session by connection_id,
        // otherwise use the session_id.
        // More info Link to: https://github.com/datafuselabs/databend/discussions/5405.
        match id.parse::<u32>() {
            Ok(mysql_conn_id) => match self.ctx.get_id_by_mysql_conn_id(&Some(mysql_conn_id)) {
                Some(get) => self.execute_kill(&get).await,
                None => Err(ErrorCode::UnknownSession(format!(
                    "MySQL connection id {} not found session id",
                    mysql_conn_id
                ))),
            },
            Err(_) => self.execute_kill(id).await,
        }
    }
}
