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
use databend_common_sql::plans::SetPriorityPlan;

use crate::clusters::ClusterHelper;
use crate::clusters::FlightParams;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::servers::flight::v1::actions::SET_PRIORITY;
use crate::sessions::QueryContext;

pub struct SetPriorityInterpreter {
    ctx: Arc<QueryContext>,
    plan: SetPriorityPlan,
    proxy_to_cluster: bool,
}

impl SetPriorityInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: SetPriorityPlan) -> Result<Self> {
        Ok(SetPriorityInterpreter {
            ctx,
            plan,
            proxy_to_cluster: true,
        })
    }

    pub fn from_flight(ctx: Arc<QueryContext>, plan: SetPriorityPlan) -> Result<Self> {
        Ok(SetPriorityInterpreter {
            ctx,
            plan,
            proxy_to_cluster: false,
        })
    }

    #[async_backtrace::framed]
    async fn set_cluster_priority(&self) -> Result<PipelineBuildResult> {
        let cluster = self.ctx.get_cluster();

        let mut message = HashMap::with_capacity(cluster.nodes.len());
        for node_info in &cluster.nodes {
            if node_info.id != cluster.local_id {
                message.insert(node_info.id.clone(), self.plan.clone());
            }
        }

        let settings = self.ctx.get_settings();
        let flight_params = FlightParams {
            timeout: settings.get_flight_client_timeout()?,
            retry_times: settings.get_max_flight_retry_times()?,
            retry_interval: settings.get_flight_retry_interval()?,
        };
        let res = cluster
            .do_action::<_, bool>(SET_PRIORITY, message, flight_params)
            .await?;

        match res.values().any(|x| *x) {
            true => Ok(PipelineBuildResult::create()),
            false => Err(ErrorCode::UnknownSession(format!(
                "Not found session id {}",
                self.plan.id
            ))),
        }
    }
}

#[async_trait::async_trait]
impl Interpreter for SetPriorityInterpreter {
    fn name(&self) -> &str {
        "SetPriorityInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let id = &self.plan.id;
        match self.ctx.get_session_by_id(id) {
            None => match self.proxy_to_cluster {
                true => self.set_cluster_priority().await,
                false => Err(ErrorCode::UnknownSession(format!(
                    "Not found session id {}",
                    id
                ))),
            },
            Some(set_session) => {
                set_session.set_query_priority(self.plan.priority);
                Ok(PipelineBuildResult::create())
            }
        }
    }
}
