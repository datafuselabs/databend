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
use databend_common_exception::set_backtrace;
use databend_common_exception::Result;
use databend_common_sql::plans::SystemAction;
use databend_common_sql::plans::SystemPlan;

use crate::clusters::ClusterHelper;
use crate::clusters::FlightParams;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::servers::flight::v1::actions::SYSTEM_ACTION;
use crate::sessions::QueryContext;

pub struct SystemActionInterpreter {
    ctx: Arc<QueryContext>,
    plan: SystemPlan,
    proxy_to_cluster: bool,
}

impl SystemActionInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: SystemPlan) -> Result<Self> {
        Ok(SystemActionInterpreter {
            ctx,
            plan,
            proxy_to_cluster: true,
        })
    }

    pub fn from_flight(ctx: Arc<QueryContext>, plan: SystemPlan) -> Result<Self> {
        Ok(SystemActionInterpreter {
            ctx,
            plan,
            proxy_to_cluster: false,
        })
    }
}

#[async_trait::async_trait]
impl Interpreter for SystemActionInterpreter {
    fn name(&self) -> &str {
        "SystemActionInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        if self.proxy_to_cluster {
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
            cluster
                .do_action::<_, ()>(SYSTEM_ACTION, message, flight_params)
                .await?;
        }

        match self.plan.action {
            SystemAction::Backtrace(switch) => {
                set_backtrace(switch);
            }
        }
        Ok(PipelineBuildResult::create())
    }
}
