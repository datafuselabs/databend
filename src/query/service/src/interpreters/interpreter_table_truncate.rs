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

use databend_common_catalog::lock::LockTableOption;
use databend_common_catalog::table::TableExt;
use databend_common_exception::Result;
use databend_common_sql::plans::TruncateTablePlan;

use crate::clusters::ClusterHelper;
use crate::clusters::FlightParams;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::servers::flight::v1::actions::TRUNCATE_TABLE;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct TruncateTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: TruncateTablePlan,

    proxy_to_cluster: bool,
}

impl TruncateTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: TruncateTablePlan) -> Result<Self> {
        Ok(TruncateTableInterpreter {
            ctx,
            plan,
            proxy_to_cluster: true,
        })
    }

    pub fn from_flight(ctx: Arc<QueryContext>, plan: TruncateTablePlan) -> Result<Self> {
        Ok(TruncateTableInterpreter {
            ctx,
            plan,
            proxy_to_cluster: false,
        })
    }
}

#[async_trait::async_trait]
impl Interpreter for TruncateTableInterpreter {
    fn name(&self) -> &str {
        "TruncateTableInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        // try add lock table.
        let lock_guard = self
            .ctx
            .clone()
            .acquire_table_lock(
                &self.plan.catalog,
                &self.plan.database,
                &self.plan.table,
                &LockTableOption::LockWithRetry,
            )
            .await?;

        let table = self
            .ctx
            .get_table(&self.plan.catalog, &self.plan.database, &self.plan.table)
            .await?;
        // check mutability
        table.check_mutable()?;

        if self.proxy_to_cluster && table.broadcast_truncate_to_cluster() {
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
                .do_action::<_, ()>(TRUNCATE_TABLE, message, flight_params)
                .await?;
        }

        let mut build_res = PipelineBuildResult::create();
        build_res.main_pipeline.add_lock_guard(lock_guard);
        table
            .truncate(self.ctx.clone(), &mut build_res.main_pipeline)
            .await?;
        Ok(build_res)
    }
}
