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

use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_license::license::Feature::Vacuum;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_pipeline_core::query_spill_prefix;
use databend_common_sql::plans::VacuumTemporaryFilesPlan;
use databend_enterprise_vacuum_handler::get_vacuum_handler;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct VacuumTemporaryFilesInterpreter {
    ctx: Arc<QueryContext>,
    plan: VacuumTemporaryFilesPlan,
}

impl VacuumTemporaryFilesInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: VacuumTemporaryFilesPlan) -> Result<Self> {
        Ok(VacuumTemporaryFilesInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for VacuumTemporaryFilesInterpreter {
    fn name(&self) -> &str {
        "VacuumTemporaryFiles"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Vacuum)?;

        let handler = get_vacuum_handler();

        let temporary_files_prefix = query_spill_prefix(self.ctx.get_tenant().tenant_name(), "");
        let removed_files = handler
            .do_vacuum_temporary_files(
                temporary_files_prefix,
                self.plan.retain,
                self.plan.limit.map(|x| x as usize).unwrap_or(usize::MAX),
            )
            .await?;

        PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
            UInt64Type::from_data(vec![removed_files as u64]),
        ])])
    }
}
