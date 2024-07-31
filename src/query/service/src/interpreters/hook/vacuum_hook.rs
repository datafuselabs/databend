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
use std::time::Duration;

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_license::license::Feature::Vacuum;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_pipeline_core::query_spill_prefix;
use databend_common_storage::DataOperator;
use databend_enterprise_vacuum_handler::get_vacuum_handler;
use opendal::Buffer;

use crate::sessions::QueryContext;

pub fn hook_vacuum_temp_files(query_ctx: &Arc<QueryContext>) -> Result<()> {
    let tenant = query_ctx.get_tenant();
    let settings = query_ctx.get_settings();
    let spill_prefix = query_spill_prefix(tenant.tenant_name(), &query_ctx.get_id());
    let vacuum_limit = settings.get_max_vacuum_temp_files_after_query()?;

    // disable all s3 operator if vacuum limit = 0
    if vacuum_limit != 0
        && LicenseManagerSwitch::instance()
            .check_enterprise_enabled(query_ctx.get_license_key(), Vacuum)
            .is_ok()
    {
        let handler = get_vacuum_handler();

        let _ = GlobalIORuntime::instance().block_on(async move {
            let removed_files = handler
                .do_vacuum_temporary_files(
                    spill_prefix.clone(),
                    Some(Duration::from_secs(0)),
                    vacuum_limit as usize,
                )
                .await;

            if vacuum_limit != 0 && matches!(removed_files, Ok(res) if res == vacuum_limit as usize)
            {
                // Have not been removed files
                let op = DataOperator::instance().operator();
                op.create_dir(&format!("{}/", spill_prefix)).await?;
                op.write(&format!("{}/finished", spill_prefix), Buffer::new())
                    .await?;
            }

            Ok(())
        });
    }

    Ok(())
}
