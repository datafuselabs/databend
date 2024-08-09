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

use databend_common_catalog::table::NavigationDescriptor;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_meta_types::MatchSeq;

use crate::FuseTable;

impl FuseTable {
    #[async_backtrace::framed]
    pub async fn do_revert_to(
        &self,
        ctx: Arc<dyn TableContext>,
        navigation_descriptor: NavigationDescriptor,
    ) -> Result<()> {
        // 1. try navigate to the point
        let table = self
            .navigate_to_point(
                &navigation_descriptor.point,
                ctx.clone().get_abort_checker(),
            )
            .await?;
        let table_reverting_to = FuseTable::try_from_table(table.as_ref())?;
        let table_info = table_reverting_to.get_table_info();

        // shortcut. if reverting to the same point, just return ok
        if self.snapshot_loc().await? == table_reverting_to.snapshot_loc().await? {
            return Ok(());
        }

        // 2. prepare table meta which being reverted to
        let table_meta_to_be_committed = table_reverting_to.table_info.meta.clone();

        // 3. prepare the request
        //  using the CURRENT version as the base table version
        let base_version = self.table_info.ident.seq;
        let catalog = ctx.get_catalog(table_info.catalog()).await?;
        let table_id = table_info.ident.table_id;
        let req = UpdateTableMetaReq {
            table_id,
            seq: MatchSeq::Exact(base_version),
            new_table_meta: table_meta_to_be_committed,
        };

        // 4. let's roll
        let reply = catalog.update_single_table_meta(req, table_info).await;
        if reply.is_ok() {
            // try keeping the snapshot hit
            let snapshot_location = table_reverting_to.snapshot_loc().await?.ok_or_else(|| {
                    ErrorCode::Internal("internal error, fuse table which navigated to given point has no snapshot location")
                })?;
            Self::write_last_snapshot_hint(
                ctx.as_ref(),
                &table_reverting_to.operator,
                &table_reverting_to.meta_location_generator,
                &snapshot_location,
            )
            .await;
        };

        reply.map(|_| ())
    }
}
