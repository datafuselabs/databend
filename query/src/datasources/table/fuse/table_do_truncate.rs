//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::sync::Arc;

use backoff::ExponentialBackoff;
use common_context::IOContext;
use common_context::TableIOContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::MetaVersion;
use common_planners::TruncateTablePlan;
use uuid::Uuid;

use crate::catalogs::Catalog;
use crate::catalogs::Table;
use crate::datasources::table::fuse::io;
use crate::datasources::table::fuse::meta::TBL_OPT_KEY_SNAPSHOT_LOC;
use crate::datasources::table::fuse::FuseTable;
use crate::datasources::table::fuse::TableSnapshot;
use crate::sessions::DatabendQueryContext;

impl FuseTable {
    #[inline]
    pub async fn do_truncate(
        &self,
        io_ctx: Arc<TableIOContext>,
        _truncate_plan: TruncateTablePlan,
    ) -> Result<()> {
        let version_wrapper = common_base::tokio::sync::Mutex::new(self.table_info.version);
        let backoff = ExponentialBackoff::default();
        backoff::future::retry(backoff, || self.commit_truncate(&version_wrapper, &io_ctx)).await?;
        Ok(())
    }

    async fn commit_truncate(
        &self,
        version_wrapper: &common_base::tokio::sync::Mutex<MetaVersion>,
        io_ctx: &Arc<TableIOContext>,
    ) -> std::result::Result<(), backoff::Error<ErrorCode>> {
        let mut version = version_wrapper.lock().await;
        let prev_snapshot = self.table_snapshot_with_version(*version, io_ctx)?;
        let prev_id = prev_snapshot.map_or_else(|| None, |s| Some(s.snapshot_id));
        // "clear" segments and stats
        let new_snapshot = TableSnapshot {
            snapshot_id: Uuid::new_v4(),
            prev_snapshot_id: prev_id,
            schema: self.table_info.schema.as_ref().clone(),
            summary: Default::default(),
            segments: vec![],
        };

        // save new snapshot
        let ctx: Arc<DatabendQueryContext> = io_ctx
            .get_user_data()?
            .expect("DatabendQueryContext should not be None");
        let new_snapshot_loc =
            io::snapshot_location(new_snapshot.snapshot_id.to_simple().to_string());
        let da = io_ctx.get_data_accessor()?;
        let bytes = serde_json::to_vec(&new_snapshot).map_err(ErrorCode::from_std_error)?;
        da.put(&new_snapshot_loc, bytes).await?;

        // update table snapshot location
        let catalog = ctx.get_catalog();
        let table_id = self.get_id();
        match catalog.upsert_table_option(
            table_id,
            *version,
            TBL_OPT_KEY_SNAPSHOT_LOC.to_string(),
            new_snapshot_loc,
        ) {
            Err(err) => {
                if err.code() == ErrorCode::TableVersionMissMatch("").code() {
                    let table = catalog
                        .get_table(self.table_info.db.as_str(), self.table_info.name.as_str())?;
                    *version = table.get_table_info().version;
                    Err(backoff::Error::Transient(err))
                } else {
                    Err(backoff::Error::Permanent(err))
                }
            }
            Ok(_) => Ok(()),
        }
    }
}
