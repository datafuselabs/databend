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

use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_meta_app::schema::TableStatistics;
use common_meta_app::schema::TruncateTableReq;
use common_meta_app::schema::UpdateTableMetaReq;
use common_meta_types::MatchSeq;
use storages_common_table_meta::meta::TableSnapshot;
use storages_common_table_meta::meta::Versioned;
use storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;

use crate::FuseTable;

impl FuseTable {
    #[inline]
    #[async_backtrace::framed]
    pub async fn do_truncate(&self, ctx: Arc<dyn TableContext>, purge: bool) -> Result<()> {
        if let Some(prev_snapshot) = self.read_table_snapshot().await? {
            // 1. prepare new snapshot
            let prev_id = prev_snapshot.snapshot_id;
            let prev_format_version = self.snapshot_format_version(None).await?;
            let new_snapshot = TableSnapshot::new(
                &prev_snapshot.timestamp,
                Some((prev_id, prev_format_version, prev_snapshot.table_version)),
                Some(self.current_table_version()),
                prev_snapshot.schema.clone(),
                Default::default(),
                vec![],
                self.cluster_key_meta.clone(),
                // truncate MUST reset ts location
                None,
            );

            // 2. write down new snapshot
            let loc = self.meta_location_generator();
            let new_snapshot_loc = loc.gen_snapshot_location(
                &new_snapshot.snapshot_id,
                TableSnapshot::VERSION,
                new_snapshot.table_version,
            )?;
            let bytes = new_snapshot.to_bytes()?;
            self.operator.write(&new_snapshot_loc, bytes).await?;

            // 3. commit new meta to meta server
            let mut new_table_meta = self.table_info.meta.clone();

            // update snapshot location
            new_table_meta.options.insert(
                OPT_KEY_SNAPSHOT_LOCATION.to_owned(),
                new_snapshot_loc.clone(),
            );
            // reset table statistics
            new_table_meta.statistics = TableStatistics::default();

            let table_id = self.table_info.ident.table_id;
            let table_version = self.table_info.ident.seq;
            let catalog = ctx.get_catalog(self.table_info.catalog()).await?;

            // commit table meta to meta server.
            // `truncate_table` is not supposed to be retry-able, thus we use
            // `update_data_table_meta` directly.
            catalog
                .update_table_meta(&self.table_info, UpdateTableMetaReq {
                    table_id,
                    seq: MatchSeq::Exact(table_version),
                    new_table_meta,
                    copied_files: None,
                    deduplicated_label: None,
                })
                .await?;

            catalog
                .truncate_table(&self.table_info, TruncateTableReq {
                    table_id,
                    batch_size: None,
                })
                .await?;

            // try keep a hit file of last snapshot
            Self::write_last_snapshot_hint(
                &self.operator,
                &self.meta_location_generator,
                new_snapshot_loc,
            )
            .await;

            // best effort to remove historical data. if failed, let `vacuum` to do the job.
            // TODO: consider remove the `purge` option from `truncate`
            // - it is not a safe operation, there is NO retention interval protection here
            // - it is incompatible with time travel features
            if purge {
                let snapshot_files = self.list_snapshot_files().await?;
                let keep_last_snapshot = false;
                let ret = self
                    .do_purge(&ctx, snapshot_files, None, keep_last_snapshot, false)
                    .await;
                if let Err(e) = ret {
                    return Err(e);
                } else {
                    return Ok(());
                }
            }
        }

        Ok(())
    }
}
