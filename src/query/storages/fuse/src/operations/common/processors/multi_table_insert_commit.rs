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

use async_trait::async_trait;
use async_trait::unboxed_simple;
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_meta_app::schema::UpdateMultiTableMetaReq;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_meta_types::MatchSeq;
use databend_common_pipeline_sinks::AsyncSink;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::Versioned;

use super::TransformMergeCommitMeta;
use crate::operations::common::CommitMeta;
use crate::operations::AppendGenerator;
use crate::operations::SnapshotGenerator;
use crate::FuseTable;
pub struct CommitMultiTableInsert {
    commit_metas: HashMap<u64, CommitMeta>,
    tables: HashMap<u64, Arc<dyn Table>>,
    ctx: Arc<dyn TableContext>,
    overwrite: bool,
    update_stream_meta: Vec<UpdateStreamMetaReq>,
    deduplicated_label: Option<String>,
    catalog: Arc<dyn Catalog>,
}

impl CommitMultiTableInsert {
    pub fn create(
        tables: HashMap<u64, Arc<dyn Table>>,
        ctx: Arc<dyn TableContext>,
        overwrite: bool,
        update_stream_meta: Vec<UpdateStreamMetaReq>,
        deduplicated_label: Option<String>,
        catalog: Arc<dyn Catalog>,
    ) -> Self {
        Self {
            commit_metas: Default::default(),
            tables,
            ctx,
            overwrite,
            update_stream_meta,
            deduplicated_label,
            catalog,
        }
    }
}

#[async_trait]
impl AsyncSink for CommitMultiTableInsert {
    const NAME: &'static str = "CommitMultiTableInsert";

    #[async_backtrace::framed]
    async fn on_finish(&mut self) -> Result<()> {
        let mut update_table_meta_reqs = Vec::with_capacity(self.commit_metas.len());
        let mut table_infos = Vec::with_capacity(self.commit_metas.len());
        for (table_id, commit_meta) in std::mem::take(&mut self.commit_metas).into_iter() {
            // generate snapshot
            let mut snapshot_generator = AppendGenerator::new(self.ctx.clone(), self.overwrite);
            snapshot_generator.set_conflict_resolve_context(commit_meta.conflict_resolve_context);
            let table = self.tables.get(&table_id).unwrap();
            let fuse_table = FuseTable::try_from_table(table.as_ref())?;
            let previous = fuse_table.read_table_snapshot().await?;
            let snapshot = snapshot_generator.generate_new_snapshot(
                table.schema().as_ref().clone(),
                fuse_table.cluster_key_meta.clone(),
                previous,
                Some(fuse_table.table_info.ident.seq),
            )?;

            // write snapshot
            let dal = fuse_table.get_operator();
            let location_generator = &fuse_table.meta_location_generator;
            let location = location_generator
                .snapshot_location_from_uuid(&snapshot.snapshot_id, TableSnapshot::VERSION)?;
            dal.write(&location, snapshot.to_bytes()?).await?;

            // build new table meta
            let new_table_meta =
                FuseTable::build_new_table_meta(&fuse_table.table_info.meta, &location, &snapshot)?;
            let table_id = fuse_table.table_info.ident.table_id;
            let table_version = fuse_table.table_info.ident.seq;

            let req = UpdateTableMetaReq {
                table_id,
                seq: MatchSeq::Exact(table_version),
                new_table_meta,
                copied_files: None,
                deduplicated_label: None,
                update_stream_meta: vec![],
            };
            update_table_meta_reqs.push(req);
            table_infos.push(table.get_table_info());
        }
        let is_active = self.ctx.txn_mgr().lock().is_active();
        match is_active {
            true => {
                update_table_meta_reqs[0].update_stream_meta =
                    std::mem::take(&mut self.update_stream_meta);
                update_table_meta_reqs[0].deduplicated_label = self.deduplicated_label.clone();
                for (req, info) in update_table_meta_reqs
                    .into_iter()
                    .zip(table_infos.into_iter())
                {
                    self.catalog.update_table_meta(info, req).await?;
                }
            }
            false => {
                let update_multi_table_meta_req = UpdateMultiTableMetaReq {
                    update_table_metas: update_table_meta_reqs,
                    copied_files: vec![],
                    update_stream_metas: std::mem::take(&mut self.update_stream_meta),
                    deduplicated_labels: self.deduplicated_label.clone().into_iter().collect(),
                };
                self.catalog
                    .update_multi_table_meta(update_multi_table_meta_req)
                    .await?;
            }
        }
        Ok(())
    }

    #[unboxed_simple]
    #[async_backtrace::framed]
    async fn consume(&mut self, data_block: DataBlock) -> Result<bool> {
        let input_meta = data_block
            .get_meta()
            .cloned()
            .ok_or_else(|| ErrorCode::Internal("No block meta. It's a bug"))?;

        let meta = CommitMeta::downcast_from(input_meta)
            .ok_or_else(|| ErrorCode::Internal("No commit meta. It's a bug"))?;
        match self.commit_metas.get_mut(&meta.table_id) {
            Some(m) => {
                let table = self.tables.get(&meta.table_id).unwrap();
                let table = FuseTable::try_from_table(table.as_ref()).unwrap();
                *m = TransformMergeCommitMeta::merge_commit_meta(
                    m.clone(),
                    meta,
                    table.cluster_key_id(),
                );
            }
            None => {
                self.commit_metas.insert(meta.table_id, meta);
            }
        }
        Ok(false)
    }
}
