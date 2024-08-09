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
use std::time::Instant;

use async_trait::async_trait;
use async_trait::unboxed_simple;
use backoff::backoff::Backoff;
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
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
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_txn::TxnManagerRef;
use log::debug;
use log::error;
use log::info;

use crate::operations::set_backoff;
use crate::operations::AppendGenerator;
use crate::operations::CommitMeta;
use crate::operations::SnapshotGenerator;
use crate::operations::TransformMergeCommitMeta;
use crate::FuseTable;

pub struct CommitMultiTableInsert {
    commit_metas: HashMap<u64, CommitMeta>,
    tables: HashMap<u64, Arc<dyn Table>>,
    ctx: Arc<dyn TableContext>,
    overwrite: bool,
    update_stream_meta: Vec<UpdateStreamMetaReq>,
    deduplicated_label: Option<String>,
    catalog: Arc<dyn Catalog>,
    table_meta_timestampss: HashMap<u64, TableMetaTimestamps>,
}

impl CommitMultiTableInsert {
    pub fn create(
        tables: HashMap<u64, Arc<dyn Table>>,
        ctx: Arc<dyn TableContext>,
        overwrite: bool,
        update_stream_meta: Vec<UpdateStreamMetaReq>,
        deduplicated_label: Option<String>,
        catalog: Arc<dyn Catalog>,
        table_meta_timestampss: HashMap<u64, TableMetaTimestamps>,
    ) -> Self {
        Self {
            commit_metas: Default::default(),
            tables,
            ctx,
            overwrite,
            update_stream_meta,
            deduplicated_label,
            catalog,
            table_meta_timestampss,
        }
    }
}

#[async_trait]
impl AsyncSink for CommitMultiTableInsert {
    const NAME: &'static str = "CommitMultiTableInsert";

    #[async_backtrace::framed]
    async fn on_finish(&mut self) -> Result<()> {
        let mut update_table_metas = Vec::with_capacity(self.commit_metas.len());
        let mut snapshot_generators = HashMap::with_capacity(self.commit_metas.len());
        for (table_id, commit_meta) in std::mem::take(&mut self.commit_metas).into_iter() {
            // generate snapshot
            let mut snapshot_generator = AppendGenerator::new(self.ctx.clone(), self.overwrite);
            snapshot_generator.set_conflict_resolve_context(commit_meta.conflict_resolve_context);
            let table = self.tables.get(&table_id).unwrap();
            update_table_metas.push((
                build_update_table_meta_req(
                    table.as_ref(),
                    &snapshot_generator,
                    self.ctx.txn_mgr(),
                    *self.table_meta_timestampss.get(&table.get_id()).unwrap(),
                )
                .await?,
                table.get_table_info().clone(),
            ));
            snapshot_generators.insert(table_id, snapshot_generator);
        }

        let mut backoff = set_backoff(None, None, None);
        let mut retries = 0;

        loop {
            let update_multi_table_meta_req = UpdateMultiTableMetaReq {
                update_table_metas: update_table_metas.clone(),
                copied_files: vec![],
                update_stream_metas: self.update_stream_meta.clone(),
                deduplicated_labels: self.deduplicated_label.clone().into_iter().collect(),
            };

            let update_meta_result = match self
                .catalog
                .retryable_update_multi_table_meta(update_multi_table_meta_req)
                .await
            {
                Ok(ret) => ret,
                Err(e) => {
                    // other errors may occur, especially the version mismatch of streams,
                    // let's log it here for the convenience of diagnostics
                    error!(
                        "Non-recoverable fault occurred during updating tables. {}",
                        e
                    );
                    return Err(e);
                }
            };

            let Err(update_failed_tbls) = update_meta_result else {
                let table_descriptions = self
                    .tables
                    .values()
                    .map(|tbl| {
                        let table_info = tbl.get_table_info();
                        (&table_info.desc, &table_info.ident, &table_info.meta.engine)
                    })
                    .collect::<Vec<_>>();
                let stream_descriptions = self
                    .update_stream_meta
                    .iter()
                    .map(|s| (s.stream_id, s.seq, "stream"))
                    .collect::<Vec<_>>();
                info!(
                    "update tables success (auto commit), tables updated {:?}, streams updated {:?}",
                    table_descriptions, stream_descriptions
                );

                return Ok(());
            };
            let update_failed_tbl_descriptions: Vec<_> = update_failed_tbls
                .iter()
                .map(|(tid, seq, meta)| {
                    let tbl_info = self.tables.get(tid).unwrap().get_table_info();
                    (&tbl_info.desc, (tid, seq), &meta.engine)
                })
                .collect();
            match backoff.next_backoff() {
                Some(duration) => {
                    retries += 1;

                    debug!(
                        "Failed(temporarily) to update tables: {:?}, the commit process of multi-table insert will be retried after {} ms, retrying {} times",
                        update_failed_tbl_descriptions,
                        duration.as_millis(),
                        retries,
                    );
                    databend_common_base::base::tokio::time::sleep(duration).await;
                    for (tid, seq, meta) in update_failed_tbls {
                        let table = self.tables.get_mut(&tid).unwrap();
                        *table = table
                            .refresh_with_seq_meta(self.ctx.as_ref(), seq, meta)
                            .await?;
                        for (req, _) in update_table_metas.iter_mut() {
                            if req.table_id == tid {
                                *req = build_update_table_meta_req(
                                    table.as_ref(),
                                    snapshot_generators.get(&tid).unwrap(),
                                    self.ctx.txn_mgr(),
                                    *self.table_meta_timestampss.get(&tid).unwrap(),
                                )
                                .await?;
                                break;
                            }
                        }
                    }
                }
                None => {
                    let err_msg = format!(
                        "Can not fulfill the tx after retries({} times, {} ms), aborted. updated tables {:?}",
                        retries,
                        Instant::now()
                            .duration_since(backoff.start_time)
                            .as_millis(),
                        update_failed_tbl_descriptions,
                    );
                    error!("{}", err_msg);
                    return Err(ErrorCode::OCCRetryFailure(err_msg));
                }
            }
        }
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

async fn build_update_table_meta_req(
    table: &dyn Table,
    snapshot_generator: &AppendGenerator,
    txn_mgr: TxnManagerRef,
    table_meta_timestamps: TableMetaTimestamps,
) -> Result<UpdateTableMetaReq> {
    let fuse_table = FuseTable::try_from_table(table)?;
    let previous = fuse_table.read_table_snapshot().await?;
    let snapshot = snapshot_generator.generate_new_snapshot(
        table.schema().as_ref().clone(),
        fuse_table.cluster_key_meta.clone(),
        previous,
        Some(fuse_table.table_info.ident.seq),
        txn_mgr,
        table.get_id(),
        table_meta_timestamps,
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
    };
    Ok(req)
}
