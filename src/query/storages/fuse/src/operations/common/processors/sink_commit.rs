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

use std::any::Any;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use backoff::backoff::Backoff;
use backoff::ExponentialBackoff;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TruncateTableReq;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_common_meta_app::schema::UpsertTableCopiedFileReq;
use databend_common_metrics::storage::*;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_storages_common_table_meta::meta::ClusterKey;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SnapshotId;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::Versioned;
use log::debug;
use log::error;
use log::info;
use log::warn;
use opendal::Operator;

use crate::io::TableMetaLocationGenerator;
use crate::operations::set_backoff;
use crate::operations::AppendGenerator;
use crate::operations::CommitMeta;
use crate::operations::SnapshotGenerator;
use crate::operations::TruncateGenerator;
use crate::operations::TruncateMode;
use crate::FuseTable;
enum State {
    None,
    FillDefault,
    RefreshTable,
    GenerateSnapshot {
        previous: Option<Arc<TableSnapshot>>,
        cluster_key_meta: Option<ClusterKey>,
        table_info: TableInfo,
    },
    TryCommit {
        data: Vec<u8>,
        snapshot: TableSnapshot,
        table_info: TableInfo,
    },
    Abort(ErrorCode),
    Finish,
}

// Gathers all the segments and commits to the meta server.
pub struct CommitSink<F: SnapshotGenerator> {
    state: State,

    input: Arc<InputPort>,

    ctx: Arc<dyn TableContext>,
    dal: Operator,
    location_gen: TableMetaLocationGenerator,

    table: Arc<dyn Table>,
    copied_files: Option<UpsertTableCopiedFileReq>,
    snapshot_gen: F,
    purge: bool,
    retries: u64,
    max_retry_elapsed: Option<Duration>,
    backoff: ExponentialBackoff,

    new_segment_locs: Vec<Location>,
    start_time: Instant,
    prev_snapshot_id: Option<SnapshotId>,

    change_tracking: bool,
    update_stream_meta: Vec<UpdateStreamMetaReq>,
    deduplicated_label: Option<String>,
    table_meta_timestamps: TableMetaTimestamps,
}

impl<F> CommitSink<F>
where F: SnapshotGenerator + Send + 'static
{
    #[allow(clippy::too_many_arguments)]
    pub fn try_create(
        table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        copied_files: Option<UpsertTableCopiedFileReq>,
        update_stream_meta: Vec<UpdateStreamMetaReq>,
        snapshot_gen: F,
        input: Arc<InputPort>,
        max_retry_elapsed: Option<Duration>,
        prev_snapshot_id: Option<SnapshotId>,
        deduplicated_label: Option<String>,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<ProcessorPtr> {
        let purge = Self::do_purge(table, &snapshot_gen);
        Ok(ProcessorPtr::create(Box::new(CommitSink {
            state: State::None,
            ctx,
            dal: table.get_operator(),
            location_gen: table.meta_location_generator.clone(),
            table: Arc::new(table.clone()),
            copied_files,
            snapshot_gen,
            purge,
            backoff: ExponentialBackoff::default(),
            retries: 0,
            max_retry_elapsed,
            input,
            new_segment_locs: vec![],
            start_time: Instant::now(),
            prev_snapshot_id,
            change_tracking: table.change_tracking_enabled(),
            update_stream_meta,
            deduplicated_label,
            table_meta_timestamps,
        })))
    }

    fn is_error_recoverable(&self, e: &ErrorCode) -> bool {
        // When prev_snapshot_id is some, means it is an alter table column modification or truncate.
        // In this case if commit to meta fail and error is TABLE_VERSION_MISMATCHED operation will be aborted.
        if self.prev_snapshot_id.is_some() && e.code() == ErrorCode::TABLE_VERSION_MISMATCHED {
            return false;
        }
        FuseTable::is_error_recoverable(e, self.purge)
    }

    fn read_meta(&mut self) -> Result<Event> {
        self.start_time = Instant::now();
        {
            self.ctx.set_status_info("begin commit");
        }

        let input_meta = self
            .input
            .pull_data()
            .unwrap()?
            .get_meta()
            .cloned()
            .ok_or_else(|| ErrorCode::Internal("No block meta. It's a bug"))?;

        self.input.finish();

        let meta = CommitMeta::downcast_from(input_meta)
            .ok_or_else(|| ErrorCode::Internal("No commit meta. It's a bug"))?;

        self.new_segment_locs = meta.new_segment_locs;

        self.backoff = set_backoff(None, None, self.max_retry_elapsed);

        self.snapshot_gen
            .set_conflict_resolve_context(meta.conflict_resolve_context);
        self.state = State::FillDefault;

        Ok(Event::Async)
    }

    fn do_purge(table: &FuseTable, snapshot_gen: &F) -> bool {
        if table.transient() {
            return true;
        }

        snapshot_gen
            .as_any()
            .downcast_ref::<TruncateGenerator>()
            .is_some_and(|gen| matches!(gen.mode(), TruncateMode::Purge))
    }

    fn do_truncate(&self) -> bool {
        self.snapshot_gen
            .as_any()
            .downcast_ref::<TruncateGenerator>()
            .is_some_and(|gen| !matches!(gen.mode(), TruncateMode::Delete))
    }

    fn is_append_only_txn(&self) -> bool {
        self.snapshot_gen
            .as_any()
            .downcast_ref::<AppendGenerator>()
            .is_some()
    }
}

#[async_trait::async_trait]
impl<F> Processor for CommitSink<F>
where F: SnapshotGenerator + Send + 'static
{
    fn name(&self) -> String {
        "CommitSink".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(
            &self.state,
            State::GenerateSnapshot { .. } | State::Abort(_)
        ) {
            return Ok(Event::Sync);
        }

        if matches!(
            &self.state,
            State::FillDefault | State::TryCommit { .. } | State::RefreshTable
        ) {
            return Ok(Event::Async);
        }

        if matches!(self.state, State::Finish) {
            return Ok(Event::Finished);
        }

        if self.input.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.input.has_data() {
            self.input.set_need_data();
            return Ok(Event::NeedData);
        }

        self.read_meta()
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::None) {
            State::GenerateSnapshot {
                previous,
                cluster_key_meta,
                table_info,
            } => {
                let change_tracking_enabled_during_commit = {
                    let no_change_tracking_at_beginning = !self.change_tracking;
                    // note that `self.table` might be refreshed if commit retried
                    let change_tracking_enabled_now = self.table.change_tracking_enabled();

                    no_change_tracking_at_beginning && change_tracking_enabled_now
                };

                if !self.is_append_only_txn() && change_tracking_enabled_during_commit {
                    // If change tracking is not enabled when the txn start, but is enabled when committing,
                    // then the txn should be aborted.
                    // For mutations other than append-only, stream column values (like _origin_block_id)
                    // must be properly generated. If not, CDC will not function as expected.
                    self.state = State::Abort(ErrorCode::StorageOther(
                        "commit failed because change tracking was enabled during the commit process",
                    ));
                    return Ok(());
                }

                // now:
                // - either current txn IS append-only
                //    even if this is a conflict txn T (in the meaning of table version) has been
                // committed, which has changed the change-tracking state from disabled to enabled,
                // merging with transaction T is still safe, since the CDC mechanism allows it.
                // - or change-tracking state is NOT changed.
                //    in this case, we only need standard conflict resolution.
                // therefore, we can safely proceed.

                let schema = self.table.schema().as_ref().clone();
                match self.snapshot_gen.generate_new_snapshot(
                    schema,
                    cluster_key_meta,
                    previous,
                    Some(table_info.ident.seq),
                    self.ctx.txn_mgr(),
                    table_info.ident.table_id,
                    self.table_meta_timestamps,
                ) {
                    Ok(snapshot) => {
                        self.state = State::TryCommit {
                            data: snapshot.to_bytes()?,
                            snapshot,
                            table_info,
                        };
                    }
                    Err(e) => {
                        self.state = State::Abort(e);
                    }
                }
            }
            State::Abort(e) => {
                let duration = self.start_time.elapsed();
                metrics_inc_commit_aborts();
                metrics_inc_commit_milliseconds(duration.as_millis());
                error!(
                    "transaction aborted after {} retries, which took {} ms, cause: {:?}",
                    self.retries,
                    duration.as_millis(),
                    e
                );
                return Err(e);
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::None) {
            State::FillDefault => {
                let schema = self.table.schema().as_ref().clone();

                let fuse_table = FuseTable::try_from_table(self.table.as_ref())?.to_owned();
                let previous = fuse_table.read_table_snapshot().await?;
                // save current table info when commit to meta server
                // if table_id not match, update table meta will fail
                let table_info = fuse_table.table_info.clone();
                // check if snapshot has been changed
                let snapshot_has_changed = self.prev_snapshot_id.is_some_and(|prev_snapshot_id| {
                    previous
                        .as_ref()
                        .map_or(true, |previous| previous.snapshot_id != prev_snapshot_id)
                });
                if snapshot_has_changed {
                    // if snapshot has changed abort operation
                    self.state = State::Abort(ErrorCode::StorageOther(
                        "commit failed because the snapshot had changed during the commit process",
                    ));
                } else {
                    self.snapshot_gen
                        .fill_default_values(schema, &previous)
                        .await?;

                    self.state = State::GenerateSnapshot {
                        previous,
                        cluster_key_meta: fuse_table.cluster_key_meta.clone(),
                        table_info,
                    };
                }
            }
            State::TryCommit {
                data,
                snapshot,
                table_info,
            } => {
                let location = self
                    .location_gen
                    .snapshot_location_from_uuid(&snapshot.snapshot_id, TableSnapshot::VERSION)?;

                self.dal.write(&location, data).await?;

                let catalog = self.ctx.get_catalog(table_info.catalog()).await?;
                match FuseTable::update_table_meta(
                    catalog.clone(),
                    &table_info,
                    &self.location_gen,
                    snapshot,
                    location,
                    &self.copied_files,
                    &self.update_stream_meta,
                    &self.dal,
                    self.deduplicated_label.clone(),
                )
                .await
                {
                    Ok(_) => {
                        if self.do_truncate() {
                            catalog
                                .truncate_table(&table_info, TruncateTableReq {
                                    table_id: table_info.ident.table_id,
                                    batch_size: None,
                                })
                                .await?;
                        }

                        if self.purge {
                            // Removes historical data, if purge is true
                            let latest = self.table.refresh(self.ctx.as_ref()).await?;
                            let tbl = FuseTable::try_from_table(latest.as_ref())?;

                            warn!(
                                "purging historical data. table: {}, ident: {}",
                                tbl.table_info.name, tbl.table_info.ident
                            );

                            let keep_last_snapshot = true;
                            let snapshot_files = tbl.list_snapshot_files().await?;
                            if let Err(e) = tbl
                                .do_purge(
                                    &self.ctx,
                                    snapshot_files,
                                    None,
                                    keep_last_snapshot,
                                    false,
                                )
                                .await
                            {
                                // Errors of GC, if any, are ignored, since GC task can be picked up
                                warn!(
                                    "GC of table not success (this is not a permanent error). the error : {}",
                                    e
                                );
                            } else {
                                info!("GC of table done");
                            }
                        }
                        metrics_inc_commit_mutation_success();
                        {
                            let elapsed_time = self.start_time.elapsed();
                            let status = format!(
                                "commit mutation success after {} retries, which took {:?}",
                                self.retries, elapsed_time
                            );
                            metrics_inc_commit_milliseconds(elapsed_time.as_millis());
                            self.ctx.set_status_info(&status);
                        }
                        if let Some(files) = &self.copied_files {
                            metrics_inc_commit_copied_files(files.file_info.len() as u64);
                        }
                        for segment_loc in std::mem::take(&mut self.new_segment_locs).into_iter() {
                            self.ctx.add_segment_location(segment_loc)?;
                        }

                        let target_descriptions = {
                            let table_info = self.table.get_table_info();
                            let tbl = (&table_info.name, table_info.ident, &table_info.meta.engine);

                            let stream_descriptions = self
                                .update_stream_meta
                                .iter()
                                .map(|s| (s.stream_id, s.seq, "stream"))
                                .collect::<Vec<_>>();
                            (tbl, stream_descriptions)
                        };
                        info!("commit mutation success, targets {:?}", target_descriptions);
                        self.state = State::Finish;
                    }
                    Err(e) if self.is_error_recoverable(&e) => {
                        let table_info = self.table.get_table_info();
                        match self.backoff.next_backoff() {
                            Some(d) => {
                                let name = table_info.name.clone();
                                debug!(
                                    "got error TableVersionMismatched, tx will be retried {} ms later. table name {}, identity {}",
                                    d.as_millis(),
                                    name.as_str(),
                                    table_info.ident
                                );
                                databend_common_base::base::tokio::time::sleep(d).await;
                                self.retries += 1;
                                self.state = State::RefreshTable;
                            }
                            None => {
                                // Commit not fulfilled. try to abort the operations.
                                // if it is safe to do so.
                                if FuseTable::no_side_effects_in_meta_store(&e) {
                                    // if we are sure that table state inside metastore has not been
                                    // modified by this operation, abort this operation.
                                    self.state = State::Abort(e);
                                } else {
                                    return Err(ErrorCode::OCCRetryFailure(format!(
                                        "can not fulfill the tx after retries({} times, {} ms), aborted. table name {}, identity {}",
                                        self.retries,
                                        Instant::now()
                                            .duration_since(self.backoff.start_time)
                                            .as_millis(),
                                        table_info.name.as_str(),
                                        table_info.ident,
                                    )));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        // we are not sure about if the table state has been modified or not, just propagate the error
                        // and return, without aborting anything.
                        return Err(e);
                    }
                };
            }
            State::RefreshTable => {
                self.table = self.table.refresh(self.ctx.as_ref()).await?;
                let fuse_table = FuseTable::try_from_table(self.table.as_ref())?.to_owned();
                let previous = fuse_table.read_table_snapshot().await?;
                let cluster_key_meta = fuse_table.cluster_key_meta.clone();
                self.state = State::GenerateSnapshot {
                    previous,
                    cluster_key_meta,
                    table_info: fuse_table.table_info.clone(),
                };
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }
}
