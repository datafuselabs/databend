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
use common_catalog::lock::Lock;
use common_catalog::table::Table;
use common_catalog::table::TableExt;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockMetaInfoDowncast;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::UpsertTableCopiedFileReq;
use common_metrics::storage::*;
use common_pipeline_core::processors::Event;
use common_pipeline_core::processors::InputPort;
use common_pipeline_core::processors::Processor;
use common_pipeline_core::processors::ProcessorPtr;
use common_pipeline_core::LockGuard;
use log::debug;
use log::error;
use log::info;
use log::warn;
use opendal::Operator;
use storages_common_locks::set_backoff;
use storages_common_locks::LockManager;
use storages_common_table_meta::meta::ClusterKey;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::SnapshotId;
use storages_common_table_meta::meta::TableSnapshot;
use storages_common_table_meta::meta::Versioned;

use crate::io::TableMetaLocationGenerator;
use crate::operations::common::AbortOperation;
use crate::operations::common::CommitMeta;
use crate::operations::common::SnapshotGenerator;
use crate::FuseTable;

enum State {
    None,
    FillDefault,
    TryLock,
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
    AbortOperation,
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
    transient: bool,
    retries: u64,
    max_retry_elapsed: Option<Duration>,
    backoff: ExponentialBackoff,

    abort_operation: AbortOperation,
    lock_guard: Option<LockGuard>,
    need_lock: bool,
    start_time: Instant,
    prev_snapshot_id: Option<SnapshotId>,
}

impl<F> CommitSink<F>
where F: SnapshotGenerator + Send + 'static
{
    #[allow(clippy::too_many_arguments)]
    pub fn try_create(
        table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        copied_files: Option<UpsertTableCopiedFileReq>,
        snapshot_gen: F,
        input: Arc<InputPort>,
        max_retry_elapsed: Option<Duration>,
        need_lock: bool,
        prev_snapshot_id: Option<SnapshotId>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(CommitSink {
            state: State::None,
            ctx,
            dal: table.get_operator(),
            location_gen: table.meta_location_generator.clone(),
            table: Arc::new(table.clone()),
            copied_files,
            snapshot_gen,
            abort_operation: AbortOperation::default(),
            lock_guard: None,
            transient: table.transient(),
            backoff: ExponentialBackoff::default(),
            retries: 0,
            max_retry_elapsed,
            input,
            need_lock,
            start_time: Instant::now(),
            prev_snapshot_id,
        })))
    }

    fn is_error_recoverable(&self, e: &ErrorCode) -> bool {
        // When prev_snapshot_id is some, means it is an alter table column modification.
        // In this case if commit to meta fail and error is TABLE_VERSION_MISMATCHED operation will be aborted.
        if self.prev_snapshot_id.is_some() && e.code() == ErrorCode::TABLE_VERSION_MISMATCHED {
            return false;
        }
        FuseTable::is_error_recoverable(e, self.transient)
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
            .ok_or(ErrorCode::Internal("No block meta. It's a bug"))?;

        self.input.finish();

        let meta = CommitMeta::downcast_from(input_meta)
            .ok_or(ErrorCode::Internal("No commit meta. It's a bug"))?;

        self.abort_operation = meta.abort_operation;

        self.backoff = set_backoff(None, None, self.max_retry_elapsed);

        self.snapshot_gen
            .set_conflict_resolve_context(meta.conflict_resolve_context);
        if self.need_lock {
            self.state = State::TryLock;
        } else {
            self.state = State::FillDefault;
        }

        Ok(Event::Async)
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
        if matches!(&self.state, State::GenerateSnapshot { .. }) {
            return Ok(Event::Sync);
        }

        if matches!(
            &self.state,
            State::FillDefault
                | State::TryCommit { .. }
                | State::RefreshTable
                | State::AbortOperation
        ) {
            return Ok(Event::Async);
        }

        if matches!(self.state, State::Finish) {
            // release the lock manually.
            std::mem::take(&mut self.lock_guard);
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
                let schema = self.table.schema().as_ref().clone();
                match self
                    .snapshot_gen
                    .generate_new_snapshot(schema, cluster_key_meta, previous)
                {
                    Ok(snapshot) => {
                        self.state = State::TryCommit {
                            data: snapshot.to_bytes()?,
                            snapshot,
                            table_info,
                        };
                    }
                    Err(e) => {
                        error!(
                            "commit mutation failed after {} retries, error: {:?}",
                            self.retries, e,
                        );
                        self.state = State::AbortOperation;
                    }
                }
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
                    error!("commit mutation failed cause snapshot has changed when commit");
                    // if snapshot has changed abort operation
                    self.state = State::AbortOperation;
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
            State::TryLock => {
                let table_lock =
                    LockManager::create_table_lock(self.table.get_table_info().clone())?;
                match table_lock.try_lock(self.ctx.clone()).await {
                    Ok(guard) => {
                        self.lock_guard = guard;
                        self.state = State::FillDefault;
                    }
                    Err(e) => {
                        error!(
                            "commit mutation failed cause get lock failed, error: {:?}",
                            e
                        );
                        self.state = State::AbortOperation;
                    }
                }
            }
            State::TryCommit {
                data,
                snapshot,
                table_info,
            } => {
                let location = self.location_gen.gen_snapshot_location(
                    &snapshot.snapshot_id,
                    TableSnapshot::VERSION,
                    snapshot.table_version,
                )?;

                self.dal.write(&location, data).await?;

                let fuse_table = FuseTable::try_from_table(self.table.as_ref())?.to_owned();
                match fuse_table
                    .update_table_meta(
                        self.ctx.as_ref(),
                        &table_info,
                        &self.location_gen,
                        snapshot,
                        location,
                        &self.copied_files,
                        &self.dal,
                    )
                    .await
                {
                    Ok(_) => {
                        if self.transient {
                            // Removes historical data, if table is transient
                            let latest = self.table.refresh(self.ctx.as_ref()).await?;
                            let tbl = FuseTable::try_from_table(latest.as_ref())?;

                            warn!(
                                "transient table detected, purging historical data. ({})",
                                tbl.table_info.ident
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
                                    "GC of transient table not success (this is not a permanent error). the error : {}",
                                    e
                                );
                            } else {
                                info!("GC of transient table done");
                            }
                        }
                        metrics_inc_commit_mutation_success();
                        {
                            let elapsed_time = self.start_time.elapsed().as_millis();
                            let status = format!(
                                "commit mutation success after {} retries, which took {} ms",
                                self.retries, elapsed_time
                            );
                            metrics_inc_commit_milliseconds(elapsed_time);
                            self.ctx.set_status_info(&status);
                        }
                        if let Some(files) = &self.copied_files {
                            metrics_inc_commit_copied_files(files.file_info.len() as u64);
                        }
                        for segment in self.abort_operation.segments.iter() {
                            self.ctx.add_segment_location((
                                segment.to_string(),
                                SegmentInfo::VERSION,
                            ))?;
                        }
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
                                common_base::base::tokio::time::sleep(d).await;
                                self.retries += 1;
                                self.state = State::RefreshTable;
                            }
                            None => {
                                // Commit not fulfilled. try to abort the operations.
                                // if it is safe to do so.
                                if FuseTable::no_side_effects_in_meta_store(&e) {
                                    // if we are sure that table state inside metastore has not been
                                    // modified by this operation, abort this operation.
                                    self.state = State::AbortOperation;
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
            State::AbortOperation => {
                let duration = self.start_time.elapsed();
                metrics_inc_commit_aborts();
                // todo: use histogram when it ready
                metrics_inc_commit_milliseconds(duration.as_millis());
                let op = self.abort_operation.clone();
                op.abort(self.ctx.clone(), self.dal.clone()).await?;
                return Err(ErrorCode::StorageOther(format!(
                    "transaction aborted after {} retries, which took {} ms",
                    self.retries,
                    duration.as_millis()
                )));
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }
}
