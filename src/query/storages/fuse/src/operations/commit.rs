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

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use backoff::backoff::Backoff;
use chrono::Utc;
use common_catalog::table::Table;
use common_catalog::table::TableExt;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::TableSchemaRef;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableStatistics;
use common_meta_app::schema::UpdateTableMetaReq;
use common_meta_app::schema::UpsertTableCopiedFileReq;
use common_meta_types::MatchSeq;
use common_metrics::storage::*;
use common_pipeline_core::processors::ProcessorPtr;
use common_pipeline_core::Pipeline;
use common_pipeline_transforms::processors::AsyncAccumulatingTransformer;
use common_sql::executor::physical_plans::MutationKind;
use log::debug;
use log::info;
use log::warn;
use opendal::Operator;
use storages_common_cache::CacheAccessor;
use storages_common_cache_manager::CachedObject;
use storages_common_locks::set_backoff;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::SnapshotId;
use storages_common_table_meta::meta::Statistics;
use storages_common_table_meta::meta::TableSnapshot;
use storages_common_table_meta::meta::TableSnapshotStatistics;
use storages_common_table_meta::meta::Versioned;
use storages_common_table_meta::table::OPT_KEY_LEGACY_SNAPSHOT_LOC;
use storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;

use crate::io::MetaWriter;
use crate::io::SegmentsIO;
use crate::io::TableMetaLocationGenerator;
use crate::operations::common::AbortOperation;
use crate::operations::common::AppendGenerator;
use crate::operations::common::CommitSink;
use crate::operations::common::ConflictResolveContext;
use crate::operations::common::TableMutationAggregator;
use crate::operations::common::TransformSerializeSegment;
use crate::statistics::merge_statistics;
use crate::FuseTable;

impl FuseTable {
    #[async_backtrace::framed]
    pub fn do_commit(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        copied_files: Option<UpsertTableCopiedFileReq>,
        overwrite: bool,
        prev_snapshot_id: Option<SnapshotId>,
    ) -> Result<()> {
        let block_thresholds = self.get_block_thresholds();

        pipeline.try_resize(1)?;

        pipeline.add_transform(|input, output| {
            let proc =
                TransformSerializeSegment::new(ctx.clone(), input, output, self, block_thresholds);
            proc.into_processor()
        })?;

        pipeline.add_transform(|input, output| {
            let aggregator =
                TableMutationAggregator::new(self, ctx.clone(), vec![], MutationKind::Insert);
            Ok(ProcessorPtr::create(AsyncAccumulatingTransformer::create(
                input, output, aggregator,
            )))
        })?;

        let snapshot_gen =
            AppendGenerator::new(ctx.clone(), overwrite, Some(self.current_table_version()));
        pipeline.add_sink(|input| {
            CommitSink::try_create(
                self,
                ctx.clone(),
                copied_files.clone(),
                snapshot_gen.clone(),
                input,
                None,
                false,
                prev_snapshot_id,
            )
        })?;

        Ok(())
    }

    async fn check_lvt_conflict(
        &self,
        ctx: &dyn TableContext,
        table_info: &TableInfo,
        snapshot: &TableSnapshot,
    ) -> Result<()> {
        let catalog = table_info.catalog();
        let catalog = ctx.get_catalog(catalog).await?;
        let lvt = catalog.get_table_lvt(table_info.ident.table_id).await?;

        if let Some(lvt) = lvt.time {
            let prev_snapshot_opt = {
                let prev_snapshot_opt = self.prev_snapshot.read().await;
                prev_snapshot_opt.to_owned()
            };
            let prev_snapshot = if let Some(prev_snapshot) = prev_snapshot_opt.to_owned() {
                Some(prev_snapshot)
            } else if let Some((prev_snapshot_id, format_version, table_version)) =
                snapshot.prev_snapshot_id
            {
                let location_generator = &self.meta_location_generator;
                let prev_snapshot_loc = location_generator.gen_snapshot_location(
                    &prev_snapshot_id,
                    format_version,
                    table_version,
                )?;
                let read_prev_snapshot_opt = self
                    .read_table_snapshot_by_location(prev_snapshot_loc.clone())
                    .await?;
                if let Some(prev_snapshot) = read_prev_snapshot_opt {
                    let mut prev_snapshot_opt = self.prev_snapshot.write().await;
                    *prev_snapshot_opt = Some(prev_snapshot.clone());
                    Some(prev_snapshot)
                } else {
                    return Err(ErrorCode::StorageOther(format!(
                        "read prev snapshot at {:?} fail",
                        prev_snapshot_loc
                    )));
                }
            } else {
                None
            };

            if let Some(prev_snapshot) = prev_snapshot {
                let now_ms = if let Some(snapshot_time) = prev_snapshot.timestamp {
                    snapshot_time
                } else {
                    chrono::Utc::now()
                };
                if lvt > now_ms {
                    info!(
                        "when commit table {:?}, lvt {} conflict",
                        table_info.name, lvt
                    );
                    return Err(ErrorCode::StorageOther(
                        "commit fail by lvt conflict".to_string(),
                    ));
                }
            }
        }

        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn commit_to_meta_server(
        &self,
        ctx: &dyn TableContext,
        table_info: &TableInfo,
        snapshot: TableSnapshot,
        table_statistics: Option<TableSnapshotStatistics>,
        copied_files: &Option<UpsertTableCopiedFileReq>,
    ) -> Result<()> {
        let location_generator = &self.meta_location_generator;
        let snapshot_location = location_generator.gen_snapshot_location(
            &snapshot.snapshot_id,
            TableSnapshot::VERSION,
            snapshot.table_version,
        )?;
        let need_to_save_statistics =
            snapshot.table_statistics_location.is_some() && table_statistics.is_some();

        // 1. write down snapshot
        let operator = &self.operator;
        snapshot.write_meta(operator, &snapshot_location).await?;
        if need_to_save_statistics {
            table_statistics
                .clone()
                .unwrap()
                .write_meta(
                    operator,
                    &snapshot.table_statistics_location.clone().unwrap(),
                )
                .await?;
        }

        let table_statistics_location = snapshot.table_statistics_location.clone();
        // 2. update table meta
        let res = self
            .update_table_meta(
                ctx,
                table_info,
                location_generator,
                snapshot,
                snapshot_location,
                copied_files,
                operator,
            )
            .await;
        if need_to_save_statistics {
            let table_statistics_location = table_statistics_location.unwrap();
            match &res {
                Ok(_) => TableSnapshotStatistics::cache().put(
                    table_statistics_location,
                    Arc::new(table_statistics.unwrap()),
                ),
                Err(e) => {
                    if Self::no_side_effects_in_meta_store(e) {
                        let _ = operator.delete(&table_statistics_location).await;
                    }
                }
            }
        }
        res
    }

    #[async_backtrace::framed]
    #[allow(clippy::too_many_arguments)]
    pub async fn update_table_meta(
        &self,
        ctx: &dyn TableContext,
        table_info: &TableInfo,
        location_generator: &TableMetaLocationGenerator,
        snapshot: TableSnapshot,
        snapshot_location: String,
        copied_files: &Option<UpsertTableCopiedFileReq>,
        operator: &Operator,
    ) -> Result<()> {
        self.check_lvt_conflict(ctx, table_info, &snapshot).await?;

        // 1. prepare table meta
        let mut new_table_meta = table_info.meta.clone();
        // 1.1 set new snapshot location
        new_table_meta.options.insert(
            OPT_KEY_SNAPSHOT_LOCATION.to_owned(),
            snapshot_location.clone(),
        );
        // remove legacy options
        Self::remove_legacy_options(&mut new_table_meta.options);

        // 1.2 setup table statistics
        let stats = &snapshot.summary;
        // update statistics
        new_table_meta.statistics = TableStatistics {
            number_of_rows: stats.row_count,
            data_bytes: stats.uncompressed_byte_size,
            compressed_data_bytes: stats.compressed_byte_size,
            index_data_bytes: stats.index_size,
            number_of_segments: Some(snapshot.segments.len() as u64),
            number_of_blocks: Some(stats.block_count),
        };
        new_table_meta.updated_on = Utc::now();

        // 2. prepare the request
        let catalog = ctx.get_catalog(table_info.catalog()).await?;
        let table_id = table_info.ident.table_id;
        let table_version = table_info.ident.seq;

        let req = UpdateTableMetaReq {
            table_id,
            seq: MatchSeq::Exact(table_version),
            new_table_meta,
            copied_files: copied_files.clone(),
            deduplicated_label: ctx.get_settings().get_deduplicate_label()?,
        };

        // 3. let's roll
        let reply = catalog.update_table_meta(table_info, req).await;
        match reply {
            Ok(_) => {
                TableSnapshot::cache().put(snapshot_location.clone(), Arc::new(snapshot));
                // try keep a hit file of last snapshot
                Self::write_last_snapshot_hint(operator, location_generator, snapshot_location)
                    .await;
                Ok(())
            }
            Err(e) => {
                // commit snapshot to meta server failed.
                // figure out if the un-committed snapshot is safe to be removed.
                if Self::no_side_effects_in_meta_store(&e) {
                    // currently, only in this case (TableVersionMismatched),  we are SURE about
                    // that the table state insides meta store has NOT been changed.
                    info!(
                        "removing uncommitted table snapshot at location {}, of table {}, {}",
                        snapshot_location, table_info.desc, table_info.ident
                    );
                    let _ = operator.delete(&snapshot_location).await;
                }
                Err(e)
            }
        }
    }

    // Left a hint file which indicates the location of the latest snapshot
    #[async_backtrace::framed]
    pub async fn write_last_snapshot_hint(
        operator: &Operator,
        location_generator: &TableMetaLocationGenerator,
        last_snapshot_path: String,
    ) {
        // Just try our best to write down the hint file of last snapshot
        // - will retry in the case of temporary failure
        // but
        // - errors are ignored if writing is eventually failed
        // - errors (if any) will not be propagated to caller
        // - "data race" ignored
        //   if multiple different versions of hints are written concurrently
        //   it is NOT guaranteed that the latest version will be kept

        let hint_path = location_generator.gen_last_snapshot_hint_location();
        let last_snapshot_path = {
            let operator_meta_data = operator.info();
            let storage_prefix = operator_meta_data.root();
            format!("{}{}", storage_prefix, last_snapshot_path)
        };

        operator
            .write(&hint_path, last_snapshot_path)
            .await
            .unwrap_or_else(|e| {
                warn!("write last snapshot hint failure. {}", e);
            });
    }

    // TODO refactor, it is called by segment compaction
    #[async_backtrace::framed]
    pub async fn commit_mutation(
        &self,
        ctx: &Arc<dyn TableContext>,
        base_snapshot: Arc<TableSnapshot>,
        base_segments: &[Location],
        base_summary: Statistics,
        abort_operation: AbortOperation,
        max_retry_elapsed: Option<Duration>,
    ) -> Result<()> {
        let mut retries = 0;
        let mut backoff = set_backoff(None, None, max_retry_elapsed);

        let mut latest_snapshot = base_snapshot.clone();
        let mut latest_table_info = &self.table_info;
        let default_cluster_key_id = self.cluster_key_id();

        // holding the reference of latest table during retries
        let mut latest_table_ref: Arc<dyn Table>;

        // potentially concurrently appended segments, init it to empty
        let mut concurrently_appended_segment_locations: &[Location] = &[];

        // Status
        ctx.set_status_info("mutation: begin try to commit");
        let table_version = self.current_table_version();

        loop {
            let mut snapshot_tobe_committed =
                TableSnapshot::from_previous(latest_snapshot.as_ref(), Some(table_version));

            let schema = self.schema();
            let (segments_tobe_committed, statistics_tobe_committed) = Self::merge_with_base(
                ctx.clone(),
                self.operator.clone(),
                base_segments,
                &base_summary,
                concurrently_appended_segment_locations,
                schema,
                default_cluster_key_id,
            )
            .await?;
            snapshot_tobe_committed.segments = segments_tobe_committed;
            snapshot_tobe_committed.summary = statistics_tobe_committed;

            match self
                .commit_to_meta_server(
                    ctx.as_ref(),
                    latest_table_info,
                    snapshot_tobe_committed,
                    None,
                    &None,
                )
                .await
            {
                Err(e) if e.code() == ErrorCode::TABLE_VERSION_MISMATCHED => {
                    match backoff.next_backoff() {
                        Some(d) => {
                            let name = self.table_info.name.clone();
                            debug!(
                                "got error TableVersionMismatched, tx will be retried {} ms later. table name {}, identity {}",
                                d.as_millis(),
                                name.as_str(),
                                self.table_info.ident
                            );

                            common_base::base::tokio::time::sleep(d).await;
                            latest_table_ref = self.refresh(ctx.as_ref()).await?;
                            let latest_fuse_table =
                                FuseTable::try_from_table(latest_table_ref.as_ref())?;
                            latest_snapshot =
                                latest_fuse_table
                                    .read_table_snapshot()
                                    .await?
                                    .ok_or_else(|| {
                                        ErrorCode::Internal(
                                            "mutation meets empty snapshot during conflict reconciliation",
                                        )
                                    })?;
                            latest_table_info = &latest_fuse_table.table_info;

                            // Check if there is only insertion during the operation.
                            if let Some(range_of_newly_append) =
                                ConflictResolveContext::is_latest_snapshot_append_only(
                                    &base_snapshot,
                                    &latest_snapshot,
                                )
                            {
                                info!("resolvable conflicts detected");
                                metrics_inc_commit_mutation_latest_snapshot_append_only();
                                concurrently_appended_segment_locations =
                                    &latest_snapshot.segments[range_of_newly_append];
                            } else {
                                abort_operation
                                    .abort(ctx.clone(), self.operator.clone())
                                    .await?;
                                metrics_inc_commit_mutation_unresolvable_conflict();
                                break Err(ErrorCode::UnresolvableConflict(
                                    "segment compact conflict with other operations",
                                ));
                            }

                            retries += 1;
                            metrics_inc_commit_mutation_retry();
                            continue;
                        }
                        None => {
                            // Commit not fulfilled. try to abort the operations.
                            //
                            // Note that, here the last error we have seen is TableVersionMismatched,
                            // otherwise we should have been returned, thus it is safe to abort the operation here.
                            abort_operation
                                .abort(ctx.clone(), self.operator.clone())
                                .await?;
                            break Err(ErrorCode::StorageOther(format!(
                                "commit mutation failed after {} retries",
                                retries
                            )));
                        }
                    }
                }
                Err(e) => {
                    // we are not sure about if the table state has been modified or not, just propagate the error
                    // and return, without aborting anything.
                    break Err(e);
                }
                Ok(_) => {
                    break {
                        metrics_inc_commit_mutation_success();
                        Ok(())
                    };
                }
            }
        }
    }

    #[async_backtrace::framed]
    async fn merge_with_base(
        ctx: Arc<dyn TableContext>,
        operator: Operator,
        base_segments: &[Location],
        base_summary: &Statistics,
        concurrently_appended_segment_locations: &[Location],
        schema: TableSchemaRef,
        default_cluster_key_id: Option<u32>,
    ) -> Result<(Vec<Location>, Statistics)> {
        if concurrently_appended_segment_locations.is_empty() {
            Ok((base_segments.to_owned(), base_summary.clone()))
        } else {
            // place the concurrently appended segments at the head of segment list
            let new_segments = concurrently_appended_segment_locations
                .iter()
                .chain(base_segments.iter())
                .cloned()
                .collect();

            let fuse_segment_io = SegmentsIO::create(ctx, operator, schema);
            let concurrent_appended_segment_infos = fuse_segment_io
                .read_segments::<SegmentInfo>(concurrently_appended_segment_locations, true)
                .await?;

            let mut new_statistics = base_summary.clone();
            for result in concurrent_appended_segment_infos.into_iter() {
                let concurrent_appended_segment = result?;
                new_statistics = merge_statistics(
                    &new_statistics,
                    &concurrent_appended_segment.summary,
                    default_cluster_key_id,
                );
            }
            Ok((new_segments, new_statistics))
        }
    }

    #[inline]
    pub fn is_error_recoverable(e: &ErrorCode, is_table_transient: bool) -> bool {
        let code = e.code();
        code == ErrorCode::TABLE_VERSION_MISMATCHED
            || (is_table_transient && code == ErrorCode::STORAGE_NOT_FOUND)
    }

    #[inline]
    pub fn no_side_effects_in_meta_store(e: &ErrorCode) -> bool {
        // currently, the only error that we know,  which indicates there are no side effects
        // is TABLE_VERSION_MISMATCHED
        e.code() == ErrorCode::TABLE_VERSION_MISMATCHED
    }

    // check if there are any fuse table legacy options
    pub fn remove_legacy_options(table_options: &mut BTreeMap<String, String>) {
        table_options.remove(OPT_KEY_LEGACY_SNAPSHOT_LOC);
    }
}
