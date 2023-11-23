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
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use chrono::DateTime;
use chrono::Utc;
use common_base::runtime::execute_futures_in_parallel;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use futures::stream::StreamExt;
use futures_util::TryStreamExt;
use log::info;
use log::warn;
use minitrace::full_name;
use minitrace::prelude::*;
use opendal::EntryMode;
use opendal::Metakey;
use opendal::Operator;
use storages_common_cache::LoadParams;
use storages_common_table_meta::meta::FormatVersion;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::SnapshotId;
use storages_common_table_meta::meta::TableSnapshot;
use storages_common_table_meta::meta::TableSnapshotLite;
use storages_common_table_meta::meta::TableVersion;

use crate::io::MetaReaders;
use crate::io::SnapshotHistoryReader;
use crate::io::TableMetaLocationGenerator;

#[derive(Clone, Debug)]
pub struct SnapshotLiteExtended {
    pub format_version: u64,
    pub snapshot_id: SnapshotId,
    pub table_version: Option<TableVersion>,
    pub timestamp: Option<DateTime<Utc>>,
    pub segments: HashSet<Location>,
    pub table_statistics_location: Option<String>,
}

// Read snapshot related operations.
pub struct SnapshotsIO {
    ctx: Arc<dyn TableContext>,
    operator: Operator,
}

impl SnapshotsIO {
    pub fn create(ctx: Arc<dyn TableContext>, operator: Operator) -> Self {
        Self { ctx, operator }
    }

    pub fn get_ctx(&self) -> Arc<dyn TableContext> {
        self.ctx.clone()
    }

    pub fn get_operator(&self) -> Operator {
        self.operator.clone()
    }

    #[async_backtrace::framed]
    async fn read_snapshot(
        snapshot_location: String,
        data_accessor: Operator,
    ) -> Result<(Arc<TableSnapshot>, FormatVersion)> {
        let reader = MetaReaders::table_snapshot_reader(data_accessor);
        let ver = TableMetaLocationGenerator::snapshot_version(snapshot_location.as_str());
        let load_params = LoadParams {
            location: snapshot_location,
            len_hint: None,
            ver,
            put_cache: true,
        };
        let snapshot = reader.read(&load_params).await?;
        Ok((snapshot, ver))
    }

    #[async_backtrace::framed]
    async fn read_snapshot_lite(
        snapshot_location: String,
        data_accessor: Operator,
        min_snapshot_timestamp: Option<DateTime<Utc>>,
    ) -> Result<TableSnapshotLite> {
        let reader = MetaReaders::table_snapshot_reader(data_accessor);
        let ver = TableMetaLocationGenerator::snapshot_version(snapshot_location.as_str());
        let load_params = LoadParams {
            location: snapshot_location,
            len_hint: None,
            ver,
            put_cache: false,
        };
        let snapshot = reader.read(&load_params).await?;

        if snapshot.timestamp > min_snapshot_timestamp {
            // filter out snapshots which have larger (artificial)timestamp , they are
            // not members of precedents of the current snapshot, whose timestamp is
            // min_snapshot_timestamp.
            //
            // NOTE: it is NOT the case that all those have lesser timestamp, are
            // members of precedents of the current snapshot, though.
            // Error is directly returned, since it can be ignored through flatten
            // in read_snapshot_lites_ext.
            return Err(ErrorCode::StorageOther(
                "The timestamp of snapshot need less than the min_snapshot_timestamp",
            ));
        }
        Ok(TableSnapshotLite::from((snapshot.as_ref(), ver)))
    }

    #[async_backtrace::framed]
    #[minitrace::trace]
    async fn read_snapshot_lites(
        &self,
        snapshot_files: &[String],
        min_snapshot_timestamp: Option<DateTime<Utc>>,
    ) -> Result<Vec<Result<TableSnapshotLite>>> {
        // combine all the tasks.
        let mut iter = snapshot_files.iter();
        let tasks = std::iter::from_fn(move || {
            iter.next().map(|location| {
                Self::read_snapshot_lite(
                    location.clone(),
                    self.operator.clone(),
                    min_snapshot_timestamp,
                )
                .in_span(Span::enter_with_local_parent(full_name!()))
            })
        });

        let threads_nums = self.ctx.get_settings().get_max_threads()? as usize;

        execute_futures_in_parallel(
            tasks,
            threads_nums,
            threads_nums * 2,
            "fuse-req-snapshots-worker".to_owned(),
        )
        .await
    }

    // Read all the snapshots by the root file.
    #[async_backtrace::framed]
    pub async fn read_snapshot_lites_ext<T>(
        &self,
        root_snapshot_file: String,
        min_snapshot_timestamp: Option<DateTime<Utc>>,
        status_callback: T,
    ) -> Result<(Vec<TableSnapshotLite>, Vec<TableSnapshotLite>)>
    where
        T: Fn(String),
    {
        let ctx = self.ctx.clone();
        let data_accessor = self.operator.clone();

        // List all the snapshot file paths
        // note that snapshot file paths of ongoing txs might be included
        let mut snapshot_files = vec![];
        if let Some(prefix) = Self::get_s3_prefix_from_file(&root_snapshot_file) {
            snapshot_files = Self::list_files(self.operator.clone(), &prefix, None).await?;
        }

        // 1. Get all the snapshot by chunks.
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let mut snapshot_lites = Vec::with_capacity(snapshot_files.len());

        let start = Instant::now();
        let mut count = 0;
        for chunk in snapshot_files.chunks(max_threads) {
            let results = self
                .read_snapshot_lites(chunk, min_snapshot_timestamp)
                .await?;

            for snapshot_lite in results.into_iter().flatten() {
                snapshot_lites.push(snapshot_lite);
            }

            // Refresh status.
            {
                count += chunk.len();
                let status = format!(
                    "read snapshot files:{}/{}, cost:{} sec",
                    count,
                    snapshot_files.len(),
                    start.elapsed().as_secs()
                );
                info!("{}", status);
                (status_callback)(status);
            }
        }

        let (root_snapshot, format_version) =
            Self::read_snapshot(root_snapshot_file.clone(), data_accessor.clone()).await?;

        Ok(Self::chain_snapshots(
            snapshot_lites,
            &root_snapshot,
            format_version,
        ))
    }

    // read all the precedent snapshots of given `root_snapshot`
    #[async_backtrace::framed]
    pub async fn read_chained_snapshot_lites(
        &self,
        location_generator: TableMetaLocationGenerator,
        root_snapshot: String,
        limit: Option<usize>,
    ) -> Result<Vec<TableSnapshotLite>> {
        let table_snapshot_reader =
            MetaReaders::table_snapshot_reader(self.ctx.get_data_operator()?.operator());
        let format_version = TableMetaLocationGenerator::snapshot_version(root_snapshot.as_str());
        let lite_snapshot_stream = table_snapshot_reader
            .snapshot_history(root_snapshot, format_version, location_generator)
            .map_ok(|(snapshot, format_version)| {
                TableSnapshotLite::from((snapshot.as_ref(), format_version))
            });
        if let Some(l) = limit {
            lite_snapshot_stream.take(l).try_collect::<Vec<_>>().await
        } else {
            lite_snapshot_stream.try_collect::<Vec<_>>().await
        }
    }

    #[async_backtrace::framed]
    async fn read_snapshot_lite_extend(
        snapshot_location: String,
        data_accessor: Operator,
        root_snapshot: Arc<SnapshotLiteExtended>,
        ignore_timestamp: bool,
    ) -> Result<SnapshotLiteExtended> {
        let reader = MetaReaders::table_snapshot_reader(data_accessor);
        let ver = TableMetaLocationGenerator::snapshot_version(snapshot_location.as_str());
        let load_params = LoadParams {
            location: snapshot_location,
            len_hint: None,
            ver,
            put_cache: false,
        };
        let snapshot = reader.read(&load_params).await?;

        if !ignore_timestamp && snapshot.timestamp >= root_snapshot.timestamp {
            // filter out snapshots which have larger (artificial)timestamp , they are
            // not members of precedents of the current snapshot, whose timestamp is
            // min_snapshot_timestamp.
            //
            // NOTE: it is NOT the case that all those have lesser timestamp, are
            // members of precedents of the current snapshot, though.
            // Error is directly returned, since we can be ignored through flatten.
            return Err(ErrorCode::StorageOther(
                "The timestamp of snapshot need less than the min_snapshot_timestamp",
            ));
        }
        let mut segments = HashSet::new();
        // collects extended segments.
        for segment_location in &snapshot.segments {
            if root_snapshot.segments.contains(segment_location) {
                continue;
            }
            segments.insert(segment_location.clone());
        }
        let table_statistics_location =
            if snapshot.table_statistics_location != root_snapshot.table_statistics_location {
                snapshot.table_statistics_location.clone()
            } else {
                None
            };

        Ok(SnapshotLiteExtended {
            format_version: ver,
            snapshot_id: snapshot.snapshot_id,
            table_version: snapshot.table_version,
            timestamp: snapshot.timestamp,
            segments,
            table_statistics_location,
        })
    }

    // If `ignore_timestamp` is true, ignore filter out snapshots which have larger (artificial)timestamp
    #[async_backtrace::framed]
    #[minitrace::trace]
    pub async fn read_snapshot_lite_extends(
        &self,
        snapshot_files: &[String],
        root_snapshot: Arc<SnapshotLiteExtended>,
        ignore_timestamp: bool,
    ) -> Result<Vec<Result<SnapshotLiteExtended>>> {
        // combine all the tasks.
        let mut iter = snapshot_files.iter();
        let tasks = std::iter::from_fn(move || {
            iter.next().map(|location| {
                Self::read_snapshot_lite_extend(
                    location.clone(),
                    self.operator.clone(),
                    root_snapshot.clone(),
                    ignore_timestamp,
                )
                .in_span(Span::enter_with_local_parent(full_name!()))
            })
        });

        let threads_nums = self.ctx.get_settings().get_max_threads()? as usize;

        execute_futures_in_parallel(
            tasks,
            threads_nums,
            threads_nums * 2,
            "fuse-req-snapshots-worker".to_owned(),
        )
        .await
    }

    fn chain_snapshots(
        snapshot_lites: Vec<TableSnapshotLite>,
        root_snapshot: &TableSnapshot,
        format_version: FormatVersion,
    ) -> (Vec<TableSnapshotLite>, Vec<TableSnapshotLite>) {
        let mut snapshot_map = HashMap::new();
        let mut chained_snapshot_lites = vec![];
        for snapshot_lite in snapshot_lites.into_iter() {
            snapshot_map.insert(snapshot_lite.snapshot_id, snapshot_lite);
        }
        let root_snapshot_lite = TableSnapshotLite::from((root_snapshot, format_version));
        let mut prev_snapshot_id_tuple = root_snapshot_lite.prev_snapshot_id;
        chained_snapshot_lites.push(root_snapshot_lite);
        while let Some((prev_snapshot_id, _, _)) = prev_snapshot_id_tuple {
            let prev_snapshot_lite = snapshot_map.remove(&prev_snapshot_id);
            match prev_snapshot_lite {
                None => {
                    break;
                }
                Some(prev_snapshot) => {
                    prev_snapshot_id_tuple = prev_snapshot.prev_snapshot_id;
                    chained_snapshot_lites.push(prev_snapshot);
                }
            }
        }
        // remove root from orphan list
        snapshot_map.remove(&root_snapshot.snapshot_id);
        (chained_snapshot_lites, snapshot_map.into_values().collect())
    }

    #[async_backtrace::framed]
    pub async fn list_files(
        op: Operator,
        prefix: &str,
        exclude_file: Option<&str>,
    ) -> Result<Vec<String>> {
        let mut file_list = vec![];
        let mut ds = op
            .lister_with(prefix)
            .metakey(Metakey::Mode | Metakey::LastModified)
            .await?;
        while let Some(de) = ds.try_next().await? {
            let meta = de.metadata();
            match meta.mode() {
                EntryMode::FILE => match exclude_file {
                    Some(path) if de.path() == path => continue,
                    _ => {
                        let location = de.path().to_string();
                        let modified = meta.last_modified();
                        file_list.push((location, modified));
                    }
                },
                _ => {
                    warn!("found not snapshot file in {:}, found: {:?}", prefix, de);
                    continue;
                }
            }
        }

        // let mut vector: Vec<(i32, Option<i32>)> = vec![(1, Some(1)), (2, None), (3, Some(3)), (4, None)];
        // vector.sort_by(|(_, k1), (_, k2)| k2.cmp(k1));
        // Result:
        // [(3, Some(3)), (1, Some(1)), (2, None),(4, None)]
        file_list.sort_by(|(_, m1), (_, m2)| m2.cmp(m1));
        Ok(file_list.into_iter().map(|v| v.0).collect())
    }

    // _ss/xx/yy.json -> _ss/xx/
    pub fn get_s3_prefix_from_file(file_path: &str) -> Option<String> {
        if let Some(path) = Path::new(&file_path).parent() {
            let mut prefix = path.to_str().unwrap_or("").to_string();

            if !prefix.contains('/') {
                return None;
            }

            // Append '/' to the end if need.
            if !prefix.ends_with('/') {
                prefix += "/";
            }
            return Some(prefix);
        }
        None
    }
}
