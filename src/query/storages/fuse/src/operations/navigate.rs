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

use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use databend_common_catalog::table::NavigationPoint;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AbortChecker;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableStatistics;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use databend_storages_common_table_meta::table::OPT_KEY_SOURCE_TABLE_ID;
use futures::TryStreamExt;
use log::warn;
use opendal::EntryMode;
use opendal::Metakey;

use crate::io::MetaReaders;
use crate::io::SnapshotHistoryReader;
use crate::io::SnapshotsIO;
use crate::io::TableMetaLocationGenerator;
use crate::FuseTable;
use crate::FUSE_TBL_SNAPSHOT_PREFIX;

impl FuseTable {
    #[fastrace::trace]
    #[async_backtrace::framed]
    pub async fn navigate_to_point(
        &self,
        point: &NavigationPoint,
        abort_checker: AbortChecker,
    ) -> Result<Arc<FuseTable>> {
        match point {
            NavigationPoint::SnapshotID(snapshot_id) => {
                self.navigate_to_snapshot(snapshot_id.as_str(), abort_checker)
                    .await
            }
            NavigationPoint::TimePoint(time_point) => {
                let Some(location) = self.snapshot_loc().await? else {
                    return Err(ErrorCode::TableHistoricalDataNotFound(
                        "Empty Table has no historical data",
                    ));
                };
                self.navigate_to_time_point(location, *time_point, abort_checker)
                    .await
            }
            NavigationPoint::StreamInfo(info) => self.navigate_to_stream(info).await,
        }
    }

    #[async_backtrace::framed]
    pub async fn navigate_to_stream(&self, stream_info: &TableInfo) -> Result<Arc<FuseTable>> {
        let options = stream_info.options();
        let stream_table_id = options
            .get(OPT_KEY_SOURCE_TABLE_ID)
            .ok_or_else(|| ErrorCode::Internal("table id must be set"))?
            .parse::<u64>()?;
        if stream_table_id != self.table_info.ident.table_id {
            return Err(ErrorCode::IllegalStream(format!(
                "The stream '{}' is not match the table '{}'",
                stream_info.desc, self.table_info.desc
            )));
        }

        let Some(snapshot_loc) = options.get(OPT_KEY_SNAPSHOT_LOCATION) else {
            let mut table_info = self.table_info.clone();
            table_info.meta.options.remove(OPT_KEY_SNAPSHOT_LOCATION);
            table_info.meta.statistics = TableStatistics::default();
            let table = FuseTable::do_create(table_info)?;
            return Ok(table.into());
        };
        let (snapshot, format_version) =
            SnapshotsIO::read_snapshot(snapshot_loc.clone(), self.get_operator()).await?;
        self.load_table_by_snapshot(snapshot.as_ref(), format_version)
    }

    #[async_backtrace::framed]
    pub async fn navigate_to_time_point(
        &self,
        location: String,
        time_point: DateTime<Utc>,
        aborting: AbortChecker,
    ) -> Result<Arc<FuseTable>> {
        self.find(location, aborting, |snapshot| {
            if let Some(ts) = snapshot.timestamp {
                ts <= time_point
            } else {
                false
            }
        })
        .await
    }

    #[async_backtrace::framed]
    pub async fn navigate_to_snapshot(
        &self,
        snapshot_id: &str,
        abort_checker: AbortChecker,
    ) -> Result<Arc<FuseTable>> {
        let Some(location) = self.snapshot_loc().await? else {
            return Err(ErrorCode::TableHistoricalDataNotFound(
                "Empty Table has no historical data",
            ));
        };

        self.find(location, abort_checker, |snapshot| {
            snapshot
                .snapshot_id
                .simple()
                .to_string()
                .as_str()
                .starts_with(snapshot_id)
        })
        .await
    }

    #[async_backtrace::framed]
    pub async fn find<P>(
        &self,
        location: String,
        abort_checker: AbortChecker,
        mut pred: P,
    ) -> Result<Arc<FuseTable>>
    where
        P: FnMut(&TableSnapshot) -> bool,
    {
        let snapshot_version = TableMetaLocationGenerator::snapshot_version(location.as_str());
        let reader = MetaReaders::table_snapshot_reader(self.get_operator());
        // grab the table history as stream
        // snapshots are order by timestamp DESC.
        let mut snapshot_stream = reader.snapshot_history(
            location,
            snapshot_version,
            self.meta_location_generator().clone(),
        );

        // Find the instant which matches the given `time_point`.
        let mut instant = None;
        while let Some(snapshot_with_version) = snapshot_stream.try_next().await? {
            abort_checker.try_check_aborting()?;
            if pred(snapshot_with_version.0.as_ref()) {
                instant = Some(snapshot_with_version);
                break;
            }
        }

        if let Some((snapshot, format_version)) = instant {
            self.load_table_by_snapshot(snapshot.as_ref(), format_version)
        } else {
            Err(ErrorCode::TableHistoricalDataNotFound(
                "No historical data found at given point",
            ))
        }
    }

    /// Load the table instance by the snapshot
    fn load_table_by_snapshot(
        &self,
        snapshot: &TableSnapshot,
        format_version: u64,
    ) -> Result<Arc<FuseTable>> {
        // The `seq` of ident that we cloned here is JUST a place holder
        // we should NOT use it other than a pure place holder.
        let mut table_info = self.table_info.clone();

        // There are more to be kept in snapshot, like engine_options, ordering keys...
        // or we could just keep a clone of TableMeta in the snapshot.
        //
        // currently, here are what we can recovery from the snapshot:

        // 1. the table schema
        table_info.meta.schema = Arc::new(snapshot.schema.clone());

        // 2. the table option `snapshot_location`
        let loc = self
            .meta_location_generator
            .snapshot_location_from_uuid(&snapshot.snapshot_id, format_version)?;
        table_info
            .meta
            .options
            .insert(OPT_KEY_SNAPSHOT_LOCATION.to_owned(), loc);

        // 3. The statistics
        let summary = &snapshot.summary;
        table_info.meta.statistics = TableStatistics {
            number_of_rows: summary.row_count,
            data_bytes: summary.uncompressed_byte_size,
            compressed_data_bytes: summary.compressed_byte_size,
            index_data_bytes: summary.index_size,
            number_of_segments: Some(snapshot.segments.len() as u64),
            number_of_blocks: Some(summary.block_count),
        };

        // let's instantiate it
        let table = FuseTable::do_create(table_info)?;
        Ok(table.into())
    }

    #[async_backtrace::framed]
    pub async fn navigate_for_purge(
        &self,
        ctx: &Arc<dyn TableContext>,
        instant: Option<NavigationPoint>,
    ) -> Result<(Arc<FuseTable>, Vec<String>)> {
        let retention =
            Duration::days(ctx.get_settings().get_data_retention_time_in_days()? as i64);
        let root_snapshot = if let Some(snapshot) = self.read_table_snapshot().await? {
            snapshot
        } else {
            return Err(ErrorCode::TableHistoricalDataNotFound(
                "No historical data found at given point",
            ));
        };

        assert!(root_snapshot.timestamp.is_some());
        let mut time_point = root_snapshot.timestamp.unwrap() - retention;

        let (location, files) = match instant {
            Some(NavigationPoint::TimePoint(point)) => {
                time_point = std::cmp::min(point, time_point);
                self.list_by_time_point(time_point).await
            }
            Some(NavigationPoint::SnapshotID(snapshot_id)) => {
                self.list_by_snapshot_id(snapshot_id.as_str(), time_point)
                    .await
            }
            Some(NavigationPoint::StreamInfo(info)) => self.list_by_stream(info, time_point).await,
            None => self.list_by_time_point(time_point).await,
        }?;

        let table = self
            .navigate_to_time_point(location, time_point, ctx.clone().get_abort_checker())
            .await?;

        Ok((table, files))
    }

    #[async_backtrace::framed]
    pub async fn list_by_time_point(
        &self,
        time_point: DateTime<Utc>,
    ) -> Result<(String, Vec<String>)> {
        let prefix = format!(
            "{}/{}/",
            self.meta_location_generator().prefix(),
            FUSE_TBL_SNAPSHOT_PREFIX,
        );

        let files = self
            .list_files(prefix, |_, modified| modified <= time_point)
            .await?;
        if files.is_empty() {
            return Err(ErrorCode::TableHistoricalDataNotFound(
                "No historical data found at given point",
            ));
        }

        let location = files[0].clone();
        let reader = MetaReaders::table_snapshot_reader(self.get_operator());
        let ver = TableMetaLocationGenerator::snapshot_version(location.as_str());
        let load_params = LoadParams {
            location,
            len_hint: None,
            ver,
            put_cache: false,
        };
        let snapshot = reader.read(&load_params).await?;
        // Take the prev snapshot as base snapshot to avoid get orphan snapshot.
        let prev = snapshot.prev_snapshot_id;
        match prev {
            Some((id, v)) => {
                let new_loc = self
                    .meta_location_generator()
                    .snapshot_location_from_uuid(&id, v)?;
                Ok((new_loc, files))
            }
            None => Err(ErrorCode::TableHistoricalDataNotFound(
                "No historical data found at given point",
            )),
        }
    }

    #[async_backtrace::framed]
    pub async fn list_by_snapshot_id(
        &self,
        snapshot_id: &str,
        retention_point: DateTime<Utc>,
    ) -> Result<(String, Vec<String>)> {
        // TODO(Sky): unify location related logic into a single place
        let mut location = None;
        let prefix = format!(
            "{}/{}/",
            self.meta_location_generator().prefix(),
            FUSE_TBL_SNAPSHOT_PREFIX,
        );
        let prefix_loc = format!("{}{}", prefix, snapshot_id);
        let prefix_loc_v5 = format!("{}g{}", prefix, snapshot_id);

        let files = self
            .list_files(prefix, |loc, modified| {
                if loc.starts_with(&prefix_loc) || loc.starts_with(&prefix_loc_v5) {
                    location = Some(loc);
                }
                modified <= retention_point
            })
            .await?;
        let location = location.ok_or_else(|| {
            ErrorCode::TableHistoricalDataNotFound("No historical data found at given point")
        })?;
        Ok((location, files))
    }

    #[async_backtrace::framed]
    pub async fn list_by_stream(
        &self,
        stream_info: TableInfo,
        retention_point: DateTime<Utc>,
    ) -> Result<(String, Vec<String>)> {
        let options = stream_info.options();
        let stream_table_id = options
            .get(OPT_KEY_SOURCE_TABLE_ID)
            .ok_or_else(|| ErrorCode::Internal("table id must be set"))?
            .parse::<u64>()?;
        if stream_table_id != self.table_info.ident.table_id {
            return Err(ErrorCode::IllegalStream(format!(
                "The stream '{}' is not match the table '{}'",
                stream_info.desc, self.table_info.desc
            )));
        }

        let snapshot_loc = options
            .get(OPT_KEY_SNAPSHOT_LOCATION)
            .ok_or_else(|| {
                ErrorCode::TableHistoricalDataNotFound("No historical data found at given point")
            })?
            .parse::<String>()?;

        let mut found = false;
        let prefix = format!(
            "{}/{}/",
            self.meta_location_generator().prefix(),
            FUSE_TBL_SNAPSHOT_PREFIX,
        );

        let files = self
            .list_files(prefix, |loc, modified| {
                if loc == snapshot_loc {
                    found = true;
                }
                modified <= retention_point
            })
            .await?;

        if !found {
            return Err(ErrorCode::TableHistoricalDataNotFound(
                "No historical data found at given point",
            ));
        }
        Ok((snapshot_loc, files))
    }

    #[async_backtrace::framed]
    pub async fn list_files<F>(&self, prefix: String, mut f: F) -> Result<Vec<String>>
    where F: FnMut(String, DateTime<Utc>) -> bool {
        let mut file_list = vec![];
        let op = self.operator.clone();
        let mut ds = op
            .lister_with(&prefix)
            .metakey(Metakey::Mode | Metakey::LastModified)
            .await?;
        while let Some(de) = ds.try_next().await? {
            let meta = de.metadata();
            match meta.mode() {
                EntryMode::FILE => {
                    let modified = meta.last_modified();
                    let location = de.path().to_string();
                    if let Some(modified) = modified {
                        if f(location.clone(), modified) {
                            file_list.push((location, modified));
                        }
                    }
                }
                _ => {
                    warn!("found not snapshot file in {:}, found: {:?}", prefix, de);
                    continue;
                }
            }
        }

        file_list.sort_by(|(_, m1), (_, m2)| m2.cmp(m1));

        Ok(file_list.into_iter().map(|v| v.0).collect())
    }
}
