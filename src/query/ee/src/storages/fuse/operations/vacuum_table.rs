// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashSet;
use std::debug_assert;
use std::sync::Arc;
use std::time::Instant;

use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_storages_fuse::io::SnapshotsIO;
use common_storages_fuse::io::TableMetaLocationGenerator;
use common_storages_fuse::FuseTable;
use futures::TryStreamExt;
use log::info;
use log::warn;
use opendal::EntryMode;
use opendal::Metakey;
use storages_common_table_meta::meta::time_from_snapshot_id;
use storages_common_table_meta::meta::TableSnapshot;
use storages_common_table_meta::meta::TableVersion;

const DRY_RUN_LIMIT: usize = 1000;
const BATCH_PURGE_FILE_NUM: usize = 1000;

type SnapshotLocationWithTime = (Option<DateTime<Utc>>, String);
type TableVersionSet = HashSet<TableVersion>;
type PrefixAndReferencedFileSet = (Option<String>, HashSet<String>);

struct VacuumOperator {
    pub fuse_table: FuseTable,
    pub current_snapshot: Arc<TableSnapshot>,
    pub current_snapshot_location: String,
    pub ctx: Arc<dyn TableContext>,
    pub retention_time: DateTime<Utc>,
    pub start: Instant,
}

#[derive(Debug)]
struct SnapshotReferencedFileSet {
    pub segments: PrefixAndReferencedFileSet,
    pub blocks: PrefixAndReferencedFileSet,
    pub blocks_index: PrefixAndReferencedFileSet,
}

struct ReferencedFilesPrefix {
    pub segment_prefix: Option<String>,
    pub block_prefix: Option<String>,
    pub block_index_prefix: Option<String>,
}

struct VacuumContext {
    pub snapshot_files_with_time: Vec<SnapshotLocationWithTime>,
    pub root_gc_snapshot_index: usize,
    pub root_gc_snapshot_reference_files: SnapshotReferencedFileSet,
    pub keep_snapshot_table_versions: TableVersionSet,
}

impl VacuumOperator {
    // list all snapshot files and sort by time
    #[async_backtrace::framed]
    async fn list_snapshot_files_with_time(&self) -> Result<Vec<SnapshotLocationWithTime>> {
        // List all the snapshot file paths
        // note that snapshot file paths of ongoing txs might be included
        let operator = self.fuse_table.get_operator();
        let mut snapshot_files = vec![];
        if let Some(prefix) = SnapshotsIO::get_s3_prefix_from_file(&self.current_snapshot_location)
        {
            snapshot_files = SnapshotsIO::list_files(operator, &prefix, None).await?;
        }

        let mut snapshot_files_with_time = Vec::with_capacity(snapshot_files.len());
        if snapshot_files.is_empty() {
            return Ok(snapshot_files_with_time);
        }

        for snapshot_file in snapshot_files {
            let snapshot_id =
                TableMetaLocationGenerator::snapshot_id_from_location(&snapshot_file)?;
            let timestamp = time_from_snapshot_id(&snapshot_id);
            snapshot_files_with_time.push((timestamp, snapshot_file));
        }

        // sort snapshot files with time in descending order
        snapshot_files_with_time.sort_by(|(t1, _), (t2, _)| t2.cmp(t1));

        Ok(snapshot_files_with_time)
    }

    // reform `snapshot_files_with_time` to make current snapshot in index 0
    // if all the snapshot file in old format
    fn reform_snapshot_files_with_time_if_needed(
        snapshot_files_with_time: &mut [SnapshotLocationWithTime],
        current_snapshot_location: &String,
    ) {
        // timestamp is_some means that not all the snapshot file name in old format
        // in this case has nothing to do
        if snapshot_files_with_time[0].0.is_some() {
            return;
        }

        // find current snapshot location and move it to index 0
        for (i, (_, snapshot)) in snapshot_files_with_time.iter().enumerate() {
            if snapshot != current_snapshot_location {
                continue;
            }
            let from_snapshot_with_file = snapshot_files_with_time[0].clone();
            let to_snapshot_with_file = snapshot_files_with_time[i].clone();
            snapshot_files_with_time[i] = from_snapshot_with_file;
            snapshot_files_with_time[0] = to_snapshot_with_file;
            break;
        }
    }

    // check if:
    // 1. current snapshot is snapshot_files_with_time[0]
    // 2. and current snapshot timestamp < retention time
    #[async_backtrace::framed]
    async fn check_if_current_snapshot_root_gc(
        &self,
        snapshot_files_with_time: &mut [SnapshotLocationWithTime],
    ) -> Result<Option<usize>> {
        Self::reform_snapshot_files_with_time_if_needed(
            snapshot_files_with_time,
            &self.current_snapshot_location,
        );

        let snapshot_file_with_time = &snapshot_files_with_time[0];
        if self.current_snapshot_location != snapshot_file_with_time.1 {
            let status = format!(
                "do_vacuum with table {}: snapshot_file_with_time[0] snapshot is not current snapshot",
                self.fuse_table.get_table_info().name,
            );
            self.log_status(status);
            return Ok(None);
        }

        // now snapshot_files_with_time[0] is current snapshot, check if timestamp < retentime time
        let timestamp = if let Some(timestamp) = self.current_snapshot.timestamp {
            timestamp
        } else if let Some(timestamp) = snapshot_file_with_time.0 {
            timestamp
        } else {
            let status = format!(
                "do_vacuum with table {}: snapshot {:?} has no timestamp",
                self.fuse_table.get_table_info().name,
                snapshot_file_with_time.1,
            );
            self.log_status(status);
            return Ok(None);
        };

        if timestamp >= self.retention_time {
            if snapshot_files_with_time.len() == 1 {
                return Ok(Some(0));
            }

            // if retention time is in [current_snapshot.prev_snapshot.timestamp, current_snapshot.timestamp]
            // then current snapshot is root gc snapshot, otherwise return None
            let prev_snapshot_file_with_time = &snapshot_files_with_time[1];
            let prev_snapshot_timestamp = match prev_snapshot_file_with_time.0 {
                Some(timestamp) => timestamp,
                None => {
                    let prev_snapshot_location =
                        if let Some((prev_snapshot_id, format_version, table_version)) =
                            &self.current_snapshot.prev_snapshot_id
                        {
                            self.fuse_table
                                .meta_location_generator()
                                .gen_snapshot_location(
                                    prev_snapshot_id,
                                    *format_version,
                                    *table_version,
                                )?
                        } else {
                            return Ok(None);
                        };

                    let prev_snapshot = self
                        .fuse_table
                        .read_table_snapshot_by_location(prev_snapshot_location)
                        .await?;

                    if let Some(prev_snapshot) = prev_snapshot {
                        if let Some(timestamp) = prev_snapshot.timestamp {
                            timestamp
                        } else {
                            return Ok(None);
                        }
                    } else {
                        return Ok(None);
                    }
                }
            };

            if self.retention_time >= prev_snapshot_timestamp {
                Ok(Some(0))
            } else {
                Ok(None)
            }
        } else {
            // retention time cannot larger than current snapshot timestamp
            let status = format!(
                "do_vacuum with table {}: currerent snapshot {:?} timestamp < retention_time",
                self.fuse_table.get_table_info().name,
                snapshot_file_with_time.1,
            );
            self.log_status(status);
            Ok(None)
        }
    }

    #[async_backtrace::framed]
    async fn check_if_has_committed_success(
        &self,
        snapshot_files_with_time: &[SnapshotLocationWithTime],
        index: usize,
    ) -> Result<bool> {
        let snapshot_file_with_time = &snapshot_files_with_time[index];
        if index == 0 {
            return Ok(self.current_snapshot_location == snapshot_file_with_time.1);
        }

        let next_index = index - 1;
        let next_snapshot_location = &snapshot_files_with_time[next_index].1;
        let next_snapshot = match self
            .fuse_table
            .read_table_snapshot_by_location(next_snapshot_location.to_string())
            .await?
        {
            Some(next_snapshot) => next_snapshot,
            None => {
                return Ok(false);
            }
        };

        let prev_snapshot_location = if let Some((prev_snapshot_id, version, table_version)) =
            next_snapshot.prev_snapshot_id
        {
            let generator = self.fuse_table.meta_location_generator();
            generator.gen_snapshot_location(&prev_snapshot_id, version, table_version)?
        } else {
            return Ok(false);
        };

        Ok(prev_snapshot_location == snapshot_file_with_time.1)
    }

    // get root gc snapshot file and index in snapshot_files_with_time vector
    // we call the snapshot file name with timestamp the `new snapshot format`, where without timestamp the `old snapshot format`
    //
    // there two cases may find the root gc snapshot:
    //
    //                              snapshot timestamp >= retention time          snapshot timestamp < retention time
    //                           +--------------------------------------+ prev  +-----------------------------------+
    //           +---------+     |                                      |       |                                   |     +------+
    // case 1:   |   ....  +-----|          root gc snapshot            |------->   root snapshot'prev snapshot     |---->| .... |
    //           |         |     |                                      |       |                                   |     |      |
    //           +---------+     +--------------------------------------+       +-----------------------------------+     +------+
    //                              snapshot file name in new format            snapshot file name in new format
    //
    //
    //      snapshot timestamp < retention time
    //          +-----------------+   prev  +---------+
    // case 2:  |root gc snapshot |         |   ....  |
    //          |                 |-------> |         |
    //          +-----------------+         +---------+
    //          current snapshot
    //
    #[async_backtrace::framed]
    async fn get_root_gc_snapshot_file(
        &self,
        snapshot_files_with_time: &mut Vec<SnapshotLocationWithTime>,
    ) -> Result<Option<usize>> {
        if snapshot_files_with_time.is_empty() {
            return Ok(None);
        }

        let retention_time = &self.retention_time;
        // last snapshot index that snapshot's timestamp >= retention time
        let mut last_snapshot_index_opt = None;

        // iterator snapshot files with time in descending order
        for (i, (timestamp, snapshot_file)) in snapshot_files_with_time.iter().enumerate() {
            let timestamp = if let Some(timestamp) = timestamp {
                timestamp
            } else {
                // now we get a snapshot with old file name(no timestamp in snapshot file name)
                if last_snapshot_index_opt.is_some() {
                    // if all the succeed snapshots timestamp >= timestamp, return None
                    let status = format!(
                        "do_vacuum with table {}: reach snapshot {:?} in old format, but cannot find any snapshot that timestamp < retention_time",
                        self.fuse_table.get_table_info().name,
                        snapshot_file,
                    );
                    self.log_status(status);
                    return Ok(None);
                } else {
                    // case 2: all the snapshot in old snapshot file format
                    // check if current snapshot is the root gc
                    return self
                        .check_if_current_snapshot_root_gc(snapshot_files_with_time)
                        .await;
                }
            };

            if timestamp >= retention_time {
                // save the last snapshot index that timestamp >= retention time
                last_snapshot_index_opt = Some(i);
            } else if last_snapshot_index_opt.is_none() {
                // case 2: all the snapshot in new snapshot file format
                // check if current snapshot is the root gc
                return self
                    .check_if_current_snapshot_root_gc(snapshot_files_with_time)
                    .await;
            } else {
                // now get the last snapshot timestamp < retention time, break the loop
                break;
            }
        }

        debug_assert!(last_snapshot_index_opt.is_some());
        let root_gc_index = last_snapshot_index_opt.unwrap();

        // case 1, now check last index snapshot has been committed success
        if !self
            .check_if_has_committed_success(snapshot_files_with_time, root_gc_index)
            .await?
        {
            let status = format!(
                "do_vacuum with table {}: cannot make sure last index snapshot {:?} has been committed success",
                self.fuse_table.get_table_info().name,
                snapshot_files_with_time[root_gc_index].1,
            );
            self.log_status(status);
            return Ok(None);
        }

        // if root_gc_index is not the last index, check last index + 1 snapshot has been committed success
        if root_gc_index != snapshot_files_with_time.len() - 1
            && !self
                .check_if_has_committed_success(snapshot_files_with_time, root_gc_index + 1)
                .await?
        {
            let status = format!(
                "do_vacuum with table {}: cannot make sure last index + 1 snapshot {:?} has been committed success",
                self.fuse_table.get_table_info().name,
                snapshot_files_with_time[root_gc_index + 1].1,
            );
            self.log_status(status);
            return Ok(None);
        }

        Ok(Some(root_gc_index))
    }

    #[async_backtrace::framed]
    fn get_keep_snapshot_table_versions(
        &self,
        snapshot_files_with_time: &[SnapshotLocationWithTime],
        root_gc_snapshot_index: usize,
    ) -> TableVersionSet {
        let mut table_version_set = HashSet::new();
        for snapshot_file_with_time in
            snapshot_files_with_time[0..root_gc_snapshot_index + 1].iter()
        {
            let snapshot_file = &snapshot_file_with_time.1;
            // all snapshot file commit after root_gc_snapshot will with table version, so safe to unwrap()
            if let Some(table_version) =
                TableMetaLocationGenerator::location_table_version(snapshot_file)
            {
                table_version_set.insert(table_version);
            }
        }
        table_version_set
    }

    #[async_backtrace::framed]
    async fn get_reference_files_prefix(&self) -> Result<ReferencedFilesPrefix> {
        fn get_prefix(referenced_file: Option<String>) -> Option<String> {
            if let Some(referenced_file) = referenced_file {
                SnapshotsIO::get_s3_prefix_from_file(&referenced_file)
            } else {
                None
            }
        }

        let current_snapshot_segment = &self.current_snapshot.segments;
        let segments = if !current_snapshot_segment.is_empty() {
            vec![current_snapshot_segment[0].clone()]
        } else {
            vec![]
        };
        let locations_referenced = self
            .fuse_table
            .get_block_locations(self.ctx.clone(), &segments, false, false)
            .await?;

        let segment = segments.first().map(|segment| segment.0.clone());
        let segment_prefix = get_prefix(segment);
        let block_prefix = get_prefix(locations_referenced.block_location.iter().next().cloned());
        let block_index_prefix =
            get_prefix(locations_referenced.bloom_location.iter().next().cloned());

        Ok(ReferencedFilesPrefix {
            segment_prefix,
            block_prefix,
            block_index_prefix,
        })
    }

    // get all root gc snapshot file referenced files: segments\block\block index
    #[async_backtrace::framed]
    async fn get_root_gc_snapshot_reference_files(
        &self,
        root_gc_snapshot: &TableSnapshot,
    ) -> Result<SnapshotReferencedFileSet> {
        let prefix_set = self.get_reference_files_prefix().await?;

        let segments: HashSet<String> = root_gc_snapshot
            .segments
            .iter()
            .map(|(location, _)| location.clone())
            .collect();

        let locations_referenced = self
            .fuse_table
            .get_block_locations(self.ctx.clone(), &root_gc_snapshot.segments, false, false)
            .await?;
        let (blocks, blocks_index) = (
            locations_referenced.block_location,
            locations_referenced.bloom_location,
        );

        Ok(SnapshotReferencedFileSet {
            segments: (prefix_set.segment_prefix.clone(), segments),
            blocks: (prefix_set.block_prefix.clone(), blocks),
            blocks_index: (prefix_set.block_index_prefix, blocks_index),
        })
    }

    // return true if purge_files.len() >= DRY_RUN_LIMIT
    #[async_backtrace::framed]
    async fn batch_gc_files(
        &self,
        batch_purge_files: &[String],
        purge_files: &mut Option<&mut Vec<String>>,
    ) -> Result<bool> {
        if let Some(purge_files) = purge_files {
            purge_files.extend(batch_purge_files.to_vec());
            Ok(purge_files.len() >= DRY_RUN_LIMIT)
        } else {
            let files: HashSet<String> = batch_purge_files.iter().cloned().collect();
            self.fuse_table
                .try_purge_location_files(self.ctx.clone(), files)
                .await?;
            Ok(false)
        }
    }

    // return true if purge_files.len() >= DRY_RUN_LIMIT
    #[async_backtrace::framed]
    async fn gc_files_in_dir(
        &self,
        context: &VacuumContext,
        prefix_referenced_files: &PrefixAndReferencedFileSet,
        purge_files: &mut Option<&mut Vec<String>>,
    ) -> Result<bool> {
        let (prefix, referenced_files) = prefix_referenced_files;

        let prefix = if let Some(prefix) = prefix {
            prefix
        } else {
            return Ok(false);
        };

        let operator = self.fuse_table.get_operator();
        let mut ds = operator.lister_with(prefix).metakey(Metakey::Mode).await?;
        let keep_snapshot_table_versions = &context.keep_snapshot_table_versions;
        let mut batch_purge_files = Vec::with_capacity(BATCH_PURGE_FILE_NUM);
        let mut count = 0;
        while let Some(de) = ds.try_next().await? {
            let meta = de.metadata();
            if meta.mode() != EntryMode::FILE {
                warn!("found not snapshot file in {:}, found: {:?}", prefix, de);
                continue;
            }
            let location = de.path().to_string();

            let is_table_version_in_reference =
                match TableMetaLocationGenerator::location_table_version(&location) {
                    Some(table_version) => keep_snapshot_table_versions.contains(&table_version),
                    None => false,
                };

            // ignore file which is referenced
            if is_table_version_in_reference || referenced_files.contains(&location) {
                continue;
            }

            // batch gc files
            batch_purge_files.push(location);
            if batch_purge_files.len() >= BATCH_PURGE_FILE_NUM {
                count += batch_purge_files.len();
                let end = self.batch_gc_files(&batch_purge_files, purge_files).await?;
                batch_purge_files.clear();
                let status = format!(
                    "do_vacuum with table {}: do_gc_files_in_dir in prefix {}, count:{},cos:{} sec, end: {}",
                    self.fuse_table.get_table_info().name,
                    prefix,
                    count,
                    self.start.elapsed().as_secs(),
                    end
                );
                self.log_status(status);
                if end {
                    return Ok(end);
                }
            }
        }

        if !batch_purge_files.is_empty() {
            count += batch_purge_files.len();
            let end = self.batch_gc_files(&batch_purge_files, purge_files).await?;
            let status = format!(
                "do_vacuum with table {}: do_gc_files_in_dir in prefix {}, count:{},cos:{} sec, end: {}",
                self.fuse_table.get_table_info().name,
                prefix,
                count,
                self.start.elapsed().as_secs(),
                end
            );
            self.log_status(status);
            if end {
                return Ok(end);
            }
        }

        Ok(false)
    }

    #[async_backtrace::framed]
    async fn gc_files(
        &self,
        context: VacuumContext,
        purge_files: &mut Option<&mut Vec<String>>,
    ) -> Result<()> {
        let snapshot_files_with_time = &context.snapshot_files_with_time;
        let root_gc_snapshot_index = context.root_gc_snapshot_index;
        let mut purge_snapshot_files = vec![];
        if root_gc_snapshot_index < snapshot_files_with_time.len() - 1 {
            for snapshot_file_with_time in
                snapshot_files_with_time[root_gc_snapshot_index + 1..].iter()
            {
                let snapshot_location = snapshot_file_with_time.1.clone();
                purge_snapshot_files.push(snapshot_location);
            }
        }
        if let Some(purge_files) = purge_files {
            purge_files.extend(purge_snapshot_files.clone());
            if purge_files.len() > DRY_RUN_LIMIT {
                return Ok(());
            }
        }

        // purge unreference files
        let root_gc_snapshot_reference_files = &context.root_gc_snapshot_reference_files;
        let reference_file_set = vec![
            &root_gc_snapshot_reference_files.blocks_index,
            &root_gc_snapshot_reference_files.blocks,
            &root_gc_snapshot_reference_files.segments,
        ];
        for referenced_files in reference_file_set {
            if self
                .gc_files_in_dir(&context, referenced_files, purge_files)
                .await?
            {
                return Ok(());
            }
        }

        if purge_files.is_some() {
            return Ok(());
        }

        // purge old of retention time snapshot files
        let snapshots_to_be_purged: HashSet<String> =
            purge_snapshot_files.iter().cloned().collect();
        self.fuse_table
            .try_purge_location_files_and_cache::<TableSnapshot, _, _>(
                self.ctx.clone(),
                snapshots_to_be_purged,
            )
            .await?;
        let status = format!(
            "do_vacuum with table {}: purge snapshot files:{},cos:{} sec",
            self.fuse_table.get_table_info().name,
            purge_snapshot_files.len(),
            self.start.elapsed().as_secs(),
        );
        self.log_status(status);

        Ok(())
    }

    #[async_backtrace::framed]
    async fn gc(&self, purge_files: &mut Option<&mut Vec<String>>) -> Result<()> {
        let mut snapshot_files_with_time = self.list_snapshot_files_with_time().await?;
        if snapshot_files_with_time.is_empty() {
            self.log_status(format!(
                "do_vacuum with table {}: list_snapshot_files_with_time return empty",
                self.fuse_table.get_table_info().name
            ));
            return Ok(());
        }
        self.log_status(format!(
            "do_vacuum with table {}: list_snapshot_files_with_time:{}, cost:{} sec",
            self.fuse_table.get_table_info().name,
            snapshot_files_with_time.len(),
            self.start.elapsed().as_secs()
        ));

        let (root_gc_snapshot_index, root_gc_snapshot_location) = match self
            .get_root_gc_snapshot_file(&mut snapshot_files_with_time)
            .await?
        {
            Some(index) => {
                let root_gc_snapshot_location = snapshot_files_with_time[index].1.clone();
                self.log_status(format!(
                    "do_vacuum with table {}: get_root_gc_snapshot_file {:?}",
                    self.fuse_table.get_table_info().name,
                    root_gc_snapshot_location,
                ));
                (index, root_gc_snapshot_location)
            }
            None => {
                self.log_status(format!(
                    "do_vacuum with table {}: get_root_gc_snapshot_file return None",
                    self.fuse_table.get_table_info().name,
                ));
                return Ok(());
            }
        };

        let root_gc_snapshot = match self
            .fuse_table
            .read_table_snapshot_by_location(root_gc_snapshot_location.clone())
            .await?
        {
            Some(snapshot) => snapshot,
            None => {
                self.log_status(format!(
                    "do_vacuum with table {}: read root gc snapshot with read_table_snapshot_by_location {} return None",
                    self.fuse_table.get_table_info().name,
                    root_gc_snapshot_location
                ));
                return Ok(());
            }
        };

        let root_gc_snapshot_reference_files = self
            .get_root_gc_snapshot_reference_files(root_gc_snapshot.as_ref())
            .await?;
        self.log_status(format!(
            "do_vacuum with table {}: get_root_gc_snapshot_reference_files segment:{}, block:{}, index:{}, cost:{} sec",
            root_gc_snapshot_reference_files.segments.1.len(),
            root_gc_snapshot_reference_files.blocks.1.len(),
            root_gc_snapshot_reference_files.blocks_index.1.len(),
            self.fuse_table.get_table_info().name,
            self.start.elapsed().as_secs()
        ));

        let keep_snapshot_table_versions = self
            .get_keep_snapshot_table_versions(&snapshot_files_with_time, root_gc_snapshot_index);

        let context = VacuumContext {
            snapshot_files_with_time,
            root_gc_snapshot_index,
            root_gc_snapshot_reference_files,
            keep_snapshot_table_versions,
        };

        self.gc_files(context, purge_files).await?;
        self.log_status(format!(
            "do_vacuum with table {}: do_gc_files, cost:{} sec",
            self.fuse_table.get_table_info().name,
            self.start.elapsed().as_secs()
        ));

        Ok(())
    }

    #[async_backtrace::framed]
    fn log_status(&self, status: String) {
        self.ctx.set_status_info(&status);
        info!("{}", status);
    }
}

#[async_backtrace::framed]
pub async fn do_vacuum(
    fuse_table: &FuseTable,
    ctx: Arc<dyn TableContext>,
    retention_time: DateTime<Utc>,
    dry_run: bool,
) -> Result<Option<Vec<String>>> {
    let start = Instant::now();
    let retention = Duration::hours(ctx.get_settings().get_retention_period()? as i64);
    // use min(now - get_retention_period(), retention_time) as gc files retention time
    // to protect files that generated by txn which has not been committed being gc.
    let mut retention_time = std::cmp::min(chrono::Utc::now() - retention, retention_time);

    let table_info = fuse_table.get_table_info();
    let catalog = table_info.catalog();
    let catalog = ctx.get_catalog(catalog).await?;
    let table_id = fuse_table.get_table_info().ident.table_id;

    // Read the root snapshot location.
    let current_snapshot_location = match fuse_table.snapshot_loc().await? {
        Some(root_snapshot_location) => root_snapshot_location,
        None => {
            let status = format!(
                "do_vacuum with table {}: read root snapshot location return None",
                fuse_table.get_table_info().name,
            );
            ctx.set_status_info(&status);
            info!("{}", status);
            return Ok(None);
        }
    };
    let current_snapshot = match fuse_table
        .read_table_snapshot_by_location(current_snapshot_location.clone())
        .await?
    {
        Some(snapshot) => snapshot,
        None => return Ok(None),
    };

    // make sure retention_time =< current_snapshot.timestamp
    if let Some(timestamp) = current_snapshot.timestamp {
        retention_time = std::cmp::min(timestamp, retention_time);
        debug_assert!(timestamp >= retention_time);
    }

    let vacuum_operator = VacuumOperator {
        fuse_table: fuse_table.clone(),
        current_snapshot,
        current_snapshot_location,
        ctx,
        retention_time,
        start,
    };
    if dry_run {
        let lvt = catalog.get_table_lvt(table_id).await?;
        if let Some(lvt) = lvt.time {
            if lvt > retention_time {
                info!(
                    "when dry run vacuum table {:?}, lvt {:?} > retention_time {:?}",
                    table_info.name, lvt, retention_time
                );
                return Ok(Some(vec![]));
            }
        }

        let mut purge_files = Vec::with_capacity(DRY_RUN_LIMIT);
        let mut purge_files_opt = Some(&mut purge_files);
        vacuum_operator.gc(&mut purge_files_opt).await?;
        Ok(Some(purge_files))
    } else {
        let lvt = catalog.set_table_lvt(table_id, retention_time).await?;
        if lvt.time > retention_time {
            info!(
                "when vacuum table {:?}, lvt {:?} > retention_time {:?}",
                table_info.name, lvt.time, retention_time
            );
            return Ok(None);
        }
        let mut purge_files_opt = None;
        vacuum_operator.gc(&mut purge_files_opt).await?;

        Ok(None)
    }
}
