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

use std::marker::PhantomData;

use common_exception::Result;
use common_expression::DataBlock;
use storages_common_table_meta::meta::snapshot_id_from_string;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::SnapshotId;
use storages_common_table_meta::meta::SnapshotVersion;
use storages_common_table_meta::meta::TableSnapshotStatisticsVersion;
use storages_common_table_meta::meta::TableVersion;
use storages_common_table_meta::meta::Versioned;
use uuid::Uuid;

use crate::constants::FUSE_TBL_BLOCK_PREFIX;
use crate::constants::FUSE_TBL_SEGMENT_PREFIX;
use crate::constants::FUSE_TBL_SNAPSHOT_PREFIX;
use crate::constants::FUSE_TBL_SNAPSHOT_STATISTICS_PREFIX;
use crate::constants::FUSE_TBL_VIRTUAL_BLOCK_PREFIX;
use crate::index::filters::BlockFilter;
use crate::FUSE_TBL_AGG_INDEX_PREFIX;
use crate::FUSE_TBL_LAST_SNAPSHOT_HINT;
use crate::FUSE_TBL_XOR_BLOOM_INDEX_PREFIX;

static SNAPSHOT_V0: SnapshotVersion = SnapshotVersion::V0(PhantomData);
static SNAPSHOT_V1: SnapshotVersion = SnapshotVersion::V1(PhantomData);
static SNAPSHOT_V2: SnapshotVersion = SnapshotVersion::V2(PhantomData);
static SNAPSHOT_V3: SnapshotVersion = SnapshotVersion::V3(PhantomData);
static SNAPSHOT_V4: SnapshotVersion = SnapshotVersion::V4(PhantomData);
static SNAPSHOT_V5: SnapshotVersion = SnapshotVersion::V5(PhantomData);

static SNAPSHOT_STATISTICS_V0: TableSnapshotStatisticsVersion =
    TableSnapshotStatisticsVersion::V0(PhantomData);

#[derive(Clone)]
pub struct TableMetaLocationGenerator {
    prefix: String,
    part_prefix: String,
    snapshot_table_version: TableVersion,
}

impl TableMetaLocationGenerator {
    pub fn new(prefix: String, snapshot_table_version: TableVersion) -> Self {
        Self {
            prefix,
            part_prefix: "".to_string(),
            snapshot_table_version,
        }
    }

    pub fn with_part_prefix(mut self, part_prefix: String) -> Self {
        self.part_prefix = part_prefix;
        self
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn part_prefix(&self) -> &str {
        &self.part_prefix
    }

    pub fn gen_block_location_of_v4(&self) -> (Location, Uuid) {
        let part_uuid = Uuid::new_v4();
        let location_path = format!(
            "{}/{}/{}{}_v{}.parquet",
            &self.prefix,
            FUSE_TBL_BLOCK_PREFIX,
            &self.part_prefix,
            part_uuid.as_simple(),
            2,
        );

        ((location_path, DataBlock::VERSION), part_uuid)
    }

    pub fn gen_block_location(&self) -> (Location, Uuid) {
        let part_uuid = Uuid::new_v4();
        let location_path = format!(
            "{}/{}/{}{}_{}_v{}.parquet",
            &self.prefix,
            FUSE_TBL_BLOCK_PREFIX,
            &self.part_prefix,
            part_uuid.as_simple(),
            self.snapshot_table_version,
            DataBlock::VERSION,
        );

        ((location_path, DataBlock::VERSION), part_uuid)
    }

    pub fn block_bloom_index_location_of_v4(&self, block_id: &Uuid) -> Location {
        (
            format!(
                "{}/{}/{}_v{}.parquet",
                &self.prefix,
                FUSE_TBL_XOR_BLOOM_INDEX_PREFIX,
                block_id.as_simple(),
                BlockFilter::VERSION,
            ),
            3,
        )
    }

    pub fn block_bloom_index_location(&self, block_id: &Uuid) -> Location {
        (
            format!(
                "{}/{}/{}_{}_v{}.parquet",
                &self.prefix,
                FUSE_TBL_XOR_BLOOM_INDEX_PREFIX,
                block_id.as_simple(),
                self.snapshot_table_version,
                BlockFilter::VERSION,
            ),
            BlockFilter::VERSION,
        )
    }

    pub fn gen_segment_info_location_of_v4(&self) -> String {
        let segment_uuid = Uuid::new_v4().simple().to_string();
        format!(
            "{}/{}/{}_v4.mpk",
            &self.prefix, FUSE_TBL_SEGMENT_PREFIX, segment_uuid,
        )
    }

    pub fn gen_segment_info_location(&self) -> String {
        let segment_uuid = Uuid::new_v4().simple().to_string();
        format!(
            "{}/{}/{}_{}_v{}.mpk",
            &self.prefix,
            FUSE_TBL_SEGMENT_PREFIX,
            segment_uuid,
            self.snapshot_table_version,
            SegmentInfo::VERSION,
        )
    }

    pub fn gen_snapshot_location_of_v4(&self, id: &Uuid, version: u64) -> Result<String> {
        let snapshot_version = SnapshotVersion::try_from(version)?;
        Ok(snapshot_version.create(id, &self.prefix, None))
    }

    pub fn gen_snapshot_location(
        &self,
        id: &Uuid,
        version: u64,
        table_version: Option<TableVersion>,
    ) -> Result<String> {
        let snapshot_version = SnapshotVersion::try_from(version)?;
        Ok(snapshot_version.create(id, &self.prefix, table_version))
    }

    pub fn snapshot_version(location: impl AsRef<str>) -> u64 {
        if location.as_ref().ends_with(SNAPSHOT_V5.suffix().as_str()) {
            SNAPSHOT_V5.version()
        } else if location.as_ref().ends_with(SNAPSHOT_V4.suffix().as_str()) {
            SNAPSHOT_V4.version()
        } else if location.as_ref().ends_with(SNAPSHOT_V3.suffix().as_str()) {
            SNAPSHOT_V3.version()
        } else if location.as_ref().ends_with(SNAPSHOT_V2.suffix().as_str()) {
            SNAPSHOT_V2.version()
        } else if location.as_ref().ends_with(SNAPSHOT_V1.suffix().as_str()) {
            SNAPSHOT_V1.version()
        } else {
            SNAPSHOT_V0.version()
        }
    }

    pub fn location_table_version(location: &str) -> Option<TableVersion> {
        let v: Vec<&str> = location.split('/').collect();
        let v: Vec<&str> = v[v.len() - 1].split('_').collect();
        if v.len() > 1 {
            match v[1].parse() {
                Ok(v) => Some(v),
                Err(_) => None,
            }
        } else {
            None
        }
    }

    pub fn snapshot_id_from_location(location: &str) -> Result<SnapshotId> {
        let v: Vec<&str> = location.split('/').collect();
        let v: Vec<&str> = v[v.len() - 1].split('_').collect();
        let snapshot_str = v[0];
        snapshot_id_from_string(snapshot_str)
    }

    pub fn snapshot_statistics_location_from_uuid(
        &self,
        id: &Uuid,
        version: u64,
        table_version: Option<TableVersion>,
    ) -> Result<String> {
        let statistics_version = TableSnapshotStatisticsVersion::try_from(version)?;
        Ok(statistics_version.create(id, &self.prefix, table_version))
    }

    pub fn snapshot_statistics_version(_location: impl AsRef<str>) -> u64 {
        SNAPSHOT_STATISTICS_V0.version()
    }

    pub fn gen_last_snapshot_hint_location(&self) -> String {
        format!("{}/{}", &self.prefix, FUSE_TBL_LAST_SNAPSHOT_HINT)
    }

    pub fn gen_virtual_block_location(location: &str) -> String {
        location.replace(FUSE_TBL_BLOCK_PREFIX, FUSE_TBL_VIRTUAL_BLOCK_PREFIX)
    }

    pub fn gen_agg_index_location_from_block_location(loc: &str, index_id: u64) -> String {
        let splits = loc.split('/').collect::<Vec<_>>();
        let len = splits.len();
        let prefix = splits[..len - 2].join("/");
        let block_name = splits[len - 1];
        format!("{prefix}/{FUSE_TBL_AGG_INDEX_PREFIX}/{index_id}/{block_name}")
    }
}

trait SnapshotLocationCreator {
    fn create(
        &self,
        id: &Uuid,
        prefix: impl AsRef<str>,
        table_version: Option<TableVersion>,
    ) -> String;
    fn suffix(&self) -> String;
}

impl SnapshotLocationCreator for SnapshotVersion {
    fn create(
        &self,
        id: &Uuid,
        prefix: impl AsRef<str>,
        table_version: Option<TableVersion>,
    ) -> String {
        match table_version {
            Some(table_version) => format!(
                "{}/{}/{}_{}{}",
                prefix.as_ref(),
                FUSE_TBL_SNAPSHOT_PREFIX,
                id.simple(),
                table_version,
                self.suffix(),
            ),
            None => format!(
                "{}/{}/{}{}",
                prefix.as_ref(),
                FUSE_TBL_SNAPSHOT_PREFIX,
                id.simple(),
                self.suffix(),
            ),
        }
    }

    fn suffix(&self) -> String {
        match self {
            SnapshotVersion::V0(_) => "".to_string(),
            SnapshotVersion::V1(_) => "_v1.json".to_string(),
            SnapshotVersion::V2(_) => "_v2.json".to_string(),
            SnapshotVersion::V3(_) => "_v3.bincode".to_string(),
            SnapshotVersion::V4(_) => "_v4.mpk".to_string(),
            SnapshotVersion::V5(_) => "_v5.mpk".to_string(),
            SnapshotVersion::V6(_) => "_v6.mpk".to_string(),
        }
    }
}

impl SnapshotLocationCreator for TableSnapshotStatisticsVersion {
    fn create(
        &self,
        id: &Uuid,
        prefix: impl AsRef<str>,
        table_version: Option<TableVersion>,
    ) -> String {
        match table_version {
            Some(table_version) => format!(
                "{}/{}/{}_{}{}",
                prefix.as_ref(),
                FUSE_TBL_SNAPSHOT_STATISTICS_PREFIX,
                id.simple(),
                table_version,
                self.suffix(),
            ),
            None => format!(
                "{}/{}/{}{}",
                prefix.as_ref(),
                FUSE_TBL_SNAPSHOT_STATISTICS_PREFIX,
                id.simple(),
                self.suffix(),
            ),
        }
    }

    fn suffix(&self) -> String {
        match self {
            TableSnapshotStatisticsVersion::V0(_) => "_ts_v0.json".to_string(),
            TableSnapshotStatisticsVersion::V1(_) => "_ts_v5.json".to_string(),
        }
    }
}
