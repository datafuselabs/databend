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

use databend_common_expression::DataBlock;
use databend_common_storages_fuse::FUSE_TBL_BLOCK_PREFIX;
use databend_common_storages_fuse::FUSE_TBL_SEGMENT_PREFIX;
use databend_storages_common_table_meta::meta::uuid_from_date_time;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::meta::V5_OBJECT_KEY_PREFIX;
use uuid::Uuid;
#[derive(Clone)]
pub struct TableMetaLocationGenerator {
    prefix: String,
}

impl TableMetaLocationGenerator {
    pub fn with_prefix(prefix: String) -> Self {
        Self { prefix }
    }

    pub fn gen_block_location(
        &self,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> (Location, Uuid) {
        let part_uuid = uuid_from_date_time(table_meta_timestamps.base_timestamp);
        let location_path = format!(
            "{}/{}/{}{}_v{}.parquet",
            &self.prefix,
            FUSE_TBL_BLOCK_PREFIX,
            V5_OBJECT_KEY_PREFIX,
            part_uuid.as_simple(),
            DataBlock::VERSION,
        );

        ((location_path, DataBlock::VERSION), part_uuid)
    }

    pub fn gen_segment_info_location(&self, table_meta_timestamps: TableMetaTimestamps) -> String {
        let segment_uuid = uuid_from_date_time(table_meta_timestamps.base_timestamp);
        format!(
            "{}/{}/{}{}_v{}.mpk",
            &self.prefix,
            FUSE_TBL_SEGMENT_PREFIX,
            V5_OBJECT_KEY_PREFIX,
            segment_uuid.as_simple(),
            SegmentInfo::VERSION,
        )
    }
}
