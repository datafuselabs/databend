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

#![allow(clippy::too_many_arguments)]

mod compression;
mod current;
mod format;
mod statistics;
mod utils;
mod v0;
mod v1;
mod v2;
mod v3;
mod v4;
mod v5;
mod versions;
pub use compression::Compression;
// table meta types of current version
pub use current::*;
pub(crate) use format::load_json;
pub(crate) use format::MetaCompression;
pub(crate) use format::MetaEncoding;
pub use format::NUM_BLOCK_ID_BITS;
pub use statistics::*;
// export legacy versioned table meta types locally,
// currently, used by versioned readers only
pub(crate) use testing::*;
pub use utils::trim_v5_object_prefix;
pub use utils::try_extract_uuid_str_from_path;
pub use utils::uuid_from_date_time;
pub use utils::TableMetaTimestamps;
pub use utils::V5_OBJECT_KEY_PREFIX;
pub(crate) use utils::*;
pub use versions::testify_version;
pub use versions::SegmentInfoVersion;
pub use versions::SnapshotVersion;
pub use versions::TableSnapshotStatisticsVersion;
pub use versions::Versioned;

// - export legacy versioned table meta types for testing purposes
//   currently, only used by crate `test_kits`
// - export meta encoding to benchmarking tests
pub mod testing {
    pub use super::format::MetaEncoding;
    pub use super::v0::statistics::Statistics as StatisticsV0;
    pub use super::v1::TableSnapshot as TableSnapshotV1;
    pub use super::v2::SegmentInfo as SegmentInfoV2;
    pub use super::v2::TableSnapshot as TableSnapshotV2;
    pub use super::v3::SegmentInfo as SegmentInfoV3;
    pub use super::v3::TableSnapshot as TableSnapshotV3;
    pub use super::v4::TableSnapshot as TableSnapshotV4;
    pub use super::v5::TableSnapshot as TableSnapshotV5;
}
