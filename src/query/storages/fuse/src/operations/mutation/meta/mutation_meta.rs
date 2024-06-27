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

use databend_common_expression::BlockMetaInfo;
use databend_storages_common_table_meta::meta::ClusterStatistics;

use crate::operations::common::BlockMetaIndex;
use crate::operations::mutation::CompactExtraInfo;
use crate::operations::mutation::DeletedSegmentInfo;
use crate::operations::LazyCompactedBlock;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub enum SerializeDataMeta {
    SerializeBlock(SerializeBlock),
    DeletedSegment(DeletedSegmentInfo),
    CompactExtras(CompactExtraInfo),
}

#[typetag::serde(name = "serialize_data_meta")]
impl BlockMetaInfo for SerializeDataMeta {
    fn equals(&self, _info: &Box<dyn BlockMetaInfo>) -> bool {
        unreachable!("SerializeDataMeta should not be compared")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum ClusterStatsGenType {
    Generally,
    WithOrigin(Option<ClusterStatistics>),
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct SerializeBlock {
    pub index: BlockMetaIndex,
    pub stats_type: ClusterStatsGenType,
    pub lazy_compacted_block: Option<LazyCompactedBlock>,
}

impl SerializeBlock {
    pub fn create(
        index: BlockMetaIndex,
        stats_type: ClusterStatsGenType,
        lazy_compacted_block: Option<LazyCompactedBlock>,
    ) -> SerializeBlock {
        SerializeBlock {
            index,
            stats_type,
            lazy_compacted_block,
        }
    }
}
