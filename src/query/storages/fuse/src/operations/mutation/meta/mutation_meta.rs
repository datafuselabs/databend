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

use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ClusterStatistics;

use crate::operations::common::BlockMetaIndex;
use crate::operations::mutation::CompactExtraInfo;
use crate::operations::mutation::DeletedSegmentInfo;
use crate::BlockReadResult;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub enum SerializeDataMeta {
    SerializeBlock(SerializeBlock),
    DeletedSegment(DeletedSegmentInfo),
    CompactExtras(CompactExtraInfo),
}

#[typetag::serde(name = "serialize_data_meta")]
impl BlockMetaInfo for SerializeDataMeta {
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        SerializeDataMeta::downcast_ref_from(info).is_some_and(|other| self == other)
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

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct SerializeBlock {
    pub index: BlockMetaIndex,
    pub stats_type: ClusterStatsGenType,
}

impl SerializeBlock {
    pub fn create(index: BlockMetaIndex, stats_type: ClusterStatsGenType) -> Self {
        SerializeBlock { index, stats_type }
    }
}

pub enum CompactSourceMeta {
    Concat {
        read_res: Vec<BlockReadResult>,
        metas: Vec<Arc<BlockMeta>>,
        index: BlockMetaIndex,
    },
    Extras(CompactExtraInfo),
}

#[typetag::serde(name = "compact_data_source")]
impl BlockMetaInfo for CompactSourceMeta {
    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals CompactSourceMeta")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unimplemented!("Unimplemented clone CompactSourceMeta")
    }
}

impl std::fmt::Debug for CompactSourceMeta {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("CompactSourceMeta").finish()
    }
}

impl serde::Serialize for CompactSourceMeta {
    fn serialize<S>(&self, _: S) -> std::result::Result<S::Ok, S::Error>
    where S: serde::Serializer {
        unimplemented!("Unimplemented serialize DataSourceMeta")
    }
}

impl<'de> serde::Deserialize<'de> for CompactSourceMeta {
    fn deserialize<D>(_: D) -> std::result::Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        unimplemented!("Unimplemented deserialize DataSourceMeta")
    }
}
