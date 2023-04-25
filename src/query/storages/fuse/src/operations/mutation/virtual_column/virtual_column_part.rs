// Copyright 2023 Datafuse Labs.
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

use common_catalog::plan::PartInfo;
use common_catalog::plan::PartInfoPtr;
use common_exception::ErrorCode;
use common_exception::Result;
use storages_common_table_meta::meta::BlockMeta;

use crate::operations::merge_into::mutation_meta::mutation_log::BlockMetaIndex;

#[derive(serde::Serialize, serde::Deserialize, PartialEq)]
pub struct VirtualColumnPartInfo {
    pub block: Arc<BlockMeta>,
    pub index: BlockMetaIndex,
}

#[typetag::serde(name = "virtual_column")]
impl PartInfo for VirtualColumnPartInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        match info.as_any().downcast_ref::<VirtualColumnPartInfo>() {
            None => false,
            Some(other) => self == other,
        }
    }

    fn hash(&self) -> u64 {
        0
    }
}

impl VirtualColumnPartInfo {
    pub fn create(block: Arc<BlockMeta>, index: BlockMetaIndex) -> PartInfoPtr {
        Arc::new(Box::new(VirtualColumnPartInfo { block, index }))
    }

    pub fn from_part(info: &PartInfoPtr) -> Result<&VirtualColumnPartInfo> {
        match info.as_any().downcast_ref::<VirtualColumnPartInfo>() {
            Some(part_ref) => Ok(part_ref),
            None => Err(ErrorCode::Internal(
                "Cannot downcast from PartInfo to VirtualColumnPartInfo.",
            )),
        }
    }
}
