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

use databend_common_catalog::plan::PartInfoPtr;
use databend_common_expression::BlockMetaInfo;

use crate::io::BlockReadResult;
use crate::io::VirtualBlockReadResult;
use crate::operations::read::data_source_with_meta::DataSourceWithMeta;

pub enum ParquetDataSource {
    AggIndex((PartInfoPtr, BlockReadResult)),
    Normal((BlockReadResult, Option<VirtualBlockReadResult>)),
}

#[typetag::serde(name = "fuse_data_source")]
impl BlockMetaInfo for DataSourceWithMeta<ParquetDataSource> {
    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals DataSourceMeta")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unimplemented!("Unimplemented clone DataSourceMeta")
    }
}
