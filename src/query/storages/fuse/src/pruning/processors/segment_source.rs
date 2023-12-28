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

use databend_common_base::base::tokio::io::AsyncReadExt;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::DataBlock;
use databend_common_expression::SEGMENT_NAME_COL_NAME;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;
use databend_storages_common_pruner::InternalColumnPruner;
use databend_storages_common_table_meta::meta::Location;
use opendal::Operator;
use serde::Deserialize;
use serde::Serialize;

use crate::FuseLazyPartInfo;

pub struct SegmentSource {
    ctx: Arc<dyn TableContext>,
    dal: Operator,
    internal_column_pruner: Option<Arc<InternalColumnPruner>>,
}

impl SegmentSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        dal: Operator,
        internal_column_pruner: Option<Arc<InternalColumnPruner>>,
        output: Arc<OutputPort>,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx, output, Self {
            ctx,
            dal,
            internal_column_pruner,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for SegmentSource {
    const NAME: &'static str = "segment source";

    #[async_trait::unboxed_simple]
    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        loop {
            let part = self.ctx.get_partition();
            if part.is_none() {
                return Ok(None);
            }
            let part = part.unwrap();
            let fuse_part = part.as_any().downcast_ref::<FuseLazyPartInfo>().unwrap();
            if let Some(p) = self.internal_column_pruner.as_ref() {
                if !p.should_keep(SEGMENT_NAME_COL_NAME, &fuse_part.segment_location.0) {
                    continue;
                }
            }
            let mut reader = self.dal.reader(&fuse_part.segment_location.0).await?;
            let mut buffer = vec![];
            reader.read_to_end(&mut buffer).await?;
            return Ok(Some(DataBlock::empty_with_meta(Box::new(SegmentBytes {
                bytes: buffer,
                segment_index: fuse_part.segment_index,
                segment_location: fuse_part.segment_location.clone(),
            }))));
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SegmentBytes {
    pub bytes: Vec<u8>,
    pub segment_index: usize,
    pub segment_location: Location,
}

#[typetag::serde(name = "segment_bytes")]
impl BlockMetaInfo for SegmentBytes {
    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("equals is unimplemented for SegmentBytes")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}
