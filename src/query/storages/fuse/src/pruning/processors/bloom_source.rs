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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;

use async_channel::Receiver;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_sql::BloomIndexColumns;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_table_meta::meta::BlockMeta;
use opendal::Operator;
use serde::Deserialize;
use serde::Serialize;

use crate::io::BloomBlockFilterReader;
use crate::pruning::BloomPrunerCreator;

struct BloomSource {
    block_meta_receiver: Receiver<Arc<BlockMeta>>,
    bloom_pruner: BloomPrunerCreator,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct BloomFilterBytes {
    bytes: Vec<Vec<u8>>,
}

#[typetag::serde(name = "bloom_filter_bytes")]
impl BlockMetaInfo for BloomFilterBytes {
    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("equals is unimplemented for SegmentBytes")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

#[async_trait::async_trait]
impl AsyncSource for BloomSource {
    const NAME: &'static str = "bloom source";

    #[async_trait::unboxed_simple]
    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        let block_meta = self.block_meta_receiver.recv().await;
        match block_meta {
            Ok(block_meta) => {
                let column_ids = block_meta.col_metas.keys().cloned().collect::<Vec<_>>();
                Ok(Some(DataBlock::empty_with_meta(Box::new(
                    BloomFilterBytes {
                        bytes: self
                            .bloom_pruner
                            .load_filters_without_deserialize(
                                &block_meta.bloom_filter_index_location.as_ref().unwrap(),
                                block_meta.block_size,
                                column_ids,
                            )
                            .await?,
                    },
                ))))
            }
            Err(_) => Ok(None), // channel is empty and closed.
        }
    }
}
