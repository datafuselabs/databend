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
use std::time::Instant;

use databend_common_base::base::ProgressValues;
use databend_common_catalog::plan::gen_mutation_stream_meta;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_metrics::storage::*;
use databend_common_pipeline_sources::PrefetchAsyncSource;
use databend_common_pipeline_transforms::processors::BlockMetaTransform;
use databend_common_pipeline_transforms::processors::UnknownMode;
use databend_common_sql::StreamContext;
use databend_storages_common_io::ReadSettings;

use crate::io::BlockReader;
use crate::operations::ClusterStatsGenType;
use crate::operations::CompactBlockPartInfo;
use crate::operations::CompactSourceMeta;
use crate::operations::SerializeBlock;
use crate::operations::SerializeDataMeta;
use crate::FuseStorageFormat;

pub struct CompactSource {
    ctx: Arc<dyn TableContext>,
    block_reader: Arc<BlockReader>,
    prefetch_num: usize,
}

impl CompactSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        block_reader: Arc<BlockReader>,
        prefetch_num: usize,
    ) -> Self {
        Self {
            ctx,
            block_reader,
            prefetch_num,
        }
    }
}

#[async_trait::async_trait]
impl PrefetchAsyncSource for CompactSource {
    const NAME: &'static str = "CompactSource";

    const SKIP_EMPTY_DATA_BLOCK: bool = false;

    fn is_full(&self, prefetched: &[DataBlock]) -> bool {
        prefetched.len() >= self.prefetch_num
    }

    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        let part = match self.ctx.get_partition() {
            Some(part) => part,
            None => return Ok(None),
        };

        let part = CompactBlockPartInfo::from_part(&part)?;
        let meta = match part {
            CompactBlockPartInfo::CompactTaskInfo(task) => {
                let mut task_futures = Vec::new();
                for block in &task.blocks {
                    let settings = ReadSettings::from_ctx(&self.ctx)?;
                    let block_reader = self.block_reader.clone();
                    let block = block.clone();
                    // read block in parallel.
                    task_futures.push(async move {
                        databend_common_base::runtime::spawn(async move {
                            // Perf
                            {
                                metrics_inc_compact_block_read_nums(1);
                                metrics_inc_compact_block_read_bytes(block.block_size);
                            }

                            block_reader
                                .read_columns_data_by_merge_io(
                                    &settings,
                                    &block.location.0,
                                    &block.col_metas,
                                    &None,
                                )
                                .await
                        })
                        .await
                        .unwrap()
                    });
                }

                let start = Instant::now();

                let read_res = futures::future::try_join_all(task_futures).await?;
                // Perf.
                {
                    metrics_inc_compact_block_read_milliseconds(start.elapsed().as_millis() as u64);
                }
                Box::new(CompactSourceMeta::Concat {
                    read_res,
                    metas: task.blocks.clone(),
                    index: task.index.clone(),
                })
            }
            CompactBlockPartInfo::CompactExtraInfo(extra) => {
                Box::new(CompactSourceMeta::Extras(extra.clone()))
            }
        };
        Ok(Some(DataBlock::empty_with_meta(meta)))
    }
}

pub struct CompactTransform {
    ctx: Arc<dyn TableContext>,
    block_reader: Arc<BlockReader>,
    storage_format: FuseStorageFormat,
    stream_ctx: Option<StreamContext>,
}

impl CompactTransform {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        block_reader: Arc<BlockReader>,
        storage_format: FuseStorageFormat,
        stream_ctx: Option<StreamContext>,
    ) -> Self {
        Self {
            ctx,
            block_reader,
            storage_format,
            stream_ctx,
        }
    }
}

#[async_trait::async_trait]
impl BlockMetaTransform<CompactSourceMeta> for CompactTransform {
    const UNKNOWN_MODE: UnknownMode = UnknownMode::Pass;
    const NAME: &'static str = "CompactTransform";

    fn transform(&mut self, meta: CompactSourceMeta) -> Result<Vec<DataBlock>> {
        match meta {
            CompactSourceMeta::Concat {
                read_res,
                metas,
                index,
            } => {
                let blocks = read_res
                    .into_iter()
                    .zip(metas.into_iter())
                    .map(|(data, meta)| {
                        let mut block = self.block_reader.deserialize_chunks_with_meta(
                            &meta,
                            &self.storage_format,
                            data,
                        )?;

                        if let Some(stream_ctx) = &self.stream_ctx {
                            let stream_meta = gen_mutation_stream_meta(None, &meta.location.0)?;
                            block = stream_ctx.apply(block, &stream_meta)?;
                        }
                        Ok(block)
                    })
                    .collect::<Result<Vec<_>>>()?;

                // concat blocks.
                let block = if blocks.len() == 1 {
                    blocks[0].convert_to_full()
                } else {
                    DataBlock::concat(&blocks)?
                };

                let meta = Box::new(SerializeDataMeta::SerializeBlock(SerializeBlock::create(
                    index,
                    ClusterStatsGenType::Generally,
                )));
                let new_block = block.add_meta(Some(meta))?;

                let progress_values = ProgressValues {
                    rows: new_block.num_rows(),
                    bytes: new_block.memory_size(),
                };
                self.ctx.get_write_progress().incr(&progress_values);
                Ok(vec![new_block])
            }
            CompactSourceMeta::Extras(extra) => {
                let meta = Box::new(SerializeDataMeta::CompactExtras(extra));
                Ok(vec![DataBlock::empty_with_meta(meta)])
            }
        }
    }
}
