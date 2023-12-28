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

use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::InternalColumnMeta;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::TopK;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_pipeline_core::Pipeline;

use crate::fuse_part::FusePartInfo;
use crate::io::AggIndexReader;
use crate::io::BlockReader;
use crate::io::VirtualColumnReader;
use crate::operations::read::DeserializeDataTransform;
use crate::operations::read::NativeDeserializeDataTransform;
use crate::operations::read::ReadNativeDataSource;
use crate::operations::read::ReadParquetDataSource;

#[allow(clippy::too_many_arguments)]
pub fn build_fuse_native_source_pipeline(
    ctx: Arc<dyn TableContext>,
    pipeline: &mut Pipeline,
    block_reader: Arc<BlockReader>,
    plan: &DataSourcePlan,
    topk: Option<TopK>,
    index_reader: Arc<Option<AggIndexReader>>,
    virtual_reader: Arc<Option<VirtualColumnReader>>,
) -> Result<()> {
    match block_reader.support_blocking_api() {
        true => {
            pipeline.add_transform(|input, output| {
                ReadNativeDataSource::<true>::create(
                    ctx.clone(),
                    input,
                    output,
                    block_reader.clone(),
                    index_reader.clone(),
                    virtual_reader.clone(),
                )
            })?;
        }
        false => {
            pipeline.add_transform(|input, output| {
                ReadNativeDataSource::<false>::create(
                    ctx.clone(),
                    input,
                    output,
                    block_reader.clone(),
                    index_reader.clone(),
                    virtual_reader.clone(),
                )
            })?;
        }
    };

    pipeline.add_transform(|transform_input, transform_output| {
        NativeDeserializeDataTransform::create(
            ctx.clone(),
            block_reader.clone(),
            plan,
            topk.clone(),
            transform_input,
            transform_output,
            index_reader.clone(),
            virtual_reader.clone(),
        )
    })
}

#[allow(clippy::too_many_arguments)]
pub fn build_fuse_parquet_source_pipeline(
    ctx: Arc<dyn TableContext>,
    pipeline: &mut Pipeline,
    block_reader: Arc<BlockReader>,
    plan: &DataSourcePlan,
    index_reader: Arc<Option<AggIndexReader>>,
    virtual_reader: Arc<Option<VirtualColumnReader>>,
) -> Result<()> {
    match block_reader.support_blocking_api() {
        true => {
            pipeline.add_transform(|input, output| {
                ReadParquetDataSource::<true>::create(
                    ctx.clone(),
                    input,
                    output,
                    block_reader.clone(),
                    index_reader.clone(),
                    virtual_reader.clone(),
                )
            })?;
        }
        false => {
            pipeline.add_transform(|input, output| {
                ReadParquetDataSource::<false>::create(
                    ctx.clone(),
                    input,
                    output,
                    block_reader.clone(),
                    index_reader.clone(),
                    virtual_reader.clone(),
                )
            })?;
        }
    };

    pipeline.add_transform(|transform_input, transform_output| {
        DeserializeDataTransform::create(
            ctx.clone(),
            block_reader.clone(),
            plan,
            transform_input,
            transform_output,
            index_reader.clone(),
            virtual_reader.clone(),
        )
    })
}

#[allow(unused)]
pub fn dispatch_partitions(
    ctx: Arc<dyn TableContext>,
    plan: &DataSourcePlan,
    max_streams: usize,
) -> Vec<VecDeque<PartInfoPtr>> {
    let mut results = Vec::with_capacity(max_streams);
    // Lazy part, we can dispatch them now.
    if plan.parts.is_lazy {
        return results;
    }

    results = vec![VecDeque::new(); max_streams];
    const BATCH_SIZE: usize = 64;
    let mut partitions = Vec::with_capacity(BATCH_SIZE);
    loop {
        let p = ctx.get_partitions(BATCH_SIZE);
        if p.is_empty() {
            break;
        }
        partitions.extend(p);
    }

    // that means the partition is lazy
    if partitions.is_empty() {
        return results;
    }

    for (i, part) in partitions.iter().enumerate() {
        results[i % max_streams].push_back(part.clone());
    }
    results
}

#[allow(unused)]
pub fn adjust_threads_and_request(
    is_native: bool,
    mut max_threads: usize,
    mut max_io_requests: usize,
    plan: &DataSourcePlan,
) -> (usize, usize) {
    if !plan.parts.is_lazy {
        let mut block_nums = plan.parts.partitions.len();

        // If the read bytes of a partition is small enough, less than 16k rows
        // we will not use an extra heavy thread to process it.
        // now only works for native reader
        static MIN_ROWS_READ_PER_THREAD: u64 = 16 * 1024;
        if is_native {
            plan.parts.partitions.iter().for_each(|part| {
                if let Some(part) = part.as_any().downcast_ref::<FusePartInfo>() {
                    let to_read_rows = part
                        .columns_meta
                        .values()
                        .map(|meta| meta.read_rows(part.range()))
                        .find(|rows| *rows > 0)
                        .unwrap_or(part.nums_rows as u64);

                    if to_read_rows < MIN_ROWS_READ_PER_THREAD {
                        block_nums -= 1;
                    }
                }
            });
        }

        // At least max(1/8 of the original parts, 1), in case of too many small partitions but io threads is just one.
        block_nums = std::cmp::max(block_nums, plan.parts.partitions.len() / 8);
        block_nums = std::cmp::max(block_nums, 1);

        max_threads = std::cmp::min(max_threads, block_nums);
        max_io_requests = std::cmp::min(max_io_requests, block_nums);
    }
    (max_threads, max_io_requests)
}

pub(crate) fn fill_internal_column_meta(
    data_block: DataBlock,
    fuse_part: &FusePartInfo,
    offsets: Option<Vec<usize>>,
    base_block_ids: Option<Scalar>,
) -> Result<DataBlock> {
    // Fill `BlockMetaInfoPtr` if query internal columns
    let block_meta = fuse_part.block_meta_index().unwrap();
    let internal_column_meta = InternalColumnMeta {
        segment_idx: block_meta.segment_idx,
        block_id: block_meta.block_id,
        block_location: block_meta.block_location.clone(),
        segment_location: block_meta.segment_location.clone(),
        snapshot_location: block_meta.snapshot_location.clone(),
        offsets,
        base_block_ids,
    };

    let meta: Option<BlockMetaInfoPtr> = Some(Box::new(internal_column_meta));
    data_block.add_meta(meta)
}
