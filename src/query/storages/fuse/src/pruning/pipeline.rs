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

use databend_common_exception::Result;
use databend_common_pipeline_core::Pipeline;

use super::processors::range_index_prune_sink::InverseRangeIndexContext;
use super::processors::range_index_prune_sink::RangeIndexPruneSink;
use super::processors::segment_source::SegmentSource;
use super::FusePruner;
use super::PruningContext;

pub fn build_pruning_pipelines(fuse_pruner: FusePruner) -> Result<Vec<Pipeline>> {
    let mut pipelines = vec![];

    let FusePruner {
        max_concurrency,
        table_schema,
        pruning_ctx,
        push_down,
        inverse_range_index,
        deleted_segments,
    } = fuse_pruner;
    let PruningContext {
        ctx,
        dal,
        pruning_runtime,
        pruning_semaphore,
        limit_pruner,
        range_pruner,
        bloom_pruner,
        page_pruner,
        internal_column_pruner,
        pruning_stats,
    } = pruning_ctx.as_ref();

    let (sender, receiver) = async_channel::unbounded();
    let (inverse_range_index_context, whole_block_delete_receiver, whole_segment_delete_recevier) =
        match inverse_range_index {
            Some(inverse_range_index) => {
                let (whole_block_delete_sender, whole_block_delete_receiver) =
                    async_channel::unbounded();
                let (whole_segment_delete_sender, whole_segment_delete_recevier) =
                    async_channel::unbounded();
                let inverse_range_index_context = Some(Arc::new(InverseRangeIndexContext {
                    whole_block_delete_sender,
                    whole_segment_delete_sender,
                    inverse_range_index: inverse_range_index.clone(),
                }));
                (
                    inverse_range_index_context,
                    Some(whole_block_delete_receiver),
                    Some(whole_segment_delete_recevier),
                )
            }
            None => (None, None, None),
        };

    let mut range_index_pruning_pipeline = Pipeline::create();
    range_index_pruning_pipeline.set_max_threads(ctx.get_settings().get_max_threads()? as usize);
    range_index_pruning_pipeline.add_source(
        |output| {
            SegmentSource::create(
                ctx.clone(),
                dal.clone(),
                internal_column_pruner.clone(),
                output,
            )
        },
        fuse_pruner.max_concurrency,
    );
    range_index_pruning_pipeline.add_sink(|input| {
        let sender = sender.clone();
        let inverse_range_index_context = inverse_range_index_context.clone();
        RangeIndexPruneSink::create(
            ctx.clone(),
            input,
            sender,
            table_schema.clone(),
            range_pruner.clone(),
            internal_column_pruner.clone(),
            inverse_range_index_context,
        )
    });
    pipelines.push(range_index_pruning_pipeline);

    let mut bloom_index_pruning_pipeline = Pipeline::create();
    bloom_index_pruning_pipeline.set_max_threads(ctx.get_settings().get_max_threads()? as usize);
    pipelines.push(bloom_index_pruning_pipeline);

    Ok(pipelines)
}
