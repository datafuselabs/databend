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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::runtime::execute_futures_in_parallel;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::BlockThresholds;
use databend_common_expression::DataBlock;
use databend_common_metrics::storage::metrics_inc_recluster_write_block_nums;
use databend_common_pipeline_transforms::processors::AsyncAccumulatingTransform;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::Versioned;
use itertools::Itertools;
use opendal::Operator;

use crate::io::SegmentsIO;
use crate::io::SerializedSegment;
use crate::io::TableMetaLocationGenerator;
use crate::operations::common::AbortOperation;
use crate::operations::common::CommitMeta;
use crate::operations::common::ConflictResolveContext;
use crate::operations::common::SnapshotChanges;
use crate::statistics::reduce_block_metas;
use crate::statistics::reducers::merge_statistics_mut;
use crate::statistics::sort_by_cluster_stats;
use crate::FuseTable;
use crate::DEFAULT_BLOCK_PER_SEGMENT;
use crate::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;

pub struct ReclusterAggregator {
    ctx: Arc<dyn TableContext>,
    dal: Operator,
    location_gen: TableMetaLocationGenerator,

    default_cluster_key: u32,
    block_thresholds: BlockThresholds,
    block_per_seg: usize,
    start_time: Instant,

    abort_operation: AbortOperation,
    merged_blocks: Vec<Arc<BlockMeta>>,

    removed_segment_indexes: Vec<usize>,
    removed_statistics: Statistics,
    table_id: u64,
}

#[async_trait::async_trait]
impl AsyncAccumulatingTransform for ReclusterAggregator {
    const NAME: &'static str = "ReclusterAggregator";

    #[async_backtrace::framed]
    async fn transform(&mut self, data: DataBlock) -> Result<Option<DataBlock>> {
        // gather the input data.
        if let Some(meta) = data.get_owned_meta().and_then(BlockMeta::downcast_from) {
            self.abort_operation.add_block(&meta);
            self.merged_blocks.push(Arc::new(meta));
            // Refresh status
            {
                metrics_inc_recluster_write_block_nums();
                let status = format!(
                    "recluster: generate new blocks:{}, cost:{} sec",
                    self.abort_operation.blocks.len(),
                    self.start_time.elapsed().as_secs()
                );
                self.ctx.set_status_info(&status);
            }
        }
        // no partial output
        Ok(None)
    }

    #[async_backtrace::framed]
    async fn on_finish(&mut self, _output: bool) -> Result<Option<DataBlock>> {
        let mut new_segments = self.apply().await?;

        let default_cluster_key = Some(self.default_cluster_key);
        let new_segments_len = new_segments.len();
        let removed_segments_len = self.removed_segment_indexes.len();
        let replaced_segments_len = new_segments_len.min(removed_segments_len);
        let mut merged_statistics = Statistics::default();
        let mut appended_segments = Vec::new();
        let mut replaced_segments = HashMap::with_capacity(replaced_segments_len);

        if new_segments_len > removed_segments_len {
            // The remain new segments will be append.
            let appended = new_segments.split_off(removed_segments_len);
            for (location, stats) in appended.into_iter().rev() {
                self.abort_operation.add_segment(location.clone());
                appended_segments.push((location, SegmentInfo::VERSION));
                merge_statistics_mut(&mut merged_statistics, &stats, default_cluster_key);
            }
        }

        for (i, (location, stats)) in new_segments.into_iter().enumerate() {
            // The old segments will be replaced with the news.
            self.abort_operation.add_segment(location.clone());
            replaced_segments.insert(
                self.removed_segment_indexes[i],
                (location, SegmentInfo::VERSION),
            );
            merge_statistics_mut(&mut merged_statistics, &stats, default_cluster_key);
        }

        let conflict_resolve_context =
            ConflictResolveContext::ModifiedSegmentExistsInLatest(SnapshotChanges {
                appended_segments,
                removed_segment_indexes: self.removed_segment_indexes[replaced_segments_len..]
                    .to_vec(),
                replaced_segments,
                removed_statistics: self.removed_statistics.clone(),
                merged_statistics,
            });

        let meta = CommitMeta::new(
            conflict_resolve_context,
            std::mem::take(&mut self.abort_operation),
            self.table_id,
        );
        let block_meta: BlockMetaInfoPtr = Box::new(meta);
        Ok(Some(DataBlock::empty_with_meta(block_meta)))
    }
}

impl ReclusterAggregator {
    pub fn new(
        table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        merged_blocks: Vec<Arc<BlockMeta>>,
        removed_segment_indexes: Vec<usize>,
        removed_statistics: Statistics,
    ) -> Self {
        let block_per_seg =
            table.get_option(FUSE_OPT_KEY_BLOCK_PER_SEGMENT, DEFAULT_BLOCK_PER_SEGMENT);
        let default_cluster_key = table.cluster_key_meta.clone().unwrap().0;
        ReclusterAggregator {
            ctx,
            dal: table.get_operator(),
            location_gen: table.meta_location_generator().clone(),
            default_cluster_key,
            block_thresholds: table.get_block_thresholds(),
            block_per_seg,
            merged_blocks,
            removed_segment_indexes,
            removed_statistics,
            start_time: Instant::now(),
            abort_operation: AbortOperation::default(),
            table_id: table.get_id(),
        }
    }

    async fn apply(&mut self) -> Result<Vec<(String, Statistics)>> {
        // sort ascending.
        self.merged_blocks.sort_by(|a, b| {
            sort_by_cluster_stats(&a.cluster_stats, &b.cluster_stats, self.default_cluster_key)
        });

        let mut tasks = Vec::new();
        let merged_blocks = std::mem::take(&mut self.merged_blocks);
        let segments_num = (merged_blocks.len() / self.block_per_seg).max(1);
        let chunk_size = merged_blocks.len().div_ceil(segments_num);
        let default_cluster_key = Some(self.default_cluster_key);
        let block_thresholds = self.block_thresholds;
        for chunk in &merged_blocks.into_iter().chunks(chunk_size) {
            let new_blocks = chunk.collect::<Vec<_>>();

            let location_gen = self.location_gen.clone();
            let op = self.dal.clone();
            tasks.push(async move {
                let location = location_gen.gen_segment_info_location();
                let mut new_summary =
                    reduce_block_metas(&new_blocks, block_thresholds, default_cluster_key);
                if new_summary.block_count > 1 {
                    // To fix issue #13217.
                    if new_summary.block_count > new_summary.perfect_block_count {
                        log::warn!(
                            "recluster: generate new segment: {}, perfect_block_count: {}, block_count: {}",
                            location, new_summary.perfect_block_count, new_summary.block_count,
                        );
                        new_summary.perfect_block_count = new_summary.block_count;
                    }
                }
                // create new segment info
                let new_segment = SegmentInfo::new(new_blocks, new_summary.clone());

                // write the segment info.
                let serialized_segment = SerializedSegment {
                    path: location.clone(),
                    segment: Arc::new(new_segment),
                };
                SegmentsIO::write_segment(op, serialized_segment).await?;
                Ok::<_, ErrorCode>((location, new_summary))
            });
        }

        let threads_nums = self.ctx.get_settings().get_max_threads()? as usize;

        execute_futures_in_parallel(
            tasks,
            threads_nums,
            threads_nums * 2,
            "fuse-write-segments-worker".to_owned(),
        )
        .await?
        .into_iter()
        .collect::<Result<Vec<_>>>()
    }
}
