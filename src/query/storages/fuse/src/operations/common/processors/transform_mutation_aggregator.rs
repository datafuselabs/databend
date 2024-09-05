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
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::runtime::execute_futures_in_parallel;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::BlockThresholds;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_common_metrics::storage::metrics_inc_recluster_write_block_nums;
use databend_common_pipeline_transforms::processors::AsyncAccumulatingTransform;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::Versioned;
use itertools::Itertools;
use log::debug;
use log::info;
use log::warn;
use opendal::Operator;

use crate::io::SegmentsIO;
use crate::io::SerializedSegment;
use crate::io::TableMetaLocationGenerator;
use crate::operations::common::CommitMeta;
use crate::operations::common::ConflictResolveContext;
use crate::operations::common::MutationLogEntry;
use crate::operations::common::MutationLogs;
use crate::operations::common::SnapshotChanges;
use crate::operations::common::SnapshotMerged;
use crate::operations::mutation::BlockIndex;
use crate::operations::mutation::SegmentIndex;
use crate::statistics::reducers::merge_statistics_mut;
use crate::statistics::reducers::reduce_block_metas;
use crate::statistics::sort_by_cluster_stats;
use crate::FuseTable;
use crate::DEFAULT_BLOCK_PER_SEGMENT;
use crate::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;

pub struct TableMutationAggregator {
    ctx: Arc<dyn TableContext>,
    schema: TableSchemaRef,
    dal: Operator,
    location_gen: TableMetaLocationGenerator,

    thresholds: BlockThresholds,
    block_per_seg: usize,
    default_cluster_key_id: Option<u32>,

    base_segments: Vec<Location>,
    // Used for recluster.
    recluster_merged_blocks: Vec<Arc<BlockMeta>>,

    mutations: HashMap<SegmentIndex, BlockMutations>,
    appended_segments: Vec<Location>,
    appended_statistics: Statistics,
    removed_segment_indexes: Vec<SegmentIndex>,
    removed_statistics: Statistics,

    kind: MutationKind,
    start_time: Instant,
    finished_tasks: usize,
    table_id: u64,
    table_meta_timestamps: TableMetaTimestamps,
}

// takes in table mutation logs and aggregates them (former mutation_transform)
#[async_trait::async_trait]
impl AsyncAccumulatingTransform for TableMutationAggregator {
    const NAME: &'static str = "MutationAggregator";

    #[async_backtrace::framed]
    async fn transform(&mut self, data: DataBlock) -> Result<Option<DataBlock>> {
        let mutation_logs = MutationLogs::try_from(data)?;
        let task_num = mutation_logs.entries.len();
        mutation_logs.entries.into_iter().for_each(|entry| {
            self.accumulate_log_entry(entry);
        });
        self.refresh_status(task_num);
        Ok(None)
    }

    #[async_backtrace::framed]
    async fn on_finish(&mut self, _output: bool) -> Result<Option<DataBlock>> {
        let mut new_segment_locs = Vec::new();
        new_segment_locs.extend(self.appended_segments.clone());

        let conflict_resolve_context = match self.kind {
            MutationKind::Insert => ConflictResolveContext::AppendOnly((
                SnapshotMerged {
                    merged_segments: std::mem::take(&mut self.appended_segments),
                    merged_statistics: std::mem::take(&mut self.appended_statistics),
                },
                self.schema.clone(),
            )),
            MutationKind::Recluster => self.apply_recluster(&mut new_segment_locs).await?,
            _ => self.apply_mutation(&mut new_segment_locs).await?,
        };

        let meta = CommitMeta::new(conflict_resolve_context, new_segment_locs, self.table_id);
        debug!("mutations {:?}", meta);
        let block_meta: BlockMetaInfoPtr = Box::new(meta);
        Ok(Some(DataBlock::empty_with_meta(block_meta)))
    }
}

impl TableMutationAggregator {
    pub fn create(
        table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        base_segments: Vec<Location>,
        recluster_merged_blocks: Vec<Arc<BlockMeta>>,
        removed_segment_indexes: Vec<usize>,
        removed_statistics: Statistics,
        kind: MutationKind,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Self {
        TableMutationAggregator {
            ctx,
            schema: table.schema(),
            dal: table.get_operator(),
            location_gen: table.meta_location_generator().clone(),
            thresholds: table.get_block_thresholds(),
            default_cluster_key_id: table.cluster_key_id(),
            block_per_seg: table
                .get_option(FUSE_OPT_KEY_BLOCK_PER_SEGMENT, DEFAULT_BLOCK_PER_SEGMENT),
            mutations: HashMap::new(),
            appended_segments: vec![],
            base_segments,
            recluster_merged_blocks,
            appended_statistics: Statistics::default(),
            removed_segment_indexes,
            removed_statistics,
            kind,
            finished_tasks: 0,
            start_time: Instant::now(),
            table_id: table.get_id(),
            table_meta_timestamps,
        }
    }

    pub fn refresh_status(&mut self, task_num: usize) {
        self.finished_tasks += task_num;

        // Refresh status
        {
            let status = format!(
                "{}: run tasks:{}, cost:{:?}",
                self.kind,
                self.finished_tasks,
                self.start_time.elapsed()
            );
            self.ctx.set_status_info(&status);
        }
    }

    pub fn accumulate_log_entry(&mut self, log_entry: MutationLogEntry) {
        match log_entry {
            MutationLogEntry::ReplacedBlock { index, block_meta } => {
                match self.mutations.entry(index.segment_idx) {
                    Entry::Occupied(mut v) => {
                        v.get_mut().push_replaced(index.block_idx, block_meta);
                    }
                    Entry::Vacant(v) => {
                        v.insert(BlockMutations::new_replacement(index.block_idx, block_meta));
                    }
                }
            }
            MutationLogEntry::ReclusterAppendBlock { block_meta } => {
                metrics_inc_recluster_write_block_nums();
                self.recluster_merged_blocks.push(block_meta);
            }
            MutationLogEntry::DeletedBlock { index } => {
                self.mutations
                    .entry(index.segment_idx)
                    .and_modify(|v| v.push_deleted(index.block_idx))
                    .or_insert(BlockMutations::new_deletion(index.block_idx));
            }
            MutationLogEntry::DeletedSegment { deleted_segment } => {
                self.removed_segment_indexes.push(deleted_segment.index);
                merge_statistics_mut(
                    &mut self.removed_statistics,
                    &deleted_segment.summary,
                    self.default_cluster_key_id,
                );
            }
            MutationLogEntry::DoNothing => (),
            MutationLogEntry::AppendSegment {
                segment_location,
                format_version,
                summary,
            } => {
                merge_statistics_mut(
                    &mut self.appended_statistics,
                    &summary,
                    self.default_cluster_key_id,
                );

                self.appended_segments
                    .push((segment_location, format_version))
            }
            MutationLogEntry::CompactExtras { extras } => {
                match self.mutations.entry(extras.segment_index) {
                    Entry::Occupied(mut v) => {
                        v.get_mut().replaced_blocks.extend(extras.unchanged_blocks);
                    }
                    Entry::Vacant(v) => {
                        v.insert(BlockMutations {
                            replaced_blocks: extras.unchanged_blocks,
                            deleted_blocks: vec![],
                        });
                    }
                }

                self.removed_segment_indexes
                    .extend(extras.removed_segment_indexes);
                merge_statistics_mut(
                    &mut self.removed_statistics,
                    &extras.removed_segment_summary,
                    self.default_cluster_key_id,
                );
            }
        }
    }

    async fn apply_recluster(
        &mut self,
        new_segment_locs: &mut Vec<Location>,
    ) -> Result<ConflictResolveContext> {
        // safe to unwrap.
        let default_cluster_key_id = self.default_cluster_key_id.unwrap();
        // sort ascending.
        self.recluster_merged_blocks.sort_by(|a, b| {
            sort_by_cluster_stats(&a.cluster_stats, &b.cluster_stats, default_cluster_key_id)
        });

        let mut tasks = Vec::new();
        let merged_blocks = std::mem::take(&mut self.recluster_merged_blocks);
        let segments_num = (merged_blocks.len() / self.block_per_seg).max(1);
        let chunk_size = merged_blocks.len().div_ceil(segments_num);
        let default_cluster_key = Some(default_cluster_key_id);
        let thresholds = self.thresholds;
        for chunk in &merged_blocks.into_iter().chunks(chunk_size) {
            let new_blocks = chunk.collect::<Vec<_>>();
            let all_perfect = new_blocks.len() > 1;

            let location_gen = self.location_gen.clone();
            let op = self.dal.clone();
            let table_meta_timestamps = self.table_meta_timestamps;
            tasks.push(async move {
                write_segment(
                    op,
                    location_gen,
                    new_blocks,
                    thresholds,
                    default_cluster_key,
                    all_perfect,
                    MutationKind::Recluster,
                    table_meta_timestamps,
                )
                .await
            });
        }

        let threads_nums = self.ctx.get_settings().get_max_threads()? as usize;

        let mut new_segments = execute_futures_in_parallel(
            tasks,
            threads_nums,
            threads_nums * 2,
            "fuse-write-segments-worker".to_owned(),
        )
        .await?
        .into_iter()
        .collect::<Result<Vec<_>>>()?;

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
                let segment_loc = (location, SegmentInfo::VERSION);
                new_segment_locs.push(segment_loc.clone());
                appended_segments.push(segment_loc);
                merge_statistics_mut(&mut merged_statistics, &stats, self.default_cluster_key_id);
            }
        }

        for (i, (location, stats)) in new_segments.into_iter().enumerate() {
            // The old segments will be replaced with the news.
            let segment_loc = (location, SegmentInfo::VERSION);
            new_segment_locs.push(segment_loc.clone());
            replaced_segments.insert(self.removed_segment_indexes[i], segment_loc);
            merge_statistics_mut(&mut merged_statistics, &stats, self.default_cluster_key_id);
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
        Ok(conflict_resolve_context)
    }

    async fn apply_mutation(
        &mut self,
        new_segment_locs: &mut Vec<Location>,
    ) -> Result<ConflictResolveContext> {
        let start = Instant::now();
        let mut count = 0;

        let appended_segments = std::mem::take(&mut self.appended_segments);
        let appended_statistics = std::mem::take(&mut self.appended_statistics);

        let mut replaced_segments = HashMap::new();
        let mut merged_statistics = Statistics::default();
        let chunk_size = self.ctx.get_settings().get_max_threads()? as usize;
        let segment_indices = self.mutations.keys().cloned().collect::<Vec<_>>();
        for chunk in segment_indices.chunks(chunk_size) {
            let results = self.partial_apply_mutation(chunk.to_vec()).await?;
            for result in results {
                if let Some((location, summary)) = result.new_segment_info {
                    // replace the old segment location with the new one.
                    let new_segment_loc = (location, SegmentInfo::VERSION);
                    new_segment_locs.push(new_segment_loc.clone());
                    merge_statistics_mut(
                        &mut merged_statistics,
                        &summary,
                        self.default_cluster_key_id,
                    );
                    replaced_segments.insert(result.index, new_segment_loc);
                } else {
                    self.removed_segment_indexes.push(result.index);
                }

                if let Some(origin_summary) = result.origin_summary {
                    merge_statistics_mut(
                        &mut self.removed_statistics,
                        &origin_summary,
                        self.default_cluster_key_id,
                    );
                }
            }

            // Refresh status
            {
                count += chunk.len();
                let status = format!(
                    "{}: generate new segment files:{}/{}, cost:{:?}",
                    self.kind,
                    count,
                    segment_indices.len(),
                    start.elapsed()
                );
                self.ctx.set_status_info(&status);
            }
        }

        info!("removed_segment_indexes:{:?}", self.removed_segment_indexes);

        merge_statistics_mut(
            &mut merged_statistics,
            &appended_statistics,
            self.default_cluster_key_id,
        );

        Ok(ConflictResolveContext::ModifiedSegmentExistsInLatest(
            SnapshotChanges {
                appended_segments,
                replaced_segments,
                removed_segment_indexes: std::mem::take(&mut self.removed_segment_indexes),
                merged_statistics,
                removed_statistics: std::mem::take(&mut self.removed_statistics),
            },
        ))
    }

    async fn partial_apply_mutation(
        &mut self,
        segment_indices: Vec<usize>,
    ) -> Result<Vec<SegmentLite>> {
        let thresholds = self.thresholds;
        let default_cluster_key_id = self.default_cluster_key_id;
        let mut tasks = Vec::with_capacity(segment_indices.len());
        for index in segment_indices {
            let segment_mutation = self.mutations.remove(&index).unwrap();
            let location = self.base_segments.get(index).cloned();
            let schema = self.schema.clone();
            let op = self.dal.clone();
            let location_gen = self.location_gen.clone();
            let kind = self.kind;

            let table_meta_timestamps = self.table_meta_timestamps;
            let mut all_perfect = false;
            tasks.push(async move {
                let (new_blocks, origin_summary) = if let Some(loc) = location {
                    // read the old segment
                    let compact_segment_info =
                        SegmentsIO::read_compact_segment(op.clone(), loc, schema, false).await?;
                    let mut segment_info = SegmentInfo::try_from(compact_segment_info)?;

                    // take away the blocks, they are being mutated
                    let mut block_editor = BTreeMap::<_, _>::from_iter(
                        std::mem::take(&mut segment_info.blocks)
                            .into_iter()
                            .enumerate(),
                    );
                    for (idx, new_meta) in segment_mutation.replaced_blocks {
                        block_editor.insert(idx, new_meta);
                    }
                    for idx in segment_mutation.deleted_blocks {
                        block_editor.remove(&idx);
                    }

                    if block_editor.is_empty() {
                        return Ok(SegmentLite {
                            index,
                            new_segment_info: None,
                            origin_summary: Some(segment_info.summary),
                        });
                    }

                    // assign back the mutated blocks to segment
                    let new_blocks = block_editor.into_values().collect::<Vec<_>>();
                    (new_blocks, Some(segment_info.summary))
                } else {
                    // use by compact.
                    assert!(segment_mutation.deleted_blocks.is_empty());
                    // There are more than 1 blocks, means that the blocks can no longer be compacted.
                    // They can be marked as perfect blocks.
                    all_perfect = segment_mutation.replaced_blocks.len() > 1;
                    let new_blocks = segment_mutation
                        .replaced_blocks
                        .into_iter()
                        .sorted_by(|a, b| a.0.cmp(&b.0))
                        .map(|(_, meta)| meta)
                        .collect::<Vec<_>>();
                    (new_blocks, None)
                };

                let new_segment_info = write_segment(
                    op,
                    location_gen,
                    new_blocks,
                    thresholds,
                    default_cluster_key_id,
                    all_perfect,
                    kind,
                    table_meta_timestamps,
                )
                .await?;

                Ok(SegmentLite {
                    index,
                    new_segment_info: Some(new_segment_info),
                    origin_summary,
                })
            });
        }

        let threads_nums = self.ctx.get_settings().get_max_threads()? as usize;

        execute_futures_in_parallel(
            tasks,
            threads_nums,
            threads_nums * 2,
            "fuse-req-segments-worker".to_owned(),
        )
        .await?
        .into_iter()
        .collect::<Result<Vec<_>>>()
    }
}

#[derive(Default)]
struct BlockMutations {
    replaced_blocks: Vec<(BlockIndex, Arc<BlockMeta>)>,
    deleted_blocks: Vec<BlockIndex>,
}

impl BlockMutations {
    fn new_replacement(block_idx: BlockIndex, block_meta: Arc<BlockMeta>) -> Self {
        BlockMutations {
            replaced_blocks: vec![(block_idx, block_meta)],
            deleted_blocks: vec![],
        }
    }

    fn new_deletion(block_idx: BlockIndex) -> Self {
        BlockMutations {
            replaced_blocks: vec![],
            deleted_blocks: vec![block_idx],
        }
    }

    fn push_replaced(&mut self, block_idx: BlockIndex, block_meta: Arc<BlockMeta>) {
        self.replaced_blocks.push((block_idx, block_meta));
    }

    fn push_deleted(&mut self, block_idx: BlockIndex) {
        self.deleted_blocks.push(block_idx)
    }
}

struct SegmentLite {
    // segment index.
    index: usize,
    // new segment location and summary.
    new_segment_info: Option<(String, Statistics)>,
    // origin segment summary.
    origin_summary: Option<Statistics>,
}

async fn write_segment(
    dal: Operator,
    location_gen: TableMetaLocationGenerator,
    blocks: Vec<Arc<BlockMeta>>,
    thresholds: BlockThresholds,
    default_cluster_key: Option<u32>,
    all_perfect: bool,
    kind: MutationKind,
    table_meta_timestamps: TableMetaTimestamps,
) -> Result<(String, Statistics)> {
    let location = location_gen.gen_segment_info_location(table_meta_timestamps);
    let mut new_summary = reduce_block_metas(&blocks, thresholds, default_cluster_key);
    if all_perfect {
        // To fix issue #13217.
        if new_summary.block_count > new_summary.perfect_block_count {
            warn!(
                "{}: generate new segment: {}, perfect_block_count: {}, block_count: {}",
                kind, location, new_summary.perfect_block_count, new_summary.block_count,
            );
            new_summary.perfect_block_count = new_summary.block_count;
        }
    }
    // create new segment info
    let new_segment = SegmentInfo::new(blocks, new_summary.clone());

    // write the segment info.
    let serialized_segment = SerializedSegment {
        path: location.clone(),
        segment: Arc::new(new_segment),
    };
    SegmentsIO::write_segment(dal, serialized_segment).await?;
    Ok((location, new_summary))
}
