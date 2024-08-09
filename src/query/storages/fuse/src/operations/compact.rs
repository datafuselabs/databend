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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_base::runtime::Runtime;
use databend_common_catalog::plan::PartInfoType;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::table::CompactionLimits;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::ComputedExpr;
use databend_common_expression::FieldIndex;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::StreamContext;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;

use crate::operations::common::TableMutationAggregator;
use crate::operations::common::TransformSerializeBlock;
use crate::operations::mutation::BlockCompactMutator;
use crate::operations::mutation::CompactLazyPartInfo;
use crate::operations::mutation::CompactSource;
use crate::operations::mutation::SegmentCompactMutator;
use crate::FuseTable;
use crate::Table;
use crate::TableContext;
use crate::DEFAULT_BLOCK_PER_SEGMENT;
use crate::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;

#[derive(Clone)]
pub struct CompactOptions {
    // the snapshot that compactor working on, it never changed during phases compaction.
    pub base_snapshot: Arc<TableSnapshot>,
    pub block_per_seg: usize,
    pub num_segment_limit: Option<usize>,
    pub num_block_limit: Option<usize>,
}

impl FuseTable {
    #[async_backtrace::framed]
    pub(crate) async fn do_compact_segments(
        &self,
        ctx: Arc<dyn TableContext>,
        num_segment_limit: Option<usize>,
    ) -> Result<()> {
        let compact_options = if let Some(v) = self
            .compact_options_with_segment_limit(num_segment_limit)
            .await?
        {
            v
        } else {
            return Ok(());
        };

        let mut segment_mutator = SegmentCompactMutator::try_create(
            ctx.clone(),
            compact_options,
            self.meta_location_generator().clone(),
            self.operator.clone(),
            self.cluster_key_id(),
            self.get_id(),
        )?;

        if !segment_mutator.target_select().await? {
            return Ok(());
        }

        segment_mutator.try_commit(Arc::new(self.clone())).await
    }

    #[async_backtrace::framed]
    pub(crate) async fn do_compact_blocks(
        &self,
        ctx: Arc<dyn TableContext>,
        limits: CompactionLimits,
    ) -> Result<Option<(Partitions, Arc<TableSnapshot>)>> {
        let compact_options = if let Some(v) = self
            .compact_options(limits.segment_limit, limits.block_limit)
            .await?
        {
            v
        } else {
            return Ok(None);
        };

        let thresholds = self.get_block_thresholds();
        let mut mutator = BlockCompactMutator::new(
            ctx.clone(),
            thresholds,
            compact_options,
            self.operator.clone(),
            self.cluster_key_id(),
        );

        let partitions = mutator.target_select().await?;
        if partitions.is_empty() {
            return Ok(None);
        }

        Ok(Some((
            partitions,
            mutator.compact_params.base_snapshot.clone(),
        )))
    }

    pub fn build_compact_source(
        &self,
        ctx: Arc<dyn TableContext>,
        parts: Partitions,
        column_ids: HashSet<ColumnId>,
        pipeline: &mut Pipeline,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<()> {
        let is_lazy = parts.partitions_type() == PartInfoType::LazyLevel;
        let thresholds = self.get_block_thresholds();
        let cluster_key_id = self.cluster_key_id();
        let mut max_threads = ctx.get_settings().get_max_threads()? as usize;

        if is_lazy {
            let query_ctx = ctx.clone();

            let lazy_parts = parts
                .partitions
                .into_iter()
                .map(|v| {
                    v.as_any()
                        .downcast_ref::<CompactLazyPartInfo>()
                        .unwrap()
                        .clone()
                })
                .collect::<Vec<_>>();

            pipeline.set_on_init(move || {
                let ctx = query_ctx.clone();
                let column_ids = column_ids.clone();
                let partitions = Runtime::with_worker_threads(2, None)?.block_on(async move {
                    let partitions = BlockCompactMutator::build_compact_tasks(
                        ctx.clone(),
                        column_ids,
                        cluster_key_id,
                        thresholds,
                        lazy_parts,
                    )
                    .await?;

                    Result::<_, ErrorCode>::Ok(partitions)
                })?;

                let partitions = Partitions::create(PartitionsShuffleKind::Mod, partitions);
                query_ctx.set_partitions(partitions)?;
                Ok(())
            });
        } else {
            max_threads = max_threads.min(parts.len()).max(1);
            ctx.set_partitions(parts)?;
        }

        let all_column_indices = self.all_column_indices();
        let projection = Projection::Columns(all_column_indices);
        let block_reader = self.create_block_reader(
            ctx.clone(),
            projection,
            false,
            self.change_tracking_enabled(),
            false,
        )?;
        let stream_ctx = if self.change_tracking_enabled() {
            Some(StreamContext::try_create(
                ctx.get_function_context()?,
                self.schema_with_stream(),
                self.get_table_info().ident.seq,
                false,
                false,
            )?)
        } else {
            None
        };
        // Add source pipe.
        pipeline.add_source(
            |output| {
                CompactSource::try_create(
                    ctx.clone(),
                    self.storage_format,
                    block_reader.clone(),
                    stream_ctx.clone(),
                    output,
                )
            },
            max_threads,
        )?;

        // sort
        let cluster_stats_gen =
            self.cluster_gen_for_append(ctx.clone(), pipeline, thresholds, None)?;
        pipeline.add_transform(
            |input: Arc<databend_common_pipeline_core::processors::InputPort>, output| {
                let proc = TransformSerializeBlock::try_create(
                    ctx.clone(),
                    input,
                    output,
                    self,
                    cluster_stats_gen.clone(),
                    MutationKind::Compact,
                    table_meta_timestamps,
                )?;
                proc.into_processor()
            },
        )?;

        if is_lazy {
            pipeline.try_resize(1)?;
            pipeline.add_async_accumulating_transformer(|| {
                TableMutationAggregator::create(
                    self,
                    ctx.clone(),
                    vec![],
                    vec![],
                    vec![],
                    Statistics::default(),
                    MutationKind::Compact,
                    table_meta_timestamps,
                )
            });
        }
        Ok(())
    }

    async fn compact_options_with_segment_limit(
        &self,
        num_segment_limit: Option<usize>,
    ) -> Result<Option<CompactOptions>> {
        self.compact_options(num_segment_limit, None).await
    }

    #[async_backtrace::framed]
    async fn compact_options(
        &self,
        num_segment_limit: Option<usize>,
        num_block_limit: Option<usize>,
    ) -> Result<Option<CompactOptions>> {
        let snapshot_opt = self.read_table_snapshot().await?;
        let base_snapshot = if let Some(val) = snapshot_opt {
            val
        } else {
            // no snapshot, no compaction.
            return Ok(None);
        };

        if base_snapshot.summary.block_count <= 1 {
            return Ok(None);
        }

        let block_per_seg =
            self.get_option(FUSE_OPT_KEY_BLOCK_PER_SEGMENT, DEFAULT_BLOCK_PER_SEGMENT);

        Ok(Some(CompactOptions {
            base_snapshot,
            block_per_seg,
            num_segment_limit,
            num_block_limit,
        }))
    }

    pub fn all_column_indices(&self) -> Vec<FieldIndex> {
        self.schema_with_stream()
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| !matches!(f.computed_expr(), Some(ComputedExpr::Virtual(_))))
            .map(|(i, _)| i)
            .collect::<Vec<FieldIndex>>()
    }
}
