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

use databend_common_base::runtime::Runtime;
use databend_common_catalog::plan::block_id_in_segment;
use databend_common_catalog::plan::PruningStatistics;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_exception::Result;
use databend_common_expression::ConstantFolder;
use databend_common_expression::Domain;
use databend_common_expression::Expr;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::field_default_value;
use databend_common_sql::BloomIndexColumns;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_pruner::RangePruner;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use log::warn;
use opendal::Operator;

use crate::columnar_prune::util::parallelize_workload;
use crate::index::RangeIndex;
use crate::io::BloomIndexBuilder;
use crate::io::ColumnarSegmentInfoReader;
use crate::io::MetaReaders;
use crate::operations::DeletedSegmentInfo;
use crate::SegmentLocation;
use crate::TableContext;
pub async fn prune(
    ctx: &Arc<dyn TableContext>,
    op: Operator,
    table_schema: TableSchemaRef,
    segments: &[SegmentLocation],
    push_down: &Option<PushDownInfo>,
    bloom_index_cols: BloomIndexColumns,
    bloom_index_builder: Option<BloomIndexBuilder>,
) -> Result<(Vec<(BlockMetaIndex, Arc<BlockMeta>)>, PruningStatistics)> {
    const MIN_CONCURRENCY: usize = 10;
    let runtime = Arc::new(Runtime::with_worker_threads(
        ctx.get_settings().get_max_threads()? as usize,
        Some("pruning-worker".to_owned()),
    )?);
    let max_io_requests = ctx.get_settings().get_max_storage_io_requests()? as usize;
    let concurrency = std::cmp::max(max_io_requests, MIN_CONCURRENCY);
    if concurrency > max_io_requests {
        warn!(
            "max_storage_io_requests setting is too low {}, increased to {}",
            max_io_requests, concurrency
        )
    }
    let prune_ctx = Arc::new(PruneContext::new(
        ctx.clone(),
        op,
        table_schema,
        push_down,
        bloom_index_cols,
        bloom_index_builder,
        None,
    )?);
    let results =
        parallelize_workload(&runtime, concurrency, segments, prune_ctx, do_prune).await?;
    let mut all_keeped_blocks = Vec::new();
    let mut all_pruning_stats = PruningStatistics::default();
    for result in results {
        let PruneResult {
            keeped_blocks,
            _deleted_segments: _,
            pruning_stats,
        } = result?;
        all_keeped_blocks.extend(keeped_blocks);
        all_pruning_stats.merge(&pruning_stats);
    }
    Ok((all_keeped_blocks, all_pruning_stats))
}

struct PruneResult {
    keeped_blocks: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
    _deleted_segments: Vec<DeletedSegmentInfo>,
    pruning_stats: PruningStatistics,
}

struct PruneContext {
    segment_reader: ColumnarSegmentInfoReader,
    range_index: Option<RangeIndex>,
    inverse_range_index: Option<RangeIndex>,
}

impl PruneContext {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        op: Operator,
        table_schema: TableSchemaRef,
        push_down: &Option<PushDownInfo>,
        _bloom_index_cols: BloomIndexColumns,
        _bloom_index_builder: Option<BloomIndexBuilder>,
        inverse_range_index: Option<RangeIndex>,
    ) -> Result<Self> {
        let func_ctx = ctx.get_function_context()?;
        let filter_expr = push_down.as_ref().and_then(|extra| {
            extra
                .filters
                .as_ref()
                .map(|f| f.filter.as_expr(&BUILTIN_FUNCTIONS))
        });
        let default_stats: StatisticsOfColumns = filter_expr
            .as_ref()
            .map(|f| f.column_refs())
            .into_iter()
            .flatten()
            .filter_map(|(name, _)| {
                let field = table_schema.field_with_name(&name).ok()?;
                let default_scalar = field_default_value(ctx.clone(), field).ok()?;

                let stats =
                    ColumnStatistics::new(default_scalar.clone(), default_scalar, 0, 0, Some(1));
                Some((field.column_id(), stats))
            })
            .collect();
        let range_index = filter_expr
            .map(|exprs| {
                RangeIndex::try_create(func_ctx, &exprs, table_schema.clone(), default_stats)
            })
            .transpose()?;
        Ok(Self {
            segment_reader: MetaReaders::columnar_segment_info_reader(op, table_schema),
            inverse_range_index,
            range_index,
        })
    }
}

async fn do_prune(
    segments: Vec<SegmentLocation>,
    prune_ctx: Arc<PruneContext>,
) -> Result<PruneResult> {
    let PruneContext {
        segment_reader,
        range_index,
        inverse_range_index,
    } = prune_ctx.as_ref();
    let mut keeped_blocks = Vec::new();
    let mut deleted_segments = Vec::new();

    for segment_location in segments {
        let segment = segment_reader
            .read(&LoadParams {
                location: segment_location.location.0.clone(),
                len_hint: None,
                ver: segment_location.location.1,
                put_cache: true,
            })
            .await?;

        if range_index
            .as_ref()
            .is_some_and(|index| !index.should_keep(&segment.summary.col_stats, None))
        {
            continue;
        }

        if inverse_range_index
            .as_ref()
            .is_some_and(|index| !index.should_keep(&segment.summary.col_stats, None))
        {
            deleted_segments.push(DeletedSegmentInfo {
                index: segment_location.segment_idx,
                summary: segment.summary.clone(),
            });
            continue;
        }

        let block_metas = &segment.blocks;
        // let col_refs: Vec<_> = range_index.expr.column_refs().into_iter().collect();
        // let mut col_stats = Vec::new();
        // for (col_name, _) in col_refs {
        //     let column_ids = range_index.schema.leaf_columns_of(&col_name);
        //     let col_stat = column_ids
        //         .iter()
        //         .map(|column_id| block_metas.column_by_name(&column_id.to_string()))
        //         .collect::<Vec<_>>();
        //     col_stats.push(col_stat);
        // }
        let block_num = block_metas.num_rows();
        for block_idx in 0..block_num {
            let input_domains = HashMap::new();
            if should_keep(range_index, input_domains)? {
                let block_meta_index = BlockMetaIndex {
                    segment_idx: segment_location.segment_idx,
                    block_idx,
                    range: None,
                    page_size: 0,
                    block_id: block_id_in_segment(block_num, block_idx),
                    block_location: segment.block_location(block_idx).to_string(),
                    segment_location: segment_location.location.0.clone(),
                    snapshot_location: segment_location.snapshot_loc.clone(),
                    matched_rows: None,
                };
                keeped_blocks.push((block_meta_index, segment.block_meta(block_idx)));
            }
        }
    }
    Ok(PruneResult {
        keeped_blocks,
        _deleted_segments: deleted_segments,
        pruning_stats: PruningStatistics::default(),
    })
}

fn should_keep(
    range_index: &Option<RangeIndex>,
    input_domains: HashMap<String, Domain>,
) -> Result<bool> {
    let Some(range_index) = range_index else {
        return Ok(true);
    };
    let (new_expr, _) = ConstantFolder::fold_with_domain(
        &range_index.expr,
        &input_domains,
        &range_index.func_ctx,
        &BUILTIN_FUNCTIONS,
    );
    Ok(!matches!(new_expr, Expr::Constant {
        scalar: Scalar::Boolean(false),
        ..
    }))
}
