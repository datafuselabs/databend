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

use databend_common_base::base::tokio::sync::Semaphore;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::TrySpawn;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::RemoteExpr;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::SEGMENT_NAME_COL_NAME;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::field_default_value;
use databend_common_sql::BloomIndexColumns;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache_manager::BlockMetaCache;
use databend_storages_common_cache_manager::CacheManager;
use databend_storages_common_index::RangeIndex;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_pruner::InternalColumnPruner;
use databend_storages_common_pruner::Limiter;
use databend_storages_common_pruner::LimiterPrunerCreator;
use databend_storages_common_pruner::PagePruner;
use databend_storages_common_pruner::PagePrunerCreator;
use databend_storages_common_pruner::RangePruner;
use databend_storages_common_pruner::RangePrunerCreator;
use databend_storages_common_pruner::TopNPrunner;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ClusterKey;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use log::info;
use log::warn;
use opendal::Operator;

use crate::io::BloomIndexBuilder;
use crate::operations::DeletedSegmentInfo;
use crate::pruning::segment_pruner::SegmentPruner;
use crate::pruning::BlockPruner;
use crate::pruning::BloomPruner;
use crate::pruning::BloomPrunerCreator;
use crate::pruning::FusePruningStatistics;
use crate::pruning::InvertedIndexPruner;
use crate::pruning::SegmentLocation;

pub struct PruningContext {
    pub ctx: Arc<dyn TableContext>,
    pub dal: Operator,
    pub pruning_runtime: Arc<Runtime>,
    pub pruning_semaphore: Arc<Semaphore>,

    pub limit_pruner: Arc<dyn Limiter + Send + Sync>,
    pub range_pruner: Arc<dyn RangePruner + Send + Sync>,
    pub bloom_pruner: Option<Arc<dyn BloomPruner + Send + Sync>>,
    pub page_pruner: Arc<dyn PagePruner + Send + Sync>,
    pub internal_column_pruner: Option<Arc<InternalColumnPruner>>,
    pub inverted_index_pruner: Option<Arc<InvertedIndexPruner>>,

    pub pruning_stats: Arc<FusePruningStatistics>,
}

impl PruningContext {
    #[allow(clippy::too_many_arguments)]
    pub fn try_create(
        ctx: &Arc<dyn TableContext>,
        dal: Operator,
        table_schema: TableSchemaRef,
        push_down: &Option<PushDownInfo>,
        cluster_key_meta: Option<ClusterKey>,
        cluster_keys: Vec<RemoteExpr<String>>,
        bloom_index_cols: BloomIndexColumns,
        max_concurrency: usize,
        bloom_index_builder: Option<BloomIndexBuilder>,
    ) -> Result<Arc<PruningContext>> {
        let func_ctx = ctx.get_function_context()?;

        let filter_expr = push_down.as_ref().and_then(|extra| {
            extra
                .filters
                .as_ref()
                .map(|f| f.filter.as_expr(&BUILTIN_FUNCTIONS))
        });

        // Limit pruner.
        // if there are ordering/filter clause, ignore limit, even it has been pushed down
        let limit = push_down
            .as_ref()
            .filter(|p| p.order_by.is_empty() && p.filters.is_none())
            .and_then(|p| p.limit);
        // prepare the limiter. in case that limit is none, an unlimited limiter will be returned
        let limit_pruner = LimiterPrunerCreator::create(limit);

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

        // Range filter.
        // if filter_expression is none, an dummy pruner will be returned, which prunes nothing
        let range_pruner = RangePrunerCreator::try_create_with_default_stats(
            func_ctx.clone(),
            &table_schema,
            filter_expr.as_ref(),
            default_stats,
        )?;

        // Bloom pruner.
        // None will be returned, if filter is not applicable (e.g. unsuitable filter expression, index not available, etc.)
        let bloom_pruner = BloomPrunerCreator::create(
            func_ctx.clone(),
            &table_schema,
            dal.clone(),
            filter_expr.as_ref(),
            bloom_index_cols,
            bloom_index_builder,
        )?;

        // Page pruner, used in native format
        let page_pruner = PagePrunerCreator::try_create(
            func_ctx.clone(),
            &table_schema,
            filter_expr.as_ref(),
            cluster_key_meta,
            cluster_keys,
        )?;

        // inverted index pruner, used to search matched rows in block
        let inverted_index_pruner = InvertedIndexPruner::try_create(dal.clone(), push_down)?;

        // Internal column pruner, if there are predicates using internal columns,
        // we can use them to prune segments and blocks.
        let internal_column_pruner =
            InternalColumnPruner::try_create(func_ctx, filter_expr.as_ref());

        // Constraint the degree of parallelism
        let max_threads = ctx.get_settings().get_max_threads()? as usize;

        // Pruning runtime.
        let pruning_runtime = Arc::new(Runtime::with_worker_threads(
            max_threads,
            Some("pruning-worker".to_owned()),
        )?);
        let pruning_semaphore = Arc::new(Semaphore::new(max_concurrency));
        let pruning_stats = Arc::new(FusePruningStatistics::default());

        let pruning_ctx = Arc::new(PruningContext {
            ctx: ctx.clone(),
            dal,
            pruning_runtime,
            pruning_semaphore,
            limit_pruner,
            range_pruner,
            bloom_pruner,
            page_pruner,
            internal_column_pruner,
            inverted_index_pruner,
            pruning_stats,
        });
        Ok(pruning_ctx)
    }
}

pub struct FusePruner {
    max_concurrency: usize,
    pub table_schema: TableSchemaRef,
    pub pruning_ctx: Arc<PruningContext>,
    pub push_down: Option<PushDownInfo>,
    pub inverse_range_index: Option<RangeIndex>,
    pub deleted_segments: Vec<DeletedSegmentInfo>,
    pub block_meta_cache: Option<BlockMetaCache>,
}

impl FusePruner {
    // Create normal fuse pruner.
    pub fn create(
        ctx: &Arc<dyn TableContext>,
        dal: Operator,
        table_schema: TableSchemaRef,
        push_down: &Option<PushDownInfo>,
        bloom_index_cols: BloomIndexColumns,
        bloom_index_builder: Option<BloomIndexBuilder>,
    ) -> Result<Self> {
        Self::create_with_pages(
            ctx,
            dal,
            table_schema,
            push_down,
            None,
            vec![],
            bloom_index_cols,
            bloom_index_builder,
        )
    }

    // Create fuse pruner with pages.
    pub fn create_with_pages(
        ctx: &Arc<dyn TableContext>,
        dal: Operator,
        table_schema: TableSchemaRef,
        push_down: &Option<PushDownInfo>,
        cluster_key_meta: Option<ClusterKey>,
        cluster_keys: Vec<RemoteExpr<String>>,
        bloom_index_cols: BloomIndexColumns,
        bloom_index_builder: Option<BloomIndexBuilder>,
    ) -> Result<Self> {
        let max_concurrency = {
            let max_io_requests = ctx.get_settings().get_max_storage_io_requests()? as usize;
            // Prevent us from miss-configured max_storage_io_requests setting, e.g. 0
            let v = std::cmp::max(max_io_requests, 10);
            if v > max_io_requests {
                warn!(
                    "max_storage_io_requests setting is too low {}, increased to {}",
                    max_io_requests, v
                )
            }
            v
        };

        info!("max concurrency of pruning is set to {}", max_concurrency);

        let pruning_ctx = PruningContext::try_create(
            ctx,
            dal,
            table_schema.clone(),
            push_down,
            cluster_key_meta,
            cluster_keys,
            bloom_index_cols,
            max_concurrency,
            bloom_index_builder,
        )?;

        Ok(FusePruner {
            max_concurrency,
            table_schema,
            push_down: push_down.clone(),
            pruning_ctx,
            inverse_range_index: None,
            deleted_segments: vec![],
            block_meta_cache: CacheManager::instance().get_block_meta_cache(),
        })
    }

    #[async_backtrace::framed]
    pub async fn read_pruning(
        &mut self,
        segment_locs: Vec<SegmentLocation>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        self.pruning(segment_locs, false).await
    }

    #[async_backtrace::framed]
    pub async fn delete_pruning(
        &mut self,
        segment_locs: Vec<SegmentLocation>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        self.pruning(segment_locs, true).await
    }

    // Pruning chain:
    // segment pruner -> block pruner -> topn pruner
    #[async_backtrace::framed]
    pub async fn pruning(
        &mut self,
        mut segment_locs: Vec<SegmentLocation>,
        delete_pruning: bool,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        // Segment pruner.
        let segment_pruner =
            SegmentPruner::create(self.pruning_ctx.clone(), self.table_schema.clone())?;
        let block_pruner = Arc::new(BlockPruner::create(self.pruning_ctx.clone())?);

        let mut remain = segment_locs.len() % self.max_concurrency;
        let batch_size = segment_locs.len() / self.max_concurrency;
        let mut works = Vec::with_capacity(self.max_concurrency);

        while !segment_locs.is_empty() {
            let gap_size = std::cmp::min(1, remain);
            let batch_size = batch_size + gap_size;
            remain -= gap_size;

            let mut batch = segment_locs.drain(0..batch_size).collect::<Vec<_>>();
            let inverse_range_index = self.get_inverse_range_index();
            works.push(self.pruning_ctx.pruning_runtime.spawn({
                let block_pruner = block_pruner.clone();
                let segment_pruner = segment_pruner.clone();
                let pruning_ctx = self.pruning_ctx.clone();

                async move {
                    // Build pruning tasks.
                    if let Some(internal_column_pruner) = &pruning_ctx.internal_column_pruner {
                        batch = batch
                            .into_iter()
                            .filter(|segment| {
                                internal_column_pruner
                                    .should_keep(SEGMENT_NAME_COL_NAME, &segment.location.0)
                            })
                            .collect::<Vec<_>>();
                    }

                    let mut res = vec![];
                    let mut deleted_segments = vec![];
                    let pruned_segments = segment_pruner.pruning(batch).await?;

                    if delete_pruning {
                        for (segment_location, compact_segment_info) in &pruned_segments {
                            if let Some(range_index) = &inverse_range_index {
                                if !range_index
                                    .should_keep(&compact_segment_info.summary.col_stats, None)
                                {
                                    deleted_segments.push(DeletedSegmentInfo {
                                        index: segment_location.segment_idx,
                                        summary: compact_segment_info.summary.clone(),
                                    });
                                    continue;
                                };
                            }
                            // do not populate the block meta cache for deletion operations,
                            // since block metas touched by deletion are not likely to
                            // be accessed soon.
                            let populate_block_meta_cache = false;
                            let block_metas = Self::extract_block_metas(
                                &segment_location.location.0,
                                compact_segment_info,
                                populate_block_meta_cache,
                            )?;
                            res.extend(
                                block_pruner
                                    .pruning(segment_location.clone(), block_metas)
                                    .await?,
                            );
                        }
                    } else {
                        for (location, info) in pruned_segments {
                            let block_metas =
                                Self::extract_block_metas(&location.location.0, &info, true)?;
                            res.extend(block_pruner.pruning(location.clone(), block_metas).await?);
                        }
                    }
                    Result::<_, ErrorCode>::Ok((res, deleted_segments))
                }
            }));
        }

        let workers = futures::future::try_join_all(works).await?;

        let mut metas = vec![];
        for worker in workers {
            let mut res = worker?;
            metas.extend(res.0);
            self.deleted_segments.append(&mut res.1);
        }
        if delete_pruning {
            Ok(metas)
        } else {
            // Todo:: for now, all operation (contains other mutation other than delete, like select,update etc.)
            // will get here, we can prevent other mutations like update and so on.
            // TopN pruner.
            self.topn_pruning(metas)
        }
    }

    fn extract_block_metas(
        segment_path: &str,
        segment: &CompactSegmentInfo,
        populate_cache: bool,
    ) -> Result<Arc<Vec<Arc<BlockMeta>>>> {
        if let Some(cache) = CacheManager::instance().get_block_meta_cache() {
            if let Some(metas) = cache.get(segment_path) {
                Ok(metas)
            } else {
                let block_metas = Arc::new(segment.block_metas()?);
                if populate_cache {
                    cache.put(segment_path.to_string(), block_metas.clone());
                }
                Ok(block_metas)
            }
        } else {
            Ok(Arc::new(segment.block_metas()?))
        }
    }

    #[async_backtrace::framed]
    pub async fn stream_pruning(
        &mut self,
        mut block_metas: Vec<Arc<BlockMeta>>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        let mut remain = block_metas.len() % self.max_concurrency;
        let batch_size = block_metas.len() / self.max_concurrency;
        let mut works = Vec::with_capacity(self.max_concurrency);
        let block_pruner = Arc::new(BlockPruner::create(self.pruning_ctx.clone())?);
        let mut segment_idx = 0;

        while !block_metas.is_empty() {
            let gap_size = std::cmp::min(1, remain);
            let batch_size = batch_size + gap_size;
            remain -= gap_size;

            let batch = block_metas.drain(0..batch_size).collect::<Vec<_>>();
            works.push(self.pruning_ctx.pruning_runtime.spawn({
                let block_pruner = block_pruner.clone();
                async move {
                    // Build pruning tasks.
                    let res = block_pruner
                        .pruning(
                            // unused segment location.
                            SegmentLocation {
                                segment_idx,
                                location: ("".to_string(), 0),
                                snapshot_loc: None,
                            },
                            Arc::new(batch),
                        )
                        .await?;

                    Result::<_, ErrorCode>::Ok(res)
                }
            }));
            segment_idx += 1;
        }

        let workers = futures::future::try_join_all(works).await?;

        let mut metas = vec![];
        for worker in workers {
            let res = worker?;
            metas.extend(res);
        }
        // Todo:: for now, all operation (contains other mutation other than delete, like select,update etc.)
        // will get here, we can prevent other mutations like update and so on.
        // TopN pruner.
        self.topn_pruning(metas)
    }

    // topn pruner:
    // if there are ordering + limit clause and no filters, use topn pruner
    fn topn_pruning(
        &self,
        metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)>,
    ) -> Result<Vec<(BlockMetaIndex, Arc<BlockMeta>)>> {
        let push_down = self.push_down.clone();
        if push_down
            .as_ref()
            .filter(|p| !p.order_by.is_empty() && p.limit.is_some() && p.filters.is_none())
            .is_some()
        {
            let schema = self.table_schema.clone();
            let push_down = push_down.as_ref().unwrap();
            let limit = push_down.limit.unwrap();
            let sort = push_down.order_by.clone();
            let topn_pruner = TopNPrunner::create(schema, sort, limit);
            return Ok(topn_pruner.prune(metas.clone()).unwrap_or(metas));
        }
        Ok(metas)
    }

    // Pruning stats.
    pub fn pruning_stats(&self) -> databend_common_catalog::plan::PruningStatistics {
        let stats = self.pruning_ctx.pruning_stats.clone();

        let segments_range_pruning_before = stats.get_segments_range_pruning_before() as usize;
        let segments_range_pruning_after = stats.get_segments_range_pruning_after() as usize;

        let blocks_range_pruning_before = stats.get_blocks_range_pruning_before() as usize;
        let blocks_range_pruning_after = stats.get_blocks_range_pruning_after() as usize;

        let blocks_bloom_pruning_before = stats.get_blocks_bloom_pruning_before() as usize;
        let blocks_bloom_pruning_after = stats.get_blocks_bloom_pruning_after() as usize;

        let blocks_inverted_index_pruning_before =
            stats.get_blocks_inverted_index_pruning_before() as usize;
        let blocks_inverted_index_pruning_after =
            stats.get_blocks_inverted_index_pruning_after() as usize;

        databend_common_catalog::plan::PruningStatistics {
            segments_range_pruning_before,
            segments_range_pruning_after,
            blocks_range_pruning_before,
            blocks_range_pruning_after,
            blocks_bloom_pruning_before,
            blocks_bloom_pruning_after,
            blocks_inverted_index_pruning_before,
            blocks_inverted_index_pruning_after,
        }
    }

    pub fn set_inverse_range_index(&mut self, index: RangeIndex) {
        self.inverse_range_index = Some(index)
    }

    pub fn get_inverse_range_index(&self) -> Option<RangeIndex> {
        self.inverse_range_index.clone()
    }
}
