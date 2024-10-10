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

use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PruningStatistics;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::TopK;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
use databend_common_sql::field_default_value;
use databend_common_storage::ColumnNodes;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CachedObject;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_pruner::BlockMetaIndex;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::table::ChangeType;
use log::debug;
use log::info;
use sha2::Digest;
use sha2::Sha256;

use crate::columnar_prune;
use crate::fuse_part::FuseBlockPartInfo;
use crate::io::BloomIndexBuilder;
use crate::pruning::create_segment_location_vector;
use crate::pruning::FusePruner;
use crate::pruning::SegmentLocation;
use crate::FuseLazyPartInfo;
use crate::FuseTable;

impl FuseTable {
    #[fastrace::trace]
    #[async_backtrace::framed]
    pub async fn do_read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        debug!("fuse table do read partitions, push downs:{:?}", push_downs);
        if let Some(changes_desc) = &self.changes_desc {
            // For "ANALYZE TABLE" statement, we need set the default change type to "Insert".
            let change_type = push_downs.as_ref().map_or(ChangeType::Insert, |v| {
                v.change_type.clone().unwrap_or(ChangeType::Insert)
            });
            return self
                .do_read_changes_partitions(ctx, push_downs, change_type, &changes_desc.location)
                .await;
        }

        let snapshot = self.read_table_snapshot().await?;
        match snapshot {
            Some(snapshot) => {
                let snapshot_loc = self
                    .meta_location_generator
                    .snapshot_location_from_uuid(&snapshot.snapshot_id, snapshot.format_version)?;

                let mut nodes_num = 1;
                let cluster = ctx.get_cluster();

                if !cluster.is_empty() {
                    nodes_num = cluster.nodes.len();
                }

                if !dry_run && snapshot.segments.len() > nodes_num {
                    let mut segments = Vec::with_capacity(snapshot.segments.len());
                    for (idx, segment_location) in snapshot.segments.iter().enumerate() {
                        segments.push(FuseLazyPartInfo::create(idx, segment_location.clone()))
                    }

                    return Ok((
                        PartStatistics::new_estimated(
                            Some(snapshot_loc),
                            snapshot.summary.row_count as usize,
                            snapshot.summary.compressed_byte_size as usize,
                            snapshot.segments.len(),
                            snapshot.segments.len(),
                        ),
                        Partitions::create(PartitionsShuffleKind::Mod, segments),
                    ));
                }

                let snapshot_loc = Some(snapshot_loc);
                let table_schema = self.schema_with_stream();
                let summary = snapshot.summary.block_count as usize;
                let segments_location =
                    create_segment_location_vector(snapshot.segments.clone(), snapshot_loc);

                self.prune_snapshot_blocks(
                    ctx.clone(),
                    push_downs.clone(),
                    table_schema,
                    segments_location,
                    summary,
                )
                .await
            }
            None => Ok((PartStatistics::default(), Partitions::default())),
        }
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    pub async fn prune_snapshot_blocks(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        table_schema: TableSchemaRef,
        segments_location: Vec<SegmentLocation>,
        summary: usize,
    ) -> Result<(PartStatistics, Partitions)> {
        let start = Instant::now();
        info!(
            "segment numbers" = segments_location.len();
            "prune snapshot block start"
        );

        let dal = self.operator.clone();

        type CacheItem = (PartStatistics, Partitions);

        let derterministic_cache_key =
            push_downs
                .as_ref()
                .filter(|p| p.is_deterministic)
                .map(|push_downs| {
                    format!(
                        "{:x}",
                        Sha256::digest(format!("{:?}_{:?}", segments_location, push_downs))
                    )
                });

        if let Some(cache_key) = derterministic_cache_key.as_ref() {
            if let Some(cache) = CacheItem::cache() {
                if let Some(data) = cache.get(cache_key) {
                    info!(
                        "prune snapshot block from cache, final block numbers:{}, cost:{:?}",
                        data.1.len(),
                        start.elapsed()
                    );
                    return Ok((data.0.clone(), data.1.clone()));
                }
            }
        }

        let bloom_index_builder = if ctx
            .get_settings()
            .get_enable_auto_fix_missing_bloom_index()?
        {
            let storage_format = self.storage_format;

            let bloom_columns_map = self
                .bloom_index_cols()
                .bloom_index_fields(table_schema.clone(), BloomIndex::supported_type)?;

            Some(BloomIndexBuilder {
                table_ctx: ctx.clone(),
                table_schema: table_schema.clone(),
                table_dal: dal.clone(),
                storage_format,
                bloom_columns_map,
            })
        } else {
            None
        };

        let (block_metas, pruning_stats) =
            if ctx.get_settings().get_enable_columnar_segment_info()? {
                columnar_prune::prune(
                    &ctx,
                    dal,
                    table_schema.clone(),
                    &segments_location,
                    &push_downs,
                    self.bloom_index_cols(),
                    bloom_index_builder,
                )
                .await?
            } else {
                let mut pruner = if !self.is_native() || self.cluster_key_meta.is_none() {
                    FusePruner::create(
                        &ctx,
                        dal,
                        table_schema.clone(),
                        &push_downs,
                        self.bloom_index_cols(),
                        bloom_index_builder,
                    )?
                } else {
                    let cluster_keys = self.cluster_keys(ctx.clone());

                    FusePruner::create_with_pages(
                        &ctx,
                        dal,
                        table_schema,
                        &push_downs,
                        self.cluster_key_meta.clone(),
                        cluster_keys,
                        self.bloom_index_cols(),
                        bloom_index_builder,
                    )?
                };
                let block_metas: Vec<(BlockMetaIndex, Arc<BlockMeta>)> =
                    pruner.read_pruning(segments_location).await?;
                let pruning_stats: PruningStatistics = pruner.pruning_stats();
                (block_metas, pruning_stats)
            };

        info!(
            "prune snapshot block end, final block numbers:{}, cost:{:?}",
            block_metas.len(),
            start.elapsed()
        );

        let block_metas: Vec<(Option<BlockMetaIndex>, Arc<BlockMeta>)> = block_metas
            .into_iter()
            .map(|(block_meta_index, block_meta)| (Some(block_meta_index), block_meta))
            .collect::<Vec<_>>();

        let schema = self.schema_with_stream();
        let result = self.read_partitions_with_metas(
            ctx.clone(),
            schema,
            push_downs,
            &block_metas,
            summary,
            pruning_stats,
        )?;

        if let Some(cache_key) = derterministic_cache_key {
            if let Some(cache) = CacheItem::cache() {
                cache.insert(cache_key, result.clone());
            }
        }
        Ok(result)
    }

    pub fn read_partitions_with_metas(
        &self,
        ctx: Arc<dyn TableContext>,
        schema: TableSchemaRef,
        push_downs: Option<PushDownInfo>,
        block_metas: &[(Option<BlockMetaIndex>, Arc<BlockMeta>)],
        partitions_total: usize,
        pruning_stats: PruningStatistics,
    ) -> Result<(PartStatistics, Partitions)> {
        let arrow_schema = schema.as_ref().into();
        let column_nodes = ColumnNodes::new_from_schema(&arrow_schema, Some(&schema));

        let partitions_scanned = block_metas.len();

        let top_k = push_downs
            .as_ref()
            .filter(|_| self.is_native()) // Only native format supports topk push down.
            .and_then(|p| p.top_k(self.schema().as_ref()))
            .map(|topk| field_default_value(ctx.clone(), &topk.field).map(|d| (topk, d)))
            .transpose()?;

        let (mut statistics, parts) =
            Self::to_partitions(Some(&schema), block_metas, &column_nodes, top_k, push_downs);

        // Update planner statistics.
        statistics.partitions_total = partitions_total;
        statistics.partitions_scanned = partitions_scanned;
        statistics.pruning_stats = pruning_stats;

        // Update context statistics.
        self.data_metrics
            .inc_partitions_total(partitions_total as u64);
        self.data_metrics
            .inc_partitions_scanned(partitions_scanned as u64);

        Ok((statistics, parts))
    }

    pub fn to_partitions(
        schema: Option<&TableSchemaRef>,
        block_metas: &[(Option<BlockMetaIndex>, Arc<BlockMeta>)],
        column_nodes: &ColumnNodes,
        top_k: Option<(TopK, Scalar)>,
        push_downs: Option<PushDownInfo>,
    ) -> (PartStatistics, Partitions) {
        let limit = push_downs
            .as_ref()
            .filter(|p| p.order_by.is_empty() && p.filters.is_none())
            .and_then(|p| p.limit)
            .unwrap_or(usize::MAX);

        let mut block_metas = block_metas.to_vec();
        if let Some((top_k, default)) = &top_k {
            let default_stats = ColumnStatistics {
                min: default.clone(),
                max: default.clone(),
                // These fields will not be used in the following sorting.
                null_count: 0,
                in_memory_size: 0,
                distinct_of_values: None,
            };
            if top_k.asc {
                block_metas.sort_by(|a, b| {
                    let a =
                        a.1.col_stats
                            .get(&top_k.field.column_id)
                            .unwrap_or(&default_stats);
                    let b =
                        b.1.col_stats
                            .get(&top_k.field.column_id)
                            .unwrap_or(&default_stats);
                    (a.min().as_ref(), a.max().as_ref()).cmp(&(b.min().as_ref(), b.max().as_ref()))
                });
            } else {
                block_metas.sort_by(|a, b| {
                    let a =
                        a.1.col_stats
                            .get(&top_k.field.column_id)
                            .unwrap_or(&default_stats);
                    let b =
                        b.1.col_stats
                            .get(&top_k.field.column_id)
                            .unwrap_or(&default_stats);
                    (b.max().as_ref(), b.min().as_ref()).cmp(&(a.max().as_ref(), a.min().as_ref()))
                });
            }
        }

        let (mut statistics, mut partitions) = match &push_downs {
            None => Self::all_columns_partitions(schema, &block_metas, top_k.clone(), limit),
            Some(extras) => match &extras.projection {
                None => Self::all_columns_partitions(schema, &block_metas, top_k.clone(), limit),
                Some(projection) => Self::projection_partitions(
                    &block_metas,
                    column_nodes,
                    projection,
                    top_k.clone(),
                    limit,
                ),
            },
        };

        if top_k.is_some() {
            partitions.kind = PartitionsShuffleKind::Seq;
        }

        statistics.is_exact = statistics.is_exact && Self::is_exact(&push_downs);
        (statistics, partitions)
    }

    fn is_exact(push_downs: &Option<PushDownInfo>) -> bool {
        push_downs
            .as_ref()
            .map_or(true, |extra| extra.filters.is_none())
    }

    fn all_columns_partitions(
        schema: Option<&TableSchemaRef>,
        block_metas: &[(Option<BlockMetaIndex>, Arc<BlockMeta>)],
        top_k: Option<(TopK, Scalar)>,
        limit: usize,
    ) -> (PartStatistics, Partitions) {
        let mut statistics = PartStatistics::default_exact();
        let mut partitions = Partitions::create(PartitionsShuffleKind::Mod, vec![]);

        if limit == 0 {
            return (statistics, partitions);
        }

        let mut remaining = limit;
        for (block_meta_index, block_meta) in block_metas.iter() {
            let rows = block_meta.row_count as usize;
            partitions.partitions.push(Self::all_columns_part(
                schema,
                block_meta_index,
                &top_k,
                block_meta,
            ));
            statistics.read_rows += rows;
            statistics.read_bytes += block_meta.block_size as usize;

            if remaining > rows {
                remaining -= rows;
            } else {
                // the last block we shall take
                if remaining != rows {
                    statistics.is_exact = false;
                }
                break;
            }
        }

        (statistics, partitions)
    }

    fn projection_partitions(
        block_metas: &[(Option<BlockMetaIndex>, Arc<BlockMeta>)],
        column_nodes: &ColumnNodes,
        projection: &Projection,
        top_k: Option<(TopK, Scalar)>,
        limit: usize,
    ) -> (PartStatistics, Partitions) {
        let mut statistics = PartStatistics::default_exact();
        let mut partitions = Partitions::default();

        if limit == 0 {
            return (statistics, partitions);
        }

        let columns = projection.project_column_nodes(column_nodes).unwrap();
        let mut remaining = limit;

        for (block_meta_index, block_meta) in block_metas.iter() {
            partitions.partitions.push(Self::projection_part(
                block_meta,
                block_meta_index,
                column_nodes,
                top_k.clone(),
                projection,
            ));

            let rows = block_meta.row_count as usize;

            statistics.read_rows += rows;
            for column in &columns {
                for column_id in &column.leaf_column_ids {
                    // ignore all deleted field
                    if let Some(col_metas) = block_meta.col_metas.get(column_id) {
                        let (_, len) = col_metas.offset_length();
                        statistics.read_bytes += len as usize;
                    }
                }
            }

            if remaining > rows {
                remaining -= rows;
            } else {
                // the last block we shall take
                if remaining != rows {
                    statistics.is_exact = false;
                }
                break;
            }
        }
        (statistics, partitions)
    }

    fn all_columns_part(
        schema: Option<&TableSchemaRef>,
        block_meta_index: &Option<BlockMetaIndex>,
        top_k: &Option<(TopK, Scalar)>,
        meta: &BlockMeta,
    ) -> PartInfoPtr {
        let mut columns_meta = HashMap::with_capacity(meta.col_metas.len());
        let mut columns_stats = HashMap::with_capacity(meta.col_stats.len());

        for column_id in meta.col_metas.keys() {
            // ignore all deleted field
            if let Some(schema) = schema {
                if schema.is_column_deleted(*column_id) {
                    continue;
                }
            }

            // ignore column this block dose not exist
            if let Some(meta) = meta.col_metas.get(column_id) {
                columns_meta.insert(*column_id, meta.clone());
            }

            if let Some(stat) = meta.col_stats.get(column_id) {
                columns_stats.insert(*column_id, stat.clone());
            }
        }

        let rows_count = meta.row_count;
        let location = meta.location.0.clone();
        let create_on = meta.create_on;

        let sort_min_max = top_k.as_ref().map(|(top_k, default)| {
            meta.col_stats
                .get(&top_k.field.column_id)
                .map(|stat| (stat.min().clone(), stat.max().clone()))
                .unwrap_or((default.clone(), default.clone()))
        });

        FuseBlockPartInfo::create(
            location,
            rows_count,
            columns_meta,
            Some(columns_stats),
            meta.compression(),
            sort_min_max,
            block_meta_index.to_owned(),
            create_on,
        )
    }

    pub(crate) fn projection_part(
        meta: &BlockMeta,
        block_meta_index: &Option<BlockMetaIndex>,
        column_nodes: &ColumnNodes,
        top_k: Option<(TopK, Scalar)>,
        projection: &Projection,
    ) -> PartInfoPtr {
        let mut columns_meta = HashMap::with_capacity(projection.len());
        let mut columns_stat = HashMap::with_capacity(projection.len());

        let columns = projection.project_column_nodes(column_nodes).unwrap();
        for column in &columns {
            for column_id in &column.leaf_column_ids {
                // ignore column this block dose not exist
                if let Some(column_meta) = meta.col_metas.get(column_id) {
                    columns_meta.insert(*column_id, column_meta.clone());
                }
                if let Some(column_stat) = meta.col_stats.get(column_id) {
                    columns_stat.insert(*column_id, column_stat.clone());
                }
            }
        }

        let rows_count = meta.row_count;
        let location = meta.location.0.clone();
        let create_on = meta.create_on;

        let sort_min_max = top_k.map(|(top_k, default)| {
            let stat = meta.col_stats.get(&top_k.field.column_id);
            stat.map(|stat| (stat.min().clone(), stat.max().clone()))
                .unwrap_or((default.clone(), default))
        });

        // TODO
        // row_count should be a hint value of  LIMIT,
        // not the count the rows in this partition
        FuseBlockPartInfo::create(
            location,
            rows_count,
            columns_meta,
            Some(columns_stat),
            meta.compression(),
            sort_min_max,
            block_meta_index.to_owned(),
            create_on,
        )
    }
}
