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

use databend_common_base::base::ProgressValues;
use databend_common_catalog::plan::Filters;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PruningStatistics;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ComputedExpr;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::Evaluator;
use databend_common_expression::FieldIndex;
use databend_common_expression::RemoteExpr;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_expression::Value;
use databend_common_expression::ROW_ID_COL_NAME;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_metrics::storage::*;
use databend_common_pipeline_core::Pipeline;
use databend_common_sql::evaluator::BlockOperator;
use databend_storages_common_index::RangeIndex;
use databend_storages_common_pruner::RangePruner;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::TableSnapshot;

use crate::operations::mutation::Mutation;
use crate::operations::mutation::MutationAction;
use crate::operations::mutation::MutationPartInfo;
use crate::operations::mutation::MutationSource;
use crate::pruning::FusePruner;
use crate::FuseLazyPartInfo;
use crate::FuseTable;
use crate::SegmentLocation;

pub struct MutationTaskInfo {
    pub total_tasks: usize,
    pub num_whole_block_mutation: usize,
}

impl FuseTable {
    /// return None if the deletion is done, otherwise return the partitions to be deleted
    #[async_backtrace::framed]
    pub async fn fast_delete(
        &self,
        ctx: Arc<dyn TableContext>,
        filters: Option<Filters>,
        col_indices: Vec<usize>,
        query_row_id_col: bool,
    ) -> Result<Option<Arc<TableSnapshot>>> {
        let snapshot_opt = self.read_table_snapshot().await?;

        // check if table is empty
        let snapshot = if let Some(val) = snapshot_opt {
            val
        } else {
            // no snapshot, no deletion
            return Ok(None);
        };

        if snapshot.summary.row_count == 0 {
            // empty snapshot, no deletion
            return Ok(None);
        }

        // check if unconditional deletion
        let deletion_filters = match filters {
            None => {
                let progress_values = ProgressValues {
                    rows: snapshot.summary.row_count as usize,
                    bytes: snapshot.summary.uncompressed_byte_size as usize,
                };
                ctx.get_write_progress().incr(&progress_values);
                // deleting the whole table... just a truncate
                let purge = false;
                return self.do_truncate(ctx.clone(), purge).await.map(|_| None);
            }
            Some(filters) => filters,
        };

        if col_indices.is_empty() && !query_row_id_col {
            // here the situation: filter_expr is not null, but col_indices in empty, which
            // indicates the expr being evaluated is unrelated to the value of rows:
            //   e.g.
            //       `delete from t where 1 = 1`, `delete from t where now()`,
            //       or `delete from t where RANDOM()::INT::BOOLEAN`
            // if the `filter_expr` is of "constant" nullary :
            //   for the whole block, whether all of the rows should be kept or dropped,
            //   we can just return from here, without accessing the block data
            if self.try_eval_const(ctx.clone(), &self.schema(), &deletion_filters.filter)? {
                let progress_values = ProgressValues {
                    rows: snapshot.summary.row_count as usize,
                    bytes: snapshot.summary.uncompressed_byte_size as usize,
                };
                ctx.get_write_progress().incr(&progress_values);

                // deleting the whole table... just a truncate
                let purge = false;
                return self.do_truncate(ctx.clone(), purge).await.map(|_| None);
            }
        }
        Ok(Some(snapshot.clone()))
    }

    pub fn try_eval_const(
        &self,
        ctx: Arc<dyn TableContext>,
        schema: &TableSchema,
        filter: &RemoteExpr<String>,
    ) -> Result<bool> {
        let dummy_field = DataField::new("dummy", DataType::Null);
        let _dummy_schema = Arc::new(DataSchema::new(vec![dummy_field]));
        let dummy_value = Value::Column(Column::Null { len: 1 });
        let dummy_block = DataBlock::new(vec![BlockEntry::new(DataType::Null, dummy_value)], 1);

        let filter = filter
            .as_expr(&BUILTIN_FUNCTIONS)
            .project_column_ref(|name| schema.index_of(name).unwrap());

        assert_eq!(filter.data_type(), &DataType::Boolean);

        let func_ctx = ctx.get_function_context()?;
        let evaluator = Evaluator::new(&dummy_block, &func_ctx, &BUILTIN_FUNCTIONS);
        let predicates = evaluator
            .run(&filter)
            .map_err(|e| e.add_message("eval try eval const failed:"))?
            .try_downcast::<BooleanType>()
            .unwrap();

        Ok(match &predicates {
            Value::Scalar(v) => *v,
            Value::Column(bitmap) => bitmap.unset_bits() == 0,
        })
    }

    #[async_backtrace::framed]
    pub fn add_deletion_source(
        &self,
        ctx: Arc<dyn TableContext>,
        filter: &RemoteExpr<String>,
        col_indices: Vec<usize>,
        query_row_id_col: bool,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let projection = Projection::Columns(col_indices.clone());

        let block_reader = self.create_block_reader(
            ctx.clone(),
            projection,
            false,
            self.change_tracking_enabled(),
            false,
        )?;
        let mut schema = block_reader.schema().as_ref().clone();
        if query_row_id_col {
            schema.add_internal_field(
                ROW_ID_COL_NAME,
                TableDataType::Number(NumberDataType::UInt64),
                1,
            );
        }

        let filter_expr = Arc::new(Some(
            filter
                .as_expr(&BUILTIN_FUNCTIONS)
                .project_column_ref(|name| schema.index_of(name).unwrap()),
        ));

        let all_column_indices = self.all_column_indices();
        let remain_column_indices: Vec<usize> = all_column_indices
            .into_iter()
            .filter(|index| !col_indices.contains(index))
            .collect();
        let mut source_col_indices = col_indices;
        let remain_reader = if remain_column_indices.is_empty() {
            Arc::new(None)
        } else {
            source_col_indices.extend_from_slice(&remain_column_indices);
            Arc::new(Some(
                (*self.create_block_reader(
                    ctx.clone(),
                    Projection::Columns(remain_column_indices),
                    false,
                    self.change_tracking_enabled(),
                    false,
                )?)
                .clone(),
            ))
        };

        // resort the block.
        let mut projection = (0..source_col_indices.len()).collect::<Vec<_>>();
        projection.sort_by_key(|&i| source_col_indices[i]);
        let ops = vec![BlockOperator::Project { projection }];

        let _max_threads = (ctx.get_settings().get_max_threads()? as usize)
            .min(ctx.partition_num())
            .max(1);
        // Add source pipe.
        pipeline.add_transform(|input, output| {
            MutationSource::try_create(
                ctx.clone(),
                MutationAction::Deletion,
                input,
                output,
                filter_expr.clone(),
                block_reader.clone(),
                remain_reader.clone(),
                ops.clone(),
                self.storage_format,
                query_row_id_col,
            )
        })?;
        Ok(())
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

    pub async fn mutation_read_partitions(
        &self,
        snapshot: Arc<TableSnapshot>,
    ) -> Result<Partitions> {
        let mut segments = Vec::with_capacity(snapshot.segments.len());
        for (idx, segment_location) in snapshot.segments.iter().enumerate() {
            segments.push(FuseLazyPartInfo::create(idx, segment_location.clone()));
        }
        Ok(Partitions::create(
            PartitionsShuffleKind::Mod,
            segments,
            true,
        ))
    }

    #[async_backtrace::framed]
    #[allow(clippy::too_many_arguments)]
    pub async fn do_mutation_block_pruning(
        &self,
        ctx: Arc<dyn TableContext>,
        filters: Option<Filters>,
        projection: Projection,
        prune_ctx: MutationBlockPruningContext,
        with_origin: bool,
        is_delete: bool,
    ) -> Result<(Partitions, MutationTaskInfo)> {
        let MutationBlockPruningContext {
            segment_locations,
            block_count,
        } = prune_ctx;
        let push_down = Some(PushDownInfo {
            projection: Some(projection),
            filters: filters.clone(),
            ..PushDownInfo::default()
        });

        let mut pruner = FusePruner::create(
            &ctx,
            self.operator.clone(),
            self.schema_with_stream(),
            &push_down,
            self.bloom_index_cols(),
        )?;

        if let Some(inverse) = filters.map(|f| f.inverted_filter) {
            // now the `block_metas` refers to the blocks that need to be deleted completely or partially.
            //
            // let's try pruning the blocks further to get the blocks that need to be deleted completely, so that
            // later during mutation, we need not to load the data of these blocks:
            //
            // 1. invert the filter expression
            // 2. apply the inverse filter expression to the block metas, utilizing range index
            //  - for those blocks that need to be deleted completely, they will be filtered out.
            //  - for those blocks that need to be deleted partially, they will NOT be filtered out.
            //
            let inverse = inverse.as_expr(&BUILTIN_FUNCTIONS);
            let func_ctx = ctx.get_function_context()?;
            let range_index = RangeIndex::try_create(
                func_ctx,
                &inverse,
                self.table_info.schema(),
                StatisticsOfColumns::default(), // TODO default values
            )?;
            pruner.set_inverse_range_index(range_index);
        }

        let block_metas = if is_delete {
            pruner.delete_pruning(segment_locations).await?
        } else {
            pruner.read_pruning(segment_locations).await?
        };

        let mut whole_block_deletions = std::collections::HashSet::new();

        if !block_metas.is_empty() {
            if let Some(range_index) = pruner.get_inverse_range_index() {
                for (block_meta_idx, block_meta) in &block_metas {
                    if !range_index.should_keep(&block_meta.as_ref().col_stats, None) {
                        // this block should be deleted completely
                        whole_block_deletions
                            .insert((block_meta_idx.segment_idx, block_meta_idx.block_idx));
                    }
                }
            }
        }

        let range_block_metas = block_metas
            .clone()
            .into_iter()
            .map(|(block_meta_index, block_meta)| (Some(block_meta_index), block_meta))
            .collect::<Vec<_>>();

        let (_, inner_parts) = self.read_partitions_with_metas(
            ctx.clone(),
            self.schema_with_stream(),
            None,
            &range_block_metas,
            block_count.unwrap_or_default(),
            PruningStatistics::default(),
        )?;

        let mut parts = Partitions::create_nolazy(
            PartitionsShuffleKind::Mod,
            block_metas
                .into_iter()
                .zip(inner_parts.partitions.into_iter())
                .map(|((index, block_meta), inner_part)| {
                    let cluster_stats = if with_origin {
                        block_meta.cluster_stats.clone()
                    } else {
                        None
                    };
                    let key = (index.segment_idx, index.block_idx);
                    let whole_block_mutation = whole_block_deletions.contains(&key);
                    let part_info_ptr: PartInfoPtr =
                        Arc::new(Box::new(Mutation::MutationPartInfo(MutationPartInfo {
                            index,
                            cluster_stats,
                            inner_part,
                            whole_block_mutation,
                        })));
                    part_info_ptr
                })
                .collect(),
        );

        let mut part_num = parts.len();
        let mut num_whole_block_mutation = whole_block_deletions.len();
        let segment_num = pruner.deleted_segments.len();
        // now try to add deleted_segment
        for deleted_segment in pruner.deleted_segments {
            part_num += deleted_segment.summary.block_count as usize;
            num_whole_block_mutation += deleted_segment.summary.block_count as usize;
            parts
                .partitions
                .push(Arc::new(Box::new(Mutation::MutationDeletedSegment(
                    deleted_segment,
                ))));
        }

        if let Some(block_count) = block_count {
            metrics_inc_deletion_block_range_pruned_nums(block_count as u64 - part_num as u64);
        }
        metrics_inc_deletion_block_range_pruned_whole_block_nums(num_whole_block_mutation as u64);
        metrics_inc_deletion_segment_range_purned_whole_segment_nums(segment_num as u64);
        Ok((parts, MutationTaskInfo {
            total_tasks: part_num,
            num_whole_block_mutation,
        }))
    }
}

pub struct MutationBlockPruningContext {
    pub segment_locations: Vec<SegmentLocation>,
    pub block_count: Option<usize>,
}
