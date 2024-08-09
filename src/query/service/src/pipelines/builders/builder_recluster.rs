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

use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::SortColumnDescription;
use databend_common_metrics::storage::metrics_inc_recluster_block_bytes_to_read;
use databend_common_metrics::storage::metrics_inc_recluster_block_nums_to_read;
use databend_common_metrics::storage::metrics_inc_recluster_row_nums_to_read;
use databend_common_pipeline_sources::EmptySource;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_sql::evaluator::CompoundBlockOperator;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::executor::physical_plans::Recluster;
use databend_common_sql::StreamContext;
use databend_common_storages_factory::Table;
use databend_common_storages_fuse::operations::TransformSerializeBlock;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::TableContext;

use crate::pipelines::builders::SortPipelineBuilder;
use crate::pipelines::processors::TransformAddStreamColumns;
use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_recluster(&mut self, recluster: &Recluster) -> Result<()> {
        match recluster.tasks.len() {
            0 => self.main_pipeline.add_source(EmptySource::create, 1),
            1 => {
                let table = self
                    .ctx
                    .build_table_by_table_info(&recluster.table_info, None)?;
                let table = FuseTable::try_from_table(table.as_ref())?;

                let task = &recluster.tasks[0];
                let recluster_block_nums = task.parts.len();
                let block_thresholds = table.get_block_thresholds();
                let table_info = table.get_table_info();
                let schema = table.schema_with_stream();
                let description = task.stats.get_description(&table_info.desc);
                let plan = DataSourcePlan {
                    source_info: DataSourceInfo::TableSource(table_info.clone()),
                    output_schema: schema.clone(),
                    parts: task.parts.clone(),
                    statistics: task.stats.clone(),
                    description,
                    tbl_args: table.table_args(),
                    push_downs: None,
                    query_internal_columns: false,
                    base_block_ids: None,
                    update_stream_columns: table.change_tracking_enabled(),
                    data_mask_policy: None,
                    table_index: usize::MAX,
                };

                {
                    metrics_inc_recluster_block_nums_to_read(recluster_block_nums as u64);
                    metrics_inc_recluster_block_bytes_to_read(task.total_bytes as u64);
                    metrics_inc_recluster_row_nums_to_read(task.total_rows as u64);

                    log::info!(
                        "Number of blocks scheduled for recluster: {}",
                        recluster_block_nums
                    );
                }

                self.ctx.set_partitions(plan.parts.clone())?;

                // ReadDataKind to avoid OOM.
                table.read_data(self.ctx.clone(), &plan, &mut self.main_pipeline, false)?;

                let num_input_columns = schema.fields().len();
                if table.change_tracking_enabled() {
                    let stream_ctx = StreamContext::try_create(
                        self.ctx.get_function_context()?,
                        schema,
                        table_info.ident.seq,
                        false,
                        false,
                    )?;
                    self.main_pipeline
                        .add_transformer(|| TransformAddStreamColumns::new(stream_ctx.clone()));
                }

                let cluster_stats_gen = table.get_cluster_stats_gen(
                    self.ctx.clone(),
                    task.level + 1,
                    block_thresholds,
                    None,
                )?;
                let operators = cluster_stats_gen.operators.clone();
                if !operators.is_empty() {
                    let func_ctx2 = cluster_stats_gen.func_ctx.clone();
                    self.main_pipeline.add_transformer(move || {
                        CompoundBlockOperator::new(
                            operators.clone(),
                            func_ctx2.clone(),
                            num_input_columns,
                        )
                    });
                }

                // merge sort
                let final_block_size =
                    block_thresholds.calc_rows_per_block(task.total_bytes, task.total_rows);
                let partial_block_size = if self.main_pipeline.output_len() > 1 {
                    std::cmp::min(
                        final_block_size,
                        self.ctx.get_settings().get_max_block_size()? as usize,
                    )
                } else {
                    final_block_size
                };

                // construct output fields
                let output_fields = cluster_stats_gen.out_fields.clone();
                let schema = DataSchemaRefExt::create(output_fields);
                let sort_descs = cluster_stats_gen
                    .cluster_key_index
                    .iter()
                    .map(|offset| SortColumnDescription {
                        offset: *offset,
                        asc: true,
                        nulls_first: false,
                        is_nullable: false, // This information is not needed here.
                    })
                    .collect();

                self.ctx.set_enable_sort_spill(false);
                let sort_pipeline_builder =
                    SortPipelineBuilder::create(self.ctx.clone(), schema, Arc::new(sort_descs))
                        .with_partial_block_size(partial_block_size)
                        .with_final_block_size(final_block_size)
                        .remove_order_col_at_last();
                sort_pipeline_builder.build_merge_sort_pipeline(&mut self.main_pipeline, false)?;

                let output_block_num = task.total_rows.div_ceil(final_block_size);
                let max_threads = std::cmp::min(
                    self.ctx.get_settings().get_max_threads()? as usize,
                    output_block_num,
                );
                self.main_pipeline.try_resize(max_threads)?;
                self.main_pipeline
                    .add_transform(|transform_input_port, transform_output_port| {
                        let proc = TransformSerializeBlock::try_create(
                            self.ctx.clone(),
                            transform_input_port,
                            transform_output_port,
                            table,
                            cluster_stats_gen.clone(),
                            MutationKind::Recluster,
                            recluster.table_meta_timestamps,
                        )?;
                        proc.into_processor()
                    })
            }
            _ => Err(ErrorCode::Internal(
                "A node can only execute one recluster task".to_string(),
            )),
        }
    }
}
