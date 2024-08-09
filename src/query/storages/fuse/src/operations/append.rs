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

use std::str::FromStr;
use std::sync::Arc;

use databend_common_catalog::table::AppendMode;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::Expr;
use databend_common_expression::SortColumnDescription;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_transforms::processors::create_dummy_item;
use databend_common_pipeline_transforms::processors::BlockCompactor;
use databend_common_pipeline_transforms::processors::BlockCompactorForCopy;
use databend_common_pipeline_transforms::processors::TransformCompact;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_pipeline_transforms::processors::TransformSortPartial;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::evaluator::CompoundBlockOperator;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;

use crate::operations::common::TransformSerializeBlock;
use crate::statistics::ClusterStatsGenerator;
use crate::FuseTable;

impl FuseTable {
    pub fn do_append_data(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        append_mode: AppendMode,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<()> {
        let block_thresholds = self.get_block_thresholds();

        match append_mode {
            AppendMode::Normal => {
                pipeline.add_transform(|transform_input_port, transform_output_port| {
                    Ok(ProcessorPtr::create(TransformCompact::try_create(
                        transform_input_port,
                        transform_output_port,
                        BlockCompactor::new(block_thresholds),
                    )?))
                })?;
            }
            AppendMode::Copy => {
                pipeline.try_resize(1)?;
                pipeline.add_transform(|transform_input_port, transform_output_port| {
                    Ok(ProcessorPtr::create(TransformCompact::try_create(
                        transform_input_port,
                        transform_output_port,
                        BlockCompactorForCopy::new(block_thresholds),
                    )?))
                })?;
                pipeline.try_resize(ctx.get_settings().get_max_threads()? as usize)?;
            }
        }

        let schema = DataSchema::from(self.schema()).into();
        let cluster_stats_gen =
            self.cluster_gen_for_append(ctx.clone(), pipeline, block_thresholds, Some(schema))?;
        pipeline.add_transform(|input, output| {
            let proc = TransformSerializeBlock::try_create(
                ctx.clone(),
                input,
                output,
                self,
                cluster_stats_gen.clone(),
                MutationKind::Insert,
                table_meta_timestamps,
            )?;
            proc.into_processor()
        })?;

        Ok(())
    }

    pub fn cluster_gen_for_append_with_specified_len(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        block_thresholds: BlockThresholds,
        transform_len: usize,
        need_match: bool,
    ) -> Result<ClusterStatsGenerator> {
        let cluster_stats_gen =
            self.get_cluster_stats_gen(ctx.clone(), 0, block_thresholds, None)?;

        let operators = cluster_stats_gen.operators.clone();
        if !operators.is_empty() {
            let num_input_columns = self.table_info.schema().fields().len();
            let func_ctx2 = cluster_stats_gen.func_ctx.clone();
            let mut builder = pipeline.try_create_transform_pipeline_builder_with_len(
                move || {
                    Ok(CompoundBlockOperator::new(
                        operators.clone(),
                        func_ctx2.clone(),
                        num_input_columns,
                    ))
                },
                transform_len,
            )?;
            if need_match {
                builder.add_items_prepend(vec![create_dummy_item()]);
            }
            pipeline.add_pipe(builder.finalize());
        }

        let cluster_keys = &cluster_stats_gen.cluster_key_index;
        if !cluster_keys.is_empty() {
            let sort_desc: Vec<SortColumnDescription> = cluster_keys
                .iter()
                .map(|index| SortColumnDescription {
                    offset: *index,
                    asc: true,
                    nulls_first: false,
                    is_nullable: false, // This information is not needed here.
                })
                .collect();
            let sort_desc = Arc::new(sort_desc);

            let mut builder = pipeline.try_create_transform_pipeline_builder_with_len(
                || Ok(TransformSortPartial::new(None, sort_desc.clone())),
                transform_len,
            )?;
            if need_match {
                builder.add_items_prepend(vec![create_dummy_item()]);
            }
            pipeline.add_pipe(builder.finalize());
        }
        Ok(cluster_stats_gen)
    }

    pub fn cluster_gen_for_append(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        block_thresholds: BlockThresholds,
        modified_schema: Option<Arc<DataSchema>>,
    ) -> Result<ClusterStatsGenerator> {
        let cluster_stats_gen =
            self.get_cluster_stats_gen(ctx.clone(), 0, block_thresholds, modified_schema)?;

        let operators = cluster_stats_gen.operators.clone();
        if !operators.is_empty() {
            let num_input_columns = self.table_info.schema().fields().len();
            let func_ctx2 = cluster_stats_gen.func_ctx.clone();

            pipeline.add_transformer(move || {
                CompoundBlockOperator::new(operators.clone(), func_ctx2.clone(), num_input_columns)
            });
        }

        let cluster_keys = &cluster_stats_gen.cluster_key_index;
        if !cluster_keys.is_empty() {
            let sort_desc: Vec<SortColumnDescription> = cluster_keys
                .iter()
                .map(|index| SortColumnDescription {
                    offset: *index,
                    asc: true,
                    nulls_first: false,
                    is_nullable: false, // This information is not needed here.
                })
                .collect();
            let sort_desc = Arc::new(sort_desc);
            pipeline.add_transformer(|| TransformSortPartial::new(None, sort_desc.clone()));
        }
        Ok(cluster_stats_gen)
    }

    pub fn get_cluster_stats_gen(
        &self,
        ctx: Arc<dyn TableContext>,
        level: i32,
        block_thresholds: BlockThresholds,
        modified_schema: Option<Arc<DataSchema>>,
    ) -> Result<ClusterStatsGenerator> {
        let cluster_keys = self.cluster_keys(ctx.clone());
        if cluster_keys.is_empty() {
            return Ok(ClusterStatsGenerator::default());
        }

        let input_schema =
            modified_schema.unwrap_or(DataSchema::from(self.schema_with_stream()).into());
        let mut merged = input_schema.fields().clone();

        let mut cluster_key_index = Vec::with_capacity(cluster_keys.len());
        let mut extra_key_num = 0;

        let mut exprs = Vec::with_capacity(cluster_keys.len());

        for remote_expr in &cluster_keys {
            let expr = remote_expr
                .as_expr(&BUILTIN_FUNCTIONS)
                .project_column_ref(|name| input_schema.index_of(name).unwrap());
            let index = match &expr {
                Expr::ColumnRef { id, .. } => *id,
                _ => {
                    let cname = format!("{}", expr);
                    merged.push(DataField::new(cname.as_str(), expr.data_type().clone()));
                    exprs.push(expr);

                    let offset = merged.len() - 1;
                    extra_key_num += 1;
                    offset
                }
            };
            cluster_key_index.push(index);
        }

        let operators = if exprs.is_empty() {
            vec![]
        } else {
            vec![BlockOperator::Map {
                exprs,
                projections: None,
            }]
        };

        Ok(ClusterStatsGenerator::new(
            self.cluster_key_meta.as_ref().unwrap().0,
            cluster_key_index,
            extra_key_num,
            self.get_max_page_size(),
            level,
            block_thresholds,
            operators,
            merged,
            ctx.get_function_context()?,
        ))
    }

    pub fn get_option<T: FromStr>(&self, opt_key: &str, default: T) -> T {
        self.table_info
            .options()
            .get(opt_key)
            .and_then(|s| s.parse::<T>().ok())
            .unwrap_or(default)
    }
}
