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
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_catalog::catalog::CatalogManager;
use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::DynTransformBuilder;
use databend_common_pipeline_sinks::AsyncSinker;
use databend_common_pipeline_transforms::processors::TransformSortPartial;
use databend_common_sql::executor::physical_plans::ChunkAppendData;
use databend_common_sql::executor::physical_plans::ChunkCastSchema;
use databend_common_sql::executor::physical_plans::ChunkCommitInsert;
use databend_common_sql::executor::physical_plans::ChunkEvalScalar;
use databend_common_sql::executor::physical_plans::ChunkFillAndReorder;
use databend_common_sql::executor::physical_plans::ChunkFilter;
use databend_common_sql::executor::physical_plans::ChunkMerge;
use databend_common_sql::executor::physical_plans::Duplicate;
use databend_common_sql::executor::physical_plans::Shuffle;
use databend_common_storages_fuse::operations::CommitMultiTableInsert;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::TableContext;

use crate::pipelines::PipelineBuilder;
use crate::sql::evaluator::CompoundBlockOperator;
impl PipelineBuilder {
    pub(crate) fn build_duplicate(&mut self, plan: &Duplicate) -> Result<()> {
        self.build_pipeline(&plan.input)?;
        self.main_pipeline.duplicate(true, plan.n)?;
        Ok(())
    }

    pub(crate) fn build_shuffle(&mut self, plan: &Shuffle) -> Result<()> {
        self.build_pipeline(&plan.input)?;
        self.main_pipeline
            .reorder_inputs(plan.strategy.shuffle(self.main_pipeline.output_len())?);
        Ok(())
    }

    pub(crate) fn build_chunk_filter(&mut self, plan: &ChunkFilter) -> Result<()> {
        self.build_pipeline(&plan.input)?;
        if plan.predicates.iter().all(|x| x.is_none()) {
            return Ok(());
        }
        let mut f: Vec<DynTransformBuilder> = Vec::with_capacity(plan.predicates.len());
        let projection: HashSet<_> = (0..plan.input.output_schema()?.fields.len()).collect();
        for predicate in plan.predicates.iter() {
            if let Some(predicate) = predicate {
                f.push(Box::new(self.filter_transform_builder(
                    &[predicate.clone()],
                    projection.clone(),
                )?));
            } else {
                f.push(Box::new(self.dummy_transform_builder()?));
            }
        }
        self.main_pipeline.add_transforms_by_chunk(f)?;
        Ok(())
    }

    pub(crate) fn build_chunk_eval_scalar(&mut self, plan: &ChunkEvalScalar) -> Result<()> {
        self.build_pipeline(&plan.input)?;
        if plan.eval_scalars.iter().all(|x| x.is_none()) {
            return Ok(());
        }
        let num_input_columns = plan.input.output_schema()?.num_fields();
        let mut f: Vec<DynTransformBuilder> = Vec::with_capacity(plan.eval_scalars.len());
        for eval_scalar in plan.eval_scalars.iter() {
            if let Some(eval_scalar) = eval_scalar {
                f.push(Box::new(self.map_transform_builder(
                    num_input_columns,
                    eval_scalar.remote_exprs.clone(),
                    Some(eval_scalar.projection.clone()),
                )?));
            } else {
                f.push(Box::new(self.dummy_transform_builder()?));
            }
        }
        self.main_pipeline.add_transforms_by_chunk(f)?;
        Ok(())
    }

    pub(crate) fn build_chunk_cast_schema(&mut self, plan: &ChunkCastSchema) -> Result<()> {
        self.build_pipeline(&plan.input)?;
        if plan.cast_schemas.iter().all(|x| x.is_none()) {
            return Ok(());
        }
        let mut f: Vec<DynTransformBuilder> = Vec::with_capacity(plan.cast_schemas.len());
        for cast_schema in plan.cast_schemas.iter() {
            if let Some(cast_schema) = cast_schema {
                f.push(Box::new(self.cast_schema_transform_builder(
                    cast_schema.source_schema.clone(),
                    cast_schema.target_schema.clone(),
                )?));
            } else {
                f.push(Box::new(self.dummy_transform_builder()?));
            }
        }
        self.main_pipeline.add_transforms_by_chunk(f)?;
        Ok(())
    }

    pub(crate) fn build_chunk_fill_and_reorder(
        &mut self,
        plan: &ChunkFillAndReorder,
    ) -> Result<()> {
        self.build_pipeline(&plan.input)?;
        if plan.fill_and_reorders.iter().all(|x| x.is_none()) {
            return Ok(());
        }
        let mut f: Vec<DynTransformBuilder> = Vec::with_capacity(plan.fill_and_reorders.len());
        for fill_and_reorder in plan.fill_and_reorders.iter() {
            if let Some(fill_and_reorder) = fill_and_reorder {
                let table = self
                    .ctx
                    .build_table_by_table_info(&fill_and_reorder.target_table_info, None)?;
                f.push(Box::new(self.fill_and_reorder_transform_builder(
                    table,
                    fill_and_reorder.source_schema.clone(),
                )?));
            } else {
                f.push(Box::new(self.dummy_transform_builder()?));
            }
        }
        self.main_pipeline.add_transforms_by_chunk(f)?;
        Ok(())
    }

    pub(crate) fn build_chunk_merge(&mut self, plan: &ChunkMerge) -> Result<()> {
        self.build_pipeline(&plan.input)?;
        let group_ids = &plan.group_ids;
        assert_eq!(self.main_pipeline.output_len() % group_ids.len(), 0);
        let chunk_size = self.main_pipeline.output_len() / group_ids.len();
        let mut widths = Vec::with_capacity(group_ids.len());
        let mut last_group_id = group_ids[0];
        let mut width = 1;
        for group_id in group_ids.iter().skip(1) {
            if *group_id == last_group_id {
                width += 1;
            } else {
                widths.push(width * chunk_size);
                last_group_id = *group_id;
                width = 1;
            }
        }
        widths.push(width * chunk_size);
        self.main_pipeline.resize_partial_one_with_width(widths)?;
        Ok(())
    }

    pub(crate) fn build_chunk_append_data(&mut self, plan: &ChunkAppendData) -> Result<()> {
        self.build_pipeline(&plan.input)?;
        let mut compact_builders: Vec<DynTransformBuilder> =
            Vec::with_capacity(plan.target_tables.len());
        let mut serialize_block_builders: Vec<DynTransformBuilder> =
            Vec::with_capacity(plan.target_tables.len());
        let mut eval_cluster_key_builders: Vec<DynTransformBuilder> =
            Vec::with_capacity(plan.target_tables.len());
        let mut eval_cluster_key_num = 0;
        let mut sort_builders: Vec<DynTransformBuilder> =
            Vec::with_capacity(plan.target_tables.len());
        let mut sort_num = 0;

        for append_data in plan.target_tables.iter() {
            let table = self
                .ctx
                .build_table_by_table_info(&append_data.target_table_info, None)?;
            let block_thresholds = table.get_block_thresholds();
            compact_builders.push(Box::new(
                self.block_compact_transform_builder(block_thresholds)?,
            ));
            let schema: Arc<DataSchema> = DataSchema::from(table.schema()).into();
            let num_input_columns = schema.num_fields();
            let fuse_table = FuseTable::try_from_table(table.as_ref())?;
            let cluster_stats_gen = fuse_table.get_cluster_stats_gen(
                self.ctx.clone(),
                0,
                block_thresholds,
                Some(schema),
            )?;
            let operators = cluster_stats_gen.operators.clone();
            if !operators.is_empty() {
                let func_ctx2 = cluster_stats_gen.func_ctx.clone();

                eval_cluster_key_builders.push(Box::new(move |input, output| {
                    Ok(ProcessorPtr::create(CompoundBlockOperator::create(
                        input,
                        output,
                        num_input_columns,
                        func_ctx2.clone(),
                        operators.clone(),
                    )))
                }));
                eval_cluster_key_num += 1;
            } else {
                eval_cluster_key_builders.push(Box::new(self.dummy_transform_builder()?));
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
                sort_builders.push(Box::new(
                    move |transform_input_port, transform_output_port| {
                        Ok(ProcessorPtr::create(TransformSortPartial::try_create(
                            transform_input_port,
                            transform_output_port,
                            None,
                            sort_desc.clone(),
                        )?))
                    },
                ));
                sort_num += 1;
            } else {
                sort_builders.push(Box::new(self.dummy_transform_builder()?));
            }
            serialize_block_builders.push(Box::new(
                self.with_tid_serialize_block_transform_builder(
                    table,
                    cluster_stats_gen,
                    append_data.table_meta_timestamps,
                )?,
            ));
        }
        self.main_pipeline
            .add_transforms_by_chunk(compact_builders)?;
        if eval_cluster_key_num > 0 {
            self.main_pipeline
                .add_transforms_by_chunk(eval_cluster_key_builders)?;
        }
        if sort_num > 0 {
            self.main_pipeline.add_transforms_by_chunk(sort_builders)?;
        }
        self.main_pipeline
            .add_transforms_by_chunk(serialize_block_builders)?;
        Ok(())
    }

    pub(crate) fn build_chunk_commit_insert(&mut self, plan: &ChunkCommitInsert) -> Result<()> {
        let ChunkCommitInsert {
            plan_id: _,
            input,
            update_stream_meta,
            overwrite,
            deduplicated_label,
            targets,
        } = plan;
        let mut table_meta_timestampss = HashMap::new();
        self.build_pipeline(input)?;
        let mut serialize_segment_builders: Vec<DynTransformBuilder> =
            Vec::with_capacity(targets.len());
        let mut mutation_aggregator_builders: Vec<DynTransformBuilder> =
            Vec::with_capacity(targets.len());
        let mut tables = HashMap::new();
        for target in targets {
            let table = self
                .ctx
                .build_table_by_table_info(&target.target_table_info, None)?;
            let block_thresholds = table.get_block_thresholds();
            serialize_segment_builders.push(Box::new(self.serialize_segment_transform_builder(
                table.clone(),
                block_thresholds,
                target.table_meta_timestamps,
            )?));
            mutation_aggregator_builders.push(Box::new(
                self.mutation_aggregator_transform_builder(
                    table.clone(),
                    target.table_meta_timestamps,
                )?,
            ));
            table_meta_timestampss.insert(table.get_id(), target.table_meta_timestamps);
            tables.insert(table.get_id(), table);
        }
        self.main_pipeline
            .add_transforms_by_chunk(serialize_segment_builders)?;
        self.main_pipeline
            .add_transforms_by_chunk(mutation_aggregator_builders)?;
        self.main_pipeline.try_resize(1)?;
        let catalog = CatalogManager::instance().build_catalog(
            targets[0].target_catalog_info.clone(),
            self.ctx.session_state(),
        )?;
        self.main_pipeline.add_sink(|input| {
            Ok(ProcessorPtr::create(AsyncSinker::create(
                input,
                CommitMultiTableInsert::create(
                    tables.clone(),
                    self.ctx.clone(),
                    *overwrite,
                    update_stream_meta.clone(),
                    deduplicated_label.clone(),
                    catalog.clone(),
                    table_meta_timestampss.clone(),
                ),
            )))
        })?;
        Ok(())
    }
}
