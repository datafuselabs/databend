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

use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::RemoteExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_sql::binder::MutationType;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::evaluator::CompoundBlockOperator;
use databend_common_sql::executor::physical_plans::ColumnMutation;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_storages_fuse::operations::TableMutationAggregator;
use databend_common_storages_fuse::operations::TransformSerializeBlock;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_table_meta::meta::Statistics;

use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_column_mutation(&mut self, column_mutation: &ColumnMutation) -> Result<()> {
        self.build_pipeline(&column_mutation.input)?;
        if let Some(mutation_expr) = &column_mutation.mutation_expr {
            self.build_column_mutation_transform(
                mutation_expr.clone(),
                column_mutation.computed_expr.clone(),
                column_mutation.field_id_to_schema_index.clone(),
                column_mutation.input_num_columns,
                column_mutation.has_filter_column,
            )?;
        }

        let table = self
            .ctx
            .build_table_by_table_info(&column_mutation.table_info, None)?;
        let table = FuseTable::try_from_table(table.as_ref())?;

        if column_mutation.mutation_type == MutationType::Delete {
            let cluster_stats_gen = table.get_cluster_stats_gen(
                self.ctx.clone(),
                0,
                table.get_block_thresholds(),
                None,
            )?;
            self.main_pipeline.add_transform(|input, output| {
                let proc = TransformSerializeBlock::try_create(
                    self.ctx.clone(),
                    input,
                    output,
                    table,
                    cluster_stats_gen.clone(),
                    MutationKind::Delete,
                    column_mutation.table_meta_timestamps,
                )?;
                proc.into_processor()
            })?;

            if self.ctx.get_lazy_mutation_delete() {
                self.main_pipeline.try_resize(1)?;
                self.main_pipeline.add_async_accumulating_transformer(|| {
                    TableMutationAggregator::create(
                        table,
                        self.ctx.clone(),
                        self.ctx.get_table_snapshot().unwrap().segments.clone(),
                        vec![],
                        vec![],
                        Statistics::default(),
                        MutationKind::Delete,
                        column_mutation.table_meta_timestamps,
                    )
                });
            }
        } else {
            let block_thresholds = table.get_block_thresholds();
            let cluster_stats_gen = table.cluster_gen_for_append(
                self.ctx.clone(),
                &mut self.main_pipeline,
                block_thresholds,
                None,
            )?;
            self.main_pipeline.add_transform(|input, output| {
                let proc = TransformSerializeBlock::try_create(
                    self.ctx.clone(),
                    input,
                    output,
                    table,
                    cluster_stats_gen.clone(),
                    MutationKind::Update,
                    column_mutation.table_meta_timestamps,
                )?;
                proc.into_processor()
            })?;
        }

        Ok(())
    }

    pub(crate) fn build_column_mutation_transform(
        &mut self,
        mutation_expr: Vec<(usize, RemoteExpr)>,
        computed_expr: Option<Vec<(usize, RemoteExpr)>>,
        mut field_id_to_schema_index: HashMap<usize, usize>,
        num_input_columns: usize,
        has_filter_column: bool,
    ) -> Result<()> {
        let mut block_operators = Vec::new();
        let mut next_column_offset = num_input_columns;
        let mut schema_offset_to_new_offset = HashMap::new();

        // Build update expression BlockOperator.
        let mut exprs = Vec::with_capacity(mutation_expr.len());
        for (id, remote_expr) in mutation_expr.into_iter() {
            let expr = remote_expr.as_expr(&BUILTIN_FUNCTIONS);
            let schema_index = field_id_to_schema_index.get(&id).unwrap();
            schema_offset_to_new_offset.insert(*schema_index, next_column_offset);
            field_id_to_schema_index
                .entry(id)
                .and_modify(|e| *e = next_column_offset);
            next_column_offset += 1;
            exprs.push(expr);
        }
        if !exprs.is_empty() {
            block_operators.push(BlockOperator::Map {
                exprs,
                projections: None,
            });
        }

        // Build computed expression BlockOperator.
        if let Some(computed_expr) = computed_expr
            && !computed_expr.is_empty()
        {
            let mut exprs = Vec::with_capacity(computed_expr.len());
            for (id, remote_expr) in computed_expr.into_iter() {
                let expr = remote_expr
                    .as_expr(&BUILTIN_FUNCTIONS)
                    .project_column_ref(|index| {
                        *schema_offset_to_new_offset.get(index).unwrap_or(index)
                    });
                let schema_index = field_id_to_schema_index.get(&id).unwrap();
                schema_offset_to_new_offset.insert(*schema_index, next_column_offset);
                field_id_to_schema_index
                    .entry(id)
                    .and_modify(|e| *e = next_column_offset);
                next_column_offset += 1;
                exprs.push(expr);
            }
            block_operators.push(BlockOperator::Map {
                exprs,
                projections: None,
            });
        }

        // Keep the original order of the columns.
        let num_output_columns = num_input_columns - has_filter_column as usize;
        let mut projection = Vec::with_capacity(num_output_columns);
        for idx in 0..num_output_columns {
            if let Some(index) = schema_offset_to_new_offset.get(&idx) {
                projection.push(*index);
            } else {
                projection.push(idx);
            }
        }
        block_operators.push(BlockOperator::Project { projection });

        self.main_pipeline.add_transformer(|| {
            CompoundBlockOperator::new(
                block_operators.clone(),
                self.func_ctx.clone(),
                num_input_columns,
            )
        });

        Ok(())
    }
}
