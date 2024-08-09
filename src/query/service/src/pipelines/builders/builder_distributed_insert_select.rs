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

use databend_common_catalog::table::AppendMode;
use databend_common_exception::Result;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_sql::executor::physical_plans::DistributedInsertSelect;

use crate::pipelines::processors::TransformCastSchema;
use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub fn build_distributed_insert_select(
        &mut self,
        insert_select: &DistributedInsertSelect,
    ) -> Result<()> {
        let select_schema = &insert_select.select_schema;
        let insert_schema = &insert_select.insert_schema;

        self.build_pipeline(&insert_select.input)?;

        // should render result for select
        PipelineBuilder::build_result_projection(
            &self.func_ctx,
            insert_select.input.output_schema()?,
            &insert_select.select_column_bindings,
            &mut self.main_pipeline,
            false,
        )?;

        if insert_select.cast_needed {
            self.main_pipeline.try_add_transformer(|| {
                TransformCastSchema::try_new(
                    select_schema.clone(),
                    insert_schema.clone(),
                    self.func_ctx.clone(),
                )
            })?;
        }

        let table = self
            .ctx
            .build_table_by_table_info(&insert_select.table_info, None)?;

        let source_schema = insert_schema;
        Self::fill_and_reorder_columns(
            self.ctx.clone(),
            &mut self.main_pipeline,
            table.clone(),
            source_schema.clone(),
        )?;

        table.append_data(
            self.ctx.clone(),
            &mut self.main_pipeline,
            AppendMode::Normal,
            insert_select.table_meta_timestamps,
        )?;

        Ok(())
    }
}
