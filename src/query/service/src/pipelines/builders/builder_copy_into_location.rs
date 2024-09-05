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
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_sql::executor::physical_plans::CopyIntoLocation;
use databend_common_storages_stage::StageTable;

use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_copy_into_location(&mut self, copy: &CopyIntoLocation) -> Result<()> {
        self.build_pipeline(&copy.input)?;

        // Reorder the result for select clause
        PipelineBuilder::build_result_projection(
            &self.func_ctx,
            copy.input.output_schema()?,
            &copy.project_columns,
            &mut self.main_pipeline,
            false,
        )?;

        let to_table = StageTable::try_create(copy.to_stage_info.clone())?;
        PipelineBuilder::build_append2table_with_commit_pipeline(
            self.ctx.clone(),
            &mut self.main_pipeline,
            to_table,
            copy.input_schema.clone(),
            None,
            vec![],
            false,
            AppendMode::Normal,
            unsafe { self.ctx.get_settings().get_deduplicate_label()? },
            Default::default(),
        )
    }
}
