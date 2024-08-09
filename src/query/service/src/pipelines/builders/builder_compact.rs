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

use databend_common_exception::Result;
use databend_common_pipeline_sources::EmptySource;
use databend_common_sql::executor::physical_plans::CompactSource;
use databend_common_storages_fuse::FuseTable;

use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_compact_source(&mut self, compact_block: &CompactSource) -> Result<()> {
        let table = self
            .ctx
            .build_table_by_table_info(&compact_block.table_info, None)?;
        let table = FuseTable::try_from_table(table.as_ref())?;

        if compact_block.parts.is_empty() {
            return self.main_pipeline.add_source(EmptySource::create, 1);
        }

        table.build_compact_source(
            self.ctx.clone(),
            compact_block.parts.clone(),
            compact_block.column_ids.clone(),
            &mut self.main_pipeline,
            compact_block.table_meta_timestamps,
        )
    }
}
