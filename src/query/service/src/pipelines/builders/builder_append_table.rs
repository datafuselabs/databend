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

use databend_common_catalog::table::AppendMode;
use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_common_meta_app::schema::UpsertTableCopiedFileReq;
use databend_common_pipeline_core::Pipeline;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;

use crate::pipelines::PipelineBuilder;
use crate::sessions::QueryContext;

/// This file implements append to table pipeline builder.
impl PipelineBuilder {
    #[allow(clippy::too_many_arguments)]
    pub fn build_append2table_with_commit_pipeline(
        ctx: Arc<QueryContext>,
        main_pipeline: &mut Pipeline,
        table: Arc<dyn Table>,
        source_schema: DataSchemaRef,
        copied_files: Option<UpsertTableCopiedFileReq>,
        update_stream_meta: Vec<UpdateStreamMetaReq>,
        overwrite: bool,
        append_mode: AppendMode,
        deduplicated_label: Option<String>,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<()> {
        Self::fill_and_reorder_columns(ctx.clone(), main_pipeline, table.clone(), source_schema)?;

        table.append_data(
            ctx.clone(),
            main_pipeline,
            append_mode,
            table_meta_timestamps,
        )?;
        table.commit_insertion(
            ctx,
            main_pipeline,
            copied_files,
            update_stream_meta,
            overwrite,
            None,
            deduplicated_label,
            table_meta_timestamps,
        )?;

        Ok(())
    }

    pub fn build_append2table_without_commit_pipeline(
        ctx: Arc<QueryContext>,
        main_pipeline: &mut Pipeline,
        table: Arc<dyn Table>,
        source_schema: DataSchemaRef,
        append_mode: AppendMode,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<()> {
        Self::fill_and_reorder_columns(ctx.clone(), main_pipeline, table.clone(), source_schema)?;

        table.append_data(ctx, main_pipeline, append_mode, table_meta_timestamps)?;

        Ok(())
    }
}
