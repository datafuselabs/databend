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

use std::collections::BTreeMap;
use std::sync::Arc;

use chrono::Utc;
use databend_common_catalog::table::AppendMode;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::Scalar;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::ParquetFileFormatParams;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::schema::TableCopiedFileInfo;
use databend_common_meta_app::schema::UpsertTableCopiedFileReq;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_sql::executor::physical_plans::CopyIntoTable;
use databend_common_sql::executor::physical_plans::CopyIntoTableSource;
use databend_common_sql::plans::CopyIntoTableMode;
use databend_common_storage::StageFileInfo;
use log::debug;
use log::info;

use crate::pipelines::processors::transforms::TransformAddConstColumns;
use crate::pipelines::processors::TransformCastSchema;
use crate::pipelines::processors::TransformNullIf;
use crate::pipelines::PipelineBuilder;
use crate::sessions::QueryContext;

/// This file implements copy into table pipeline builder.
impl PipelineBuilder {
    pub(crate) fn build_copy_into_table(&mut self, copy: &CopyIntoTable) -> Result<()> {
        let to_table = self.ctx.build_table_by_table_info(&copy.table_info, None)?;
        let source_schema = match &copy.source {
            CopyIntoTableSource::Query(input) => {
                self.build_pipeline(input)?;
                // Reorder the result for select clause
                PipelineBuilder::build_result_projection(
                    &self.func_ctx,
                    input.output_schema()?,
                    copy.project_columns.as_ref().unwrap(),
                    &mut self.main_pipeline,
                    false,
                )?;
                let fields = copy
                    .project_columns
                    .as_ref()
                    .unwrap()
                    .iter()
                    .map(|column_binding| {
                        DataField::new(
                            &column_binding.column_name,
                            *column_binding.data_type.clone(),
                        )
                    })
                    .collect();
                DataSchemaRefExt::create(fields)
            }
            CopyIntoTableSource::Stage(input) => {
                self.ctx
                    .set_read_block_thresholds(to_table.get_block_thresholds());

                self.build_pipeline(input)?;
                copy.required_source_schema.clone()
            }
        };
        Self::build_append_data_pipeline(
            self.ctx.clone(),
            &mut self.main_pipeline,
            copy,
            source_schema,
            to_table,
        )?;
        Ok(())
    }

    fn need_null_if_processor<'a>(
        plan: &'a CopyIntoTable,
        source_schema: &Arc<DataSchema>,
        dest_schema: &Arc<DataSchema>,
    ) -> Option<&'a [String]> {
        if plan.is_transform {
            return None;
        }
        if let FileFormatParams::Parquet(ParquetFileFormatParams { null_if, .. }) =
            &plan.stage_table_info.stage_info.file_format_params
        {
            if !null_if.is_empty()
                && source_schema
                    .fields
                    .iter()
                    .zip(dest_schema.fields.iter())
                    .any(|(src_field, dest_field)| {
                        TransformNullIf::column_need_transform(
                            src_field.data_type(),
                            dest_field.data_type(),
                        )
                    })
            {
                return Some(null_if);
            }
        }
        None
    }

    fn build_append_data_pipeline(
        ctx: Arc<QueryContext>,
        main_pipeline: &mut Pipeline,
        plan: &CopyIntoTable,
        source_schema: Arc<DataSchema>,
        to_table: Arc<dyn Table>,
    ) -> Result<()> {
        let plan_required_source_schema = &plan.required_source_schema;
        let plan_values_consts = &plan.values_consts;
        let plan_required_values_schema = &plan.required_values_schema;
        let plan_write_mode = &plan.write_mode;

        let source_schema = if let Some(null_if) =
            Self::need_null_if_processor(plan, &source_schema, plan_required_source_schema)
        {
            let func_ctx = ctx.get_function_context()?;
            main_pipeline.try_add_transformer(|| {
                TransformNullIf::try_new(
                    source_schema.clone(),
                    plan_required_source_schema.clone(),
                    func_ctx.clone(),
                    null_if,
                )
            })?;
            TransformNullIf::new_schema(&source_schema)
        } else {
            source_schema
        };

        if &source_schema != plan_required_source_schema {
            // only parquet need cast
            let func_ctx = ctx.get_function_context()?;
            main_pipeline.try_add_transformer(|| {
                TransformCastSchema::try_new(
                    source_schema.clone(),
                    plan_required_source_schema.clone(),
                    func_ctx.clone(),
                )
            })?;
        }

        if !plan_values_consts.is_empty() {
            Self::fill_const_columns(
                ctx.clone(),
                main_pipeline,
                source_schema,
                plan_required_values_schema.clone(),
                plan_values_consts,
            )?;
        }

        // append data without commit.
        match plan_write_mode {
            CopyIntoTableMode::Insert { overwrite: _ } => {
                Self::build_append2table_without_commit_pipeline(
                    ctx,
                    main_pipeline,
                    to_table.clone(),
                    plan_required_values_schema.clone(),
                    AppendMode::Copy,
                    plan.table_meta_timestamps,
                )?
            }
            CopyIntoTableMode::Replace => {}
            CopyIntoTableMode::Copy => Self::build_append2table_without_commit_pipeline(
                ctx,
                main_pipeline,
                to_table.clone(),
                plan_required_values_schema.clone(),
                AppendMode::Copy,
                plan.table_meta_timestamps,
            )?,
        }
        Ok(())
    }

    pub(crate) fn build_upsert_copied_files_to_meta_req(
        ctx: Arc<QueryContext>,
        to_table: &dyn Table,
        stage_info: &StageInfo,
        copied_files: &[StageFileInfo],
        force: bool,
    ) -> Result<Option<UpsertTableCopiedFileReq>> {
        let mut copied_file_tree = BTreeMap::new();
        for file in copied_files {
            // Short the etag to 7 bytes for less space in metasrv.
            let short_etag = file.etag.clone().map(|mut v| {
                v.truncate(7);
                v
            });
            copied_file_tree.insert(file.path.clone(), TableCopiedFileInfo {
                etag: short_etag,
                content_length: file.size,
                last_modified: Some(file.last_modified),
            });
        }

        let expire_hours = ctx.get_settings().get_load_file_metadata_expire_hours()?;

        let upsert_copied_files_request = {
            if stage_info.copy_options.purge && force {
                // if `purge-after-copy` is enabled, and in `force` copy mode,
                // we do not need to upsert copied files into meta server
                info!(
                    "[purge] and [force] are both enabled,  will not update copied-files set. ({})",
                    &to_table.get_table_info().desc
                );
                None
            } else if copied_file_tree.is_empty() {
                None
            } else {
                debug!("upsert_copied_files_info: {:?}", copied_file_tree);
                let expire_at = expire_hours * 60 * 60 + Utc::now().timestamp() as u64;
                let req = UpsertTableCopiedFileReq {
                    file_info: copied_file_tree,
                    expire_at: Some(expire_at),
                    fail_if_duplicated: !force,
                };
                Some(req)
            }
        };

        Ok(upsert_copied_files_request)
    }

    fn fill_const_columns(
        ctx: Arc<QueryContext>,
        pipeline: &mut Pipeline,
        input_schema: DataSchemaRef,
        output_schema: DataSchemaRef,
        const_values: &[Scalar],
    ) -> Result<()> {
        pipeline.try_add_transformer(|| {
            TransformAddConstColumns::try_new(
                ctx.clone(),
                input_schema.clone(),
                output_schema.clone(),
                const_values.to_vec(),
            )
        })
    }
}
