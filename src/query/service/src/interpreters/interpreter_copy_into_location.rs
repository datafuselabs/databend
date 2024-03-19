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

use databend_common_catalog::plan::StageTableInfo;
use databend_common_catalog::table::AppendMode;
use databend_common_exception::Result;
use databend_common_expression::infer_table_schema;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_meta_app::principal::StageInfo;
use databend_common_storage::StageFilesInfo;
use databend_common_storages_stage::StageTable;
use log::debug;

use crate::interpreters::common::check_deduplicate_label;
use crate::interpreters::Interpreter;
use crate::interpreters::SelectInterpreter;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::PipelineBuilder;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::CopyIntoLocationPlan;
use crate::sql::plans::Plan;

pub struct CopyIntoLocationInterpreter {
    ctx: Arc<QueryContext>,
    plan: CopyIntoLocationPlan,
}

impl CopyIntoLocationInterpreter {
    /// Create a CopyInterpreter with context and [`CopyIntoLocationPlan`].
    pub fn try_create(ctx: Arc<QueryContext>, plan: CopyIntoLocationPlan) -> Result<Self> {
        Ok(CopyIntoLocationInterpreter { ctx, plan })
    }

    #[async_backtrace::framed]
    async fn build_query(&self, query: &Plan) -> Result<(SelectInterpreter, DataSchemaRef)> {
        let (s_expr, metadata, bind_context, formatted_ast) = match query {
            Plan::Query {
                s_expr,
                metadata,
                bind_context,
                formatted_ast,
                ..
            } => (s_expr, metadata, bind_context, formatted_ast),
            v => unreachable!("Input plan must be Query, but it's {}", v),
        };

        let select_interpreter = SelectInterpreter::try_create(
            self.ctx.clone(),
            *(bind_context.clone()),
            *s_expr.clone(),
            metadata.clone(),
            formatted_ast.clone(),
            false,
        )?;

        // Building data schema from bind_context columns
        // TODO(leiyskey): Extract the following logic as new API of BindContext.
        let fields = bind_context
            .columns
            .iter()
            .map(|column_binding| {
                DataField::new(
                    &column_binding.column_name,
                    *column_binding.data_type.clone(),
                )
            })
            .collect();
        let data_schema = DataSchemaRefExt::create(fields);

        Ok((select_interpreter, data_schema))
    }

    /// Build a pipeline for local copy into stage.
    #[async_backtrace::framed]
    async fn build_local_copy_into_stage_pipeline(
        &self,
        stage: &StageInfo,
        path: &str,
        query: &Plan,
    ) -> Result<PipelineBuildResult> {
        let (select_interpreter, data_schema) = self.build_query(query).await?;
        let plan = select_interpreter.build_physical_plan().await?;
        let mut build_res = select_interpreter.build_pipeline(plan).await?;
        let table_schema = infer_table_schema(&data_schema)?;
        let stage_table_info = StageTableInfo {
            schema: table_schema,
            stage_info: stage.clone(),
            files_info: StageFilesInfo {
                path: path.to_string(),
                files: None,
                pattern: None,
                start_after: None,
            },
            files_to_copy: None,
            duplicated_files_detected: vec![],
            is_select: false,
            default_values: None,
        };
        let to_table = StageTable::try_create(stage_table_info)?;
        PipelineBuilder::build_append2table_with_commit_pipeline(
            self.ctx.clone(),
            &mut build_res.main_pipeline,
            to_table,
            data_schema,
            None,
            vec![],
            false,
            AppendMode::Normal,
            unsafe { self.ctx.get_settings().get_deduplicate_label()? },
        )?;
        Ok(build_res)
    }
}

#[async_trait::async_trait]
impl Interpreter for CopyIntoLocationInterpreter {
    fn name(&self) -> &str {
        "CopyIntoLocationInterpreterV2"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[minitrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "copy_into_location_interpreter_execute_v2");

        if check_deduplicate_label(self.ctx.clone()).await? {
            return Ok(PipelineBuildResult::create());
        }
        self.build_local_copy_into_stage_pipeline(
            &self.plan.stage,
            &self.plan.path,
            &self.plan.from,
        )
        .await
    }
}
