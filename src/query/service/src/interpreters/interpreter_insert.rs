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

use databend_common_catalog::lock::LockTableOption;
use databend_common_catalog::table::AppendMode;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_pipeline_sources::AsyncSourcer;
use databend_common_sql::executor::physical_plans::DistributedInsertSelect;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_sql::executor::PhysicalPlanBuilder;
use databend_common_sql::plans::Insert;
use databend_common_sql::plans::InsertInputSource;
use databend_common_sql::plans::InsertValue;
use databend_common_sql::plans::Plan;
use databend_common_sql::NameResolutionContext;
use log::info;

use crate::interpreters::common::check_deduplicate_label;
use crate::interpreters::common::dml_build_update_stream_req;
use crate::interpreters::HookOperator;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::PipelineBuilder;
use crate::pipelines::RawValueSource;
use crate::pipelines::ValueSource;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct InsertInterpreter {
    ctx: Arc<QueryContext>,
    plan: Insert,
}

impl InsertInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: Insert) -> Result<InterpreterPtr> {
        Ok(Arc::new(InsertInterpreter { ctx, plan }))
    }

    fn check_schema_cast(&self, plan: &Plan) -> Result<bool> {
        let output_schema = &self.plan.schema;
        let select_schema = plan.schema();

        // validate schema
        if select_schema.fields().len() != output_schema.fields().len() {
            return Err(ErrorCode::BadArguments(format!(
                "Fields in select statement is not equal with expected, select fields: {}, insert fields: {}",
                select_schema.fields().len(),
                output_schema.fields().len(),
            )));
        }

        // check if cast needed
        let cast_needed = select_schema.as_ref() != &DataSchema::from(output_schema.as_ref());
        Ok(cast_needed)
    }
}

#[async_trait::async_trait]
impl Interpreter for InsertInterpreter {
    fn name(&self) -> &str {
        "InsertIntoInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        if check_deduplicate_label(self.ctx.clone()).await? {
            return Ok(PipelineBuildResult::create());
        }
        let table = if let Some(table_info) = &self.plan.table_info {
            // if table_info is provided, we should instantiated table with it.
            self.ctx
                .get_catalog(&self.plan.catalog)
                .await?
                .get_table_by_info(table_info)?
        } else {
            self.ctx
                .get_table(&self.plan.catalog, &self.plan.database, &self.plan.table)
                .await?
        };

        // check mutability
        table.check_mutable()?;
        let table_meta_timestamps = if table.engine() == "FUSE" {
            let fuse_table =
                databend_common_storages_fuse::FuseTable::try_from_table(table.as_ref())?;
            let snapshot = fuse_table.read_table_snapshot().await?;
            self.ctx
                .get_table_meta_timestamps(table.get_id(), snapshot)?
        } else {
            Default::default()
        };

        let mut build_res = PipelineBuildResult::create();

        match &self.plan.source {
            InsertInputSource::Stage(_) => {
                unreachable!()
            }
            InsertInputSource::Values(InsertValue::Values { rows }) => {
                build_res.main_pipeline.add_source(
                    |output| {
                        let inner = ValueSource::new(rows.clone(), self.plan.dest_schema());
                        AsyncSourcer::create(self.ctx.clone(), output, inner)
                    },
                    1,
                )?;
            }
            InsertInputSource::Values(InsertValue::RawValues { data, start }) => {
                build_res.main_pipeline.add_source(
                    |output| {
                        let name_resolution_ctx = NameResolutionContext {
                            deny_column_reference: true,
                            ..Default::default()
                        };
                        let inner = RawValueSource::new(
                            data.to_string(),
                            self.ctx.clone(),
                            name_resolution_ctx,
                            self.plan.dest_schema(),
                            *start,
                        );
                        AsyncSourcer::create(self.ctx.clone(), output, inner)
                    },
                    1,
                )?;
            }
            InsertInputSource::SelectPlan(plan) => {
                let table1 = table.clone();
                let (mut select_plan, select_column_bindings, metadata) = match plan.as_ref() {
                    Plan::Query {
                        s_expr,
                        metadata,
                        bind_context,
                        ..
                    } => {
                        let mut builder1 =
                            PhysicalPlanBuilder::new(metadata.clone(), self.ctx.clone(), false);
                        (
                            builder1.build(s_expr, bind_context.column_set()).await?,
                            bind_context.columns.clone(),
                            metadata,
                        )
                    }
                    _ => unreachable!(),
                };

                let explain_plan = select_plan
                    .format(metadata.clone(), Default::default())?
                    .format_pretty()?;
                info!("Insert select plan: \n{}", explain_plan);

                let update_stream_meta =
                    dml_build_update_stream_req(self.ctx.clone(), metadata).await?;

                // here we remove the last exchange merge plan to trigger distribute insert
                let insert_select_plan = match select_plan {
                    PhysicalPlan::Exchange(ref mut exchange) => {
                        // insert can be dispatched to different nodes
                        let input = exchange.input.clone();
                        exchange.input = Box::new(PhysicalPlan::DistributedInsertSelect(Box::new(
                            DistributedInsertSelect {
                                // TODO(leiysky): we reuse the id of exchange here,
                                // which is not correct. We should generate a new id for insert.
                                plan_id: exchange.plan_id,
                                input,
                                table_info: table1.get_table_info().clone(),
                                select_schema: plan.schema(),
                                select_column_bindings,
                                insert_schema: self.plan.dest_schema(),
                                cast_needed: self.check_schema_cast(plan)?,
                                table_meta_timestamps,
                            },
                        )));
                        select_plan
                    }
                    other_plan => {
                        // insert should wait until all nodes finished
                        PhysicalPlan::DistributedInsertSelect(Box::new(DistributedInsertSelect {
                            // TODO: we reuse the id of other plan here,
                            // which is not correct. We should generate a new id for insert.
                            plan_id: other_plan.get_id(),
                            input: Box::new(other_plan),
                            table_info: table1.get_table_info().clone(),
                            select_schema: plan.schema(),
                            select_column_bindings,
                            insert_schema: self.plan.dest_schema(),
                            cast_needed: self.check_schema_cast(plan)?,
                            table_meta_timestamps,
                        }))
                    }
                };

                let mut build_res =
                    build_query_pipeline_without_render_result_set(&self.ctx, &insert_select_plan)
                        .await?;

                table.commit_insertion(
                    self.ctx.clone(),
                    &mut build_res.main_pipeline,
                    None,
                    update_stream_meta,
                    self.plan.overwrite,
                    None,
                    unsafe { self.ctx.get_settings().get_deduplicate_label()? },
                    table_meta_timestamps,
                )?;

                //  Execute the hook operator.
                {
                    let hook_operator = HookOperator::create(
                        self.ctx.clone(),
                        self.plan.catalog.clone(),
                        self.plan.database.clone(),
                        self.plan.table.clone(),
                        MutationKind::Insert,
                        LockTableOption::LockNoRetry,
                    );
                    hook_operator.execute(&mut build_res.main_pipeline).await;
                }

                return Ok(build_res);
            }
        };

        PipelineBuilder::build_append2table_with_commit_pipeline(
            self.ctx.clone(),
            &mut build_res.main_pipeline,
            table.clone(),
            self.plan.dest_schema(),
            None,
            vec![],
            self.plan.overwrite,
            AppendMode::Normal,
            unsafe { self.ctx.get_settings().get_deduplicate_label()? },
            table_meta_timestamps,
        )?;

        //  Execute the hook operator.
        {
            let hook_operator = HookOperator::create(
                self.ctx.clone(),
                self.plan.catalog.clone(),
                self.plan.database.clone(),
                self.plan.table.clone(),
                MutationKind::Insert,
                LockTableOption::LockNoRetry,
            );
            hook_operator.execute(&mut build_res.main_pipeline).await;
        }

        Ok(build_res)
    }
}
