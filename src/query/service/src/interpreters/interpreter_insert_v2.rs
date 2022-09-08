// Copyright 2022 Datafuse Labs.
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

use std::collections::VecDeque;
use std::sync::Arc;

use common_catalog::table::Table;
use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_exception::Result;
use common_functions::scalars::CastFunction;
use common_planners::StageKind;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;
use futures_util::StreamExt;
use parking_lot::Mutex;

use super::commit2table;
use super::commit2table_with_append_entries;
use super::interpreter_common::append2table;
use super::plan_schedulers::build_schedule_pipepline;
use super::ProcessorExecutorStream;
use crate::clusters::ClusterHelper;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::interpreters::SelectInterpreterV2;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelinePullingExecutor;
use crate::pipelines::processors::port::OutputPort;
use crate::pipelines::processors::BlocksSource;
use crate::pipelines::processors::TransformCastSchema;
use crate::pipelines::Pipeline;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::SourcePipeBuilder;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::executor::DistributedInsertSelect;
use crate::sql::executor::Exchange;
use crate::sql::executor::PhysicalPlan;
use crate::sql::executor::PhysicalPlanBuilder;
use crate::sql::plans::Insert;
use crate::sql::plans::InsertInputSource;
use crate::sql::plans::Plan;

pub struct InsertInterpreterV2 {
    ctx: Arc<QueryContext>,
    plan: Insert,
    source_pipe_builder: Mutex<Option<SourcePipeBuilder>>,
    async_insert: bool,
}

impl InsertInterpreterV2 {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        plan: Insert,
        async_insert: bool,
    ) -> Result<InterpreterPtr> {
        Ok(Arc::new(InsertInterpreterV2 {
            ctx,
            plan,
            source_pipe_builder: Mutex::new(None),
            async_insert,
        }))
    }

    async fn execute_new(&self) -> Result<SendableDataBlockStream> {
        let plan = &self.plan;
        let settings = self.ctx.get_settings();
        let table = self
            .ctx
            .get_table(&plan.catalog, &plan.database, &plan.table)
            .await?;

        let mut build_res = self.create_new_pipeline().await?;
        let mut builder = SourcePipeBuilder::create();

        if self.async_insert {
            build_res.main_pipeline.add_pipe(
                ((*self.source_pipe_builder.lock()).clone())
                    .ok_or_else(|| ErrorCode::EmptyData("empty source pipe builder"))?
                    .finalize(),
            );
        } else {
            match &self.plan.source {
                InsertInputSource::Values(values) => {
                    let blocks =
                        Arc::new(Mutex::new(VecDeque::from_iter(vec![values.block.clone()])));

                    for _index in 0..settings.get_max_threads()? {
                        let output = OutputPort::create();
                        builder.add_source(
                            output.clone(),
                            BlocksSource::create(self.ctx.clone(), output.clone(), blocks.clone())?,
                        );
                    }
                    build_res.main_pipeline.add_pipe(builder.finalize());
                }
                InsertInputSource::StreamingWithFormat(_) => {
                    build_res.main_pipeline.add_pipe(
                        ((*self.source_pipe_builder.lock()).clone())
                            .ok_or_else(|| ErrorCode::EmptyData("empty source pipe builder"))?
                            .finalize(),
                    );
                }
                InsertInputSource::SelectPlan(plan) => {
                    if !self.ctx.get_cluster().is_empty() {
                        // distributed insert select
                        return self
                            .schedule_insert_select(plan, self.plan.catalog.clone(), table.clone())
                            .await;
                    }

                    let select_interpreter = match &**plan {
                        Plan::Query {
                            s_expr,
                            metadata,
                            bind_context,
                            ..
                        } => SelectInterpreterV2::try_create(
                            self.ctx.clone(),
                            *bind_context.clone(),
                            *s_expr.clone(),
                            metadata.clone(),
                        ),
                        _ => unreachable!(),
                    };

                    build_res = select_interpreter?.create_new_pipeline().await?;

                    if self.check_schema_cast(plan)? {
                        let mut functions = Vec::with_capacity(self.plan.schema().fields().len());

                        for (target_field, original_field) in self
                            .plan
                            .schema()
                            .fields()
                            .iter()
                            .zip(plan.schema().fields().iter())
                        {
                            let target_type_name = target_field.data_type().name();
                            let from_type = original_field.data_type().clone();
                            let cast_function =
                                CastFunction::create("cast", &target_type_name, from_type)?;
                            functions.push(cast_function);
                        }

                        let func_ctx = self.ctx.try_get_function_context()?;
                        build_res.main_pipeline.add_transform(
                            |transform_input_port, transform_output_port| {
                                TransformCastSchema::try_create(
                                    transform_input_port,
                                    transform_output_port,
                                    self.plan.schema(),
                                    functions.clone(),
                                    func_ctx.clone(),
                                )
                            },
                        )?;
                    }
                }
            };
        }

        append2table(self.ctx.clone(), table.clone(), plan.schema(), build_res)?;
        commit2table(self.ctx.clone(), table.clone(), self.plan.overwrite).await?;

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }

    fn check_schema_cast(&self, plan: &Plan) -> common_exception::Result<bool> {
        let output_schema = &self.plan.schema;
        let select_schema = plan.schema();

        // validate schema
        if select_schema.fields().len() < output_schema.fields().len() {
            return Err(ErrorCode::BadArguments(
                "Fields in select statement is less than expected",
            ));
        }

        // check if cast needed
        let cast_needed = select_schema != *output_schema;
        Ok(cast_needed)
    }

    async fn schedule_insert_select(
        &self,
        plan: &Plan,
        catalog: String,
        table: Arc<dyn Table>,
    ) -> Result<SendableDataBlockStream> {
        let (inner_plan, select_column_bindings) = match plan {
            Plan::Query {
                s_expr,
                metadata,
                bind_context,
                ..
            } => {
                let builder = PhysicalPlanBuilder::new(metadata.clone(), self.ctx.clone());
                (builder.build(s_expr).await?, bind_context.columns.clone())
            }
            _ => unreachable!(),
        };

        table.get_table_info();

        let insert_select_plan =
            PhysicalPlan::DistributedInsertSelect(Box::new(DistributedInsertSelect {
                input: Box::new(inner_plan),
                catalog,
                table_info: table.get_table_info().clone(),
                select_schema: plan.schema(),
                select_column_bindings,
                insert_schema: self.plan.schema(),
                cast_needed: self.check_schema_cast(plan)?,
            }));

        let final_plan = PhysicalPlan::Exchange(Exchange {
            input: Box::new(insert_select_plan),
            kind: StageKind::Merge,
            keys: vec![],
        });

        let mut build_res = build_schedule_pipepline(self.ctx.clone(), &final_plan).await?;

        let settings = self.ctx.get_settings();
        let query_need_abort = self.ctx.query_need_abort();
        let executor_settings = ExecutorSettings::try_create(&settings)?;
        build_res.set_max_threads(settings.get_max_threads()? as usize);

        let executor = PipelinePullingExecutor::from_pipelines(
            query_need_abort,
            build_res,
            executor_settings,
        )?;

        let mut append_entries = vec![];
        let mut stream: SendableDataBlockStream =
            Box::pin(ProcessorExecutorStream::create(executor)?);
        while let Some(block) = stream.next().await {
            append_entries.push(block?);
        }

        commit2table_with_append_entries(
            self.ctx.clone(),
            table,
            self.plan.overwrite,
            append_entries,
        )
        .await?;

        Ok(Box::pin(DataBlockStream::create(
            self.plan.schema(),
            None,
            vec![],
        )))
    }
}

#[async_trait::async_trait]
impl Interpreter for InsertInterpreterV2 {
    fn name(&self) -> &str {
        "InsertIntoInterpreter"
    }

    async fn execute(&self) -> Result<SendableDataBlockStream> {
        self.execute_new().await
    }

    async fn create_new_pipeline(&self) -> Result<PipelineBuildResult> {
        let insert_pipeline = Pipeline::create();
        Ok(PipelineBuildResult {
            main_pipeline: insert_pipeline,
            sources_pipelines: vec![],
        })
    }

    fn set_source_pipe_builder(&self, builder: Option<SourcePipeBuilder>) -> Result<()> {
        let mut guard = self.source_pipe_builder.lock();
        *guard = builder;
        Ok(())
    }
}
