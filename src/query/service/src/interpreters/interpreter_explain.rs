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
use std::sync::Arc;

use databend_common_ast::ast::ExplainKind;
use databend_common_ast::ast::FormatTreeNode;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::StringType;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_pipeline_core::processors::PlanProfile;
use databend_common_sql::binder::ExplainConfig;
use databend_common_sql::binder::MergeIntoType;
use databend_common_sql::optimizer::ColumnSet;
use databend_common_sql::plans::UpdatePlan;
use databend_common_sql::BindContext;
use databend_common_sql::InsertInputSource;
use databend_common_sql::MetadataRef;
use databend_common_storages_factory::Table;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_result_cache::gen_result_cache_key;
use databend_common_storages_result_cache::ResultCacheReader;
use databend_common_users::UserApiProvider;
use databend_storages_common_table_meta::meta::TableSnapshot;

use super::InterpreterFactory;
use super::UpdateInterpreter;
use crate::interpreters::Interpreter;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::pipelines::executor::PipelinePullingExecutor;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline;
use crate::schedulers::Fragmenter;
use crate::schedulers::QueryFragmentsActions;
use crate::sessions::QueryContext;
use crate::sql::executor::PhysicalPlan;
use crate::sql::executor::PhysicalPlanBuilder;
use crate::sql::optimizer::SExpr;
use crate::sql::plans::Plan;

pub struct ExplainInterpreter {
    ctx: Arc<QueryContext>,
    config: ExplainConfig,
    kind: ExplainKind,
    plan: Plan,
}

#[async_trait::async_trait]
impl Interpreter for ExplainInterpreter {
    fn name(&self) -> &str {
        "ExplainInterpreterV2"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let blocks = match &self.kind {
            ExplainKind::Raw | ExplainKind::Optimized => self.explain_plan(&self.plan)?,
            ExplainKind::Plan if self.config.logical => self.explain_plan(&self.plan)?,
            ExplainKind::Plan => match &self.plan {
                Plan::Query {
                    s_expr,
                    metadata,
                    bind_context,
                    formatted_ast,
                    ..
                } => {
                    self.explain_query(s_expr, metadata, bind_context, formatted_ast)
                        .await?
                }
                Plan::Insert(insert_plan) => {
                    let mut res = self.explain_plan(&self.plan)?;
                    if let InsertInputSource::SelectPlan(plan) = &insert_plan.source {
                        if let Plan::Query {
                            s_expr,
                            metadata,
                            bind_context,
                            formatted_ast,
                            ..
                        } = &**plan
                        {
                            let query = self
                                .explain_query(s_expr, metadata, bind_context, formatted_ast)
                                .await?;
                            res.extend(query);
                        }
                    }
                    vec![DataBlock::concat(&res)?]
                }
                Plan::CreateTable(plan) => match &plan.as_select {
                    Some(box Plan::Query {
                        s_expr,
                        metadata,
                        bind_context,
                        formatted_ast,
                        ..
                    }) => {
                        let mut res =
                            vec![DataBlock::new_from_columns(vec![StringType::from_data(
                                vec!["CreateTableAsSelect:", ""],
                            )])];
                        res.extend(
                            self.explain_query(s_expr, metadata, bind_context, formatted_ast)
                                .await?,
                        );
                        vec![DataBlock::concat(&res)?]
                    }
                    _ => self.explain_plan(&self.plan)?,
                },
                _ => self.explain_plan(&self.plan)?,
            },

            ExplainKind::Join => match &self.plan {
                Plan::Query {
                    s_expr,
                    metadata,
                    bind_context,
                    ..
                } => {
                    let ctx = self.ctx.clone();
                    let mut builder = PhysicalPlanBuilder::new(metadata.clone(), ctx, true);
                    let plan = builder.build(s_expr, bind_context.column_set()).await?;
                    self.explain_join_order(&plan, metadata)?
                }
                _ => Err(ErrorCode::Unimplemented(
                    "Unsupported EXPLAIN JOIN statement",
                ))?,
            },

            ExplainKind::AnalyzePlan => match &self.plan {
                Plan::Query {
                    s_expr,
                    metadata,
                    bind_context,
                    ignore_result,
                    ..
                } => {
                    self.explain_analyze(
                        s_expr,
                        metadata,
                        bind_context.column_set(),
                        *ignore_result,
                    )
                    .await?
                }
                Plan::MergeInto(plan) => {
                    // if we enable merge_into_source_build_bloom, we should set the segments here
                    if !plan.change_join_order
                        && matches!(plan.merge_type, MergeIntoType::FullOperation)
                        && self
                            .ctx
                            .get_settings()
                            .get_enable_merge_into_source_build_bloom()?
                    {
                        let merge_into_join = self.ctx.get_merge_into_join();
                        let table = merge_into_join.table.as_ref().unwrap();
                        let fuse_table =
                            table.as_any().downcast_ref::<FuseTable>().ok_or_else(|| {
                                ErrorCode::Unimplemented(format!(
                                    "table {}, engine type {}, does not support MERGE INTO",
                                    table.name(),
                                    table.get_table_info().engine(),
                                ))
                            })?;
                        let base_snapshot =
                            fuse_table.read_table_snapshot().await?.unwrap_or_else(|| {
                                Arc::new(TableSnapshot::new_empty_snapshot(
                                    fuse_table.schema().as_ref().clone(),
                                ))
                            });
                        self.ctx.set_merge_into_source_build_segments(Arc::new(
                            base_snapshot.segments.clone(),
                        ));
                    }

                    self.explain_analyze(
                        &plan.input,
                        &plan.meta_data,
                        *plan.columns_set.clone(),
                        true,
                    )
                    .await?
                }
                _ => Err(ErrorCode::Unimplemented(
                    "Unsupported EXPLAIN ANALYZE statement",
                ))?,
            },

            ExplainKind::Pipeline => {
                // todo:(JackTan25), we need to make all execute2() just do `build pipeline` work,
                // don't take real actions. for now we fix #13657 like below.
                let pipeline = match &self.plan {
                    Plan::Query { .. } => {
                        let interpter =
                            InterpreterFactory::get(self.ctx.clone(), &self.plan).await?;
                        interpter.execute2().await?
                    }
                    _ => PipelineBuildResult::create(),
                };

                Self::format_pipeline(&pipeline)
            }

            ExplainKind::Fragments => match &self.plan {
                Plan::Query {
                    s_expr,
                    metadata,
                    bind_context,
                    ..
                } => {
                    self.explain_fragments(
                        *s_expr.clone(),
                        metadata.clone(),
                        bind_context.column_set(),
                    )
                    .await?
                }
                Plan::Update(update) => self.explain_update_fragments(update.as_ref()).await?,
                _ => {
                    return Err(ErrorCode::Unimplemented("Unsupported EXPLAIN statement"));
                }
            },

            ExplainKind::Graph => {
                return Err(ErrorCode::Unimplemented(
                    "ExplainKind graph is unimplemented",
                ));
            }

            ExplainKind::Ast(display_string)
            | ExplainKind::Syntax(display_string)
            | ExplainKind::Memo(display_string) => {
                let line_split_result: Vec<&str> = display_string.lines().collect();
                let column = StringType::from_data(line_split_result);
                vec![DataBlock::new_from_columns(vec![column])]
            }
        };

        PipelineBuildResult::from_blocks(blocks)
    }
}

impl ExplainInterpreter {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        plan: Plan,
        kind: ExplainKind,
        config: ExplainConfig,
    ) -> Result<Self> {
        Ok(ExplainInterpreter {
            ctx,
            plan,
            kind,
            config,
        })
    }

    pub fn explain_plan(&self, plan: &Plan) -> Result<Vec<DataBlock>> {
        let result = plan.format_indent(self.config.verbose)?;
        let line_split_result: Vec<&str> = result.lines().collect();
        let formatted_plan = StringType::from_data(line_split_result);
        Ok(vec![DataBlock::new_from_columns(vec![formatted_plan])])
    }

    pub async fn explain_physical_plan(
        &self,
        plan: &PhysicalPlan,
        metadata: &MetadataRef,
        formatted_ast: &Option<String>,
    ) -> Result<Vec<DataBlock>> {
        if self.ctx.get_settings().get_enable_query_result_cache()? && self.ctx.get_cacheable() {
            let key = gen_result_cache_key(formatted_ast.as_ref().unwrap());
            let kv_store = UserApiProvider::instance().get_meta_store_client();
            let cache_reader = ResultCacheReader::create(
                self.ctx.clone(),
                &key,
                kv_store.clone(),
                self.ctx
                    .get_settings()
                    .get_query_result_cache_allow_inconsistent()?,
            );
            if let Some(v) = cache_reader.check_cache().await? {
                // Construct a format tree for result cache reading
                let children = vec![
                    FormatTreeNode::new(format!("SQL: {}", v.sql)),
                    FormatTreeNode::new(format!("Number of rows: {}", v.num_rows)),
                    FormatTreeNode::new(format!("Result size: {}", v.result_size)),
                ];

                let format_tree =
                    FormatTreeNode::with_children("ReadQueryResultCache".to_string(), children);

                let result = format_tree.format_pretty()?;
                let line_split_result: Vec<&str> = result.lines().collect();
                let formatted_plan = StringType::from_data(line_split_result);
                return Ok(vec![DataBlock::new_from_columns(vec![formatted_plan])]);
            }
        }

        let result = plan
            .format(metadata.clone(), Default::default())?
            .format_pretty()?;
        let line_split_result: Vec<&str> = result.lines().collect();
        let formatted_plan = StringType::from_data(line_split_result);
        Ok(vec![DataBlock::new_from_columns(vec![formatted_plan])])
    }

    pub fn explain_join_order(
        &self,
        plan: &PhysicalPlan,
        metadata: &MetadataRef,
    ) -> Result<Vec<DataBlock>> {
        let result = plan.format_join(metadata)?.format_pretty()?;
        let line_split_result: Vec<&str> = result.lines().collect();
        let formatted_plan = StringType::from_data(line_split_result);
        Ok(vec![DataBlock::new_from_columns(vec![formatted_plan])])
    }

    fn format_pipeline(build_res: &PipelineBuildResult) -> Vec<DataBlock> {
        let mut blocks = Vec::with_capacity(1 + build_res.sources_pipelines.len());
        // Format root pipeline
        let line_split_result = format!("{}", build_res.main_pipeline.display_indent())
            .lines()
            .map(|l| l.to_string())
            .collect::<Vec<_>>();
        let column = StringType::from_data(line_split_result);
        blocks.push(DataBlock::new_from_columns(vec![column]));
        // Format child pipelines
        for pipeline in build_res.sources_pipelines.iter() {
            let line_split_result = format!("\n{}", pipeline.display_indent())
                .lines()
                .map(|l| l.to_string())
                .collect::<Vec<_>>();
            let column = StringType::from_data(line_split_result);
            blocks.push(DataBlock::new_from_columns(vec![column]));
        }
        blocks
    }

    #[async_backtrace::framed]
    async fn explain_fragments(
        &self,
        s_expr: SExpr,
        metadata: MetadataRef,
        required: ColumnSet,
    ) -> Result<Vec<DataBlock>> {
        let ctx = self.ctx.clone();
        let plan = PhysicalPlanBuilder::new(metadata.clone(), self.ctx.clone(), true)
            .build(&s_expr, required)
            .await?;

        let root_fragment = Fragmenter::try_create(ctx.clone())?.build_fragment(&plan)?;

        let mut fragments_actions = QueryFragmentsActions::create(ctx.clone());
        root_fragment.get_actions(ctx, &mut fragments_actions)?;

        let display_string = fragments_actions.display_indent(&metadata).to_string();
        let line_split_result = display_string.lines().collect::<Vec<_>>();
        let formatted_plan = StringType::from_data(line_split_result);
        Ok(vec![DataBlock::new_from_columns(vec![formatted_plan])])
    }

    #[async_backtrace::framed]
    async fn explain_update_fragments(&self, update: &UpdatePlan) -> Result<Vec<DataBlock>> {
        let interpreter = UpdateInterpreter::try_create(self.ctx.clone(), update.clone())?;
        let display_string = if let Some(plan) = interpreter.get_physical_plan().await? {
            let root_fragment = Fragmenter::try_create(self.ctx.clone())?.build_fragment(&plan)?;

            let mut fragments_actions = QueryFragmentsActions::create(self.ctx.clone());
            root_fragment.get_actions(self.ctx.clone(), &mut fragments_actions)?;

            let ident = fragments_actions.display_indent(&update.metadata);
            ident.to_string()
        } else {
            "Nothing to update".to_string()
        };
        let line_split_result = display_string.lines().collect::<Vec<_>>();
        let formatted_plan = StringType::from_data(line_split_result);
        Ok(vec![DataBlock::new_from_columns(vec![formatted_plan])])
    }

    #[async_backtrace::framed]
    async fn explain_analyze(
        &self,
        s_expr: &SExpr,
        metadata: &MetadataRef,
        required: ColumnSet,
        ignore_result: bool,
    ) -> Result<Vec<DataBlock>> {
        let mut builder = PhysicalPlanBuilder::new(metadata.clone(), self.ctx.clone(), true);
        let plan = builder.build(s_expr, required).await?;
        let build_res = build_query_pipeline(&self.ctx, &[], &plan, ignore_result).await?;

        // Drain the data
        let query_profiles = self.execute_and_get_profiles(build_res)?;

        let result = plan
            .format(metadata.clone(), query_profiles)?
            .format_pretty()?;
        let line_split_result: Vec<&str> = result.lines().collect();
        let formatted_plan = StringType::from_data(line_split_result);
        Ok(vec![DataBlock::new_from_columns(vec![formatted_plan])])
    }

    fn execute_and_get_profiles(
        &self,
        mut build_res: PipelineBuildResult,
    ) -> Result<HashMap<u32, PlanProfile>> {
        let settings = self.ctx.get_settings();
        let query_id = self.ctx.get_id();
        build_res.set_max_threads(settings.get_max_threads()? as usize);
        let settings = ExecutorSettings::try_create(&settings, query_id.clone())?;

        match build_res.main_pipeline.is_complete_pipeline()? {
            true => {
                let mut pipelines = build_res.sources_pipelines;
                pipelines.push(build_res.main_pipeline);

                let executor = PipelineCompleteExecutor::from_pipelines(pipelines, settings)?;
                executor.execute()?;
                self.ctx.add_query_profiles(
                    &executor
                        .get_inner()
                        .get_profiles()
                        .iter()
                        .filter(|x| x.plan_id.is_some())
                        .map(|x| PlanProfile::create(x))
                        .collect::<Vec<_>>(),
                );
            }
            false => {
                let mut executor = PipelinePullingExecutor::from_pipelines(build_res, settings)?;
                executor.start();
                while (executor.pull_data()?).is_some() {}
                self.ctx.add_query_profiles(
                    &executor
                        .get_inner()
                        .get_profiles()
                        .iter()
                        .filter(|x| x.plan_id.is_some())
                        .map(|x| PlanProfile::create(x))
                        .collect::<Vec<_>>(),
                );
            }
        }

        Ok(self
            .ctx
            .get_query_profiles()
            .into_iter()
            .filter(|x| x.id.is_some())
            .map(|x| (x.id.unwrap(), x))
            .collect::<HashMap<_, _>>())
    }

    async fn explain_query(
        &self,
        s_expr: &SExpr,
        metadata: &MetadataRef,
        bind_context: &BindContext,
        formatted_ast: &Option<String>,
    ) -> Result<Vec<DataBlock>> {
        let ctx = self.ctx.clone();
        // If `formatted_ast` is Some, it means we may use query result cache.
        // If we use result cache for this query,
        // we should not use `dry_run` mode to build the physical plan.
        // It's because we need to get the same partitions as the original selecting plan.
        let mut builder = PhysicalPlanBuilder::new(metadata.clone(), ctx, formatted_ast.is_none());
        let plan = builder.build(s_expr, bind_context.column_set()).await?;
        self.explain_physical_plan(&plan, metadata, formatted_ast)
            .await
    }
}
