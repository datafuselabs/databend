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
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_ast::ast::ExplainKind;
use databend_common_ast::ast::FormatTreeNode;
use databend_common_base::runtime::profile::get_statistics_desc;
use databend_common_base::runtime::profile::ProfileDesc;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::StringType;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FromData;
use databend_common_pipeline_core::always_callback;
use databend_common_pipeline_core::processors::PlanProfile;
use databend_common_pipeline_core::ExecutionInfo;
use databend_common_sql::binder::ExplainConfig;
use databend_common_sql::executor::format_partial_tree;
use databend_common_sql::optimizer::ColumnSet;
use databend_common_sql::plans::Mutation;
use databend_common_sql::BindContext;
use databend_common_sql::MetadataRef;
use databend_common_storages_result_cache::gen_result_cache_key;
use databend_common_storages_result_cache::ResultCacheReader;
use databend_common_users::UserApiProvider;
use serde::Serialize;
use serde_json;

use super::InsertMultiTableInterpreter;
use super::InterpreterFactory;
use crate::interpreters::interpreter::on_execution_finished;
use crate::interpreters::interpreter_mutation::MutationInterpreter;
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
    partial: bool,
    graphical: bool,
    plan: Plan,
}

#[derive(Serialize)]
pub struct GraphicalProfiles {
    query_id: String,
    profiles: Vec<PlanProfile>,
    statistics_desc: Arc<BTreeMap<ProfileStatisticsName, ProfileDesc>>,
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
                Plan::Insert(insert_plan) => insert_plan.explain(self.config.verbose).await?,
                Plan::Replace(replace_plan) => replace_plan.explain(self.config.verbose).await?,
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
                Plan::InsertMultiTable(plan) => {
                    let physical_plan = InsertMultiTableInterpreter::try_create_static(
                        self.ctx.clone(),
                        *plan.clone(),
                    )?
                    .build_physical_plan()
                    .await?;
                    self.explain_physical_plan(&physical_plan, &plan.meta_data, &None)
                        .await?
                }
                Plan::DataMutation {
                    s_expr,
                    schema,
                    metadata,
                } => {
                    let mutation: Mutation = s_expr.plan().clone().try_into()?;
                    let interpreter = MutationInterpreter::try_create(
                        self.ctx.clone(),
                        *s_expr.clone(),
                        schema.clone(),
                    )?;
                    let plan = interpreter.build_physical_plan(&mutation, None).await?;
                    self.explain_physical_plan(&plan, metadata, &None).await?
                }
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

            ExplainKind::AnalyzePlan | ExplainKind::Graphical => match &self.plan {
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
                Plan::DataMutation { s_expr, .. } => {
                    let plan: Mutation = s_expr.plan().clone().try_into()?;
                    self.explain_analyze(
                        s_expr.child(0)?,
                        &plan.metadata,
                        *plan.required_columns.clone(),
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
                let mut pipeline = match &self.plan {
                    Plan::Query { .. } | Plan::DataMutation { .. } => {
                        let interpter =
                            InterpreterFactory::get(self.ctx.clone(), &self.plan).await?;
                        interpter.execute2().await?
                    }
                    _ => PipelineBuildResult::create(),
                };

                // The explain pipeline does not require executing on_init and on_finished.
                let _ = pipeline.main_pipeline.take_on_init();
                let _ = pipeline.main_pipeline.take_on_finished();

                for pipeline in &mut pipeline.sources_pipelines {
                    let _ = pipeline.take_on_init();
                    let _ = pipeline.take_on_finished();
                }

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
                Plan::DataMutation { s_expr, schema, .. } => {
                    self.explain_merge_fragments(*s_expr.clone(), schema.clone())
                        .await?
                }
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
        partial: bool,
        graphical: bool,
    ) -> Result<Self> {
        Ok(ExplainInterpreter {
            ctx,
            plan,
            kind,
            config,
            partial,
            graphical,
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
        if self.ctx.get_settings().get_enable_query_result_cache()?
            && self.ctx.get_cacheable()
            && formatted_ast.is_some()
        {
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

    fn graphical_profiles_to_datablocks(profiles: GraphicalProfiles) -> Vec<DataBlock> {
        let json_string = serde_json::to_string_pretty(&profiles)
            .unwrap_or_else(|_| "Failed to format profiles".to_string());

        let line_split_result: Vec<&str> = json_string.lines().collect();
        let formatted_block = StringType::from_data(line_split_result);

        vec![DataBlock::new_from_columns(vec![formatted_block])]
    }

    #[async_backtrace::framed]
    async fn explain_analyze_graphical(
        &self,
        s_expr: &SExpr,
        metadata: &MetadataRef,
        required: ColumnSet,
        ignore_result: bool,
    ) -> Result<GraphicalProfiles> {
        let query_ctx = self.ctx.clone();

        let mut builder = PhysicalPlanBuilder::new(metadata.clone(), self.ctx.clone(), true);
        let plan = builder.build(s_expr, required).await?;
        let build_res = build_query_pipeline(&self.ctx, &[], &plan, ignore_result).await?;

        // Drain the data
        let query_profiles = self.execute_and_get_profiles(build_res)?;

        Ok(GraphicalProfiles {
            query_id: query_ctx.get_id(),
            profiles: query_profiles.values().cloned().collect(),
            statistics_desc: get_statistics_desc(),
        })
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

        let result = if self.partial {
            format_partial_tree(&plan, metadata, &query_profiles)?.format_pretty()?
        } else {
            plan.format(metadata.clone(), query_profiles.clone())?
                .format_pretty()?
        };
        let line_split_result: Vec<&str> = result.lines().collect();
        let formatted_plan = StringType::from_data(line_split_result);

        if self.graphical {
            let profiles = GraphicalProfiles {
                query_id: self.ctx.clone().get_id(),
                profiles: query_profiles.clone().values().cloned().collect(),
                statistics_desc: get_statistics_desc(),
            };
            return Ok(Self::graphical_profiles_to_datablocks(profiles));
        }

        Ok(vec![DataBlock::new_from_columns(vec![formatted_plan])])
    }

    fn execute_and_get_profiles(
        &self,
        mut build_res: PipelineBuildResult,
    ) -> Result<HashMap<u32, PlanProfile>> {
        let settings = self.ctx.get_settings();
        build_res.set_max_threads(settings.get_max_threads()? as usize);
        let settings = ExecutorSettings::try_create(self.ctx.clone())?;
        let ctx = self.ctx.clone();
        build_res
            .main_pipeline
            .set_on_finished(always_callback(move |info: &ExecutionInfo| {
                on_execution_finished(info, ctx)
            }));
        match build_res.main_pipeline.is_complete_pipeline()? {
            true => {
                let mut pipelines = build_res.sources_pipelines;
                pipelines.push(build_res.main_pipeline);

                let executor = PipelineCompleteExecutor::from_pipelines(pipelines, settings)?;
                executor.execute()?;
                self.ctx
                    .add_query_profiles(&executor.get_inner().fetch_profiling(false));
            }
            false => {
                let mut executor = PipelinePullingExecutor::from_pipelines(build_res, settings)?;
                executor.start();
                while (executor.pull_data()?).is_some() {}
                self.ctx
                    .add_query_profiles(&executor.get_inner().fetch_profiling(false));
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

    async fn explain_merge_fragments(
        &self,
        s_expr: SExpr,
        schema: DataSchemaRef,
    ) -> Result<Vec<DataBlock>> {
        let mutation: Mutation = s_expr.plan().clone().try_into()?;
        let interpreter = MutationInterpreter::try_create(self.ctx.clone(), s_expr, schema)?;
        let plan = interpreter.build_physical_plan(&mutation, None).await?;
        let root_fragment = Fragmenter::try_create(self.ctx.clone())?.build_fragment(&plan)?;

        let mut fragments_actions = QueryFragmentsActions::create(self.ctx.clone());
        root_fragment.get_actions(self.ctx.clone(), &mut fragments_actions)?;

        let display_string = fragments_actions
            .display_indent(&mutation.metadata)
            .to_string();

        let line_split_result = display_string.lines().collect::<Vec<_>>();
        let formatted_plan = StringType::from_data(line_split_result);
        Ok(vec![DataBlock::new_from_columns(vec![formatted_plan])])
    }
}
