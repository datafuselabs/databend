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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FromData;
use databend_common_expression::RemoteExpr;
use databend_common_expression::SendableDataBlockStream;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::executor::physical_plans::CastSchema;
use databend_common_sql::executor::physical_plans::ChunkAppendData;
use databend_common_sql::executor::physical_plans::ChunkCastSchema;
use databend_common_sql::executor::physical_plans::ChunkCommitInsert;
use databend_common_sql::executor::physical_plans::ChunkEvalScalar;
use databend_common_sql::executor::physical_plans::ChunkFillAndReorder;
use databend_common_sql::executor::physical_plans::ChunkMerge;
use databend_common_sql::executor::physical_plans::FillAndReorder;
use databend_common_sql::executor::physical_plans::MultiInsertEvalScalar;
use databend_common_sql::executor::physical_plans::SerializableTable;
use databend_common_sql::executor::physical_plans::ShuffleStrategy;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_sql::executor::PhysicalPlanBuilder;
use databend_common_sql::plans::Else;
use databend_common_sql::plans::FunctionCall;
use databend_common_sql::plans::InsertMultiTable;
use databend_common_sql::plans::Into;
use databend_common_sql::plans::Plan;
use databend_common_sql::MetadataRef;
use databend_common_sql::ScalarExpr;
use databend_common_storages_fuse::FuseTable;

use crate::interpreters::common::dml_build_update_stream_req;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterPtr;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::executor::cast_expr_to_non_null_boolean;
use crate::sql::executor::physical_plans::ChunkFilter;
use crate::sql::executor::physical_plans::Duplicate;
use crate::sql::executor::physical_plans::Shuffle;
use crate::storages::Table;
use crate::stream::DataBlockStream;
pub struct InsertMultiTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: InsertMultiTable,
}

impl InsertMultiTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: InsertMultiTable) -> Result<InterpreterPtr> {
        Ok(Arc::new(Self { ctx, plan }))
    }

    pub fn try_create_static(ctx: Arc<QueryContext>, plan: InsertMultiTable) -> Result<Self> {
        Ok(Self { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for InsertMultiTableInterpreter {
    fn name(&self) -> &str {
        "InsertMultiTableInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let physical_plan = self.build_physical_plan().await?;
        let build_res =
            build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan).await?;
        Ok(build_res)
    }

    fn inject_result(&self) -> Result<SendableDataBlockStream> {
        let mut columns = vec![];
        let status = self.ctx.get_multi_table_insert_status();
        let guard = status.lock();
        for (tid, _) in &self.plan.target_tables {
            let insert_rows = guard.insert_rows.get(tid).cloned().unwrap_or_default();
            columns.push(UInt64Type::from_data(vec![insert_rows]));
        }
        let blocks = vec![DataBlock::new_from_columns(columns)];
        Ok(Box::pin(DataBlockStream::create(None, blocks)))
    }
}

impl InsertMultiTableInterpreter {
    pub async fn build_physical_plan(&self) -> Result<PhysicalPlan> {
        let (mut root, metadata) = self.build_source_physical_plan().await?;
        let update_stream_meta = dml_build_update_stream_req(self.ctx.clone(), &metadata).await?;
        let source_schema = root.output_schema()?;
        let branches = self.build_insert_into_branches().await?;
        let serializable_tables = branches
            .build_serializable_target_tables(self.ctx.clone())
            .await?;
        let deduplicated_serializable_tables = branches
            .build_deduplicated_serializable_target_tables(self.ctx.clone())
            .await?;
        let predicates = branches.build_predicates(source_schema.as_ref())?;
        let eval_scalars = branches.build_eval_scalars(source_schema.as_ref())?;

        // Source schemas for each branch to be casted to the target schema
        // It may be output of eval scalar(if exists) or just the source subquery's schema
        let source_schemas = eval_scalars
            .iter()
            .map(|opt_exprs| {
                opt_exprs
                    .as_ref()
                    .map(
                        |MultiInsertEvalScalar {
                             remote_exprs,
                             projection: _,
                         }| {
                            let mut evaled_fields = Vec::with_capacity(remote_exprs.len());
                            for eval_scalar_expr in remote_exprs {
                                let data_type = eval_scalar_expr
                                    .as_expr(&BUILTIN_FUNCTIONS)
                                    .data_type()
                                    .clone();
                                evaled_fields.push(DataField::new("", data_type));
                            }
                            Arc::new(DataSchema::new(evaled_fields))
                        },
                    )
                    .unwrap_or_else(|| source_schema.clone())
            })
            .collect::<Vec<_>>();
        let cast_schemas = branches.build_cast_schema(source_schemas);
        let fill_and_reorders = branches.build_fill_and_reorder(self.ctx.clone()).await?;
        let group_ids = branches.build_group_ids();

        root = PhysicalPlan::Duplicate(Box::new(Duplicate {
            plan_id: 0,
            input: Box::new(root),
            n: branches.len(),
        }));

        let shuffle_strategy = ShuffleStrategy::Transpose(branches.len());
        root = PhysicalPlan::Shuffle(Box::new(Shuffle {
            plan_id: 0,
            input: Box::new(root),
            strategy: shuffle_strategy,
        }));

        root = PhysicalPlan::ChunkFilter(Box::new(ChunkFilter {
            plan_id: 0,
            input: Box::new(root),
            predicates,
        }));

        root = PhysicalPlan::ChunkEvalScalar(Box::new(ChunkEvalScalar {
            plan_id: 0,
            input: Box::new(root),
            eval_scalars,
        }));

        root = PhysicalPlan::ChunkCastSchema(Box::new(ChunkCastSchema {
            plan_id: 0,
            input: Box::new(root),
            cast_schemas,
        }));

        root = PhysicalPlan::ChunkFillAndReorder(Box::new(ChunkFillAndReorder {
            plan_id: 0,
            input: Box::new(root),
            fill_and_reorders,
        }));

        root = PhysicalPlan::ChunkAppendData(Box::new(ChunkAppendData {
            plan_id: 0,
            input: Box::new(root),
            target_tables: serializable_tables.clone(),
        }));

        root = PhysicalPlan::ChunkMerge(Box::new(ChunkMerge {
            plan_id: 0,
            input: Box::new(root),
            group_ids,
        }));

        root = PhysicalPlan::ChunkCommitInsert(Box::new(ChunkCommitInsert {
            plan_id: 0,
            input: Box::new(root),
            update_stream_meta,
            overwrite: self.plan.overwrite,
            deduplicated_label: None,
            targets: deduplicated_serializable_tables,
        }));
        let mut next_plan_id = 0;
        root.adjust_plan_id(&mut next_plan_id);
        Ok(root)
    }

    async fn build_source_physical_plan(&self) -> Result<(PhysicalPlan, MetadataRef)> {
        match &self.plan.input_source {
            Plan::Query {
                s_expr,
                metadata,
                bind_context,
                ..
            } => {
                let mut builder1 =
                    PhysicalPlanBuilder::new(metadata.clone(), self.ctx.clone(), false);
                let input_source = builder1.build(s_expr, bind_context.column_set()).await?;
                Ok((input_source, metadata.clone()))
            }
            _ => unreachable!(),
        }
    }

    async fn build_insert_into_branches(&self) -> Result<InsertIntoBranches> {
        let InsertMultiTable {
            input_source: _,
            whens,
            opt_else,
            overwrite: _,
            is_first,
            intos,
            target_tables: _,
            meta_data: _,
        } = &self.plan;
        let mut branches = InsertIntoBranches::default();
        let mut condition_intos = vec![];
        let mut previous_not: Option<ScalarExpr> = None;
        for when in whens {
            let mut condition = when.condition.clone();
            if *is_first {
                if let Some(prev_not) = &previous_not {
                    let merged_condition = and(prev_not.clone(), condition.clone());
                    previous_not = Some(and(prev_not.clone(), not(condition.clone())));
                    condition = merged_condition;
                } else {
                    previous_not = Some(not(condition.clone()));
                };
            }
            for into in &when.intos {
                condition_intos.push((Some(condition.clone()), into));
            }
        }
        if let Some(Else { intos }) = opt_else {
            let condition = if *is_first { previous_not.take() } else { None };
            for into in intos {
                condition_intos.push((condition.clone(), into));
            }
        }
        for into in intos {
            condition_intos.push((None, into));
        }

        condition_intos.sort_by(|a, b| {
            a.1.catalog
                .cmp(&b.1.catalog)
                .then(a.1.database.cmp(&b.1.database))
                .then(a.1.table.cmp(&b.1.table))
        });

        for (condition, into) in condition_intos {
            let Into {
                catalog,
                database,
                table,
                casted_schema,
                source_scalar_exprs,
            } = into;
            let table = self.ctx.get_table(catalog, database, table).await?;
            branches.push(
                table,
                condition,
                source_scalar_exprs.clone(),
                casted_schema.clone(),
            );
        }

        Ok(branches)
    }
}

fn and(left: ScalarExpr, right: ScalarExpr) -> ScalarExpr {
    ScalarExpr::FunctionCall(FunctionCall {
        span: None,
        func_name: "and".to_string(),
        params: vec![],
        arguments: vec![left, right],
    })
}

fn not(expr: ScalarExpr) -> ScalarExpr {
    ScalarExpr::FunctionCall(FunctionCall {
        span: None,
        func_name: "not".to_string(),
        params: vec![],
        arguments: vec![expr],
    })
}

fn scalar_expr_to_remote_expr(expr: &ScalarExpr, block_schema: &DataSchema) -> Result<RemoteExpr> {
    let expr = expr
        .as_expr()?
        .project_column_ref(|col| block_schema.index_of(&col.index.to_string()).unwrap());
    Ok(expr.as_remote_expr())
}

#[derive(Default)]
struct InsertIntoBranches {
    tables: Vec<Arc<dyn Table>>,
    conditions: Vec<Option<ScalarExpr>>,
    source_exprs: Vec<Option<Vec<ScalarExpr>>>,
    casted_schemas: Vec<DataSchemaRef>,
    len: usize,
}

impl InsertIntoBranches {
    fn push(
        &mut self,
        table: Arc<dyn Table>,
        condition: Option<ScalarExpr>,
        source_exprs: Option<Vec<ScalarExpr>>,
        casted_schema: DataSchemaRef,
    ) {
        self.tables.push(table);
        self.conditions.push(condition);
        self.source_exprs.push(source_exprs);
        self.casted_schemas.push(casted_schema);
        self.len += 1;
    }

    fn len(&self) -> usize {
        self.len
    }

    async fn build_serializable_target_tables(
        &self,
        ctx: Arc<QueryContext>,
    ) -> Result<Vec<SerializableTable>> {
        let mut serializable_tables = vec![];
        for table in &self.tables {
            let table_info = table.get_table_info();
            let catalog_info = ctx.get_catalog(table_info.catalog()).await?.info();
            let snapshot = FuseTable::try_from_table(table.as_ref())?
                .read_table_snapshot()
                .await?;
            let table_meta_timestamps = ctx.get_table_meta_timestamps(table.get_id(), snapshot)?;
            serializable_tables.push(SerializableTable {
                target_catalog_info: catalog_info,
                target_table_info: table_info.clone(),
                table_meta_timestamps,
            });
        }
        Ok(serializable_tables)
    }

    async fn build_deduplicated_serializable_target_tables(
        &self,
        ctx: Arc<QueryContext>,
    ) -> Result<Vec<SerializableTable>> {
        let mut serializable_tables = vec![];
        let mut last_table_id = None;
        for table in &self.tables {
            let table_info = table.get_table_info();
            let table_id = table_info.ident.table_id;
            if last_table_id == Some(table_id) {
                continue;
            }
            last_table_id = Some(table_id);
            let catalog_info = ctx.get_catalog(table_info.catalog()).await?.info();
            let snapshot = FuseTable::try_from_table(table.as_ref())?
                .read_table_snapshot()
                .await?;
            let table_meta_timestamps = ctx.get_table_meta_timestamps(table.get_id(), snapshot)?;
            serializable_tables.push(SerializableTable {
                target_catalog_info: catalog_info,
                target_table_info: table_info.clone(),
                table_meta_timestamps,
            });
        }
        Ok(serializable_tables)
    }

    fn build_predicates(&self, source_schema: &DataSchema) -> Result<Vec<Option<RemoteExpr>>> {
        let mut predicates = vec![];
        let prepare_filter = |expr: ScalarExpr| {
            let expr = cast_expr_to_non_null_boolean(expr.as_expr()?.project_column_ref(|col| {
                source_schema.index_of(&col.index.to_string()).unwrap()
            }))?;
            Ok::<RemoteExpr, ErrorCode>(expr.as_remote_expr())
        };
        for opt_scalar_expr in self.conditions.iter() {
            if let Some(scalar_expr) = opt_scalar_expr {
                predicates.push(Some(prepare_filter(scalar_expr.clone())?));
            } else {
                predicates.push(None);
            }
        }
        Ok(predicates)
    }

    fn build_eval_scalars(
        &self,
        source_schema: &DataSchema,
    ) -> Result<Vec<Option<MultiInsertEvalScalar>>> {
        let mut eval_scalars = vec![];
        for opt_scalar_exprs in self.source_exprs.iter() {
            if let Some(scalar_exprs) = opt_scalar_exprs {
                let exprs = scalar_exprs
                    .iter()
                    .map(|scalar_expr| scalar_expr_to_remote_expr(scalar_expr, source_schema))
                    .collect::<Result<Vec<_>>>()?;
                let source_field_num = source_schema.fields().len();
                let evaled_num = scalar_exprs.len();
                let projection = (source_field_num..source_field_num + evaled_num).collect();
                eval_scalars.push(Some(MultiInsertEvalScalar {
                    remote_exprs: exprs,
                    projection,
                }));
            } else {
                eval_scalars.push(None);
            }
        }
        Ok(eval_scalars)
    }

    fn build_cast_schema(&self, source_schemas: Vec<DataSchemaRef>) -> Vec<Option<CastSchema>> {
        self.casted_schemas
            .iter()
            .zip(source_schemas.iter())
            .map(|(casted_schema, source_schema)| {
                if casted_schema != source_schema {
                    Some(CastSchema {
                        source_schema: source_schema.clone(),
                        target_schema: casted_schema.clone(),
                    })
                } else {
                    None
                }
            })
            .collect()
    }

    async fn build_fill_and_reorder(
        &self,
        _ctx: Arc<dyn TableContext>,
    ) -> Result<Vec<Option<FillAndReorder>>> {
        let mut fill_and_reorders = vec![];
        for (table, casted_schema) in self.tables.iter().zip(self.casted_schemas.iter()) {
            let target_schema: DataSchemaRef = Arc::new(table.schema().into());
            if target_schema.as_ref() != casted_schema.as_ref() {
                let table_info = table.get_table_info();
                fill_and_reorders.push(Some(FillAndReorder {
                    source_schema: casted_schema.clone(),
                    target_table_info: table_info.clone(),
                }));
            } else {
                fill_and_reorders.push(None);
            }
        }
        Ok(fill_and_reorders)
    }

    fn build_group_ids(&self) -> Vec<u64> {
        self.tables.iter().map(|table| table.get_id()).collect()
    }
}
