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

use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_catalog::plan::Filters;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::DataBlock;
use databend_common_expression::ROW_ID_COL_NAME;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::TableInfo;
use databend_common_sql::binder::ColumnBindingBuilder;
use databend_common_sql::executor::physical_plans::CommitSink;
use databend_common_sql::executor::physical_plans::DeleteSource;
use databend_common_sql::executor::physical_plans::Exchange;
use databend_common_sql::executor::physical_plans::FragmentKind;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_sql::optimizer::optimize_query;
use databend_common_sql::optimizer::OptimizerContext;
use databend_common_sql::optimizer::SExpr;
use databend_common_sql::plans::BoundColumnRef;
use databend_common_sql::plans::ConstantExpr;
use databend_common_sql::plans::EvalScalar;
use databend_common_sql::plans::FunctionCall;
use databend_common_sql::plans::RelOperator;
use databend_common_sql::plans::ScalarItem;
use databend_common_sql::plans::SubqueryDesc;
use databend_common_sql::BindContext;
use databend_common_sql::ColumnBinding;
use databend_common_sql::MetadataRef;
use databend_common_sql::ScalarExpr;
use databend_common_sql::Visibility;
use databend_common_storages_factory::Table;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_table_meta::meta::TableSnapshot;
use futures_util::TryStreamExt;
use log::debug;

use crate::interpreters::common::create_push_down_filters;
use crate::interpreters::Interpreter;
use crate::interpreters::SelectInterpreter;
use crate::locks::LockManager;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelinePullingExecutor;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::DeletePlan;
use crate::stream::PullingExecutorStream;

/// interprets DeletePlan
pub struct DeleteInterpreter {
    ctx: Arc<QueryContext>,
    plan: DeletePlan,
}

impl DeleteInterpreter {
    /// Create the DeleteInterpreter from DeletePlan
    pub fn try_create(ctx: Arc<QueryContext>, plan: DeletePlan) -> Result<Self> {
        Ok(DeleteInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DeleteInterpreter {
    /// Get the name of current interpreter
    fn name(&self) -> &str {
        "DeleteInterpreter"
    }

    #[minitrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "delete_interpreter_execute");

        let is_distributed = !self.ctx.get_cluster().is_empty();

        let catalog_name = self.plan.catalog_name.as_str();
        let catalog = self.ctx.get_catalog(catalog_name).await?;
        let catalog_info = catalog.info();

        let db_name = self.plan.database_name.as_str();
        let tbl_name = self.plan.table_name.as_str();
        let tbl = catalog
            .get_table(self.ctx.get_tenant().as_str(), db_name, tbl_name)
            .await?;

        // Add table lock.
        let table_lock = LockManager::create_table_lock(tbl.get_table_info().clone())?;
        let lock_guard = table_lock.try_lock(self.ctx.clone()).await?;

        // refresh table.
        let tbl = tbl.refresh(self.ctx.as_ref()).await?;

        // check mutability
        tbl.check_mutable()?;

        let selection = if !self.plan.subquery_desc.is_empty() {
            let support_row_id = tbl.support_row_id_column();
            if !support_row_id {
                return Err(ErrorCode::from_string(
                    "table doesn't support row_id, so it can't use delete with subquery"
                        .to_string(),
                ));
            }
            let table_index = self.plan.metadata.read().get_table_index(
                Some(self.plan.database_name.as_str()),
                self.plan.table_name.as_str(),
            );
            let row_id_column_binding = ColumnBindingBuilder::new(
                ROW_ID_COL_NAME.to_string(),
                self.plan.subquery_desc[0].index,
                Box::new(DataType::Number(NumberDataType::UInt64)),
                Visibility::InVisible,
            )
            .database_name(Some(self.plan.database_name.clone()))
            .table_name(Some(self.plan.table_name.clone()))
            .table_index(table_index)
            .build();
            let mut filters = VecDeque::new();
            for subquery_desc in &self.plan.subquery_desc {
                let filter = subquery_filter(
                    self.ctx.clone(),
                    self.plan.metadata.clone(),
                    &row_id_column_binding,
                    subquery_desc,
                )
                .await?;
                filters.push_front(filter);
            }
            // Traverse `selection` and put `filters` into `selection`.
            let mut selection = self.plan.selection.clone().unwrap();
            replace_subquery(&mut filters, &mut selection)?;
            Some(selection)
        } else {
            self.plan.selection.clone()
        };

        let (filters, col_indices) = if let Some(scalar) = selection {
            // prepare the filter expression
            let filters = create_push_down_filters(&scalar)?;

            let expr = filters.filter.as_expr(&BUILTIN_FUNCTIONS);
            if !expr.is_deterministic(&BUILTIN_FUNCTIONS) {
                return Err(ErrorCode::Unimplemented(
                    "Delete must have deterministic predicate",
                ));
            }

            let col_indices: Vec<usize> = if !self.plan.subquery_desc.is_empty() {
                let mut col_indices = HashSet::new();
                for subquery_desc in &self.plan.subquery_desc {
                    col_indices.extend(subquery_desc.outer_columns.iter());
                }
                col_indices.into_iter().collect()
            } else {
                scalar.used_columns().into_iter().collect()
            };
            (Some(filters), col_indices)
        } else {
            (None, vec![])
        };

        let fuse_table = tbl.as_any().downcast_ref::<FuseTable>().ok_or_else(|| {
            ErrorCode::Unimplemented(format!(
                "table {}, engine type {}, does not support DELETE FROM",
                tbl.name(),
                tbl.get_table_info().engine(),
            ))
        })?;

        let mut build_res = PipelineBuildResult::create();
        let query_row_id_col = !self.plan.subquery_desc.is_empty();
        if let Some(snapshot) = fuse_table
            .fast_delete(
                self.ctx.clone(),
                filters.clone(),
                col_indices.clone(),
                query_row_id_col,
            )
            .await?
        {
            let partitions = fuse_table
                .mutation_read_partitions(snapshot.clone())
                .await?;

            // Safe to unwrap, because if filters is None, fast_delete will do truncate and return None.
            let filters = filters.unwrap();
            let physical_plan = Self::build_physical_plan(
                filters,
                partitions,
                fuse_table.get_table_info().clone(),
                col_indices,
                snapshot,
                catalog_info,
                is_distributed,
                query_row_id_col,
            )?;

            build_res =
                build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan, false)
                    .await?;
        }

        build_res.main_pipeline.add_lock_guard(lock_guard);

        Ok(build_res)
    }
}

impl DeleteInterpreter {
    #[allow(clippy::too_many_arguments)]
    pub fn build_physical_plan(
        filters: Filters,
        partitions: Partitions,
        table_info: TableInfo,
        col_indices: Vec<usize>,
        snapshot: Arc<TableSnapshot>,
        catalog_info: CatalogInfo,
        is_distributed: bool,
        query_row_id_col: bool,
    ) -> Result<PhysicalPlan> {
        let merge_meta = partitions.is_lazy;
        let mut root = PhysicalPlan::DeleteSource(Box::new(DeleteSource {
            parts: partitions,
            filters,
            table_info: table_info.clone(),
            catalog_info: catalog_info.clone(),
            col_indices,
            query_row_id_col,
            snapshot: snapshot.clone(),
        }));

        if is_distributed {
            root = PhysicalPlan::Exchange(Exchange {
                plan_id: 0,
                input: Box::new(root),
                kind: FragmentKind::Merge,
                keys: vec![],
                allow_adjust_parallelism: true,
                ignore_exchange: false,
            });
        }

        Ok(PhysicalPlan::CommitSink(Box::new(CommitSink {
            input: Box::new(root),
            snapshot,
            table_info,
            catalog_info,
            mutation_kind: MutationKind::Delete,
            update_stream_meta: vec![],
            merge_meta,
            need_lock: false,
        })))
    }
}

pub async fn subquery_filter(
    ctx: Arc<QueryContext>,
    metadata: MetadataRef,
    row_id_column_binding: &ColumnBinding,
    subquery_desc: &SubqueryDesc,
) -> Result<ScalarExpr> {
    // Select `_row_id` column
    let input_expr = subquery_desc.input_expr.clone();

    let mut s_expr = SExpr::create_unary(
        Arc::new(RelOperator::EvalScalar(EvalScalar {
            items: vec![ScalarItem {
                scalar: ScalarExpr::BoundColumnRef(BoundColumnRef {
                    span: None,
                    column: row_id_column_binding.clone(),
                }),
                index: 0,
            }],
        })),
        Arc::new(input_expr),
    );
    // Optimize expression
    let mut bind_context = Box::new(BindContext::new());
    bind_context.add_column_binding(row_id_column_binding.clone());

    let opt_ctx = OptimizerContext::new(ctx.clone(), metadata.clone())
        .with_enable_distributed_optimization(false)
        .with_enable_join_reorder(unsafe { !ctx.get_settings().get_disable_join_reorder()? })
        .with_enable_dphyp(ctx.get_settings().get_enable_dphyp()?);

    s_expr = optimize_query(opt_ctx, s_expr.clone())?;

    // Create `input_expr` pipeline and execute it to get `_row_id` data block.
    let select_interpreter = SelectInterpreter::try_create(
        ctx.clone(),
        *bind_context,
        s_expr,
        metadata.clone(),
        None,
        false,
    )?;
    // Build physical plan
    let physical_plan = select_interpreter.build_physical_plan().await?;
    // Create pipeline for physical plan
    let pipeline = build_query_pipeline(
        &ctx,
        &[row_id_column_binding.clone()],
        &physical_plan,
        false,
    )
    .await?;

    // Execute pipeline
    let settings = ctx.get_settings();
    let query_id = ctx.get_id();
    let settings = ExecutorSettings::try_create(&settings, query_id)?;
    let pulling_executor = PipelinePullingExecutor::from_pipelines(pipeline, settings)?;
    ctx.set_executor(pulling_executor.get_inner())?;
    let stream_blocks = PullingExecutorStream::create(pulling_executor)?
        .try_collect::<Vec<DataBlock>>()
        .await?;
    let row_id_array = if !stream_blocks.is_empty() {
        let block = DataBlock::concat(&stream_blocks)?;
        let row_id_col = block.columns()[0]
            .value
            .convert_to_full_column(&DataType::Number(NumberDataType::UInt64), block.num_rows());
        // Make a selection: `_row_id` IN (row_id_col)
        // Construct array function for `row_id_col`
        let mut row_id_array = Vec::with_capacity(row_id_col.len());
        for row_id in row_id_col.iter() {
            let scalar = row_id.to_owned();
            let constant_scalar_expr = ScalarExpr::ConstantExpr(ConstantExpr {
                span: None,
                value: scalar,
            });
            row_id_array.push(constant_scalar_expr);
        }
        row_id_array
    } else {
        vec![]
    };

    let array_raw_expr = ScalarExpr::FunctionCall(FunctionCall {
        span: None,
        func_name: "array".to_string(),
        params: vec![],
        arguments: row_id_array,
    });

    let row_id_expr = ScalarExpr::BoundColumnRef(BoundColumnRef {
        span: None,
        column: row_id_column_binding.clone(),
    });

    Ok(ScalarExpr::FunctionCall(FunctionCall {
        span: None,
        func_name: "contains".to_string(),
        params: vec![],
        arguments: vec![array_raw_expr, row_id_expr],
    }))
}

pub fn replace_subquery(
    filters: &mut VecDeque<ScalarExpr>,
    selection: &mut ScalarExpr,
) -> Result<()> {
    match selection {
        ScalarExpr::FunctionCall(func) => {
            for arg in &mut func.arguments {
                replace_subquery(filters, arg)?;
            }
        }
        ScalarExpr::UDFServerCall(udf) => {
            for arg in &mut udf.arguments {
                replace_subquery(filters, arg)?;
            }
        }
        ScalarExpr::SubqueryExpr { .. } => {
            let filter = filters.pop_back().unwrap();
            *selection = filter;
        }
        _ => {}
    }
    Ok(())
}
