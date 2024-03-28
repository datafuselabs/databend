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
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_catalog::plan::Filters;
use databend_common_catalog::plan::PartInfoType;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::FieldIndex;
use databend_common_expression::RemoteExpr;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::PREDICATE_COLUMN_NAME;
use databend_common_expression::ROW_ID_COLUMN_ID;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_license::license::Feature::ComputedColumn;
use databend_common_license::license_manager::get_license_manager;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::TableInfo;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_transforms::processors::AsyncAccumulatingTransformer;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::executor::physical_plans::CommitSink;
use databend_common_sql::executor::physical_plans::Exchange;
use databend_common_sql::executor::physical_plans::FragmentKind;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::executor::physical_plans::UpdateSource;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_sql::executor::PhysicalPlanBuilder;
use databend_common_storages_factory::Table;
use databend_common_storages_fuse::operations::MutationGenerator;
use databend_common_storages_fuse::operations::SubqueryMutation;
use databend_common_storages_fuse::operations::TableMutationAggregator;
use databend_common_storages_fuse::operations::TransformMutationSubquery;
use databend_common_storages_fuse::operations::TransformSerializeBlock;
use databend_common_storages_fuse::operations::TransformSerializeSegment;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_table_meta::meta::TableSnapshot;
use log::debug;

use crate::interpreters::common::check_deduplicate_label;
use crate::interpreters::common::create_push_down_filters;
use crate::interpreters::HookOperator;
use crate::interpreters::Interpreter;
use crate::locks::LockManager;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::PipelineBuilder;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::UpdatePlan;

/// interprets UpdatePlan
pub struct UpdateInterpreter {
    ctx: Arc<QueryContext>,
    plan: UpdatePlan,
}

impl UpdateInterpreter {
    /// Create the UpdateInterpreter from UpdatePlan
    pub fn try_create(ctx: Arc<QueryContext>, plan: UpdatePlan) -> Result<Self> {
        Ok(UpdateInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for UpdateInterpreter {
    /// Get the name of current interpreter
    fn name(&self) -> &str {
        "UpdateInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[minitrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "update_interpreter_execute");

        if check_deduplicate_label(self.ctx.clone()).await? {
            return Ok(PipelineBuildResult::create());
        }

        let catalog_name = self.plan.catalog.as_str();
        let catalog = self.ctx.get_catalog(catalog_name).await?;

        let db_name = self.plan.database.as_str();
        let tbl_name = self.plan.table.as_str();
        let tbl = catalog
            .get_table(self.ctx.get_tenant().name(), db_name, tbl_name)
            .await?;

        // Add table lock.
        let table_lock = LockManager::create_table_lock(tbl.get_table_info().clone())?;
        let lock_guard = table_lock.try_lock(self.ctx.clone()).await?;

        if !self.plan.subquery_desc.is_empty() {
            let support_row_id = tbl.supported_internal_column(ROW_ID_COLUMN_ID);
            if !support_row_id {
                return Err(ErrorCode::from_string(format!(
                    "Update with subquery is not supported for the table '{}', which lacks row_id support.",
                    tbl.name(),
                )));
            }

            let mut build_res = self.update_by_subquery(tbl.clone()).await?;
            build_res.main_pipeline.add_lock_guard(lock_guard);
            return Ok(build_res);
        }

        // build physical plan.
        let physical_plan = self.get_physical_plan().await?;

        // build pipeline.
        let mut build_res = PipelineBuildResult::create();
        if let Some(physical_plan) = physical_plan {
            build_res =
                build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan).await?;
            {
                let hook_operator = HookOperator::create(
                    self.ctx.clone(),
                    catalog_name.to_string(),
                    db_name.to_string(),
                    tbl_name.to_string(),
                    "update".to_string(),
                    // table lock has been added, no need to check.
                    false,
                );
                hook_operator
                    .execute_refresh(&mut build_res.main_pipeline)
                    .await;
            }
        }

        build_res.main_pipeline.add_lock_guard(lock_guard);
        Ok(build_res)
    }
}

impl UpdateInterpreter {
    fn generate_operators(&self, table: &FuseTable) -> Result<Vec<BlockOperator>> {
        let mut col_indices: Vec<usize> = self.plan.subquery_desc[0]
            .outer_columns
            .clone()
            .into_iter()
            .collect();
        col_indices.sort();
        debug_assert!(!col_indices.is_empty());

        let update_list = self.plan.generate_update_list(
            self.ctx.clone(),
            table.schema_with_stream().into(),
            col_indices.clone(),
        )?;

        let computed_list = self
            .plan
            .generate_stored_computed_list(self.ctx.clone(), Arc::new(table.schema().into()))?;

        let mut cap = 2;
        if !computed_list.is_empty() {
            cap += 1;
        }
        let mut ops = Vec::with_capacity(cap);
        let schema = table.schema_with_stream();
        let all_column_indices = table.all_column_indices();
        let mut offset_map = BTreeMap::new();
        let mut pos = 0;

        let (_projection, input_schema) = {
            col_indices.iter().for_each(|&index| {
                offset_map.insert(index, pos);
                pos += 1;
            });

            let mut fields: Vec<TableField> = col_indices
                .iter()
                .map(|index| schema.fields()[*index].clone())
                .collect();

            // add `_predicate` column into input schema
            fields.push(TableField::new(
                PREDICATE_COLUMN_NAME,
                TableDataType::Boolean,
            ));
            pos += 1;

            let remain_col_indices: Vec<FieldIndex> = all_column_indices
                .into_iter()
                .filter(|index| !col_indices.contains(index))
                .collect();
            if !remain_col_indices.is_empty() {
                remain_col_indices.iter().for_each(|&index| {
                    offset_map.insert(index, pos);
                    pos += 1;
                });

                let reader = table.create_block_reader(
                    self.ctx.clone(),
                    Projection::Columns(remain_col_indices),
                    false,
                    false,
                    false,
                )?;
                fields.extend_from_slice(reader.schema().fields());
            }

            (
                Projection::Columns(col_indices),
                Arc::new(TableSchema::new(fields)),
            )
        };

        let mut exprs = Vec::with_capacity(update_list.len());
        for (id, remote_expr) in update_list.into_iter() {
            let expr = remote_expr
                .as_expr(&BUILTIN_FUNCTIONS)
                .project_column_ref(|name| input_schema.index_of(name).unwrap());
            exprs.push(expr);
            offset_map.insert(id, pos);
            pos += 1;
        }
        if !exprs.is_empty() {
            ops.push(BlockOperator::Map {
                exprs,
                projections: None,
            });
        }

        let mut computed_exprs = Vec::with_capacity(computed_list.len());
        for (id, remote_expr) in computed_list.into_iter() {
            let expr = remote_expr
                .as_expr(&BUILTIN_FUNCTIONS)
                .project_column_ref(|name| {
                    let id = schema.index_of(name).unwrap();
                    let pos = offset_map.get(&id).unwrap();
                    *pos as FieldIndex
                });
            computed_exprs.push(expr);
            offset_map.insert(id, pos);
            pos += 1;
        }
        // regenerate related stored computed columns.
        if !computed_exprs.is_empty() {
            ops.push(BlockOperator::Map {
                exprs: computed_exprs,
                projections: None,
            });
        }

        ops.push(BlockOperator::Project {
            projection: offset_map.values().cloned().collect(),
        });
        Ok(ops)
    }

    async fn update_by_subquery(&self, table: Arc<dyn Table>) -> Result<PipelineBuildResult> {
        let subquery_desc = &self.plan.subquery_desc[0];
        let input_expr = &subquery_desc.input_expr;
        let mut outer_columns = subquery_desc.outer_columns.clone();
        outer_columns.extend(subquery_desc.predicate_columns.iter());
        let ctx = &self.ctx;
        let table = FuseTable::try_from_table(table.as_ref())?;
        let snapshot = if let Some(snapshot) = table.read_table_snapshot().await? {
            snapshot
        } else {
            return Err(ErrorCode::from_string(format!(
                "read table {:?} snapshot failed",
                table.name()
            )));
        };

        let operators = self.generate_operators(table)?;

        // 1. build sub query join input
        let mut builder =
            PhysicalPlanBuilder::new(self.plan.metadata.clone(), self.ctx.clone(), false);
        let join_input = builder.build(input_expr, outer_columns).await?;

        let pipeline_builder = PipelineBuilder::create(
            ctx.get_function_context()?,
            ctx.get_settings(),
            ctx.clone(),
            vec![],
        );
        let mut build_res = pipeline_builder.finalize(&join_input)?;

        // 2. add TransformMutationSubquery
        build_res.main_pipeline.add_transform(|input, output| {
            TransformMutationSubquery::try_create(
                ctx.get_function_context()?,
                input,
                output,
                SubqueryMutation::Update(operators.clone()),
                subquery_desc.compare_op,
            )?
            .into_processor()
        })?;

        // 3. add TransformSerializeBlock
        let block_thresholds = table.get_block_thresholds();
        let cluster_stats_gen =
            table.get_cluster_stats_gen(self.ctx.clone(), 0, block_thresholds, None)?;
        build_res.main_pipeline.add_transform(|input, output| {
            let proc = TransformSerializeBlock::try_create(
                ctx.clone(),
                input,
                output,
                table,
                cluster_stats_gen.clone(),
                MutationKind::Replace,
            )?;
            proc.into_processor()
        })?;

        // 4. add TransformSerializeSegment
        build_res.main_pipeline.try_resize(1)?;
        build_res.main_pipeline.add_transform(|input, output| {
            let proc =
                TransformSerializeSegment::new(ctx.clone(), input, output, table, block_thresholds);
            proc.into_processor()
        })?;

        // 5. add TableMutationAggregator
        build_res.main_pipeline.add_transform(|input, output| {
            let aggregator =
                TableMutationAggregator::new(table, ctx.clone(), vec![], MutationKind::Replace);
            Ok(ProcessorPtr::create(AsyncAccumulatingTransformer::create(
                input, output, aggregator,
            )))
        })?;

        // 6. add CommitSink
        let snapshot_gen = MutationGenerator::new(snapshot, true);
        let lock = None;
        build_res.main_pipeline.add_sink(|input| {
            databend_common_storages_fuse::operations::CommitSink::try_create(
                table,
                ctx.clone(),
                None,
                vec![],
                snapshot_gen.clone(),
                input,
                None,
                lock.clone(),
                None,
                None,
            )
        })?;

        Ok(build_res)
    }

    pub async fn get_physical_plan(&self) -> Result<Option<PhysicalPlan>> {
        debug_assert!(self.plan.subquery_desc.is_empty());

        let catalog_name = self.plan.catalog.as_str();
        let catalog = self.ctx.get_catalog(catalog_name).await?;
        let catalog_info = catalog.info();

        let db_name = self.plan.database.as_str();
        let tbl_name = self.plan.table.as_str();
        let tbl = catalog
            .get_table(self.ctx.get_tenant().name(), db_name, tbl_name)
            .await?;
        // refresh table.
        let tbl = tbl.refresh(self.ctx.as_ref()).await?;
        // check mutability
        tbl.check_mutable()?;

        let selection = self.plan.selection.clone();

        let (mut filters, col_indices) = if let Some(scalar) = selection {
            // prepare the filter expression
            let filters = create_push_down_filters(&scalar)?;

            let expr = filters.filter.as_expr(&BUILTIN_FUNCTIONS);
            if !expr.is_deterministic(&BUILTIN_FUNCTIONS) {
                return Err(ErrorCode::Unimplemented(
                    "Update must have deterministic predicate",
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

        let update_list = self.plan.generate_update_list(
            self.ctx.clone(),
            tbl.schema_with_stream().into(),
            col_indices.clone(),
        )?;

        let computed_list = self
            .plan
            .generate_stored_computed_list(self.ctx.clone(), Arc::new(tbl.schema().into()))?;

        if !computed_list.is_empty() {
            let license_manager = get_license_manager();
            license_manager
                .manager
                .check_enterprise_enabled(self.ctx.get_license_key(), ComputedColumn)?;
        }

        let fuse_table = tbl.as_any().downcast_ref::<FuseTable>().ok_or_else(|| {
            ErrorCode::Unimplemented(format!(
                "table {}, engine type {}, does not support UPDATE",
                tbl.name(),
                tbl.get_table_info().engine(),
            ))
        })?;

        if let Some(snapshot) = fuse_table
            .fast_update(self.ctx.clone(), &mut filters, col_indices.clone(), false)
            .await?
        {
            let partitions = fuse_table
                .mutation_read_partitions(
                    self.ctx.clone(),
                    snapshot.clone(),
                    col_indices.clone(),
                    filters.clone(),
                    false,
                    false,
                )
                .await?;

            let is_distributed = !self.ctx.get_cluster().is_empty();
            let physical_plan = Self::build_physical_plan(
                filters,
                update_list,
                computed_list,
                partitions,
                fuse_table.get_table_info().clone(),
                col_indices,
                snapshot,
                catalog_info,
                false,
                is_distributed,
                self.ctx.clone(),
            )?;
            return Ok(Some(physical_plan));
        }
        Ok(None)
    }
    #[allow(clippy::too_many_arguments)]
    pub fn build_physical_plan(
        filters: Option<Filters>,
        update_list: Vec<(FieldIndex, RemoteExpr<String>)>,
        computed_list: BTreeMap<FieldIndex, RemoteExpr<String>>,
        partitions: Partitions,
        table_info: TableInfo,
        col_indices: Vec<usize>,
        snapshot: Arc<TableSnapshot>,
        catalog_info: CatalogInfo,
        query_row_id_col: bool,
        is_distributed: bool,
        ctx: Arc<QueryContext>,
    ) -> Result<PhysicalPlan> {
        let merge_meta = partitions.partitions_type() == PartInfoType::LazyLevel;
        let mut root = PhysicalPlan::UpdateSource(Box::new(UpdateSource {
            parts: partitions,
            filters,
            table_info: table_info.clone(),
            catalog_info: catalog_info.clone(),
            col_indices,
            query_row_id_col,
            update_list,
            computed_list,
            plan_id: u32::MAX,
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
        let mut plan = PhysicalPlan::CommitSink(Box::new(CommitSink {
            input: Box::new(root),
            snapshot,
            table_info,
            catalog_info,
            mutation_kind: MutationKind::Update,
            update_stream_meta: vec![],
            merge_meta,
            need_lock: false,
            deduplicated_label: unsafe { ctx.get_settings().get_deduplicate_label()? },
            plan_id: u32::MAX,
        }));
        plan.adjust_plan_id(&mut 0);
        Ok(plan)
    }
}
