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

use databend_common_catalog::lock::LockTableOption;
use databend_common_catalog::plan::Filters;
use databend_common_catalog::plan::PartInfoType;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::FieldIndex;
use databend_common_expression::RemoteExpr;
use databend_common_expression::ROW_ID_COLUMN_ID;
use databend_common_expression::ROW_ID_COL_NAME;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_license::license::Feature::ComputedColumn;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::schema::TableInfo;
use databend_common_sql::binder::ColumnBindingBuilder;
use databend_common_sql::executor::physical_plans::CommitSink;
use databend_common_sql::executor::physical_plans::Exchange;
use databend_common_sql::executor::physical_plans::FragmentKind;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::executor::physical_plans::UpdateSource;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_sql::Visibility;
use databend_common_storages_factory::Table;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_table_meta::meta::TableSnapshot;
use log::debug;

use crate::interpreters::common::check_deduplicate_label;
use crate::interpreters::common::create_push_down_filters;
use crate::interpreters::interpreter_delete::subquery_filter;
use crate::interpreters::HookOperator;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
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

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "update_interpreter_execute");

        if check_deduplicate_label(self.ctx.clone()).await? {
            return Ok(PipelineBuildResult::create());
        }

        let catalog_name = self.plan.catalog.as_str();
        let db_name = self.plan.database.as_str();
        let tbl_name = self.plan.table.as_str();

        // build physical plan.
        let physical_plan: Option<PhysicalPlan> = self.get_physical_plan().await?;

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
                    MutationKind::Update,
                    // table lock has been added, no need to check.
                    LockTableOption::NoLock,
                );
                hook_operator
                    .execute_refresh(&mut build_res.main_pipeline)
                    .await;
            }
        }

        build_res
            .main_pipeline
            .add_lock_guard(self.plan.lock_guard.clone());
        Ok(build_res)
    }
}

impl UpdateInterpreter {
    pub async fn get_physical_plan(&self) -> Result<Option<PhysicalPlan>> {
        let catalog_name = self.plan.catalog.as_str();
        let db_name = self.plan.database.as_str();
        let tbl_name = self.plan.table.as_str();
        let tbl = self.ctx.get_table(catalog_name, db_name, tbl_name).await?;
        // check mutability
        tbl.check_mutable()?;

        let selection = if let Some(subquery_desc) = &self.plan.subquery_desc {
            let support_row_id = tbl.supported_internal_column(ROW_ID_COLUMN_ID);
            if !support_row_id {
                return Err(ErrorCode::from_string(format!(
                    "Update with subquery is not supported for the table '{}', which lacks row_id support.",
                    tbl.name(),
                )));
            }
            let table_index = self
                .plan
                .metadata
                .read()
                .get_table_index(Some(self.plan.database.as_str()), self.plan.table.as_str());
            let row_id_column_binding = ColumnBindingBuilder::new(
                ROW_ID_COL_NAME.to_string(),
                subquery_desc.index,
                Box::new(DataType::Number(NumberDataType::UInt64)),
                Visibility::InVisible,
            )
            .database_name(Some(self.plan.database.clone()))
            .table_name(Some(self.plan.table.clone()))
            .table_index(table_index)
            .build();
            let filter = subquery_filter(
                self.ctx.clone(),
                self.plan.metadata.clone(),
                &row_id_column_binding,
                subquery_desc,
            )
            .await?;
            Some(filter)
        } else {
            self.plan.selection.clone()
        };

        let (mut filters, col_indices) = if let Some(scalar) = selection {
            // prepare the filter expression
            let filters = create_push_down_filters(&scalar)?;

            let expr = filters.filter.as_expr(&BUILTIN_FUNCTIONS);
            if !expr.is_deterministic(&BUILTIN_FUNCTIONS) {
                return Err(ErrorCode::Unimplemented(
                    "Update must have deterministic predicate",
                ));
            }

            let mut used_columns = scalar.used_columns().clone();
            let col_indices: Vec<usize> = if let Some(subquery_desc) = &self.plan.subquery_desc {
                // add scalar.used_columns() but ignore _row_id index
                let mut col_indices = subquery_desc.outer_columns.clone();
                used_columns.remove(&subquery_desc.index);
                col_indices.extend(used_columns.iter());
                col_indices.into_iter().collect()
            } else {
                used_columns.into_iter().collect()
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
            LicenseManagerSwitch::instance()
                .check_enterprise_enabled(self.ctx.get_license_key(), ComputedColumn)?;
        }

        let fuse_table = tbl.as_any().downcast_ref::<FuseTable>().ok_or_else(|| {
            ErrorCode::Unimplemented(format!(
                "table {}, engine type {}, does not support UPDATE",
                tbl.name(),
                tbl.get_table_info().engine(),
            ))
        })?;

        let query_row_id_col = self.plan.subquery_desc.is_some();
        if let Some(snapshot) = fuse_table
            .fast_update(
                self.ctx.clone(),
                &mut filters,
                col_indices.clone(),
                query_row_id_col,
            )
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
                query_row_id_col,
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
        query_row_id_col: bool,
        is_distributed: bool,
        ctx: Arc<QueryContext>,
    ) -> Result<PhysicalPlan> {
        let merge_meta = partitions.partitions_type() == PartInfoType::LazyLevel;
        let mut root = PhysicalPlan::UpdateSource(Box::new(UpdateSource {
            parts: partitions,
            filters,
            table_info: table_info.clone(),
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
            snapshot: Some(snapshot),
            table_info,
            mutation_kind: MutationKind::Update,
            update_stream_meta: vec![],
            merge_meta,
            deduplicated_label: unsafe { ctx.get_settings().get_deduplicate_label()? },
            plan_id: u32::MAX,
            recluster_info: None,
        }));
        plan.adjust_plan_id(&mut 0);
        Ok(plan)
    }
}
