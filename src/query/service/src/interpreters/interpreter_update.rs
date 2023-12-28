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
use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_catalog::plan::Filters;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::FieldIndex;
use databend_common_expression::RemoteExpr;
use databend_common_expression::ROW_ID_COL_NAME;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_license::license::Feature::ComputedColumn;
use databend_common_license::license_manager::get_license_manager;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::TableInfo;
use databend_common_sql::binder::ColumnBindingBuilder;
use databend_common_sql::executor::physical_plans::CommitSink;
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
use crate::interpreters::hook::hook_refresh;
use crate::interpreters::hook::RefreshDesc;
use crate::interpreters::interpreter_delete::replace_subquery;
use crate::interpreters::interpreter_delete::subquery_filter;
use crate::interpreters::Interpreter;
use crate::locks::LockManager;
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

    #[minitrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        debug!("ctx.id" = self.ctx.get_id().as_str(); "update_interpreter_execute");

        if check_deduplicate_label(self.ctx.clone()).await? {
            return Ok(PipelineBuildResult::create());
        }

        let catalog_name = self.plan.catalog.as_str();
        let catalog = self.ctx.get_catalog(catalog_name).await?;
        let catalog_info = catalog.info();

        let db_name = self.plan.database.as_str();
        let tbl_name = self.plan.table.as_str();
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
            let table_index = self
                .plan
                .metadata
                .read()
                .get_table_index(Some(self.plan.database.as_str()), self.plan.table.as_str());
            let row_id_column_binding = ColumnBindingBuilder::new(
                ROW_ID_COL_NAME.to_string(),
                self.plan.subquery_desc[0].index,
                Box::new(DataType::Number(NumberDataType::UInt64)),
                Visibility::InVisible,
            )
            .database_name(Some(self.plan.database.clone()))
            .table_name(Some(self.plan.table.clone()))
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
            tbl.schema().into(),
            col_indices.clone(),
            None,
            false,
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

        let mut build_res = PipelineBuildResult::create();
        let query_row_id_col = !self.plan.subquery_desc.is_empty();
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
                .mutation_read_partitions(snapshot.clone())
                .await?;

            let physical_plan = Self::build_physical_plan(
                filters,
                update_list,
                computed_list,
                partitions,
                fuse_table.get_table_info().clone(),
                col_indices,
                snapshot,
                catalog_info,
                query_row_id_col,
            )?;

            build_res =
                build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan, false)
                    .await?;

            // generate sync aggregating indexes if `enable_refresh_aggregating_index_after_write` on.
            // generate virtual columns if `enable_refresh_virtual_column_after_write` on.
            {
                let refresh_desc = RefreshDesc {
                    catalog: catalog_name.to_string(),
                    database: db_name.to_string(),
                    table: tbl_name.to_string(),
                };

                hook_refresh(self.ctx.clone(), &mut build_res.main_pipeline, refresh_desc).await?;
            }
        }

        build_res.main_pipeline.add_lock_guard(lock_guard);
        Ok(build_res)
    }
}

impl UpdateInterpreter {
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
    ) -> Result<PhysicalPlan> {
        let merge_meta = partitions.is_lazy;
        let root = PhysicalPlan::UpdateSource(Box::new(UpdateSource {
            parts: partitions,
            filters,
            table_info: table_info.clone(),
            catalog_info: catalog_info.clone(),
            col_indices,
            query_row_id_col,
            update_list,
            computed_list,
        }));

        Ok(PhysicalPlan::CommitSink(Box::new(CommitSink {
            input: Box::new(root),
            snapshot,
            table_info,
            catalog_info,
            mutation_kind: MutationKind::Update,
            update_stream_meta: vec![],
            merge_meta,
            need_lock: false,
        })))
    }
}
