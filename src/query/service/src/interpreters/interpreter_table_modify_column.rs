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

use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ComputedExpr;
use databend_common_expression::DataSchema;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_license::license::Feature::ComputedColumn;
use databend_common_license::license::Feature::DataMask;
use databend_common_license::license_manager::get_license_manager;
use databend_common_meta_app::schema::DatabaseType;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyAction;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyReq;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_meta_types::MatchSeq;
use databend_common_sql::executor::physical_plans::DistributedInsertSelect;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_sql::executor::PhysicalPlanBuilder;
use databend_common_sql::field_default_value;
use databend_common_sql::plans::ModifyColumnAction;
use databend_common_sql::plans::ModifyTableColumnPlan;
use databend_common_sql::plans::Plan;
use databend_common_sql::BloomIndexColumns;
use databend_common_sql::Planner;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_share::update_share_table_info;
use databend_common_storages_stream::stream_table::STREAM_ENGINE;
use databend_common_storages_view::view_table::VIEW_ENGINE;
use databend_common_users::UserApiProvider;
use databend_enterprise_data_mask_feature::get_datamask_handler;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_table_meta::table::OPT_KEY_BLOOM_INDEX_COLUMNS;

use crate::interpreters::common::check_referenced_computed_columns;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct ModifyTableColumnInterpreter {
    ctx: Arc<QueryContext>,
    plan: ModifyTableColumnPlan,
}

impl ModifyTableColumnInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ModifyTableColumnPlan) -> Result<Self> {
        Ok(ModifyTableColumnInterpreter { ctx, plan })
    }

    // Set data mask policy to a column is a ee feature.
    async fn do_set_data_mask_policy(
        &self,
        catalog: Arc<dyn Catalog>,
        table: Arc<dyn Table>,
        column: String,
        mask_name: String,
    ) -> Result<PipelineBuildResult> {
        let license_manager = get_license_manager();
        license_manager
            .manager
            .check_enterprise_enabled(self.ctx.get_license_key(), DataMask)?;

        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let handler = get_datamask_handler();
        let policy = handler
            .get_data_mask(meta_api, &self.ctx.get_tenant(), mask_name.clone())
            .await?;

        // check if column type match to the input type
        let policy_data_type = policy.args[0].1.to_string().to_lowercase();
        let schema = table.schema();
        let table_info = table.get_table_info();
        if let Some((_, data_field)) = schema.column_with_name(&column) {
            let data_type = data_field.data_type().to_string().to_lowercase();
            if data_type != policy_data_type {
                return Err(ErrorCode::UnmatchColumnDataType(format!(
                    "Column '{}' data type {} does not match to the mask policy type {}",
                    column, data_type, policy_data_type,
                )));
            }
        } else {
            return Err(ErrorCode::UnknownColumn(format!(
                "Cannot find column {}",
                column
            )));
        }

        let table_id = table_info.ident.table_id;
        let table_version = table_info.ident.seq;

        let prev_column_mask_name =
            if let Some(column_mask_policy) = &table_info.meta.column_mask_policy {
                column_mask_policy.get(&column).cloned()
            } else {
                None
            };
        let req = SetTableColumnMaskPolicyReq {
            tenant: self.ctx.get_tenant(),
            seq: MatchSeq::Exact(table_version),
            table_id,
            column,
            action: SetTableColumnMaskPolicyAction::Set(mask_name, prev_column_mask_name),
        };

        let _resp = catalog.set_table_column_mask_policy(req).await?;

        Ok(PipelineBuildResult::create())
    }

    // Set data column type.
    async fn do_set_data_type(
        &self,
        table: Arc<dyn Table>,
        field_and_comments: &[(TableField, String)],
    ) -> Result<PipelineBuildResult> {
        let schema = table.schema().as_ref().clone();
        let table_info = table.get_table_info();
        let mut new_schema = schema.clone();

        // first check default expr before lock table
        for (field, _comment) in field_and_comments {
            let column = &field.name.to_string();
            let data_type = &field.data_type;
            if let Some((i, _)) = schema.column_with_name(column) {
                if let Some(default_expr) = &field.default_expr {
                    let default_expr = default_expr.to_string();
                    new_schema.fields[i].data_type = data_type.clone();
                    new_schema.fields[i].default_expr = Some(default_expr);
                    let _ = field_default_value(self.ctx.clone(), &new_schema.fields[i])?;
                }
            } else {
                return Err(ErrorCode::UnknownColumn(format!(
                    "Cannot find column {}",
                    column
                )));
            }
        }

        let catalog_name = table_info.catalog();
        let catalog = self.ctx.get_catalog(catalog_name).await?;

        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        let base_snapshot = fuse_table.read_table_snapshot().await?;
        let table_meta_timestamps = self
            .ctx
            .get_table_meta_timestamps(fuse_table.get_id(), base_snapshot.clone())?;

        let mut bloom_index_cols = vec![];
        if let Some(v) = table_info.options().get(OPT_KEY_BLOOM_INDEX_COLUMNS) {
            if let BloomIndexColumns::Specify(cols) = v.parse::<BloomIndexColumns>()? {
                bloom_index_cols = cols;
            }
        }

        let mut table_info = table.get_table_info().clone();
        table_info.meta.fill_field_comments();
        let mut modify_comment = false;
        for (field, comment) in field_and_comments {
            let column = &field.name.to_string();
            let data_type = &field.data_type;
            if let Some((i, old_field)) = schema.column_with_name(column) {
                if data_type != &new_schema.fields[i].data_type {
                    // Check if this column is referenced by computed columns.
                    let mut data_schema: DataSchema = table_info.schema().into();
                    data_schema.set_field_type(i, data_type.into());
                    check_referenced_computed_columns(
                        self.ctx.clone(),
                        Arc::new(data_schema),
                        column,
                    )?;

                    // If the column is defined in bloom index columns,
                    // check whether the data type is supported for bloom index.
                    if bloom_index_cols.iter().any(|v| v.as_str() == column)
                        && !BloomIndex::supported_type(data_type)
                    {
                        return Err(ErrorCode::TableOptionInvalid(format!(
                            "Unsupported data type '{}' for bloom index",
                            data_type
                        )));
                    }
                    // If the column is inverted index column, the type can't be changed.
                    if !table_info.meta.indexes.is_empty() {
                        for (index_name, index) in &table_info.meta.indexes {
                            if index.column_ids.contains(&old_field.column_id)
                                && old_field.data_type.remove_nullable()
                                    != field.data_type.remove_nullable()
                            {
                                return Err(ErrorCode::ColumnReferencedByInvertedIndex(format!(
                                    "column `{}` is referenced by inverted index, drop inverted index `{}` first",
                                    column, index_name,
                                )));
                            }
                        }
                    }
                    new_schema.fields[i].data_type = data_type.clone();
                }
                if table_info.meta.field_comments[i] != *comment {
                    table_info.meta.field_comments[i] = comment.to_string();
                    modify_comment = true;
                }
            } else {
                return Err(ErrorCode::UnknownColumn(format!(
                    "Cannot find column {}",
                    column
                )));
            }
        }

        // check if schema has changed
        if schema == new_schema && !modify_comment {
            return Ok(PipelineBuildResult::create());
        }

        // if schema is same and only modify comment, don't need to modify schema
        if schema == new_schema && modify_comment {
            let table_id = table_info.ident.table_id;
            let table_version = table_info.ident.seq;

            let req = UpdateTableMetaReq {
                table_id,
                seq: MatchSeq::Exact(table_version),
                new_table_meta: table_info.meta,
            };

            let resp = catalog
                .update_single_table_meta(req, table.get_table_info())
                .await?;
            if let Some(share_vec_table_infos) = &resp.share_vec_table_infos {
                for (share_name_vec, db_id, share_table_info) in share_vec_table_infos {
                    update_share_table_info(
                        self.ctx.get_tenant().tenant_name(),
                        self.ctx.get_application_level_data_operator()?.operator(),
                        share_name_vec,
                        *db_id,
                        share_table_info,
                    )
                    .await?;
                }
            }

            return Ok(PipelineBuildResult::create());
        }

        // if alter column from string to binary, we don't need to rebuild table
        let is_alter_column_string_to_binary =
            schema
                .fields()
                .iter()
                .zip(new_schema.fields())
                .all(|(old_field, new_field)| {
                    fn is_string_to_binary(old_ty: &TableDataType, new_ty: &TableDataType) -> bool {
                        match (old_ty, new_ty) {
                            (TableDataType::String, TableDataType::Binary) => true,
                            (TableDataType::Nullable(old_ty), TableDataType::Nullable(new_ty)) => {
                                is_string_to_binary(old_ty, new_ty)
                            }
                            (TableDataType::Map(old_ty), TableDataType::Map(new_ty)) => {
                                is_string_to_binary(old_ty, new_ty)
                            }
                            (TableDataType::Array(old_ty), TableDataType::Array(new_ty)) => {
                                is_string_to_binary(old_ty, new_ty)
                            }
                            (
                                TableDataType::Tuple {
                                    fields_type: old_tys,
                                    ..
                                },
                                TableDataType::Tuple {
                                    fields_type: new_tys,
                                    ..
                                },
                            ) => {
                                old_tys.len() == new_tys.len()
                                    && old_tys
                                        .iter()
                                        .zip(new_tys)
                                        .all(|(old_ty, new_ty)| is_string_to_binary(old_ty, new_ty))
                            }
                            _ => false,
                        }
                    }

                    let TableField {
                        name: old_name,
                        default_expr: old_default_expr,
                        data_type: old_data_type,
                        column_id: old_column_id,
                        computed_expr: old_computed_expr,
                    } = old_field;
                    let TableField {
                        name: new_name,
                        default_expr: new_default_expr,
                        data_type: new_data_type,
                        column_id: new_column_id,
                        computed_expr: new_computed_expr,
                    } = new_field;
                    old_name == new_name
                        && old_default_expr == new_default_expr
                        && old_column_id == new_column_id
                        && old_computed_expr == new_computed_expr
                        && (old_data_type == new_data_type
                            || is_string_to_binary(&old_field.data_type, &new_field.data_type))
                });

        if is_alter_column_string_to_binary {
            table_info.meta.schema = new_schema.into();

            let table_id = table_info.ident.table_id;
            let table_version = table_info.ident.seq;

            let req = UpdateTableMetaReq {
                table_id,
                seq: MatchSeq::Exact(table_version),
                new_table_meta: table_info.meta,
            };

            let resp = catalog
                .update_single_table_meta(req, table.get_table_info())
                .await?;
            if let Some(share_vec_table_infos) = &resp.share_vec_table_infos {
                for (share_name_vec, db_id, share_table_info) in share_vec_table_infos {
                    update_share_table_info(
                        self.ctx.get_tenant().tenant_name(),
                        self.ctx.get_application_level_data_operator()?.operator(),
                        share_name_vec,
                        *db_id,
                        share_table_info,
                    )
                    .await?;
                }
            }

            return Ok(PipelineBuildResult::create());
        }

        // 1. construct sql for selecting data from old table
        let mut sql = "select".to_string();
        schema
            .fields()
            .iter()
            .enumerate()
            .for_each(|(index, field)| {
                if index != schema.fields().len() - 1 {
                    sql = format!("{} `{}`,", sql, field.name.clone());
                } else {
                    sql = format!(
                        "{} `{}` from `{}`.`{}`",
                        sql,
                        field.name.clone(),
                        self.plan.database,
                        self.plan.table
                    );
                }
            });

        // 2. build plan by sql
        let mut planner = Planner::new(self.ctx.clone());
        let (plan, _extras) = planner.plan_sql(&sql).await?;

        // 3. build physical plan by plan
        let (select_plan, select_column_bindings) = match plan {
            Plan::Query {
                s_expr,
                metadata,
                bind_context,
                ..
            } => {
                let mut builder1 =
                    PhysicalPlanBuilder::new(metadata.clone(), self.ctx.clone(), false);
                (
                    builder1.build(&s_expr, bind_context.column_set()).await?,
                    bind_context.columns.clone(),
                )
            }
            _ => unreachable!(),
        };

        // 4. define select schema and insert schema of DistributedInsertSelect plan
        table_info.meta.schema = new_schema.clone().into();
        let new_table = FuseTable::try_create(table_info)?;

        // 5. build DistributedInsertSelect plan
        let insert_plan =
            PhysicalPlan::DistributedInsertSelect(Box::new(DistributedInsertSelect {
                plan_id: select_plan.get_id(),
                input: Box::new(select_plan),
                table_info: new_table.get_table_info().clone(),
                select_schema: Arc::new(Arc::new(schema).into()),
                select_column_bindings,
                insert_schema: Arc::new(Arc::new(new_schema).into()),
                cast_needed: true,
                table_meta_timestamps,
            }));
        let mut build_res =
            build_query_pipeline_without_render_result_set(&self.ctx, &insert_plan).await?;

        // 6. commit new meta schema and snapshots
        new_table.commit_insertion(
            self.ctx.clone(),
            &mut build_res.main_pipeline,
            None,
            vec![],
            true,
            base_snapshot.as_ref().map(|b| b.snapshot_id),
            None,
            table_meta_timestamps,
        )?;

        Ok(build_res)
    }

    // unset data mask policy to a column is a ee feature.
    async fn do_unset_data_mask_policy(
        &self,
        catalog: Arc<dyn Catalog>,
        table: Arc<dyn Table>,
        column: String,
    ) -> Result<PipelineBuildResult> {
        let license_manager = get_license_manager();
        license_manager
            .manager
            .check_enterprise_enabled(self.ctx.get_license_key(), DataMask)?;

        let table_info = table.get_table_info();
        let table_id = table_info.ident.table_id;
        let table_version = table_info.ident.seq;

        let prev_column_mask_name =
            if let Some(column_mask_policy) = &table_info.meta.column_mask_policy {
                column_mask_policy.get(&column).cloned()
            } else {
                None
            };

        if let Some(prev_column_mask_name) = prev_column_mask_name {
            let req = SetTableColumnMaskPolicyReq {
                tenant: self.ctx.get_tenant(),
                seq: MatchSeq::Exact(table_version),
                table_id,
                column,
                action: SetTableColumnMaskPolicyAction::Unset(prev_column_mask_name),
            };

            let resp = catalog.set_table_column_mask_policy(req).await?;
            if let Some((share_name_vec, db_id, share_table_info)) = resp.share_vec_table_info {
                update_share_table_info(
                    self.ctx.get_tenant().tenant_name(),
                    self.ctx.get_application_level_data_operator()?.operator(),
                    &share_name_vec,
                    db_id,
                    &share_table_info,
                )
                .await?;
            }
        }

        Ok(PipelineBuildResult::create())
    }

    async fn do_convert_stored_computed_column(
        &self,
        catalog: Arc<dyn Catalog>,
        table: Arc<dyn Table>,
        table_meta: TableMeta,
        column: String,
    ) -> Result<PipelineBuildResult> {
        let license_manager = get_license_manager();
        license_manager
            .manager
            .check_enterprise_enabled(self.ctx.get_license_key(), ComputedColumn)?;

        let table_info = table.get_table_info();
        let schema = table.schema();
        let new_schema = if let Some((i, field)) = schema.column_with_name(&column) {
            match field.computed_expr {
                Some(ComputedExpr::Stored(_)) => {}
                _ => {
                    return Err(ErrorCode::UnknownColumn(format!(
                        "Column '{}' is not a stored computed column",
                        column
                    )));
                }
            }
            let mut new_field = field.clone();
            new_field.computed_expr = None;
            let mut fields = schema.fields().clone();
            fields[i] = new_field;
            TableSchema::new_from(fields, schema.metadata.clone())
        } else {
            return Err(ErrorCode::UnknownColumn(format!(
                "Cannot find column {}",
                column
            )));
        };

        let mut new_table_meta = table_meta;
        new_table_meta.schema = new_schema.into();

        let table_id = table_info.ident.table_id;
        let table_version = table_info.ident.seq;

        let req = UpdateTableMetaReq {
            table_id,
            seq: MatchSeq::Exact(table_version),
            new_table_meta,
        };

        let resp = catalog.update_single_table_meta(req, table_info).await?;
        if let Some(share_vec_table_infos) = &resp.share_vec_table_infos {
            for (share_name_vec, db_id, share_table_info) in share_vec_table_infos {
                update_share_table_info(
                    self.ctx.get_tenant().tenant_name(),
                    self.ctx.get_application_level_data_operator()?.operator(),
                    share_name_vec,
                    *db_id,
                    share_table_info,
                )
                .await?;
            }
        }

        Ok(PipelineBuildResult::create())
    }
}

#[async_trait::async_trait]
impl Interpreter for ModifyTableColumnInterpreter {
    fn name(&self) -> &str {
        "ModifyTableColumnInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog_name = self.plan.catalog.as_str();
        let db_name = self.plan.database.as_str();
        let tbl_name = self.plan.table.as_str();

        let catalog = self.ctx.get_catalog(catalog_name).await?;
        let table = self.ctx.get_table(catalog_name, db_name, tbl_name).await?;

        table.check_mutable()?;

        let table_info = table.get_table_info();
        let engine = table.engine();
        if matches!(engine, VIEW_ENGINE | STREAM_ENGINE) {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "{}.{} engine is {} that doesn't support alter",
                db_name, tbl_name, engine
            )));
        }
        if table_info.db_type != DatabaseType::NormalDB {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "{}.{} doesn't support alter",
                db_name, tbl_name
            )));
        }

        let table_meta = table.get_table_info().meta.clone();

        // NOTICE: if we support modify column data type,
        // need to check whether this column is referenced by other computed columns.
        let mut build_res = match &self.plan.action {
            ModifyColumnAction::SetMaskingPolicy(column, mask_name) => {
                self.do_set_data_mask_policy(catalog, table, column.to_string(), mask_name.clone())
                    .await?
            }
            ModifyColumnAction::UnsetMaskingPolicy(column) => {
                self.do_unset_data_mask_policy(catalog, table, column.to_string())
                    .await?
            }
            ModifyColumnAction::SetDataType(field_and_comment) => {
                self.do_set_data_type(table, field_and_comment).await?
            }
            ModifyColumnAction::ConvertStoredComputedColumn(column) => {
                self.do_convert_stored_computed_column(
                    catalog,
                    table,
                    table_meta,
                    column.to_string(),
                )
                .await?
            }
        };

        build_res
            .main_pipeline
            .add_lock_guard(self.plan.lock_guard.clone());
        Ok(build_res)
    }
}
