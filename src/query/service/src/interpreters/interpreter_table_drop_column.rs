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

use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_meta_app::schema::DatabaseType;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_meta_types::MatchSeq;
use databend_common_sql::plans::DropTableColumnPlan;
use databend_common_sql::BloomIndexColumns;
use databend_common_storages_share::update_share_table_info;
use databend_common_storages_stream::stream_table::STREAM_ENGINE;
use databend_common_storages_view::view_table::VIEW_ENGINE;
use databend_storages_common_table_meta::table::OPT_KEY_BLOOM_INDEX_COLUMNS;

use crate::interpreters::common::check_referenced_computed_columns;
use crate::interpreters::interpreter_table_add_column::generate_new_snapshot;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct DropTableColumnInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropTableColumnPlan,
}

impl DropTableColumnInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropTableColumnPlan) -> Result<Self> {
        Ok(DropTableColumnInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropTableColumnInterpreter {
    fn name(&self) -> &str {
        "DropTableColumnInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let catalog_name = self.plan.catalog.as_str();
        let db_name = self.plan.database.as_str();
        let tbl_name = self.plan.table.as_str();
        let table = self
            .ctx
            .get_catalog(catalog_name)
            .await?
            .get_table(&self.ctx.get_tenant(), db_name, tbl_name)
            .await?;

        // check mutability
        table.check_mutable()?;

        let table_info = table.get_table_info();
        let engine = table_info.engine();
        if matches!(engine, VIEW_ENGINE | STREAM_ENGINE) {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "{}.{} engine is {} that doesn't support alter",
                &self.plan.database, &self.plan.table, engine
            )));
        }
        if table_info.db_type != DatabaseType::NormalDB {
            return Err(ErrorCode::TableEngineNotSupported(format!(
                "{}.{} doesn't support alter",
                &self.plan.database, &self.plan.table
            )));
        }

        let table_schema = table_info.schema();
        let field = table_schema.field_with_name(self.plan.column.as_str())?;
        if field.computed_expr().is_none() {
            let mut schema: DataSchema = table_info.schema().into();
            schema.drop_column(self.plan.column.as_str())?;
            // Check if this column is referenced by computed columns.
            check_referenced_computed_columns(
                self.ctx.clone(),
                Arc::new(schema),
                self.plan.column.as_str(),
            )?;
        }
        // If the column is inverted index column, the column can't be dropped.
        if !table_info.meta.indexes.is_empty() {
            for (index_name, index) in &table_info.meta.indexes {
                if index.column_ids.contains(&field.column_id) {
                    return Err(ErrorCode::ColumnReferencedByInvertedIndex(format!(
                        "column `{}` is referenced by inverted index, drop inverted index `{}` first",
                        field.name, index_name,
                    )));
                }
            }
        }

        let catalog = self.ctx.get_catalog(catalog_name).await?;
        let mut new_table_meta = table.get_table_info().meta.clone();
        new_table_meta.drop_column(&self.plan.column)?;

        // update table options
        let opts = &mut new_table_meta.options;
        if let Some(value) = opts.get_mut(OPT_KEY_BLOOM_INDEX_COLUMNS) {
            let bloom_index_cols = value.parse::<BloomIndexColumns>()?;
            if let BloomIndexColumns::Specify(mut cols) = bloom_index_cols {
                if let Some(pos) = cols.iter().position(|x| *x == self.plan.column) {
                    // remove from the bloom index columns.
                    cols.remove(pos);
                    *value = cols.join(",");
                }
            }
        }

        let table_id = table_info.ident.table_id;
        let table_version = table_info.ident.seq;

        generate_new_snapshot(self.ctx.as_ref(), table.as_ref(), &mut new_table_meta).await?;

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
