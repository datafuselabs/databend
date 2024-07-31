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
use databend_common_expression::TableSchemaRefExt;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_sql::plans::RefreshTableIndexPlan;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::TableContext;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct RefreshTableIndexInterpreter {
    ctx: Arc<QueryContext>,
    plan: RefreshTableIndexPlan,
}

impl RefreshTableIndexInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: RefreshTableIndexPlan) -> Result<Self> {
        Ok(RefreshTableIndexInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for RefreshTableIndexInterpreter {
    fn name(&self) -> &str {
        "RefreshTableIndexInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::InvertedIndex)?;

        let table = self
            .ctx
            .get_table(&self.plan.catalog, &self.plan.database, &self.plan.table)
            .await?;
        // check mutability
        table.check_mutable()?;

        let index_name = self.plan.index_name.clone();
        let segment_locs = self.plan.segment_locs.clone();
        let table_meta = &table.get_table_info().meta;
        let Some(index) = table_meta.indexes.get(&index_name) else {
            return Err(ErrorCode::RefreshIndexError(format!(
                "Inverted index {} does not exist",
                index_name
            )));
        };
        let mut index_fields = Vec::with_capacity(index.column_ids.len());
        for column_id in &index.column_ids {
            for field in &table_meta.schema.fields {
                if field.column_id() == *column_id {
                    index_fields.push(field.clone());
                    break;
                }
            }
        }
        if index_fields.len() != index.column_ids.len() {
            return Err(ErrorCode::RefreshIndexError(format!(
                "Inverted index {} is invalid",
                index_name
            )));
        }
        let index_version = index.version.clone();
        let index_schema = TableSchemaRefExt::create(index_fields);

        let mut build_res = PipelineBuildResult::create();

        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        fuse_table
            .do_refresh_inverted_index(
                self.ctx.clone(),
                index_name,
                index_version,
                &index.options,
                index_schema,
                segment_locs,
                &mut build_res.main_pipeline,
            )
            .await?;

        Ok(build_res)
    }
}
