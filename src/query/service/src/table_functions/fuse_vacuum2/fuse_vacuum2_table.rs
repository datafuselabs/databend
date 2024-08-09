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
use databend_common_catalog::catalog_kind::CATALOG_DEFAULT;
use databend_common_catalog::table::TableExt;
use databend_common_catalog::table_args::TableArgs;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::StringType;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::get_license_manager;
use databend_common_storages_fuse::table_functions::parse_db_tb_args;
use databend_common_storages_fuse::table_functions::string_literal;
use databend_common_storages_fuse::table_functions::SimpleTableFunc;
use databend_common_storages_fuse::FuseTable;
use databend_enterprise_vacuum_handler::get_vacuum_handler;
use databend_enterprise_vacuum_handler::VacuumHandlerWrapper;

use crate::sessions::TableContext;

const FUSE_VACUUM2_ENGINE_NAME: &str = "fuse_vacuum2_table";

enum Vacuum2TableArgs {
    SingleTable {
        arg_database_name: String,
        arg_table_name: String,
    },
    All,
}

impl From<&Vacuum2TableArgs> for TableArgs {
    fn from(value: &Vacuum2TableArgs) -> Self {
        match value {
            Vacuum2TableArgs::SingleTable {
                arg_database_name,
                arg_table_name,
            } => TableArgs::new_positioned(vec![
                string_literal(arg_database_name.as_str()),
                string_literal(arg_table_name.as_str()),
            ]),
            Vacuum2TableArgs::All => TableArgs::new_positioned(vec![]),
        }
    }
}

pub struct FuseVacuum2Table {
    args: Vacuum2TableArgs,
    handler: Arc<VacuumHandlerWrapper>,
}

#[async_trait::async_trait]
impl SimpleTableFunc for FuseVacuum2Table {
    fn table_args(&self) -> Option<TableArgs> {
        Some((&self.args).into())
    }

    fn schema(&self) -> TableSchemaRef {
        TableSchemaRefExt::create(vec![TableField::new("vacuumed", TableDataType::String)])
    }

    async fn apply(&self, ctx: &Arc<dyn TableContext>) -> Result<Option<DataBlock>> {
        let license_manager = get_license_manager();
        license_manager
            .manager
            .check_enterprise_enabled(ctx.get_license_key(), Feature::Vacuum)?;

        let catalog = ctx.get_catalog(CATALOG_DEFAULT).await?;
        let res = match &self.args {
            Vacuum2TableArgs::SingleTable {
                arg_database_name,
                arg_table_name,
            } => {
                self.apply_single_table(ctx, catalog.as_ref(), arg_database_name, arg_table_name)
                    .await?
            }
            Vacuum2TableArgs::All => self.apply_all_tables(ctx, catalog.as_ref()).await?,
        };
        Ok(Some(DataBlock::new_from_columns(vec![
            StringType::from_data(res),
        ])))
    }

    fn create(table_args: TableArgs) -> Result<Self>
    where Self: Sized {
        let args = match table_args.positioned.len() {
            0 => Vacuum2TableArgs::All,
            2 => {
                let (arg_database_name, arg_table_name) =
                    parse_db_tb_args(&table_args, FUSE_VACUUM2_ENGINE_NAME)?;
                Vacuum2TableArgs::SingleTable {
                    arg_database_name,
                    arg_table_name,
                }
            }
            _ => {
                return Err(ErrorCode::NumberArgumentsNotMatch(
                    "Expected 0 or 2 arguments".to_string(),
                ));
            }
        };
        Ok(Self {
            args,
            handler: get_vacuum_handler(),
        })
    }
}

impl FuseVacuum2Table {
    async fn apply_single_table(
        &self,
        ctx: &Arc<dyn TableContext>,
        catalog: &dyn Catalog,
        database_name: &str,
        table_name: &str,
    ) -> Result<Vec<String>> {
        let tbl = catalog
            .get_table(&ctx.get_tenant(), database_name, table_name)
            .await?;

        let tbl = FuseTable::try_from_table(tbl.as_ref()).map_err(|_| {
            ErrorCode::StorageOther("Invalid table engine, only fuse table is supported")
        })?;

        tbl.check_mutable()?;

        self.handler.do_vacuum2(tbl, ctx.clone()).await
    }

    async fn apply_all_tables(
        &self,
        ctx: &Arc<dyn TableContext>,
        catalog: &dyn Catalog,
    ) -> Result<Vec<String>> {
        let tenant_id = ctx.get_tenant();
        let dbs = catalog.list_databases(&tenant_id).await?;
        for db in dbs {
            if db.engine() != "DEFAULT" {
                continue;
            }
            let tables = catalog.list_tables(&tenant_id, db.name()).await?;
            for table in tables {
                let tbl = FuseTable::try_from_table(table.as_ref()).map_err(|_| {
                    ErrorCode::StorageOther("Invalid table engine, only fuse table is supported")
                })?;

                if table.is_read_only() {
                    continue;
                }

                let _ = self.handler.do_vacuum2(tbl, ctx.clone()).await?;
            }
        }

        Ok(vec![])
    }
}
