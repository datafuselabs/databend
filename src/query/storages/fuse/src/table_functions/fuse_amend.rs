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

use databend_common_catalog::catalog_kind::CATALOG_DEFAULT;
use databend_common_catalog::plan::DataSourcePlan;
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
use databend_enterprise_fail_safe::get_fail_safe_handler;
use databend_enterprise_fail_safe::FailSafeHandlerWrapper;

use crate::sessions::TableContext;
use crate::table_functions::parse_db_tb_args;
use crate::table_functions::string_literal;
use crate::table_functions::SimpleTableFunc;
use crate::table_functions::TableArgs;
use crate::FuseTable;

struct AmendTableArgs {
    database_name: String,
    table_name: String,
}

impl From<&AmendTableArgs> for TableArgs {
    fn from(args: &AmendTableArgs) -> Self {
        TableArgs::new_positioned(vec![
            string_literal(args.database_name.as_str()),
            string_literal(args.table_name.as_str()),
        ])
    }
}

pub struct FuseAmendTable {
    args: AmendTableArgs,
    fail_safe_handler: Arc<FailSafeHandlerWrapper>,
}

#[async_trait::async_trait]
impl SimpleTableFunc for FuseAmendTable {
    fn table_args(&self) -> Option<TableArgs> {
        Some((&self.args).into())
    }

    fn schema(&self) -> TableSchemaRef {
        TableSchemaRefExt::create(vec![TableField::new("result", TableDataType::String)])
    }

    async fn apply(
        &self,
        ctx: &Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
    ) -> Result<Option<DataBlock>> {
        let license_manager = get_license_manager();
        license_manager
            .manager
            .check_enterprise_enabled(ctx.get_license_key(), Feature::AmendTable)?;
        let tenant_id = ctx.get_tenant();
        let tbl = ctx
            .get_catalog(CATALOG_DEFAULT)
            .await?
            .get_table(
                &tenant_id,
                self.args.database_name.as_str(),
                self.args.table_name.as_str(),
            )
            .await?;

        let tbl = FuseTable::try_from_table(tbl.as_ref()).map_err(|_| {
            ErrorCode::StorageOther("Invalid table engine, only fuse table is supported")
        })?;

        self.fail_safe_handler
            .recover(tbl.table_info.clone())
            .await?;

        let col: Vec<String> = vec!["Ok".to_owned()];

        Ok(Some(DataBlock::new_from_columns(vec![
            StringType::from_data(col),
        ])))
    }

    fn create(func_name: &str, table_args: TableArgs) -> Result<Self>
    where Self: Sized {
        let fail_safe_handler = get_fail_safe_handler();
        let (arg_database_name, arg_table_name) = parse_db_tb_args(&table_args, func_name)?;
        Ok(Self {
            args: AmendTableArgs {
                database_name: arg_database_name,
                table_name: arg_table_name,
            },
            fail_safe_handler,
        })
    }
}
