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
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::TableSnapshotStatistics;

use crate::table_functions::parse_db_tb_args;
use crate::table_functions::string_literal;
use crate::table_functions::SimpleArgFunc;
use crate::table_functions::SimpleArgFuncTemplate;
use crate::FuseTable;

pub struct FuseStatsArgs {
    database_name: String,
    table_name: String,
}

impl From<&FuseStatsArgs> for TableArgs {
    fn from(args: &FuseStatsArgs) -> Self {
        let tbl_args = vec![
            string_literal(args.database_name.as_str()),
            string_literal(args.table_name.as_str()),
        ];
        TableArgs::new_positioned(tbl_args)
    }
}

impl TryFrom<(&str, TableArgs)> for FuseStatsArgs {
    type Error = ErrorCode;
    fn try_from(
        (func_name, table_args): (&str, TableArgs),
    ) -> std::result::Result<Self, Self::Error> {
        let (database_name, table_name) = parse_db_tb_args(&table_args, func_name)?;
        Ok(Self {
            database_name,
            table_name,
        })
    }
}

pub type FuseStatisticsFunc = SimpleArgFuncTemplate<FuseStatistics>;

pub struct FuseStatistics;

#[async_trait::async_trait]
impl SimpleArgFunc for FuseStatistics {
    type Args = FuseStatsArgs;

    fn schema() -> TableSchemaRef {
        FuseStatisticImpl::schema()
    }

    async fn apply(
        ctx: &Arc<dyn TableContext>,
        args: &Self::Args,
        _plan: &DataSourcePlan,
    ) -> Result<DataBlock> {
        let tenant_id = ctx.get_tenant();
        let tbl = ctx
            .get_catalog(CATALOG_DEFAULT)
            .await?
            .get_table(
                &tenant_id,
                args.database_name.as_str(),
                args.table_name.as_str(),
            )
            .await?;

        let tbl = FuseTable::try_from_table(tbl.as_ref())?;
        FuseStatisticImpl::new(tbl).get_statistic().await
    }
}

pub struct FuseStatisticImpl<'a> {
    pub table: &'a FuseTable,
}

impl<'a> FuseStatisticImpl<'a> {
    pub fn new(table: &'a FuseTable) -> Self {
        Self { table }
    }

    #[async_backtrace::framed]
    pub async fn get_statistic(self) -> Result<DataBlock> {
        let snapshot_opt = self.table.read_table_snapshot().await?;
        if let Some(snapshot) = snapshot_opt {
            let table_statistics = self
                .table
                .read_table_snapshot_statistics(Some(&snapshot))
                .await?;
            return self.to_block(&snapshot.summary, &table_statistics);
        }
        Ok(DataBlock::empty_with_schema(Arc::new(
            FuseStatisticImpl::schema().into(),
        )))
    }

    fn to_block(
        &self,
        _summy: &Statistics,
        table_statistics: &Option<Arc<TableSnapshotStatistics>>,
    ) -> Result<DataBlock> {
        let mut col_names = vec![];
        let mut col_ndvs = vec![];
        let mut col_his = vec![];
        if let Some(table_statistics) = table_statistics {
            for (i, n) in table_statistics.column_distinct_values().iter() {
                // Get column name by column id
                let table_filed = self.table.table_info.meta.schema.field_of_column_id(*i)?;
                col_names.push(table_filed.name.clone());
                col_ndvs.push(*n);
                let his_info = table_statistics.histograms.get(i);
                if let Some(his_info) = his_info {
                    let mut his_infos = vec![];
                    for (i, bucket) in his_info.buckets.iter().enumerate() {
                        let min = bucket.lower_bound().to_string()?;
                        let max = bucket.upper_bound().to_string()?;
                        let ndv = bucket.num_distinct();
                        let count = bucket.num_values();
                        let his_info = format!(
                            "[bucket id: {:?}, min: {:?}, max: {:?}, ndv: {:?}, count: {:?}]",
                            i, min, max, ndv, count
                        );
                        his_infos.push(his_info);
                    }
                    col_his.push(his_infos.join(", "));
                } else {
                    col_his.push("".to_string());
                }
            }
        };

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(col_names),
            UInt64Type::from_data(col_ndvs),
            StringType::from_data(col_his),
        ]))
    }

    pub fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("column_name", TableDataType::String),
            TableField::new(
                "distinct_count",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new("histogram", TableDataType::String),
        ])
    }
}
