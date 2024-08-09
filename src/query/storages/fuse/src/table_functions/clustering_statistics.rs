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

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::Value;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;

use crate::io::SegmentsIO;
use crate::sessions::TableContext;
use crate::table_functions::parse_db_tb_args;
use crate::table_functions::string_literal;
use crate::table_functions::SimpleArgFunc;
use crate::table_functions::SimpleArgFuncTemplate;
use crate::FuseTable;

pub struct ClusteringStatsArgs {
    database_name: String,
    table_name: String,
}

impl From<&ClusteringStatsArgs> for TableArgs {
    fn from(args: &ClusteringStatsArgs) -> Self {
        let tbl_args = vec![
            string_literal(args.database_name.as_str()),
            string_literal(args.table_name.as_str()),
        ];
        TableArgs::new_positioned(tbl_args)
    }
}

impl TryFrom<(&str, TableArgs)> for ClusteringStatsArgs {
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

pub type ClusteringStatisticsFunc = SimpleArgFuncTemplate<ClusteringStatistics>;

pub struct ClusteringStatistics;

#[async_trait::async_trait]
impl SimpleArgFunc for ClusteringStatistics {
    type Args = ClusteringStatsArgs;

    fn schema() -> TableSchemaRef {
        ClusteringStatisticsImpl::schema()
    }

    async fn apply(
        ctx: &Arc<dyn TableContext>,
        args: &Self::Args,
        plan: &DataSourcePlan,
    ) -> Result<DataBlock> {
        let tenant_id = ctx.get_tenant();

        let tbl = ctx
            .get_catalog(databend_common_catalog::catalog_kind::CATALOG_DEFAULT)
            .await?
            .get_table(
                &tenant_id,
                args.database_name.as_str(),
                args.table_name.as_str(),
            )
            .await?;
        let tbl = FuseTable::try_from_table(tbl.as_ref())?;

        let Some(cluster_key_id) = tbl.cluster_key_id() else {
            return Err(ErrorCode::UnclusteredTable(format!(
                "Unclustered table '{}.{}'",
                args.database_name, args.table_name,
            )));
        };

        let limit = plan.push_downs.as_ref().and_then(|x| x.limit);
        ClusteringStatisticsImpl::new(ctx.clone(), tbl, limit, cluster_key_id)
            .get_blocks()
            .await
    }
}

pub struct ClusteringStatisticsImpl<'a> {
    pub ctx: Arc<dyn TableContext>,
    pub table: &'a FuseTable,
    pub limit: Option<usize>,
    pub cluster_key_id: u32,
}

impl<'a> ClusteringStatisticsImpl<'a> {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        table: &'a FuseTable,
        limit: Option<usize>,
        cluster_key_id: u32,
    ) -> Self {
        Self {
            ctx,
            table,
            limit,
            cluster_key_id,
        }
    }

    #[async_backtrace::framed]
    pub async fn get_blocks(&self) -> Result<DataBlock> {
        let tbl = self.table;
        let maybe_snapshot = tbl.read_table_snapshot().await?;
        if let Some(snapshot) = maybe_snapshot {
            return self.to_block(snapshot).await;
        }

        Ok(DataBlock::empty_with_schema(Arc::new(
            Self::schema().into(),
        )))
    }

    #[async_backtrace::framed]
    async fn to_block(&self, snapshot: Arc<TableSnapshot>) -> Result<DataBlock> {
        let limit = self.limit.unwrap_or(usize::MAX);
        let len = std::cmp::min(snapshot.summary.block_count as usize, limit);

        let mut segment_name = Vec::with_capacity(len);
        let mut block_name = StringColumnBuilder::with_capacity(len, len);
        let mut max = Vec::with_capacity(len);
        let mut min = Vec::with_capacity(len);
        let mut level = Vec::with_capacity(len);
        let mut pages = Vec::with_capacity(len);

        let segments_io = SegmentsIO::create(
            self.ctx.clone(),
            self.table.operator.clone(),
            self.table.schema(),
        );

        let mut row_num = 0;
        let mut end_flag = false;
        let chunk_size =
            std::cmp::min(self.ctx.get_settings().get_max_threads()? as usize * 4, len).max(1);

        let format_vec = |v: &[Scalar]| -> String {
            format!(
                "[{}]",
                v.iter()
                    .map(|item| format!("{}", item))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        'FOR: for chunk in snapshot.segments.chunks(chunk_size) {
            let segments = segments_io
                .read_segments::<SegmentInfo>(chunk, true)
                .await?;
            for (i, segment) in segments.into_iter().enumerate() {
                let segment = segment?;
                segment_name
                    .extend(std::iter::repeat(chunk[i].0.clone()).take(segment.blocks.len()));

                for block in segment.blocks.iter() {
                    let block = block.as_ref();
                    block_name.put_str(&block.location.0);
                    block_name.commit_row();

                    let cluster_stats = block.cluster_stats.as_ref();
                    let clustered = block
                        .cluster_stats
                        .as_ref()
                        .map_or(false, |v| v.cluster_key_id == self.cluster_key_id);
                    if clustered {
                        // Safe to unwrap
                        let cluster_stats = cluster_stats.unwrap();
                        min.push(Some(format_vec(cluster_stats.min())));
                        max.push(Some(format_vec(cluster_stats.max())));
                        level.push(Some(cluster_stats.level));
                        pages.push(cluster_stats.pages.as_ref().map(|v| format_vec(v)));
                    } else {
                        min.push(None);
                        max.push(None);
                        level.push(None);
                        pages.push(None);
                    }

                    row_num += 1;
                    if row_num >= limit {
                        end_flag = true;
                        break;
                    }
                }

                if end_flag {
                    break 'FOR;
                }
            }
        }

        Ok(DataBlock::new(
            vec![
                BlockEntry::new(
                    DataType::String,
                    Value::Column(StringType::from_data(segment_name)),
                ),
                BlockEntry::new(
                    DataType::String,
                    Value::Column(Column::String(block_name.build())),
                ),
                BlockEntry::new(
                    DataType::String.wrap_nullable(),
                    Value::Column(StringType::from_opt_data(min)),
                ),
                BlockEntry::new(
                    DataType::String.wrap_nullable(),
                    Value::Column(StringType::from_opt_data(max)),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::Int32).wrap_nullable(),
                    Value::Column(Int32Type::from_opt_data(level)),
                ),
                BlockEntry::new(
                    DataType::String.wrap_nullable(),
                    Value::Column(StringType::from_opt_data(pages)),
                ),
            ],
            row_num,
        ))
    }

    pub fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("segment_name", TableDataType::String),
            TableField::new("block_name", TableDataType::String),
            TableField::new("min", TableDataType::String.wrap_nullable()),
            TableField::new("max", TableDataType::String.wrap_nullable()),
            TableField::new(
                "level",
                TableDataType::Number(NumberDataType::Int32).wrap_nullable(),
            ),
            TableField::new("pages", TableDataType::String.wrap_nullable()),
        ])
    }
}
