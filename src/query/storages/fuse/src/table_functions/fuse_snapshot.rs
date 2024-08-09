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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::number::UInt64Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_storages_common_table_meta::meta::TableSnapshotLite;
use log::info;

use crate::io::SnapshotsIO;
use crate::io::TableMetaLocationGenerator;
use crate::sessions::TableContext;
use crate::table_functions::parse_db_tb_args;
use crate::table_functions::string_literal;
use crate::table_functions::SimpleTableFunc;
use crate::FuseTable;

pub struct FuseSnapshotArgs {
    database_name: String,
    table_name: String,
}

pub struct FuseSnapshotFunc {
    args: FuseSnapshotArgs,
}

impl From<&FuseSnapshotArgs> for TableArgs {
    fn from(args: &FuseSnapshotArgs) -> Self {
        TableArgs::new_positioned(vec![
            string_literal(args.database_name.as_str()),
            string_literal(args.table_name.as_str()),
        ])
    }
}

impl FuseSnapshotFunc {
    fn to_block(
        &self,
        location_generator: &TableMetaLocationGenerator,
        snapshots: &[TableSnapshotLite],
        latest_snapshot_version: u64,
    ) -> Result<DataBlock> {
        let len = snapshots.len();
        let mut snapshot_ids: Vec<String> = Vec::with_capacity(len);
        let mut snapshot_locations: Vec<String> = Vec::with_capacity(len);
        let mut prev_snapshot_ids: Vec<Option<String>> = Vec::with_capacity(len);
        let mut format_versions: Vec<u64> = Vec::with_capacity(len);
        let mut segment_count: Vec<u64> = Vec::with_capacity(len);
        let mut block_count: Vec<u64> = Vec::with_capacity(len);
        let mut row_count: Vec<u64> = Vec::with_capacity(len);
        let mut compressed: Vec<u64> = Vec::with_capacity(len);
        let mut uncompressed: Vec<u64> = Vec::with_capacity(len);
        let mut index_size: Vec<u64> = Vec::with_capacity(len);
        let mut timestamps: Vec<Option<i64>> = Vec::with_capacity(len);
        let mut current_snapshot_version = latest_snapshot_version;
        for s in snapshots {
            snapshot_ids.push(s.snapshot_id.simple().to_string());
            snapshot_locations.push(
                location_generator
                    .snapshot_location_from_uuid(&s.snapshot_id, current_snapshot_version)?,
            );
            let (id, ver) = s
                .prev_snapshot_id
                .map_or((None, 0), |(id, v)| (Some(id.simple().to_string()), v));
            prev_snapshot_ids.push(id);
            format_versions.push(s.format_version);
            segment_count.push(s.segment_count);
            block_count.push(s.block_count);
            row_count.push(s.row_count);
            compressed.push(s.compressed_byte_size);
            uncompressed.push(s.uncompressed_byte_size);
            index_size.push(s.index_size);
            timestamps.push(s.timestamp.map(|dt| (dt.timestamp_micros())));
            current_snapshot_version = ver;
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(snapshot_ids),
            StringType::from_data(snapshot_locations),
            UInt64Type::from_data(format_versions),
            StringType::from_opt_data(prev_snapshot_ids),
            UInt64Type::from_data(segment_count),
            UInt64Type::from_data(block_count),
            UInt64Type::from_data(row_count),
            UInt64Type::from_data(uncompressed),
            UInt64Type::from_data(compressed),
            UInt64Type::from_data(index_size),
            TimestampType::from_opt_data(timestamps),
        ]))
    }
}

#[async_trait::async_trait]
impl SimpleTableFunc for FuseSnapshotFunc {
    fn table_args(&self) -> Option<TableArgs> {
        Some((&self.args).into())
    }

    fn schema(&self) -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("snapshot_id", TableDataType::String),
            TableField::new("snapshot_location", TableDataType::String),
            TableField::new(
                "format_version",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "previous_snapshot_id",
                TableDataType::String.wrap_nullable(),
            ),
            TableField::new(
                "segment_count",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new("block_count", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("row_count", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new(
                "bytes_uncompressed",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "bytes_compressed",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new("index_size", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("timestamp", TableDataType::Timestamp.wrap_nullable()),
        ])
    }

    async fn apply(
        &self,
        ctx: &Arc<dyn TableContext>,
        plan: &DataSourcePlan,
    ) -> Result<Option<DataBlock>> {
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

        let table = FuseTable::try_from_table(tbl.as_ref()).map_err(|_| {
            ErrorCode::StorageOther("Invalid table engine, only FUSE table supports fuse_snapshot")
        })?;

        let meta_location_generator = table.meta_location_generator.clone();
        let snapshot_location = table.snapshot_loc().await?;
        let snapshot = table.read_table_snapshot().await?;
        if let Some(snapshot_location) = snapshot_location {
            let snapshot_version =
                TableMetaLocationGenerator::snapshot_version(snapshot_location.as_str());
            let snapshots_io = SnapshotsIO::create(ctx.clone(), table.operator.clone());

            let limit = plan.push_downs.as_ref().and_then(|v| v.limit);
            let snapshot_lite = if limit.is_none() {
                info!("getting snapshots, using parallel strategy");
                // Use SnapshotsIO::read_snapshot_lites only if limit is None
                //
                // SnapshotsIO::read_snapshot lists snapshots from object storage, taking limit into
                // account, BEFORE the snapshots are chained, so there might be the case that although
                // there are more than limited number of snapshots could be chained, the number of
                // snapshots returned is lesser.
                let (chained_snapshot_lites, _) = snapshots_io
                    .read_snapshot_lites_ext(
                        snapshot_location,
                        snapshot.and_then(|s| s.timestamp),
                        &|_| {},
                    )
                    .await?;
                Ok::<_, ErrorCode>(chained_snapshot_lites)
            } else {
                // SnapshotsIO::read_chained_snapshot_lists traverses the history of snapshot sequentially, by using the
                // TableSnapshot::prev_snapshot_id, which guarantees that the number of snapshot
                // returned is as expected
                let snapshot_lites = snapshots_io
                    .read_chained_snapshot_lites(
                        table.operator.clone(),
                        meta_location_generator.clone(),
                        snapshot_location,
                        limit,
                    )
                    .await?;
                info!(
                    "getting snapshots in chained mod, limit: {:?}, number of snapshot_lite found: {}",
                    limit,
                    snapshot_lites.len()
                );
                Ok(snapshot_lites)
            }?;

            info!("got {} snapshots", snapshot_lite.len());
            return Ok(Some(self.to_block(
                &meta_location_generator,
                &snapshot_lite,
                snapshot_version,
            )?));
        }
        Ok(Some(DataBlock::empty_with_schema(Arc::new(
            self.schema().into(),
        ))))
    }

    fn create(func_name: &str, table_args: TableArgs) -> Result<Self>
    where Self: Sized {
        let (arg_database_name, arg_table_name) = parse_db_tb_args(&table_args, func_name)?;
        Ok(Self {
            args: FuseSnapshotArgs {
                database_name: arg_database_name,
                table_name: arg_table_name,
            },
        })
    }
}
