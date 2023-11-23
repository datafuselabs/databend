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

use common_exception::Result;
use common_expression::types::number::UInt64Type;
use common_expression::types::NumberDataType;
use common_expression::types::StringType;
use common_expression::types::TimestampType;
use common_expression::DataBlock;
use common_expression::FromData;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use common_expression::TableSchemaRefExt;
use storages_common_table_meta::meta::TableSnapshotLite;

use crate::io::SnapshotsIO;
use crate::io::TableMetaLocationGenerator;
use crate::sessions::TableContext;
use crate::FuseTable;

pub struct FuseSnapshot<'a> {
    pub ctx: Arc<dyn TableContext>,
    pub table: &'a FuseTable,
}

impl<'a> FuseSnapshot<'a> {
    pub fn new(ctx: Arc<dyn TableContext>, table: &'a FuseTable) -> Self {
        Self { ctx, table }
    }

    #[async_backtrace::framed]
    pub async fn get_snapshots(self, limit: Option<usize>) -> Result<DataBlock> {
        let meta_location_generator = self.table.meta_location_generator.clone();
        let snapshot_location = self.table.snapshot_loc().await?;
        let snapshot = self.table.read_table_snapshot().await?;
        if let Some(snapshot_location) = snapshot_location {
            let snapshot_version =
                TableMetaLocationGenerator::snapshot_version(snapshot_location.as_str());
            let snapshots_io = SnapshotsIO::create(self.ctx.clone(), self.table.operator.clone());
            let snapshot_lite = if limit.is_none() {
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
                Ok(chained_snapshot_lites)
            } else {
                // SnapshotsIO::read_chained_snapshot_lists traverses the history of snapshot sequentially, by using the
                // TableSnapshot::prev_snapshot_id, which guarantees that the number of snapshot
                // returned is as expected
                snapshots_io
                    .read_chained_snapshot_lites(
                        meta_location_generator.clone(),
                        snapshot_location,
                        limit,
                    )
                    .await
            }?;

            return self.to_block(&meta_location_generator, &snapshot_lite, snapshot_version);
        }
        Ok(DataBlock::empty_with_schema(Arc::new(
            FuseSnapshot::schema().into(),
        )))
    }

    fn to_block(
        &self,
        location_generator: &TableMetaLocationGenerator,
        snapshots: &[TableSnapshotLite],
        latest_snapshot_version: u64,
    ) -> Result<DataBlock> {
        let len = snapshots.len();
        let mut snapshot_ids: Vec<Vec<u8>> = Vec::with_capacity(len);
        let mut snapshot_locations: Vec<Vec<u8>> = Vec::with_capacity(len);
        let mut prev_snapshot_ids: Vec<Option<Vec<u8>>> = Vec::with_capacity(len);
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
            snapshot_ids.push(s.snapshot_id.simple().to_string().into_bytes());
            snapshot_locations.push(
                location_generator
                    .gen_snapshot_location(
                        &s.snapshot_id,
                        current_snapshot_version,
                        s.table_version,
                    )?
                    .into_bytes(),
            );
            let (id, ver, _table_version) =
                s.prev_snapshot_id
                    .map_or((None, 0, None), |(id, v, table_version)| {
                        (Some(id.simple().to_string().into_bytes()), v, table_version)
                    });
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

    pub fn schema() -> Arc<TableSchema> {
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
}
