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

use common_catalog::table::Table;
use common_exception::Result;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::UInt32Type;
use common_expression::types::UInt64Type;
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::DataBlock;
use common_expression::FromData;
use common_expression::Scalar;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchema;
use common_expression::TableSchemaRefExt;
use common_expression::Value;
use futures_util::TryStreamExt;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::TableSnapshot;

use crate::io::MetaReaders;
use crate::io::SegmentsIO;
use crate::io::SnapshotHistoryReader;
use crate::sessions::TableContext;
use crate::FuseTable;

pub struct FuseColumn<'a> {
    pub ctx: Arc<dyn TableContext>,
    pub table: &'a FuseTable,
    pub snapshot_id: Option<String>,
    pub limit: Option<usize>,
}

impl<'a> FuseColumn<'a> {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        table: &'a FuseTable,
        snapshot_id: Option<String>,
        limit: Option<usize>,
    ) -> Self {
        Self {
            ctx,
            table,
            snapshot_id,
            limit,
        }
    }

    #[async_backtrace::framed]
    pub async fn get_blocks(&self) -> Result<DataBlock> {
        let tbl = self.table;
        let snapshot_id = self.snapshot_id.clone();
        let maybe_snapshot = tbl.read_table_snapshot().await?;
        if let Some(snapshot) = maybe_snapshot {
            if let Some(snapshot_id) = snapshot_id {
                // prepare the stream of snapshot
                let snapshot_version = tbl.snapshot_format_version(None).await?;
                let snapshot_location = tbl.meta_location_generator.gen_snapshot_location(
                    &snapshot.snapshot_id,
                    snapshot_version,
                    snapshot.table_version,
                )?;
                let reader = MetaReaders::table_snapshot_reader(tbl.get_operator());
                let mut snapshot_stream = reader.snapshot_history(
                    snapshot_location,
                    snapshot_version,
                    tbl.meta_location_generator().clone(),
                );

                // find the element by snapshot_id in stream
                while let Some((snapshot, _)) = snapshot_stream.try_next().await? {
                    if snapshot.snapshot_id.simple().to_string() == snapshot_id {
                        return self.to_block(snapshot).await;
                    }
                }
            } else {
                return self.to_block(snapshot).await;
            }
        }

        Ok(DataBlock::empty_with_schema(Arc::new(
            Self::schema().into(),
        )))
    }

    #[async_backtrace::framed]
    async fn to_block(&self, snapshot: Arc<TableSnapshot>) -> Result<DataBlock> {
        let limit = self.limit.unwrap_or(usize::MAX);
        let len = std::cmp::min(snapshot.summary.block_count as usize, limit);

        let snapshot_id = snapshot.snapshot_id.simple().to_string().into_bytes();
        let timestamp = snapshot.timestamp.unwrap_or_default().timestamp_micros();
        let mut block_location = StringColumnBuilder::with_capacity(len, len);
        let mut block_size = vec![];
        let mut file_size = vec![];
        let mut row_count = vec![];

        let mut column_name = StringColumnBuilder::with_capacity(len, len);
        let mut column_type = StringColumnBuilder::with_capacity(len, len);
        let mut column_id = vec![];
        let mut block_offset = vec![];
        let mut bytes_compressed = vec![];

        let segments_io = SegmentsIO::create(
            self.ctx.clone(),
            self.table.operator.clone(),
            self.table.schema(),
        );

        let mut row_num = 0;
        let chunk_size =
            std::cmp::min(self.ctx.get_settings().get_max_threads()? as usize * 4, len).max(1);

        let schema = self.table.schema();
        let leaf_fields = schema.leaf_fields();

        let mut end = false;
        'FOR: for chunk in snapshot.segments.chunks(chunk_size) {
            let segments = segments_io
                .read_segments::<SegmentInfo>(chunk, true)
                .await?;
            for segment in segments {
                let segment = segment?;
                for block in segment.blocks.iter() {
                    let block = block.as_ref();

                    for (id, column) in block.col_metas.iter() {
                        if let Some(f) = leaf_fields.iter().find(|f| f.column_id == *id) {
                            block_location.put_slice(block.location.0.as_bytes());
                            block_location.commit_row();
                            block_size.push(block.block_size);
                            file_size.push(block.file_size);
                            row_count.push(column.total_rows() as u64);

                            column_name.put_slice(f.name.as_bytes());
                            column_name.commit_row();

                            column_type.put_slice(f.data_type.to_string().as_bytes());
                            column_type.commit_row();

                            column_id.push(*id);

                            let (offset, length) = column.offset_length();
                            block_offset.push(offset);
                            bytes_compressed.push(length);

                            row_num += 1;

                            if row_num >= limit {
                                end = true;
                                break;
                            }
                        }
                    }

                    if end {
                        break 'FOR;
                    }
                }
            }
        }

        Ok(DataBlock::new(
            vec![
                BlockEntry::new(
                    DataType::String,
                    Value::Scalar(Scalar::String(snapshot_id.to_vec())),
                ),
                BlockEntry::new(
                    DataType::Timestamp,
                    Value::Scalar(Scalar::Timestamp(timestamp)),
                ),
                BlockEntry::new(
                    DataType::String,
                    Value::Column(Column::String(block_location.build())),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Column(UInt64Type::from_data(block_size)),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Column(UInt64Type::from_data(file_size)),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Column(UInt64Type::from_data(row_count)),
                ),
                BlockEntry::new(
                    DataType::String,
                    Value::Column(Column::String(column_name.build())),
                ),
                BlockEntry::new(
                    DataType::String,
                    Value::Column(Column::String(column_type.build())),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt32),
                    Value::Column(UInt32Type::from_data(column_id)),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Column(UInt64Type::from_data(block_offset)),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Column(UInt64Type::from_data(bytes_compressed)),
                ),
            ],
            row_num,
        ))
    }

    pub fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("snapshot_id", TableDataType::String),
            TableField::new("timestamp", TableDataType::Timestamp),
            TableField::new("block_location", TableDataType::String),
            TableField::new("block_size", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("file_size", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("row_count", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("column_name", TableDataType::String),
            TableField::new("column_type", TableDataType::String),
            TableField::new("column_id", TableDataType::Number(NumberDataType::UInt32)),
            TableField::new(
                "block_offset",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "bytes_compressed",
                TableDataType::Number(NumberDataType::UInt64),
            ),
        ])
    }
}
