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

use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::Value;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;

use crate::io::SegmentsIO;
use crate::sessions::TableContext;
use crate::table_functions::function_template::TableMetaFunc;
use crate::table_functions::TableMetaFuncTemplate;
use crate::FuseTable;

pub struct FuseBlock;
pub type FuseBlockFunc = TableMetaFuncTemplate<FuseBlock>;

#[async_trait::async_trait]
impl TableMetaFunc for FuseBlock {
    fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("snapshot_id", TableDataType::String),
            TableField::new("timestamp", TableDataType::Timestamp),
            TableField::new("block_location", TableDataType::String),
            TableField::new("block_size", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("file_size", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("row_count", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new(
                "bloom_filter_location",
                TableDataType::String.wrap_nullable(),
            ),
            TableField::new(
                "bloom_filter_size",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "inverted_index_size",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
        ])
    }

    async fn apply(
        ctx: &Arc<dyn TableContext>,
        tbl: &FuseTable,
        snapshot: Arc<TableSnapshot>,
        limit: Option<usize>,
    ) -> Result<DataBlock> {
        let limit = limit.unwrap_or(usize::MAX);
        let len = std::cmp::min(snapshot.summary.block_count as usize, limit);

        let snapshot_id = snapshot.snapshot_id.simple().to_string();
        let timestamp = snapshot.timestamp.unwrap_or_default().timestamp_micros();
        let mut block_location = StringColumnBuilder::with_capacity(len, len);
        let mut block_size = Vec::with_capacity(len);
        let mut file_size = Vec::with_capacity(len);
        let mut row_count = Vec::with_capacity(len);
        let mut bloom_filter_location = vec![];
        let mut bloom_filter_size = Vec::with_capacity(len);
        let mut inverted_index_size = Vec::with_capacity(len);

        let segments_io = SegmentsIO::create(ctx.clone(), tbl.operator.clone(), tbl.schema());

        let mut row_num = 0;
        let mut end_flag = false;
        let chunk_size =
            std::cmp::min(ctx.get_settings().get_max_threads()? as usize * 4, len).max(1);
        'FOR: for chunk in snapshot.segments.chunks(chunk_size) {
            let segments = segments_io
                .read_segments::<SegmentInfo>(chunk, true)
                .await?;
            for segment in segments {
                let segment = segment?;

                for block in segment.blocks.iter() {
                    let block = block.as_ref();
                    block_location.put_str(&block.location.0);
                    block_location.commit_row();
                    block_size.push(block.block_size);
                    file_size.push(block.file_size);
                    row_count.push(block.row_count);
                    bloom_filter_location.push(
                        block
                            .bloom_filter_index_location
                            .as_ref()
                            .map(|s| s.0.clone()),
                    );
                    bloom_filter_size.push(block.bloom_filter_index_size);
                    inverted_index_size.push(block.inverted_index_size);

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
                BlockEntry::new(DataType::String, Value::Scalar(Scalar::String(snapshot_id))),
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
                    DataType::String.wrap_nullable(),
                    Value::Column(StringType::from_opt_data(bloom_filter_location)),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Column(UInt64Type::from_data(bloom_filter_size)),
                ),
                BlockEntry::new(
                    DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt64))),
                    Value::Column(UInt64Type::from_opt_data(inverted_index_size)),
                ),
            ],
            row_num,
        ))
    }
}
