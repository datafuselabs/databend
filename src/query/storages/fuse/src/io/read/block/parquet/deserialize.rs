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

use std::collections::HashMap;

use arrow_array::RecordBatch;
use databend_common_expression::converts::arrow::table_schema_to_arrow_schema;
use databend_common_expression::types::DecimalDataType;
use databend_common_expression::ColumnId;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_storages_common_table_meta::meta::is_possible_non_standard_decimal_block;
use databend_storages_common_table_meta::meta::Compression;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;
use parquet::arrow::arrow_to_parquet_schema;
use parquet::arrow::parquet_to_arrow_field_levels;
use parquet::arrow::ProjectionMask;
use parquet::basic::Compression as ParquetCompression;

use crate::io::read::block::block_reader_merge_io::DataItem;
use crate::io::read::block::parquet::adapter::RowGroupImplBuilder;
use crate::io::read::block::parquet::arrow_parquet::arrow_to_parquet_schema_fix;

/// The returned record batch contains all deserialized columns in the same nested structure as the original schema.
pub fn column_chunks_to_record_batch(
    original_schema: &TableSchema,
    num_rows: usize,
    column_chunks: &HashMap<ColumnId, DataItem>,
    compression: &Compression,
    block_path: &str,
) -> databend_common_exception::Result<RecordBatch> {
    let use_v1 = !is_possible_non_standard_decimal_block(block_path)?
        && original_schema.fields.iter().any(|f| {
            matches!(
                f.data_type.remove_nullable(),
                TableDataType::Decimal(DecimalDataType::Decimal256(_))
            )
        });
    log::info!("deserialize {block_path} with use_v1: {use_v1}");
    let arrow_schema = table_schema_to_arrow_schema(original_schema);
    let parquet_schema = if !use_v1 {
        arrow_to_parquet_schema(&arrow_schema)?
    } else {
        arrow_to_parquet_schema_fix(&arrow_schema)?
    };

    let column_id_to_dfs_id = original_schema
        .to_leaf_column_ids()
        .iter()
        .enumerate()
        .map(|(dfs_id, column_id)| (*column_id, dfs_id))
        .collect::<HashMap<_, _>>();
    let mut projection_mask = Vec::with_capacity(column_chunks.len());
    let mut builder = RowGroupImplBuilder::new(
        num_rows,
        &parquet_schema,
        ParquetCompression::from(*compression),
    );
    for (column_id, data_item) in column_chunks.iter() {
        match data_item {
            DataItem::RawData(bytes) => {
                let dfs_id = column_id_to_dfs_id.get(column_id).cloned().unwrap();
                projection_mask.push(dfs_id);
                builder.add_column_chunk(dfs_id, bytes.clone());
            }
            DataItem::ColumnArray(_) => {}
        }
    }
    let row_group = Box::new(builder.build());
    let field_levels = parquet_to_arrow_field_levels(
        &parquet_schema,
        ProjectionMask::leaves(&parquet_schema, projection_mask),
        Some(arrow_schema.fields()),
    )?;
    let mut record_reader = ParquetRecordBatchReader::try_new_with_row_groups(
        &field_levels,
        row_group.as_ref(),
        num_rows,
        None,
    )?;
    let record = record_reader.next().unwrap()?;
    assert!(record_reader.next().is_none());
    Ok(record)
}
