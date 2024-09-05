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

use std::any::Any;
use std::sync::Arc;

use databend_common_base::base::uuid::Uuid;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::decimal::DecimalScalar;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalDataType;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::BlockEntry;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::ColumnId;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::Value;
use databend_common_expression::ORIGIN_BLOCK_ID_COLUMN_ID;
use databend_common_expression::ORIGIN_BLOCK_ROW_NUM_COLUMN_ID;
use databend_common_expression::ORIGIN_VERSION_COLUMN_ID;
use databend_common_expression::ROW_VERSION_COLUMN_ID;
use databend_storages_common_table_meta::meta::try_extract_uuid_str_from_path;

use crate::plan::PartInfo;
use crate::plan::PartInfoPtr;
use crate::plan::Partitions;

#[derive(serde::Serialize, serde::Deserialize, PartialEq)]
pub struct StreamTablePart {
    inner: Partitions,
    base_block_ids: Scalar,
}

#[typetag::serde(name = "stream")]
impl PartInfo for StreamTablePart {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        info.as_any()
            .downcast_ref::<StreamTablePart>()
            .is_some_and(|other| self == other)
    }

    fn hash(&self) -> u64 {
        0
    }
}

impl StreamTablePart {
    pub fn create(inner: Partitions, base_block_ids: Scalar) -> Arc<Box<dyn PartInfo>> {
        Arc::new(Box::new(StreamTablePart {
            inner,
            base_block_ids,
        }))
    }

    pub fn from_part(info: &PartInfoPtr) -> Option<&StreamTablePart> {
        info.as_any().downcast_ref::<StreamTablePart>()
    }

    pub fn inner(&self) -> Partitions {
        self.inner.clone()
    }

    pub fn base_block_ids(&self) -> Scalar {
        self.base_block_ids.clone()
    }
}

// meta data for generate internal columns
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq)]
pub struct StreamColumnMeta {
    pub block_id: i128,
    pub inner: Option<BlockMetaInfoPtr>,
}

#[typetag::serde(name = "stream_column_meta")]
impl BlockMetaInfo for StreamColumnMeta {
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        StreamColumnMeta::downcast_ref_from(info).is_some_and(|other| self == other)
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

impl StreamColumnMeta {
    pub fn from_meta(info: &BlockMetaInfoPtr) -> Result<&StreamColumnMeta> {
        StreamColumnMeta::downcast_ref_from(info).ok_or_else(|| {
            ErrorCode::Internal("Cannot downcast from BlockMetaInfo to StreamColumnMeta.")
        })
    }

    pub fn build_origin_block_id(&self) -> Value<AnyType> {
        Value::Scalar(Scalar::Decimal(DecimalScalar::Decimal128(
            self.block_id,
            DecimalSize {
                precision: 38,
                scale: 0,
            },
        )))
    }
}

pub fn build_origin_block_row_num(num_rows: usize) -> BlockEntry {
    let mut row_ids = Vec::with_capacity(num_rows);
    for i in 0..num_rows {
        row_ids.push(i as u64);
    }
    let column = Value::Column(UInt64Type::from_data(row_ids));

    BlockEntry::new(
        DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt64))),
        column.wrap_nullable(None),
    )
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum StreamColumnType {
    OriginVersion,
    OriginBlockId,
    OriginRowNum,
    RowVersion,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct StreamColumn {
    pub column_name: String,
    pub column_type: StreamColumnType,
}

impl StreamColumn {
    pub fn new(name: &str, column_type: StreamColumnType) -> Self {
        Self {
            column_name: name.to_string(),
            column_type,
        }
    }

    pub fn table_field(&self) -> TableField {
        TableField::new_from_column_id(&self.column_name, self.table_data_type(), self.column_id())
    }

    pub fn column_type(&self) -> &StreamColumnType {
        &self.column_type
    }

    pub fn table_data_type(&self) -> TableDataType {
        match self.column_type {
            StreamColumnType::OriginVersion => {
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64)))
            }
            StreamColumnType::OriginBlockId => TableDataType::Nullable(Box::new(
                TableDataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
                    precision: 38,
                    scale: 0,
                })),
            )),
            StreamColumnType::OriginRowNum => {
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64)))
            }
            StreamColumnType::RowVersion => TableDataType::Number(NumberDataType::UInt64),
        }
    }

    pub fn column_name(&self) -> &str {
        &self.column_name
    }

    pub fn data_type(&self) -> DataType {
        let t = &self.table_data_type();
        t.into()
    }

    pub fn column_id(&self) -> ColumnId {
        match &self.column_type {
            StreamColumnType::OriginVersion => ORIGIN_VERSION_COLUMN_ID,
            StreamColumnType::OriginBlockId => ORIGIN_BLOCK_ID_COLUMN_ID,
            StreamColumnType::OriginRowNum => ORIGIN_BLOCK_ROW_NUM_COLUMN_ID,
            StreamColumnType::RowVersion => ROW_VERSION_COLUMN_ID,
        }
    }

    pub fn generate_column_values(&self, meta: &StreamColumnMeta, num_rows: usize) -> BlockEntry {
        match &self.column_type {
            StreamColumnType::OriginVersion | StreamColumnType::RowVersion => unreachable!(),
            StreamColumnType::OriginBlockId => BlockEntry::new(
                DataType::Nullable(Box::new(DataType::Decimal(DecimalDataType::Decimal128(
                    DecimalSize {
                        precision: 38,
                        scale: 0,
                    },
                )))),
                meta.build_origin_block_id(),
            ),
            StreamColumnType::OriginRowNum => build_origin_block_row_num(num_rows),
        }
    }
}

pub fn block_id_from_location(path: &str) -> Result<i128> {
    let uuid = try_extract_uuid_str_from_path(path)
        .map_err(|e| e.add_message(format!("invalid block path {}", path)))?;
    let block_id = Uuid::parse_str(uuid).map_err(|e| e.to_string())?.as_u128();
    Ok(block_id as i128)
}

pub fn gen_mutation_stream_meta(
    inner: Option<BlockMetaInfoPtr>,
    path: &str,
) -> Result<StreamColumnMeta> {
    let block_id = block_id_from_location(path)?;
    Ok(StreamColumnMeta { block_id, inner })
}
