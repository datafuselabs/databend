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

use databend_common_arrow::arrow::bitmap::MutableBitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::number::F32;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalDataType;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::Float32Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::BlockEntry;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::Value;
use databend_common_expression::BASE_BLOCK_IDS_COLUMN_ID;
use databend_common_expression::BASE_ROW_ID_COLUMN_ID;
use databend_common_expression::BLOCK_NAME_COLUMN_ID;
use databend_common_expression::ROW_ID_COLUMN_ID;
use databend_common_expression::SEARCH_MATCHED_COLUMN_ID;
use databend_common_expression::SEARCH_SCORE_COLUMN_ID;
use databend_common_expression::SEGMENT_NAME_COLUMN_ID;
use databend_common_expression::SNAPSHOT_NAME_COLUMN_ID;
use databend_storages_common_table_meta::meta::try_extract_uuid_str_from_path;
use databend_storages_common_table_meta::meta::NUM_BLOCK_ID_BITS;

// Segment and Block id Bits when generate internal column `_row_id`
// Assumes that the max block count of a segment is 2 ^ NUM_BLOCK_ID_BITS
// Since `DEFAULT_BLOCK_PER_SEGMENT` is 1000, so `block_id` 10 bits is enough.
// for compact_segment, we will get 2*thresholds-1 blocks in one segment at most.
pub const NUM_SEGMENT_ID_BITS: usize = 22;
pub const NUM_ROW_ID_PREFIX_BITS: usize = NUM_BLOCK_ID_BITS + NUM_SEGMENT_ID_BITS;

#[inline(always)]
pub fn compute_row_id_prefix(seg_id: u64, block_id: u64) -> u64 {
    // `seg_id` is the offset in the segment list in the snapshot meta.
    // The bigger the `seg_id`, the older the segment.
    // So, to make the row id monotonic increasing, we need to reverse the `seg_id`.
    let seg_id = (!seg_id) & ((1 << NUM_SEGMENT_ID_BITS) - 1);
    let block_id = block_id & ((1 << NUM_BLOCK_ID_BITS) - 1);
    ((seg_id << NUM_BLOCK_ID_BITS) | block_id) & ((1 << NUM_ROW_ID_PREFIX_BITS) - 1)
}

#[inline(always)]
pub fn compute_row_id(prefix: u64, idx: u64) -> u64 {
    (prefix << (64 - NUM_ROW_ID_PREFIX_BITS)) | (idx & ((1 << (64 - NUM_ROW_ID_PREFIX_BITS)) - 1))
}

#[inline(always)]
pub fn split_row_id(id: u64) -> (u64, u64) {
    let prefix = id >> (64 - NUM_ROW_ID_PREFIX_BITS);
    let idx = id & ((1 << (64 - NUM_ROW_ID_PREFIX_BITS)) - 1);
    (prefix, idx)
}

pub fn split_prefix(id: u64) -> (u64, u64) {
    let block_id = id & ((1 << NUM_BLOCK_ID_BITS) - 1);

    let seg_id = id >> NUM_BLOCK_ID_BITS;
    let seg_id = (!seg_id) & ((1 << NUM_SEGMENT_ID_BITS) - 1);
    (seg_id, block_id)
}

#[inline(always)]
pub fn block_id_in_segment(block_num: usize, block_idx: usize) -> usize {
    block_num - block_idx - 1
}

#[inline(always)]
pub fn block_idx_in_segment(block_num: usize, block_id: usize) -> usize {
    block_num - (block_id + 1)
}

// meta data for generate internal columns
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct InternalColumnMeta {
    pub segment_idx: usize,
    pub block_id: usize,
    pub block_location: String,
    pub segment_location: String,
    pub snapshot_location: Option<String>,
    /// The row offsets in the block.
    pub offsets: Option<Vec<usize>>,
    pub base_block_ids: Option<Scalar>,
    pub inner: Option<BlockMetaInfoPtr>,
    // The search matched rows and optional scores in the block.
    pub matched_rows: Option<Vec<(usize, Option<F32>)>>,
}

#[typetag::serde(name = "internal_column_meta")]
impl BlockMetaInfo for InternalColumnMeta {
    fn equals(&self, info: &Box<dyn BlockMetaInfo>) -> bool {
        InternalColumnMeta::downcast_ref_from(info).is_some_and(|other| self == other)
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        Box::new(self.clone())
    }
}

impl InternalColumnMeta {
    pub fn from_meta(info: &BlockMetaInfoPtr) -> Result<&InternalColumnMeta> {
        InternalColumnMeta::downcast_ref_from(info).ok_or_else(|| {
            ErrorCode::Internal("Cannot downcast from BlockMetaInfo to InternalColumnMeta.")
        })
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum InternalColumnType {
    RowId,
    BlockName,
    SegmentName,
    SnapshotName,

    // stream columns
    BaseRowId,
    BaseBlockIds,

    // search columns
    SearchMatched,
    SearchScore,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct InternalColumn {
    pub column_name: String,
    pub column_type: InternalColumnType,
}

impl InternalColumn {
    pub fn new(name: &str, column_type: InternalColumnType) -> Self {
        InternalColumn {
            column_name: name.to_string(),
            column_type,
        }
    }

    pub fn column_type(&self) -> &InternalColumnType {
        &self.column_type
    }

    pub fn table_data_type(&self) -> TableDataType {
        match &self.column_type {
            InternalColumnType::RowId => TableDataType::Number(NumberDataType::UInt64),
            InternalColumnType::BlockName => TableDataType::String,
            InternalColumnType::SegmentName => TableDataType::String,
            InternalColumnType::SnapshotName => TableDataType::String,
            InternalColumnType::BaseRowId => TableDataType::String,
            InternalColumnType::BaseBlockIds => TableDataType::Array(Box::new(
                TableDataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
                    precision: 38,
                    scale: 0,
                })),
            )),
            InternalColumnType::SearchMatched => TableDataType::Boolean,
            InternalColumnType::SearchScore => TableDataType::Number(NumberDataType::Float32),
        }
    }

    pub fn data_type(&self) -> DataType {
        let t = &self.table_data_type();
        t.into()
    }

    pub fn column_name(&self) -> &String {
        &self.column_name
    }

    pub fn column_id(&self) -> ColumnId {
        match &self.column_type {
            InternalColumnType::RowId => ROW_ID_COLUMN_ID,
            InternalColumnType::BlockName => BLOCK_NAME_COLUMN_ID,
            InternalColumnType::SegmentName => SEGMENT_NAME_COLUMN_ID,
            InternalColumnType::SnapshotName => SNAPSHOT_NAME_COLUMN_ID,
            InternalColumnType::BaseRowId => BASE_ROW_ID_COLUMN_ID,
            InternalColumnType::BaseBlockIds => BASE_BLOCK_IDS_COLUMN_ID,
            InternalColumnType::SearchMatched => SEARCH_MATCHED_COLUMN_ID,
            InternalColumnType::SearchScore => SEARCH_SCORE_COLUMN_ID,
        }
    }

    pub fn generate_column_values(&self, meta: &InternalColumnMeta, num_rows: usize) -> BlockEntry {
        match &self.column_type {
            InternalColumnType::RowId => {
                let block_id = meta.block_id as u64;
                let seg_id = meta.segment_idx as u64;
                let high_32bit = compute_row_id_prefix(seg_id, block_id);
                let mut row_ids = Vec::with_capacity(num_rows);
                if let Some(offsets) = &meta.offsets {
                    for i in offsets {
                        let row_id = compute_row_id(high_32bit, *i as u64);
                        row_ids.push(row_id);
                    }
                } else {
                    for i in 0..num_rows {
                        let row_id = compute_row_id(high_32bit, i as u64);
                        row_ids.push(row_id);
                    }
                }

                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Column(UInt64Type::from_data(row_ids)),
                )
            }
            InternalColumnType::BlockName => {
                let mut builder = StringColumnBuilder::with_capacity(1, meta.block_location.len());
                builder.put_str(&meta.block_location);
                builder.commit_row();
                BlockEntry::new(
                    DataType::String,
                    Value::Scalar(Scalar::String(builder.build_scalar())),
                )
            }
            InternalColumnType::SegmentName => {
                let mut builder =
                    StringColumnBuilder::with_capacity(1, meta.segment_location.len());
                builder.put_str(&meta.segment_location);
                builder.commit_row();
                BlockEntry::new(
                    DataType::String,
                    Value::Scalar(Scalar::String(builder.build_scalar())),
                )
            }
            InternalColumnType::SnapshotName => {
                let mut builder = StringColumnBuilder::with_capacity(
                    1,
                    meta.snapshot_location
                        .clone()
                        .unwrap_or("".to_string())
                        .len(),
                );
                builder.put_str(&meta.snapshot_location.clone().unwrap_or("".to_string()));
                builder.commit_row();
                BlockEntry::new(
                    DataType::String,
                    Value::Scalar(Scalar::String(builder.build_scalar())),
                )
            }
            InternalColumnType::BaseRowId => {
                let uuid =
                    try_extract_uuid_str_from_path(&meta.block_location).unwrap_or_else(|e| {
                        panic!(
                            "Internal error: block_location {} should be a valid table object key: {}",
                            &meta.block_location, e
                        )
                    });
                let mut row_ids = Vec::with_capacity(num_rows);
                if let Some(offsets) = &meta.offsets {
                    for i in offsets {
                        let row_id = format!("{}{:06x}", uuid, *i);
                        row_ids.push(row_id);
                    }
                } else {
                    for i in 0..num_rows {
                        let row_id = format!("{}{:06x}", uuid, i);
                        row_ids.push(row_id);
                    }
                }
                BlockEntry::new(
                    DataType::String,
                    Value::Column(StringType::from_data(row_ids)),
                )
            }
            InternalColumnType::BaseBlockIds => {
                assert!(meta.base_block_ids.is_some());
                BlockEntry::new(
                    DataType::Array(Box::new(DataType::Decimal(DecimalDataType::Decimal128(
                        DecimalSize {
                            precision: 38,
                            scale: 0,
                        },
                    )))),
                    Value::Scalar(meta.base_block_ids.clone().unwrap()),
                )
            }
            InternalColumnType::SearchMatched => {
                assert!(meta.matched_rows.is_some());
                let matched_rows = meta.matched_rows.as_ref().unwrap();

                let mut bitmap = MutableBitmap::from_len_zeroed(num_rows);
                for (idx, _) in matched_rows.iter() {
                    bitmap.set(*idx, true);
                }
                BlockEntry::new(
                    DataType::Boolean,
                    Value::Column(Column::Boolean(bitmap.into())),
                )
            }
            InternalColumnType::SearchScore => {
                assert!(meta.matched_rows.is_some());
                let matched_rows = meta.matched_rows.as_ref().unwrap();

                let mut scores = vec![F32::from(0_f32); num_rows];
                for (idx, score) in matched_rows.iter() {
                    if let Some(val) = scores.get_mut(*idx) {
                        assert!(score.is_some());
                        *val = F32::from(*score.unwrap());
                    }
                }
                BlockEntry::new(
                    DataType::Number(NumberDataType::Float32),
                    Value::Column(Float32Type::from_data(scores)),
                )
            }
        }
    }
}
