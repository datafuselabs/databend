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

use std::io::Cursor;
use std::io::Read;
use std::sync::Arc;

use arrow::array::Array;
use arrow::array::AsArray;
use arrow::array::Date32Array;
use arrow::array::Decimal128Array;
use arrow::array::Decimal256Array;
use arrow::array::Float32Array;
use arrow::array::Float64Array;
use arrow::array::Int16Array;
use arrow::array::Int32Array;
use arrow::array::Int64Array;
use arrow::array::Int8Array;
use arrow::array::LargeStringArray;
use arrow::array::RecordBatch;
use arrow::array::StringArray;
use arrow::array::StructArray;
use arrow::array::TimestampMicrosecondArray;
use arrow::array::TimestampNanosecondArray;
use arrow::array::UInt16Array;
use arrow::array::UInt32Array;
use arrow::array::UInt64Array;
use arrow::array::UInt8Array;
use arrow::buffer::NullBuffer;
use arrow::datatypes::DataType as ArrowDataType;
use arrow::datatypes::Field;
use arrow::datatypes::Fields;
use arrow::datatypes::Schema;
use arrow::datatypes::TimeUnit;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::converts::arrow::table_type_to_arrow_type;
use databend_common_expression::types::DecimalDataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use serde::Deserialize;
use serde::Serialize;

use super::super::v2;
use super::super::v3;
use crate::meta::format::compress;
use crate::meta::format::decode_segment_header;
use crate::meta::format::encode;
use crate::meta::format::read_and_deserialize;
use crate::meta::format::MetaCompression;
use crate::meta::format::SegmentHeader;
use crate::meta::format::MAX_SEGMENT_BLOCK_NUMBER;
use crate::meta::v2::BlockMeta;
use crate::meta::ColumnMeta;
use crate::meta::FormatVersion;
use crate::meta::MetaEncoding;
use crate::meta::Statistics;
use crate::meta::Versioned;

/// A segment comprises one or more blocks
/// The structure of the segment is the same as that of v2, but the serialization and deserialization methods are different
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, Clone)]
pub struct SegmentInfo {
    /// format version of SegmentInfo table meta data
    ///
    /// Note that:
    ///
    /// - A instance of v3::SegmentInfo may have a value of v2/v1::SegmentInfo::VERSION for this field.
    ///
    ///   That indicates this instance is converted from a v2/v1::SegmentInfo.
    ///
    /// - The meta writers are responsible for only writing down the latest version of SegmentInfo, and
    ///   the format_version being written is of the latest version.
    ///
    ///   e.g. if the current version of SegmentInfo is v3::SegmentInfo, then the format_version
    ///   that will be written down to object storage as part of SegmentInfo table meta data,
    ///   should always be v3::SegmentInfo::VERSION (which is 3)
    pub format_version: FormatVersion,
    /// blocks belong to this segment
    pub blocks: Vec<Arc<BlockMeta>>,
    /// summary statistics
    pub summary: Statistics,
}

impl SegmentInfo {
    pub fn new(blocks: Vec<Arc<BlockMeta>>, summary: Statistics) -> Self {
        assert!(
            blocks.len() <= MAX_SEGMENT_BLOCK_NUMBER,
            "number of block overflow: {},  max number allowed {}",
            blocks.len(),
            MAX_SEGMENT_BLOCK_NUMBER,
        );

        Self {
            format_version: SegmentInfo::VERSION,
            blocks,
            summary,
        }
    }

    // Total block bytes of this segment.
    pub fn total_bytes(&self) -> u64 {
        self.summary.uncompressed_byte_size
    }

    // Encode self.blocks as RawBlockMeta.
    fn block_raw_bytes(&self) -> Result<RawBlockMeta> {
        let encoding = MetaEncoding::MessagePack;
        let bytes = encode(&encoding, &self.blocks)?;

        let compression = MetaCompression::default();
        let compressed = compress(&compression, bytes)?;

        Ok(RawBlockMeta {
            bytes: compressed,
            encoding,
            compression,
        })
    }
}

// use the chain of converters, for versions before v3
impl<T> From<T> for SegmentInfo
where T: Into<v2::SegmentInfo>
{
    fn from(value: T) -> Self {
        Self::from_v2(value.into())
    }
}

impl From<v3::SegmentInfo> for SegmentInfo {
    fn from(value: v3::SegmentInfo) -> Self {
        Self::from_v3(value)
    }
}

impl SegmentInfo {
    pub fn from_v3(s: v3::SegmentInfo) -> Self {
        // NOTE: it is important to let the format_version return from here
        // carries the format_version of segment info being converted.
        Self {
            format_version: s.format_version,
            blocks: s.blocks.into_iter().map(|v| Arc::new(v.into())).collect(),
            summary: s.summary.into(),
        }
    }
    pub fn from_v2(s: v2::SegmentInfo) -> Self {
        // NOTE: it is important to let the format_version return from here
        // carries the format_version of segment info being converted.
        Self {
            format_version: s.format_version,
            blocks: s.blocks,
            summary: s.summary,
        }
    }

    /// Serializes the Segment struct to a byte vector.
    ///
    /// The byte vector contains the format version, encoding, compression, and compressed block data and
    /// summary data. The encoding and compression are set to default values. The block data and summary
    /// data are encoded and compressed, respectively.
    ///
    /// # Returns
    ///
    /// A Result containing the serialized Segment data as a byte vector. If any errors occur during
    /// encoding, compression, or writing to the byte vector, an error will be returned.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        self.to_bytes_with_encoding(MetaEncoding::MessagePack)
    }

    fn to_bytes_with_encoding(&self, encoding: MetaEncoding) -> Result<Vec<u8>> {
        let compression = MetaCompression::default();

        let blocks = encode(&encoding, &self.blocks)?;
        let blocks_compress = compress(&compression, blocks)?;

        let summary = encode(&encoding, &self.summary)?;
        let summary_compress = compress(&compression, summary)?;

        let data_size = self.format_version.to_le_bytes().len()
            + 2
            + blocks_compress.len().to_le_bytes().len()
            + blocks_compress.len()
            + summary_compress.len().to_le_bytes().len()
            + summary_compress.len();
        let mut buf = Vec::with_capacity(data_size);

        buf.extend_from_slice(&self.format_version.to_le_bytes());
        buf.push(encoding as u8);
        buf.push(compression as u8);
        buf.extend_from_slice(&blocks_compress.len().to_le_bytes());
        buf.extend_from_slice(&summary_compress.len().to_le_bytes());

        buf.extend(blocks_compress);
        buf.extend(summary_compress);

        Ok(buf)
    }

    pub fn from_slice(bytes: &[u8]) -> Result<Self> {
        let mut cursor = Cursor::new(bytes);
        let SegmentHeader {
            version,
            encoding,
            compression,
            blocks_size,
            summary_size,
        } = decode_segment_header(&mut cursor)?;

        let blocks: Vec<Arc<BlockMeta>> =
            read_and_deserialize(&mut cursor, blocks_size, &encoding, &compression)?;
        let summary: Statistics =
            read_and_deserialize(&mut cursor, summary_size, &encoding, &compression)?;

        let mut segment = Self::new(blocks, summary);

        // bytes may represent an encoded v[n]::SegmentInfo, where n <= self::SegmentInfo::VERSION
        // please see PR https://github.com/datafuselabs/databend/pull/11211 for the adjustment of
        // format_version`'s "semantic"
        segment.format_version = version;
        Ok(segment)
    }
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone)]
pub struct RawBlockMeta {
    pub bytes: Vec<u8>,
    pub encoding: MetaEncoding,
    pub compression: MetaCompression,
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Clone)]
pub struct CompactSegmentInfo {
    pub format_version: FormatVersion,
    pub summary: Statistics,
    pub raw_block_metas: RawBlockMeta,
}

impl CompactSegmentInfo {
    pub fn from_reader(mut r: impl Read) -> Result<Self> {
        let SegmentHeader {
            version,
            encoding,
            compression,
            blocks_size,
            summary_size,
        } = decode_segment_header(&mut r)?;

        let mut block_metas_raw_bytes = vec![0; blocks_size as usize];
        r.read_exact(&mut block_metas_raw_bytes)?;

        let summary: Statistics =
            read_and_deserialize(&mut r, summary_size, &encoding, &compression)?;

        let segment = CompactSegmentInfo {
            format_version: version,
            summary,
            raw_block_metas: RawBlockMeta {
                bytes: block_metas_raw_bytes,
                encoding,
                compression,
            },
        };
        Ok(segment)
    }

    pub fn block_metas(&self) -> Result<Vec<Arc<BlockMeta>>> {
        let mut reader = Cursor::new(&self.raw_block_metas.bytes);
        read_and_deserialize(
            &mut reader,
            self.raw_block_metas.bytes.len() as u64,
            &self.raw_block_metas.encoding,
            &self.raw_block_metas.compression,
        )
    }
}

impl TryFrom<Arc<CompactSegmentInfo>> for SegmentInfo {
    type Error = ErrorCode;
    fn try_from(value: Arc<CompactSegmentInfo>) -> std::result::Result<Self, Self::Error> {
        let blocks = value.block_metas()?;
        Ok(SegmentInfo {
            format_version: value.format_version,
            blocks,
            summary: value.summary.clone(),
        })
    }
}

impl TryFrom<&CompactSegmentInfo> for SegmentInfo {
    type Error = ErrorCode;
    fn try_from(value: &CompactSegmentInfo) -> std::result::Result<Self, Self::Error> {
        let blocks = value.block_metas()?;
        Ok(SegmentInfo {
            format_version: value.format_version,
            blocks,
            summary: value.summary.clone(),
        })
    }
}

impl TryFrom<&SegmentInfo> for CompactSegmentInfo {
    type Error = ErrorCode;

    fn try_from(value: &SegmentInfo) -> std::result::Result<Self, Self::Error> {
        let bytes = value.block_raw_bytes()?;
        Ok(Self {
            format_version: value.format_version,
            summary: value.summary.clone(),
            raw_block_metas: bytes,
        })
    }
}

impl TryFrom<SegmentInfo> for CompactSegmentInfo {
    type Error = ErrorCode;

    fn try_from(value: SegmentInfo) -> std::result::Result<Self, Self::Error> {
        let bytes = value.block_raw_bytes()?;
        Ok(Self {
            format_version: value.format_version,
            summary: value.summary,
            raw_block_metas: bytes,
        })
    }
}

pub struct ColumnarSegmentInfo {
    pub format_version: FormatVersion,
    pub summary: Statistics,
    pub blocks: RecordBatch,
}

impl ColumnarSegmentInfo {
    pub fn try_from_segment_info_and_schema(
        value: SegmentInfo,
        schema: &TableSchema,
    ) -> std::result::Result<Self, ErrorCode> {
        let data_block = block_metas_to_columnar(&value.blocks, schema)?;
        Ok(Self {
            format_version: value.format_version,
            summary: value.summary,
            blocks: data_block,
        })
    }

    pub fn block_location(&self, idx: usize) -> &str {
        let array = self.blocks.column(3).as_struct();
        let location_array = array.column(0).as_string::<i32>();
        location_array.value(idx)
    }

    pub fn block_meta(&self, _idx: usize) -> Arc<BlockMeta> {
        todo!()
    }
}

fn block_metas_to_columnar(
    blocks: &[Arc<BlockMeta>],
    table_schema: &TableSchema,
) -> Result<RecordBatch> {
    let mut row_counts = Vec::with_capacity(blocks.len());
    let mut block_sizes = Vec::with_capacity(blocks.len());
    let mut file_sizes = Vec::with_capacity(blocks.len());
    let mut location_paths = Vec::with_capacity(blocks.len());
    let mut location_format_versions = Vec::with_capacity(blocks.len());
    let mut bloom_filter_index_location_paths = Vec::with_capacity(blocks.len());
    let mut bloom_filter_index_location_format_versions = Vec::with_capacity(blocks.len());
    let mut bloom_filter_index_sizes = Vec::with_capacity(blocks.len());
    let mut inverted_index_sizes = Vec::with_capacity(blocks.len());
    let mut compressions = Vec::with_capacity(blocks.len());
    let mut create_ons = Vec::with_capacity(blocks.len());
    for block in blocks {
        row_counts.push(block.row_count);
        block_sizes.push(block.block_size);
        file_sizes.push(block.file_size);
        location_paths.push(block.location.0.clone());
        location_format_versions.push(block.location.1);
        bloom_filter_index_location_paths.push(
            block
                .bloom_filter_index_location
                .as_ref()
                .map(|l| l.0.clone()),
        );
        bloom_filter_index_location_format_versions
            .push(block.bloom_filter_index_location.as_ref().map(|l| l.1));
        bloom_filter_index_sizes.push(block.bloom_filter_index_size);
        inverted_index_sizes.push(block.inverted_index_size);
        compressions.push(block.compression as u8);
        create_ons.push(block.create_on.map(|t| t.timestamp_nanos_opt().unwrap()));
    }

    let location_fields: Fields = vec![
        Field::new("path", ArrowDataType::Utf8, false),
        Field::new("format_version", ArrowDataType::UInt64, false),
    ]
    .into();
    let location_type = ArrowDataType::Struct(location_fields.clone());
    let mut fields = vec![
        Field::new("row_count", ArrowDataType::UInt64, false),
        Field::new("block_size", ArrowDataType::UInt64, false),
        Field::new("file_size", ArrowDataType::UInt64, false),
        Field::new("location", location_type.clone(), false),
        Field::new("bloom_filter_index_location", location_type, true),
        Field::new("bloom_filter_index_size", ArrowDataType::UInt64, false),
        Field::new("inverted_index_size", ArrowDataType::UInt64, true),
        Field::new("compression", ArrowDataType::UInt8, false),
        Field::new(
            "create_on",
            ArrowDataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
    ];

    let mut columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(UInt64Array::from(row_counts)),
        Arc::new(UInt64Array::from(block_sizes)),
        Arc::new(UInt64Array::from(file_sizes)),
        Arc::new(StructArray::try_new(
            location_fields.clone(),
            vec![
                Arc::new(StringArray::from(location_paths)),
                Arc::new(UInt64Array::from(location_format_versions)),
            ],
            None,
        )?),
        Arc::new(StructArray::try_new(
            location_fields,
            vec![
                Arc::new(StringArray::from(bloom_filter_index_location_paths)),
                Arc::new(UInt64Array::from(
                    bloom_filter_index_location_format_versions,
                )),
            ],
            None,
        )?),
        Arc::new(UInt64Array::from(bloom_filter_index_sizes)),
        Arc::new(UInt64Array::from(inverted_index_sizes)),
        Arc::new(UInt8Array::from(compressions)),
        Arc::new(TimestampNanosecondArray::from(create_ons)),
    ];

    let is_parquet = matches!(
        blocks[0].col_metas.values().next().unwrap(),
        ColumnMeta::Parquet(_)
    );

    for table_field in table_schema.fields.iter() {
        if !range_index_supported_type(&table_field.data_type) {
            continue;
        }
        let col_stats_fields: Fields = vec![
            Field::new(
                "min",
                table_type_to_arrow_type(&table_field.data_type),
                false,
            ),
            Field::new(
                "max",
                table_type_to_arrow_type(&table_field.data_type),
                false,
            ),
            Field::new("null_count", ArrowDataType::UInt64, false),
            Field::new("in_memory_size", ArrowDataType::UInt64, false),
            Field::new("distinct_of_values", ArrowDataType::UInt64, true),
        ]
        .into();
        fields.push(Field::new(
            table_field.column_id.to_string(),
            ArrowDataType::Struct(col_stats_fields.clone()),
            true,
        ));
        let null_count: Vec<u64> = blocks
            .iter()
            .map(|b| {
                b.col_stats
                    .get(&table_field.column_id)
                    .map(|s| s.null_count)
                    .unwrap_or_default()
            })
            .collect();
        let in_memory_size: Vec<u64> = blocks
            .iter()
            .map(|b| {
                b.col_stats
                    .get(&table_field.column_id)
                    .map(|s| s.in_memory_size)
                    .unwrap_or_default()
            })
            .collect();
        let distinct_of_values: Vec<Option<u64>> = blocks
            .iter()
            .map(|b| {
                b.col_stats
                    .get(&table_field.column_id)
                    .and_then(|s| s.distinct_of_values)
            })
            .collect();
        let min = scalar_iter_to_array(
            blocks
                .iter()
                .map(|b| b.col_stats.get(&table_field.column_id).map(|s| &s.min)),
            &table_field.data_type,
        )?;
        let max = scalar_iter_to_array(
            blocks
                .iter()
                .map(|b| b.col_stats.get(&table_field.column_id).map(|s| &s.max)),
            &table_field.data_type,
        )?;
        let nulls = blocks
            .iter()
            .map(|b| b.col_stats.contains_key(&table_field.column_id))
            .collect::<Vec<_>>();
        let arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(min),
            Arc::new(max),
            Arc::new(UInt64Array::from(null_count)),
            Arc::new(UInt64Array::from(in_memory_size)),
            Arc::new(UInt64Array::from(distinct_of_values)),
        ];
        columns.push(Arc::new(StructArray::try_new(
            col_stats_fields,
            arrays,
            Some(NullBuffer::from(nulls.clone())),
        )?));

        if is_parquet {
            let col_meta_fields: Fields = vec![
                Field::new("offset", ArrowDataType::UInt64, false),
                Field::new("len", ArrowDataType::UInt64, false),
                Field::new("num_values", ArrowDataType::UInt64, false),
            ]
            .into();
            let col_meta_type = ArrowDataType::Struct(col_meta_fields.clone());
            fields.push(Field::new(
                table_field.column_id.to_string(),
                col_meta_type,
                true,
            ));
            let offset = blocks
                .iter()
                .map(|b| {
                    b.col_metas
                        .get(&table_field.column_id)
                        .map(|c: &ColumnMeta| c.as_parquet().unwrap().offset)
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>();
            let len = blocks
                .iter()
                .map(|b| {
                    b.col_metas
                        .get(&table_field.column_id)
                        .map(|c: &ColumnMeta| c.as_parquet().unwrap().len)
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>();
            let num_values = blocks
                .iter()
                .map(|b| {
                    b.col_metas
                        .get(&table_field.column_id)
                        .map(|c: &ColumnMeta| c.as_parquet().unwrap().num_values)
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>();
            let arrays: Vec<Arc<dyn Array>> = vec![
                Arc::new(UInt64Array::from(offset)),
                Arc::new(UInt64Array::from(len)),
                Arc::new(UInt64Array::from(num_values)),
            ];
            columns.push(Arc::new(StructArray::try_new(
                col_meta_fields,
                arrays,
                Some(NullBuffer::from(nulls.clone())),
            )?));
        } else {
            todo!()
        }
    }

    let schema = Schema::new(fields);
    let record_batch = RecordBatch::try_new(Arc::new(schema.clone()), columns)?;
    Ok(record_batch)
}

fn scalar_iter_to_array<'a, I>(iter: I, data_type: &TableDataType) -> Result<Arc<dyn Array>>
where I: Iterator<Item = Option<&'a Scalar>> {
    match data_type.remove_nullable() {
        TableDataType::Number(NumberDataType::Int8) => {
            let values = iter
                .map(|s| {
                    s.map(|s| *s.as_number().unwrap().as_int8().unwrap())
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(Int8Array::from(values)))
        }
        TableDataType::Number(NumberDataType::Int16) => {
            let values = iter
                .map(|s| {
                    s.map(|s| *s.as_number().unwrap().as_int16().unwrap())
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(Int16Array::from(values)))
        }
        TableDataType::Number(NumberDataType::Int32) => {
            let values = iter
                .map(|s| {
                    s.map(|s| *s.as_number().unwrap().as_int32().unwrap())
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(Int32Array::from(values)))
        }
        TableDataType::Number(NumberDataType::Int64) => {
            let values = iter
                .map(|s| {
                    s.map(|s| *s.as_number().unwrap().as_int64().unwrap())
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(Int64Array::from(values)))
        }
        TableDataType::Number(NumberDataType::UInt8) => {
            let values = iter
                .map(|s| {
                    s.map(|s| *s.as_number().unwrap().as_u_int8().unwrap())
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(UInt8Array::from(values)))
        }
        TableDataType::Number(NumberDataType::UInt16) => {
            let values = iter
                .map(|s| {
                    s.map(|s| *s.as_number().unwrap().as_u_int16().unwrap())
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(UInt16Array::from(values)))
        }
        TableDataType::Number(NumberDataType::UInt32) => {
            let values = iter
                .map(|s| {
                    s.map(|s| *s.as_number().unwrap().as_u_int32().unwrap())
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(UInt32Array::from(values)))
        }
        TableDataType::Number(NumberDataType::UInt64) => {
            let values = iter
                .map(|s| {
                    s.map(|s| *s.as_number().unwrap().as_u_int64().unwrap())
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(UInt64Array::from(values)))
        }
        TableDataType::Number(NumberDataType::Float32) => {
            let values = iter
                .map(|s| {
                    s.map(|s| *s.as_number().unwrap().as_float32().unwrap().as_ref())
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(Float32Array::from(values)))
        }
        TableDataType::Number(NumberDataType::Float64) => {
            let values = iter
                .map(|s| {
                    s.map(|s| *s.as_number().unwrap().as_float64().unwrap().as_ref())
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(Float64Array::from(values)))
        }
        TableDataType::Date => {
            let values = iter
                .map(|s| s.map(|s| *s.as_date().unwrap()).unwrap_or_default())
                .collect::<Vec<_>>();
            Ok(Arc::new(Date32Array::from(values)))
        }
        TableDataType::Timestamp => {
            let values = iter
                .map(|s| s.map(|s| *s.as_timestamp().unwrap()).unwrap_or_default())
                .collect::<Vec<_>>();
            Ok(Arc::new(TimestampMicrosecondArray::from(values)))
        }
        TableDataType::String => {
            let values = iter
                .map(|s| {
                    s.map(|s| s.as_string().unwrap().to_string())
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(LargeStringArray::from(values)))
        }
        TableDataType::Decimal(DecimalDataType::Decimal128(_)) => {
            let values = iter
                .map(|s| {
                    s.map(|s| *s.as_decimal().unwrap().as_decimal128().unwrap().0)
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(Decimal128Array::from(values)))
        }
        TableDataType::Decimal(DecimalDataType::Decimal256(_)) => {
            let values = iter
                .map(|s| unsafe {
                    s.map(|s| {
                        std::mem::transmute::<_, arrow::datatypes::i256>(
                            *s.as_decimal().unwrap().as_decimal256().unwrap().0,
                        )
                    })
                    .unwrap_or_default()
                })
                .collect::<Vec<_>>();
            Ok(Arc::new(Decimal256Array::from(values)))
        }
        _ => unreachable!(),
    }
}

fn range_index_supported_type(data_type: &TableDataType) -> bool {
    let inner_type = data_type.remove_nullable();
    matches!(
        inner_type,
        TableDataType::Number(_)
            | TableDataType::Date
            | TableDataType::Timestamp
            | TableDataType::String
            | TableDataType::Decimal(_)
    )
}
