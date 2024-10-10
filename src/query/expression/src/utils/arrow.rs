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
use std::io::Write;

use databend_common_arrow::arrow::array::Array;
use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::bitmap::MutableBitmap;
use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_arrow::arrow::datatypes::Schema;
use databend_common_arrow::arrow::io::ipc::read::read_file_metadata;
use databend_common_arrow::arrow::io::ipc::read::FileReader;
use databend_common_arrow::arrow::io::ipc::write::FileWriter;
use databend_common_arrow::arrow::io::ipc::write::WriteOptions as IpcWriteOptions;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::BlockEntry;
use crate::Column;
use crate::ColumnBuilder;
use crate::DataField;
use crate::Value;

pub fn bitmap_into_mut(bitmap: Bitmap) -> MutableBitmap {
    bitmap
        .into_mut()
        .map_left(|bitmap| {
            let mut builder = MutableBitmap::new();
            builder.extend_from_bitmap(&bitmap);
            builder
        })
        .into_inner()
}

pub fn repeat_bitmap(bitmap: &mut Bitmap, n: usize) -> MutableBitmap {
    let mut builder = MutableBitmap::new();
    for _ in 0..n {
        builder.extend_from_bitmap(bitmap);
    }
    builder
}

pub fn append_bitmap(bitmap: &mut MutableBitmap, other: &Bitmap) {
    bitmap.extend_from_bitmap(other)
}

pub fn buffer_into_mut<T: Clone>(mut buffer: Buffer<T>) -> Vec<T> {
    unsafe {
        buffer
            .get_mut()
            .map(std::mem::take)
            .unwrap_or_else(|| buffer.to_vec())
    }
}

pub fn serialize_column(col: &Column) -> Vec<u8> {
    let mut buffer = Vec::new();
    serialize_column_in(col, &mut buffer).unwrap();
    buffer
}

pub fn serialize_column_in(
    col: &Column,
    w: &mut impl Write,
) -> databend_common_arrow::arrow::error::Result<()> {
    let schema = Schema::from(vec![col.arrow_field()]);
    let mut writer = FileWriter::new(w, schema, None, IpcWriteOptions::default());
    writer.start()?;
    writer.write(
        &databend_common_arrow::arrow::chunk::Chunk::new(vec![col.as_arrow()]),
        None,
    )?;
    writer.finish()
}

pub fn deserialize_column(bytes: &[u8]) -> Result<Column> {
    let mut cursor = Cursor::new(bytes);

    let metadata = read_file_metadata(&mut cursor)?;
    let f = metadata.schema.fields[0].clone();
    let data_field = DataField::try_from(&f)?;

    let mut reader = FileReader::new(cursor, metadata, None, None);
    let col = reader
        .next()
        .ok_or_else(|| ErrorCode::Internal("expected one arrow array"))??
        .into_arrays()
        .remove(0);

    Column::from_arrow(col.as_ref(), data_field.data_type())
}

/// Convert a column to a arrow array.
pub fn column_to_arrow_array(column: &BlockEntry, num_rows: usize) -> Box<dyn Array> {
    match &column.value {
        Value::Scalar(v) => {
            let builder = ColumnBuilder::repeat(&v.as_ref(), num_rows, &column.data_type);
            builder.build().as_arrow()
        }
        Value::Column(c) => c.as_arrow(),
    }
}

pub fn and_validities(lhs: Option<Bitmap>, rhs: Option<Bitmap>) -> Option<Bitmap> {
    match (lhs, rhs) {
        (Some(lhs), None) => Some(lhs),
        (None, Some(rhs)) => Some(rhs),
        (None, None) => None,
        (Some(lhs), Some(rhs)) => Some((&lhs) & (&rhs)),
    }
}

pub fn or_validities(lhs: Option<Bitmap>, rhs: Option<Bitmap>) -> Option<Bitmap> {
    match (lhs, rhs) {
        (Some(lhs), None) => Some(lhs),
        (None, Some(rhs)) => Some(rhs),
        (None, None) => None,
        (Some(lhs), Some(rhs)) => Some((&lhs) | (&rhs)),
    }
}
