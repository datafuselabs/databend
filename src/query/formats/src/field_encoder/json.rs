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

use databend_common_expression::types::array::ArrayColumn;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::ValueType;
use databend_common_expression::Column;
use databend_common_geobuf::GeoJson;
use databend_common_io::constants::FALSE_BYTES_LOWER;
use databend_common_io::constants::NULL_BYTES_LOWER;
use databend_common_io::constants::TRUE_BYTES_LOWER;
use geozero::wkb::Ewkb;
use geozero::ToJson;

use crate::field_encoder::helpers::write_json_string;
use crate::field_encoder::FieldEncoderValues;
use crate::FileFormatOptionsExt;
use crate::OutputCommonSettings;

pub struct FieldEncoderJSON {
    pub simple: FieldEncoderValues,
    pub quote_denormals: bool,
    pub escape_forward_slashes: bool,
}

impl FieldEncoderJSON {
    pub fn create(options: &FileFormatOptionsExt) -> Self {
        FieldEncoderJSON {
            simple: FieldEncoderValues {
                common_settings: OutputCommonSettings {
                    true_bytes: TRUE_BYTES_LOWER.as_bytes().to_vec(),
                    false_bytes: FALSE_BYTES_LOWER.as_bytes().to_vec(),
                    nan_bytes: NULL_BYTES_LOWER.as_bytes().to_vec(),
                    inf_bytes: NULL_BYTES_LOWER.as_bytes().to_vec(),
                    null_bytes: NULL_BYTES_LOWER.as_bytes().to_vec(),
                    timezone: options.timezone,
                    binary_format: Default::default(),
                    geometry_format: Default::default(),
                },
                quote_char: 0,
            },
            quote_denormals: false,
            escape_forward_slashes: true,
        }
    }
}

impl FieldEncoderJSON {
    pub(crate) fn write_field(&self, column: &Column, row_index: usize, out_buf: &mut Vec<u8>) {
        match &column {
            Column::Nullable(box c) => self.write_nullable(c, row_index, out_buf),

            Column::Binary(c) => {
                let buf = unsafe { c.index_unchecked(row_index) };
                self.write_string(buf, out_buf);
            }
            Column::String(c) => {
                let buf = unsafe { c.index_unchecked(row_index) };
                self.write_string(buf.as_bytes(), out_buf);
            }

            Column::Date(..) | Column::Timestamp(..) | Column::Bitmap(..) => {
                let mut buf = Vec::new();
                self.simple.write_field(column, row_index, &mut buf, false);
                self.write_string(&buf, out_buf);
            }

            Column::Variant(c) => {
                let v = unsafe { c.index_unchecked(row_index) };
                out_buf.extend_from_slice(jsonb::to_string(v).as_bytes());
            }
            Column::Geometry(c) => {
                let v = unsafe { c.index_unchecked(row_index) };
                out_buf.extend_from_slice(Ewkb(v.to_vec()).to_json().unwrap().as_bytes())
            }
            Column::Geography(c) => {
                let v = unsafe { c.index_unchecked(row_index) };
                let GeoJson(str) = v.0.try_into().unwrap();
                out_buf.extend_from_slice(str.as_bytes())
            }

            Column::Array(box c) => self.write_array(c, row_index, out_buf),
            Column::Map(box c) => self.write_map(c, row_index, out_buf),
            Column::Tuple(fields) => self.write_tuple(fields, row_index, out_buf),

            Column::Null { .. }
            | Column::EmptyArray { .. }
            | Column::EmptyMap { .. }
            | Column::Number(_)
            | Column::Decimal(_)
            | Column::Boolean(_) => self.simple.write_field(column, row_index, out_buf, false),
        }
    }

    fn write_nullable<T: ValueType>(
        &self,
        column: &NullableColumn<T>,
        row_index: usize,
        out_buf: &mut Vec<u8>,
    ) {
        if !column.validity.get_bit(row_index) {
            self.simple.write_null(out_buf)
        } else {
            self.write_field(&T::upcast_column(column.column.clone()), row_index, out_buf)
        }
    }

    pub fn write_string(&self, in_buf: &[u8], out_buf: &mut Vec<u8>) {
        out_buf.push(b'\"');
        write_json_string(
            in_buf,
            out_buf,
            self.quote_denormals,
            self.escape_forward_slashes,
        );
        out_buf.push(b'\"');
    }

    fn write_array<T: ValueType>(
        &self,
        column: &ArrayColumn<T>,
        row_index: usize,
        out_buf: &mut Vec<u8>,
    ) {
        let start = unsafe { *column.offsets.get_unchecked(row_index) as usize };
        let end = unsafe { *column.offsets.get_unchecked(row_index + 1) as usize };
        out_buf.push(b'[');
        let inner = &T::upcast_column(column.values.clone());
        for i in start..end {
            if i != start {
                out_buf.extend_from_slice(b",");
            }
            self.write_field(inner, i, out_buf);
        }
        out_buf.push(b']');
    }

    fn write_map<T: ValueType>(
        &self,
        column: &ArrayColumn<T>,
        row_index: usize,
        out_buf: &mut Vec<u8>,
    ) {
        let start = unsafe { *column.offsets.get_unchecked(row_index) as usize };
        let end = unsafe { *column.offsets.get_unchecked(row_index + 1) as usize };
        out_buf.push(b'{');
        let inner = &T::upcast_column(column.values.clone());
        match inner {
            Column::Tuple(fields) => {
                for i in start..end {
                    if i != start {
                        out_buf.extend_from_slice(b",");
                    }
                    self.write_field(&fields[0], i, out_buf);
                    out_buf.extend_from_slice(b":");
                    self.write_field(&fields[1], i, out_buf);
                }
            }
            _ => unreachable!(),
        }
        out_buf.push(b'}');
    }

    fn write_tuple(&self, columns: &[Column], row_index: usize, out_buf: &mut Vec<u8>) {
        // write tuple as JSON Object
        out_buf.push(b'{');
        for (i, inner) in columns.iter().enumerate() {
            if i > 0 {
                out_buf.extend_from_slice(b",");
            }
            let key = format!("{}", i + 1);
            self.write_string(key.as_bytes(), out_buf);
            out_buf.extend_from_slice(b":");
            self.write_field(inner, row_index, out_buf);
        }
        out_buf.push(b'}');
    }
}
