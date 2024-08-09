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

use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::ValueType;
use databend_common_expression::Column;
use databend_common_io::constants::FALSE_BYTES_LOWER;
use databend_common_io::constants::FALSE_BYTES_NUM;
use databend_common_io::constants::INF_BYTES_LONG;
use databend_common_io::constants::NULL_BYTES_ESCAPE;
use databend_common_io::constants::TRUE_BYTES_LOWER;
use databend_common_io::constants::TRUE_BYTES_NUM;
use databend_common_meta_app::principal::CsvFileFormatParams;
use databend_common_meta_app::principal::TsvFileFormatParams;
use geozero::wkb::Ewkb;
use geozero::ToWkt;

use crate::binary::encode_binary;
use crate::field_encoder::write_tsv_escaped_string;
use crate::field_encoder::FieldEncoderValues;
use crate::FileFormatOptionsExt;
use crate::OutputCommonSettings;

pub enum StringFormatter {
    Csv { quote_char: u8 },
    Tsv { record_delimiter: u8 },
}

impl StringFormatter {
    fn write_string(&self, bytes: &[u8], buf: &mut Vec<u8>) {
        match self {
            StringFormatter::Csv { quote_char } => write_csv_string(bytes, buf, *quote_char),
            StringFormatter::Tsv { record_delimiter } => {
                write_tsv_escaped_string(bytes, buf, *record_delimiter)
            }
        }
    }
}

// todo(youngsofun): support quote style
pub fn write_csv_string(bytes: &[u8], buf: &mut Vec<u8>, quote: u8) {
    buf.push(quote);
    let mut start = 0;

    for (i, &byte) in bytes.iter().enumerate() {
        if byte == quote {
            if start < i {
                buf.extend_from_slice(&bytes[start..i]);
            }
            buf.push(quote);
            buf.push(quote);
            start = i + 1;
        }
    }

    if start != bytes.len() {
        buf.extend_from_slice(&bytes[start..]);
    }
    buf.push(quote);
}

pub struct FieldEncoderCSV {
    pub simple: FieldEncoderValues,
    pub nested: FieldEncoderValues,
    pub string_formatter: StringFormatter,
}

impl FieldEncoderCSV {
    pub fn create_csv(params: &CsvFileFormatParams, options_ext: &FileFormatOptionsExt) -> Self {
        Self {
            nested: FieldEncoderValues::create(options_ext),
            simple: FieldEncoderValues {
                common_settings: OutputCommonSettings {
                    true_bytes: TRUE_BYTES_LOWER.as_bytes().to_vec(),
                    false_bytes: FALSE_BYTES_LOWER.as_bytes().to_vec(),
                    null_bytes: NULL_BYTES_ESCAPE.as_bytes().to_vec(),
                    nan_bytes: params.nan_display.as_bytes().to_vec(),
                    inf_bytes: INF_BYTES_LONG.as_bytes().to_vec(),
                    timezone: options_ext.timezone,
                    binary_format: params.binary_format,
                    geometry_format: params.geometry_format,
                },
                quote_char: 0, // not used
            },
            string_formatter: StringFormatter::Csv {
                quote_char: params.quote.as_bytes()[0],
            },
        }
    }

    pub fn create_tsv(params: &TsvFileFormatParams, options_ext: &FileFormatOptionsExt) -> Self {
        Self {
            nested: FieldEncoderValues::create(options_ext),
            simple: FieldEncoderValues {
                common_settings: OutputCommonSettings {
                    true_bytes: TRUE_BYTES_NUM.as_bytes().to_vec(),
                    false_bytes: FALSE_BYTES_NUM.as_bytes().to_vec(),
                    null_bytes: NULL_BYTES_ESCAPE.as_bytes().to_vec(),
                    nan_bytes: params.nan_display.as_bytes().to_vec(),
                    inf_bytes: INF_BYTES_LONG.as_bytes().to_vec(),
                    timezone: options_ext.timezone,
                    binary_format: Default::default(),
                    geometry_format: Default::default(),
                },
                quote_char: 0, // not used
            },
            string_formatter: StringFormatter::Tsv {
                record_delimiter: params.field_delimiter.as_bytes().to_vec()[0],
            },
        }
    }

    pub(crate) fn write_field(&self, column: &Column, row_index: usize, out_buf: &mut Vec<u8>) {
        match &column {
            Column::Nullable(box c) => self.write_nullable(c, row_index, out_buf),

            Column::Binary(c) => {
                let buf = unsafe { c.index_unchecked(row_index) };
                let encoded = encode_binary(buf, self.simple.common_settings.binary_format);
                out_buf.extend_from_slice(&encoded);
            }
            Column::String(c) => {
                let buf = unsafe { c.index_unchecked(row_index) };
                self.string_formatter.write_string(buf.as_bytes(), out_buf);
            }

            Column::Date(..) | Column::Timestamp(..) | Column::Bitmap(..) | Column::Variant(..) => {
                let mut buf = Vec::new();
                self.simple.write_field(column, row_index, &mut buf, false);
                self.string_formatter.write_string(&buf, out_buf);
            }

            Column::Geometry(g) => {
                let buf = unsafe { g.index_unchecked(row_index) };
                let geom = Ewkb(buf.to_vec()).to_ewkt(None).unwrap();
                self.string_formatter.write_string(geom.as_bytes(), out_buf);
            }

            Column::Geography(c) => {
                use databend_common_geobuf::Wkt;
                let geog = unsafe { c.index_unchecked(row_index) };
                let Wkt(str) = geog.0.try_into().unwrap();
                self.string_formatter.write_string(str.as_bytes(), out_buf);
            }

            Column::Array(..) | Column::Map(..) | Column::Tuple(..) => {
                let mut buf = Vec::new();
                self.nested.write_field(column, row_index, &mut buf, false);
                self.string_formatter.write_string(&buf, out_buf);
            }

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
}
