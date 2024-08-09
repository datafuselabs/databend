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
use std::io::Cursor;

use bstr::ByteSlice;
use databend_common_arrow::arrow::bitmap::MutableBitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::serialize::read_decimal_with_size;
use databend_common_expression::serialize::uniform_date;
use databend_common_expression::types::array::ArrayColumnBuilder;
use databend_common_expression::types::binary::BinaryColumnBuilder;
use databend_common_expression::types::date::check_date;
use databend_common_expression::types::decimal::Decimal;
use databend_common_expression::types::decimal::DecimalColumnBuilder;
use databend_common_expression::types::decimal::DecimalSize;
use databend_common_expression::types::geography::Geography;
use databend_common_expression::types::geography::GeographyColumnBuilder;
use databend_common_expression::types::nullable::NullableColumnBuilder;
use databend_common_expression::types::timestamp::check_timestamp;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::Number;
use databend_common_expression::types::NumberColumnBuilder;
use databend_common_expression::with_decimal_type;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::ColumnBuilder;
use databend_common_io::constants::FALSE_BYTES_LOWER;
use databend_common_io::constants::FALSE_BYTES_NUM;
use databend_common_io::constants::NULL_BYTES_ESCAPE;
use databend_common_io::constants::TRUE_BYTES_LOWER;
use databend_common_io::constants::TRUE_BYTES_NUM;
use databend_common_io::cursor_ext::collect_number;
use databend_common_io::cursor_ext::read_num_text_exact;
use databend_common_io::cursor_ext::BufferReadDateTimeExt;
use databend_common_io::cursor_ext::DateTimeResType;
use databend_common_io::cursor_ext::ReadBytesExt;
use databend_common_io::parse_bitmap;
use databend_common_io::parse_geometry;
use databend_common_io::parse_to_ewkb;
use databend_common_meta_app::principal::CsvFileFormatParams;
use databend_common_meta_app::principal::TsvFileFormatParams;
use jsonb::parse_value;
use lexical_core::FromLexical;
use num_traits::NumCast;

use crate::binary::decode_binary;
use crate::field_decoder::FieldDecoder;
use crate::FileFormatOptionsExt;
use crate::InputCommonSettings;
use crate::NestedValues;

#[derive(Clone)]
pub struct SeparatedTextDecoder {
    common_settings: InputCommonSettings,
    nested_decoder: NestedValues,
}

impl FieldDecoder for SeparatedTextDecoder {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// in CSV, we find the exact bound of each field before decode it to a type.
/// which is diff from the case when parsing values.
impl SeparatedTextDecoder {
    pub fn create_csv(params: &CsvFileFormatParams, options_ext: &FileFormatOptionsExt) -> Self {
        SeparatedTextDecoder {
            common_settings: InputCommonSettings {
                true_bytes: TRUE_BYTES_LOWER.as_bytes().to_vec(),
                false_bytes: FALSE_BYTES_LOWER.as_bytes().to_vec(),
                null_if: vec![params.null_display.as_bytes().to_vec()],
                timezone: options_ext.timezone,
                disable_variant_check: options_ext.disable_variant_check,
                binary_format: params.binary_format,
                is_rounding_mode: options_ext.is_rounding_mode,
                enable_dst_hour_fix: options_ext.enable_dst_hour_fix,
            },
            nested_decoder: NestedValues::create(options_ext),
        }
    }

    pub fn create_tsv(_params: &TsvFileFormatParams, options_ext: &FileFormatOptionsExt) -> Self {
        SeparatedTextDecoder {
            common_settings: InputCommonSettings {
                null_if: vec![NULL_BYTES_ESCAPE.as_bytes().to_vec()],
                true_bytes: TRUE_BYTES_NUM.as_bytes().to_vec(),
                false_bytes: FALSE_BYTES_NUM.as_bytes().to_vec(),
                timezone: options_ext.timezone,
                disable_variant_check: options_ext.disable_variant_check,
                binary_format: Default::default(),
                is_rounding_mode: options_ext.is_rounding_mode,
                enable_dst_hour_fix: options_ext.enable_dst_hour_fix,
            },
            nested_decoder: NestedValues::create(options_ext),
        }
    }

    fn common_settings(&self) -> &InputCommonSettings {
        &self.common_settings
    }

    pub fn read_field(&self, column: &mut ColumnBuilder, data: &[u8]) -> Result<()> {
        match column {
            ColumnBuilder::Null { len } => {
                *len += 1;
                Ok(())
            }
            ColumnBuilder::Binary(c) => {
                let data = decode_binary(data, self.common_settings().binary_format)?;
                c.put_slice(&data);
                c.commit_row();
                Ok(())
            }
            ColumnBuilder::String(c) => {
                c.put_str(std::str::from_utf8(data)?);
                c.commit_row();
                Ok(())
            }
            ColumnBuilder::Boolean(c) => self.read_bool(c, data),
            ColumnBuilder::Nullable(c) => self.read_nullable(c, data),
            ColumnBuilder::Number(c) => with_number_mapped_type!(|NUM_TYPE| match c {
                NumberColumnBuilder::NUM_TYPE(c) => {
                    if NUM_TYPE::FLOATING {
                        self.read_float(c, data)
                    } else {
                        self.read_int(c, data)
                    }
                }
            }),
            ColumnBuilder::Decimal(c) => with_decimal_type!(|DECIMAL_TYPE| match c {
                DecimalColumnBuilder::DECIMAL_TYPE(c, size) => self.read_decimal(c, *size, data),
            }),
            ColumnBuilder::Date(c) => self.read_date(c, data),
            ColumnBuilder::Timestamp(c) => self.read_timestamp(c, data),
            ColumnBuilder::Array(c) => self.read_array(c, data),
            ColumnBuilder::Map(c) => self.read_map(c, data),
            ColumnBuilder::Bitmap(c) => self.read_bitmap(c, data),
            ColumnBuilder::Tuple(fields) => self.read_tuple(fields, data),
            ColumnBuilder::Variant(c) => self.read_variant(c, data),
            ColumnBuilder::Geometry(c) => self.read_geometry(c, data),
            ColumnBuilder::Geography(c) => self.read_geography(c, data),
            ColumnBuilder::EmptyArray { .. } => {
                unreachable!("EmptyArray")
            }
            ColumnBuilder::EmptyMap { .. } => {
                unreachable!("EmptyMap")
            }
        }
    }

    fn read_bool(&self, column: &mut MutableBitmap, data: &[u8]) -> Result<()> {
        if data == self.common_settings().false_bytes {
            column.push(false);
        } else if data == self.common_settings().true_bytes {
            column.push(true);
        } else {
            let err_msg = format!(
                "Incorrect boolean value, expect {} or {}",
                self.common_settings().true_bytes.to_str().unwrap(),
                self.common_settings().false_bytes.to_str().unwrap()
            );
            return Err(ErrorCode::BadBytes(err_msg));
        }
        Ok(())
    }

    fn read_nullable(
        &self,
        column: &mut NullableColumnBuilder<AnyType>,
        data: &[u8],
    ) -> Result<()> {
        for null in &self.common_settings().null_if {
            if data == null {
                column.push_null();
                return Ok(());
            }
        }
        self.read_field(&mut column.builder, data)?;
        column.validity.push(true);
        Ok(())
    }

    fn read_int<T>(&self, column: &mut Vec<T>, data: &[u8]) -> Result<()>
    where
        T: Number + From<T::Native>,
        T::Native: FromLexical + NumCast,
    {
        // can not use read_num_text_exact directly, because we need to allow int like '1.0'
        let (n_in, effective) = collect_number(data);
        if n_in != data.len() {
            return Err(ErrorCode::BadBytes("invalid text for number"));
        }
        let val: Result<T::Native> = read_num_text_exact(&data[..effective]);
        let v = match val {
            Ok(v) => v,
            Err(_) => {
                // cast float value to integer value
                let val: f64 = read_num_text_exact(&data[..effective])?;
                let new_val: Option<T::Native> = if self.common_settings.is_rounding_mode {
                    num_traits::cast::cast(val.round())
                } else {
                    num_traits::cast::cast(val)
                };
                if let Some(v) = new_val {
                    v
                } else {
                    return Err(ErrorCode::BadBytes(format!("number {} is overflowed", val)));
                }
            }
        };
        column.push(v.into());
        Ok(())
    }

    fn read_float<T>(&self, column: &mut Vec<T>, data: &[u8]) -> Result<()>
    where
        T: Number + From<T::Native>,
        T::Native: FromLexical,
    {
        let v: T::Native = read_num_text_exact(data)?;
        column.push(v.into());
        Ok(())
    }

    fn read_decimal<D: Decimal>(
        &self,
        column: &mut Vec<D>,
        size: DecimalSize,
        data: &[u8],
    ) -> Result<()> {
        let (n, n_read) = read_decimal_with_size(data, size, false, true)?;
        if n_read != data.len() {
            return Err(ErrorCode::BadBytes(
                "unexpected remaining bytes".to_string(),
            ));
        }
        column.push(n);
        Ok(())
    }

    fn read_date(&self, column: &mut Vec<i32>, data: &[u8]) -> Result<()> {
        let mut buffer_readr = Cursor::new(&data);
        let date = buffer_readr.read_date_text(
            &self.common_settings().timezone,
            self.common_settings().enable_dst_hour_fix,
        )?;
        let days = uniform_date(date);
        check_date(days as i64)?;
        column.push(days);
        Ok(())
    }

    fn read_timestamp(&self, column: &mut Vec<i64>, data: &[u8]) -> Result<()> {
        let ts = if !data.contains(&b'-') {
            read_num_text_exact(data)?
        } else {
            let mut buffer_readr = Cursor::new(&data);
            let t = buffer_readr.read_timestamp_text(
                &self.common_settings().timezone,
                false,
                self.common_settings.enable_dst_hour_fix,
            )?;
            match t {
                DateTimeResType::Datetime(t) => {
                    if !buffer_readr.eof() {
                        let data = data.to_str().unwrap_or("not utf8");
                        let msg = format!(
                            "fail to deserialize timestamp, unexpected end at pos {} of {}",
                            buffer_readr.position(),
                            data
                        );
                        return Err(ErrorCode::BadBytes(msg));
                    }
                    t.timestamp_micros()
                }
                _ => unreachable!(),
            }
        };
        check_timestamp(ts)?;
        column.push(ts);
        Ok(())
    }

    fn read_bitmap(&self, column: &mut BinaryColumnBuilder, data: &[u8]) -> Result<()> {
        let rb = parse_bitmap(data)?;
        rb.serialize_into(&mut column.data).unwrap();
        column.commit_row();
        Ok(())
    }

    fn read_variant(&self, column: &mut BinaryColumnBuilder, data: &[u8]) -> Result<()> {
        match parse_value(data) {
            Ok(value) => {
                value.write_to_vec(&mut column.data);
                column.commit_row();
            }
            Err(e) => {
                if self.common_settings().disable_variant_check {
                    column.commit_row();
                } else {
                    return Err(ErrorCode::BadBytes(e.to_string()));
                }
            }
        }
        Ok(())
    }

    fn read_geometry(&self, column: &mut BinaryColumnBuilder, data: &[u8]) -> Result<()> {
        let geom = parse_to_ewkb(data, None)?;
        column.put_slice(geom.as_bytes());
        column.commit_row();
        Ok(())
    }

    #[allow(clippy::ptr_arg)]
    fn read_geography(&self, column: &mut GeographyColumnBuilder, data: &[u8]) -> Result<()> {
        let geom = parse_geometry(data)?;
        column.push(Geography(geom).as_ref());
        Ok(())
    }

    fn read_array(&self, column: &mut ArrayColumnBuilder<AnyType>, data: &[u8]) -> Result<()> {
        let mut cursor = Cursor::new(data);
        self.nested_decoder.read_array(column, &mut cursor)
    }

    fn read_map(&self, column: &mut ArrayColumnBuilder<AnyType>, data: &[u8]) -> Result<()> {
        let mut cursor = Cursor::new(data);
        self.nested_decoder.read_map(column, &mut cursor)
    }

    fn read_tuple(&self, column: &mut [ColumnBuilder], data: &[u8]) -> Result<()> {
        let mut cursor = Cursor::new(data);
        self.nested_decoder.read_tuple(column, &mut cursor)
    }
}
