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

// DO NOT EDIT.
// This crate keeps some legacy codes for compatibility, it's locked by bincode of meta's v3 version

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_exception::Result;
use enum_as_inner::EnumAsInner;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

use crate::types::array::ArrayColumn;
use crate::types::binary::BinaryColumn;
use crate::types::decimal::DecimalColumn;
use crate::types::decimal::DecimalScalar;
use crate::types::nullable::NullableColumn;
use crate::types::number::NumberColumn;
use crate::types::number::NumberScalar;
use crate::types::*;
use crate::Column;
use crate::Scalar;

#[derive(Clone, Serialize, Deserialize)]
pub enum LegacyScalar {
    Null,
    EmptyArray,
    EmptyMap,
    Number(NumberScalar),
    Decimal(DecimalScalar),
    Timestamp(i64),
    Date(i32),
    Boolean(bool),
    String(Vec<u8>),
    Array(LegacyColumn),
    Map(LegacyColumn),
    Bitmap(Vec<u8>),
    Tuple(Vec<Scalar>),
    Variant(Vec<u8>),
}

#[derive(Clone, EnumAsInner)]
pub enum LegacyColumn {
    Null { len: usize },
    EmptyArray { len: usize },
    EmptyMap { len: usize },
    Number(NumberColumn),
    Decimal(DecimalColumn),
    Boolean(Bitmap),
    String(BinaryColumn),
    Timestamp(Buffer<i64>),
    Date(Buffer<i32>),
    Array(Box<LegacyArrayColumn>),
    Map(Box<LegacyArrayColumn>),
    Bitmap(BinaryColumn),
    Nullable(Box<LegacyNullableColumn>),
    Tuple(Vec<LegacyColumn>),
    Variant(BinaryColumn),
}

#[derive(Clone)]
pub struct LegacyArrayColumn {
    pub values: LegacyColumn,
    pub offsets: Buffer<u64>,
}

#[derive(Clone)]
pub struct LegacyNullableColumn {
    pub column: LegacyColumn,
    pub validity: Bitmap,
}

impl From<LegacyScalar> for Scalar {
    fn from(value: LegacyScalar) -> Self {
        match value {
            LegacyScalar::Null => Scalar::Null,
            LegacyScalar::EmptyArray => Scalar::EmptyArray,
            LegacyScalar::EmptyMap => Scalar::EmptyMap,
            LegacyScalar::Number(num_scalar) => Scalar::Number(num_scalar),
            LegacyScalar::Decimal(dec_scalar) => Scalar::Decimal(dec_scalar),
            LegacyScalar::Timestamp(ts) => Scalar::Timestamp(ts),
            LegacyScalar::Date(date) => Scalar::Date(date),
            LegacyScalar::Boolean(b) => Scalar::Boolean(b),
            LegacyScalar::String(s) => Scalar::String(String::from_utf8_lossy(&s).into_owned()),
            LegacyScalar::Array(col) => Scalar::Array(col.into()),
            LegacyScalar::Map(col) => Scalar::Map(col.into()),
            LegacyScalar::Bitmap(bmp) => Scalar::Bitmap(bmp),
            LegacyScalar::Tuple(tuple) => Scalar::Tuple(tuple),
            LegacyScalar::Variant(variant) => Scalar::Variant(variant),
        }
    }
}

impl From<LegacyColumn> for Column {
    fn from(value: LegacyColumn) -> Self {
        match value {
            LegacyColumn::Null { len } => Column::Null { len },
            LegacyColumn::EmptyArray { len } => Column::EmptyArray { len },
            LegacyColumn::EmptyMap { len } => Column::EmptyMap { len },
            LegacyColumn::Number(num_col) => Column::Number(num_col),
            LegacyColumn::Decimal(dec_col) => Column::Decimal(dec_col),
            LegacyColumn::Boolean(bmp) => Column::Boolean(bmp),
            LegacyColumn::String(str_col) => Column::String(str_col.try_into().unwrap()),
            LegacyColumn::Timestamp(buf) => Column::Timestamp(buf),
            LegacyColumn::Date(buf) => Column::Date(buf),
            LegacyColumn::Array(arr_col) => Column::Array(Box::new(ArrayColumn::<AnyType> {
                values: arr_col.values.into(),
                offsets: arr_col.offsets,
            })),
            LegacyColumn::Map(map_col) => Column::Map(Box::new(ArrayColumn::<AnyType> {
                values: map_col.values.into(),
                offsets: map_col.offsets,
            })),
            LegacyColumn::Bitmap(str_col) => Column::Bitmap(str_col),
            LegacyColumn::Nullable(nullable_col) => {
                Column::Nullable(Box::new(NullableColumn::<AnyType> {
                    column: nullable_col.column.into(),
                    validity: nullable_col.validity,
                }))
            }
            LegacyColumn::Tuple(tuple) => {
                Column::Tuple(tuple.into_iter().map(|c| c.into()).collect())
            }
            LegacyColumn::Variant(variant) => Column::Variant(variant),
        }
    }
}

impl From<Scalar> for LegacyScalar {
    fn from(value: Scalar) -> Self {
        match value {
            Scalar::Null => LegacyScalar::Null,
            Scalar::EmptyArray => LegacyScalar::EmptyArray,
            Scalar::EmptyMap => LegacyScalar::EmptyMap,
            Scalar::Number(num_scalar) => LegacyScalar::Number(num_scalar),
            Scalar::Decimal(dec_scalar) => LegacyScalar::Decimal(dec_scalar),
            Scalar::Timestamp(ts) => LegacyScalar::Timestamp(ts),
            Scalar::Date(date) => LegacyScalar::Date(date),
            Scalar::Boolean(b) => LegacyScalar::Boolean(b),
            Scalar::Binary(_) | Scalar::Geometry(_) | Scalar::Geography(_) => unreachable!(),
            Scalar::String(string) => LegacyScalar::String(string.as_bytes().to_vec()),
            Scalar::Array(column) => LegacyScalar::Array(column.into()),
            Scalar::Map(column) => LegacyScalar::Map(column.into()),
            Scalar::Bitmap(bitmap) => LegacyScalar::Bitmap(bitmap),
            Scalar::Tuple(tuple) => LegacyScalar::Tuple(tuple),
            Scalar::Variant(variant) => LegacyScalar::Variant(variant),
        }
    }
}

impl From<Column> for LegacyColumn {
    fn from(value: Column) -> Self {
        match value {
            Column::Null { len } => LegacyColumn::Null { len },
            Column::EmptyArray { len } => LegacyColumn::EmptyArray { len },
            Column::EmptyMap { len } => LegacyColumn::EmptyMap { len },
            Column::Number(num_col) => LegacyColumn::Number(num_col),
            Column::Decimal(dec_col) => LegacyColumn::Decimal(dec_col),
            Column::Boolean(bmp) => LegacyColumn::Boolean(bmp),
            Column::Binary(_) | Column::Geometry(_) | Column::Geography(_) => unreachable!(),
            Column::String(str_col) => LegacyColumn::String(str_col.into()),
            Column::Timestamp(buf) => LegacyColumn::Timestamp(buf),
            Column::Date(buf) => LegacyColumn::Date(buf),
            Column::Array(arr_col) => LegacyColumn::Array(Box::new(LegacyArrayColumn {
                values: arr_col.values.into(),
                offsets: arr_col.offsets,
            })),
            Column::Map(map_col) => LegacyColumn::Map(Box::new(LegacyArrayColumn {
                values: map_col.values.into(),
                offsets: map_col.offsets,
            })),
            Column::Bitmap(str_col) => LegacyColumn::Bitmap(str_col),
            Column::Nullable(nullable_col) => {
                LegacyColumn::Nullable(Box::new(LegacyNullableColumn {
                    column: nullable_col.column.into(),
                    validity: nullable_col.validity,
                }))
            }
            Column::Tuple(tuple) => {
                LegacyColumn::Tuple(tuple.into_iter().map(|c| c.into()).collect())
            }
            Column::Variant(variant) => LegacyColumn::Variant(variant),
        }
    }
}

// Serialize a column to a base64 string.
// Because we may use serde::json/bincode to serialize the column, so we wrap it into string
impl Serialize for LegacyColumn {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        let c: Column = self.clone().into();

        Serialize::serialize(&c, serializer)
    }
}

impl<'de> Deserialize<'de> for LegacyColumn {
    fn deserialize<D>(deserializer: D) -> Result<LegacyColumn, D::Error>
    where D: Deserializer<'de> {
        let c: Column = Deserialize::deserialize(deserializer)?;
        Ok(c.into())
    }
}
