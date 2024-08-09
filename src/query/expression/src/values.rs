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

use std::cmp::Ordering;
use std::hash::Hash;
use std::io::Read;
use std::io::Write;
use std::ops::Range;

use base64::engine::general_purpose;
use base64::prelude::*;
use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::bitmap::MutableBitmap;
use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_arrow::arrow::trusted_len::TrustedLen;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_io::prelude::BinaryRead;
use enum_as_inner::EnumAsInner;
use ethnum::i256;
use geo::Geometry;
use geo::Point;
use geozero::CoordDimensions;
use geozero::ToWkb;
use itertools::Itertools;
use ordered_float::OrderedFloat;
use roaring::RoaringTreemap;
use serde::de::Visitor;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

use crate::property::Domain;
use crate::types::array::ArrayColumn;
use crate::types::array::ArrayColumnBuilder;
use crate::types::binary::BinaryColumn;
use crate::types::binary::BinaryColumnBuilder;
use crate::types::bitmap::BitmapType;
use crate::types::boolean::BooleanDomain;
use crate::types::date::DATE_MAX;
use crate::types::date::DATE_MIN;
use crate::types::decimal::Decimal;
use crate::types::decimal::DecimalColumn;
use crate::types::decimal::DecimalColumnBuilder;
use crate::types::decimal::DecimalDataType;
use crate::types::decimal::DecimalScalar;
use crate::types::decimal::DecimalSize;
use crate::types::decimal::DecimalType;
use crate::types::geography::Geography;
use crate::types::geography::GeographyColumn;
use crate::types::geography::GeographyColumnBuilder;
use crate::types::geography::GeographyRef;
use crate::types::geometry::compare_geometry;
use crate::types::geometry::GeometryType;
use crate::types::nullable::NullableColumn;
use crate::types::nullable::NullableColumnBuilder;
use crate::types::nullable::NullableColumnVec;
use crate::types::nullable::NullableDomain;
use crate::types::number::NumberColumn;
use crate::types::number::NumberColumnBuilder;
use crate::types::number::NumberScalar;
use crate::types::number::SimpleDomain;
use crate::types::number::F32;
use crate::types::number::F64;
use crate::types::string::StringColumn;
use crate::types::string::StringColumnBuilder;
use crate::types::string::StringDomain;
use crate::types::timestamp::check_timestamp;
use crate::types::timestamp::TIMESTAMP_MAX;
use crate::types::timestamp::TIMESTAMP_MIN;
use crate::types::variant::JSONB_NULL;
use crate::types::*;
use crate::utils::arrow::append_bitmap;
use crate::utils::arrow::bitmap_into_mut;
use crate::utils::arrow::buffer_into_mut;
use crate::utils::arrow::deserialize_column;
use crate::utils::arrow::serialize_column;
use crate::utils::FromData;
use crate::values::decimal::DecimalColumnVec;
use crate::values::map::KvPair;
use crate::with_decimal_mapped_type;
use crate::with_decimal_type;
use crate::with_number_mapped_type;
use crate::with_number_type;

#[derive(Debug, Clone, PartialEq, EnumAsInner)]
pub enum Value<T: ValueType> {
    Scalar(T::Scalar),
    Column(T::Column),
}

#[derive(Debug, Clone, PartialEq, EnumAsInner)]
pub enum ValueRef<'a, T: ValueType> {
    Scalar(T::ScalarRef<'a>),
    Column(T::Column),
}

#[derive(
    Debug, Clone, EnumAsInner, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize,
)]
pub enum Scalar {
    Null,
    EmptyArray,
    EmptyMap,
    Number(NumberScalar),
    Decimal(DecimalScalar),
    Timestamp(i64),
    Date(i32),
    Boolean(bool),
    Binary(Vec<u8>),
    String(String),
    Array(Column),
    Map(Column),
    Bitmap(Vec<u8>),
    Tuple(Vec<Scalar>),
    Variant(Vec<u8>),
    Geometry(Vec<u8>),
    Geography(Geography),
}

#[derive(Clone, Default, Eq, EnumAsInner)]
pub enum ScalarRef<'a> {
    #[default]
    Null,
    EmptyArray,
    EmptyMap,
    Number(NumberScalar),
    Decimal(DecimalScalar),
    Boolean(bool),
    Binary(&'a [u8]),
    String(&'a str),
    Timestamp(i64),
    Date(i32),
    Array(Column),
    Map(Column),
    Bitmap(&'a [u8]),
    Tuple(Vec<ScalarRef<'a>>),
    Variant(&'a [u8]),
    Geometry(&'a [u8]),
    Geography(GeographyRef<'a>),
}

#[derive(Clone, EnumAsInner)]
pub enum Column {
    Null { len: usize },
    EmptyArray { len: usize },
    EmptyMap { len: usize },
    Number(NumberColumn),
    Decimal(DecimalColumn),
    Boolean(Bitmap),
    Binary(BinaryColumn),
    String(StringColumn),
    Timestamp(Buffer<i64>),
    Date(Buffer<i32>),
    Array(Box<ArrayColumn<AnyType>>),
    Map(Box<ArrayColumn<AnyType>>),
    Bitmap(BinaryColumn),
    Nullable(Box<NullableColumn<AnyType>>),
    Tuple(Vec<Column>),
    Variant(BinaryColumn),
    Geometry(BinaryColumn),
    Geography(GeographyColumn),
}

#[derive(Clone, EnumAsInner, Debug, PartialEq)]
pub enum ColumnVec {
    Null,
    EmptyArray,
    EmptyMap,
    Number(NumberColumnVec),
    Decimal(DecimalColumnVec),
    Boolean(Vec<Bitmap>),
    Binary(Vec<BinaryColumn>),
    String(Vec<StringColumn>),
    Timestamp(Vec<Buffer<i64>>),
    Date(Vec<Buffer<i32>>),
    Array(Vec<ArrayColumn<AnyType>>),
    Map(Vec<ArrayColumn<KvPair<AnyType, AnyType>>>),
    Bitmap(Vec<BinaryColumn>),
    Nullable(Box<NullableColumnVec>),
    Tuple(Vec<ColumnVec>),
    Variant(Vec<BinaryColumn>),
    Geometry(Vec<BinaryColumn>),
    Geography(Vec<GeographyColumn>),
}

#[derive(Debug, Clone, EnumAsInner)]
pub enum ColumnBuilder {
    Null { len: usize },
    EmptyArray { len: usize },
    EmptyMap { len: usize },
    Number(NumberColumnBuilder),
    Decimal(DecimalColumnBuilder),
    Boolean(MutableBitmap),
    Binary(BinaryColumnBuilder),
    String(StringColumnBuilder),
    Timestamp(Vec<i64>),
    Date(Vec<i32>),
    Array(Box<ArrayColumnBuilder<AnyType>>),
    Map(Box<ArrayColumnBuilder<AnyType>>),
    Bitmap(BinaryColumnBuilder),
    Nullable(Box<NullableColumnBuilder<AnyType>>),
    Tuple(Vec<ColumnBuilder>),
    Variant(BinaryColumnBuilder),
    Geometry(BinaryColumnBuilder),
    Geography(GeographyColumnBuilder),
}

impl<'a, T: ValueType> ValueRef<'a, T> {
    pub fn to_owned(self) -> Value<T> {
        match self {
            ValueRef::Scalar(scalar) => Value::Scalar(T::to_owned_scalar(scalar)),
            ValueRef::Column(col) => Value::Column(col),
        }
    }

    pub fn semantically_eq(&'a self, other: &'a Self) -> bool {
        match (self, other) {
            (ValueRef::Scalar(s1), ValueRef::Scalar(s2)) => s1 == s2,
            (ValueRef::Column(c1), ValueRef::Column(c2)) => c1 == c2,
            (ValueRef::Scalar(s), ValueRef::Column(c))
            | (ValueRef::Column(c), ValueRef::Scalar(s)) => {
                T::iter_column(c).all(|scalar| scalar == *s)
            }
        }
    }

    pub fn len(&'a self) -> usize {
        match self {
            ValueRef::Scalar(_) => 1,
            ValueRef::Column(col) => T::column_len(col),
        }
    }

    pub fn index(&'a self, index: usize) -> Option<T::ScalarRef<'a>> {
        match self {
            ValueRef::Scalar(scalar) => Some(scalar.clone()),
            ValueRef::Column(col) => T::index_column(col, index),
        }
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    pub unsafe fn index_unchecked(&'a self, index: usize) -> T::ScalarRef<'a> {
        match self {
            ValueRef::Scalar(scalar) => scalar.clone(),
            ValueRef::Column(c) => T::index_column_unchecked(c, index),
        }
    }

    pub fn memory_size(&'a self) -> usize {
        match self {
            ValueRef::Scalar(scalar) => T::scalar_memory_size(scalar),
            ValueRef::Column(c) => T::column_memory_size(c),
        }
    }
}

impl<'a, T: ValueType> Value<T> {
    pub fn as_ref(&'a self) -> ValueRef<'a, T> {
        match self {
            Value::Scalar(scalar) => ValueRef::Scalar(T::to_scalar_ref(scalar)),
            Value::Column(col) => ValueRef::Column(col.clone()),
        }
    }

    pub fn index(&'a self, index: usize) -> Option<T::ScalarRef<'a>> {
        match self {
            Value::Scalar(scalar) => Some(T::to_scalar_ref(scalar)),
            Value::Column(col) => T::index_column(col, index),
        }
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    pub unsafe fn index_unchecked(&'a self, index: usize) -> T::ScalarRef<'a> {
        match self {
            Value::Scalar(scalar) => T::to_scalar_ref(scalar),
            Value::Column(c) => T::index_column_unchecked(c, index),
        }
    }
}

impl<T: ArgType> Value<T> {
    pub fn upcast(self) -> Value<AnyType> {
        match self {
            Value::Scalar(scalar) => Value::Scalar(T::upcast_scalar(scalar)),
            Value::Column(col) => Value::Column(T::upcast_column(col)),
        }
    }
}

impl<T: Decimal> Value<DecimalType<T>> {
    pub fn upcast_decimal(self, size: DecimalSize) -> Value<AnyType> {
        match self {
            Value::Scalar(scalar) => Value::Scalar(T::upcast_scalar(scalar, size)),
            Value::Column(col) => Value::Column(T::upcast_column(col, size)),
        }
    }
}

impl Value<AnyType> {
    pub fn convert_to_full_column(&self, ty: &DataType, num_rows: usize) -> Column {
        match self {
            Value::Scalar(s) => {
                let builder = ColumnBuilder::repeat(&s.as_ref(), num_rows, ty);
                builder.build()
            }
            Value::Column(c) => c.clone(),
        }
    }

    pub fn try_downcast<T: ValueType>(&self) -> Option<Value<T>> {
        Some(self.as_ref().try_downcast::<T>()?.to_owned())
    }

    pub fn wrap_nullable(self, validity: Option<Bitmap>) -> Self {
        match self {
            Value::Column(c) => Value::Column(c.wrap_nullable(validity)),
            scalar => scalar,
        }
    }
}

impl<'a> ValueRef<'a, AnyType> {
    pub fn try_downcast<T: ValueType>(&self) -> Option<ValueRef<'_, T>> {
        Some(match self {
            ValueRef::Scalar(scalar) => ValueRef::Scalar(T::try_downcast_scalar(scalar)?),
            ValueRef::Column(col) => ValueRef::Column(T::try_downcast_column(col)?),
        })
    }

    pub fn domain(&self, data_type: &DataType) -> Domain {
        match self {
            ValueRef::Scalar(scalar) => scalar.domain(data_type),
            ValueRef::Column(col) => col.domain(),
        }
    }
}

impl Scalar {
    pub fn as_ref(&self) -> ScalarRef {
        match self {
            Scalar::Null => ScalarRef::Null,
            Scalar::EmptyArray => ScalarRef::EmptyArray,
            Scalar::EmptyMap => ScalarRef::EmptyMap,
            Scalar::Number(n) => ScalarRef::Number(*n),
            Scalar::Decimal(d) => ScalarRef::Decimal(*d),
            Scalar::Boolean(b) => ScalarRef::Boolean(*b),
            Scalar::Binary(s) => ScalarRef::Binary(s.as_slice()),
            Scalar::String(s) => ScalarRef::String(s.as_str()),
            Scalar::Timestamp(t) => ScalarRef::Timestamp(*t),
            Scalar::Date(d) => ScalarRef::Date(*d),
            Scalar::Array(col) => ScalarRef::Array(col.clone()),
            Scalar::Map(col) => ScalarRef::Map(col.clone()),
            Scalar::Bitmap(b) => ScalarRef::Bitmap(b.as_slice()),
            Scalar::Tuple(fields) => ScalarRef::Tuple(fields.iter().map(Scalar::as_ref).collect()),
            Scalar::Variant(s) => ScalarRef::Variant(s.as_slice()),
            Scalar::Geometry(s) => ScalarRef::Geometry(s.as_slice()),
            Scalar::Geography(g) => ScalarRef::Geography(g.as_ref()),
        }
    }

    pub fn default_value(ty: &DataType) -> Scalar {
        match ty {
            DataType::Null => Scalar::Null,
            DataType::EmptyArray => Scalar::EmptyArray,
            DataType::EmptyMap => Scalar::EmptyMap,
            DataType::Boolean => Scalar::Boolean(false),
            DataType::Binary => Scalar::Binary(Vec::new()),
            DataType::String => Scalar::String(String::new()),
            DataType::Number(num_ty) => Scalar::Number(match num_ty {
                NumberDataType::UInt8 => NumberScalar::UInt8(0),
                NumberDataType::UInt16 => NumberScalar::UInt16(0),
                NumberDataType::UInt32 => NumberScalar::UInt32(0),
                NumberDataType::UInt64 => NumberScalar::UInt64(0),
                NumberDataType::Int8 => NumberScalar::Int8(0),
                NumberDataType::Int16 => NumberScalar::Int16(0),
                NumberDataType::Int32 => NumberScalar::Int32(0),
                NumberDataType::Int64 => NumberScalar::Int64(0),
                NumberDataType::Float32 => NumberScalar::Float32(OrderedFloat(0.0)),
                NumberDataType::Float64 => NumberScalar::Float64(OrderedFloat(0.0)),
            }),
            DataType::Decimal(ty) => Scalar::Decimal(ty.default_scalar()),
            DataType::Timestamp => Scalar::Timestamp(0),
            DataType::Date => Scalar::Date(0),
            DataType::Nullable(_) => Scalar::Null,
            DataType::Array(ty) => {
                let builder = ColumnBuilder::with_capacity(ty, 0);
                let col = builder.build();
                Scalar::Array(col)
            }
            DataType::Map(ty) => {
                let builder = ColumnBuilder::with_capacity(ty, 0);
                let col = builder.build();
                Scalar::Map(col)
            }
            DataType::Bitmap => Scalar::Bitmap(vec![]),
            DataType::Tuple(tys) => Scalar::Tuple(tys.iter().map(Scalar::default_value).collect()),
            DataType::Variant => Scalar::Variant(vec![]),
            DataType::Geometry => Scalar::Geometry(vec![]),
            DataType::Geography => Scalar::Geography(Geography::default()),

            _ => unimplemented!(),
        }
    }

    pub fn is_nested_scalar(&self) -> bool {
        match self {
            Scalar::Null
            | Scalar::EmptyArray
            | Scalar::EmptyMap
            | Scalar::Number(_)
            | Scalar::Decimal(_)
            | Scalar::Timestamp(_)
            | Scalar::Date(_)
            | Scalar::Boolean(_)
            | Scalar::Binary(_)
            | Scalar::String(_)
            | Scalar::Bitmap(_)
            | Scalar::Variant(_)
            | Scalar::Geometry(_)
            | Scalar::Geography(_) => false,
            Scalar::Array(_) | Scalar::Map(_) | Scalar::Tuple(_) => true,
        }
    }

    pub fn is_positive(&self) -> bool {
        match self {
            Scalar::Number(n) => n.is_positive(),
            Scalar::Decimal(d) => d.is_positive(),
            Scalar::Timestamp(t) => *t > 0,
            Scalar::Date(d) => *d > 0,
            _ => unreachable!("is_positive() called on non-numeric scalar"),
        }
    }

    pub fn get_i64(&self) -> Option<i64> {
        match self {
            Scalar::Number(n) => match n {
                NumberScalar::Int8(x) => Some(*x as _),
                NumberScalar::Int16(x) => Some(*x as _),
                NumberScalar::Int32(x) => Some(*x as _),
                NumberScalar::Int64(x) => Some(*x as _),
                NumberScalar::UInt8(x) => Some(*x as _),
                NumberScalar::UInt16(x) => Some(*x as _),
                NumberScalar::UInt32(x) => Some(*x as _),
                NumberScalar::UInt64(x) => i64::try_from(*x).ok(),
                _ => None,
            },
            _ => None,
        }
    }
}

impl<'a> ScalarRef<'a> {
    pub fn to_owned(&self) -> Scalar {
        match self {
            ScalarRef::Null => Scalar::Null,
            ScalarRef::EmptyArray => Scalar::EmptyArray,
            ScalarRef::EmptyMap => Scalar::EmptyMap,
            ScalarRef::Number(n) => Scalar::Number(*n),
            ScalarRef::Decimal(d) => Scalar::Decimal(*d),
            ScalarRef::Boolean(b) => Scalar::Boolean(*b),
            ScalarRef::Binary(s) => Scalar::Binary(s.to_vec()),
            ScalarRef::String(s) => Scalar::String(s.to_string()),
            ScalarRef::Timestamp(t) => Scalar::Timestamp(*t),
            ScalarRef::Date(d) => Scalar::Date(*d),
            ScalarRef::Array(col) => Scalar::Array(col.clone()),
            ScalarRef::Map(col) => Scalar::Map(col.clone()),
            ScalarRef::Bitmap(b) => Scalar::Bitmap(b.to_vec()),
            ScalarRef::Tuple(fields) => {
                Scalar::Tuple(fields.iter().map(ScalarRef::to_owned).collect())
            }
            ScalarRef::Variant(s) => Scalar::Variant(s.to_vec()),
            ScalarRef::Geometry(s) => Scalar::Geometry(s.to_vec()),
            ScalarRef::Geography(s) => Scalar::Geography(s.to_owned()),
        }
    }

    pub fn domain(&self, data_type: &DataType) -> Domain {
        if !self.is_null() {
            if let DataType::Nullable(ty) = data_type {
                let domain = self.domain(ty.as_ref());
                return Domain::Nullable(NullableDomain {
                    has_null: false,
                    value: Some(Box::new(domain)),
                });
            }
        }

        match self {
            ScalarRef::Null => Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            }),
            ScalarRef::EmptyArray => Domain::Array(None),
            ScalarRef::EmptyMap => Domain::Map(None),
            ScalarRef::Number(num) => Domain::Number(num.domain()),
            ScalarRef::Decimal(dec) => Domain::Decimal(dec.domain()),
            ScalarRef::Boolean(true) => Domain::Boolean(BooleanDomain {
                has_false: false,
                has_true: true,
            }),
            ScalarRef::Boolean(false) => Domain::Boolean(BooleanDomain {
                has_false: true,
                has_true: false,
            }),
            ScalarRef::String(s) => Domain::String(StringDomain {
                min: s.to_string(),
                max: Some(s.to_string()),
            }),
            ScalarRef::Timestamp(t) => Domain::Timestamp(SimpleDomain { min: *t, max: *t }),
            ScalarRef::Date(d) => Domain::Date(SimpleDomain { min: *d, max: *d }),
            ScalarRef::Array(array) => {
                if array.len() == 0 {
                    Domain::Array(None)
                } else {
                    Domain::Array(Some(Box::new(array.domain())))
                }
            }
            ScalarRef::Map(map) => {
                if map.len() == 0 {
                    Domain::Map(None)
                } else {
                    Domain::Map(Some(Box::new(map.domain())))
                }
            }
            ScalarRef::Tuple(fields) => {
                let types = data_type.as_tuple().unwrap();
                Domain::Tuple(
                    fields
                        .iter()
                        .zip(types.iter())
                        .map(|(field, data_type)| field.domain(data_type))
                        .collect(),
                )
            }
            ScalarRef::Binary(_)
            | ScalarRef::Bitmap(_)
            | ScalarRef::Variant(_)
            | ScalarRef::Geometry(_)
            | ScalarRef::Geography(_) => Domain::Undefined,
        }
    }

    pub fn memory_size(&self) -> usize {
        match self {
            ScalarRef::Null | ScalarRef::EmptyArray | ScalarRef::EmptyMap => 0,
            ScalarRef::Number(NumberScalar::UInt8(_)) => 1,
            ScalarRef::Number(NumberScalar::UInt16(_)) => 2,
            ScalarRef::Number(NumberScalar::UInt32(_)) => 4,
            ScalarRef::Number(NumberScalar::UInt64(_)) => 8,
            ScalarRef::Number(NumberScalar::Float32(_)) => 4,
            ScalarRef::Number(NumberScalar::Float64(_)) => 8,
            ScalarRef::Number(NumberScalar::Int8(_)) => 1,
            ScalarRef::Number(NumberScalar::Int16(_)) => 2,
            ScalarRef::Number(NumberScalar::Int32(_)) => 4,
            ScalarRef::Number(NumberScalar::Int64(_)) => 8,
            ScalarRef::Decimal(DecimalScalar::Decimal128(_, _)) => 16,
            ScalarRef::Decimal(DecimalScalar::Decimal256(_, _)) => 32,
            ScalarRef::Boolean(_) => 1,
            ScalarRef::Binary(s) => s.len(),
            ScalarRef::String(s) => s.len(),
            ScalarRef::Timestamp(_) => 8,
            ScalarRef::Date(_) => 4,
            ScalarRef::Array(col) => col.memory_size(),
            ScalarRef::Map(col) => col.memory_size(),
            ScalarRef::Bitmap(b) => b.len(),
            ScalarRef::Tuple(scalars) => scalars.iter().map(|s| s.memory_size()).sum(),
            ScalarRef::Variant(buf) => buf.len(),
            ScalarRef::Geometry(buf) => buf.len(),
            ScalarRef::Geography(s) => s.0.memory_size(),
        }
    }

    /// Infer the data type of the scalar.
    /// If the scalar is Null, the data type is `DataType::Null`,
    /// otherwise, the inferred data type is non-nullable.
    pub fn infer_data_type(&self) -> DataType {
        match self {
            ScalarRef::Null => DataType::Null,
            ScalarRef::EmptyArray => DataType::EmptyArray,
            ScalarRef::EmptyMap => DataType::EmptyMap,
            ScalarRef::Number(s) => DataType::Number(s.data_type()),
            ScalarRef::Decimal(s) => with_decimal_type!(|DECIMAL_TYPE| match s {
                DecimalScalar::DECIMAL_TYPE(_, size) =>
                    DataType::Decimal(DecimalDataType::DECIMAL_TYPE(*size)),
            }),
            ScalarRef::Boolean(_) => DataType::Boolean,
            ScalarRef::Binary(_) => DataType::Binary,
            ScalarRef::String(_) => DataType::String,
            ScalarRef::Timestamp(_) => DataType::Timestamp,
            ScalarRef::Date(_) => DataType::Date,
            ScalarRef::Array(array) => DataType::Array(Box::new(array.data_type())),
            ScalarRef::Map(col) => DataType::Map(Box::new(col.data_type())),
            ScalarRef::Bitmap(_) => DataType::Bitmap,
            ScalarRef::Tuple(fields) => {
                let inner = fields
                    .iter()
                    .map(|field| field.infer_data_type())
                    .collect::<Vec<_>>();
                DataType::Tuple(inner)
            }
            ScalarRef::Variant(_) => DataType::Variant,
            ScalarRef::Geometry(_) => DataType::Geometry,
            ScalarRef::Geography(_) => DataType::Geography,
        }
    }

    /// Infer the common data type of the scalar and the other scalar.
    ///
    /// If both of the scalar is Null, the data type is `DataType::Null`,
    /// and if one of the scalar is Null, the data type is nullable,
    /// otherwise, the inferred data type is non-nullable.
    ///
    /// Auto type cast is not considered in this method. For example,
    /// `infer_common_type` for a scalar of `UInt8` and a scalar of`UInt16`
    ///  will return `None`.
    pub fn infer_common_type(&self, other: &Self) -> Option<DataType> {
        match (self, other) {
            (ScalarRef::Null, ScalarRef::Null) => Some(DataType::Null),
            (ScalarRef::Null, other) | (other, ScalarRef::Null) => {
                Some(other.infer_data_type().wrap_nullable())
            }
            (ScalarRef::EmptyArray, ScalarRef::EmptyArray) => Some(DataType::EmptyArray),
            (ScalarRef::EmptyMap, ScalarRef::EmptyMap) => Some(DataType::EmptyMap),
            (ScalarRef::Number(s1), ScalarRef::Number(s2)) => {
                with_number_type!(|NUM_TYPE| match (s1, s2) {
                    (NumberScalar::NUM_TYPE(_), NumberScalar::NUM_TYPE(_)) => {
                        Some(DataType::Number(NumberDataType::NUM_TYPE))
                    }
                    _ => None,
                })
            }
            (ScalarRef::Decimal(s1), ScalarRef::Decimal(s2)) => {
                with_decimal_type!(|DECIMAL_TYPE| match (s1, s2) {
                    (
                        DecimalScalar::DECIMAL_TYPE(_, size1),
                        DecimalScalar::DECIMAL_TYPE(_, size2),
                    ) => {
                        if size1 == size2 {
                            Some(DataType::Decimal(DecimalDataType::DECIMAL_TYPE(*size1)))
                        } else {
                            None
                        }
                    }
                    _ => None,
                })
            }
            (ScalarRef::Boolean(_), ScalarRef::Boolean(_)) => Some(DataType::Boolean),
            (ScalarRef::Binary(_), ScalarRef::Binary(_)) => Some(DataType::Binary),
            (ScalarRef::String(_), ScalarRef::String(_)) => Some(DataType::String),
            (ScalarRef::Timestamp(_), ScalarRef::Timestamp(_)) => Some(DataType::Timestamp),
            (ScalarRef::Date(_), ScalarRef::Date(_)) => Some(DataType::Date),
            (ScalarRef::Array(s1), ScalarRef::Array(s2)) if s1.data_type() == s2.data_type() => {
                Some(DataType::Array(Box::new(s1.data_type())))
            }
            (ScalarRef::Map(s1), ScalarRef::Map(s2)) if s1.data_type() == s2.data_type() => {
                Some(DataType::Map(Box::new(s1.data_type())))
            }
            (ScalarRef::Bitmap(_), ScalarRef::Bitmap(_)) => Some(DataType::Bitmap),
            (ScalarRef::Tuple(s1), ScalarRef::Tuple(s2)) => {
                let inner = s1
                    .iter()
                    .zip(s2.iter())
                    .map(|(s1, s2)| s1.infer_common_type(s2))
                    .collect::<Option<Vec<_>>>()?;
                Some(DataType::Tuple(inner))
            }
            (ScalarRef::Variant(_), ScalarRef::Variant(_)) => Some(DataType::Variant),
            (ScalarRef::Geometry(_), ScalarRef::Geometry(_)) => Some(DataType::Geometry),
            (ScalarRef::Geography(_), ScalarRef::Geography(_)) => Some(DataType::Geography),
            _ => None,
        }
    }

    /// Check if the scalar is valid for the given data type.
    pub fn is_value_of_type(&self, data_type: &DataType) -> bool {
        match (self, data_type) {
            (ScalarRef::Null, DataType::Null) => true,
            (ScalarRef::Null, DataType::Nullable(_)) => true,
            _ => match (self, data_type.remove_nullable()) {
                (ScalarRef::EmptyArray, DataType::EmptyArray) => true,
                (ScalarRef::EmptyMap, DataType::EmptyMap) => true,
                (ScalarRef::Number(_), DataType::Number(_)) => true,
                (ScalarRef::Decimal(_), DataType::Decimal(_)) => true,
                (ScalarRef::Boolean(_), DataType::Boolean) => true,
                (ScalarRef::Binary(_), DataType::Binary) => true,
                (ScalarRef::String(_), DataType::String) => true,
                (ScalarRef::Timestamp(_), DataType::Timestamp) => true,
                (ScalarRef::Date(_), DataType::Date) => true,
                (ScalarRef::Bitmap(_), DataType::Bitmap) => true,
                (ScalarRef::Variant(_), DataType::Variant) => true,
                (ScalarRef::Geometry(_), DataType::Geometry) => true,
                (ScalarRef::Geography(_), DataType::Geography) => true,
                (ScalarRef::Array(val), DataType::Array(ty)) => val.data_type() == *ty,
                (ScalarRef::Map(val), DataType::Map(ty)) => val.data_type() == *ty,
                (ScalarRef::Tuple(val), DataType::Tuple(ty)) => {
                    if val.len() != ty.len() {
                        return false;
                    }
                    val.iter()
                        .zip(ty)
                        .all(|(val, ty)| val.is_value_of_type(&ty))
                }
                _ => false,
            },
        }
    }
}

impl PartialOrd for Scalar {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Scalar::Null, Scalar::Null) => Some(Ordering::Equal),
            (Scalar::EmptyArray, Scalar::EmptyArray) => Some(Ordering::Equal),
            (Scalar::EmptyMap, Scalar::EmptyMap) => Some(Ordering::Equal),
            (Scalar::Number(n1), Scalar::Number(n2)) => n1.partial_cmp(n2),
            (Scalar::Decimal(d1), Scalar::Decimal(d2)) => d1.partial_cmp(d2),
            (Scalar::Boolean(b1), Scalar::Boolean(b2)) => b1.partial_cmp(b2),
            (Scalar::Binary(s1), Scalar::Binary(s2)) => s1.partial_cmp(s2),
            (Scalar::String(s1), Scalar::String(s2)) => s1.partial_cmp(s2),
            (Scalar::Timestamp(t1), Scalar::Timestamp(t2)) => t1.partial_cmp(t2),
            (Scalar::Date(d1), Scalar::Date(d2)) => d1.partial_cmp(d2),
            (Scalar::Array(a1), Scalar::Array(a2)) => a1.partial_cmp(a2),
            (Scalar::Map(m1), Scalar::Map(m2)) => m1.partial_cmp(m2),
            (Scalar::Bitmap(b1), Scalar::Bitmap(b2)) => b1.partial_cmp(b2),
            (Scalar::Tuple(t1), Scalar::Tuple(t2)) => t1.partial_cmp(t2),
            (Scalar::Variant(v1), Scalar::Variant(v2)) => {
                jsonb::compare(v1.as_slice(), v2.as_slice()).ok()
            }
            (Scalar::Geometry(g1), Scalar::Geometry(g2)) => compare_geometry(g1, g2),
            (Scalar::Geography(g1), Scalar::Geography(g2)) => g1.partial_cmp(g2),
            _ => None,
        }
    }
}

impl Ord for Scalar {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

impl PartialEq for Scalar {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other) == Some(Ordering::Equal)
    }
}

impl<'a, 'b> PartialOrd<ScalarRef<'b>> for ScalarRef<'a> {
    fn partial_cmp(&self, other: &ScalarRef<'b>) -> Option<Ordering> {
        match (self, other) {
            (ScalarRef::Null, ScalarRef::Null) => Some(Ordering::Equal),
            (ScalarRef::EmptyArray, ScalarRef::EmptyArray) => Some(Ordering::Equal),
            (ScalarRef::EmptyMap, ScalarRef::EmptyMap) => Some(Ordering::Equal),
            (ScalarRef::Number(n1), ScalarRef::Number(n2)) => n1.partial_cmp(n2),
            (ScalarRef::Decimal(d1), ScalarRef::Decimal(d2)) => d1.partial_cmp(d2),
            (ScalarRef::Boolean(b1), ScalarRef::Boolean(b2)) => b1.partial_cmp(b2),
            (ScalarRef::Binary(s1), ScalarRef::Binary(s2)) => s1.partial_cmp(s2),
            (ScalarRef::String(s1), ScalarRef::String(s2)) => s1.partial_cmp(s2),
            (ScalarRef::Timestamp(t1), ScalarRef::Timestamp(t2)) => t1.partial_cmp(t2),
            (ScalarRef::Date(d1), ScalarRef::Date(d2)) => d1.partial_cmp(d2),
            (ScalarRef::Array(a1), ScalarRef::Array(a2)) => a1.partial_cmp(a2),
            (ScalarRef::Map(m1), ScalarRef::Map(m2)) => m1.partial_cmp(m2),
            (ScalarRef::Bitmap(b1), ScalarRef::Bitmap(b2)) => b1.partial_cmp(b2),
            (ScalarRef::Tuple(t1), ScalarRef::Tuple(t2)) => t1.partial_cmp(t2),
            (ScalarRef::Variant(v1), ScalarRef::Variant(v2)) => jsonb::compare(v1, v2).ok(),
            (ScalarRef::Geometry(g1), ScalarRef::Geometry(g2)) => compare_geometry(g1, g2),
            (ScalarRef::Geography(g1), ScalarRef::Geography(g2)) => g1.partial_cmp(g2),

            // By default, null is biggest in pgsql
            (ScalarRef::Null, _) => Some(Ordering::Greater),
            (_, ScalarRef::Null) => Some(Ordering::Less),
            _ => None,
        }
    }
}

impl Ord for ScalarRef<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

impl<'a, 'b> PartialEq<ScalarRef<'b>> for ScalarRef<'a> {
    fn eq(&self, other: &ScalarRef<'b>) -> bool {
        self.partial_cmp(other) == Some(Ordering::Equal)
    }
}

impl Hash for ScalarRef<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            ScalarRef::Null | ScalarRef::EmptyArray | ScalarRef::EmptyMap => {}
            ScalarRef::Number(t) => with_number_type!(|NUM_TYPE| match t {
                NumberScalar::NUM_TYPE(v) => {
                    v.hash(state);
                }
            }),
            ScalarRef::Decimal(t) => with_decimal_type!(|DECIMAL_TYPE| match t {
                DecimalScalar::DECIMAL_TYPE(v, _) => {
                    v.hash(state);
                }
            }),
            ScalarRef::Boolean(v) => v.hash(state),
            ScalarRef::Binary(v) => v.hash(state),
            ScalarRef::String(v) => v.hash(state),
            ScalarRef::Timestamp(v) => v.hash(state),
            ScalarRef::Date(v) => v.hash(state),
            ScalarRef::Array(v) => {
                let str = serialize_column(v);
                str.hash(state);
            }
            ScalarRef::Map(v) => {
                let str = serialize_column(v);
                str.hash(state);
            }
            ScalarRef::Bitmap(v) => v.hash(state),
            ScalarRef::Tuple(v) => {
                v.hash(state);
            }
            ScalarRef::Variant(v) => v.hash(state),
            ScalarRef::Geometry(v) => v.hash(state),
            ScalarRef::Geography(v) => v.hash(state),
        }
    }
}

impl Hash for Scalar {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.as_ref().hash(state);
    }
}

impl PartialOrd for Column {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Column::Null { len: col1 }, Column::Null { len: col2 }) => col1.partial_cmp(col2),
            (Column::EmptyArray { len: col1 }, Column::EmptyArray { len: col2 }) => {
                col1.partial_cmp(col2)
            }
            (Column::EmptyMap { len: col1 }, Column::EmptyMap { len: col2 }) => {
                col1.partial_cmp(col2)
            }
            (Column::Number(col1), Column::Number(col2)) => col1.partial_cmp(col2),
            (Column::Decimal(col1), Column::Decimal(col2)) => col1.partial_cmp(col2),
            (Column::Boolean(col1), Column::Boolean(col2)) => col1.iter().partial_cmp(col2.iter()),
            (Column::Binary(col1), Column::Binary(col2)) => col1.iter().partial_cmp(col2.iter()),
            (Column::String(col1), Column::String(col2)) => col1.iter().partial_cmp(col2.iter()),
            (Column::Timestamp(col1), Column::Timestamp(col2)) => {
                col1.iter().partial_cmp(col2.iter())
            }
            (Column::Date(col1), Column::Date(col2)) => col1.iter().partial_cmp(col2.iter()),
            (Column::Array(col1), Column::Array(col2)) => col1.iter().partial_cmp(col2.iter()),
            (Column::Map(col1), Column::Map(col2)) => col1.iter().partial_cmp(col2.iter()),
            (Column::Bitmap(col1), Column::Bitmap(col2)) => col1.iter().partial_cmp(col2.iter()),
            (Column::Nullable(col1), Column::Nullable(col2)) => {
                col1.iter().partial_cmp(col2.iter())
            }
            (Column::Tuple(fields1), Column::Tuple(fields2)) => fields1.partial_cmp(fields2),
            (Column::Variant(col1), Column::Variant(col2)) => col1
                .iter()
                .partial_cmp_by(col2.iter(), |v1, v2| jsonb::compare(v1, v2).ok()),
            (Column::Geometry(col1), Column::Geometry(col2)) => {
                col1.iter().partial_cmp_by(col2.iter(), compare_geometry)
            }
            (Column::Geography(col1), Column::Geography(col2)) => {
                col1.iter().partial_cmp(col2.iter())
            }
            _ => None,
        }
    }
}

impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other) == Some(Ordering::Equal)
    }
}

impl Column {
    pub fn len(&self) -> usize {
        match self {
            Column::Null { len } => *len,
            Column::EmptyArray { len } => *len,
            Column::EmptyMap { len } => *len,
            Column::Number(col) => col.len(),
            Column::Decimal(col) => col.len(),
            Column::Boolean(col) => col.len(),
            Column::Binary(col) => col.len(),
            Column::String(col) => col.len(),
            Column::Timestamp(col) => col.len(),
            Column::Date(col) => col.len(),
            Column::Array(col) => col.len(),
            Column::Map(col) => col.len(),
            Column::Bitmap(col) => col.len(),
            Column::Nullable(col) => col.len(),
            Column::Tuple(fields) => fields[0].len(),
            Column::Variant(col) => col.len(),
            Column::Geometry(col) => col.len(),
            Column::Geography(col) => col.len(),
        }
    }

    pub fn index(&self, index: usize) -> Option<ScalarRef> {
        match self {
            Column::Null { .. } => Some(ScalarRef::Null),
            Column::EmptyArray { .. } => Some(ScalarRef::EmptyArray),
            Column::EmptyMap { .. } => Some(ScalarRef::EmptyMap),
            Column::Number(col) => Some(ScalarRef::Number(col.index(index)?)),
            Column::Decimal(col) => Some(ScalarRef::Decimal(col.index(index)?)),
            Column::Boolean(col) => Some(ScalarRef::Boolean(col.get(index)?)),
            Column::Binary(col) => Some(ScalarRef::Binary(col.index(index)?)),
            Column::String(col) => Some(ScalarRef::String(col.index(index)?)),
            Column::Timestamp(col) => Some(ScalarRef::Timestamp(col.get(index).cloned()?)),
            Column::Date(col) => Some(ScalarRef::Date(col.get(index).cloned()?)),
            Column::Array(col) => Some(ScalarRef::Array(col.index(index)?)),
            Column::Map(col) => Some(ScalarRef::Map(col.index(index)?)),
            Column::Bitmap(col) => Some(ScalarRef::Bitmap(col.index(index)?)),
            Column::Nullable(col) => Some(col.index(index)?.unwrap_or(ScalarRef::Null)),
            Column::Tuple(fields) => Some(ScalarRef::Tuple(
                fields
                    .iter()
                    .map(|field| field.index(index))
                    .collect::<Option<Vec<_>>>()?,
            )),
            Column::Variant(col) => Some(ScalarRef::Variant(col.index(index)?)),
            Column::Geometry(col) => Some(ScalarRef::Geometry(col.index(index)?)),
            Column::Geography(col) => Some(ScalarRef::Geography(col.index(index)?)),
        }
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    pub unsafe fn index_unchecked(&self, index: usize) -> ScalarRef {
        match self {
            Column::Null { .. } => ScalarRef::Null,
            Column::EmptyArray { .. } => ScalarRef::EmptyArray,
            Column::EmptyMap { .. } => ScalarRef::EmptyMap,
            Column::Number(col) => ScalarRef::Number(col.index_unchecked(index)),
            Column::Decimal(col) => ScalarRef::Decimal(col.index_unchecked(index)),
            Column::Boolean(col) => ScalarRef::Boolean(col.get_bit_unchecked(index)),
            Column::Binary(col) => ScalarRef::Binary(col.index_unchecked(index)),
            Column::String(col) => ScalarRef::String(col.index_unchecked(index)),
            Column::Timestamp(col) => ScalarRef::Timestamp(*col.get_unchecked(index)),
            Column::Date(col) => ScalarRef::Date(*col.get_unchecked(index)),
            Column::Array(col) => ScalarRef::Array(col.index_unchecked(index)),
            Column::Map(col) => ScalarRef::Map(col.index_unchecked(index)),
            Column::Bitmap(col) => ScalarRef::Bitmap(col.index_unchecked(index)),
            Column::Nullable(col) => col.index_unchecked(index).unwrap_or(ScalarRef::Null),
            Column::Tuple(fields) => ScalarRef::Tuple(
                fields
                    .iter()
                    .map(|field| field.index_unchecked(index))
                    .collect::<Vec<_>>(),
            ),
            Column::Variant(col) => ScalarRef::Variant(col.index_unchecked(index)),
            Column::Geometry(col) => ScalarRef::Geometry(col.index_unchecked(index)),
            Column::Geography(col) => ScalarRef::Geography(col.index_unchecked(index)),
        }
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        assert!(
            range.end <= self.len(),
            "range {:?} out of len {}",
            range,
            self.len()
        );

        if range.is_empty() {
            let builder = ColumnBuilder::with_capacity(&self.data_type(), 0);
            return builder.build();
        }

        match self {
            Column::Null { .. } => Column::Null {
                len: range.end - range.start,
            },
            Column::EmptyArray { .. } => Column::EmptyArray {
                len: range.end - range.start,
            },
            Column::EmptyMap { .. } => Column::EmptyMap {
                len: range.end - range.start,
            },
            Column::Number(col) => Column::Number(col.slice(range)),
            Column::Decimal(col) => Column::Decimal(col.slice(range)),
            Column::Boolean(col) => {
                Column::Boolean(col.clone().sliced(range.start, range.end - range.start))
            }
            Column::Binary(col) => Column::Binary(col.slice(range)),
            Column::String(col) => Column::String(col.slice(range)),
            Column::Timestamp(col) => {
                Column::Timestamp(col.clone().sliced(range.start, range.end - range.start))
            }
            Column::Date(col) => {
                Column::Date(col.clone().sliced(range.start, range.end - range.start))
            }
            Column::Array(col) => Column::Array(Box::new(col.slice(range))),
            Column::Map(col) => Column::Map(Box::new(col.slice(range))),
            Column::Bitmap(col) => Column::Bitmap(col.slice(range)),
            Column::Nullable(col) => Column::Nullable(Box::new(col.slice(range))),
            Column::Tuple(fields) => Column::Tuple(
                fields
                    .iter()
                    .map(|field| field.slice(range.clone()))
                    .collect(),
            ),
            Column::Variant(col) => Column::Variant(col.slice(range)),
            Column::Geometry(col) => Column::Geometry(col.slice(range)),
            Column::Geography(col) => Column::Geography(col.slice(range)),
        }
    }

    pub fn iter(&self) -> ColumnIterator {
        ColumnIterator {
            column: self,
            index: 0,
            len: self.len(),
        }
    }

    pub fn domain(&self) -> Domain {
        if !matches!(self, Column::Array(_) | Column::Map(_)) {
            assert!(self.len() > 0);
        }
        match self {
            Column::Null { .. } => Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            }),
            Column::EmptyArray { .. } => Domain::Array(None),
            Column::EmptyMap { .. } => Domain::Map(None),
            Column::Number(col) => Domain::Number(col.domain()),
            Column::Decimal(col) => Domain::Decimal(col.domain()),
            Column::Boolean(col) => Domain::Boolean(BooleanDomain {
                has_false: col.unset_bits() > 0,
                has_true: col.len() - col.unset_bits() > 0,
            }),
            Column::String(col) => {
                let (min, max) = StringType::iter_column(col).minmax().into_option().unwrap();
                Domain::String(StringDomain {
                    min: min.to_string(),
                    max: Some(max.to_string()),
                })
            }
            Column::Timestamp(col) => {
                let (min, max) = col.iter().minmax().into_option().unwrap();
                Domain::Timestamp(SimpleDomain {
                    min: *min,
                    max: *max,
                })
            }
            Column::Date(col) => {
                let (min, max) = col.iter().minmax().into_option().unwrap();
                Domain::Date(SimpleDomain {
                    min: *min,
                    max: *max,
                })
            }
            Column::Array(col) => {
                if col.len() == 0 || col.values.len() == 0 {
                    Domain::Array(None)
                } else {
                    let inner_domain = col.values.domain();
                    Domain::Array(Some(Box::new(inner_domain)))
                }
            }
            Column::Map(col) => {
                if col.len() == 0 || col.values.len() == 0 {
                    Domain::Map(None)
                } else {
                    let inner_domain = col.values.domain();
                    Domain::Map(Some(Box::new(inner_domain)))
                }
            }
            Column::Nullable(col) => {
                let inner_domain = col.column.domain();
                Domain::Nullable(NullableDomain {
                    has_null: col.validity.unset_bits() > 0,
                    value: Some(Box::new(inner_domain)),
                })
            }
            Column::Tuple(fields) => {
                let domains = fields.iter().map(|col| col.domain()).collect::<Vec<_>>();
                Domain::Tuple(domains)
            }
            Column::Binary(_)
            | Column::Bitmap(_)
            | Column::Variant(_)
            | Column::Geometry(_)
            | Column::Geography(_) => Domain::Undefined,
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            Column::Null { .. } => DataType::Null,
            Column::EmptyArray { .. } => DataType::EmptyArray,
            Column::EmptyMap { .. } => DataType::EmptyMap,
            Column::Number(c) => with_number_type!(|NUM_TYPE| match c {
                NumberColumn::NUM_TYPE(_) => DataType::Number(NumberDataType::NUM_TYPE),
            }),
            Column::Decimal(c) => with_decimal_type!(|DECIMAL_TYPE| match c {
                DecimalColumn::DECIMAL_TYPE(_, size) =>
                    DataType::Decimal(DecimalDataType::DECIMAL_TYPE(*size)),
            }),
            Column::Boolean(_) => DataType::Boolean,
            Column::Binary(_) => DataType::Binary,
            Column::String(_) => DataType::String,
            Column::Timestamp(_) => DataType::Timestamp,
            Column::Date(_) => DataType::Date,
            Column::Array(array) => {
                let inner = array.values.data_type();
                DataType::Array(Box::new(inner))
            }
            Column::Map(col) => {
                let inner = col.values.data_type();
                DataType::Map(Box::new(inner))
            }
            Column::Bitmap(_) => DataType::Bitmap,
            Column::Nullable(inner) => {
                let inner = inner.column.data_type();
                inner.wrap_nullable()
            }
            Column::Tuple(fields) => {
                let inner = fields.iter().map(|col| col.data_type()).collect::<Vec<_>>();
                DataType::Tuple(inner)
            }
            Column::Variant(_) => DataType::Variant,
            Column::Geometry(_) => DataType::Geometry,
            Column::Geography(_) => DataType::Geography,
        }
    }

    pub fn check_valid(&self) -> Result<()> {
        match self {
            Column::Binary(x) => x.check_valid(),
            Column::String(x) => x.check_valid(),
            Column::Variant(x) => x.check_valid(),
            Column::Geometry(x) => x.check_valid(),
            Column::Bitmap(x) => x.check_valid(),
            Column::Map(x) => {
                for y in x.iter() {
                    y.check_valid()?;
                }
                Ok(())
            }
            Column::Array(x) => x.check_valid(),
            Column::Nullable(x) => {
                if x.column.len() != x.validity.len() {
                    return Err(ErrorCode::Internal(
                        "column and validity length mismatch".to_string(),
                    ));
                }
                x.column.check_valid()
            }
            Column::Tuple(x) => {
                for y in x.iter() {
                    y.check_valid()?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    pub fn random(ty: &DataType, len: usize, seed: Option<u64>) -> Self {
        use rand::distributions::Alphanumeric;
        use rand::rngs::SmallRng;
        use rand::Rng;
        use rand::SeedableRng;
        let mut rng = match seed {
            None => SmallRng::from_entropy(),
            Some(seed) => SmallRng::seed_from_u64(seed),
        };
        match ty {
            DataType::Null => Column::Null { len },
            DataType::EmptyArray => Column::EmptyArray { len },
            DataType::EmptyMap => Column::EmptyMap { len },
            DataType::Boolean => {
                BooleanType::from_data((0..len).map(|_| rng.gen_bool(0.5)).collect_vec())
            }
            DataType::Binary => BinaryType::from_data(
                (0..len)
                    .map(|_| {
                        let rng = match seed {
                            None => SmallRng::from_entropy(),
                            Some(seed) => SmallRng::seed_from_u64(seed),
                        };
                        rng.sample_iter(&Alphanumeric)
                            // randomly generate 5 characters.
                            .take(5)
                            .map(u8::from)
                            .collect::<Vec<_>>()
                    })
                    .collect_vec(),
            ),
            DataType::String => StringType::from_data(
                (0..len)
                    .map(|_| {
                        let rng = match seed {
                            None => SmallRng::from_entropy(),
                            Some(seed) => SmallRng::seed_from_u64(seed),
                        };
                        rng.sample_iter(&Alphanumeric)
                            // randomly generate 5 characters.
                            .take(5)
                            .map(char::from)
                            .collect::<String>()
                    })
                    .collect_vec(),
            ),
            DataType::Number(num_ty) => {
                with_number_mapped_type!(|NUM_TYPE| match num_ty {
                    NumberDataType::NUM_TYPE => {
                        NumberType::<NUM_TYPE>::from_data(
                            (0..len).map(|_| rng.gen::<NUM_TYPE>()).collect_vec(),
                        )
                    }
                })
            }
            DataType::Decimal(t) => match t {
                DecimalDataType::Decimal128(size) => {
                    let values = (0..len)
                        .map(|_| i128::from(rng.gen::<i16>()))
                        .collect::<Vec<i128>>();
                    Column::Decimal(DecimalColumn::Decimal128(values.into(), *size))
                }
                DecimalDataType::Decimal256(size) => {
                    let values = (0..len)
                        .map(|_| i256::from(rng.gen::<i16>()))
                        .collect::<Vec<i256>>();
                    Column::Decimal(DecimalColumn::Decimal256(values.into(), *size))
                }
            },
            DataType::Timestamp => TimestampType::from_data(
                (0..len)
                    .map(|_| rng.gen_range(TIMESTAMP_MIN..=TIMESTAMP_MAX))
                    .collect::<Vec<i64>>(),
            ),
            DataType::Date => DateType::from_data(
                (0..len)
                    .map(|_| rng.gen_range(DATE_MIN..=DATE_MAX))
                    .collect::<Vec<i32>>(),
            ),
            DataType::Nullable(ty) => Column::Nullable(Box::new(NullableColumn {
                column: Column::random(ty, len, seed),
                validity: Bitmap::from((0..len).map(|_| rng.gen_bool(0.5)).collect::<Vec<bool>>()),
            })),
            DataType::Array(inner_ty) => {
                let mut inner_len = 0;
                let mut offsets: Vec<u64> = Vec::with_capacity(len + 1);
                offsets.push(0);
                for _ in 0..len {
                    inner_len += rng.gen_range(0..=3);
                    offsets.push(inner_len);
                }
                Column::Array(Box::new(ArrayColumn {
                    values: Column::random(inner_ty, inner_len as usize, seed),
                    offsets: offsets.into(),
                }))
            }
            DataType::Map(inner_ty) => {
                let mut inner_len = 0;
                let mut offsets: Vec<u64> = Vec::with_capacity(len + 1);
                offsets.push(0);
                for _ in 0..len {
                    inner_len += rng.gen_range(0..=3);
                    offsets.push(inner_len);
                }
                Column::Map(Box::new(ArrayColumn {
                    values: Column::random(inner_ty, inner_len as usize, seed),
                    offsets: offsets.into(),
                }))
            }
            DataType::Bitmap => BitmapType::from_data(
                (0..len)
                    .map(|_| {
                        let data: [u64; 4] = rng.gen();
                        let rb = RoaringTreemap::from_iter(data.iter());
                        let mut buf = vec![];
                        rb.serialize_into(&mut buf)
                            .expect("failed serialize roaring treemap");
                        buf
                    })
                    .collect_vec(),
            ),
            DataType::Tuple(fields) => {
                let fields = fields
                    .iter()
                    .map(|ty| Column::random(ty, len, seed))
                    .collect::<Vec<_>>();
                Column::Tuple(fields)
            }
            DataType::Variant => {
                let mut data = Vec::with_capacity(len);
                for _ in 0..len {
                    let val = jsonb::rand_value();
                    data.push(val.to_vec());
                }
                VariantType::from_data(data)
            }
            DataType::Geometry => {
                let mut data = Vec::with_capacity(len);
                (0..len).for_each(|_| {
                    let x = rng.gen::<f64>();
                    let y = rng.gen::<f64>();
                    let val = Point::new(x, y);
                    data.push(
                        Geometry::from(val)
                            .to_ewkb(CoordDimensions::xy(), None)
                            .unwrap(),
                    );
                });
                GeometryType::from_data(data)
            }
            DataType::Geography => {
                let mut builder = GeographyType::create_builder(len, &[]);
                for _ in 0..len {
                    let item = GeographyType::point(
                        rng.gen_range(-180.0..=180.0),
                        rng.gen_range(-90.0..=90.0),
                    );
                    builder.push(item.as_ref());
                }
                Column::Geography(builder.build())
            }
            DataType::Generic(_) => unreachable!(),
        }
    }

    pub fn remove_nullable(&self) -> Self {
        match self {
            Column::Nullable(inner) => inner.column.clone(),
            _ => self.clone(),
        }
    }

    pub fn wrap_nullable(self, validity: Option<Bitmap>) -> Self {
        match self {
            c @ Column::Null { .. } => c,
            Column::Nullable(null_column) => {
                let validity = match validity {
                    Some(v) => &v & (&null_column.validity),
                    None => null_column.validity.clone(),
                };
                Column::Nullable(Box::new(NullableColumn {
                    column: null_column.column.clone(),
                    validity,
                }))
            }
            _ => {
                let validity = validity.unwrap_or_else(|| Bitmap::new_constant(true, self.len()));
                Column::Nullable(Box::new(NullableColumn {
                    column: self.clone(),
                    validity,
                }))
            }
        }
    }

    pub fn memory_size(&self) -> usize {
        match self {
            Column::Null { .. } => std::mem::size_of::<usize>(),
            Column::EmptyArray { .. } => std::mem::size_of::<usize>(),
            Column::EmptyMap { .. } => std::mem::size_of::<usize>(),
            Column::Number(NumberColumn::UInt8(col)) => col.len(),
            Column::Number(NumberColumn::UInt16(col)) => col.len() * 2,
            Column::Number(NumberColumn::UInt32(col)) => col.len() * 4,
            Column::Number(NumberColumn::UInt64(col)) => col.len() * 8,
            Column::Number(NumberColumn::Float32(col)) => col.len() * 4,
            Column::Number(NumberColumn::Float64(col)) => col.len() * 8,
            Column::Number(NumberColumn::Int8(col)) => col.len(),
            Column::Number(NumberColumn::Int16(col)) => col.len() * 2,
            Column::Number(NumberColumn::Int32(col)) => col.len() * 4,
            Column::Number(NumberColumn::Int64(col)) => col.len() * 8,
            Column::Decimal(DecimalColumn::Decimal128(col, _)) => col.len() * 16,
            Column::Decimal(DecimalColumn::Decimal256(col, _)) => col.len() * 32,
            Column::Boolean(c) => c.as_slice().0.len(),
            Column::Binary(col) => col.memory_size(),
            Column::String(col) => col.memory_size(),
            Column::Timestamp(col) => col.len() * 8,
            Column::Date(col) => col.len() * 4,
            Column::Array(col) => col.values.memory_size() + col.offsets.len() * 8,
            Column::Map(col) => col.values.memory_size() + col.offsets.len() * 8,
            Column::Bitmap(col) => col.memory_size(),
            Column::Nullable(c) => c.column.memory_size() + c.validity.as_slice().0.len(),
            Column::Tuple(fields) => fields.iter().map(|f| f.memory_size()).sum(),
            Column::Variant(col) => col.memory_size(),
            Column::Geometry(col) => col.memory_size(),
            Column::Geography(col) => GeographyType::column_memory_size(col),
        }
    }

    pub fn serialize_size(&self) -> usize {
        match self {
            Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => 0,
            Column::Number(NumberColumn::UInt8(col)) => col.len(),
            Column::Number(NumberColumn::UInt16(col)) => col.len() * 2,
            Column::Number(NumberColumn::UInt32(col)) => col.len() * 4,
            Column::Number(NumberColumn::UInt64(col)) => col.len() * 8,
            Column::Number(NumberColumn::Float32(col)) => col.len() * 4,
            Column::Number(NumberColumn::Float64(col)) => col.len() * 8,
            Column::Number(NumberColumn::Int8(col)) => col.len(),
            Column::Number(NumberColumn::Int16(col)) => col.len() * 2,
            Column::Number(NumberColumn::Int32(col)) | Column::Date(col) => col.len() * 4,
            Column::Number(NumberColumn::Int64(col)) | Column::Timestamp(col) => col.len() * 8,
            Column::Decimal(DecimalColumn::Decimal128(col, _)) => col.len() * 16,
            Column::Decimal(DecimalColumn::Decimal256(col, _)) => col.len() * 32,
            Column::Geography(col) => GeographyType::column_memory_size(col),
            Column::Boolean(c) => c.len(),
            Column::Binary(col)
            | Column::Bitmap(col)
            | Column::Variant(col)
            | Column::Geometry(col) => col.memory_size(),
            Column::String(col) => col.memory_size(),
            Column::Array(col) | Column::Map(col) => col.values.serialize_size() + col.len() * 8,
            Column::Nullable(c) => c.column.serialize_size() + c.len(),
            Column::Tuple(fields) => fields.iter().map(|f| f.serialize_size()).sum(),
        }
    }

    /// Returns (is_all_null, Option bitmap)
    pub fn validity(&self) -> (bool, Option<&Bitmap>) {
        match self {
            Column::Null { .. } => (true, None),
            Column::Nullable(c) => {
                if c.validity.unset_bits() == c.validity.len() {
                    (true, Some(&c.validity))
                } else {
                    (false, Some(&c.validity))
                }
            }
            _ => (false, None),
        }
    }
}

/// Serialize a column to a base64 string.
/// Because we may use serde::json/bincode to serialize the column, so we wrap it into string
impl Serialize for Column {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        let bytes = serialize_column(self);
        let base64_str = general_purpose::STANDARD.encode(bytes);
        serializer.serialize_str(&base64_str)
    }
}

impl<'de> Deserialize<'de> for Column {
    fn deserialize<D>(deserializer: D) -> Result<Column, D::Error>
    where D: Deserializer<'de> {
        struct ColumnVisitor;

        impl<'de> Visitor<'de> for ColumnVisitor {
            type Value = Column;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("an arrow chunk with exactly one column")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where E: serde::de::Error {
                let bytes = general_purpose::STANDARD.decode(v).unwrap();
                let column = deserialize_column(&bytes)
                    .expect("expecting an arrow chunk with exactly one column");
                Ok(column)
            }
        }

        deserializer.deserialize_string(ColumnVisitor)
    }
}

impl BorshSerialize for Column {
    fn serialize<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        let bytes = serialize_column(self);
        BorshSerialize::serialize(&bytes, writer)
    }
}

impl BorshDeserialize for Column {
    fn deserialize_reader<R: Read>(reader: &mut R) -> std::io::Result<Self> {
        let bytes: Vec<u8> = borsh::BorshDeserialize::deserialize_reader(reader)?;
        let column =
            deserialize_column(&bytes).expect("expecting an arrow chunk with exactly one column");
        Ok(column)
    }
}

impl Eq for Column {}

impl ColumnBuilder {
    pub fn from_column(col: Column) -> Self {
        match col {
            Column::Null { len } => ColumnBuilder::Null { len },
            Column::EmptyArray { len } => ColumnBuilder::EmptyArray { len },
            Column::EmptyMap { len } => ColumnBuilder::EmptyMap { len },
            Column::Number(col) => ColumnBuilder::Number(NumberColumnBuilder::from_column(col)),
            Column::Decimal(col) => ColumnBuilder::Decimal(DecimalColumnBuilder::from_column(col)),
            Column::Boolean(col) => ColumnBuilder::Boolean(bitmap_into_mut(col)),
            Column::Binary(col) => ColumnBuilder::Binary(BinaryColumnBuilder::from_column(col)),
            Column::String(col) => ColumnBuilder::String(StringColumnBuilder::from_column(col)),
            Column::Timestamp(col) => ColumnBuilder::Timestamp(buffer_into_mut(col)),
            Column::Date(col) => ColumnBuilder::Date(buffer_into_mut(col)),
            Column::Array(box col) => {
                ColumnBuilder::Array(Box::new(ArrayColumnBuilder::from_column(col)))
            }
            Column::Map(box col) => {
                ColumnBuilder::Map(Box::new(ArrayColumnBuilder::from_column(col)))
            }
            Column::Bitmap(col) => ColumnBuilder::Bitmap(BinaryColumnBuilder::from_column(col)),
            Column::Nullable(box col) => {
                ColumnBuilder::Nullable(Box::new(NullableColumnBuilder::from_column(col)))
            }
            Column::Tuple(fields) => ColumnBuilder::Tuple(
                fields
                    .iter()
                    .map(|col| ColumnBuilder::from_column(col.clone()))
                    .collect(),
            ),
            Column::Variant(col) => ColumnBuilder::Variant(BinaryColumnBuilder::from_column(col)),
            Column::Geometry(col) => ColumnBuilder::Geometry(BinaryColumnBuilder::from_column(col)),
            Column::Geography(col) => {
                ColumnBuilder::Geography(GeographyType::column_to_builder(col))
            }
        }
    }

    pub fn repeat(scalar: &ScalarRef, n: usize, data_type: &DataType) -> ColumnBuilder {
        if !scalar.is_null() {
            if let DataType::Nullable(ty) = data_type {
                let mut builder = ColumnBuilder::with_capacity(ty, 1);
                builder.push_repeat(scalar, n);
                return ColumnBuilder::Nullable(Box::new(NullableColumnBuilder {
                    builder,
                    validity: Bitmap::new_constant(true, n).make_mut(),
                }));
            }
        }

        match scalar {
            ScalarRef::Null => match data_type {
                DataType::Null => ColumnBuilder::Null { len: n },
                DataType::Nullable(inner) => {
                    ColumnBuilder::Nullable(Box::new(NullableColumnBuilder {
                        builder: Self::repeat_default(inner, n),
                        validity: MutableBitmap::from_len_zeroed(n),
                    }))
                }
                _ => unreachable!(),
            },
            ScalarRef::EmptyArray => ColumnBuilder::EmptyArray { len: n },
            ScalarRef::EmptyMap => ColumnBuilder::EmptyMap { len: n },
            ScalarRef::Number(num) => ColumnBuilder::Number(NumberColumnBuilder::repeat(*num, n)),
            ScalarRef::Decimal(dec) => {
                ColumnBuilder::Decimal(DecimalColumnBuilder::repeat(*dec, n))
            }
            ScalarRef::Boolean(b) => ColumnBuilder::Boolean(Bitmap::new_constant(*b, n).make_mut()),
            ScalarRef::Binary(s) => ColumnBuilder::Binary(BinaryColumnBuilder::repeat(s, n)),
            ScalarRef::String(s) => ColumnBuilder::String(StringColumnBuilder::repeat(s, n)),
            ScalarRef::Timestamp(d) => ColumnBuilder::Timestamp(vec![*d; n]),
            ScalarRef::Date(d) => ColumnBuilder::Date(vec![*d; n]),
            ScalarRef::Array(col) => {
                ColumnBuilder::Array(Box::new(ArrayColumnBuilder::repeat(col, n)))
            }
            ScalarRef::Map(col) => ColumnBuilder::Map(Box::new(ArrayColumnBuilder::repeat(col, n))),
            ScalarRef::Bitmap(b) => ColumnBuilder::Bitmap(BinaryColumnBuilder::repeat(b, n)),
            ScalarRef::Tuple(fields) => {
                let fields_ty = match data_type {
                    DataType::Tuple(fields_ty) => fields_ty,
                    _ => unreachable!(),
                };
                ColumnBuilder::Tuple(
                    fields
                        .iter()
                        .zip(fields_ty)
                        .map(|(field, ty)| ColumnBuilder::repeat(field, n, ty))
                        .collect(),
                )
            }
            ScalarRef::Variant(s) => ColumnBuilder::Variant(BinaryColumnBuilder::repeat(s, n)),
            ScalarRef::Geometry(s) => ColumnBuilder::Geometry(BinaryColumnBuilder::repeat(s, n)),
            ScalarRef::Geography(s) => {
                ColumnBuilder::Geography(GeographyColumnBuilder::repeat(s, n))
            }
        }
    }

    pub fn len(&self) -> usize {
        match self {
            ColumnBuilder::Null { len } => *len,
            ColumnBuilder::EmptyArray { len } => *len,
            ColumnBuilder::EmptyMap { len } => *len,
            ColumnBuilder::Number(col) => col.len(),
            ColumnBuilder::Decimal(col) => col.len(),
            ColumnBuilder::Boolean(builder) => builder.len(),
            ColumnBuilder::Binary(builder) => builder.len(),
            ColumnBuilder::String(builder) => builder.len(),
            ColumnBuilder::Timestamp(builder) => builder.len(),
            ColumnBuilder::Date(builder) => builder.len(),
            ColumnBuilder::Array(builder) => builder.len(),
            ColumnBuilder::Map(builder) => builder.len(),
            ColumnBuilder::Bitmap(builder) => builder.len(),
            ColumnBuilder::Nullable(builder) => builder.len(),
            ColumnBuilder::Tuple(fields) => fields[0].len(),
            ColumnBuilder::Variant(builder) => builder.len(),
            ColumnBuilder::Geometry(builder) => builder.len(),
            ColumnBuilder::Geography(builder) => builder.len(),
        }
    }

    pub fn memory_size(&self) -> usize {
        match self {
            ColumnBuilder::Null { .. } => std::mem::size_of::<usize>(),
            ColumnBuilder::EmptyArray { .. } => std::mem::size_of::<usize>(),
            ColumnBuilder::EmptyMap { .. } => std::mem::size_of::<usize>(),
            ColumnBuilder::Number(NumberColumnBuilder::UInt8(builder)) => builder.len(),
            ColumnBuilder::Number(NumberColumnBuilder::UInt16(builder)) => builder.len() * 2,
            ColumnBuilder::Number(NumberColumnBuilder::UInt32(builder)) => builder.len() * 4,
            ColumnBuilder::Number(NumberColumnBuilder::UInt64(builder)) => builder.len() * 8,
            ColumnBuilder::Number(NumberColumnBuilder::Float32(builder)) => builder.len() * 4,
            ColumnBuilder::Number(NumberColumnBuilder::Float64(builder)) => builder.len() * 8,
            ColumnBuilder::Number(NumberColumnBuilder::Int8(builder)) => builder.len(),
            ColumnBuilder::Number(NumberColumnBuilder::Int16(builder)) => builder.len() * 2,
            ColumnBuilder::Number(NumberColumnBuilder::Int32(builder)) => builder.len() * 4,
            ColumnBuilder::Number(NumberColumnBuilder::Int64(builder)) => builder.len() * 8,
            ColumnBuilder::Decimal(DecimalColumnBuilder::Decimal128(builder, _)) => {
                builder.len() * 16
            }
            ColumnBuilder::Decimal(DecimalColumnBuilder::Decimal256(builder, _)) => {
                builder.len() * 32
            }
            ColumnBuilder::Boolean(c) => c.as_slice().len(),
            ColumnBuilder::Binary(col) => col.data.len() + col.offsets.len() * 8,
            ColumnBuilder::String(col) => col.data.len() + col.offsets.len() * 8,
            ColumnBuilder::Timestamp(col) => col.len() * 8,
            ColumnBuilder::Date(col) => col.len() * 4,
            ColumnBuilder::Array(col) => col.builder.memory_size() + col.offsets.len() * 8,
            ColumnBuilder::Map(col) => col.builder.memory_size() + col.offsets.len() * 8,
            ColumnBuilder::Bitmap(col) => col.data.len() + col.offsets.len() * 8,
            ColumnBuilder::Nullable(c) => c.builder.memory_size() + c.validity.as_slice().len(),
            ColumnBuilder::Tuple(fields) => fields.iter().map(|f| f.memory_size()).sum(),
            ColumnBuilder::Variant(col) => col.data.len() + col.offsets.len() * 8,
            ColumnBuilder::Geometry(col) => col.data.len() + col.offsets.len() * 8,
            ColumnBuilder::Geography(builder) => builder.memory_size(),
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            ColumnBuilder::Null { .. } => DataType::Null,
            ColumnBuilder::EmptyArray { .. } => DataType::EmptyArray,
            ColumnBuilder::EmptyMap { .. } => DataType::EmptyMap,
            ColumnBuilder::Number(col) => with_number_type!(|NUM_TYPE| match col {
                NumberColumnBuilder::NUM_TYPE(_) => DataType::Number(NumberDataType::NUM_TYPE),
            }),
            ColumnBuilder::Decimal(col) => with_decimal_type!(|DECIMAL_TYPE| match col {
                DecimalColumnBuilder::DECIMAL_TYPE(_, size) =>
                    DataType::Decimal(DecimalDataType::DECIMAL_TYPE(*size)),
            }),
            ColumnBuilder::Boolean(_) => DataType::Boolean,
            ColumnBuilder::Binary(_) => DataType::Binary,
            ColumnBuilder::String(_) => DataType::String,
            ColumnBuilder::Timestamp(_) => DataType::Timestamp,
            ColumnBuilder::Date(_) => DataType::Date,
            ColumnBuilder::Array(col) => {
                let inner = col.builder.data_type();
                DataType::Array(Box::new(inner))
            }
            ColumnBuilder::Map(col) => {
                let inner = col.builder.data_type();
                DataType::Map(Box::new(inner))
            }
            ColumnBuilder::Bitmap(_) => DataType::Bitmap,
            ColumnBuilder::Nullable(col) => DataType::Nullable(Box::new(col.builder.data_type())),
            ColumnBuilder::Tuple(fields) => {
                DataType::Tuple(fields.iter().map(|f| f.data_type()).collect::<Vec<_>>())
            }
            ColumnBuilder::Variant(_) => DataType::Variant,
            ColumnBuilder::Geometry(_) => DataType::Geometry,
            ColumnBuilder::Geography(_) => DataType::Geography,
        }
    }

    pub fn with_capacity(ty: &DataType, capacity: usize) -> ColumnBuilder {
        ColumnBuilder::with_capacity_hint(ty, capacity, true)
    }

    /// Create a new column builder with capacity and enable_datasize_hint
    /// enable_datasize_hint is used in StringColumnBuilder to decide whether to pre-allocate values
    pub fn with_capacity_hint(
        ty: &DataType,
        capacity: usize,
        enable_datasize_hint: bool,
    ) -> ColumnBuilder {
        match ty {
            DataType::Null => ColumnBuilder::Null { len: 0 },
            DataType::EmptyArray => ColumnBuilder::EmptyArray { len: 0 },
            DataType::EmptyMap => ColumnBuilder::EmptyMap { len: 0 },
            DataType::Number(num_ty) => {
                ColumnBuilder::Number(NumberColumnBuilder::with_capacity(num_ty, capacity))
            }
            DataType::Decimal(decimal_ty) => {
                ColumnBuilder::Decimal(DecimalColumnBuilder::with_capacity(decimal_ty, capacity))
            }
            DataType::Boolean => ColumnBuilder::Boolean(MutableBitmap::with_capacity(capacity)),
            DataType::Binary => {
                let data_capacity = if enable_datasize_hint { 0 } else { capacity };
                ColumnBuilder::Binary(BinaryColumnBuilder::with_capacity(capacity, data_capacity))
            }
            DataType::String => {
                let data_capacity = if enable_datasize_hint { 0 } else { capacity };
                ColumnBuilder::String(StringColumnBuilder::with_capacity(capacity, data_capacity))
            }
            DataType::Timestamp => ColumnBuilder::Timestamp(Vec::with_capacity(capacity)),
            DataType::Date => ColumnBuilder::Date(Vec::with_capacity(capacity)),
            DataType::Nullable(ty) => ColumnBuilder::Nullable(Box::new(NullableColumnBuilder {
                builder: Self::with_capacity_hint(ty, capacity, enable_datasize_hint),
                validity: MutableBitmap::with_capacity(capacity),
            })),
            DataType::Array(ty) => {
                let mut offsets = Vec::with_capacity(capacity + 1);
                offsets.push(0);
                ColumnBuilder::Array(Box::new(ArrayColumnBuilder {
                    builder: Self::with_capacity_hint(ty, 0, enable_datasize_hint),
                    offsets,
                }))
            }
            DataType::Map(ty) => {
                let mut offsets = Vec::with_capacity(capacity + 1);
                offsets.push(0);
                ColumnBuilder::Map(Box::new(ArrayColumnBuilder {
                    builder: Self::with_capacity_hint(ty, 0, enable_datasize_hint),
                    offsets,
                }))
            }
            DataType::Tuple(fields) => {
                assert!(!fields.is_empty());
                ColumnBuilder::Tuple(
                    fields
                        .iter()
                        .map(|field| {
                            Self::with_capacity_hint(field, capacity, enable_datasize_hint)
                        })
                        .collect(),
                )
            }
            DataType::Bitmap => {
                let data_capacity = if enable_datasize_hint { 0 } else { capacity };
                ColumnBuilder::Bitmap(BinaryColumnBuilder::with_capacity(capacity, data_capacity))
            }
            DataType::Variant => {
                let data_capacity = if enable_datasize_hint { 0 } else { capacity };
                ColumnBuilder::Variant(BinaryColumnBuilder::with_capacity(capacity, data_capacity))
            }
            DataType::Geometry => {
                let data_capacity = if enable_datasize_hint { 0 } else { capacity };
                ColumnBuilder::Geometry(BinaryColumnBuilder::with_capacity(capacity, data_capacity))
            }
            DataType::Geography => {
                let data_capacity = if enable_datasize_hint { 0 } else { capacity };
                ColumnBuilder::Geography(GeographyColumnBuilder::with_capacity(
                    capacity,
                    data_capacity,
                ))
            }
            DataType::Generic(_) => {
                unreachable!("unable to initialize column builder for generic type")
            }
        }
    }

    pub fn repeat_default(ty: &DataType, len: usize) -> ColumnBuilder {
        match ty {
            DataType::Null => ColumnBuilder::Null { len },
            DataType::EmptyArray => ColumnBuilder::EmptyArray { len },
            DataType::EmptyMap => ColumnBuilder::EmptyMap { len },

            // bitmap
            DataType::Nullable(inner) => ColumnBuilder::Nullable(Box::new(NullableColumnBuilder {
                builder: Self::repeat_default(inner, len),
                validity: MutableBitmap::from_len_zeroed(len),
            })),
            DataType::Boolean => ColumnBuilder::Boolean(MutableBitmap::from_len_zeroed(len)),

            // number based
            DataType::Number(num_ty) => {
                ColumnBuilder::Number(NumberColumnBuilder::repeat_default(num_ty, len))
            }
            DataType::Decimal(decimal_ty) => {
                ColumnBuilder::Decimal(DecimalColumnBuilder::repeat_default(decimal_ty, len))
            }
            DataType::Timestamp => ColumnBuilder::Timestamp(vec![0; len]),
            DataType::Date => ColumnBuilder::Date(vec![0; len]),
            DataType::Geography => {
                ColumnBuilder::Geography(GeographyColumnBuilder::repeat_default(len))
            }

            // binary based
            DataType::Binary => ColumnBuilder::Binary(BinaryColumnBuilder::repeat_default(len)),
            DataType::Bitmap => ColumnBuilder::Bitmap(BinaryColumnBuilder::repeat_default(len)),
            DataType::String => ColumnBuilder::String(StringColumnBuilder::repeat_default(len)),
            DataType::Variant => ColumnBuilder::Variant(BinaryColumnBuilder::repeat_default(len)),
            DataType::Geometry => ColumnBuilder::Geometry(BinaryColumnBuilder::repeat_default(len)),

            DataType::Array(ty) => ColumnBuilder::Array(Box::new(ArrayColumnBuilder {
                builder: Self::with_capacity(ty, 0),
                offsets: vec![0; len + 1],
            })),
            DataType::Map(ty) => ColumnBuilder::Map(Box::new(ArrayColumnBuilder {
                builder: Self::with_capacity(ty, 0),
                offsets: vec![0; len + 1],
            })),
            DataType::Tuple(fields) => {
                assert!(!fields.is_empty());
                ColumnBuilder::Tuple(
                    fields
                        .iter()
                        .map(|field| Self::repeat_default(field, len))
                        .collect(),
                )
            }

            DataType::Generic(_) => {
                unreachable!("unable to initialize column builder for generic type")
            }
        }
    }

    pub fn push(&mut self, item: ScalarRef) {
        match (self, item) {
            (ColumnBuilder::Null { len }, ScalarRef::Null) => *len += 1,
            (ColumnBuilder::EmptyArray { len }, ScalarRef::EmptyArray) => *len += 1,
            (ColumnBuilder::EmptyMap { len }, ScalarRef::EmptyMap) => *len += 1,
            (ColumnBuilder::Number(builder), ScalarRef::Number(value)) => builder.push(value),
            (ColumnBuilder::Decimal(builder), ScalarRef::Decimal(value)) => builder.push(value),
            (ColumnBuilder::Boolean(builder), ScalarRef::Boolean(value)) => {
                BooleanType::push_item(builder, value);
            }
            (ColumnBuilder::Binary(builder), ScalarRef::Binary(value)) => {
                BinaryType::push_item(builder, value);
            }
            (ColumnBuilder::String(builder), ScalarRef::String(value)) => {
                StringType::push_item(builder, value)
            }
            (ColumnBuilder::Timestamp(builder), ScalarRef::Timestamp(value)) => {
                TimestampType::push_item(builder, value)
            }
            (ColumnBuilder::Date(builder), ScalarRef::Date(value)) => {
                DateType::push_item(builder, value)
            }
            (ColumnBuilder::Array(builder), ScalarRef::Array(value)) => {
                ArrayType::push_item(builder, value);
            }
            (ColumnBuilder::Map(builder), ScalarRef::Map(value)) => {
                ArrayType::push_item(builder, value);
            }
            (ColumnBuilder::Bitmap(builder), ScalarRef::Bitmap(value)) => {
                BitmapType::push_item(builder, value);
            }
            (ColumnBuilder::Nullable(builder), ScalarRef::Null) => {
                builder.push_null();
            }
            (ColumnBuilder::Nullable(builder), scalar) => {
                builder.push(scalar);
            }
            (ColumnBuilder::Tuple(fields), ScalarRef::Tuple(value)) => {
                assert_eq!(fields.len(), value.len());
                for (field, scalar) in fields.iter_mut().zip(value.into_iter()) {
                    field.push(scalar);
                }
            }
            (ColumnBuilder::Variant(builder), ScalarRef::Variant(value)) => {
                VariantType::push_item(builder, value);
            }
            (ColumnBuilder::Geometry(builder), ScalarRef::Geometry(value)) => {
                GeometryType::push_item(builder, value);
            }
            (ColumnBuilder::Geography(builder), ScalarRef::Geography(value)) => {
                GeographyType::push_item(builder, value);
            }
            (builder, scalar) => unreachable!("unable to push {scalar:?} to {builder:?}"),
        }
    }

    pub fn push_repeat(&mut self, item: &ScalarRef, n: usize) {
        match (self, item) {
            (ColumnBuilder::Null { len }, ScalarRef::Null)
            | (ColumnBuilder::EmptyArray { len }, ScalarRef::EmptyArray)
            | (ColumnBuilder::EmptyMap { len }, ScalarRef::EmptyMap) => *len += n,

            (ColumnBuilder::Number(builder), ScalarRef::Number(value)) => {
                builder.push_repeat(*value, n)
            }
            (ColumnBuilder::Decimal(builder), ScalarRef::Decimal(value)) => {
                builder.push_repeat(*value, n)
            }
            (ColumnBuilder::Boolean(builder), ScalarRef::Boolean(value)) => {
                BooleanType::push_item_repeat(builder, *value, n);
            }
            (ColumnBuilder::Timestamp(builder), ScalarRef::Timestamp(value)) => {
                TimestampType::push_item_repeat(builder, *value, n);
            }
            (ColumnBuilder::Date(builder), ScalarRef::Date(value)) => {
                DateType::push_item_repeat(builder, *value, n);
            }
            (ColumnBuilder::Binary(builder), ScalarRef::Binary(value)) => {
                BinaryType::push_item_repeat(builder, *value, n);
            }
            (ColumnBuilder::String(builder), ScalarRef::String(value)) => {
                StringType::push_item_repeat(builder, *value, n);
            }
            (ColumnBuilder::Array(builder), ScalarRef::Array(value))
            | (ColumnBuilder::Map(builder), ScalarRef::Map(value)) => {
                builder.push_repeat(value, n);
            }
            (ColumnBuilder::Bitmap(builder), ScalarRef::Bitmap(value)) => {
                BitmapType::push_item_repeat(builder, value, n);
            }
            (ColumnBuilder::Nullable(builder), ScalarRef::Null) => {
                NullableType::push_item_repeat(builder, None, n);
            }
            (ColumnBuilder::Nullable(builder), value) => {
                builder.push_repeat(value.clone(), n);
            }
            (ColumnBuilder::Tuple(fields), ScalarRef::Tuple(value)) => {
                assert_eq!(fields.len(), value.len());
                for (field, scalar) in fields.iter_mut().zip(value.iter()) {
                    field.push_repeat(scalar, n)
                }
            }
            (ColumnBuilder::Variant(builder), ScalarRef::Variant(value)) => {
                VariantType::push_item_repeat(builder, value, n);
            }
            (ColumnBuilder::Geometry(builder), ScalarRef::Geometry(value)) => {
                GeometryType::push_item_repeat(builder, value, n);
            }
            (ColumnBuilder::Geography(builder), ScalarRef::Geography(value)) => {
                GeographyType::push_item_repeat(builder, *value, n);
            }
            (builder, scalar) => unreachable!("unable to push {scalar:?} to {builder:?}"),
        };
    }

    pub fn push_default(&mut self) {
        match self {
            ColumnBuilder::Null { len } => *len += 1,
            ColumnBuilder::EmptyArray { len } => *len += 1,
            ColumnBuilder::EmptyMap { len } => *len += 1,
            ColumnBuilder::Number(builder) => builder.push_default(),
            ColumnBuilder::Decimal(builder) => builder.push_default(),
            ColumnBuilder::Boolean(builder) => builder.push(false),
            ColumnBuilder::Binary(builder) => builder.commit_row(),
            ColumnBuilder::String(builder) => builder.commit_row(),
            ColumnBuilder::Timestamp(builder) => builder.push(0),
            ColumnBuilder::Date(builder) => builder.push(0),
            ColumnBuilder::Array(builder) => builder.push_default(),
            ColumnBuilder::Map(builder) => builder.push_default(),
            ColumnBuilder::Bitmap(builder) => builder.commit_row(),
            ColumnBuilder::Nullable(builder) => builder.push_null(),
            ColumnBuilder::Tuple(fields) => {
                for field in fields {
                    field.push_default();
                }
            }
            ColumnBuilder::Variant(builder) => {
                builder.put_slice(JSONB_NULL);
                builder.commit_row();
            }
            ColumnBuilder::Geometry(builder) => builder.commit_row(),
            ColumnBuilder::Geography(builder) => builder.push_default(),
        }
    }

    pub fn push_binary(&mut self, reader: &mut &[u8]) -> Result<()> {
        match self {
            ColumnBuilder::Null { len } => *len += 1,
            ColumnBuilder::EmptyArray { len } => *len += 1,
            ColumnBuilder::EmptyMap { len } => *len += 1,
            ColumnBuilder::Number(builder) => with_number_mapped_type!(|NUM_TYPE| match builder {
                NumberColumnBuilder::NUM_TYPE(builder) => {
                    let value: NUM_TYPE = reader.read_scalar()?;
                    builder.push(value);
                }
            }),
            ColumnBuilder::Decimal(builder) => {
                with_decimal_mapped_type!(|DECIMAL_TYPE| match builder {
                    DecimalColumnBuilder::DECIMAL_TYPE(builder, _) =>
                        builder.push(DECIMAL_TYPE::de_binary(reader)),
                })
            }
            ColumnBuilder::Boolean(builder) => {
                let v: bool = reader.read_scalar()?;
                builder.push(v);
            }
            ColumnBuilder::Binary(builder)
            | ColumnBuilder::Variant(builder)
            | ColumnBuilder::Bitmap(builder)
            | ColumnBuilder::Geometry(builder) => {
                let offset = reader.read_scalar::<u64>()? as usize;
                builder.data.resize(offset + builder.data.len(), 0);
                let last = *builder.offsets.last().unwrap() as usize;
                reader.read_exact(&mut builder.data[last..last + offset])?;
                builder.commit_row();
            }
            ColumnBuilder::Geography(builder) => {
                let len = reader.read_scalar::<u64>()? as usize;
                let mut buf = vec![0u8; len];
                reader.read_exact(&mut buf)?;
                let value: Geography = borsh::from_slice(&buf)?;
                builder.push(value.as_ref());
            }
            ColumnBuilder::String(builder) => {
                let offset = reader.read_scalar::<u64>()? as usize;
                builder.data.resize(offset + builder.data.len(), 0);
                let last = *builder.offsets.last().unwrap() as usize;
                reader.read_exact(&mut builder.data[last..last + offset])?;
                builder.commit_row();

                #[cfg(debug_assertions)]
                string::CheckUTF8::check_utf8(&(&builder.data[last..last + offset])).unwrap();
            }
            ColumnBuilder::Timestamp(builder) => {
                let value: i64 = reader.read_scalar()?;
                check_timestamp(value)?;
                builder.push(value);
            }
            ColumnBuilder::Date(builder) => {
                let value: i32 = reader.read_scalar()?;
                builder.push(value);
            }
            ColumnBuilder::Array(builder) => {
                let len = reader.read_scalar::<u64>()?;
                for _ in 0..len {
                    builder.builder.push_binary(reader)?;
                }
                builder.commit_row();
            }
            ColumnBuilder::Map(builder) => {
                const KEY: usize = 0;
                const VALUE: usize = 1;
                let len = reader.read_scalar::<u64>()?;
                let map_builder = builder.builder.as_tuple_mut().unwrap();
                for _ in 0..len {
                    map_builder[KEY].push_binary(reader)?;
                    map_builder[VALUE].push_binary(reader)?;
                }
                builder.commit_row();
            }
            ColumnBuilder::Nullable(builder) => {
                let valid: bool = reader.read_scalar()?;
                if valid {
                    builder.builder.push_binary(reader)?;
                    builder.validity.push(true);
                } else {
                    builder.push_null();
                }
            }
            ColumnBuilder::Tuple(fields) => {
                for field in fields {
                    field.push_binary(reader)?;
                }
            }
        };

        Ok(())
    }

    pub fn push_fix_len_binaries(&mut self, reader: &[u8], step: usize, rows: usize) -> Result<()> {
        match self {
            ColumnBuilder::Null { len } => *len += rows,
            ColumnBuilder::EmptyArray { len } => *len += rows,
            ColumnBuilder::EmptyMap { len } => *len += rows,
            ColumnBuilder::Number(builder) => with_number_mapped_type!(|NUM_TYPE| match builder {
                NumberColumnBuilder::NUM_TYPE(builder) => {
                    for row in 0..rows {
                        let mut reader = &reader[step * row..];
                        let value: NUM_TYPE = reader.read_scalar()?;
                        builder.push(value);
                    }
                }
            }),
            ColumnBuilder::Decimal(builder) => {
                with_decimal_mapped_type!(|DECIMAL_TYPE| match builder {
                    DecimalColumnBuilder::DECIMAL_TYPE(builder, _) => {
                        for row in 0..rows {
                            let mut reader = &reader[step * row..];
                            builder.push(DECIMAL_TYPE::de_binary(&mut reader));
                        }
                    }
                })
            }
            ColumnBuilder::Boolean(builder) => {
                for row in 0..rows {
                    let mut reader = &reader[step * row..];
                    let v: bool = reader.read_scalar()?;
                    builder.push(v);
                }
            }
            ColumnBuilder::Binary(builder)
            | ColumnBuilder::Variant(builder)
            | ColumnBuilder::Bitmap(builder)
            | ColumnBuilder::Geometry(builder) => {
                for row in 0..rows {
                    let reader = &reader[step * row..];
                    builder.put_slice(reader);
                    builder.commit_row();
                }
            }
            ColumnBuilder::Geography(builder) => {
                for row in 0..rows {
                    let reader = &reader[step * row..];
                    let value: Geography = borsh::from_slice(reader)?;
                    builder.push(value.as_ref());
                }
            }
            ColumnBuilder::String(builder) => {
                for row in 0..rows {
                    let bytes = &reader[step * row..];

                    #[cfg(debug_assertions)]
                    string::CheckUTF8::check_utf8(&bytes).unwrap();

                    let s = unsafe { std::str::from_utf8_unchecked(bytes) };
                    builder.put_str(s);
                    builder.commit_row();
                }
            }
            ColumnBuilder::Timestamp(builder) => {
                for row in 0..rows {
                    let mut reader = &reader[step * row..];
                    let value: i64 = reader.read_scalar()?;
                    check_timestamp(value)?;
                    builder.push(value);
                }
            }
            ColumnBuilder::Date(builder) => {
                for row in 0..rows {
                    let mut reader = &reader[step * row..];
                    let value: i32 = reader.read_scalar()?;
                    builder.push(value);
                }
            }
            ColumnBuilder::Array(builder) => {
                for row in 0..rows {
                    let mut reader = &reader[step * row..];
                    let len = reader.read_uvarint()?;
                    for _ in 0..len {
                        builder.builder.push_binary(&mut reader)?;
                    }
                    builder.commit_row();
                }
            }
            ColumnBuilder::Map(builder) => {
                const KEY: usize = 0;
                const VALUE: usize = 1;
                for row in 0..rows {
                    let mut reader = &reader[step * row..];
                    let map_builder = builder.builder.as_tuple_mut().unwrap();
                    let len = reader.read_uvarint()?;
                    for _ in 0..len {
                        map_builder[KEY].push_binary(&mut reader)?;
                        map_builder[VALUE].push_binary(&mut reader)?;
                    }
                    builder.commit_row();
                }
            }
            ColumnBuilder::Nullable(_) => {
                unimplemented!()
            }
            ColumnBuilder::Tuple(fields) => {
                for row in 0..rows {
                    let mut reader = &reader[step * row..];
                    for field in fields.iter_mut() {
                        field.push_binary(&mut reader)?;
                    }
                }
            }
        }

        Ok(())
    }

    pub fn pop(&mut self) -> Option<Scalar> {
        match self {
            ColumnBuilder::Null { len } => {
                if *len > 0 {
                    *len -= 1;
                    Some(Scalar::Null)
                } else {
                    None
                }
            }
            ColumnBuilder::EmptyArray { len } => {
                if *len > 0 {
                    *len -= 1;
                    Some(Scalar::EmptyArray)
                } else {
                    None
                }
            }
            ColumnBuilder::EmptyMap { len } => {
                if *len > 0 {
                    *len -= 1;
                    Some(Scalar::EmptyMap)
                } else {
                    None
                }
            }
            ColumnBuilder::Number(builder) => builder.pop().map(Scalar::Number),
            ColumnBuilder::Decimal(builder) => builder.pop().map(Scalar::Decimal),
            ColumnBuilder::Boolean(builder) => builder.pop().map(Scalar::Boolean),
            ColumnBuilder::Binary(builder) => builder.pop().map(Scalar::Binary),
            ColumnBuilder::String(builder) => builder.pop().map(Scalar::String),
            ColumnBuilder::Timestamp(builder) => builder.pop().map(Scalar::Timestamp),
            ColumnBuilder::Date(builder) => builder.pop().map(Scalar::Date),
            ColumnBuilder::Array(builder) => builder.pop().map(Scalar::Array),
            ColumnBuilder::Map(builder) => builder.pop().map(Scalar::Map),
            ColumnBuilder::Bitmap(builder) => builder.pop().map(Scalar::Bitmap),
            ColumnBuilder::Nullable(builder) => Some(builder.pop()?.unwrap_or(Scalar::Null)),
            ColumnBuilder::Tuple(fields) => {
                if fields[0].len() > 0 {
                    Some(Scalar::Tuple(
                        fields
                            .iter_mut()
                            .map(|field| field.pop().unwrap())
                            .collect(),
                    ))
                } else {
                    None
                }
            }
            ColumnBuilder::Variant(builder) => builder.pop().map(Scalar::Variant),
            ColumnBuilder::Geometry(builder) => builder.pop().map(Scalar::Geometry),
            ColumnBuilder::Geography(builder) => builder.pop().map(Scalar::Geography),
        }
    }

    pub fn append_column(&mut self, other: &Column) {
        match (self, other) {
            (ColumnBuilder::Null { len }, Column::Null { len: other_len }) => {
                *len += other_len;
            }
            (ColumnBuilder::EmptyArray { len }, Column::EmptyArray { len: other_len }) => {
                *len += other_len;
            }
            (ColumnBuilder::EmptyMap { len }, Column::EmptyMap { len: other_len }) => {
                *len += other_len;
            }
            (ColumnBuilder::Number(builder), Column::Number(column)) => {
                builder.append_column(column);
            }
            (ColumnBuilder::Decimal(builder), Column::Decimal(column)) => {
                builder.append_column(column);
            }
            (ColumnBuilder::Boolean(builder), Column::Boolean(other)) => {
                append_bitmap(builder, other);
            }
            (ColumnBuilder::Binary(builder), Column::Binary(other)) => {
                builder.append_column(other);
            }
            (ColumnBuilder::String(builder), Column::String(other)) => {
                builder.append_column(other);
            }
            (ColumnBuilder::Variant(builder), Column::Variant(other)) => {
                builder.append_column(other);
            }
            (ColumnBuilder::Geometry(builder), Column::Geometry(other)) => {
                builder.append_column(other);
            }
            (ColumnBuilder::Geography(builder), Column::Geography(other)) => {
                GeographyType::append_column(builder, other);
            }
            (ColumnBuilder::Timestamp(builder), Column::Timestamp(other)) => {
                builder.extend_from_slice(other);
            }
            (ColumnBuilder::Date(builder), Column::Date(other)) => {
                builder.extend_from_slice(other);
            }
            (ColumnBuilder::Array(builder), Column::Array(other)) => {
                builder.append_column(other.as_ref());
            }
            (ColumnBuilder::Map(builder), Column::Map(other)) => {
                builder.append_column(other.as_ref());
            }
            (ColumnBuilder::Bitmap(builder), Column::Bitmap(other)) => {
                builder.append_column(other);
            }
            (ColumnBuilder::Nullable(builder), Column::Nullable(other)) => {
                builder.append_column(other);
            }
            (ColumnBuilder::Tuple(fields), Column::Tuple(other_fields)) => {
                assert_eq!(fields.len(), other_fields.len());
                for (field, other_field) in fields.iter_mut().zip(other_fields.iter()) {
                    field.append_column(other_field);
                }
            }
            (this, other) => unreachable!(
                "unable append column(data type: {:?}) into builder(data type: {:?})",
                other.data_type(),
                this.data_type()
            ),
        }
    }

    pub fn build(self) -> Column {
        match self {
            ColumnBuilder::Null { len } => Column::Null { len },
            ColumnBuilder::EmptyArray { len } => Column::EmptyArray { len },
            ColumnBuilder::EmptyMap { len } => Column::EmptyMap { len },
            ColumnBuilder::Number(builder) => Column::Number(builder.build()),
            ColumnBuilder::Decimal(builder) => Column::Decimal(builder.build()),
            ColumnBuilder::Array(builder) => Column::Array(Box::new(builder.build())),
            ColumnBuilder::Map(builder) => Column::Map(Box::new(builder.build())),
            ColumnBuilder::Nullable(builder) => Column::Nullable(Box::new(builder.build())),
            ColumnBuilder::Tuple(fields) => {
                assert!(fields.iter().map(|field| field.len()).all_equal());
                Column::Tuple(fields.into_iter().map(|field| field.build()).collect())
            }

            ColumnBuilder::Boolean(b) => Column::Boolean(BooleanType::build_column(b)),
            ColumnBuilder::Binary(b) => Column::Binary(BinaryType::build_column(b)),
            ColumnBuilder::String(b) => Column::String(StringType::build_column(b)),
            ColumnBuilder::Timestamp(b) => Column::Timestamp(TimestampType::build_column(b)),
            ColumnBuilder::Date(b) => Column::Date(DateType::build_column(b)),
            ColumnBuilder::Bitmap(b) => Column::Bitmap(BitmapType::build_column(b)),
            ColumnBuilder::Variant(b) => Column::Variant(VariantType::build_column(b)),
            ColumnBuilder::Geometry(b) => Column::Geometry(GeometryType::build_column(b)),
            ColumnBuilder::Geography(b) => Column::Geography(GeographyType::build_column(b)),
        }
    }

    pub fn build_scalar(self) -> Scalar {
        assert_eq!(self.len(), 1);
        match self {
            ColumnBuilder::Null { .. } => Scalar::Null,
            ColumnBuilder::EmptyArray { .. } => Scalar::EmptyArray,
            ColumnBuilder::EmptyMap { .. } => Scalar::EmptyMap,
            ColumnBuilder::Number(builder) => Scalar::Number(builder.build_scalar()),
            ColumnBuilder::Decimal(builder) => Scalar::Decimal(builder.build_scalar()),
            ColumnBuilder::Array(builder) => Scalar::Array(builder.build_scalar()),
            ColumnBuilder::Map(builder) => Scalar::Map(builder.build_scalar()),
            ColumnBuilder::Nullable(builder) => builder.build_scalar().unwrap_or(Scalar::Null),
            ColumnBuilder::Tuple(fields) => Scalar::Tuple(
                fields
                    .into_iter()
                    .map(|field| field.build_scalar())
                    .collect(),
            ),

            ColumnBuilder::Boolean(b) => Scalar::Boolean(BooleanType::build_scalar(b)),
            ColumnBuilder::Binary(b) => Scalar::Binary(BinaryType::build_scalar(b)),
            ColumnBuilder::String(b) => Scalar::String(StringType::build_scalar(b)),
            ColumnBuilder::Timestamp(b) => Scalar::Timestamp(TimestampType::build_scalar(b)),
            ColumnBuilder::Date(b) => Scalar::Date(DateType::build_scalar(b)),
            ColumnBuilder::Bitmap(b) => Scalar::Bitmap(BitmapType::build_scalar(b)),
            ColumnBuilder::Variant(b) => Scalar::Variant(VariantType::build_scalar(b)),
            ColumnBuilder::Geometry(b) => Scalar::Geometry(GeometryType::build_scalar(b)),
            ColumnBuilder::Geography(b) => Scalar::Geography(GeographyType::build_scalar(b)),
        }
    }
}

pub struct ColumnIterator<'a> {
    column: &'a Column,
    index: usize,
    len: usize,
}

impl<'a> Iterator for ColumnIterator<'a> {
    type Item = ScalarRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.len {
            let item = self.column.index(self.index)?;
            self.index += 1;
            Some(item)
        } else {
            None
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remain = self.len - self.index;
        (remain, Some(remain))
    }
}

unsafe impl<'a> TrustedLen for ColumnIterator<'a> {}

#[macro_export]
macro_rules! for_all_number_varints {
    ($macro:tt $(, $x:tt)*) => {
        $macro! {
            [$($x),*],
            { i8, Int8 },
            { i16, Int16 },
            { i32, Int32 },
            { i64, Int64 },
            { u8, UInt8 },
            { u16, UInt16 },
            { u32, UInt32 },
            { u64, UInt64 },
            { f32, Float32 },
            { f64, Float64 },
            { F32, Float32 },
            { F64, Float64 }
        }
    };
}

macro_rules! impl_scalar_from {
    ([], $( { $S: ident, $TY: ident} ),*) => {
        $(
            impl From<$S> for Scalar {
                fn from(value: $S) -> Self {
                    Scalar::Number(NumberScalar::$TY(value.into()))
                }
            }
        )*
    }
}

for_all_number_varints! {impl_scalar_from}
