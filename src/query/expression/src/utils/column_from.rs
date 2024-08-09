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

use std::iter::Iterator;

use databend_common_arrow::arrow::bitmap::MutableBitmap;
use itertools::Itertools;

use crate::types::decimal::*;
use crate::types::geometry::GeometryType;
use crate::types::nullable::NullableColumn;
use crate::types::number::*;
use crate::types::*;
use crate::Column;

pub trait FromData<D> {
    fn from_data(_: Vec<D>) -> Column;

    fn from_data_with_validity(d: Vec<D>, valids: Vec<bool>) -> Column {
        let column = Self::from_data(d);
        Column::Nullable(Box::new(NullableColumn {
            column,
            validity: valids.into(),
        }))
    }

    fn from_opt_data(_: Vec<Option<D>>) -> Column;
}

macro_rules! impl_from_data {
    ($T: ident) => {
        impl FromData<<$T as ValueType>::Scalar> for $T {
            fn from_data(d: Vec<<$T as ValueType>::Scalar>) -> Column {
                $T::upcast_column($T::column_from_iter(d.into_iter(), &[]))
            }

            fn from_opt_data(d: Vec<Option<<$T as ValueType>::Scalar>>) -> Column {
                type NT = NullableType<$T>;
                NT::upcast_column(NT::column_from_iter(d.into_iter(), &[]))
            }
        }
    };
}

impl_from_data! { Int8Type }
impl_from_data! { Int16Type }
impl_from_data! { Int32Type }
impl_from_data! { Int64Type }
impl_from_data! { UInt8Type }
impl_from_data! { UInt16Type }
impl_from_data! { UInt32Type }
impl_from_data! { UInt64Type }
impl_from_data! { Float32Type }
impl_from_data! { Float64Type }
impl_from_data! { Decimal128Type }
impl_from_data! { Decimal256Type }
impl_from_data! { BooleanType }
impl_from_data! { BinaryType }
impl_from_data! { StringType }
impl_from_data! { DateType }
impl_from_data! { TimestampType }
impl_from_data! { VariantType }
impl_from_data! { BitmapType }
impl_from_data! { GeometryType }
impl_from_data! { GeographyType }

impl<'a> FromData<&'a [u8]> for BinaryType {
    fn from_data(d: Vec<&'a [u8]>) -> Column {
        BinaryType::from_data(d.into_iter().map(|d| d.to_vec()).collect_vec())
    }

    fn from_opt_data(d: Vec<Option<&'a [u8]>>) -> Column {
        BinaryType::from_opt_data(d.into_iter().map(|d| d.map(|d| d.to_vec())).collect_vec())
    }
}

impl<'a> FromData<&'a str> for StringType {
    fn from_data(d: Vec<&'a str>) -> Column {
        StringType::from_data(d.into_iter().map(|d| d.to_string()).collect_vec())
    }

    fn from_opt_data(d: Vec<Option<&'a str>>) -> Column {
        StringType::from_opt_data(
            d.into_iter()
                .map(|d| d.map(|d| d.to_string()))
                .collect_vec(),
        )
    }
}

impl FromData<f32> for Float32Type {
    fn from_data(d: Vec<f32>) -> Column {
        Float32Type::from_data(d.into_iter().map(F32::from).collect_vec())
    }

    fn from_opt_data(d: Vec<Option<f32>>) -> Column {
        Float32Type::from_opt_data(d.into_iter().map(|d| d.map(F32::from)).collect_vec())
    }
}

impl FromData<f64> for Float64Type {
    fn from_data(d: Vec<f64>) -> Column {
        Float64Type::from_data(d.into_iter().map(F64::from).collect_vec())
    }

    fn from_opt_data(d: Vec<Option<f64>>) -> Column {
        Float64Type::from_opt_data(d.into_iter().map(|d| d.map(F64::from)).collect_vec())
    }
}

impl<Num: Decimal> DecimalType<Num> {
    pub fn from_data_with_size<D: AsRef<[Num]>>(d: D, size: DecimalSize) -> Column {
        Num::upcast_column(
            Self::column_from_iter(d.as_ref().iter().copied(), &[]),
            size,
        )
    }

    pub fn from_opt_data_with_size<D: AsRef<[Option<Num>]>>(d: D, size: DecimalSize) -> Column {
        let mut validity = MutableBitmap::with_capacity(d.as_ref().len());
        let mut data = Vec::with_capacity(d.as_ref().len());
        for v in d.as_ref() {
            if let Some(v) = v {
                data.push(*v);
                validity.push(true);
            } else {
                data.push(Num::default());
                validity.push(false);
            }
        }
        let col = Self::from_data_with_size(data, size);
        Column::Nullable(Box::new(NullableColumn {
            column: col,
            validity: validity.into(),
        }))
    }
}
