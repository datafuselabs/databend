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
use std::fmt::Display;
use std::io::Cursor;
use std::ops::Range;

use chrono::DateTime;
use chrono_tz::Tz;
use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_exception::ErrorCode;
use databend_common_io::cursor_ext::BufferReadDateTimeExt;
use databend_common_io::cursor_ext::DateTimeResType;
use databend_common_io::cursor_ext::ReadBytesExt;

use super::number::SimpleDomain;
use crate::property::Domain;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::DecimalSize;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::utils::arrow::buffer_into_mut;
use crate::utils::date_helper::DateConverter;
use crate::values::Column;
use crate::values::Scalar;
use crate::ColumnBuilder;
use crate::ScalarRef;

pub const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.6f";
/// Minimum valid timestamp `1000-01-01 00:00:00.000000`, represented by the microsecs offset from 1970-01-01.
pub const TIMESTAMP_MIN: i64 = -30610224000000000;
/// Maximum valid timestamp `9999-12-31 23:59:59.999999`, represented by the microsecs offset from 1970-01-01.
pub const TIMESTAMP_MAX: i64 = 253402300799999999;

pub const MICROS_PER_SEC: i64 = 1_000_000;
pub const MICROS_PER_MILLI: i64 = 1_000;

pub const PRECISION_MICRO: u8 = 6;
pub const PRECISION_MILLI: u8 = 3;
pub const PRECISION_SEC: u8 = 0;

/// Check if the timestamp value is valid.
#[inline]
pub fn check_timestamp(micros: i64) -> Result<i64, String> {
    if (TIMESTAMP_MIN..=TIMESTAMP_MAX).contains(&micros) {
        Ok(micros)
    } else {
        Err("timestamp is out of range".to_string())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimestampType;

impl ValueType for TimestampType {
    type Scalar = i64;
    type ScalarRef<'a> = i64;
    type Column = Buffer<i64>;
    type Domain = SimpleDomain<i64>;
    type ColumnIterator<'a> = std::iter::Cloned<std::slice::Iter<'a, i64>>;
    type ColumnBuilder = Vec<i64>;

    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: i64) -> i64 {
        long
    }

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        scalar
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        *scalar
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        match scalar {
            ScalarRef::Timestamp(scalar) => Some(*scalar),
            _ => None,
        }
    }

    fn try_downcast_column(col: &Column) -> Option<Self::Column> {
        match col {
            Column::Timestamp(column) => Some(column.clone()),
            _ => None,
        }
    }

    fn try_downcast_domain(domain: &Domain) -> Option<SimpleDomain<i64>> {
        domain.as_timestamp().cloned()
    }

    fn try_downcast_builder(builder: &mut ColumnBuilder) -> Option<&mut Self::ColumnBuilder> {
        match builder {
            ColumnBuilder::Timestamp(builder) => Some(builder),
            _ => None,
        }
    }

    fn try_downcast_owned_builder(builder: ColumnBuilder) -> Option<Self::ColumnBuilder> {
        match builder {
            ColumnBuilder::Timestamp(builder) => Some(builder),
            _ => None,
        }
    }

    fn try_upcast_column_builder(
        builder: Self::ColumnBuilder,
        _decimal_size: Option<DecimalSize>,
    ) -> Option<ColumnBuilder> {
        Some(ColumnBuilder::Timestamp(builder))
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        Scalar::Timestamp(scalar)
    }

    fn upcast_column(col: Self::Column) -> Column {
        Column::Timestamp(col)
    }

    fn upcast_domain(domain: SimpleDomain<i64>) -> Domain {
        Domain::Timestamp(domain)
    }

    fn column_len(col: &Self::Column) -> usize {
        col.len()
    }

    fn index_column(col: &Self::Column, index: usize) -> Option<Self::ScalarRef<'_>> {
        col.get(index).cloned()
    }

    #[inline(always)]
    unsafe fn index_column_unchecked(col: &Self::Column, index: usize) -> Self::ScalarRef<'_> {
        debug_assert!(index < col.len());

        *col.get_unchecked(index)
    }

    fn slice_column(col: &Self::Column, range: Range<usize>) -> Self::Column {
        col.clone().sliced(range.start, range.end - range.start)
    }

    fn iter_column(col: &Self::Column) -> Self::ColumnIterator<'_> {
        col.iter().cloned()
    }

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        buffer_into_mut(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::Scalar) {
        builder.push(item);
    }

    fn push_item_repeat(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>, n: usize) {
        builder.resize(builder.len() + n, item);
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        builder.push(Self::Scalar::default());
    }

    fn append_column(builder: &mut Self::ColumnBuilder, other: &Self::Column) {
        builder.extend_from_slice(other);
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        builder.into()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        assert_eq!(builder.len(), 1);
        builder[0]
    }

    #[inline(always)]
    fn compare(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> Ordering {
        left.cmp(&right)
    }

    #[inline(always)]
    fn equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        left == right
    }

    #[inline(always)]
    fn not_equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        left != right
    }

    #[inline(always)]
    fn greater_than(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        left > right
    }

    #[inline(always)]
    fn greater_than_equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        left >= right
    }

    #[inline(always)]
    fn less_than(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        left < right
    }

    #[inline(always)]
    fn less_than_equal(left: Self::ScalarRef<'_>, right: Self::ScalarRef<'_>) -> bool {
        left <= right
    }
}

impl ArgType for TimestampType {
    fn data_type() -> DataType {
        DataType::Timestamp
    }

    fn full_domain() -> Self::Domain {
        SimpleDomain {
            min: TIMESTAMP_MIN,
            max: TIMESTAMP_MAX,
        }
    }

    fn create_builder(capacity: usize, _generics: &GenericMap) -> Self::ColumnBuilder {
        Vec::with_capacity(capacity)
    }

    fn column_from_vec(vec: Vec<Self::Scalar>, _generics: &GenericMap) -> Self::Column {
        vec.into()
    }

    fn column_from_iter(iter: impl Iterator<Item = Self::Scalar>, _: &GenericMap) -> Self::Column {
        iter.collect()
    }

    fn column_from_ref_iter<'a>(
        iter: impl Iterator<Item = Self::ScalarRef<'a>>,
        _: &GenericMap,
    ) -> Self::Column {
        iter.collect()
    }
}

pub fn microseconds_to_seconds(micros: i64) -> i64 {
    micros / MICROS_PER_SEC
}

pub fn microseconds_to_days(micros: i64) -> i32 {
    (microseconds_to_seconds(micros) / 24 / 3600) as i32
}

#[inline]
pub fn string_to_timestamp(
    ts_str: impl AsRef<[u8]>,
    tz: Tz,
    enable_dst_hour_fix: bool,
) -> databend_common_exception::Result<DateTime<Tz>> {
    let mut reader = Cursor::new(std::str::from_utf8(ts_str.as_ref()).unwrap().as_bytes());
    match reader.read_timestamp_text(&tz, false, enable_dst_hour_fix) {
        Ok(dt) => match dt {
            DateTimeResType::Datetime(dt) => match reader.must_eof() {
                Ok(..) => Ok(dt),
                Err(_) => Err(ErrorCode::BadArguments("unexpected argument")),
            },
            _ => unreachable!(),
        },
        Err(e) => match e.code() {
            ErrorCode::BAD_BYTES => Err(e),
            _ => Err(ErrorCode::BadArguments("unexpected argument")),
        },
    }
}

#[inline]
pub fn timestamp_to_string(ts: i64, tz: Tz) -> impl Display {
    ts.to_timestamp(tz).format(TIMESTAMP_FORMAT)
}
