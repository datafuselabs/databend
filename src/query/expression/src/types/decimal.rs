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
use std::fmt::Debug;
use std::marker::PhantomData;
use std::ops::Range;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_io::display_decimal_128;
use databend_common_io::display_decimal_256;
use enum_as_inner::EnumAsInner;
use ethnum::i256;
use ethnum::u256;
use ethnum::AsI256;
use itertools::Itertools;
use num_bigint::BigInt;
use num_traits::FromBytes;
use num_traits::NumCast;
use num_traits::ToPrimitive;
use serde::Deserialize;
use serde::Serialize;

use super::SimpleDomain;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::utils::arrow::buffer_into_mut;
use crate::Column;
use crate::ColumnBuilder;
use crate::Domain;
use crate::Scalar;
use crate::ScalarRef;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecimalType<T: Decimal>(PhantomData<T>);

pub type Decimal128Type = DecimalType<i128>;
pub type Decimal256Type = DecimalType<i256>;

impl<Num: Decimal> ValueType for DecimalType<Num> {
    type Scalar = Num;
    type ScalarRef<'a> = Num;
    type Column = Buffer<Num>;
    type Domain = SimpleDomain<Num>;
    type ColumnIterator<'a> = std::iter::Cloned<std::slice::Iter<'a, Num>>;
    type ColumnBuilder = Vec<Num>;

    fn upcast_gat<'short, 'long: 'short>(long: Self::ScalarRef<'long>) -> Self::ScalarRef<'short> {
        long
    }

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        scalar
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        *scalar
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        Num::try_downcast_scalar(scalar.as_decimal()?)
    }

    fn try_downcast_column(col: &Column) -> Option<Self::Column> {
        let down_col = Num::try_downcast_column(col);
        if let Some(col) = down_col {
            Some(col.0)
        } else {
            None
        }
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        Num::try_downcast_domain(domain.as_decimal()?)
    }

    fn try_downcast_builder(builder: &mut ColumnBuilder) -> Option<&mut Self::ColumnBuilder> {
        Num::try_downcast_builder(builder)
    }

    fn try_downcast_owned_builder(builder: ColumnBuilder) -> Option<Self::ColumnBuilder> {
        Num::try_downcast_owned_builder(builder)
    }

    fn try_upcast_column_builder(
        builder: Self::ColumnBuilder,
        decimal_size: Option<DecimalSize>,
    ) -> Option<ColumnBuilder> {
        Some(ColumnBuilder::Decimal(Num::upcast_builder(
            builder,
            decimal_size.unwrap(),
        )))
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        Num::upcast_scalar(scalar, Num::default_decimal_size())
    }

    fn upcast_column(col: Self::Column) -> Column {
        Num::upcast_column(col, Num::default_decimal_size())
    }

    fn upcast_domain(domain: Self::Domain) -> Domain {
        Num::upcast_domain(domain, Num::default_decimal_size())
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

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>) {
        builder.push(item)
    }

    fn push_item_repeat(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>, n: usize) {
        if n == 1 {
            builder.push(item)
        } else {
            builder.resize(builder.len() + n, item)
        }
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        builder.push(Num::default())
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

impl<Num: Decimal> ArgType for DecimalType<Num> {
    fn data_type() -> DataType {
        Num::data_type()
    }

    fn full_domain() -> Self::Domain {
        SimpleDomain {
            min: Num::MIN,
            max: Num::MAX,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, EnumAsInner)]
pub enum DecimalDataType {
    Decimal128(DecimalSize),
    Decimal256(DecimalSize),
}

#[derive(
    Clone,
    Copy,
    PartialEq,
    Eq,
    EnumAsInner,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
)]
pub enum DecimalScalar {
    Decimal128(i128, DecimalSize),
    Decimal256(i256, DecimalSize),
}

impl DecimalScalar {
    pub fn to_float64(&self) -> f64 {
        match self {
            DecimalScalar::Decimal128(v, size) => i128::to_float64(*v, size.scale),
            DecimalScalar::Decimal256(v, size) => i256::to_float64(*v, size.scale),
        }
    }
    pub fn is_positive(&self) -> bool {
        match self {
            DecimalScalar::Decimal128(v, _) => i128::is_positive(*v),
            DecimalScalar::Decimal256(v, _) => i256::is_positive(*v),
        }
    }
}

#[derive(Clone, PartialEq, EnumAsInner)]
pub enum DecimalColumn {
    Decimal128(Buffer<i128>, DecimalSize),
    Decimal256(Buffer<i256>, DecimalSize),
}

#[derive(Clone, PartialEq, EnumAsInner, Debug)]
pub enum DecimalColumnVec {
    Decimal128(Vec<Buffer<i128>>, DecimalSize),
    Decimal256(Vec<Buffer<i256>>, DecimalSize),
}

#[derive(Debug, Clone, PartialEq, Eq, EnumAsInner)]
pub enum DecimalColumnBuilder {
    Decimal128(Vec<i128>, DecimalSize),
    Decimal256(Vec<i256>, DecimalSize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, EnumAsInner)]
pub enum DecimalDomain {
    Decimal128(SimpleDomain<i128>, DecimalSize),
    Decimal256(SimpleDomain<i256>, DecimalSize),
}

impl DecimalDomain {
    pub fn decimal_size(&self) -> DecimalSize {
        match self {
            DecimalDomain::Decimal128(_, size) => *size,
            DecimalDomain::Decimal256(_, size) => *size,
        }
    }
}

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
)]
pub struct DecimalSize {
    pub precision: u8,
    pub scale: u8,
}

pub trait Decimal:
    Sized
    + Default
    + Debug
    + std::fmt::Display
    + Copy
    + Clone
    + PartialEq
    + Eq
    + PartialOrd
    + Ord
    + Sync
    + Send
    + 'static
{
    fn zero() -> Self;
    fn one() -> Self;
    fn minus_one() -> Self;

    // 10**scale
    fn e(n: u32) -> Self;

    fn mem_size() -> usize;

    fn checked_add(self, rhs: Self) -> Option<Self>;
    fn checked_sub(self, rhs: Self) -> Option<Self>;
    fn checked_div(self, rhs: Self) -> Option<Self>;
    fn checked_mul(self, rhs: Self) -> Option<Self>;
    fn checked_rem(self, rhs: Self) -> Option<Self>;

    fn do_round_div(self, rhs: Self, mul_scale: u32) -> Option<Self>;

    // mul two decimals and return a decimal with rounding option
    fn do_round_mul(self, rhs: Self, shift_scale: u32) -> Option<Self>;

    fn min_for_precision(precision: u8) -> Self;
    fn max_for_precision(precision: u8) -> Self;

    fn default_decimal_size() -> DecimalSize;

    fn from_float(value: f64) -> Self;
    fn from_i128<U: Into<i128>>(value: U) -> Self;
    fn from_bigint(value: BigInt) -> Option<Self>;

    fn de_binary(bytes: &mut &[u8]) -> Self;
    fn display(self, scale: u8) -> String;

    fn to_float32(self, scale: u8) -> f32;
    fn to_float64(self, scale: u8) -> f64;

    fn to_int<U: NumCast>(self, scale: u8, rounding_mode: bool) -> Option<U>;

    fn try_downcast_column(column: &Column) -> Option<(Buffer<Self>, DecimalSize)>;
    fn try_downcast_builder(builder: &mut ColumnBuilder) -> Option<&mut Vec<Self>>;

    fn try_downcast_owned_builder(builder: ColumnBuilder) -> Option<Vec<Self>>;

    fn try_downcast_scalar(scalar: &DecimalScalar) -> Option<Self>;
    fn try_downcast_domain(domain: &DecimalDomain) -> Option<SimpleDomain<Self>>;

    fn upcast_scalar(scalar: Self, size: DecimalSize) -> Scalar;
    fn upcast_column(col: Buffer<Self>, size: DecimalSize) -> Column;
    fn upcast_domain(domain: SimpleDomain<Self>, size: DecimalSize) -> Domain;
    fn upcast_builder(builder: Vec<Self>, size: DecimalSize) -> DecimalColumnBuilder;
    fn data_type() -> DataType;
    const MIN: Self;
    const MAX: Self;

    fn to_column_from_buffer(value: Buffer<Self>, size: DecimalSize) -> DecimalColumn;

    fn to_column(value: Vec<Self>, size: DecimalSize) -> DecimalColumn {
        Self::to_column_from_buffer(value.into(), size)
    }

    fn to_scalar(self, size: DecimalSize) -> DecimalScalar;

    fn with_size(&self, size: DecimalSize) -> Option<Self> {
        let multiplier = Self::e(size.scale as u32);
        let min_for_precision = Self::min_for_precision(size.precision);
        let max_for_precision = Self::max_for_precision(size.precision);
        self.checked_mul(multiplier).and_then(|v| {
            if v > max_for_precision || v < min_for_precision {
                None
            } else {
                Some(v)
            }
        })
    }
}

impl Decimal for i128 {
    fn zero() -> Self {
        0_i128
    }

    fn one() -> Self {
        1_i128
    }

    fn minus_one() -> Self {
        -1_i128
    }

    fn e(n: u32) -> Self {
        10_i128.pow(n)
    }

    fn mem_size() -> usize {
        16
    }

    fn checked_add(self, rhs: Self) -> Option<Self> {
        self.checked_add(rhs)
    }

    fn checked_sub(self, rhs: Self) -> Option<Self> {
        self.checked_sub(rhs)
    }

    fn checked_div(self, rhs: Self) -> Option<Self> {
        self.checked_div(rhs)
    }

    fn checked_mul(self, rhs: Self) -> Option<Self> {
        self.checked_mul(rhs)
    }

    fn checked_rem(self, rhs: Self) -> Option<Self> {
        self.checked_rem(rhs)
    }

    fn do_round_mul(self, rhs: Self, shift_scale: u32) -> Option<Self> {
        let div = i256::e(shift_scale);
        let res = if self.is_negative() == rhs.is_negative() {
            (i256::from(self) * i256::from(rhs) + div / 2) / div
        } else {
            (i256::from(self) * i256::from(rhs) - div / 2) / div
        };

        if !(i128::MIN..=i128::MAX).contains(&res) {
            None
        } else {
            Some(res.as_i128())
        }
    }

    fn do_round_div(self, rhs: Self, mul_scale: u32) -> Option<Self> {
        let mul = i256::e(mul_scale);
        if self.is_negative() == rhs.is_negative() {
            let res = (i256::from(self) * i256::from(mul) + i256::from(rhs) / 2) / i256::from(rhs);
            Some(*res.low())
        } else {
            let res = (i256::from(self) * i256::from(mul) - i256::from(rhs) / 2) / i256::from(rhs);
            Some(*res.low())
        }
    }

    fn min_for_precision(to_precision: u8) -> Self {
        MIN_DECIMAL_FOR_EACH_PRECISION[to_precision as usize - 1]
    }

    fn max_for_precision(to_precision: u8) -> Self {
        MAX_DECIMAL_FOR_EACH_PRECISION[to_precision as usize - 1]
    }

    fn default_decimal_size() -> DecimalSize {
        DecimalSize {
            precision: MAX_DECIMAL128_PRECISION,
            scale: 0,
        }
    }

    fn to_column_from_buffer(value: Buffer<Self>, size: DecimalSize) -> DecimalColumn {
        DecimalColumn::Decimal128(value, size)
    }

    fn from_float(value: f64) -> Self {
        // still needs to be optimized.
        // An implementation similar to float64_as_i256 obtained from the ethnum library
        const M: u64 = (f64::MANTISSA_DIGITS - 1) as u64;
        const MAN_MASK: u64 = !(!0 << M);
        const MAN_ONE: u64 = 1 << M;
        const EXP_MASK: u64 = !0 >> f64::MANTISSA_DIGITS;
        const EXP_OFFSET: u64 = EXP_MASK / 2;
        const ABS_MASK: u64 = !0 >> 1;
        const SIG_MASK: u64 = !ABS_MASK;

        let abs = f64::from_bits(value.to_bits() & ABS_MASK);
        let sign = -(((value.to_bits() & SIG_MASK) >> (u64::BITS - 2)) as i128).wrapping_sub(1); // if self >= 0. { 1 } else { -1 }
        if abs >= 1.0 {
            let bits = abs.to_bits();
            let exponent = ((bits >> M) & EXP_MASK) - EXP_OFFSET;
            let mantissa = (bits & MAN_MASK) | MAN_ONE;
            if exponent <= M {
                (<i128 as From<u64>>::from(mantissa >> (M - exponent))) * sign
            } else if exponent < 127 {
                (<i128 as From<u64>>::from(mantissa) << (exponent - M)) * sign
            } else if sign > 0 {
                i128::MAX
            } else {
                i128::MIN
            }
        } else {
            Self::zero()
        }
    }

    fn from_i128<U: Into<i128>>(value: U) -> Self {
        value.into()
    }

    fn from_bigint(value: BigInt) -> Option<Self> {
        value.to_i128()
    }

    fn de_binary(bytes: &mut &[u8]) -> Self {
        let bs: [u8; std::mem::size_of::<Self>()] =
            bytes[0..std::mem::size_of::<Self>()].try_into().unwrap();
        *bytes = &bytes[std::mem::size_of::<Self>()..];

        i128::from_le_bytes(bs)
    }

    fn display(self, scale: u8) -> String {
        display_decimal_128(self, scale)
    }

    fn to_float32(self, scale: u8) -> f32 {
        let div = 10_f32.powi(scale as i32);
        self as f32 / div
    }

    fn to_float64(self, scale: u8) -> f64 {
        let div = 10_f64.powi(scale as i32);
        self as f64 / div
    }

    fn to_int<U: NumCast>(self, scale: u8, rounding_mode: bool) -> Option<U> {
        let div = 10i128.checked_pow(scale as u32)?;
        let mut val = self / div;
        if rounding_mode && scale > 0 {
            // Checking whether numbers need to be added or subtracted to calculate rounding
            if let Some(r) = self.checked_rem(div) {
                if let Some(m) = r.checked_div(i128::e(scale as u32 - 1)) {
                    if m >= 5i128 {
                        val = val.checked_add(1i128)?;
                    } else if m <= -5i128 {
                        val = val.checked_sub(1i128)?;
                    }
                }
            }
        }
        num_traits::cast(val)
    }

    fn to_scalar(self, size: DecimalSize) -> DecimalScalar {
        DecimalScalar::Decimal128(self, size)
    }

    fn try_downcast_column(column: &Column) -> Option<(Buffer<Self>, DecimalSize)> {
        let column = column.as_decimal()?;
        match column {
            DecimalColumn::Decimal128(c, size) => Some((c.clone(), *size)),
            _ => None,
        }
    }

    fn try_downcast_builder(builder: &mut ColumnBuilder) -> Option<&mut Vec<Self>> {
        match builder {
            ColumnBuilder::Decimal(DecimalColumnBuilder::Decimal128(s, _)) => Some(s),
            _ => None,
        }
    }

    fn try_downcast_owned_builder(builder: ColumnBuilder) -> Option<Vec<Self>> {
        match builder {
            ColumnBuilder::Decimal(DecimalColumnBuilder::Decimal128(s, _)) => Some(s),
            _ => None,
        }
    }

    fn try_downcast_scalar<'a>(scalar: &DecimalScalar) -> Option<Self> {
        match scalar {
            DecimalScalar::Decimal128(val, _) => Some(*val),
            _ => None,
        }
    }

    fn try_downcast_domain(domain: &DecimalDomain) -> Option<SimpleDomain<Self>> {
        match domain {
            DecimalDomain::Decimal128(val, _) => Some(*val),
            _ => None,
        }
    }

    // will mock DecimalSize need modify when use it
    fn upcast_scalar(scalar: Self, size: DecimalSize) -> Scalar {
        Scalar::Decimal(DecimalScalar::Decimal128(scalar, size))
    }

    fn upcast_column(col: Buffer<Self>, size: DecimalSize) -> Column {
        Column::Decimal(DecimalColumn::Decimal128(col, size))
    }

    fn upcast_domain(domain: SimpleDomain<Self>, size: DecimalSize) -> Domain {
        Domain::Decimal(DecimalDomain::Decimal128(domain, size))
    }

    fn upcast_builder(builder: Vec<Self>, size: DecimalSize) -> DecimalColumnBuilder {
        DecimalColumnBuilder::Decimal128(builder, size)
    }

    fn data_type() -> DataType {
        DataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
            precision: MAX_DECIMAL128_PRECISION,
            scale: 0,
        }))
    }

    const MIN: i128 = -99999999999999999999999999999999999999i128;

    const MAX: i128 = 99999999999999999999999999999999999999i128;
}

impl Decimal for i256 {
    fn zero() -> Self {
        i256::ZERO
    }

    fn one() -> Self {
        i256::ONE
    }

    fn minus_one() -> Self {
        i256::MINUS_ONE
    }

    fn e(n: u32) -> Self {
        (i256::ONE * 10).pow(n)
    }

    fn mem_size() -> usize {
        32
    }

    fn checked_add(self, rhs: Self) -> Option<Self> {
        self.checked_add(rhs)
    }

    fn checked_sub(self, rhs: Self) -> Option<Self> {
        self.checked_sub(rhs)
    }

    fn checked_div(self, rhs: Self) -> Option<Self> {
        self.checked_div(rhs)
    }

    fn checked_mul(self, rhs: Self) -> Option<Self> {
        self.checked_mul(rhs)
    }

    fn checked_rem(self, rhs: Self) -> Option<Self> {
        self.checked_rem(rhs)
    }

    fn do_round_mul(self, rhs: Self, shift_scale: u32) -> Option<Self> {
        let div = i256::e(shift_scale);
        let ret: Option<i256> = if self.is_negative() == rhs.is_negative() {
            self.checked_mul(rhs).map(|x| (x + div / 2) / div)
        } else {
            self.checked_mul(rhs).map(|x| (x - div / 2) / div)
        };

        ret.or_else(|| {
            let a = BigInt::from_le_bytes(&self.to_le_bytes());
            let b = BigInt::from_le_bytes(&rhs.to_le_bytes());
            let div = BigInt::from(10).pow(shift_scale);
            if self.is_negative() == rhs.is_negative() {
                Self::from_bigint((a * b + div.clone() / 2) / div)
            } else {
                Self::from_bigint((a * b - div.clone() / 2) / div)
            }
        })
    }

    fn do_round_div(self, rhs: Self, mul_scale: u32) -> Option<Self> {
        let fallback = || {
            let a = BigInt::from_le_bytes(&self.to_le_bytes());
            let b = BigInt::from_le_bytes(&rhs.to_le_bytes());
            let mul = BigInt::from(10).pow(mul_scale);
            if self.is_negative() == rhs.is_negative() {
                Self::from_bigint((a * mul + b.clone() / 2) / b)
            } else {
                Self::from_bigint((a * mul - b.clone() / 2) / b)
            }
        };

        if mul_scale >= MAX_DECIMAL256_PRECISION as _ {
            return fallback();
        }

        let mul = i256::e(mul_scale);
        let ret: Option<i256> = if self.is_negative() == rhs.is_negative() {
            self.checked_mul(mul).map(|x| (x + rhs / 2) / rhs)
        } else {
            self.checked_mul(mul).map(|x| (x - rhs / 2) / rhs)
        };

        ret.or_else(fallback)
    }

    fn min_for_precision(to_precision: u8) -> Self {
        MIN_DECIMAL256_BYTES_FOR_EACH_PRECISION[to_precision as usize - 1]
    }

    fn max_for_precision(to_precision: u8) -> Self {
        MAX_DECIMAL256_BYTES_FOR_EACH_PRECISION[to_precision as usize - 1]
    }

    fn default_decimal_size() -> DecimalSize {
        DecimalSize {
            precision: MAX_DECIMAL256_PRECISION,
            scale: 0,
        }
    }

    fn from_float(value: f64) -> Self {
        value.as_i256()
    }

    fn from_i128<U: Into<i128>>(value: U) -> Self {
        i256::from(value.into())
    }

    fn from_bigint(value: BigInt) -> Option<Self> {
        let mut ret: u256 = u256::ZERO;
        let mut bits = 0;

        for i in value.iter_u64_digits() {
            if bits >= 256 {
                return None;
            }
            ret |= u256::from(i) << bits;
            bits += 64;
        }

        match value.sign() {
            num_bigint::Sign::Plus => i256::try_from(ret).ok(),
            num_bigint::Sign::NoSign => Some(i256::ZERO),
            num_bigint::Sign::Minus => {
                let m: u256 = u256::ONE << 255;
                match ret.cmp(&m) {
                    Ordering::Less => Some(-i256::try_from(ret).unwrap()),
                    Ordering::Equal => Some(i256::MIN),
                    Ordering::Greater => None,
                }
            }
        }
    }

    fn de_binary(bytes: &mut &[u8]) -> Self {
        let bs: [u8; std::mem::size_of::<Self>()] =
            bytes[0..std::mem::size_of::<Self>()].try_into().unwrap();
        *bytes = &bytes[std::mem::size_of::<Self>()..];

        i256::from_le_bytes(bs)
    }

    fn display(self, scale: u8) -> String {
        display_decimal_256(self, scale)
    }

    fn to_float32(self, scale: u8) -> f32 {
        let div = 10_f32.powi(scale as i32);
        self.as_f32() / div
    }

    fn to_float64(self, scale: u8) -> f64 {
        let div = 10_f64.powi(scale as i32);
        self.as_f64() / div
    }

    fn to_int<U: NumCast>(self, scale: u8, rounding_mode: bool) -> Option<U> {
        if !(i128::MIN..=i128::MAX).contains(&self) {
            None
        } else {
            let val = self.as_i128();
            val.to_int(scale, rounding_mode)
        }
    }

    fn to_scalar(self, size: DecimalSize) -> DecimalScalar {
        DecimalScalar::Decimal256(self, size)
    }

    fn try_downcast_column(column: &Column) -> Option<(Buffer<Self>, DecimalSize)> {
        let column = column.as_decimal()?;
        match column {
            DecimalColumn::Decimal256(c, size) => Some((c.clone(), *size)),
            _ => None,
        }
    }

    fn try_downcast_builder(builder: &mut ColumnBuilder) -> Option<&mut Vec<Self>> {
        match builder {
            ColumnBuilder::Decimal(DecimalColumnBuilder::Decimal256(s, _)) => Some(s),
            _ => None,
        }
    }

    fn try_downcast_owned_builder(builder: ColumnBuilder) -> Option<Vec<Self>> {
        match builder {
            ColumnBuilder::Decimal(DecimalColumnBuilder::Decimal256(s, _)) => Some(s),
            _ => None,
        }
    }

    fn try_downcast_scalar<'a>(scalar: &DecimalScalar) -> Option<Self> {
        match scalar {
            DecimalScalar::Decimal256(val, _) => Some(*val),
            _ => None,
        }
    }

    fn try_downcast_domain(domain: &DecimalDomain) -> Option<SimpleDomain<Self>> {
        match domain {
            DecimalDomain::Decimal256(val, _) => Some(*val),
            _ => None,
        }
    }

    fn upcast_scalar(scalar: Self, size: DecimalSize) -> Scalar {
        Scalar::Decimal(DecimalScalar::Decimal256(scalar, size))
    }

    fn upcast_column(col: Buffer<Self>, size: DecimalSize) -> Column {
        Column::Decimal(DecimalColumn::Decimal256(col, size))
    }

    fn upcast_domain(domain: SimpleDomain<Self>, size: DecimalSize) -> Domain {
        Domain::Decimal(DecimalDomain::Decimal256(domain, size))
    }

    fn upcast_builder(builder: Vec<Self>, size: DecimalSize) -> DecimalColumnBuilder {
        DecimalColumnBuilder::Decimal256(builder, size)
    }

    fn data_type() -> DataType {
        DataType::Decimal(DecimalDataType::Decimal256(DecimalSize {
            precision: MAX_DECIMAL256_PRECISION,
            scale: 0,
        }))
    }

    const MIN: i256 = ethnum::int!(
        "-9999999999999999999999999999999999999999999999999999999999999999999999999999"
    );
    const MAX: i256 = ethnum::int!(
        "9999999999999999999999999999999999999999999999999999999999999999999999999999"
    );
    fn to_column_from_buffer(value: Buffer<Self>, size: DecimalSize) -> DecimalColumn {
        DecimalColumn::Decimal256(value, size)
    }
}

pub static MAX_DECIMAL128_PRECISION: u8 = 38;
pub static MAX_DECIMAL256_PRECISION: u8 = 76;

impl DecimalDataType {
    pub fn from_size(size: DecimalSize) -> Result<DecimalDataType> {
        if size.precision < 1 || size.precision > MAX_DECIMAL256_PRECISION {
            return Err(ErrorCode::Overflow(format!(
                "Decimal precision must be between 1 and {}",
                MAX_DECIMAL256_PRECISION
            )));
        }

        if size.scale > size.precision {
            return Err(ErrorCode::Overflow(format!(
                "Decimal scale must be between 0 and precision {}",
                size.precision
            )));
        }
        if size.precision <= MAX_DECIMAL128_PRECISION {
            Ok(DecimalDataType::Decimal128(size))
        } else {
            Ok(DecimalDataType::Decimal256(size))
        }
    }

    pub fn default_scalar(&self) -> DecimalScalar {
        crate::with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalDataType::DECIMAL_TYPE(size) => DecimalScalar::DECIMAL_TYPE(0.into(), *size),
        })
    }

    pub fn size(&self) -> DecimalSize {
        crate::with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalDataType::DECIMAL_TYPE(size) => *size,
        })
    }

    pub fn scale(&self) -> u8 {
        crate::with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalDataType::DECIMAL_TYPE(size) => size.scale,
        })
    }

    pub fn precision(&self) -> u8 {
        crate::with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalDataType::DECIMAL_TYPE(size) => size.precision,
        })
    }

    pub fn leading_digits(&self) -> u8 {
        crate::with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalDataType::DECIMAL_TYPE(size) => size.precision - size.scale,
        })
    }

    pub fn max_precision(&self) -> u8 {
        match self {
            DecimalDataType::Decimal128(_) => MAX_DECIMAL128_PRECISION,
            DecimalDataType::Decimal256(_) => MAX_DECIMAL256_PRECISION,
        }
    }

    pub fn max_result_precision(&self, other: &Self) -> u8 {
        if matches!(other, DecimalDataType::Decimal128(_)) {
            return self.max_precision();
        }
        other.max_precision()
    }

    pub fn belong_diff_precision(precision_a: u8, precision_b: u8) -> bool {
        (precision_a > MAX_DECIMAL128_PRECISION && precision_b <= MAX_DECIMAL128_PRECISION)
            || (precision_a <= MAX_DECIMAL128_PRECISION && precision_b > MAX_DECIMAL128_PRECISION)
    }

    // For div ops, a,b and return must belong to same width types
    pub fn div_common_type(a: &Self, b: &Self, return_size: DecimalSize) -> Result<(Self, Self)> {
        let precision_a = if Self::belong_diff_precision(a.precision(), return_size.precision) {
            return_size.precision
        } else {
            a.precision()
        };

        let precision_b = if Self::belong_diff_precision(b.precision(), return_size.precision) {
            return_size.precision
        } else {
            b.precision()
        };

        let a_type = Self::from_size(DecimalSize {
            precision: precision_a,
            scale: a.scale(),
        })?;
        let b_type = Self::from_size(DecimalSize {
            precision: precision_b,
            scale: b.scale(),
        })?;

        Ok((a_type, b_type))
    }

    // Returns binded types and result type
    pub fn binary_result_type(
        a: &Self,
        b: &Self,
        is_multiply: bool,
        is_divide: bool,
        is_plus_minus: bool,
    ) -> Result<(Self, Self, Self)> {
        let mut scale = a.scale().max(b.scale());
        let mut precision = a.max_result_precision(b);

        // from snowflake: https://docs.snowflake.com/sql-reference/operators-arithmetic
        if is_multiply {
            scale = (a.scale() + b.scale()).min(a.scale().max(b.scale()).max(12));
            let l = a.leading_digits() + b.leading_digits();
            precision = l + scale;
        } else if is_divide {
            scale = a.scale().max((a.scale() + 6).min(12)); // scale must be >= a.sale()
            let l = a.leading_digits() + b.scale(); // l must be >= a.leading_digits()
            precision = l + scale; // so precision must be >= a.precision()
        } else if is_plus_minus {
            scale = std::cmp::max(a.scale(), b.scale());
            // for addition/subtraction, we add 1 to the width to ensure we don't overflow
            let plus_min_precision = a.leading_digits().max(b.leading_digits()) + scale + 1;
            precision = precision.min(plus_min_precision);
        }

        // if the args both are Decimal128, we need to clamp the precision to 38
        if a.precision() <= MAX_DECIMAL128_PRECISION && b.precision() <= MAX_DECIMAL128_PRECISION {
            precision = precision.min(MAX_DECIMAL128_PRECISION);
        } else if precision <= MAX_DECIMAL128_PRECISION
            && Self::belong_diff_precision(a.precision(), b.precision())
        {
            // lift up to decimal256
            precision = MAX_DECIMAL128_PRECISION + 1;
        }
        precision = precision.min(MAX_DECIMAL256_PRECISION);

        let result_type = Self::from_size(DecimalSize { precision, scale })?;

        if is_multiply {
            Ok((
                Self::from_size(DecimalSize {
                    precision,
                    scale: a.scale(),
                })?,
                Self::from_size(DecimalSize {
                    precision,
                    scale: b.scale(),
                })?,
                result_type,
            ))
        } else if is_divide {
            let p = precision.max(a.precision()).max(b.precision());
            Ok((
                Self::from_size(DecimalSize {
                    precision: p,
                    scale: a.scale(),
                })?,
                Self::from_size(DecimalSize {
                    precision: p,
                    scale: b.scale(),
                })?,
                result_type,
            ))
        } else {
            Ok((result_type, result_type, result_type))
        }
    }
}

impl DecimalScalar {
    pub fn domain(&self) -> DecimalDomain {
        crate::with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalScalar::DECIMAL_TYPE(num, size) => DecimalDomain::DECIMAL_TYPE(
                SimpleDomain {
                    min: *num,
                    max: *num,
                },
                *size
            ),
        })
    }
}

impl DecimalColumn {
    pub fn len(&self) -> usize {
        crate::with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalColumn::DECIMAL_TYPE(col, _) => col.len(),
        })
    }

    pub fn index(&self, index: usize) -> Option<DecimalScalar> {
        crate::with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalColumn::DECIMAL_TYPE(col, size) =>
                Some(DecimalScalar::DECIMAL_TYPE(col.get(index).cloned()?, *size)),
        })
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    pub unsafe fn index_unchecked(&self, index: usize) -> DecimalScalar {
        debug_assert!(index < self.len());
        crate::with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalColumn::DECIMAL_TYPE(col, size) =>
                DecimalScalar::DECIMAL_TYPE(*col.get_unchecked(index), *size),
        })
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        assert!(
            range.end <= self.len(),
            "range {:?} out of len {}",
            range,
            self.len()
        );

        crate::with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalColumn::DECIMAL_TYPE(col, size) => {
                DecimalColumn::DECIMAL_TYPE(
                    col.clone().sliced(range.start, range.end - range.start),
                    *size,
                )
            }
        })
    }

    pub fn domain(&self) -> DecimalDomain {
        assert!(self.len() > 0);
        crate::with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalColumn::DECIMAL_TYPE(col, size) => {
                let (min, max) = col.iter().minmax().into_option().unwrap();
                DecimalDomain::DECIMAL_TYPE(
                    SimpleDomain {
                        min: *min,
                        max: *max,
                    },
                    *size,
                )
            }
        })
    }
}

impl DecimalColumnBuilder {
    pub fn from_column(col: DecimalColumn) -> Self {
        crate::with_decimal_type!(|DECIMAL_TYPE| match col {
            DecimalColumn::DECIMAL_TYPE(col, size) =>
                DecimalColumnBuilder::DECIMAL_TYPE(buffer_into_mut(col), size),
        })
    }

    pub fn repeat(scalar: DecimalScalar, n: usize) -> DecimalColumnBuilder {
        crate::with_decimal_type!(|DECIMAL_TYPE| match scalar {
            DecimalScalar::DECIMAL_TYPE(num, size) =>
                DecimalColumnBuilder::DECIMAL_TYPE(vec![num; n], size),
        })
    }

    pub fn repeat_default(ty: &DecimalDataType, n: usize) -> Self {
        crate::with_decimal_type!(|DECIMAL_TYPE| match ty {
            DecimalDataType::DECIMAL_TYPE(size) =>
                DecimalColumnBuilder::DECIMAL_TYPE(vec![0.into(); n], *size),
        })
    }

    pub fn len(&self) -> usize {
        crate::with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalColumnBuilder::DECIMAL_TYPE(col, _) => col.len(),
        })
    }

    pub fn with_capacity(ty: &DecimalDataType, capacity: usize) -> Self {
        crate::with_decimal_type!(|DECIMAL_TYPE| match ty {
            DecimalDataType::DECIMAL_TYPE(size) =>
                DecimalColumnBuilder::DECIMAL_TYPE(Vec::with_capacity(capacity), *size),
        })
    }

    pub fn push(&mut self, item: DecimalScalar) {
        self.push_repeat(item, 1)
    }

    pub fn push_repeat(&mut self, item: DecimalScalar, n: usize) {
        crate::with_decimal_type!(|DECIMAL_TYPE| match (self, item) {
            (
                DecimalColumnBuilder::DECIMAL_TYPE(builder, builder_size),
                DecimalScalar::DECIMAL_TYPE(value, value_size),
            ) => {
                debug_assert_eq!(*builder_size, value_size);
                if n == 1 {
                    builder.push(value)
                } else {
                    builder.resize(builder.len() + n, value)
                }
            }
            (builder, scalar) => unreachable!("unable to push {scalar:?} to {builder:?}"),
        })
    }

    pub fn push_default(&mut self) {
        crate::with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalColumnBuilder::DECIMAL_TYPE(builder, _) => builder.push(0.into()),
        })
    }

    pub fn append_column(&mut self, other: &DecimalColumn) {
        crate::with_decimal_type!(|DECIMAL_TYPE| match (self, other) {
            (
                DecimalColumnBuilder::DECIMAL_TYPE(builder, builder_size),
                DecimalColumn::DECIMAL_TYPE(other, other_size),
            ) => {
                debug_assert_eq!(builder_size, other_size);
                builder.extend_from_slice(other);
            }
            (DecimalColumnBuilder::Decimal128(_, _), DecimalColumn::Decimal256(_, _)) =>
                unreachable!(
                    "unable append column(data type: Decimal256) into builder(data type: Decimal128)"
                ),
            (DecimalColumnBuilder::Decimal256(_, _), DecimalColumn::Decimal128(_, _)) =>
                unreachable!(
                    "unable append column(data type: Decimal128) into builder(data type: Decimal256)"
                ),
        })
    }

    pub fn build(self) -> DecimalColumn {
        crate::with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalColumnBuilder::DECIMAL_TYPE(builder, size) =>
                DecimalColumn::DECIMAL_TYPE(builder.into(), size),
        })
    }

    pub fn build_scalar(self) -> DecimalScalar {
        assert_eq!(self.len(), 1);

        crate::with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalColumnBuilder::DECIMAL_TYPE(builder, size) =>
                DecimalScalar::DECIMAL_TYPE(builder[0], size),
        })
    }

    pub fn pop(&mut self) -> Option<DecimalScalar> {
        crate::with_decimal_type!(|DECIMAL_TYPE| match self {
            DecimalColumnBuilder::DECIMAL_TYPE(builder, size) => {
                builder
                    .pop()
                    .map(|num| DecimalScalar::DECIMAL_TYPE(num, *size))
            }
        })
    }

    pub fn decimal_size(&self) -> DecimalSize {
        match self {
            DecimalColumnBuilder::Decimal128(_, size)
            | DecimalColumnBuilder::Decimal256(_, size) => *size,
        }
    }
}

impl PartialOrd for DecimalScalar {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        crate::with_decimal_type!(|DECIMAL_TYPE| match (self, other) {
            (
                DecimalScalar::DECIMAL_TYPE(lhs, lhs_size),
                DecimalScalar::DECIMAL_TYPE(rhs, rhs_size),
            ) => {
                if lhs_size == rhs_size {
                    lhs.partial_cmp(rhs)
                } else {
                    None
                }
            }
            _ => None,
        })
    }
}

impl PartialOrd for DecimalColumn {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        crate::with_decimal_type!(|DECIMAL_TYPE| match (self, other) {
            (
                DecimalColumn::DECIMAL_TYPE(lhs, lhs_size),
                DecimalColumn::DECIMAL_TYPE(rhs, rhs_size),
            ) => {
                if lhs_size == rhs_size {
                    lhs.iter().partial_cmp(rhs.iter())
                } else {
                    None
                }
            }
            _ => None,
        })
    }
}

#[macro_export]
macro_rules! with_decimal_type {
    ( | $t:tt | $($tail:tt)* ) => {
        match_template::match_template! {
            $t = [Decimal128, Decimal256],
            $($tail)*
        }
    }
}

#[macro_export]
macro_rules! with_decimal_mapped_type {
    (| $t:tt | $($tail:tt)*) => {
        match_template::match_template! {
            $t = [
                Decimal128 => i128, Decimal256 => i256
            ],
            $($tail)*
        }
    }
}
// MAX decimal256 value of little-endian format for each precision.
// Each element is the max value of signed 256-bit integer for the specified precision which
// is encoded to the 32-byte width format of little-endian.
pub(crate) const MAX_DECIMAL256_BYTES_FOR_EACH_PRECISION: [i256; 76] = [
    i256::from_le_bytes([
        9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0,
    ]),
    i256::from_le_bytes([
        99, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0,
    ]),
    i256::from_le_bytes([
        231, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0,
    ]),
    i256::from_le_bytes([
        15, 39, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0,
    ]),
    i256::from_le_bytes([
        159, 134, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0,
    ]),
    i256::from_le_bytes([
        63, 66, 15, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0,
    ]),
    i256::from_le_bytes([
        127, 150, 152, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 224, 245, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 201, 154, 59, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 227, 11, 84, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 231, 118, 72, 23, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 15, 165, 212, 232, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 159, 114, 78, 24, 9, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 63, 122, 16, 243, 90, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 127, 198, 164, 126, 141, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 192, 111, 242, 134, 35, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 137, 93, 120, 69, 99, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 99, 167, 179, 182, 224, 13, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 231, 137, 4, 35, 199, 138, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 15, 99, 45, 94, 199, 107, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 159, 222, 197, 173, 201, 53, 54, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 63, 178, 186, 201, 224, 25, 30, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 127, 246, 74, 225, 199, 2, 45, 21, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 160, 237, 204, 206, 27, 194, 211, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 73, 72, 1, 20, 22, 149, 69, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 227, 210, 12, 200, 220, 210, 183, 82, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 231, 60, 128, 208, 159, 60, 46, 59, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 15, 97, 2, 37, 62, 94, 206, 79, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 159, 202, 23, 114, 109, 174, 15, 30, 67, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 63, 234, 237, 116, 70, 208, 156, 44, 159, 12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 127, 38, 75, 145, 192, 34, 32, 190, 55, 126, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 128, 239, 172, 133, 91, 65, 109, 45, 238, 4, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 9, 91, 193, 56, 147, 141, 68, 198, 77, 49, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 99, 142, 141, 55, 192, 135, 173, 190, 9, 237, 1, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 231, 143, 135, 43, 130, 77, 199, 114, 97, 66, 19, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 15, 159, 75, 179, 21, 7, 201, 123, 206, 151, 192, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 159, 54, 244, 0, 217, 70, 218, 213, 16, 238, 133, 7, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 63, 34, 138, 9, 122, 196, 134, 90, 168, 76, 59, 75, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 127, 86, 101, 95, 196, 172, 67, 137, 147, 254, 80, 240, 2, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 96, 245, 185, 171, 191, 164, 92, 195, 241, 41, 99, 29, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 201, 149, 67, 181, 124, 111, 158, 161, 113, 163, 223, 37, 1, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 227, 217, 163, 20, 223, 90, 48, 80, 112, 98, 188, 122, 11, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 231, 130, 102, 206, 182, 140, 227, 33, 99, 216, 91, 203, 114, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 15, 29, 1, 16, 36, 127, 227, 82, 223, 115, 150, 241, 123, 4, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 159, 34, 11, 160, 104, 247, 226, 60, 185, 134, 224, 111, 215, 44,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 63, 90, 111, 64, 22, 170, 221, 96, 60, 67, 197, 94, 106, 192, 1,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 127, 134, 89, 132, 222, 164, 168, 200, 91, 160, 180, 179, 39, 132,
        17, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 64, 127, 43, 177, 112, 150, 214, 149, 67, 14, 5, 141, 41,
        175, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 137, 248, 178, 235, 102, 224, 97, 218, 163, 142, 50, 130,
        159, 215, 6, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 99, 181, 253, 52, 5, 196, 210, 135, 102, 146, 249, 21, 59,
        108, 68, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 231, 21, 233, 17, 52, 168, 59, 78, 1, 184, 191, 219, 78, 58,
        172, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 15, 219, 26, 179, 8, 146, 84, 14, 13, 48, 125, 149, 20, 71,
        186, 26, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 159, 142, 12, 255, 86, 180, 77, 143, 130, 224, 227, 214, 205,
        198, 70, 11, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 63, 146, 125, 246, 101, 11, 9, 153, 25, 197, 230, 100, 10,
        196, 195, 112, 10, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 127, 182, 231, 160, 251, 113, 90, 250, 255, 178, 3, 241, 103,
        168, 165, 103, 104, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 32, 13, 73, 212, 115, 136, 199, 255, 253, 36, 106, 15,
        148, 120, 12, 20, 4, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 73, 131, 218, 74, 134, 84, 203, 253, 235, 113, 37, 154,
        200, 181, 124, 200, 40, 0, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 227, 32, 137, 236, 62, 77, 241, 233, 55, 115, 118, 5,
        214, 25, 223, 212, 151, 1, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 231, 72, 91, 61, 117, 4, 109, 35, 47, 128, 160, 54, 92,
        2, 183, 80, 238, 15, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 15, 217, 144, 101, 148, 44, 66, 98, 215, 1, 69, 34, 154,
        23, 38, 39, 79, 159, 0, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 159, 122, 168, 247, 203, 189, 149, 214, 105, 18, 178,
        86, 5, 236, 124, 135, 23, 57, 6, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 63, 202, 148, 172, 247, 105, 217, 97, 34, 184, 244, 98,
        53, 56, 225, 74, 235, 58, 62, 0, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 127, 230, 207, 189, 172, 35, 126, 210, 87, 49, 143, 221,
        21, 50, 204, 236, 48, 77, 110, 2, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 255, 0, 31, 106, 191, 100, 237, 56, 110, 237, 151, 167,
        218, 244, 249, 63, 233, 3, 79, 24, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 255, 9, 54, 37, 122, 239, 69, 57, 78, 70, 239, 139, 138,
        144, 195, 127, 28, 39, 22, 243, 0, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 255, 99, 28, 116, 197, 90, 187, 60, 14, 191, 88, 119,
        105, 165, 163, 253, 28, 135, 221, 126, 9, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 255, 231, 27, 137, 182, 139, 81, 95, 142, 118, 119, 169,
        30, 118, 100, 232, 33, 71, 167, 244, 94, 0, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 255, 15, 23, 91, 33, 117, 47, 185, 143, 161, 170, 158,
        50, 157, 236, 19, 83, 199, 136, 142, 181, 3, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 255, 159, 230, 142, 77, 147, 218, 59, 157, 79, 170, 50,
        250, 35, 62, 199, 62, 201, 87, 145, 23, 37, 0, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 255, 63, 2, 149, 7, 193, 137, 86, 36, 28, 167, 250, 197,
        103, 109, 200, 115, 220, 109, 173, 235, 114, 1, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 255, 127, 22, 210, 75, 138, 97, 97, 107, 25, 135, 202,
        187, 13, 70, 212, 133, 156, 74, 198, 52, 125, 14, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 255, 255, 224, 52, 246, 102, 207, 205, 49, 254, 70, 233,
        85, 137, 188, 74, 58, 29, 234, 190, 15, 228, 144, 0, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 255, 255, 201, 16, 158, 5, 26, 10, 242, 237, 197, 28,
        91, 93, 93, 235, 70, 36, 37, 117, 157, 232, 168, 5, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 255, 255, 227, 167, 44, 56, 4, 101, 116, 75, 187, 31,
        143, 165, 165, 49, 197, 106, 115, 147, 38, 22, 153, 56, 0,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 255, 255, 231, 142, 190, 49, 42, 242, 139, 242, 80, 61,
        151, 119, 120, 240, 179, 43, 130, 194, 129, 221, 250, 53, 2,
    ]),
    i256::from_le_bytes([
        255, 255, 255, 255, 255, 255, 255, 255, 255, 15, 149, 113, 241, 165, 117, 119, 121, 41,
        101, 232, 171, 180, 100, 7, 181, 21, 153, 17, 167, 204, 27, 22,
    ]),
];

// MIN decimal256 value of little-endian format for each precision.
// Each element is the min value of signed 256-bit integer for the specified precision which
// is encoded to the 76-byte width format of little-endian.
pub(crate) const MIN_DECIMAL256_BYTES_FOR_EACH_PRECISION: [i256; 76] = [
    i256::from_le_bytes([
        247, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        157, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        25, 252, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        241, 216, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        97, 121, 254, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        193, 189, 240, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        129, 105, 103, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 31, 10, 250, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 54, 101, 196, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 28, 244, 171, 253, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 24, 137, 183, 232, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 240, 90, 43, 23, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 96, 141, 177, 231, 246, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 192, 133, 239, 12, 165, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 128, 57, 91, 129, 114, 252, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 63, 144, 13, 121, 220, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 118, 162, 135, 186, 156, 254, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 156, 88, 76, 73, 31, 242, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 24, 118, 251, 220, 56, 117, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 240, 156, 210, 161, 56, 148, 250, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 96, 33, 58, 82, 54, 202, 201, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 192, 77, 69, 54, 31, 230, 225, 253, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 128, 9, 181, 30, 56, 253, 210, 234, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 95, 18, 51, 49, 228, 61, 44, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 182, 183, 254, 235, 233, 106, 186, 247, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 28, 45, 243, 55, 35, 45, 72, 173, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 24, 195, 127, 47, 96, 195, 209, 196, 252, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 240, 158, 253, 218, 193, 161, 49, 176, 223, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 96, 53, 232, 141, 146, 81, 240, 225, 188, 254, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 192, 21, 18, 139, 185, 47, 99, 211, 96, 243, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 128, 217, 180, 110, 63, 221, 223, 65, 200, 129, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 127, 16, 83, 122, 164, 190, 146, 210, 17, 251, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 246, 164, 62, 199, 108, 114, 187, 57, 178, 206, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 156, 113, 114, 200, 63, 120, 82, 65, 246, 18, 254, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 24, 112, 120, 212, 125, 178, 56, 141, 158, 189, 236, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 240, 96, 180, 76, 234, 248, 54, 132, 49, 104, 63, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 96, 201, 11, 255, 38, 185, 37, 42, 239, 17, 122, 248, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 192, 221, 117, 246, 133, 59, 121, 165, 87, 179, 196, 180, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 128, 169, 154, 160, 59, 83, 188, 118, 108, 1, 175, 15, 253, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 159, 10, 70, 84, 64, 91, 163, 60, 14, 214, 156, 226, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 54, 106, 188, 74, 131, 144, 97, 94, 142, 92, 32, 218, 254, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 28, 38, 92, 235, 32, 165, 207, 175, 143, 157, 67, 133, 244, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 24, 125, 153, 49, 73, 115, 28, 222, 156, 39, 164, 52, 141, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 240, 226, 254, 239, 219, 128, 28, 173, 32, 140, 105, 14, 132, 251, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 96, 221, 244, 95, 151, 8, 29, 195, 70, 121, 31, 144, 40, 211, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 192, 165, 144, 191, 233, 85, 34, 159, 195, 188, 58, 161, 149, 63, 254, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 128, 121, 166, 123, 33, 91, 87, 55, 164, 95, 75, 76, 216, 123, 238, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 191, 128, 212, 78, 143, 105, 41, 106, 188, 241, 250, 114, 214, 80, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 118, 7, 77, 20, 153, 31, 158, 37, 92, 113, 205, 125, 96, 40, 249, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 156, 74, 2, 203, 250, 59, 45, 120, 153, 109, 6, 234, 196, 147, 187, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 24, 234, 22, 238, 203, 87, 196, 177, 254, 71, 64, 36, 177, 197, 83, 253,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 240, 36, 229, 76, 247, 109, 171, 241, 242, 207, 130, 106, 235, 184, 69,
        229, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 96, 113, 243, 0, 169, 75, 178, 112, 125, 31, 28, 41, 50, 57, 185, 244,
        254, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 192, 109, 130, 9, 154, 244, 246, 102, 230, 58, 25, 155, 245, 59, 60, 143,
        245, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 128, 73, 24, 95, 4, 142, 165, 5, 0, 77, 252, 14, 152, 87, 90, 152, 151,
        255, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 223, 242, 182, 43, 140, 119, 56, 0, 2, 219, 149, 240, 107, 135, 243,
        235, 251, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 182, 124, 37, 181, 121, 171, 52, 2, 20, 142, 218, 101, 55, 74, 131,
        55, 215, 255, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 28, 223, 118, 19, 193, 178, 14, 22, 200, 140, 137, 250, 41, 230, 32,
        43, 104, 254, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 24, 183, 164, 194, 138, 251, 146, 220, 208, 127, 95, 201, 163, 253,
        72, 175, 17, 240, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 240, 38, 111, 154, 107, 211, 189, 157, 40, 254, 186, 221, 101, 232,
        217, 216, 176, 96, 255, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 96, 133, 87, 8, 52, 66, 106, 41, 150, 237, 77, 169, 250, 19, 131, 120,
        232, 198, 249, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 192, 53, 107, 83, 8, 150, 38, 158, 221, 71, 11, 157, 202, 199, 30,
        181, 20, 197, 193, 255, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 128, 25, 48, 66, 83, 220, 129, 45, 168, 206, 112, 34, 234, 205, 51,
        19, 207, 178, 145, 253, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 0, 255, 224, 149, 64, 155, 18, 199, 145, 18, 104, 88, 37, 11, 6, 192,
        22, 252, 176, 231, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 0, 246, 201, 218, 133, 16, 186, 198, 177, 185, 16, 116, 117, 111, 60,
        128, 227, 216, 233, 12, 255, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 0, 156, 227, 139, 58, 165, 68, 195, 241, 64, 167, 136, 150, 90, 92, 2,
        227, 120, 34, 129, 246, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 0, 24, 228, 118, 73, 116, 174, 160, 113, 137, 136, 86, 225, 137, 155,
        23, 222, 184, 88, 11, 161, 255, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 0, 240, 232, 164, 222, 138, 208, 70, 112, 94, 85, 97, 205, 98, 19,
        236, 172, 56, 119, 113, 74, 252, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 0, 96, 25, 113, 178, 108, 37, 196, 98, 176, 85, 205, 5, 220, 193, 56,
        193, 54, 168, 110, 232, 218, 255, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 0, 192, 253, 106, 248, 62, 118, 169, 219, 227, 88, 5, 58, 152, 146,
        55, 140, 35, 146, 82, 20, 141, 254, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 0, 128, 233, 45, 180, 117, 158, 158, 148, 230, 120, 53, 68, 242, 185,
        43, 122, 99, 181, 57, 203, 130, 241, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 0, 0, 31, 203, 9, 153, 48, 50, 206, 1, 185, 22, 170, 118, 67, 181,
        197, 226, 21, 65, 240, 27, 111, 255, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 0, 0, 54, 239, 97, 250, 229, 245, 13, 18, 58, 227, 164, 162, 162, 20,
        185, 219, 218, 138, 98, 23, 87, 250, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 0, 0, 28, 88, 211, 199, 251, 154, 139, 180, 68, 224, 112, 90, 90, 206,
        58, 149, 140, 108, 217, 233, 102, 199, 255,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 0, 0, 24, 113, 65, 206, 213, 13, 116, 13, 175, 194, 104, 136, 135, 15,
        76, 212, 125, 61, 126, 34, 5, 202, 253,
    ]),
    i256::from_le_bytes([
        1, 0, 0, 0, 0, 0, 0, 0, 0, 240, 106, 142, 14, 90, 138, 136, 134, 214, 154, 23, 84, 75, 155,
        248, 74, 234, 102, 238, 88, 51, 228, 233,
    ]),
];

/// Codes from arrow-rs: https://github.com/apache/arrow-rs/blob/9728c676b50b19c06643a23daba4aa4a1dc48055/arrow-data/src/decimal.rs
/// `MAX_DECIMAL_FOR_EACH_PRECISION[p]` holds the maximum `i128` value that can
/// be stored in [arrow_schema::DataType::Decimal128] value of precision `p`
pub const MAX_DECIMAL_FOR_EACH_PRECISION: [i128; 38] = [
    9,
    99,
    999,
    9999,
    99999,
    999999,
    9999999,
    99999999,
    999999999,
    9999999999,
    99999999999,
    999999999999,
    9999999999999,
    99999999999999,
    999999999999999,
    9999999999999999,
    99999999999999999,
    999999999999999999,
    9999999999999999999,
    99999999999999999999,
    999999999999999999999,
    9999999999999999999999,
    99999999999999999999999,
    999999999999999999999999,
    9999999999999999999999999,
    99999999999999999999999999,
    999999999999999999999999999,
    9999999999999999999999999999,
    99999999999999999999999999999,
    999999999999999999999999999999,
    9999999999999999999999999999999,
    99999999999999999999999999999999,
    999999999999999999999999999999999,
    9999999999999999999999999999999999,
    99999999999999999999999999999999999,
    999999999999999999999999999999999999,
    9999999999999999999999999999999999999,
    99999999999999999999999999999999999999,
];

/// `MIN_DECIMAL_FOR_EACH_PRECISION[p]` holds the minimum `i128` value that can
/// be stored in a [arrow_schema::DataType::Decimal128] value of precision `p`
pub const MIN_DECIMAL_FOR_EACH_PRECISION: [i128; 38] = [
    -9,
    -99,
    -999,
    -9999,
    -99999,
    -999999,
    -9999999,
    -99999999,
    -999999999,
    -9999999999,
    -99999999999,
    -999999999999,
    -9999999999999,
    -99999999999999,
    -999999999999999,
    -9999999999999999,
    -99999999999999999,
    -999999999999999999,
    -9999999999999999999,
    -99999999999999999999,
    -999999999999999999999,
    -9999999999999999999999,
    -99999999999999999999999,
    -999999999999999999999999,
    -9999999999999999999999999,
    -99999999999999999999999999,
    -999999999999999999999999999,
    -9999999999999999999999999999,
    -99999999999999999999999999999,
    -999999999999999999999999999999,
    -9999999999999999999999999999999,
    -99999999999999999999999999999999,
    -999999999999999999999999999999999,
    -9999999999999999999999999999999999,
    -99999999999999999999999999999999999,
    -999999999999999999999999999999999999,
    -9999999999999999999999999999999999999,
    -99999999999999999999999999999999999999,
];
