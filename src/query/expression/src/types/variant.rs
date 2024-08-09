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

use core::cmp::Ordering;
use std::ops::Range;

use databend_common_geobuf::GeoJson;
use databend_common_io::deserialize_bitmap;
use geozero::wkb::Ewkb;
use geozero::ToJson;

use super::binary::BinaryColumn;
use super::binary::BinaryColumnBuilder;
use super::binary::BinaryIterator;
use super::date::date_to_string;
use super::number::NumberScalar;
use super::timestamp::timestamp_to_string;
use crate::date_helper::TzLUT;
use crate::property::Domain;
use crate::types::map::KvPair;
use crate::types::AnyType;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::DecimalSize;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::values::Column;
use crate::values::Scalar;
use crate::values::ScalarRef;
use crate::ColumnBuilder;

/// JSONB bytes representation of `null`.
pub const JSONB_NULL: &[u8] = &[0x20, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VariantType;

impl ValueType for VariantType {
    type Scalar = Vec<u8>;
    type ScalarRef<'a> = &'a [u8];
    type Column = BinaryColumn;
    type Domain = ();
    type ColumnIterator<'a> = BinaryIterator<'a>;
    type ColumnBuilder = BinaryColumnBuilder;

    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: &'long [u8]) -> &'short [u8] {
        long
    }

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        scalar.to_vec()
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        scalar
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        scalar.as_variant().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Self::Column> {
        col.as_variant().cloned()
    }

    fn try_downcast_domain(domain: &Domain) -> Option<Self::Domain> {
        if domain.is_undefined() {
            Some(())
        } else {
            None
        }
    }

    fn try_downcast_builder(builder: &mut ColumnBuilder) -> Option<&mut Self::ColumnBuilder> {
        match builder {
            ColumnBuilder::Variant(builder) => Some(builder),
            _ => None,
        }
    }

    fn try_downcast_owned_builder(builder: ColumnBuilder) -> Option<Self::ColumnBuilder> {
        match builder {
            ColumnBuilder::Variant(builder) => Some(builder),
            _ => None,
        }
    }

    fn try_upcast_column_builder(
        builder: Self::ColumnBuilder,
        _decimal_size: Option<DecimalSize>,
    ) -> Option<ColumnBuilder> {
        Some(ColumnBuilder::Variant(builder))
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        Scalar::Variant(scalar)
    }

    fn upcast_column(col: Self::Column) -> Column {
        Column::Variant(col)
    }

    fn upcast_domain(_domain: Self::Domain) -> Domain {
        Domain::Undefined
    }

    fn column_len(col: &Self::Column) -> usize {
        col.len()
    }

    fn index_column(col: &Self::Column, index: usize) -> Option<Self::ScalarRef<'_>> {
        col.index(index)
    }

    #[inline(always)]
    unsafe fn index_column_unchecked(col: &Self::Column, index: usize) -> Self::ScalarRef<'_> {
        col.index_unchecked(index)
    }

    fn slice_column(col: &Self::Column, range: Range<usize>) -> Self::Column {
        col.slice(range)
    }

    fn iter_column(col: &Self::Column) -> Self::ColumnIterator<'_> {
        col.iter()
    }

    fn column_to_builder(col: Self::Column) -> Self::ColumnBuilder {
        BinaryColumnBuilder::from_column(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>) {
        builder.put_slice(item);
        builder.commit_row();
    }

    fn push_item_repeat(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>, n: usize) {
        builder.push_repeat(item, n);
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        builder.commit_row();
    }

    fn append_column(builder: &mut Self::ColumnBuilder, other_builder: &Self::Column) {
        builder.append_column(other_builder)
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        builder.build()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        builder.build_scalar()
    }

    fn scalar_memory_size(scalar: &Self::ScalarRef<'_>) -> usize {
        scalar.len()
    }

    fn column_memory_size(col: &Self::Column) -> usize {
        col.data().len() + col.offsets().len() * 8
    }

    #[inline(always)]
    fn compare(lhs: Self::ScalarRef<'_>, rhs: Self::ScalarRef<'_>) -> Option<Ordering> {
        Some(jsonb::compare(lhs, rhs).expect("unable to parse jsonb value"))
    }
}

impl ArgType for VariantType {
    fn data_type() -> DataType {
        DataType::Variant
    }

    fn full_domain() -> Self::Domain {}

    fn create_builder(capacity: usize, _: &GenericMap) -> Self::ColumnBuilder {
        BinaryColumnBuilder::with_capacity(capacity, 0)
    }
}

pub fn cast_scalar_to_variant(scalar: ScalarRef, tz: TzLUT, buf: &mut Vec<u8>) {
    let inner_tz = tz.tz;
    let value = match scalar {
        ScalarRef::Null => jsonb::Value::Null,
        ScalarRef::EmptyArray => jsonb::Value::Array(vec![]),
        ScalarRef::EmptyMap => jsonb::Value::Object(jsonb::Object::new()),
        ScalarRef::Number(n) => match n {
            NumberScalar::UInt8(n) => n.into(),
            NumberScalar::UInt16(n) => n.into(),
            NumberScalar::UInt32(n) => n.into(),
            NumberScalar::UInt64(n) => n.into(),
            NumberScalar::Int8(n) => n.into(),
            NumberScalar::Int16(n) => n.into(),
            NumberScalar::Int32(n) => n.into(),
            NumberScalar::Int64(n) => n.into(),
            NumberScalar::Float32(n) => n.0.into(),
            NumberScalar::Float64(n) => n.0.into(),
        },
        ScalarRef::Decimal(x) => x.to_float64().into(),
        ScalarRef::Boolean(b) => jsonb::Value::Bool(b),
        ScalarRef::Binary(s) => jsonb::Value::String(hex::encode_upper(s).into()),
        ScalarRef::String(s) => jsonb::Value::String(s.into()),
        ScalarRef::Timestamp(ts) => timestamp_to_string(ts, inner_tz).to_string().into(),
        ScalarRef::Date(d) => date_to_string(d, inner_tz).to_string().into(),
        ScalarRef::Array(col) => {
            let items = cast_scalars_to_variants(col.iter(), tz);
            jsonb::build_array(items.iter(), buf).expect("failed to build jsonb array");
            return;
        }
        ScalarRef::Map(col) => {
            let kv_col = KvPair::<AnyType, AnyType>::try_downcast_column(&col).unwrap();
            let kvs = kv_col
                .iter()
                .map(|(k, v)| {
                    let key = match k {
                        ScalarRef::String(v) => v.to_string(),
                        ScalarRef::Number(v) => v.to_string(),
                        ScalarRef::Decimal(v) => v.to_string(),
                        ScalarRef::Boolean(v) => v.to_string(),
                        ScalarRef::Timestamp(v) => timestamp_to_string(v, inner_tz).to_string(),
                        ScalarRef::Date(v) => date_to_string(v, inner_tz).to_string(),
                        _ => unreachable!(),
                    };
                    let mut val = vec![];
                    cast_scalar_to_variant(v, tz, &mut val);
                    (key, val)
                })
                .collect::<Vec<_>>();
            jsonb::build_object(kvs.iter().map(|(k, v)| (k, &v[..])), buf)
                .expect("failed to build jsonb object from map");
            return;
        }
        ScalarRef::Bitmap(b) => {
            jsonb::Value::Array(
                deserialize_bitmap(b)
                    .unwrap()
                    .iter()
                    .map(|x| x.into())
                    .collect(),
            )
            .write_to_vec(buf);
            return;
        }
        ScalarRef::Tuple(fields) => {
            let values = cast_scalars_to_variants(fields, tz);
            jsonb::build_object(
                values
                    .iter()
                    .enumerate()
                    .map(|(i, bytes)| (format!("{}", i + 1), bytes)),
                buf,
            )
            .expect("failed to build jsonb object from tuple");
            return;
        }
        ScalarRef::Variant(bytes) => {
            buf.extend_from_slice(bytes);
            return;
        }
        ScalarRef::Geometry(bytes) => {
            let geom = Ewkb(bytes.to_vec())
                .to_json()
                .expect("failed to decode wkb data");
            jsonb::parse_value(geom.as_bytes())
                .expect("failed to parse geojson to json value")
                .write_to_vec(buf);
            return;
        }
        ScalarRef::Geography(geog) => {
            // todo: Implement direct conversion, omitting intermediate processes
            let GeoJson(str) = geog.0.try_into().expect("failed to encode geojson");
            jsonb::parse_value(str.as_bytes())
                .expect("failed to parse geojson to json value")
                .write_to_vec(buf);
            return;
        }
    };
    value.write_to_vec(buf);
}

pub fn cast_scalars_to_variants(
    scalars: impl IntoIterator<Item = ScalarRef>,
    tz: TzLUT,
) -> BinaryColumn {
    let iter = scalars.into_iter();
    let mut builder = BinaryColumnBuilder::with_capacity(iter.size_hint().0, 0);
    for scalar in iter {
        cast_scalar_to_variant(scalar, tz, &mut builder.data);
        builder.commit_row();
    }
    builder.build()
}
