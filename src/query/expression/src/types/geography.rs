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

use std::fmt::Debug;
use std::hash::Hash;
use std::io;
use std::ops::Range;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_arrow::arrow::trusted_len::TrustedLen;
use databend_common_geobuf::Geometry;
use databend_common_geobuf::GeometryRef;
use serde::Deserialize;
use serde::Serialize;

use crate::property::Domain;
use crate::types::binary::BinaryColumn;
use crate::types::binary::BinaryColumnBuilder;
use crate::types::binary::BinaryIterator;
use crate::types::ArgType;
use crate::types::DataType;
use crate::types::DecimalSize;
use crate::types::GenericMap;
use crate::types::ValueType;
use crate::values::Column;
use crate::values::Scalar;
use crate::values::ScalarRef;
use crate::ColumnBuilder;

pub const LATITUDE_MIN: f64 = -90.0;
pub const LATITUDE_MAX: f64 = 90.0;
pub const LONGITUDE_MIN: f64 = -180.0;
pub const LONGITUDE_MAX: f64 = 180.0;

#[derive(Clone, Default, Debug, PartialOrd)]
pub struct Geography(pub Geometry);

impl Geography {
    pub fn check(&self) -> Result<(), String> {
        let r = self.0.as_ref();
        if !r
            .x()
            .iter()
            .all(|longitude| (LONGITUDE_MIN..=LONGITUDE_MAX).contains(longitude))
        {
            return Err("longitude is out of range".to_string());
        }
        if !r
            .y()
            .iter()
            .all(|latitude| (LATITUDE_MIN..=LATITUDE_MAX).contains(latitude))
        {
            return Err("latitude is out of range".to_string());
        }
        Ok(())
    }
}

impl Serialize for Geography {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        Serialize::serialize(&self.0.as_ref(), serializer)
    }
}

impl<'de> Deserialize<'de> for Geography {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        Ok(Geography(Deserialize::deserialize(deserializer)?))
    }
}

impl BorshSerialize for Geography {
    fn serialize<W: io::prelude::Write>(&self, writer: &mut W) -> io::Result<()> {
        BorshSerialize::serialize(&self.0.as_ref(), writer)
    }
}

impl BorshDeserialize for Geography {
    fn deserialize_reader<R: io::prelude::Read>(reader: &mut R) -> io::Result<Self> {
        Ok(Geography(Geometry::deserialize_reader(reader)?))
    }
}

impl PartialEq for Geography {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl Eq for Geography {}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd)]
pub struct GeographyRef<'a>(pub GeometryRef<'a>);

impl<'a> GeographyRef<'a> {
    pub fn to_owned(&self) -> Geography {
        Geography(self.0.to_owned())
    }
}

impl<'a> Hash for GeographyRef<'a> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.hash(state)
    }
}

impl Geography {
    pub fn as_ref(&self) -> GeographyRef<'_> {
        GeographyRef(self.0.as_ref())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GeographyType;

impl ValueType for GeographyType {
    type Scalar = Geography;
    type ScalarRef<'a> = GeographyRef<'a>;
    type Column = GeographyColumn;
    type Domain = ();
    type ColumnIterator<'a> = GeographyIterator<'a>;
    type ColumnBuilder = GeographyColumnBuilder;

    #[inline]
    fn upcast_gat<'short, 'long: 'short>(long: GeographyRef<'long>) -> GeographyRef<'short> {
        long
    }

    fn to_owned_scalar(scalar: Self::ScalarRef<'_>) -> Self::Scalar {
        scalar.to_owned()
    }

    fn to_scalar_ref(scalar: &Self::Scalar) -> Self::ScalarRef<'_> {
        scalar.as_ref()
    }

    fn try_downcast_scalar<'a>(scalar: &'a ScalarRef) -> Option<Self::ScalarRef<'a>> {
        scalar.as_geography().cloned()
    }

    fn try_downcast_column(col: &Column) -> Option<Self::Column> {
        col.as_geography().cloned()
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
            ColumnBuilder::Geography(builder) => Some(builder),
            _ => None,
        }
    }

    fn try_downcast_owned_builder(builder: ColumnBuilder) -> Option<Self::ColumnBuilder> {
        match builder {
            ColumnBuilder::Geography(builder) => Some(builder),
            _ => None,
        }
    }

    fn try_upcast_column_builder(
        builder: Self::ColumnBuilder,
        _decimal_size: Option<DecimalSize>,
    ) -> Option<ColumnBuilder> {
        Some(ColumnBuilder::Geography(builder))
    }

    fn upcast_scalar(scalar: Self::Scalar) -> Scalar {
        Scalar::Geography(scalar)
    }

    fn upcast_column(col: Self::Column) -> Column {
        Column::Geography(col)
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
        GeographyColumnBuilder::from_column(col)
    }

    fn builder_len(builder: &Self::ColumnBuilder) -> usize {
        builder.len()
    }

    fn push_item(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>) {
        builder.push(item)
    }

    fn push_item_repeat(builder: &mut Self::ColumnBuilder, item: Self::ScalarRef<'_>, n: usize) {
        builder.push_repeat(item, n)
    }

    fn push_default(builder: &mut Self::ColumnBuilder) {
        builder.push_default()
    }

    fn append_column(builder: &mut Self::ColumnBuilder, other: &Self::Column) {
        builder.append_column(other)
    }

    fn build_column(builder: Self::ColumnBuilder) -> Self::Column {
        builder.build()
    }

    fn build_scalar(builder: Self::ColumnBuilder) -> Self::Scalar {
        builder.build_scalar()
    }

    fn scalar_memory_size(scalar: &Self::ScalarRef<'_>) -> usize {
        scalar.0.memory_size()
    }

    fn column_memory_size(col: &Self::Column) -> usize {
        col.memory_size()
    }
}

impl ArgType for GeographyType {
    fn data_type() -> DataType {
        DataType::Geography
    }

    fn full_domain() -> Self::Domain {}

    fn create_builder(capacity: usize, _: &GenericMap) -> Self::ColumnBuilder {
        GeographyColumnBuilder::with_capacity(capacity, 0)
    }
}

impl GeographyType {
    pub fn point(x: f64, y: f64) -> Geography {
        Geography(Geometry::point(x, y))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct GeographyColumn {
    pub(crate) buf: BinaryColumn,
    pub(crate) offsets: Buffer<u64>,
    pub(crate) x: Buffer<f64>,
    pub(crate) y: Buffer<f64>,
}

impl GeographyColumn {
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    pub fn memory_size(&self) -> usize {
        let offsets = self.offsets.as_slice();
        let len = offsets.len();
        self.buf.memory_size() + len * 8 + (offsets[len - 1] - offsets[0]) as usize * 16
    }

    pub fn index(&self, index: usize) -> Option<GeographyRef> {
        if index + 1 < self.offsets.len() {
            let buf = self.buf.index(index).unwrap();
            let start = self.offsets[index] as usize;
            let end = self.offsets[index + 1] as usize;
            let x = &self.x[start..end];
            let y = &self.y[start..end];
            Some(GeographyRef(GeometryRef::new(buf, x, y)))
        } else {
            None
        }
    }

    /// # Safety
    ///
    /// Calling this method with an out-of-bounds index is *[undefined behavior]*
    #[inline]
    pub unsafe fn index_unchecked(&self, index: usize) -> GeographyRef<'_> {
        let buf = self.buf.index_unchecked(index);
        let start = *self.offsets.get_unchecked(index) as usize;
        let end = *self.offsets.get_unchecked(index + 1) as usize;
        let x = self.x.get_unchecked(start..end);
        let y = self.y.get_unchecked(start..end);
        GeographyRef(GeometryRef::new(buf, x, y))
    }

    pub fn slice(&self, range: Range<usize>) -> Self {
        let buf = self.buf.slice(range.clone());
        let offsets = self
            .offsets
            .clone()
            .sliced(range.start, range.end - range.start + 1);
        let x = self.x.clone();
        let y = self.y.clone();
        GeographyColumn { buf, offsets, x, y }
    }

    pub fn iter(&self) -> GeographyIterator<'_> {
        GeographyIterator {
            buf: self.buf.iter(),
            offsets: self.offsets.windows(2),
            x: &self.x,
            y: &self.y,
        }
    }
}

pub struct GeographyIterator<'a> {
    pub(crate) buf: BinaryIterator<'a>,
    pub(crate) offsets: std::slice::Windows<'a, u64>,
    pub(crate) x: &'a [f64],
    pub(crate) y: &'a [f64],
}

unsafe impl<'a> TrustedLen for GeographyIterator<'a> {}

unsafe impl<'a> std::iter::TrustedLen for GeographyIterator<'a> {}

impl<'a> Iterator for GeographyIterator<'a> {
    type Item = GeographyRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.buf.next().map(|buf| {
            let range = match self.offsets.next().unwrap() {
                [start, end] => (*start as usize)..(*end as usize),
                _ => unreachable!(),
            };
            let x = &self.x[range.clone()];
            let y = &self.y[range];
            GeographyRef(GeometryRef::new(buf, x, y))
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.buf.size_hint()
    }
}

#[derive(Debug, Clone)]
pub struct GeographyColumnBuilder {
    pub(crate) buf: BinaryColumnBuilder,
    pub(crate) offsets: Vec<u64>,
    pub(crate) x: Vec<f64>,
    pub(crate) y: Vec<f64>,
}

impl GeographyColumnBuilder {
    pub fn with_capacity(len: usize, data_capacity: usize) -> Self {
        let mut offsets = Vec::with_capacity(len + 1);
        offsets.push(0);
        GeographyColumnBuilder {
            buf: BinaryColumnBuilder::with_capacity(len, data_capacity),
            offsets,
            x: Vec::with_capacity(len),
            y: Vec::with_capacity(len),
        }
    }

    pub fn from_column(col: GeographyColumn) -> Self {
        GeographyColumnBuilder {
            buf: BinaryColumnBuilder::from_column(col.buf),
            offsets: col.offsets.to_vec(),
            x: col.x.to_vec(),
            y: col.y.to_vec(),
        }
    }

    pub fn repeat(item: &GeographyRef<'_>, n: usize) -> Self {
        let buf = BinaryColumnBuilder::repeat(item.0.buf(), n);

        let col_x = item.0.x();
        let col_y = item.0.y();
        let len = col_x.len();
        let mut x = Vec::with_capacity(len * n);
        let mut y = Vec::with_capacity(len * n);
        let mut offsets = Vec::with_capacity(n + 1);
        offsets.push(0);

        for _ in 0..n {
            x.extend_from_slice(col_x);
            y.extend_from_slice(col_y);
            offsets.push(x.len() as u64);
        }

        Self { buf, offsets, x, y }
    }

    pub fn repeat_default(n: usize) -> Self {
        let item = Geography::default();
        let item = item.as_ref();

        Self::repeat(&item, n)
    }

    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    pub fn push(&mut self, item: GeographyRef<'_>) {
        debug_assert_eq!(item.0.x().len(), item.0.y().len());
        self.buf.put(item.0.buf());
        self.buf.commit_row();

        self.x.extend_from_slice(item.0.x());
        self.y.extend_from_slice(item.0.y());
        self.offsets.push(self.x.len() as u64);
    }

    pub fn push_repeat(&mut self, item: GeographyRef<'_>, n: usize) {
        debug_assert_eq!(item.0.x().len(), item.0.y().len());
        self.buf.push_repeat(item.0.buf(), n);

        let x = item.0.x();
        let y = item.0.y();
        let len = x.len();
        self.x.reserve(len * n);
        self.y.reserve(len * n);
        self.offsets.reserve(len);
        for _ in 0..n {
            self.x.extend_from_slice(x);
            self.y.extend_from_slice(y);
            self.offsets.push(self.x.len() as u64);
        }
    }

    pub fn push_default(&mut self) {
        self.push(Geography::default().as_ref())
    }

    pub fn append_column(&mut self, other: &GeographyColumn) {
        // the first offset of other column may not be zero
        let other_start = *other.offsets.first().unwrap();
        let start = self.offsets.last().cloned().unwrap();
        self.offsets.extend(
            other
                .offsets
                .iter()
                .skip(1)
                .map(|offset| offset + start - other_start),
        );
        self.buf.append_column(&other.buf);
        self.x.extend(other.x.iter());
        self.y.extend(other.y.iter());
    }

    pub fn build(self) -> GeographyColumn {
        let Self { buf, offsets, x, y } = self;
        GeographyColumn {
            buf: buf.build(),
            offsets: Buffer::from(offsets),
            x: Buffer::from(x),
            y: Buffer::from(y),
        }
    }

    pub fn build_scalar(mut self) -> Geography {
        assert_eq!(self.len(), 1);
        self.pop().unwrap()
    }

    pub fn pop(&mut self) -> Option<Geography> {
        if self.len() > 0 {
            let at = self.x.len()
                - (self.offsets[self.offsets.len() - 1] - self.offsets[self.offsets.len() - 2])
                    as usize;
            self.offsets.pop();
            let x = self.x.split_off(at);
            let y = self.y.split_off(at);
            let buf = self.buf.pop().unwrap();
            Some(Geography(Geometry::new(buf, x, y)))
        } else {
            None
        }
    }

    pub fn memory_size(&self) -> usize {
        let offsets = self.offsets.as_slice();
        let len = offsets.len();
        self.buf.memory_size() + len * 8 + (offsets[len - 1] - offsets[0]) as usize * 16
    }
}
