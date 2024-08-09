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

use databend_common_arrow::arrow::buffer::Buffer;
use geozero::error::GeozeroError;
use ordered_float::OrderedFloat;

use crate::geo_buf;
use crate::geo_buf::Object;
use crate::FeatureKind;
use crate::GeoJson;
use crate::Geometry;
use crate::GeometryBuilder;
use crate::GeometryRef;
use crate::ObjectKind;
use crate::Visitor;

pub struct BoundingBox {
    pub xmin: f64,
    pub xmax: f64,
    pub ymin: f64,
    pub ymax: f64,
}

impl<'a> GeometryRef<'a> {
    pub fn bounding_box(&self) -> BoundingBox {
        fn cmp(a: &f64, b: &f64) -> std::cmp::Ordering {
            std::cmp::Ord::cmp(&OrderedFloat(*a), &OrderedFloat(*b))
        }
        BoundingBox {
            xmin: self
                .column_x
                .iter()
                .copied()
                .min_by(cmp)
                .unwrap_or(f64::NAN),
            xmax: self
                .column_x
                .iter()
                .copied()
                .max_by(cmp)
                .unwrap_or(f64::NAN),
            ymin: self
                .column_y
                .iter()
                .copied()
                .min_by(cmp)
                .unwrap_or(f64::NAN),
            ymax: self
                .column_y
                .iter()
                .copied()
                .max_by(cmp)
                .unwrap_or(f64::NAN),
        }
    }

    pub fn memory_size(&self) -> usize {
        self.buf.len() + self.column_x.len() * 16
    }

    pub fn srid(&self) -> Option<i32> {
        if self.is_light_buf() {
            None
        } else {
            self.read_object().map_or(None, |object| {
                let srid = object.srid();
                if srid == 0 { None } else { Some(srid) }
            })
        }
    }

    pub(crate) fn read_object(&self) -> Result<Object, GeozeroError> {
        geo_buf::root_as_object(&self.buf[1..]).map_err(|e| GeozeroError::Geometry(e.to_string()))
    }

    pub fn is_light_buf(&self) -> bool {
        self.buf.len() == 1
    }

    pub fn kind(&self) -> Result<FeatureKind, GeozeroError> {
        self.buf[0]
            .try_into()
            .map_err(|_| GeozeroError::Geometry("Invalid data".to_string()))
    }

    pub fn points_len(&self) -> usize {
        self.column_x.len()
    }

    // Returns the first Point in a LineString.
    pub fn start_point(&self) -> Result<Geometry, GeozeroError> {
        let kind = self.kind()?;
        if !matches!(kind.object_kind(), ObjectKind::LineString) {
            return Err(GeozeroError::Geometry("Not a LineString".to_string()));
        }

        let mut builder = GeometryBuilder::new();
        builder.set_srid(self.srid());
        builder.visit_point(self.column_x[0], self.column_x[1], false)?;
        builder.finish(FeatureKind::Geometry(ObjectKind::Point))?;
        Ok(builder.build())
    }

    pub fn is_empty_point(&self) -> Result<bool, GeozeroError> {
        Ok(self.kind()?.object_kind() == ObjectKind::Point
            && self.column_x[0].is_nan()
            && self.column_y[0].is_nan())
    }
}

impl<'a> Hash for GeometryRef<'a> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        state.write(self.buf);
        for (x, y) in self.column_x.iter().zip(self.column_y.iter()) {
            state.write_u64(x.to_bits());
            state.write_u64(y.to_bits());
        }
    }
}

impl<'a> PartialEq for GeometryRef<'a> {
    fn eq(&self, other: &Self) -> bool {
        if self.column_x.len() != other.column_x.len() {
            return false;
        }
        if self.buf != other.buf {
            return false;
        }

        let left = self.column_x.iter().zip(self.column_y.iter());
        let right = other.column_x.iter().zip(other.column_y.iter());
        left.zip(right)
            .all(|((x1, y1), (x2, y2))| eq_f64(*x1, *x2) && eq_f64(*y1, *y2))
    }
}

impl<'a> Eq for GeometryRef<'a> {}

fn eq_f64(a: f64, b: f64) -> bool {
    if a == b || a.is_nan() && b.is_nan() {
        return true;
    }
    false
    // if a.is_sign_positive() != b.is_sign_positive() {
    //     return a == b; // values of different signs are only equal if both are zero.
    // }
    // // https://jtempest.github.io/float_eq-rs/book/background/float_comparison_algorithms.html#units-in-the-last-place-ulps-comparison
    // let a_bits = a.to_bits();
    // let b_bits = b.to_bits();
    // const TOL: u64 = 10;
    // if a_bits > b_bits {
    //     a_bits - b_bits <= TOL
    // } else {
    //     b_bits - a_bits <= TOL
    // }
}

impl PartialEq for Geometry {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref().eq(&other.as_ref())
    }
}

impl Eq for Geometry {}

impl Geometry {
    pub fn new(buf: Vec<u8>, x: Vec<f64>, y: Vec<f64>) -> Self {
        Geometry {
            buf,
            column_x: Buffer::from(x),
            column_y: Buffer::from(y),
        }
    }

    pub fn point(x: f64, y: f64) -> Self {
        Self::new(
            vec![FeatureKind::Geometry(ObjectKind::Point).as_u8()],
            vec![x],
            vec![y],
        )
    }
}

impl Default for Geometry {
    fn default() -> Self {
        Self {
            buf: vec![FeatureKind::Geometry(ObjectKind::GeometryCollection).as_u8()],
            column_x: Buffer::default(),
            column_y: Buffer::default(),
        }
    }
}

impl<'a> PartialOrd for GeometryRef<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.buf[0].partial_cmp(&other.buf[0]) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.column_x.len().partial_cmp(&other.column_x.len()) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.buf.cmp(other.buf) {
            core::cmp::Ordering::Equal => {}
            ord => return Some(ord),
        }
        match self.column_x.partial_cmp(other.column_x) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.column_y.partial_cmp(other.column_y)
    }
}

impl PartialOrd for Geometry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.as_ref().partial_cmp(&other.as_ref())
    }
}

impl<'a> Debug for GeometryRef<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Ok(GeoJson(str)) = (*self).try_into() {
            f.write_str(&str)
        } else {
            f.debug_struct("GeometryRef")
                .field("buf", &self.buf)
                .field("x", &self.column_x)
                .field("y", &self.column_y)
                .finish()
        }
    }
}

impl Debug for Geometry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.as_ref())
    }
}
