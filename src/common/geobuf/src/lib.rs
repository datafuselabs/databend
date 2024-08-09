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

#![feature(iter_map_windows)]
mod builder;
mod geo_adapter;
mod geo_generated;
mod geojson_adapter;
mod geometry;
mod serde;
mod wkb_addapter;
mod wkt_adapter;

use builder::GeometryBuilder;
use databend_common_arrow::arrow::buffer::Buffer;
use geo_generated::geo_buf;
pub use geojson_adapter::GeoJson;
use geojson_adapter::JsonObject;
pub use geometry::BoundingBox;
use geozero::error::Result as GeoResult;
pub use wkb_addapter::Ewkb;
pub use wkb_addapter::Wkb;
pub use wkt_adapter::Ewkt;
pub use wkt_adapter::Wkt;

#[derive(Clone)]
pub struct Geometry {
    buf: Vec<u8>,
    column_x: Buffer<f64>,
    column_y: Buffer<f64>,
}

impl Geometry {
    pub fn as_ref(&self) -> GeometryRef<'_> {
        GeometryRef {
            buf: &self.buf,
            column_x: self.column_x.as_slice(),
            column_y: self.column_y.as_slice(),
        }
    }
}

#[derive(Clone, Copy)]
pub struct GeometryRef<'a> {
    buf: &'a [u8],
    column_x: &'a [f64],
    column_y: &'a [f64],
}

impl<'a> GeometryRef<'a> {
    pub fn new(buf: &'a [u8], x: &'a [f64], y: &'a [f64]) -> Self {
        debug_assert_eq!(x.len(), y.len());
        GeometryRef {
            buf,
            column_x: x,
            column_y: y,
        }
    }

    pub fn buf(&self) -> &[u8] {
        self.buf
    }

    pub fn x(&self) -> &[f64] {
        self.column_x
    }

    pub fn y(&self) -> &[f64] {
        self.column_y
    }
}

impl<'a> GeometryRef<'a> {
    pub fn to_owned(&self) -> Geometry {
        Geometry {
            buf: self.buf.to_vec(),
            column_x: Buffer::from(self.column_x.to_vec()),
            column_y: Buffer::from(self.column_y.to_vec()),
        }
    }
}

pub trait Visitor {
    fn visit_point(&mut self, x: f64, y: f64, multi: bool) -> GeoResult<()>;

    fn visit_points_start(&mut self, n: usize) -> GeoResult<()>;

    fn visit_points_end(&mut self, multi: bool) -> GeoResult<()>;

    fn visit_lines_start(&mut self, n: usize) -> GeoResult<()>;

    fn visit_lines_end(&mut self) -> GeoResult<()>;

    fn visit_polygon_start(&mut self, n: usize) -> GeoResult<()>;

    fn visit_polygon_end(&mut self, multi: bool) -> GeoResult<()>;

    fn visit_polygons_start(&mut self, n: usize) -> GeoResult<()>;

    fn visit_polygons_end(&mut self) -> GeoResult<()>;

    fn visit_collection_start(&mut self, n: usize) -> GeoResult<()>;

    fn visit_collection_end(&mut self) -> GeoResult<()>;

    fn visit_feature(&mut self, properties: Option<&JsonObject>) -> GeoResult<()>;

    fn finish(&mut self, kind: FeatureKind) -> GeoResult<()>;
}

pub trait Element<V: Visitor> {
    fn accept(&self, visitor: &mut V) -> GeoResult<()>;
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FeatureKind {
    Geometry(ObjectKind),
    Feature(ObjectKind),
    FeatureCollection,
}

impl FeatureKind {
    const FEATURE: u8 = 1 << 3;
    const FEATURE_COLLECTION: u8 = 1 << 4;
    pub fn as_u8(&self) -> u8 {
        match self {
            FeatureKind::FeatureCollection => Self::FEATURE_COLLECTION,
            FeatureKind::Geometry(o) => *o as u8,
            FeatureKind::Feature(o) => *o as u8 | Self::FEATURE,
        }
    }

    pub fn object_kind(&self) -> ObjectKind {
        match self {
            FeatureKind::Geometry(o) | FeatureKind::Feature(o) => *o,
            FeatureKind::FeatureCollection => ObjectKind::GeometryCollection,
        }
    }
}

impl TryFrom<u8> for FeatureKind {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, ()> {
        match value {
            FeatureKind::FEATURE_COLLECTION => Ok(FeatureKind::FeatureCollection),
            _ => {
                let object_kind = match value & !ObjectKind::FEATURE {
                    1 => ObjectKind::Point,
                    2 => ObjectKind::LineString,
                    3 => ObjectKind::Polygon,
                    4 => ObjectKind::MultiPoint,
                    5 => ObjectKind::MultiLineString,
                    6 => ObjectKind::MultiPolygon,
                    7 => ObjectKind::GeometryCollection,
                    _ => return Err(()),
                };
                if value & FeatureKind::FEATURE != 0 {
                    Ok(FeatureKind::Feature(object_kind))
                } else {
                    Ok(FeatureKind::Geometry(object_kind))
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ObjectKind {
    Point = 1,
    LineString = 2,
    Polygon = 3,
    MultiPoint = 4,
    MultiLineString = 5,
    MultiPolygon = 6,
    GeometryCollection = 7,
}

impl ObjectKind {
    pub const FEATURE: u8 = 1 << 3;
}

impl From<ObjectKind> for geo_buf::InnerObjectKind {
    fn from(val: ObjectKind) -> Self {
        geo_buf::InnerObjectKind(val as u8)
    }
}
