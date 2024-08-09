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

use databend_common_arrow::arrow::buffer::Buffer;
use flatbuffers::FlatBufferBuilder;
use flatbuffers::Vector;
use flatbuffers::WIPOffset;
use geozero::error::GeozeroError;
use geozero::error::Result as GeoResult;

use crate::geo_buf;
use crate::geo_buf::InnerObject;
use crate::geo_buf::Object;
use crate::FeatureKind;
use crate::Geometry;
use crate::JsonObject;
use crate::ObjectKind;
use crate::Visitor;

#[derive(Default)]
pub struct GeometryBuilder<'fbb> {
    fbb: flatbuffers::FlatBufferBuilder<'fbb>,
    column_x: Vec<f64>,
    column_y: Vec<f64>,
    point_offsets: Vec<u32>,
    ring_offsets: Vec<u32>,
    properties: Option<Vec<u8>>,
    temp_kind: Option<ObjectKind>,
    srid: i32,

    kind: Option<FeatureKind>,
    stack: Vec<Vec<WIPOffset<InnerObject<'fbb>>>>,
    object: Option<WIPOffset<Object<'fbb>>>,
}

impl<'fbb> GeometryBuilder<'fbb> {
    pub fn new() -> Self {
        Self {
            fbb: FlatBufferBuilder::new(),
            column_x: Vec::new(),
            column_y: Vec::new(),
            point_offsets: Vec::new(),
            ring_offsets: Vec::new(),
            properties: None,
            temp_kind: None,
            srid: 0,

            kind: None,
            stack: Vec::new(),
            object: None,
        }
    }

    pub fn point_len(&self) -> usize {
        self.column_x.len()
    }

    fn push_point(&mut self, x: f64, y: f64) {
        self.column_x.push(x);
        self.column_y.push(y);
    }

    fn create_point_offsets(&mut self) -> Option<WIPOffset<Vector<'fbb, u32>>> {
        if self.point_offsets.len() > 1 {
            let v = Some(self.fbb.create_vector(&self.point_offsets));
            self.point_offsets.clear();
            v
        } else {
            None
        }
    }

    fn create_ring_offsets(&mut self) -> Option<WIPOffset<Vector<'fbb, u32>>> {
        if self.ring_offsets.len() > 1 {
            let v = Some(self.fbb.create_vector(&self.ring_offsets));
            self.ring_offsets.clear();
            v
        } else {
            None
        }
    }

    fn create_properties(&mut self) -> Option<WIPOffset<Vector<'fbb, u8>>> {
        self.properties
            .take()
            .as_ref()
            .map(|data| self.fbb.create_vector(data))
    }

    pub fn build(self) -> Geometry {
        let GeometryBuilder {
            mut fbb,
            column_x,
            column_y,
            kind,
            object,
            ..
        } = self;

        let kind = kind.unwrap().as_u8();

        let buf = if let Some(object) = object {
            geo_buf::finish_object_buffer(&mut fbb, object);
            let (mut buf, index) = fbb.collapse();
            if index > 0 {
                let index = index - 1;
                buf[index] = kind;
                buf[index..].to_vec()
            } else {
                [vec![kind], buf].concat()
            }
        } else {
            vec![kind]
        };

        Geometry {
            buf,
            column_x: Buffer::from(column_x),
            column_y: Buffer::from(column_y),
        }
    }

    pub fn set_srid(&mut self, srid: Option<i32>) {
        if let Some(srid) = srid {
            if srid != 0 {
                self.srid = srid;
            }
        }
    }
}

impl<'fbb> Visitor for GeometryBuilder<'fbb> {
    fn visit_point(&mut self, x: f64, y: f64, multi: bool) -> GeoResult<()> {
        if self.stack.is_empty() {
            self.push_point(x, y);
            return Ok(());
        }

        if multi {
            self.push_point(x, y);
        } else {
            if self.point_offsets.is_empty() {
                self.point_offsets.push(self.point_len() as u32)
            }
            self.push_point(x, y);
            self.point_offsets.push(self.point_len() as u32);
        }

        Ok(())
    }

    fn visit_points_start(&mut self, _: usize) -> GeoResult<()> {
        if !self.stack.is_empty() && self.point_offsets.is_empty() {
            self.point_offsets.push(self.point_len() as u32)
        }
        Ok(())
    }

    fn visit_points_end(&mut self, multi: bool) -> GeoResult<()> {
        if multi || !self.stack.is_empty() {
            self.point_offsets.push(self.point_len() as u32);
        }
        Ok(())
    }

    fn visit_lines_start(&mut self, _: usize) -> GeoResult<()> {
        if self.point_offsets.is_empty() {
            self.point_offsets.push(self.point_len() as u32)
        }
        Ok(())
    }

    fn visit_lines_end(&mut self) -> GeoResult<()> {
        if !self.stack.is_empty() {
            self.ring_offsets.push(self.point_offsets.len() as u32 - 1);
        }
        Ok(())
    }

    fn visit_polygon_start(&mut self, _: usize) -> GeoResult<()> {
        if self.point_offsets.is_empty() {
            self.point_offsets.push(self.point_len() as u32)
        }
        Ok(())
    }

    fn visit_polygon_end(&mut self, multi: bool) -> GeoResult<()> {
        if multi || !self.stack.is_empty() {
            self.ring_offsets.push(self.point_offsets.len() as u32 - 1);
        }
        Ok(())
    }

    fn visit_polygons_start(&mut self, _: usize) -> GeoResult<()> {
        if self.ring_offsets.is_empty() {
            self.ring_offsets.push(self.point_len() as u32)
        }
        Ok(())
    }

    fn visit_polygons_end(&mut self) -> GeoResult<()> {
        Ok(())
    }

    fn visit_collection_start(&mut self, n: usize) -> GeoResult<()> {
        self.stack.push(Vec::with_capacity(n));
        Ok(())
    }

    fn visit_collection_end(&mut self) -> GeoResult<()> {
        Ok(())
    }

    fn visit_feature(&mut self, properties: Option<&JsonObject>) -> GeoResult<()> {
        if let Some(properties) = properties {
            if !properties.is_empty() {
                let data = flexbuffers::to_vec(properties)
                    .map_err(|e| GeozeroError::Geometry(e.to_string()))?;
                self.properties = Some(data);
            }
        }
        Ok(())
    }

    fn finish(&mut self, kind: FeatureKind) -> GeoResult<()> {
        match kind {
            FeatureKind::Geometry(object_kind) | FeatureKind::Feature(object_kind) => {
                if self.stack.is_empty() {
                    let point_offsets = self.create_point_offsets();
                    let ring_offsets = self.create_ring_offsets();
                    let properties = self.create_properties();
                    let srid = self.srid;

                    if point_offsets.is_none()
                        && ring_offsets.is_none()
                        && properties.is_none()
                        && srid == 0
                    {
                        self.object = None
                    } else {
                        self.object = Some(geo_buf::Object::create(
                            &mut self.fbb,
                            &geo_buf::ObjectArgs {
                                point_offsets,
                                ring_offsets,
                                srid,
                                properties,
                                ..Default::default()
                            },
                        ))
                    }

                    self.kind = Some(kind);
                    return Ok(());
                }

                if self.stack.len() == 1 && object_kind == ObjectKind::GeometryCollection {
                    let geometries = self.stack.pop().unwrap();
                    let collection = if !geometries.is_empty() {
                        Some(self.fbb.create_vector(&geometries))
                    } else {
                        None
                    };
                    let properties = self.create_properties();
                    let srid = self.srid;

                    if collection.is_none() && properties.is_none() && srid == 0 {
                        self.object = None;
                    } else {
                        self.object = Some(geo_buf::Object::create(
                            &mut self.fbb,
                            &geo_buf::ObjectArgs {
                                collection,
                                srid,
                                properties,
                                ..Default::default()
                            },
                        ));
                    }

                    self.kind = Some(kind);
                    return Ok(());
                }

                let object = match object_kind {
                    ObjectKind::Point
                    | ObjectKind::MultiPoint
                    | ObjectKind::LineString
                    | ObjectKind::MultiLineString
                    | ObjectKind::Polygon
                    | ObjectKind::MultiPolygon => {
                        let point_offsets = self.create_point_offsets();
                        let ring_offsets = self.create_ring_offsets();
                        let properties = self.create_properties();
                        geo_buf::InnerObject::create(&mut self.fbb, &geo_buf::InnerObjectArgs {
                            wkb_type: object_kind.into(),
                            point_offsets,
                            ring_offsets,
                            properties,
                            ..Default::default()
                        })
                    }
                    ObjectKind::GeometryCollection => {
                        let geometries = self.stack.pop().unwrap();
                        let collection = Some(self.fbb.create_vector(&geometries));
                        let properties = self.create_properties();
                        geo_buf::InnerObject::create(&mut self.fbb, &geo_buf::InnerObjectArgs {
                            wkb_type: object_kind.into(),
                            collection,
                            properties,
                            ..Default::default()
                        })
                    }
                };
                self.stack.last_mut().unwrap().push(object);
                Ok(())
            }
            FeatureKind::FeatureCollection => {
                let geometries = self.stack.pop().unwrap();
                let collection = Some(self.fbb.create_vector(&geometries));
                let properties = self.create_properties();
                let srid = self.srid;

                let object = geo_buf::Object::create(&mut self.fbb, &geo_buf::ObjectArgs {
                    collection,
                    srid,
                    properties,
                    ..Default::default()
                });
                self.kind = Some(kind);
                self.object = Some(object);
                Ok(())
            }
        }
    }
}

impl<'fbb> geozero::GeomProcessor for GeometryBuilder<'fbb> {
    fn multi_dim(&self) -> bool {
        false
    }

    fn srid(&mut self, srid: Option<i32>) -> GeoResult<()> {
        self.set_srid(srid);
        Ok(())
    }

    fn empty_point(&mut self, idx: usize) -> GeoResult<()> {
        self.xy(f64::NAN, f64::NAN, idx)
    }

    fn xy(&mut self, x: f64, y: f64, _: usize) -> GeoResult<()> {
        let multi = !matches!(self.temp_kind, Some(ObjectKind::Point));
        self.visit_point(x, y, multi)?;
        Ok(())
    }

    fn point_begin(&mut self, _: usize) -> GeoResult<()> {
        self.temp_kind.get_or_insert(ObjectKind::Point);
        Ok(())
    }

    fn point_end(&mut self, _: usize) -> GeoResult<()> {
        if let Some(kind @ ObjectKind::Point) = self.temp_kind {
            self.finish(FeatureKind::Geometry(kind))?;
            self.temp_kind = None;
        }
        Ok(())
    }

    fn multipoint_begin(&mut self, size: usize, _: usize) -> GeoResult<()> {
        self.temp_kind.get_or_insert(ObjectKind::MultiPoint);
        self.visit_points_start(size)?;
        Ok(())
    }

    fn multipoint_end(&mut self, _: usize) -> GeoResult<()> {
        let multi = !matches!(self.temp_kind, Some(ObjectKind::MultiPoint));
        self.visit_points_end(multi)?;
        if let Some(kind @ ObjectKind::MultiPoint) = self.temp_kind {
            self.finish(FeatureKind::Geometry(kind))?;
            self.temp_kind = None;
        }
        Ok(())
    }

    fn linestring_begin(&mut self, _: bool, size: usize, _: usize) -> GeoResult<()> {
        self.temp_kind.get_or_insert(ObjectKind::LineString);
        self.visit_points_start(size)
    }

    fn linestring_end(&mut self, tagged: bool, _: usize) -> GeoResult<()> {
        self.visit_points_end(!tagged)?;
        if let Some(kind @ ObjectKind::LineString) = self.temp_kind {
            self.finish(FeatureKind::Geometry(kind))?;
            self.temp_kind = None;
        }
        Ok(())
    }

    fn multilinestring_begin(&mut self, size: usize, _: usize) -> GeoResult<()> {
        self.temp_kind.get_or_insert(ObjectKind::MultiLineString);
        self.visit_lines_start(size)
    }

    fn multilinestring_end(&mut self, _: usize) -> GeoResult<()> {
        self.visit_lines_end()?;
        if let Some(kind @ ObjectKind::MultiLineString) = self.temp_kind {
            self.finish(FeatureKind::Geometry(kind))?;
            self.temp_kind = None;
        }
        Ok(())
    }

    fn polygon_begin(&mut self, _: bool, size: usize, _: usize) -> GeoResult<()> {
        self.temp_kind.get_or_insert(ObjectKind::Polygon);
        self.visit_polygon_start(size)
    }

    fn polygon_end(&mut self, tagged: bool, _: usize) -> GeoResult<()> {
        self.visit_polygon_end(!tagged)?;
        if let Some(kind @ ObjectKind::Polygon) = self.temp_kind {
            self.finish(FeatureKind::Geometry(kind))?;
            self.temp_kind = None;
        }
        Ok(())
    }

    fn multipolygon_begin(&mut self, size: usize, _: usize) -> GeoResult<()> {
        self.temp_kind.get_or_insert(ObjectKind::MultiPolygon);
        self.visit_polygons_start(size)
    }

    fn multipolygon_end(&mut self, _: usize) -> GeoResult<()> {
        self.visit_polygons_end()?;
        if let Some(kind @ ObjectKind::MultiPolygon) = self.temp_kind {
            self.finish(FeatureKind::Geometry(kind))?;
            self.temp_kind = None;
        }
        Ok(())
    }

    fn geometrycollection_begin(&mut self, size: usize, _: usize) -> GeoResult<()> {
        self.visit_collection_start(size)
    }

    fn geometrycollection_end(&mut self, _: usize) -> GeoResult<()> {
        self.visit_collection_end()?;
        self.finish(FeatureKind::Geometry(ObjectKind::GeometryCollection))
    }
}
