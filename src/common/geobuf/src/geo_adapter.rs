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

use flatbuffers::Vector;
use geozero::error::GeozeroError;
use geozero::GeozeroGeometry;

use crate::geo_buf;
use crate::geo_buf::InnerObject;
use crate::Geometry;
use crate::GeometryBuilder;
use crate::GeometryRef;
use crate::ObjectKind;

impl TryFrom<&geo::Geometry<f64>> for Geometry {
    type Error = GeozeroError;

    fn try_from(geo: &geo::Geometry<f64>) -> Result<Self, Self::Error> {
        let mut builder = GeometryBuilder::new();
        geo.process_geom(&mut builder)?;
        Ok(builder.build())
    }
}

impl TryInto<geo::Geometry<f64>> for &Geometry {
    type Error = GeozeroError;

    fn try_into(self) -> Result<geo::Geometry<f64>, Self::Error> {
        self.as_ref().try_into()
    }
}

impl<'a> TryInto<geo::Geometry<f64>> for GeometryRef<'a> {
    type Error = GeozeroError;

    fn try_into(self) -> Result<geo::Geometry<f64>, Self::Error> {
        debug_assert!(self.column_x.len() == self.column_y.len());
        geozero::ToGeo::to_geo(&self)
    }
}

impl<'a> geozero::GeozeroGeometry for GeometryRef<'a> {
    fn srid(&self) -> Option<i32> {
        GeometryRef::srid(self)
    }

    fn process_geom<P>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        P: geozero::GeomProcessor,
        Self: Sized,
    {
        debug_assert!(self.column_x.len() == self.column_y.len());
        match self.kind()?.object_kind() {
            ObjectKind::Point => {
                debug_assert!(self.column_x.len() == 1);
                let (x, y) = (self.column_x[0], self.column_y[0]);
                if x.is_nan() && y.is_nan() {
                    processor.empty_point(0)
                } else {
                    processor.point_begin(0)?;
                    processor.xy(x, y, 0)?;
                    processor.point_end(0)
                }
            }
            ObjectKind::MultiPoint => {
                processor.multipoint_begin(self.column_x.len(), 0)?;
                self.process_points(processor)?;
                processor.multipoint_end(0)
            }
            ObjectKind::LineString => {
                processor.linestring_begin(true, self.column_x.len(), 0)?;
                self.process_points(processor)?;
                processor.linestring_end(true, 0)
            }
            ObjectKind::MultiLineString => {
                if self.is_light_buf() {
                    processor.multilinestring_begin(0, 0)?;
                    processor.multilinestring_end(0)
                } else {
                    let object = self.read_object()?;
                    let point_offsets = read_point_offsets(object.point_offsets())?;
                    processor.multilinestring_begin(point_offsets.len() - 1, 0)?;
                    self.process_lines(processor, &point_offsets)?;
                    processor.multilinestring_end(0)
                }
            }
            ObjectKind::Polygon => {
                if self.is_light_buf() {
                    processor.polygon_begin(true, 0, 0)?;
                    processor.polygon_end(true, 0)
                } else {
                    let object = self.read_object()?;
                    let point_offsets = read_point_offsets(object.point_offsets())?;
                    processor.polygon_begin(true, point_offsets.len() - 1, 0)?;
                    self.process_lines(processor, &point_offsets)?;
                    processor.polygon_end(true, 0)
                }
            }
            ObjectKind::MultiPolygon => {
                if self.is_light_buf() {
                    processor.multipolygon_begin(0, 0)?;
                    processor.multipolygon_end(0)
                } else {
                    let object = self.read_object()?;
                    let ring_offsets = read_ring_offsets(object.ring_offsets())?;
                    let point_offsets = read_point_offsets(object.point_offsets())?;
                    processor.multipolygon_begin(ring_offsets.len() - 1, 0)?;
                    self.process_polygons(processor, &point_offsets, &ring_offsets)?;
                    processor.multipolygon_end(0)
                }
            }
            ObjectKind::GeometryCollection => {
                if self.is_light_buf() {
                    processor.geometrycollection_begin(0, 0)?;
                    processor.geometrycollection_end(0)
                } else {
                    let object = self.read_object()?;
                    let collection = object.collection().ok_or(GeozeroError::Geometry(
                        "Invalid Collection, collection missing".to_string(),
                    ))?;
                    processor.geometrycollection_begin(collection.len(), 0)?;
                    for (idx2, geometry) in collection.iter().enumerate() {
                        self.process_inner(processor, &geometry, idx2)?;
                    }
                    processor.geometrycollection_end(0)
                }
            }
        }
    }
}

impl<'a> GeometryRef<'a> {
    fn process_lines<P>(
        &self,
        processor: &mut P,
        point_offsets: &Vector<u32>,
    ) -> geozero::error::Result<()>
    where
        P: geozero::GeomProcessor,
    {
        point_offsets
            .iter()
            .map_windows(|[start, end]| *start as usize..*end as usize)
            .enumerate()
            .try_for_each(|(idx, range)| {
                processor.linestring_begin(false, range.len(), idx)?;
                for (idxc, pos) in range.enumerate() {
                    self.process_point(processor, pos, idxc)?;
                }
                processor.linestring_end(false, idx)
            })
    }

    fn process_point<P>(
        &self,
        processor: &mut P,
        pos: usize,
        idx: usize,
    ) -> geozero::error::Result<()>
    where
        P: geozero::GeomProcessor,
    {
        processor.xy(self.column_x[pos], self.column_y[pos], idx)
    }

    fn process_points<P>(&self, processor: &mut P) -> geozero::error::Result<()>
    where P: geozero::GeomProcessor {
        for pos in 0..self.column_x.len() {
            self.process_point(processor, pos, pos)?;
        }
        Ok(())
    }

    fn process_polygons<P>(
        &self,
        processor: &mut P,
        point_offsets: &Vector<u32>,
        ring_offsets: &Vector<u32>,
    ) -> geozero::error::Result<()>
    where
        P: geozero::GeomProcessor,
    {
        ring_offsets
            .iter()
            .map_windows(|[start, end]| *start as usize..=*end as usize)
            .enumerate()
            .try_for_each(|(idx, range)| {
                processor.polygon_begin(false, range.end() - range.start(), idx)?;
                range
                    .map(|i| point_offsets.get(i))
                    .map_windows(|[start, end]| *start as usize..*end as usize)
                    .enumerate()
                    .try_for_each(|(idx, range)| {
                        processor.linestring_begin(false, range.len(), idx)?;
                        for (idxc, pos) in range.enumerate() {
                            self.process_point(processor, pos, idxc)?;
                        }
                        processor.linestring_end(false, idx)
                    })?;
                processor.polygon_end(false, idx)
            })
    }

    pub(crate) fn process_inner<P>(
        &self,
        processor: &mut P,
        object: &InnerObject,
        idx: usize,
    ) -> geozero::error::Result<()>
    where
        P: geozero::GeomProcessor,
    {
        match object.wkb_type() {
            geo_buf::InnerObjectKind::Point => {
                let point_offsets = read_point_offsets(object.point_offsets())?;
                let pos = point_offsets.get(0) as usize;
                processor.point_begin(idx)?;
                self.process_point(processor, pos, 0)?;
                processor.point_end(idx)
            }
            geo_buf::InnerObjectKind::MultiPoint => {
                let point_offsets = read_point_offsets(object.point_offsets())?;
                let start = point_offsets.get(0);
                let end = point_offsets.get(1);

                let range = start as usize..end as usize;
                processor.multipoint_begin(range.len(), idx)?;
                for (idxc, pos) in range.enumerate() {
                    self.process_point(processor, pos, idxc)?;
                }
                processor.multipoint_end(idx)
            }
            geo_buf::InnerObjectKind::LineString => {
                let point_offsets = read_point_offsets(object.point_offsets())?;
                let start = point_offsets.get(0);
                let end = point_offsets.get(1);
                let range = start as usize..end as usize;
                processor.linestring_begin(true, range.len(), idx)?;
                for (idxc, pos) in range.enumerate() {
                    self.process_point(processor, pos, idxc)?;
                }
                processor.linestring_end(true, idx)
            }
            geo_buf::InnerObjectKind::MultiLineString => {
                let point_offsets = read_point_offsets(object.point_offsets())?;
                processor.multilinestring_begin(point_offsets.len() - 1, idx)?;
                self.process_lines(processor, &point_offsets)?;
                processor.multilinestring_end(idx)
            }
            geo_buf::InnerObjectKind::Polygon => {
                let point_offsets = read_point_offsets(object.point_offsets())?;
                processor.polygon_begin(true, point_offsets.len() - 1, idx)?;
                self.process_lines(processor, &point_offsets)?;
                processor.polygon_end(true, idx)
            }
            geo_buf::InnerObjectKind::MultiPolygon => {
                let point_offsets = read_point_offsets(object.point_offsets())?;
                let ring_offsets = read_ring_offsets(object.ring_offsets())?;
                processor.multipolygon_begin(ring_offsets.len() - 1, idx)?;
                self.process_polygons(processor, &point_offsets, &ring_offsets)?;
                processor.multipoint_end(idx)
            }
            geo_buf::InnerObjectKind::Collection => {
                let collection = object.collection().ok_or(GeozeroError::Geometry(
                    "Invalid Collection, collection missing".to_string(),
                ))?;
                processor.geometrycollection_begin(collection.len(), idx)?;
                for (idx2, geometry) in collection.iter().enumerate() {
                    self.process_inner(processor, &geometry, idx2)?;
                }
                processor.geometrycollection_end(idx)
            }
            _ => unreachable!(),
        }
    }
}

fn read_point_offsets(
    point_offsets: Option<flatbuffers::Vector<'_, u32>>,
) -> Result<flatbuffers::Vector<'_, u32>, GeozeroError> {
    let v = point_offsets.ok_or(GeozeroError::Geometry(
        "Invalid Object, point_offsets missing".to_string(),
    ))?;
    Ok(v)
}

fn read_ring_offsets(
    ring_offsets: Option<flatbuffers::Vector<'_, u32>>,
) -> Result<flatbuffers::Vector<'_, u32>, GeozeroError> {
    let v = ring_offsets.ok_or(GeozeroError::Geometry(
        "Invalid Object, ring_offsets missing".to_string(),
    ))?;
    Ok(v)
}
