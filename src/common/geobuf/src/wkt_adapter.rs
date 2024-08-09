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

use geozero::error::GeozeroError;

use crate::Element;
use crate::FeatureKind;
use crate::Geometry;
use crate::GeometryBuilder;
use crate::GeometryRef;
use crate::ObjectKind;
use crate::Visitor;

pub struct Wkt<S: AsRef<str>>(pub S);

impl<S: AsRef<str>> TryFrom<Wkt<S>> for Geometry {
    type Error = GeozeroError;

    fn try_from(str: Wkt<S>) -> Result<Self, Self::Error> {
        let wkt_struct: wkt::Wkt<f64> = str
            .0
            .as_ref()
            .parse()
            .map_err(|e: &str| GeozeroError::Geometry(e.to_string()))?;
        let mut builder = GeometryBuilder::new();
        wkt_struct.accept(&mut builder)?;
        Ok(builder.build())
    }
}

impl TryInto<Wkt<String>> for &Geometry {
    type Error = GeozeroError;

    fn try_into(self) -> Result<Wkt<String>, Self::Error> {
        self.as_ref().try_into()
    }
}

impl<'a> TryInto<Wkt<String>> for GeometryRef<'a> {
    type Error = GeozeroError;

    fn try_into(self) -> Result<Wkt<String>, Self::Error> {
        let str = geozero::ToWkt::to_wkt(&self)?;
        Ok(Wkt(str))
    }
}

pub struct Ewkt<S: AsRef<str>>(pub S);

impl<S: AsRef<str>> Ewkt<S> {
    pub fn cut_srid(&self) -> Result<(Option<i32>, &str), GeozeroError> {
        let str = self.0.as_ref();
        match str.find(';') {
            None => Ok((None, str)),
            Some(idx) => {
                let prefix = str[..idx].trim();
                match prefix
                    .strip_prefix("SRID=")
                    .or_else(|| prefix.strip_prefix("srid="))
                    .and_then(|srid| srid.trim().parse::<i32>().ok())
                {
                    Some(srid) => Ok((Some(srid), &str[idx + 1..])),
                    None => Err(GeozeroError::Geometry(format!(
                        "invalid EWKT with prefix {prefix}"
                    ))),
                }
            }
        }
    }
}

impl<S: AsRef<str>> TryFrom<Ewkt<S>> for Geometry {
    type Error = GeozeroError;

    fn try_from(str: Ewkt<S>) -> Result<Self, Self::Error> {
        let (srid, str) = str.cut_srid()?;

        let wkt_struct: wkt::Wkt<f64> = str
            .parse()
            .map_err(|e: &str| GeozeroError::Geometry(e.to_string()))?;
        let mut builder = GeometryBuilder::new();
        builder.set_srid(srid);
        wkt_struct.accept(&mut builder)?;
        Ok(builder.build())
    }
}

impl TryInto<Ewkt<String>> for &Geometry {
    type Error = GeozeroError;

    fn try_into(self) -> Result<Ewkt<String>, Self::Error> {
        self.as_ref().try_into()
    }
}

impl<'a> TryInto<Ewkt<String>> for GeometryRef<'a> {
    type Error = GeozeroError;

    fn try_into(self) -> Result<Ewkt<String>, Self::Error> {
        let str = geozero::ToWkt::to_wkt_with_opts(
            &self,
            geozero::wkt::WktDialect::Ewkt,
            geozero::CoordDimensions::xy(),
            self.srid(),
        )?;
        Ok(Ewkt(str))
    }
}

impl<V: Visitor> Element<V> for wkt::Wkt<f64> {
    fn accept(&self, visitor: &mut V) -> Result<(), GeozeroError> {
        use wkt::types::*;
        use wkt::Geometry;

        fn visit_points(
            points: &[Coord<f64>],
            visitor: &mut impl Visitor,
            multi: bool,
        ) -> Result<(), GeozeroError> {
            visitor.visit_points_start(points.len())?;
            for p in points.iter() {
                let (x, y) = normalize_coord(p)?;
                visitor.visit_point(x, y, true)?;
            }
            visitor.visit_points_end(multi)
        }

        fn accept_geom(
            geom: &Geometry<f64>,
            visitor: &mut impl Visitor,
        ) -> Result<(), GeozeroError> {
            match geom {
                Geometry::Point(point) => {
                    let (x, y) = normalize_point(point)?;
                    visitor.visit_point(x, y, false)?;
                    visitor.finish(FeatureKind::Geometry(ObjectKind::Point))
                }
                Geometry::MultiPoint(MultiPoint(points)) => {
                    visitor.visit_points_start(points.len())?;
                    for point in points {
                        let (x, y) = normalize_point(point)?;
                        visitor.visit_point(x, y, true)?;
                    }
                    visitor.visit_points_end(false)?;
                    visitor.finish(FeatureKind::Geometry(ObjectKind::MultiPoint))
                }
                Geometry::LineString(LineString(line)) => {
                    visit_points(line, visitor, false)?;
                    visitor.finish(FeatureKind::Geometry(ObjectKind::LineString))
                }
                Geometry::MultiLineString(MultiLineString(lines)) => {
                    visitor.visit_lines_start(lines.len())?;
                    for line in lines.iter() {
                        visit_points(&line.0, visitor, true)?;
                    }
                    visitor.visit_lines_end()?;
                    visitor.finish(FeatureKind::Geometry(ObjectKind::MultiLineString))
                }
                Geometry::Polygon(Polygon(polygon)) => {
                    visitor.visit_polygon_start(polygon.len())?;
                    for ring in polygon {
                        visit_points(&ring.0, visitor, true)?;
                    }
                    visitor.visit_polygon_end(false)?;
                    visitor.finish(FeatureKind::Geometry(ObjectKind::Polygon))
                }
                Geometry::MultiPolygon(MultiPolygon(polygons)) => {
                    visitor.visit_polygons_start(polygons.len())?;
                    for polygon in polygons {
                        visitor.visit_polygon_start(polygon.0.len())?;
                        for ring in &polygon.0 {
                            visit_points(&ring.0, visitor, true)?;
                        }
                        visitor.visit_polygon_end(true)?;
                    }
                    visitor.visit_polygons_end()?;
                    visitor.finish(FeatureKind::Geometry(ObjectKind::MultiPolygon))
                }
                Geometry::GeometryCollection(GeometryCollection(collection)) => {
                    visitor.visit_collection_start(collection.len())?;
                    for geom in collection {
                        accept_geom(geom, visitor)?;
                    }
                    visitor.visit_collection_end()?;
                    visitor.finish(FeatureKind::Geometry(ObjectKind::GeometryCollection))
                }
            }
        }

        accept_geom(&self.item, visitor)
    }
}

fn normalize_coord(coord: &wkt::types::Coord<f64>) -> Result<(f64, f64), GeozeroError> {
    if coord.z.is_some() || coord.m.is_some() {
        Err(GeozeroError::Geometry(
            "coordinates higher than two dimensions are not supported".to_string(),
        ))
    } else {
        Ok((coord.x, coord.y))
    }
}

fn normalize_point(point: &wkt::types::Point<f64>) -> Result<(f64, f64), GeozeroError> {
    match &point.0 {
        Some(c) => normalize_coord(c),
        None => Ok((f64::NAN, f64::NAN)),
    }
}
