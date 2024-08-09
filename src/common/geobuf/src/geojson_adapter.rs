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
use geozero::ColumnValue;
use geozero::GeozeroGeometry;

use crate::Element;
use crate::FeatureKind;
use crate::Geometry;
use crate::GeometryBuilder;
use crate::GeometryRef;
use crate::ObjectKind;
use crate::Visitor;

pub struct GeoJson<S: AsRef<str>>(pub S);

impl<S: AsRef<str>> TryFrom<GeoJson<S>> for Geometry {
    type Error = GeozeroError;

    fn try_from(str: GeoJson<S>) -> Result<Self, Self::Error> {
        let json_struct: geojson::GeoJson = str.0.as_ref().parse()?;
        let mut builder = GeometryBuilder::new();
        json_struct.accept(&mut builder)?;
        Ok(builder.build())
    }
}

impl TryInto<GeoJson<String>> for &Geometry {
    type Error = GeozeroError;

    fn try_into(self) -> Result<GeoJson<String>, Self::Error> {
        self.as_ref().try_into()
    }
}

impl<'a> TryInto<GeoJson<String>> for GeometryRef<'a> {
    type Error = GeozeroError;

    fn try_into(self) -> Result<GeoJson<String>, Self::Error> {
        let mut out: Vec<u8> = Vec::new();
        let mut p = geozero::geojson::GeoJsonWriter::new(&mut out);
        self.process_features(&mut p)?;

        match String::from_utf8(out) {
            Ok(str) => Ok(GeoJson(str)),
            Err(_) => Err(geozero::error::GeozeroError::Geometry(
                "Invalid UTF-8 encoding".to_string(),
            )),
        }
    }
}

impl<V: Visitor> Element<V> for geojson::GeoJson {
    fn accept(&self, visitor: &mut V) -> Result<(), GeozeroError> {
        use geojson::Feature;
        use geojson::GeoJson;
        use geojson::Geometry;
        use geojson::Value;

        fn visit_points(
            points: &[Vec<f64>],
            visitor: &mut impl Visitor,
            multi: bool,
        ) -> Result<(), GeozeroError> {
            visitor.visit_points_start(points.len())?;
            for p in points.iter() {
                let (x, y) = normalize_point(p)?;
                visitor.visit_point(x, y, true)?;
            }
            visitor.visit_points_end(multi)
        }

        fn accept_geom(
            visitor: &mut impl Visitor,
            geom: &Geometry,
            feature: Option<&Feature>,
        ) -> Result<(), GeozeroError> {
            let properties = feature.map(|f| f.properties.as_ref()).unwrap_or_default();
            let wrap_kind = |object_kind| {
                if feature.is_some() {
                    FeatureKind::Feature(object_kind)
                } else {
                    FeatureKind::Geometry(object_kind)
                }
            };

            match &geom.value {
                Value::Point(point) => {
                    let (x, y) = normalize_point(point)?;
                    visitor.visit_point(x, y, false)?;
                    visitor.visit_feature(properties)?;
                    visitor.finish(wrap_kind(ObjectKind::Point))
                }
                Value::MultiPoint(points) => {
                    visitor.visit_points_start(points.len())?;
                    for point in points {
                        let (x, y) = normalize_point(point)?;
                        visitor.visit_point(x, y, true)?;
                    }
                    visitor.visit_points_end(false)?;
                    visitor.visit_feature(properties)?;
                    visitor.finish(wrap_kind(ObjectKind::MultiPoint))
                }
                Value::LineString(line) => {
                    visit_points(line, visitor, false)?;
                    visitor.visit_feature(properties)?;
                    visitor.finish(wrap_kind(ObjectKind::LineString))
                }
                Value::MultiLineString(lines) => {
                    visitor.visit_lines_start(lines.len())?;
                    for line in lines.iter() {
                        visit_points(line, visitor, true)?;
                    }
                    visitor.visit_lines_end()?;
                    visitor.visit_feature(properties)?;
                    visitor.finish(wrap_kind(ObjectKind::MultiLineString))
                }
                Value::Polygon(polygon) => {
                    visitor.visit_polygon_start(polygon.len())?;
                    for ring in polygon {
                        visit_points(ring, visitor, true)?;
                    }
                    visitor.visit_polygon_end(false)?;
                    visitor.visit_feature(properties)?;
                    visitor.finish(wrap_kind(ObjectKind::Polygon))
                }
                Value::MultiPolygon(polygons) => {
                    visitor.visit_polygons_start(polygons.len())?;
                    for polygon in polygons {
                        visitor.visit_polygon_start(polygon.len())?;
                        for ring in polygon {
                            visit_points(ring, visitor, true)?;
                        }
                        visitor.visit_polygon_end(true)?;
                    }
                    visitor.visit_polygons_end()?;
                    visitor.visit_feature(properties)?;
                    visitor.finish(wrap_kind(ObjectKind::MultiPolygon))
                }
                Value::GeometryCollection(collection) => {
                    visitor.visit_collection_start(collection.len())?;
                    for geom in collection {
                        accept_geom(visitor, geom, None)?;
                    }
                    visitor.visit_collection_end()?;
                    visitor.visit_feature(properties)?;
                    visitor.finish(wrap_kind(ObjectKind::GeometryCollection))
                }
            }
        }

        let default_point = Geometry::new(Value::Point(vec![f64::NAN, f64::NAN]));

        match self {
            GeoJson::Geometry(geom) => accept_geom(visitor, geom, None),
            GeoJson::Feature(feature) => accept_geom(
                visitor,
                feature.geometry.as_ref().unwrap_or(&default_point),
                Some(feature),
            ),
            GeoJson::FeatureCollection(collection) => {
                visitor.visit_collection_start(collection.features.len())?;
                for featrue in collection {
                    accept_geom(
                        visitor,
                        featrue.geometry.as_ref().unwrap_or(&default_point),
                        Some(featrue),
                    )?;
                }
                visitor.visit_collection_end()?;
                visitor.finish(FeatureKind::FeatureCollection)
            }
        }
    }
}

fn normalize_point(point: &[f64]) -> Result<(f64, f64), GeozeroError> {
    if point.len() != 2 {
        Err(GeozeroError::Geometry(
            "coordinates higher than two dimensions are not supported".to_string(),
        ))
    } else {
        Ok((point[0], point[1]))
    }
}

impl<'a> GeometryRef<'a> {
    pub fn process_features<P>(&self, processor: &mut P) -> geozero::error::Result<()>
    where
        Self: Sized,
        P: geozero::FeatureProcessor,
    {
        let kind: FeatureKind = self.buf[0]
            .try_into()
            .map_err(|_| GeozeroError::Geometry("Invalid data".to_string()))?;

        match kind {
            FeatureKind::Geometry(_) => self.process_geom(processor),
            FeatureKind::Feature(_) => {
                let object = self.read_object()?;
                processor.feature_begin(0)?;
                processor.properties_begin()?;
                process_properties(&object.properties(), processor)?;
                processor.properties_end()?;
                processor.geometry_begin()?;
                self.process_geom(processor)?;
                processor.geometry_end()?;
                processor.feature_end(0)
            }
            FeatureKind::FeatureCollection => {
                processor.dataset_begin(None)?;
                for (idx, feature) in self.read_object()?.collection().unwrap().iter().enumerate() {
                    processor.feature_begin(idx as u64)?;
                    processor.properties_begin()?;
                    process_properties(&feature.properties(), processor)?;
                    processor.properties_end()?;
                    processor.geometry_begin()?;
                    self.process_inner(processor, &feature, 0)?;
                    processor.geometry_end()?;
                    processor.feature_end(idx as u64)?;
                }
                processor.dataset_end()
            }
        }
    }
}

pub(crate) type JsonObject = serde_json::Map<String, serde_json::Value>;

fn process_properties<P>(
    properties: &Option<Vector<u8>>,
    processor: &mut P,
) -> geozero::error::Result<()>
where
    P: geozero::FeatureProcessor,
{
    if let Some(data) = properties {
        let json: JsonObject = flexbuffers::from_slice(data.bytes())
            .map_err(|e| GeozeroError::Geometry(e.to_string()))?;
        for (idx, (name, value)) in json.iter().enumerate() {
            processor.property(
                idx,
                name,
                &ColumnValue::Json(
                    &serde_json::to_string(value)
                        .map_err(|e| GeozeroError::Geometry(e.to_string()))?,
                ),
            )?;
        }
    }
    Ok(())
}
