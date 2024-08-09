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

use databend_common_exception::ErrorCode;
use databend_common_expression::types::geometry::GeometryType;
use databend_common_expression::types::BinaryType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::NullableType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt32Type;
use databend_common_expression::types::VariantType;
use databend_common_expression::types::F64;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::vectorize_with_builder_3_arg;
use databend_common_expression::vectorize_with_builder_4_arg;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_io::geometry_format;
use databend_common_io::parse_to_ewkb;
use databend_common_io::parse_to_subtype;
use databend_common_io::Axis;
use databend_common_io::Extremum;
use databend_common_io::GeometryDataType;
use geo::dimensions::Dimensions;
use geo::BoundingRect;
use geo::Contains;
use geo::EuclideanDistance;
use geo::EuclideanLength;
use geo::HasDimensions;
use geo::HaversineDistance;
use geo::Point;
use geo::ToDegrees;
use geo::ToRadians;
use geo_types::coord;
use geo_types::Polygon;
use geohash::decode_bbox;
use geohash::encode;
use geos::geo_types;
use geos::geo_types::Coord;
use geos::geo_types::LineString;
use geos::Geometry;
use geozero::geojson::GeoJson;
use geozero::wkb::Ewkb;
use geozero::wkb::Wkb;
use geozero::CoordDimensions;
use geozero::GeozeroGeometry;
use geozero::ToGeo;
use geozero::ToGeos;
use geozero::ToJson;
use geozero::ToWkb;
use geozero::ToWkt;
use jsonb::parse_value;
use jsonb::to_string;
use num_traits::AsPrimitive;
use proj4rs::transform::transform;
use proj4rs::Proj;

pub fn register(registry: &mut FunctionRegistry) {
    // aliases
    registry.register_aliases("st_aswkb", &["st_asbinary"]);
    registry.register_aliases("st_aswkt", &["st_astext"]);
    registry.register_aliases("st_makegeompoint", &["st_geom_point"]);
    registry.register_aliases("st_makepolygon", &["st_polygon"]);
    registry.register_aliases("st_makeline", &["st_make_line"]);
    registry.register_aliases("st_npoints", &["st_numpoints"]);
    registry.register_aliases("st_geometryfromwkb", &[
        "st_geomfromwkb",
        "st_geometryfromewkb",
        "st_geomfromewkb",
    ]);
    registry.register_aliases("st_geometryfromwkt", &[
        "st_geomfromwkt",
        "st_geometryfromewkt",
        "st_geomfromewkt",
        "st_geometryfromtext",
        "st_geomfromtext",
    ]);

    // functions
    registry.register_passthrough_nullable_4_arg::<NumberType<F64>, NumberType<F64>, NumberType<F64>, NumberType<F64>, NumberType<F64>, _, _>(
        "haversine",
        |_, _, _, _, _| FunctionDomain::Full,
        vectorize_with_builder_4_arg::<NumberType<F64>, NumberType<F64>, NumberType<F64>, NumberType<F64>, NumberType<F64>,>(|lat1, lon1, lat2, lon2, builder, _| {
            let p1 = Point::new(lon1, lat1);
            let p2 = Point::new(lon2, lat2);
            let distance = p1.haversine_distance(&p2) * 0.001;
            builder.push(format!("{:.9}",distance.into_inner()).parse().unwrap());
        }),
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, VariantType, _, _>(
        "st_asgeojson",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, VariantType>(|geometry, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            let json = ewkb_to_json(geometry);
            match parse_value(json.unwrap().as_bytes()) {
                Ok(json) => {
                    json.write_to_vec(&mut builder.data);
                }
                Err(e) => {
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    );
                }
            };
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, BinaryType, _, _>(
        "st_asewkb",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, BinaryType>(|geometry, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }
            let ewkb = Ewkb(geometry);
            let srid = ewkb.to_geos().unwrap().srid();
            match Ewkb(geometry).to_ewkb(CoordDimensions::xy(), srid) {
                Ok(wkb) => builder.put_slice(wkb.as_slice()),
                Err(e) => {
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    );
                }
            };
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, BinaryType, _, _>(
        "st_aswkb",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, BinaryType>(|geometry, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match Ewkb(geometry).to_wkb(CoordDimensions::xy()) {
                Ok(wkb) => builder.put_slice(wkb.as_slice()),
                Err(e) => {
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    );
                }
            };
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, StringType, _, _>(
        "st_asewkt",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, StringType>(|geometry, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            let srid = Ewkb(geometry).to_geo().unwrap().srid();

            match Ewkb(geometry).to_ewkt(srid) {
                Ok(ewkt) => builder.put_str(&ewkt),
                Err(e) => ctx.set_error(
                    builder.len(),
                    ErrorCode::GeometryError(e.to_string()).to_string(),
                ),
            };
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, StringType, _, _>(
        "st_aswkt",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, StringType>(|geometry, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match Ewkb(geometry).to_wkt() {
                Ok(wkt) => builder.put_str(&wkt),
                Err(e) => ctx.set_error(
                    builder.len(),
                    ErrorCode::GeometryError(e.to_string()).to_string(),
                ),
            };
            builder.commit_row();
        }),
    );

    registry.register_combine_nullable_2_arg::<GeometryType, GeometryType, BooleanType, _, _>(
        "st_contains",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<GeometryType, GeometryType, NullableType<BooleanType>>(
            |l_geometry, r_geometry, builder, ctx| {
                let l_ewkb = Ewkb(l_geometry);
                let r_ewkb = Ewkb(r_geometry);
                let l_geos: Geometry = l_ewkb.to_geos().unwrap();
                let r_geos: Geometry = r_ewkb.to_geos().unwrap();
                let l_srid = l_geos.srid();
                let r_srid = r_geos.srid();
                if l_srid != r_srid {
                    builder.push_null();
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError("Srid does not match!").to_string(),
                    );
                } else {
                    let l_geo: geo::Geometry = l_geos.to_geo().unwrap();
                    let r_geo: geo::Geometry = r_geos.to_geo().unwrap();
                    if matches!(l_geo, geo::Geometry::GeometryCollection(_))
                        || matches!(r_geo, geo::Geometry::GeometryCollection(_))
                    {
                        builder.push_null();
                        ctx.set_error(
                            builder.len(),
                            ErrorCode::GeometryError(
                                "A GEOMETRY object that is a GeometryCollection",
                            )
                            .to_string(),
                        );
                    } else {
                        builder.push(l_geo.contains(&r_geo));
                    }
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<GeometryType, GeometryType, NumberType<F64>, _, _>(
        "st_distance",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<GeometryType, GeometryType, NullableType<NumberType<F64>>>(
            |l_geometry, r_geometry, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push_null();
                        return;
                    }
                }

                let left_geo = Ewkb(l_geometry);
                let right_geo = Ewkb(r_geometry);
                let geos = &vec![left_geo.to_geos().unwrap(), right_geo.to_geos().unwrap()];
                match get_shared_srid(geos).map_err(|e| ErrorCode::GeometryError(e).to_string()) {
                    Ok(_) => {}
                    Err(err_msg) => {
                        ctx.set_error(builder.len(), err_msg);
                        return;
                    }
                }

                let l_point = match <geo_types::Geometry as TryInto<Point>>::try_into(
                    Ewkb(l_geometry).to_geo().unwrap(),
                ) {
                    Ok(point) => point,
                    Err(e) => {
                        ctx.set_error(
                            builder.len(),
                            ErrorCode::GeometryError(e.to_string()).to_string(),
                        );
                        return;
                    }
                };

                let r_point = match <geo_types::Geometry as TryInto<Point>>::try_into(
                    Ewkb(r_geometry).to_geo().unwrap(),
                ) {
                    Ok(point) => point,
                    Err(e) => {
                        ctx.set_error(
                            builder.len(),
                            ErrorCode::GeometryError(e.to_string()).to_string(),
                        );
                        return;
                    }
                };
                let distance = l_point.euclidean_distance(&r_point);
                builder.push(format!("{:.9}", distance).parse().unwrap());
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, GeometryType, _, _>(
        "st_endpoint",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, GeometryType>(|geometry, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            let point = match <geo_types::Geometry as TryInto<LineString>>::try_into(
                Ewkb(geometry).to_geo().unwrap(),
            ) {
                Ok(line_string) => line_string.points().last().unwrap(),
                Err(e) => {
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    );
                    builder.commit_row();
                    return;
                }
            };

            match geo_types::Geometry::from(point).to_wkb(CoordDimensions::xy()) {
                Ok(binary) => builder.put_slice(binary.as_slice()),
                Err(e) => ctx.set_error(
                    builder.len(),
                    ErrorCode::GeometryError(e.to_string()).to_string(),
                ),
            };

            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_2_arg::<GeometryType, Int32Type, GeometryType, _, _>(
        "st_pointn",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<GeometryType, Int32Type, GeometryType>(
            |geometry, index, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.commit_row();
                        return;
                    }
                }

                let point = match <geo_types::Geometry as TryInto<LineString>>::try_into(
                    Ewkb(geometry).to_geo().unwrap(),
                ) {
                    Ok(line_string) => {
                        let len = line_string.0.len() as i32;
                        if index >= -len && index < len && index != 0 {
                            Point(
                                line_string.0
                                    [if index < 0 { len + index } else { index - 1 } as usize],
                            )
                        } else {
                            ctx.set_error(
                                builder.len(),
                                ErrorCode::GeometryError(format!(
                                    "Index { } is out of bounds",
                                    index
                                ))
                                .to_string(),
                            );
                            builder.commit_row();
                            return;
                        }
                    }
                    Err(e) => {
                        ctx.set_error(
                            builder.len(),
                            ErrorCode::GeometryError(e.to_string()).to_string(),
                        );
                        builder.commit_row();
                        return;
                    }
                };

                match geo_types::Geometry::from(point).to_wkb(CoordDimensions::xy()) {
                    Ok(binary) => builder.put_slice(binary.as_slice()),
                    Err(e) => ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    ),
                };

                builder.commit_row();
            },
        ),
    );

    registry.register_combine_nullable_1_arg::<GeometryType, Int32Type, _, _>(
        "st_dimension",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<GeometryType, NullableType<Int32Type>>(|ewkb, output, _| {
            let geo: geo_types::Geometry = Ewkb(ewkb).to_geo().unwrap();

            let dimension: Option<i32> = match geo.dimensions() {
                Dimensions::Empty => None,
                Dimensions::ZeroDimensional => Some(0),
                Dimensions::OneDimensional => Some(1),
                Dimensions::TwoDimensional => Some(2),
            };

            match dimension {
                Some(dimension) => output.push(dimension),
                None => output.push_null(),
            }
        }),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, GeometryType, _, _>(
        "st_geomfromgeohash",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<StringType, GeometryType>(|geohash, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            if geohash.len() > 12 {
                ctx.set_error(
                    builder.len(),
                    "Currently the precision only implement within 12 digits!",
                );
                builder.commit_row();
                return;
            }

            let geo: geo_types::Geometry = match decode_bbox(geohash) {
                Ok(rect) => rect.into(),
                Err(e) => {
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    );
                    builder.commit_row();
                    return;
                }
            };

            match geo.to_wkb(CoordDimensions::xy()) {
                Ok(binary) => builder.put_slice(binary.as_slice()),
                Err(e) => ctx.set_error(
                    builder.len(),
                    ErrorCode::GeometryError(e.to_string()).to_string(),
                ),
            };
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, GeometryType, _, _>(
        "st_geompointfromgeohash",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<StringType, GeometryType>(|geohash, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }
            if geohash.len() > 12 {
                ctx.set_error(
                    builder.len(),
                    ErrorCode::GeometryError(
                        "Currently the precision only implement within 12 digits!".to_string(),
                    )
                    .to_string(),
                );
                builder.commit_row();
                return;
            }

            let geo: geo_types::Geometry = match decode_bbox(geohash) {
                Ok(rect) => Point::from(rect.center()).into(),
                Err(e) => {
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    );
                    builder.commit_row();
                    return;
                }
            };

            match geo.to_wkb(CoordDimensions::xy()) {
                Ok(binary) => builder.put_slice(binary.as_slice()),
                Err(e) => ctx.set_error(
                    builder.len(),
                    ErrorCode::GeometryError(e.to_string()).to_string(),
                ),
            };
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_2_arg::<NumberType<F64>, NumberType<F64>, GeometryType, _, _>(
        "st_makegeompoint",
        |_,_, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<NumberType<F64>, NumberType<F64>, GeometryType>(|longitude, latitude, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }
            let geom = geo::Geometry::from(Point::new(longitude.0, latitude.0));
            builder.put_slice(geom.to_wkb(CoordDimensions::xy()).unwrap().as_slice());
            builder.commit_row();
        })
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, GeometryType, _, _>(
        "st_makepolygon",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, GeometryType>(|wkb, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            let polygon = Wkb(wkb)
                .to_geo()
                .unwrap()
                .try_into()
                .map_err(|e: geo_types::Error| ErrorCode::GeometryError(e.to_string()))
                .and_then(|line_string: LineString| {
                    let points = line_string.into_points();
                    if points.len() < 4 {
                        Err(ErrorCode::GeometryError(
                            "Input lines must have at least 4 points!",
                        ))
                    } else if points.last() != points.first() {
                        Err(ErrorCode::GeometryError(
                            "The first and last elements are not equal.",
                        ))
                    } else {
                        geo_types::Geometry::from(Polygon::new(LineString::from(points), vec![]))
                            .to_wkb(CoordDimensions::xy())
                            .map_err(|e| ErrorCode::GeometryError(e.to_string()))
                    }
                });

            match polygon {
                Ok(p) => builder.put_slice(p.as_slice()),
                Err(e) => ctx.set_error(
                    builder.len(),
                    ErrorCode::GeometryError(e.to_string()).to_string(),
                ),
            };
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_2_arg::<GeometryType, GeometryType, GeometryType, _, _>(
        "st_makeline",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<GeometryType, GeometryType, GeometryType>(
            |left_ewkb, right_ewkb, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.commit_row();
                        return;
                    }
                }
                let left_geo = Ewkb(left_ewkb);
                let right_geo = Ewkb(right_ewkb);
                let geos = &vec![left_geo.to_geos().unwrap(), right_geo.to_geos().unwrap()];
                // check srid
                let srid = match get_shared_srid(geos) {
                    Ok(srid) => srid,
                    Err(e) => {
                        ctx.set_error(builder.len(), ErrorCode::GeometryError(e).to_string());
                        builder.commit_row();
                        return;
                    }
                };

                let mut coords: Vec<Coord> = vec![];
                for geometry in geos.iter() {
                    let g : geo_types::Geometry = geometry.try_into().unwrap();
                    match g {
                        geo_types::Geometry::Point(point) => {
                            coords.push(point.0);
                        },
                        geo_types::Geometry::LineString(line)=> {
                            coords.append(&mut line.clone().into_inner());
                        },
                        geo_types::Geometry::MultiPoint(multi_point)=> {
                            for point in multi_point.into_iter() {
                                coords.push(point.0);
                            }
                        },
                        _ => {
                            ctx.set_error(
                                builder.len(),
                                ErrorCode::GeometryError("Geometry expression must be a Point, MultiPoint, or LineString.").to_string(),
                            );
                            builder.commit_row();
                            return;
                        }
                    }
                }
                let geom = geo::Geometry::from(LineString::new(coords));
                match geom.to_ewkb(CoordDimensions::xy(), srid) {
                    Ok(data) => builder.put_slice(data.as_slice()),
                    Err(e) => ctx.set_error(builder.len(), e.to_string()),
                }
                builder.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, StringType, _, _>(
        "st_geohash",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, StringType>(|geometry, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match point_to_geohash(geometry, None) {
                Ok(hash) => builder.put_str(&hash),
                Err(e) => {
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    );
                }
            };
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_2_arg::<GeometryType, Int32Type, StringType, _, _>(
        "st_geohash",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<GeometryType, Int32Type, StringType>(
            |geometry, precision, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.commit_row();
                        return;
                    }
                }

                if precision > 12 {
                    ctx.set_error(
                        builder.len(),
                        "Currently the precision only implement within 12 digits!",
                    );
                    builder.commit_row();
                    return;
                }

                match point_to_geohash(geometry, Some(precision)) {
                    Ok(hash) => builder.put_str(&hash),
                    Err(e) => {
                        ctx.set_error(
                            builder.len(),
                            ErrorCode::GeometryError(e.to_string()).to_string(),
                        );
                    }
                };
                builder.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, GeometryType, _, _>(
        "st_geometryfromwkb",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<StringType, GeometryType>(|str, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            let ewkb = match hex::decode(str) {
                Ok(binary) => Ewkb(binary),
                Err(e) => {
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    );
                    builder.commit_row();
                    return;
                }
            };
            let geos = match ewkb.to_geos() {
                Ok(geos) => geos,
                Err(e) => {
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    );
                    builder.commit_row();
                    return;
                }
            };

            match geos.to_ewkb(CoordDimensions::xy(), geos.srid()) {
                Ok(ewkb) => {
                    builder.put_slice(ewkb.as_slice());
                }
                Err(e) => {
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    );
                }
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<BinaryType, GeometryType, _, _>(
        "st_geometryfromwkb",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<BinaryType, GeometryType>(|binary, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }
            let ewkb = Ewkb(binary);
            let geos = match ewkb.to_geos() {
                Ok(geos) => geos,
                Err(e) => {
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    );
                    builder.commit_row();
                    return;
                }
            };

            match geos.to_ewkb(CoordDimensions::xy(), geos.srid()) {
                Ok(ewkb) => {
                    builder.put_slice(ewkb.as_slice());
                }
                Err(e) => {
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    );
                }
            }
            builder.commit_row();
        }),
    );
    registry.register_passthrough_nullable_2_arg::<StringType, Int32Type, GeometryType, _, _>(
        "st_geometryfromwkb",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<StringType, Int32Type, GeometryType>(
            |str, srid, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.commit_row();
                        return;
                    }
                }

                let binary = match hex::decode(str) {
                    Ok(binary) => binary,
                    Err(e) => {
                        ctx.set_error(
                            builder.len(),
                            ErrorCode::GeometryError(e.to_string()).to_string(),
                        );
                        builder.commit_row();
                        return;
                    }
                };

                match Ewkb(binary).to_ewkb(CoordDimensions::xy(), Some(srid)) {
                    Ok(ewkb) => {
                        builder.put_slice(ewkb.as_slice());
                    }
                    Err(e) => {
                        ctx.set_error(
                            builder.len(),
                            ErrorCode::GeometryError(e.to_string()).to_string(),
                        );
                    }
                }
                builder.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<BinaryType, Int32Type, GeometryType, _, _>(
        "st_geometryfromwkb",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<BinaryType, Int32Type, GeometryType>(
            |binary, srid, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.commit_row();
                        return;
                    }
                }
                let ewkb = Ewkb(binary);
                let geos = match ewkb.to_geos() {
                    Ok(geos) => geos,
                    Err(e) => {
                        ctx.set_error(
                            builder.len(),
                            ErrorCode::GeometryError(e.to_string()).to_string(),
                        );
                        builder.commit_row();
                        return;
                    }
                };

                match geos.to_ewkb(CoordDimensions::xy(), Some(srid)) {
                    Ok(ewkb) => {
                        builder.put_slice(ewkb.as_slice());
                    }
                    Err(e) => {
                        ctx.set_error(
                            builder.len(),
                            ErrorCode::GeometryError(e.to_string()).to_string(),
                        );
                    }
                }
                builder.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, GeometryType, _, _>(
        "st_geometryfromwkt",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<StringType, GeometryType>(|wkt, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match parse_to_ewkb(wkt.as_bytes(), None) {
                Ok(data) => builder.put_slice(data.as_slice()),
                Err(e) => ctx.set_error(
                    builder.len(),
                    ErrorCode::GeometryError(e.to_string()).to_string(),
                ),
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_2_arg::<StringType, Int32Type, GeometryType, _, _>(
        "st_geometryfromwkt",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<StringType, Int32Type, GeometryType>(
            |wkt, srid, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.commit_row();
                        return;
                    }
                }
                match parse_to_ewkb(wkt.as_bytes(), Some(srid)) {
                    Ok(data) => builder.put_slice(data.as_slice()),
                    Err(e) => ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    ),
                }
                builder.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, GeometryType, _, _>(
        "st_startpoint",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, GeometryType>(|geometry, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            let point = match <geo_types::Geometry as TryInto<LineString>>::try_into(
                Ewkb(geometry).to_geo().unwrap(),
            ) {
                Ok(line_string) => line_string.points().next().unwrap(),
                Err(e) => {
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    );
                    builder.commit_row();
                    return;
                }
            };

            match geo_types::Geometry::from(point).to_wkb(CoordDimensions::xy()) {
                Ok(binary) => builder.put_slice(binary.as_slice()),
                Err(e) => ctx.set_error(
                    builder.len(),
                    ErrorCode::GeometryError(e.to_string()).to_string(),
                ),
            };
            builder.commit_row();
        }),
    );

    registry.register_combine_nullable_1_arg::<GeometryType, NumberType<F64>, _, _>(
        "st_length",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, NullableType<NumberType<F64>>>(
            |geometry, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push_null();
                        return;
                    }
                }

                let g: geo_types::Geometry = Ewkb(geometry).to_geos().unwrap().try_into().unwrap();
                let mut distance = 0f64;
                match g {
                    geo_types::Geometry::LineString(lines) => {
                        for line in lines.lines() {
                            distance += line.euclidean_length();
                        }
                    }
                    geo_types::Geometry::MultiLineString(multi_lines) => {
                        for line_string in multi_lines.0 {
                            for line in line_string.lines() {
                                distance += line.euclidean_length();
                            }
                        }
                    }
                    geo_types::Geometry::GeometryCollection(geom_c) => {
                        for geometry in geom_c.0 {
                            if let geo::Geometry::LineString(line_string) = geometry {
                                for line in line_string.lines() {
                                    distance += line.euclidean_length();
                                }
                            }
                        }
                    }
                    _ => {}
                }

                builder.push(format!("{:.9}", distance).parse().unwrap());
            },
        ),
    );

    registry.register_combine_nullable_1_arg::<GeometryType, NumberType<F64>, _, _>(
        "st_x",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, NullableType<NumberType<F64>>>(
            |geometry, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push_null();
                        return;
                    }
                }

                match <geo_types::Geometry as TryInto<Point>>::try_into(
                    Ewkb(geometry).to_geo().unwrap(),
                ) {
                    Ok(point) => builder.push(F64::from(AsPrimitive::<f64>::as_(point.x()))),
                    Err(e) => ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    ),
                };
            },
        ),
    );

    registry.register_combine_nullable_1_arg::<GeometryType, NumberType<F64>, _, _>(
        "st_y",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, NullableType<NumberType<F64>>>(
            |geometry, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push_null();
                        return;
                    }
                }

                match <geo_types::Geometry as TryInto<Point>>::try_into(
                    Ewkb(geometry).to_geo().unwrap(),
                ) {
                    Ok(point) => builder.push(F64::from(AsPrimitive::<f64>::as_(point.y()))),
                    Err(e) => ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    ),
                };
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<GeometryType, Int32Type, GeometryType, _, _>(
        "st_setsrid",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<GeometryType, Int32Type, NullableType<GeometryType>>(
            |geometry, srid, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                let ewkb = Ewkb(geometry);
                let mut ggeom = ewkb.to_geos().unwrap();
                ggeom.set_srid(srid as usize);
                let geo = ggeom.to_ewkb(ggeom.dims(), ggeom.srid()).unwrap();
                output.push(&geo);
            },
        ),
    );

    registry.register_combine_nullable_1_arg::<GeometryType, Int32Type, _, _>(
        "st_srid",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<GeometryType, NullableType<Int32Type>>(
            |geometry, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }

                output.push(Ewkb(geometry).to_geos().unwrap().srid().unwrap_or(4326));
            },
        ),
    );

    registry.register_combine_nullable_1_arg::<GeometryType, NumberType<F64>, _, _>(
        "st_xmax",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<GeometryType, NullableType<NumberType<F64>>>(
            |geometry, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push_null();
                        return;
                    }
                }

                match st_extreme(&Ewkb(geometry).to_geo().unwrap(), Axis::X, Extremum::Max) {
                    None => builder.push_null(),
                    Some(x_max) => builder.push(F64::from(AsPrimitive::<f64>::as_(x_max))),
                };
            },
        ),
    );

    registry.register_combine_nullable_1_arg::<GeometryType, NumberType<F64>, _, _>(
        "st_xmin",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<GeometryType, NullableType<NumberType<F64>>>(
            |geometry, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push_null();
                        return;
                    }
                }
                match st_extreme(&Ewkb(geometry).to_geo().unwrap(), Axis::X, Extremum::Min) {
                    None => builder.push_null(),
                    Some(x_min) => builder.push(F64::from(AsPrimitive::<f64>::as_(x_min))),
                };
            },
        ),
    );

    registry.register_combine_nullable_1_arg::<GeometryType, NumberType<F64>, _, _>(
        "st_ymax",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<GeometryType, NullableType<NumberType<F64>>>(
            |geometry, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push_null();
                        return;
                    }
                }

                match st_extreme(&Ewkb(geometry).to_geo().unwrap(), Axis::Y, Extremum::Max) {
                    None => builder.push_null(),
                    Some(y_max) => builder.push(F64::from(AsPrimitive::<f64>::as_(y_max))),
                };
            },
        ),
    );

    registry.register_combine_nullable_1_arg::<GeometryType, NumberType<F64>, _, _>(
        "st_ymin",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<GeometryType, NullableType<NumberType<F64>>>(
            |geometry, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push_null();
                        return;
                    }
                }

                match st_extreme(&Ewkb(geometry).to_geo().unwrap(), Axis::Y, Extremum::Min) {
                    None => builder.push_null(),
                    Some(y_min) => builder.push(F64::from(AsPrimitive::<f64>::as_(y_min))),
                };
            },
        ),
    );

    registry.register_combine_nullable_1_arg::<GeometryType, UInt32Type, _, _>(
        "st_npoints",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<GeometryType, NullableType<UInt32Type>>(
            |geometry, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.push_null();
                        return;
                    }
                }
                builder.push(count_points(&Ewkb(geometry).to_geo().unwrap()) as u32);
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<GeometryType, StringType, _, _>(
        "to_string",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<GeometryType, StringType>(|b, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match geometry_format(Ewkb(b), ctx.func_ctx.geometry_output_format) {
                Ok(data) => builder.put_str(&data),
                Err(e) => {
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    );
                }
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<StringType, GeometryType, _, _>(
        "to_geometry",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<StringType, GeometryType>(|str, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }
            match str_to_geometry_impl(str, None) {
                Ok(data) => builder.put_slice(data.as_slice()),
                Err(e) => ctx.set_error(
                    builder.len(),
                    ErrorCode::GeometryError(e.to_string()).to_string(),
                ),
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_2_arg::<StringType, Int32Type, GeometryType, _, _>(
        "to_geometry",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<StringType, Int32Type, GeometryType>(
            |str, srid, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.commit_row();
                        return;
                    }
                }
                match str_to_geometry_impl(str, Some(srid)) {
                    Ok(data) => builder.put_slice(data.as_slice()),
                    Err(e) => ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    ),
                }
                builder.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<BinaryType, GeometryType, _, _>(
        "to_geometry",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<BinaryType, GeometryType>(|binary, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }
            let ewkb = Ewkb(binary);
            let geos = match ewkb.to_geos() {
                Ok(geos) => geos,
                Err(e) => {
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    );
                    builder.commit_row();
                    return;
                }
            };

            match geos.to_ewkb(CoordDimensions::xy(), geos.srid()) {
                Ok(ewkb) => builder.put_slice(ewkb.as_slice()),
                Err(e) => ctx.set_error(
                    builder.len(),
                    ErrorCode::GeometryError(e.to_string()).to_string(),
                ),
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_2_arg::<BinaryType, Int32Type, GeometryType, _, _>(
        "to_geometry",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<BinaryType, Int32Type, GeometryType>(
            |binary, srid, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.commit_row();
                        return;
                    }
                }

                let geo = match Ewkb(binary).to_geo() {
                    Ok(geo) => geo,
                    Err(e) => {
                        ctx.set_error(
                            builder.len(),
                            ErrorCode::GeometryError(e.to_string()).to_string(),
                        );
                        builder.commit_row();
                        return;
                    }
                };

                match geo.to_ewkb(CoordDimensions::xy(), Some(srid)) {
                    Ok(ewkb) => builder.put_slice(ewkb.as_slice()),
                    Err(e) => ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    ),
                };
                builder.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_1_arg::<VariantType, GeometryType, _, _>(
        "to_geometry",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<VariantType, GeometryType>(|json, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }
            match json_to_geometry_impl(json, None) {
                Ok(data) => builder.put_slice(data.as_slice()),
                Err(e) => ctx.set_error(
                    builder.len(),
                    ErrorCode::GeometryError(e.to_string()).to_string(),
                ),
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_2_arg::<VariantType, Int32Type, GeometryType, _, _>(
        "to_geometry",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<VariantType, Int32Type, GeometryType>(
            |json, srid, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.commit_row();
                        return;
                    }
                }
                match json_to_geometry_impl(json, Some(srid)) {
                    Ok(data) => builder.put_slice(data.as_slice()),
                    Err(e) => ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    ),
                }
                builder.commit_row();
            },
        ),
    );

    registry.register_combine_nullable_1_arg::<VariantType, GeometryType, _, _>(
        "try_to_geometry",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<VariantType, NullableType<GeometryType>>(
            |json, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match json_to_geometry_impl(json, None) {
                    Ok(data) => {
                        output.validity.push(true);
                        output.builder.put_slice(data.as_slice());
                        output.builder.commit_row();
                    }
                    Err(_) => output.push_null(),
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<VariantType, Int32Type, GeometryType, _, _>(
        "try_to_geometry",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<VariantType, Int32Type, NullableType<GeometryType>>(
            |json, srid, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match json_to_geometry_impl(json, Some(srid)) {
                    Ok(data) => {
                        output.validity.push(true);
                        output.builder.put_slice(data.as_slice());
                        output.builder.commit_row();
                    }
                    Err(_) => output.push_null(),
                }
            },
        ),
    );

    registry.register_combine_nullable_1_arg::<StringType, GeometryType, _, _>(
        "try_to_geometry",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<StringType, NullableType<GeometryType>>(
            |str, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match str_to_geometry_impl(str, None) {
                    Ok(data) => {
                        output.validity.push(true);
                        output.builder.put_slice(data.as_slice());
                        output.builder.commit_row();
                    }
                    Err(_) => output.push_null(),
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<StringType, Int32Type, GeometryType, _, _>(
        "try_to_geometry",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<StringType, Int32Type, NullableType<GeometryType>>(
            |str, srid, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                match str_to_geometry_impl(str, Some(srid)) {
                    Ok(data) => {
                        output.validity.push(true);
                        output.builder.put_slice(data.as_slice());
                        output.builder.commit_row();
                    }
                    Err(_) => output.push_null(),
                }
            },
        ),
    );

    registry.register_combine_nullable_1_arg::<BinaryType, GeometryType, _, _>(
        "try_to_geometry",
        |_, _| FunctionDomain::Full,
        vectorize_with_builder_1_arg::<BinaryType, NullableType<GeometryType>>(
            |binary, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                let ewkb = Ewkb(binary);
                let geos = match ewkb.to_geos() {
                    Ok(geos) => geos,
                    Err(_) => {
                        output.push_null();
                        return;
                    }
                };

                match geos.to_ewkb(CoordDimensions::xy(), geos.srid()) {
                    Ok(ewkb) => {
                        output.validity.push(true);
                        output.builder.put_slice(ewkb.as_slice());
                        output.builder.commit_row();
                    }
                    Err(_) => output.push_null(),
                }
            },
        ),
    );

    registry.register_combine_nullable_2_arg::<BinaryType, Int32Type, GeometryType, _, _>(
        "try_to_geometry",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<BinaryType, Int32Type, NullableType<GeometryType>>(
            |binary, srid, output, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(output.len()) {
                        output.push_null();
                        return;
                    }
                }
                let geo = match Ewkb(binary).to_geo() {
                    Ok(geo) => geo,
                    Err(_) => {
                        output.push_null();
                        return;
                    }
                };

                match geo.to_ewkb(CoordDimensions::xy(), Some(srid)) {
                    Ok(ewkb) => {
                        output.validity.push(true);
                        output.builder.put_slice(ewkb.as_slice());
                        output.builder.commit_row();
                    }
                    Err(_) => output.push_null(),
                }
            },
        ),
    );

    registry.register_passthrough_nullable_2_arg::<GeometryType, Int32Type, GeometryType, _, _>(
        "st_transform",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<GeometryType, Int32Type, GeometryType>(
            |original, to_srid, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.commit_row();
                        return;
                    }
                }

                // All representations of the geo types supported by crates under the GeoRust organization, have not implemented srid().
                // Currently, the srid() of all types returns the default value `None`, so we need to parse it manually here.
                let from_srid = match Ewkb(original).to_geos().unwrap().srid() {
                    Some(srid) => srid,
                    _ => {
                        ctx.set_error(
                            builder.len(),
                            ErrorCode::GeometryError("input geometry must has the correct SRID")
                                .to_string(),
                        );
                        builder.commit_row();
                        return;
                    }
                };

                match st_transform_impl(original, from_srid, to_srid) {
                    Ok(data) => {
                        builder.put_slice(data.as_slice());
                    }
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                    }
                }

                builder.commit_row();
            },
        ),
    );

    registry.register_passthrough_nullable_3_arg::<GeometryType, Int32Type, Int32Type, GeometryType, _, _>(
        "st_transform",
        |_, _, _,_| FunctionDomain::MayThrow,
        vectorize_with_builder_3_arg::<GeometryType, Int32Type, Int32Type, GeometryType>(
            |original, from_srid, to_srid, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.commit_row();
                        return;
                    }
                }

                match st_transform_impl(original, from_srid, to_srid) {
                    Ok(data) => {
                        builder.put_slice(data.as_slice());
                    }
                    Err(e) => {
                        ctx.set_error(builder.len(), e.to_string());
                    }
                }

                builder.commit_row();
            },
        ),
    );
}

fn st_transform_impl(
    original: &[u8],
    from_srid: i32,
    to_srid: i32,
) -> databend_common_exception::Result<Vec<u8>> {
    let from_proj = Proj::from_epsg_code(
        u16::try_from(from_srid).map_err(|_| ErrorCode::GeometryError("invalid from srid"))?,
    )
    .map_err(|_| ErrorCode::GeometryError("invalid from srid"))?;
    let to_proj = Proj::from_epsg_code(
        u16::try_from(to_srid).map_err(|_| ErrorCode::GeometryError("invalid to srid"))?,
    )
    .map_err(|_| ErrorCode::GeometryError("invalid to srid"))?;

    let old = Ewkb(original.to_vec());
    Ewkb(old.to_ewkb(old.dims(), Some(from_srid)).unwrap())
        .to_geo()
        .map_err(ErrorCode::from)
        .and_then(|mut geom| {
            // EPSG:4326 WGS84 in proj4rs is in radians, not degrees.
            if from_srid == 4326 {
                geom.to_radians_in_place();
            }
            transform(&from_proj, &to_proj, &mut geom).map_err(|_| {
                ErrorCode::GeometryError(format!(
                    "transform from {} to {} failed",
                    from_srid, to_srid
                ))
            })?;
            if to_srid == 4326 {
                geom.to_degrees_in_place();
            }
            let round_geom = round_geometry_coordinates(geom);
            round_geom
                .to_ewkb(round_geom.dims(), Some(to_srid))
                .map_err(ErrorCode::from)
        })
}

#[inline]
fn get_shared_srid(geometries: &Vec<Geometry>) -> Result<Option<i32>, String> {
    let mut srid: Option<i32> = None;
    let mut error_srid: String = String::new();
    let check_srid = geometries.windows(2).all(|w| {
        let v1 = w[0].srid();
        let v2 = w[1].srid();
        match v1.eq(&v2) {
            true => {
                srid = v1;
                true
            }
            false => {
                error_srid = "Srid does not match!".to_string();
                false
            }
        }
    });
    match check_srid {
        true => Ok(srid),
        false => Err(error_srid.clone()),
    }
}

pub fn ewkb_to_json(buf: &[u8]) -> databend_common_exception::Result<String> {
    Ewkb(buf)
        .to_geos()
        .map_err(|e| ErrorCode::GeometryError(e.to_string()))
        .and_then(|geos| {
            geos.to_json()
                .map_err(|e| ErrorCode::GeometryError(e.to_string()))
                .map(|json: String| json)
        })
}

/// The argument str must be a string expression that represents a valid geometric object in one of the following formats:
///
/// WKT (well-known text).
/// WKB (well-known binary) in hexadecimal format (without a leading 0x).
/// EWKT (extended well-known text).
/// EWKB (extended well-known binary) in hexadecimal format (without a leading 0x).
/// GEOJSON
fn str_to_geometry_impl(
    str: &str,
    srid: Option<i32>,
) -> databend_common_exception::Result<Vec<u8>> {
    let geo_type = match parse_to_subtype(str.as_bytes()) {
        Ok(geo_types) => geo_types,
        Err(e) => return Err(ErrorCode::GeometryError(e.to_string())),
    };
    let ewkb = match geo_type {
        GeometryDataType::WKT | GeometryDataType::EWKT => parse_to_ewkb(str.as_bytes(), srid),
        GeometryDataType::GEOJSON => GeoJson(str)
            .to_ewkb(CoordDimensions::xy(), srid)
            .map_err(|e| ErrorCode::GeometryError(e.to_string())),
        GeometryDataType::WKB | GeometryDataType::EWKB => {
            let ewkb = match hex::decode(str) {
                Ok(binary) => Ewkb(binary),
                Err(e) => return Err(ErrorCode::GeometryError(e.to_string())),
            };

            let geos = match ewkb.to_geos() {
                Ok(geos) => geos,
                Err(e) => return Err(ErrorCode::GeometryError(e.to_string())),
            };

            geos.to_ewkb(CoordDimensions::xy(), srid.or(geos.srid()))
                .map_err(|e| ErrorCode::GeometryError(e.to_string()))
        }
    };

    match ewkb {
        Ok(data) => Ok(data),
        Err(e) => Err(ErrorCode::GeometryError(e.to_string())),
    }
}

fn json_to_geometry_impl(
    binary: &[u8],
    srid: Option<i32>,
) -> databend_common_exception::Result<Vec<u8>> {
    let s = to_string(binary);
    let json = GeoJson(s.as_str());
    match json.to_ewkb(CoordDimensions::xy(), srid) {
        Ok(data) => Ok(data),
        Err(e) => Err(ErrorCode::GeometryError(e.to_string())),
    }
}

fn point_to_geohash(
    geometry: &[u8],
    precision: Option<i32>,
) -> databend_common_exception::Result<String> {
    let point = match Ewkb(geometry).to_geo() {
        Ok(geo) => Point::try_from(geo),
        Err(e) => return Err(ErrorCode::GeometryError(e.to_string())),
    };

    let hash = match point {
        Ok(point) => encode(point.0, precision.map_or(12, |p| p as usize)),
        Err(e) => return Err(ErrorCode::GeometryError(e.to_string())),
    };
    match hash {
        Ok(hash) => Ok(hash),
        Err(e) => Err(ErrorCode::GeometryError(e.to_string())),
    }
}

fn st_extreme(geometry: &geo_types::Geometry<f64>, axis: Axis, extremum: Extremum) -> Option<f64> {
    match geometry {
        geo_types::Geometry::Point(point) => {
            let coord = match axis {
                Axis::X => point.x(),
                Axis::Y => point.y(),
            };
            Some(coord)
        }
        geo_types::Geometry::MultiPoint(multi_point) => {
            let mut extreme_coord: Option<f64> = None;
            for point in multi_point {
                if let Some(coord) = st_extreme(&geo_types::Geometry::Point(*point), axis, extremum)
                {
                    extreme_coord = match extreme_coord {
                        Some(existing) => match extremum {
                            Extremum::Max => Some(existing.max(coord)),
                            Extremum::Min => Some(existing.min(coord)),
                        },
                        None => Some(coord),
                    };
                }
            }
            extreme_coord
        }
        geo_types::Geometry::Line(line) => {
            let bounding_rect = line.bounding_rect();
            let coord = match axis {
                Axis::X => match extremum {
                    Extremum::Max => bounding_rect.max().x,
                    Extremum::Min => bounding_rect.min().x,
                },
                Axis::Y => match extremum {
                    Extremum::Max => bounding_rect.max().y,
                    Extremum::Min => bounding_rect.min().y,
                },
            };
            Some(coord)
        }
        geo_types::Geometry::MultiLineString(multi_line) => {
            let mut extreme_coord: Option<f64> = None;
            for line in multi_line {
                if let Some(coord) = st_extreme(
                    &geo_types::Geometry::LineString(line.clone()),
                    axis,
                    extremum,
                ) {
                    extreme_coord = match extreme_coord {
                        Some(existing) => match extremum {
                            Extremum::Max => Some(existing.max(coord)),
                            Extremum::Min => Some(existing.min(coord)),
                        },
                        None => Some(coord),
                    };
                }
            }
            extreme_coord
        }
        geo_types::Geometry::Polygon(polygon) => {
            let bounding_rect = polygon.bounding_rect().unwrap();
            let coord = match axis {
                Axis::X => match extremum {
                    Extremum::Max => bounding_rect.max().x,
                    Extremum::Min => bounding_rect.min().x,
                },
                Axis::Y => match extremum {
                    Extremum::Max => bounding_rect.max().y,
                    Extremum::Min => bounding_rect.min().y,
                },
            };
            Some(coord)
        }
        geo_types::Geometry::MultiPolygon(multi_polygon) => {
            let mut extreme_coord: Option<f64> = None;
            for polygon in multi_polygon {
                if let Some(coord) = st_extreme(
                    &geo_types::Geometry::Polygon(polygon.clone()),
                    axis,
                    extremum,
                ) {
                    extreme_coord = match extreme_coord {
                        Some(existing) => match extremum {
                            Extremum::Max => Some(existing.max(coord)),
                            Extremum::Min => Some(existing.min(coord)),
                        },
                        None => Some(coord),
                    };
                }
            }
            extreme_coord
        }
        geo_types::Geometry::GeometryCollection(geometry_collection) => {
            let mut extreme_coord: Option<f64> = None;
            for geometry in geometry_collection {
                if let Some(coord) = st_extreme(geometry, axis, extremum) {
                    extreme_coord = match extreme_coord {
                        Some(existing) => match extremum {
                            Extremum::Max => Some(existing.max(coord)),
                            Extremum::Min => Some(existing.min(coord)),
                        },
                        None => Some(coord),
                    };
                }
            }
            extreme_coord
        }
        geo_types::Geometry::LineString(line_string) => {
            line_string.bounding_rect().map(|rect| match axis {
                Axis::X => match extremum {
                    Extremum::Max => rect.max().x,
                    Extremum::Min => rect.min().x,
                },
                Axis::Y => match extremum {
                    Extremum::Max => rect.max().y,
                    Extremum::Min => rect.min().y,
                },
            })
        }
        geo_types::Geometry::Rect(rect) => {
            let coord = match axis {
                Axis::X => match extremum {
                    Extremum::Max => rect.max().x,
                    Extremum::Min => rect.min().x,
                },
                Axis::Y => match extremum {
                    Extremum::Max => rect.max().y,
                    Extremum::Min => rect.min().y,
                },
            };
            Some(coord)
        }
        geo_types::Geometry::Triangle(triangle) => {
            let bounding_rect = triangle.bounding_rect();
            let coord = match axis {
                Axis::X => match extremum {
                    Extremum::Max => bounding_rect.max().x,
                    Extremum::Min => bounding_rect.min().x,
                },
                Axis::Y => match extremum {
                    Extremum::Max => bounding_rect.max().y,
                    Extremum::Min => bounding_rect.min().y,
                },
            };
            Some(coord)
        }
    }
}

fn count_points(geom: &geo_types::Geometry) -> usize {
    match geom {
        geo_types::Geometry::Point(_) => 1,
        geo_types::Geometry::Line(_) => 2,
        geo_types::Geometry::LineString(line_string) => line_string.0.len(),
        geo_types::Geometry::Polygon(polygon) => {
            polygon.exterior().0.len()
                + polygon
                    .interiors()
                    .iter()
                    .map(|line_string| line_string.0.len())
                    .sum::<usize>()
        }
        geo_types::Geometry::MultiPoint(multi_point) => multi_point.0.len(),
        geo_types::Geometry::MultiLineString(multi_line_string) => multi_line_string
            .0
            .iter()
            .map(|line_string| line_string.0.len())
            .sum::<usize>(),
        geo_types::Geometry::MultiPolygon(multi_polygon) => multi_polygon
            .0
            .iter()
            .map(|polygon| count_points(&geo_types::Geometry::Polygon(polygon.clone())))
            .sum::<usize>(),
        geo_types::Geometry::GeometryCollection(geometry_collection) => geometry_collection
            .0
            .iter()
            .map(count_points)
            .sum::<usize>(),
        geo_types::Geometry::Rect(_) => 5,
        geo_types::Geometry::Triangle(_) => 4,
    }
}

/// The last three decimal places of the f64 type are inconsistent between aarch64 and x86 platforms,
/// causing unit test results to fail. We will only retain six decimal places.
fn round_geometry_coordinates(geom: geo::Geometry<f64>) -> geo::Geometry<f64> {
    fn round_coordinate(coord: f64) -> f64 {
        (coord * 1_000_000.0).round() / 1_000_000.0
    }

    match geom {
        geo::Geometry::Point(point) => geo::Geometry::Point(Point::new(
            round_coordinate(point.x()),
            round_coordinate(point.y()),
        )),
        geo::Geometry::LineString(linestring) => geo::Geometry::LineString(
            linestring
                .into_iter()
                .map(|coord| coord!(x:round_coordinate(coord.x), y:round_coordinate(coord.y)))
                .collect(),
        ),
        geo::Geometry::Polygon(polygon) => {
            let outer_ring = polygon.exterior();
            let mut rounded_inner_rings = Vec::new();

            for inner_ring in polygon.interiors() {
                let rounded_coords: Vec<Coord<f64>> = inner_ring
                    .into_iter()
                    .map(
                        |coord| coord!( x: round_coordinate(coord.x), y: round_coordinate(coord.y)),
                    )
                    .collect();
                rounded_inner_rings.push(LineString(rounded_coords));
            }

            let rounded_polygon = Polygon::new(
                LineString(
                    outer_ring
                        .into_iter()
                        .map(|coord| coord!( x:round_coordinate(coord.x), y:round_coordinate(coord.y)))
                        .collect(),
                ),
                rounded_inner_rings,
            );

            geo::Geometry::Polygon(rounded_polygon)
        }
        geo::Geometry::MultiPoint(multipoint) => geo::Geometry::MultiPoint(
            multipoint
                .into_iter()
                .map(|point| Point::new(round_coordinate(point.x()), round_coordinate(point.y())))
                .collect(),
        ),
        geo::Geometry::MultiLineString(multilinestring) => {
            let rounded_lines: Vec<LineString<f64>> = multilinestring
                .into_iter()
                .map(|linestring| {
                    LineString(
                        linestring
                            .into_iter()
                            .map(|coord| coord!(x: round_coordinate(coord.x), y: round_coordinate(coord.y)))
                            .collect(),
                    )
                })
                .collect();

            geo::Geometry::MultiLineString(geo::MultiLineString::new(rounded_lines))
        }
        geo::Geometry::MultiPolygon(multipolygon) => {
            let rounded_polygons: Vec<Polygon<f64>> = multipolygon
                .into_iter()
                .map(|polygon| {
                    let outer_ring = polygon.exterior().into_iter()
                        .map(|coord| coord!( x:round_coordinate(coord.x), y:round_coordinate(coord.y)))
                        .collect::<Vec<Coord<f64>>>();

                    let mut rounded_inner_rings = Vec::new();
                    for inner_ring in polygon.interiors() {
                        let rounded_coords: Vec<Coord<f64>> = inner_ring
                            .into_iter()
                            .map(|coord| coord!( x:round_coordinate(coord.x), y: coord.y))
                            .collect();
                        rounded_inner_rings.push(LineString(rounded_coords));
                    }

                    Polygon::new(LineString(outer_ring), rounded_inner_rings)
                })
                .collect();
            geo::Geometry::MultiPolygon(geo::MultiPolygon::new(rounded_polygons))
        }
        geo::Geometry::GeometryCollection(geometrycollection) => geo::Geometry::GeometryCollection(
            geometrycollection
                .into_iter()
                .map(round_geometry_coordinates)
                .collect(),
        ),
        geo::Geometry::Line(line) => geo::Geometry::Line(geo::Line::new(
            Point::new(
                round_coordinate(line.start.x),
                round_coordinate(line.start.y),
            ),
            Point::new(round_coordinate(line.end.x), round_coordinate(line.end.y)),
        )),
        geo::Geometry::Rect(rect) => geo::Geometry::Rect(geo::Rect::new(
            Point::new(
                round_coordinate(rect.min().x),
                round_coordinate(rect.min().y),
            ),
            Point::new(
                round_coordinate(rect.max().x),
                round_coordinate(rect.max().y),
            ),
        )),
        geo::Geometry::Triangle(triangle) => geo::Geometry::Triangle(geo::Triangle::new(
            coord!(x: round_coordinate(triangle.0.x), y: round_coordinate(triangle.0.y)),
            coord!(x: round_coordinate(triangle.1.x), y: round_coordinate(triangle.1.y)),
            coord!(x: round_coordinate(triangle.2.x), y: round_coordinate(triangle.2.y)),
        )),
    }
}
