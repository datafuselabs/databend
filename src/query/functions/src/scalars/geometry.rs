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
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::F64;
use databend_common_expression::vectorize_with_builder_1_arg;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_io::parse_to_ewkb;
use geo::MultiPoint;
use geo::Point;
use geos::geo_types;
use geos::geo_types::Coord;
use geos::geo_types::LineString;
use geos::Geom;
use geos::Geometry;
use geozero::wkb::Ewkb;
use geozero::CoordDimensions;
use geozero::GeozeroGeometry;
use geozero::ToGeos;
use geozero::ToWkb;
use geozero::ToWkt;

// const GEO_TYPE_ID_MASK: u32 = 0x2000_0000;

pub fn register(registry: &mut FunctionRegistry) {
    // aliases
    registry.register_aliases("st_makegeompoint", &["st_geom_point"]);
    registry.register_aliases("st_makeline", &["st_make_line"]);
    registry.register_aliases("st_geometryfromwkb", &[
        "st_geomfromwkb",
        "st_geometryfromewkb",
        "st_geomfromewkb",
        "to_geometry",
    ]);
    registry.register_aliases("st_geometryfromwkt", &[
        "st_geomfromwkt",
        "st_geometryfromewkt",
        "st_geomfromewkt",
        "st_geometryfromtext",
        "st_geomfromtext",
        "to_geometry",
    ]);

    // functions
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
            match geom.to_ewkb(CoordDimensions::xy(), None) {
                Ok(data) => {
                    builder.put_slice(data.as_slice())
                },
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string())
                }
            }
            builder.commit_row();
        })
    );

    registry.register_passthrough_nullable_2_arg::<GeometryType, GeometryType, GeometryType, _, _>(
        "st_makeline",
        |_, _, _| FunctionDomain::Full,
        vectorize_with_builder_2_arg::<GeometryType, GeometryType, GeometryType>(
            |left_exp, right_exp, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.commit_row();
                        return;
                    }
                }
                let srid: Option<i32>;
                let params = &vec![left_exp, right_exp];
                let geos: Vec<Geometry> =
                    match binary_to_geos(params)
                    {
                        Ok(geos) => {
                                match get_shared_srid(&geos){
                                    Ok(s) => {
                                        srid = s;
                                        geos
                                    },
                                    Err(e) => {
                                        ctx.set_error(builder.len(), ErrorCode::GeometryError(e).to_string());
                                        builder.put_str("");
                                        return;
                                    }
                            }
                        },
                        Err(e) => {
                            ctx.set_error(builder.len(), ErrorCode::GeometryError(e.to_string()).to_string());
                            return builder.put_str("");
                        }
                    };

                let mut coords: Vec<Coord> = vec![];
                for geometry in geos.into_iter() {
                    let g : geo_types::Geometry = (&geometry).try_into().unwrap();
                    match g {
                        geo_types::Geometry::Point(_) => {
                            let point: Point = match g.try_into() {
                                Ok(point) => point,
                                Err(e) => {
                                    ctx.set_error(builder.len(), ErrorCode::GeometryError(e.to_string()).to_string());
                                    return builder.put_str("");
                                }
                            };
                            coords.push(point.into());
                        },
                        geo_types::Geometry::LineString(_)=> {
                            let line: LineString = match g.try_into() {
                                Ok(line) => line,
                                Err(e) => {
                                    ctx.set_error(builder.len(), e.to_string());
                                    return builder.put_str("");
                                }
                            };
                            coords.append(&mut line.into_inner());
                        },
                        geo_types::Geometry::MultiPoint(_)=> {
                            let multi_point: MultiPoint = match g.try_into() {
                                Ok(multi_point) => multi_point,
                                Err(e) => {
                                    ctx.set_error(builder.len(), e.to_string());
                                    return builder.put_str("");
                                }
                            };
                            for point in multi_point.into_iter() {
                                coords.push(point.into());
                            }
                        },
                        _ => {
                            ctx.set_error(
                                builder.len(),
                                ErrorCode::GeometryError("Geometry expression must be a Point, MultiPoint, or LineString.").to_string(),
                            );
                            return builder.put_str("");
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

    registry.register_passthrough_nullable_1_arg::<StringType, GeometryType, _, _>(
        "st_geometryfromwkb",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<StringType, GeometryType>(|ewkb, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            let b_ewkb = match hex::decode(ewkb) {
                Ok(b) => b,
                Err(e) => {
                    ctx.set_error(
                        builder.len(),
                        ErrorCode::GeometryError(e.to_string()).to_string(),
                    );
                    return builder.put_str("");
                }
            };
            match Ewkb(&b_ewkb).to_geos() {
                Ok(_) => builder.put_slice(b_ewkb.as_slice()),
                Err(e) => ctx.set_error(builder.len(), e.to_string()),
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_1_arg::<BinaryType, GeometryType, _, _>(
        "st_geometryfromwkb",
        |_, _| FunctionDomain::MayThrow,
        vectorize_with_builder_1_arg::<BinaryType, GeometryType>(|ewkb, builder, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(builder.len()) {
                    builder.commit_row();
                    return;
                }
            }

            match Ewkb(&ewkb).to_geos() {
                Ok(_) => builder.put_slice(ewkb),
                Err(e) => ctx.set_error(builder.len(), e.to_string()),
            }
            builder.commit_row();
        }),
    );

    registry.register_passthrough_nullable_2_arg::<StringType, Int32Type, GeometryType, _, _>(
        "st_geometryfromwkb",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<StringType, Int32Type, GeometryType>(
            |ewkb, srid, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.commit_row();
                        return;
                    }
                }

                let result = {
                    hex::decode(ewkb)
                        .map_err(|e| ErrorCode::from(e.to_string()))
                        .and_then(|binary| {
                            Ewkb(binary)
                                .to_geos()
                                .map_err(|e| ErrorCode::GeometryError(e.to_string()))
                                .and_then(|geos| {
                                    geos.to_ewkb(geos.dims(), Some(srid))
                                        .map_err(|e| ErrorCode::GeometryError(e.to_string()))
                                })
                        })
                };

                match result {
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

    registry.register_passthrough_nullable_2_arg::<BinaryType, Int32Type, GeometryType, _, _>(
        "st_geometryfromwkb",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<BinaryType, Int32Type, GeometryType>(
            |ewkb, srid, builder, ctx| {
                if let Some(validity) = &ctx.validity {
                    if !validity.get_bit(builder.len()) {
                        builder.commit_row();
                        return;
                    }
                }

                let result = {
                    Ewkb(ewkb)
                        .to_geos()
                        .map_err(|e| ErrorCode::GeometryError(e.to_string()))
                        .and_then(|geos| {
                            geos.to_ewkb(geos.dims(), Some(srid))
                                .map_err(|e| ErrorCode::GeometryError(e.to_string()))
                        })
                };

                match result {
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
                Err(e) => ctx.set_error(builder.len(), e.to_string()),
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
                    Err(e) => ctx.set_error(builder.len(), e.to_string()),
                }
                builder.commit_row();
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
            match Ewkb(b.to_vec()).to_ewkt(None) {
                Ok(data) => {
                    builder.put_str(&data);
                }
                Err(e) => {
                    ctx.set_error(builder.len(), e.to_string());
                }
            }
            builder.commit_row();
        }),
    );

    // registry.register_passthrough_nullable_2_arg::<GeometryType, Int32Type, GeometryType, _, _>(
    //     "st_transform",
    //     |_, _, _| FunctionDomain::MayThrow,
    //     vectorize_with_builder_2_arg::<GeometryType, Int32Type, GeometryType>(
    //         |original, srid, builder, ctx| {
    //             if let Some(validity) = &ctx.validity {
    //                 if !validity.get_bit(builder.len()) {
    //                     builder.commit_row();
    //                     return;
    //                 }
    //             }
    //
    //             #[allow(unused_assignments)]
    //             let mut from_srid = 0;
    //
    //             // All representations of the geo types supported by crates under the GeoRust organization, have not implemented srid().
    //             // Currently, the srid() of all types returns the default value `None`, so we need to parse it manually here.
    //             match read_ewkb_srid(&mut std::io::Cursor::new(original)) {
    //                 Ok(srid) if srid.is_some() => from_srid = srid.unwrap(),
    //                 _ => {
    //                     ctx.set_error(
    //                         builder.len(),
    //                         ErrorCode::GeometryError(" input geometry must has the correct SRID")
    //                             .to_string(),
    //                     );
    //                     builder.commit_row();
    //                     return;
    //                 }
    //             }
    //
    //             let result = {
    //                 Ewkb(original).to_geo().map_err(ErrorCode::from).and_then(
    //                     |mut geom: Geometry| {
    //                         Proj::new_known_crs(&make_crs(from_srid), &make_crs(srid), None)
    //                             .map_err(|e| ErrorCode::GeometryError(e.to_string()))
    //                             .and_then(|proj| {
    //                                 geom.transform(&proj)
    //                                     .map_err(|e| ErrorCode::GeometryError(e.to_string()))
    //                                     .and_then(|_| {
    //                                         geom.to_ewkb(geom.dims(), Some(srid))
    //                                             .map_err(ErrorCode::from)
    //                                     })
    //                             })
    //                     },
    //                 )
    //             };
    //
    //             match result {
    //                 Ok(data) => {
    //                     builder.put_slice(data.as_slice());
    //                 }
    //                 Err(e) => {
    //                     ctx.set_error(builder.len(), e.to_string());
    //                 }
    //             }
    //
    //             builder.commit_row();
    //         },
    //     ),
    // );
    //
    // registry.register_passthrough_nullable_3_arg::<GeometryType, Int32Type, Int32Type, GeometryType, _, _>(
    //     "st_transform",
    //     |_, _, _,_| FunctionDomain::MayThrow,
    //     vectorize_with_builder_3_arg::<GeometryType, Int32Type,Int32Type, GeometryType>(
    //         |original, from_srid, to_srid, builder, ctx| {
    //             if let Some(validity) = &ctx.validity {
    //                 if !validity.get_bit(builder.len()) {
    //                     builder.commit_row();
    //                     return;
    //                 }
    //             }
    //
    //             let result = {
    //                 Proj::new_known_crs(&make_crs(from_srid), &make_crs(to_srid), None)
    //                     .map_err(|e| ErrorCode::GeometryError(e.to_string()))
    //                     .and_then(|proj| {
    //                     let old = Ewkb(original.to_vec());
    //                     Ewkb(old.to_ewkb(old.dims(), Some(from_srid)).unwrap()).to_geo().map_err(ErrorCode::from).and_then(|mut geom| {
    //                         geom.transform(&proj).map_err(|e|ErrorCode::GeometryError(e.to_string())).and_then(|_| {
    //                             geom.to_ewkb(old.dims(), Some(to_srid)).map_err(ErrorCode::from)
    //                         })
    //                     })
    //                 })
    //             };
    //             match result {
    //                 Ok(data) => {
    //                     builder.put_slice(data.as_slice());
    //                 }
    //                 Err(e) => {
    //                     ctx.set_error(builder.len(), e.to_string());
    //                 }
    //             }
    //
    //             builder.commit_row();
    //         },
    //     ),
    // );
}

// fn make_crs(srid: i32) -> String {
//     format!("EPSG:{}", srid)
// }

// fn read_ewkb_srid<R: Read>(raw: &mut R) -> Result<Option<i32>> {
//     let byte_order = raw.ioread::<u8>()?;
//     let is_little_endian = byte_order != 0;
//     let endian = Endian::from(is_little_endian);
//     let type_id = raw.ioread_with::<u32>(endian)?;
//     let srid = if type_id & GEO_TYPE_ID_MASK == GEO_TYPE_ID_MASK {
//         Some(raw.ioread_with::<i32>(endian)?)
//     } else {
//         None
//     };
//
//     Ok(srid)
// }

#[inline]
fn binary_to_geos<'a>(binaries: &'a Vec<&'a [u8]>) -> Result<Vec<Geometry<'a>>, String> {
    let mut geos: Vec<Geometry> = Vec::with_capacity(binaries.len());
    let mut srid: Option<i32> = None;
    for (index, binary) in binaries.iter().enumerate() {
        match Geometry::new_from_wkb(binary) {
            Ok(data) => {
                if index == 0 {
                    srid = data.get_srid().map_or_else(|_| None, |v| Some(v as i32));
                } else {
                    let t_srid = data.get_srid().map_or_else(|_| None, |v| Some(v as i32));
                    if !srid.eq(&t_srid) {
                        return Err("Srid does not match!".to_string());
                    }
                }
                geos.push(data)
            }
            Err(e) => return Err(e.to_string()),
        };
    }
    Ok(geos)
}

#[inline]
fn get_shared_srid(geometries: &Vec<Geometry>) -> Result<Option<i32>, String> {
    let mut srid: Option<i32> = None;
    let mut error_srid: String = String::new();
    let check_srid = geometries.windows(2).all(|w| {
        let v1 = w[0].get_srid().map_or_else(|_| None, |v| Some(v as i32));
        let v2 = w[1].get_srid().map_or_else(|_| None, |v| Some(v as i32));
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
