// Copyright 2022 Datafuse Labs
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

use databend_common_geobuf::Geometry;
use databend_common_geobuf::Wkt;
use geozero::CoordDimensions;
use geozero::ToWkb;

#[test]
fn test_size() {
    // run_size("POINT EMPTY");
    // geo_buf_size:17, ewkb_size:21, points_size:16
    run_size("POINT(-122.35 37.55)");

    // geo_buf_size:1, ewkb_size:9, points_size:0
    run_size("LINESTRING EMPTY");
    // geo_buf_size:33, ewkb_size:41, points_size:32
    run_size("LINESTRING(-124.2 42,-120.01 41.99)");
    // geo_buf_size:49, ewkb_size:57, points_size:48
    run_size("LINESTRING(-124.2 42,-120.01 41.99,-122.5 42.01)");

    // geo_buf_size:1, ewkb_size:9, points_size:0
    run_size("POLYGON EMPTY");
    // geo_buf_size:113, ewkb_size:93, points_size:80
    run_size("POLYGON((17 17,17 30,30 30,30 17,17 17))");
    // geo_buf_size:197, ewkb_size:177, points_size:160
    run_size(
        "POLYGON((100 0,101 0,101 1,100 1,100 0),(100.8 0.8,100.8 0.2,100.2 0.2,100.2 0.8,100.8 0.8))",
    );

    // geo_buf_size:1, ewkb_size:9, points_size:0
    run_size("MULTIPOINT EMPTY");
    // geo_buf_size:33, ewkb_size:51, points_size:32
    run_size("MULTIPOINT(-122.35 37.55,0 -90)");

    // geo_buf_size:1, ewkb_size:9, points_size:0
    run_size("MULTILINESTRING EMPTY");
    // geo_buf_size:133, ewkb_size:123, points_size:96
    run_size("MULTILINESTRING((-124.2 42,-120.01 41.99,-122.5 42.01),(10 0,20 10,30 0))");
    // geo_buf_size:237, ewkb_size:237, points_size:192
    run_size(
        "MULTILINESTRING((-124.2 42,-120.01 41.99),(-124.2 42,-120.01 41.99,-122.5 42.01,-122.5 42.01),(-124.2 42,-120.01 41.99,-122.5 42.01),(10 0,20 10,30 0))",
    );

    // geo_buf_size:1, ewkb_size:9, points_size:0
    run_size("MULTIPOLYGON EMPTY");
    // geo_buf_size:189, ewkb_size:163, points_size:128
    run_size("MULTIPOLYGON(((-10 0,0 10,10 0,-10 0)),((-10 40,10 40,0 20,-10 40)))");

    // geo_buf_size:1, ewkb_size:9, points_size:0
    run_size("GEOMETRYCOLLECTION EMPTY");
    // geo_buf_size:201, ewkb_size:108, points_size:80
    run_size("GEOMETRYCOLLECTION(POINT(99 11),LINESTRING(40 60,50 50,60 40),POINT(99 10))");
    // geo_buf_size:273, ewkb_size:164, points_size:128
    run_size(
        "GEOMETRYCOLLECTION(POLYGON((-10 0,0 10,10 0,-10 0)),LINESTRING(40 60,50 50,60 40),POINT(99 11))",
    );
    // geo_buf_size:349, ewkb_size:194, points_size:144
    run_size(
        "GEOMETRYCOLLECTION(POLYGON((-10 0,0 10,10 0,-10 0)),GEOMETRYCOLLECTION(LINESTRING(40 60,50 50,60 40),POINT(99 11)),POINT(50 70))",
    );

    // todo SRID
}

fn run_size(want: &str) {
    let geom = Geometry::try_from(Wkt(want)).unwrap();
    let ewkb = geozero::wkt::Wkt(want)
        .to_ewkb(CoordDimensions::xy(), None)
        .unwrap();
    println!(
        "geo_buf_size:{}, ewkb_size:{}, points_size:{}",
        geom.as_ref().memory_size(),
        ewkb.len(),
        geom.as_ref().points_len() * 16
    );

    let Wkt(got) = (&geom).try_into().unwrap();
    assert_eq!(want, got)
}
