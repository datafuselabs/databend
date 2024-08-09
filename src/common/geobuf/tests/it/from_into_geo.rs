// Copyright 2022 Datafuse Labs.
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

#[test]
fn test_geo() {
    // geo_types::Geometry not support POINT EMPTY
    // run_geo("POINT EMPTY");
    run_geo("POINT(-122.35 37.55)");

    run_geo("LINESTRING EMPTY");
    run_geo("LINESTRING(-124.2 42,-120.01 41.99)");
    run_geo("LINESTRING(-124.2 42,-120.01 41.99,-122.5 42.01)");

    run_geo("POLYGON(EMPTY)");
    run_geo("POLYGON((17 17,17 30,30 30,30 17,17 17))");
    run_geo(
        "POLYGON((100 0,101 0,101 1,100 1,100 0),(100.8 0.8,100.8 0.2,100.2 0.2,100.2 0.8,100.8 0.8))",
    );

    run_geo("MULTIPOINT EMPTY");
    run_geo("MULTIPOINT(-122.35 37.55,0 -90)");

    run_geo("MULTILINESTRING EMPTY");
    run_geo("MULTILINESTRING((-124.2 42,-120.01 41.99,-122.5 42.01),(10 0,20 10,30 0))");
    run_geo(
        "MULTILINESTRING((-124.2 42,-120.01 41.99),(-124.2 42,-120.01 41.99,-122.5 42.01,-122.5 42.01),(-124.2 42,-120.01 41.99,-122.5 42.01),(10 0,20 10,30 0))",
    );

    run_geo("MULTIPOLYGON EMPTY");
    run_geo("MULTIPOLYGON(((-10 0,0 10,10 0,-10 0)),((-10 40,10 40,0 20,-10 40)))");
    run_geo("GEOMETRYCOLLECTION(POINT(99 11),LINESTRING(40 60,50 50,60 40),POINT(99 10))");

    run_geo("GEOMETRYCOLLECTION EMPTY");
    run_geo(
        "GEOMETRYCOLLECTION(POLYGON((-10 0,0 10,10 0,-10 0)),LINESTRING(40 60,50 50,60 40),POINT(99 11))",
    );
    run_geo(
        "GEOMETRYCOLLECTION(POLYGON((-10 0,0 10,10 0,-10 0)),GEOMETRYCOLLECTION(LINESTRING(40 60,50 50,60 40),POINT(99 11)),POINT(50 70))",
    );
}

fn run_geo(want: &str) {
    use geozero::ToGeo;
    use geozero::ToWkt;
    let want_geo = geozero::wkt::Wkt(want).to_geo().unwrap();

    let geom = Geometry::try_from(&want_geo).unwrap();
    let got_geo: geo::Geometry<f64> = (&geom).try_into().unwrap();

    let got = got_geo.to_wkt().unwrap();
    assert_eq!(want, got)
}
