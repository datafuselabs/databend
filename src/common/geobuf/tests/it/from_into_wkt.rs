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

use databend_common_geobuf::Ewkt;
use databend_common_geobuf::Geometry;
use databend_common_geobuf::Wkt;

#[test]
fn test_wkt() {
    run_wkt("POINT EMPTY");
    run_wkt("POINT(-122.35 37.55)");

    run_wkt("LINESTRING EMPTY");
    run_wkt("LINESTRING(-124.2 42,-120.01 41.99)");
    run_wkt("LINESTRING(-124.2 42,-120.01 41.99,-122.5 42.01)");

    run_wkt("POLYGON(EMPTY)");
    run_wkt("POLYGON((17 17,17 30,30 30,30 17,17 17))");
    run_wkt(
        "POLYGON((100 0,101 0,101 1,100 1,100 0),(100.8 0.8,100.8 0.2,100.2 0.2,100.2 0.8,100.8 0.8))",
    );

    run_wkt("MULTIPOINT EMPTY");
    run_wkt("MULTIPOINT(-122.35 37.55,0 -90)");

    run_wkt("MULTILINESTRING EMPTY");
    run_wkt("MULTILINESTRING((-124.2 42,-120.01 41.99,-122.5 42.01),(10 0,20 10,30 0))");
    run_wkt(
        "MULTILINESTRING((-124.2 42,-120.01 41.99),(-124.2 42,-120.01 41.99,-122.5 42.01,-122.5 42.01),(-124.2 42,-120.01 41.99,-122.5 42.01),(10 0,20 10,30 0))",
    );

    run_wkt("MULTIPOLYGON EMPTY");
    run_wkt("MULTIPOLYGON(((-10 0,0 10,10 0,-10 0)),((-10 40,10 40,0 20,-10 40)))");
    run_wkt("GEOMETRYCOLLECTION(POINT(99 11),LINESTRING(40 60,50 50,60 40),POINT(99 10))");

    run_wkt("GEOMETRYCOLLECTION EMPTY");
    run_wkt(
        "GEOMETRYCOLLECTION(POLYGON((-10 0,0 10,10 0,-10 0)),LINESTRING(40 60,50 50,60 40),POINT(99 11))",
    );
    run_wkt(
        "GEOMETRYCOLLECTION(POLYGON((-10 0,0 10,10 0,-10 0)),GEOMETRYCOLLECTION(LINESTRING(40 60,50 50,60 40),POINT(99 11)),POINT(50 70))",
    );
    run_wkt(
        "GEOMETRYCOLLECTION(POLYGON((-10 0,0 10,10 0,-10 0)),GEOMETRYCOLLECTION(LINESTRING(40 60,50 50,60 40),POINT(99 11)),POINT(50 70))",
    )
}

fn run_wkt(want: &str) {
    let geom = Geometry::try_from(Wkt(want)).unwrap();
    let Wkt(got) = (&geom).try_into().unwrap();

    assert_eq!(want, &got);
}

#[test]
fn test_ewkt() {
    // not support
    // run_ewkt("POINTZ(173 -40 20)","");
    run_ewkt("POINT EMPTY", "POINT EMPTY");
    run_ewkt(
        "srid= 123 ; POINT(-122.35 37.55)",
        "SRID=123;POINT(-122.35 37.55)",
    );
    run_ewkt(
        "SRID=123; GEOMETRYCOLLECTION(POINT(99 11),LINESTRING(40 60,50 50,60 40),POINT(99 10))",
        "SRID=123;GEOMETRYCOLLECTION(POINT(99 11),LINESTRING(40 60,50 50,60 40),POINT(99 10))",
    );
}

fn run_ewkt(input: &str, want: &str) {
    let geom = Geometry::try_from(Ewkt(input)).unwrap();
    let Ewkt(got) = (&geom).try_into().unwrap();

    assert_eq!(want, &got);
}
