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

use databend_common_geobuf::Ewkb;
use databend_common_geobuf::Geometry;
use databend_common_geobuf::Wkb;
use geozero::CoordDimensions;
use geozero::ToWkb;

#[test]
fn test_wkb() {
    // not support POINT EMPTY
    // run_wkb("POINT EMPTY");
    run_wkb("POINT(-122.35 37.55)");

    run_wkb("LINESTRING EMPTY");
    run_wkb("LINESTRING(-124.2 42,-120.01 41.99)");
    run_wkb("LINESTRING(-124.2 42,-120.01 41.99,-122.5 42.01)");

    run_wkb("POLYGON(EMPTY)");
    run_wkb("POLYGON((17 17,17 30,30 30,30 17,17 17))");
    run_wkb(
        "POLYGON((100 0,101 0,101 1,100 1,100 0),(100.8 0.8,100.8 0.2,100.2 0.2,100.2 0.8,100.8 0.8))",
    );

    run_wkb("MULTIPOINT EMPTY");
    run_wkb("MULTIPOINT(-122.35 37.55,0 -90)");

    run_wkb("MULTILINESTRING EMPTY");
    run_wkb("MULTILINESTRING((-124.2 42,-120.01 41.99,-122.5 42.01),(10 0,20 10,30 0))");
    run_wkb(
        "MULTILINESTRING((-124.2 42,-120.01 41.99),(-124.2 42,-120.01 41.99,-122.5 42.01,-122.5 42.01),(-124.2 42,-120.01 41.99,-122.5 42.01),(10 0,20 10,30 0))",
    );

    run_wkb("MULTIPOLYGON EMPTY");
    run_wkb("MULTIPOLYGON(((-10 0,0 10,10 0,-10 0)),((-10 40,10 40,0 20,-10 40)))");
    run_wkb("GEOMETRYCOLLECTION(POINT(99 11),LINESTRING(40 60,50 50,60 40),POINT(99 10))");

    run_wkb("GEOMETRYCOLLECTION EMPTY");
    run_wkb(
        "GEOMETRYCOLLECTION(POLYGON((-10 0,0 10,10 0,-10 0)),LINESTRING(40 60,50 50,60 40),POINT(99 11))",
    );
    run_wkb(
        "GEOMETRYCOLLECTION(POLYGON((-10 0,0 10,10 0,-10 0)),GEOMETRYCOLLECTION(LINESTRING(40 60,50 50,60 40),POINT(99 11)),POINT(50 70))",
    );
    run_wkb(
        "GEOMETRYCOLLECTION(POLYGON((-10 0,0 10,10 0,-10 0)),GEOMETRYCOLLECTION(LINESTRING(40 60,50 50,60 40),POINT(99 11)),POINT(50 70))",
    )
}

fn run_wkb(want: &str) {
    let want = geozero::wkt::Wkt(want)
        .to_wkb(CoordDimensions::xy())
        .unwrap();

    let geom = Geometry::try_from(Wkb(&want)).unwrap();
    let Wkb(got) = (&geom).try_into().unwrap();

    assert_eq!(&want, &got);
}

#[test]
fn test_ewkb() {
    // not support
    // test_ewkb("POINTZ(173 -40 20)");
    run_ewkb("POINT(-122.35 37.55)");
    run_ewkb("GEOMETRYCOLLECTION(POINT(99 11),LINESTRING(40 60,50 50,60 40),POINT(99 10))");
}

fn run_ewkb(want: &str) {
    let want = geozero::wkt::Wkt(want)
        .to_ewkb(CoordDimensions::xy(), Some(123))
        .unwrap();

    let geom = Geometry::try_from(Ewkb(&want)).unwrap();
    let Ewkb(got) = (&geom).try_into().unwrap();

    assert_eq!(&want, &got);
}
