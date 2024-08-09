extern crate test;

use databend_common_geobuf::GeoJson;
use databend_common_geobuf::Geometry;
use geozero::CoordDimensions;
use geozero::ToGeo;
use geozero::ToWkb;
use test::Bencher;

#[bench]
fn bench_wkb_start_point(b: &mut Bencher) {
    let wkt = geozero::wkt::Wkt("LINESTRING(-124.2 42,-120.01 41.99,-122.5 42.01)");
    let wkb = geozero::wkb::Wkb(wkt.to_ewkb(CoordDimensions::xy(), None).unwrap());

    b.iter(|| {
        let geo = wkb.to_geo().unwrap();

        match geo {
            geo_types::Geometry::LineString(line) => {
                let point = geo_types::Point(line[0]);
                let _ = geo_types::Geometry::Point(point)
                    .to_ewkb(CoordDimensions::xy(), None)
                    .unwrap();
            }
            _ => unreachable!(),
        }
    });
}

#[bench]
fn bench_geobuf_start_point(b: &mut Bencher) {
    use databend_common_geobuf::Geometry;
    use databend_common_geobuf::Wkt;

    let geo = Geometry::try_from(Wkt("LINESTRING(-124.2 42,-120.01 41.99,-122.5 42.01)")).unwrap();
    let geo = geo.as_ref();

    b.iter(|| {
        let _ = geo.start_point().unwrap();
    })
}

#[bench]
fn bench_into_wkb_line(b: &mut Bencher) {
    let wkt = geozero::wkt::Wkt("LINESTRING(-124.2 42,-120.01 41.99,-122.5 42.01)");
    let geo = wkt.to_geo().unwrap();

    b.iter(|| {
        let _ = geo.to_ewkb(CoordDimensions::xy(), None).unwrap();
    })
}

#[bench]
fn bench_into_geobuf_line(b: &mut Bencher) {
    let wkt = geozero::wkt::Wkt("LINESTRING(-124.2 42,-120.01 41.99,-122.5 42.01)");
    let geo = wkt.to_geo().unwrap();

    b.iter(|| {
        let _ = Geometry::try_from(&geo).unwrap();
    })
}

#[bench]
fn bench_into_wkb_polygon(b: &mut Bencher) {
    let input = r#"{"type":"Polygon","coordinates":[[[-8.85498046875,41.86137915587359],[-9.832763671875,36.94111143010769],[-7.4267578125,37.19533058280065],[-6.1962890625,41.590796851056005],[-8.85498046875,41.86137915587359]],[[-9.28619384765625,38.69408504756833],[-9.10491943359375,38.57823196583313],[-8.92364501953125,38.69622870885282],[-9.13787841796875,38.831149809348744],[-9.28619384765625,38.69408504756833]]]}"#;
    let geo = geozero::geojson::GeoJson(input).to_geo().unwrap();

    b.iter(|| {
        let _ = geo.to_ewkb(CoordDimensions::xy(), None).unwrap();
    })
}

#[bench]
fn bench_into_geobuf_polygon(b: &mut Bencher) {
    let input = r#"{"type":"Polygon","coordinates":[[[-8.85498046875,41.86137915587359],[-9.832763671875,36.94111143010769],[-7.4267578125,37.19533058280065],[-6.1962890625,41.590796851056005],[-8.85498046875,41.86137915587359]],[[-9.28619384765625,38.69408504756833],[-9.10491943359375,38.57823196583313],[-8.92364501953125,38.69622870885282],[-9.13787841796875,38.831149809348744],[-9.28619384765625,38.69408504756833]]]}"#;
    let geo = geozero::geojson::GeoJson(input).to_geo().unwrap();

    b.iter(|| {
        let _ = Geometry::try_from(&geo).unwrap();
    })
}

#[bench]
fn bench_from_wkb_polygon(b: &mut Bencher) {
    let input = r#"{"type":"Polygon","coordinates":[[[-8.85498046875,41.86137915587359],[-9.832763671875,36.94111143010769],[-7.4267578125,37.19533058280065],[-6.1962890625,41.590796851056005],[-8.85498046875,41.86137915587359]],[[-9.28619384765625,38.69408504756833],[-9.10491943359375,38.57823196583313],[-8.92364501953125,38.69622870885282],[-9.13787841796875,38.831149809348744],[-9.28619384765625,38.69408504756833]]]}"#;
    let wkb = geozero::geojson::GeoJson(input)
        .to_ewkb(CoordDimensions::xy(), None)
        .unwrap();

    b.iter(|| {
        let _ = geozero::wkb::Wkb(&wkb).to_geo().unwrap();
    })
}

#[bench]
fn bench_from_geobuf_polygon(b: &mut Bencher) {
    let input = r#"{"type":"Polygon","coordinates":[[[-8.85498046875,41.86137915587359],[-9.832763671875,36.94111143010769],[-7.4267578125,37.19533058280065],[-6.1962890625,41.590796851056005],[-8.85498046875,41.86137915587359]],[[-9.28619384765625,38.69408504756833],[-9.10491943359375,38.57823196583313],[-8.92364501953125,38.69622870885282],[-9.13787841796875,38.831149809348744],[-9.28619384765625,38.69408504756833]]]}"#;
    let geo = Geometry::try_from(GeoJson(input)).unwrap();
    let geo = geo.as_ref();

    b.iter(|| {
        let _ = geo.to_geo().unwrap();
    })
}
