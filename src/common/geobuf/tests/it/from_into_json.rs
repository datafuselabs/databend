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

use databend_common_geobuf::GeoJson;
use databend_common_geobuf::Geometry;
use serde_json::json;
use serde_json::Value;

#[test]
fn test_from_json() {
    // GeoJson not support POINT EMPTY
    // run_from_json(r#"{"type": "Point","coordinates":[]}"#);
    run_from_json(r#"{"type": "Point", "coordinates": [-122.35,37.55]}"#);

    run_from_json(r#"{"type": "LineString", "coordinates": []}"#);
    run_from_json(r#"{"type": "LineString", "coordinates": [[-124.2,42],[-120.01,41.99]]}"#);
    run_from_json(
        r#"{"type": "LineString", "coordinates": [[-124.2,42],[-120.01,41.99],[-122.5,42.01]]}"#,
    );

    run_from_json(r#"{"type": "Polygon", "coordinates": []}"#);
    run_from_json(
        r#"{"type": "Polygon", "coordinates": [[[17,17],[17,30],[30,30],[30,17],[17,17]]]}"#,
    );
    run_from_json(
        r#"{"type": "Polygon", "coordinates": [[[100,0],[101,0],[101,1],[100,1],[100,0]],[[100.8,0.8],[100.8,0.2],[100.2,0.2],[100.2,0.8],[100.8,0.8]]]}"#,
    );

    run_from_json(r#"{"type": "MultiPoint", "coordinates": []}"#);
    run_from_json(r#"{"type": "MultiPoint", "coordinates": [[-122.35,37.55],[0,-90]]}"#);

    run_from_json(r#"{"type": "MultiLineString", "coordinates": []}"#);
    run_from_json(
        r#"{"type": "MultiLineString", "coordinates": [[[-124.2,42],[-120.01,41.99],[-122.5,42.01]],[[10,0],[20,10],[30,0]]]}"#,
    );
    run_from_json(
        r#"{"type": "MultiLineString", "coordinates": [[[-124.2,42],[-120.01,41.99]],[[-124.2,42],[-120.01,41.99],[-122.5,42.01],[-122.5,42.01]],[[-124.2,42],[-120.01,41.99],[-122.5,42.01]],[[10,0],[20,10],[30,0]]]}"#,
    );

    run_from_json(r#"{"type": "MultiPolygon", "coordinates": []}"#);
    run_from_json(
        r#"{"type": "MultiPolygon", "coordinates": [[[[-10,0],[0,10],[10,0],[-10,0]]],[[[-10,40],[10,40],[0,20],[-10,40]]]]}"#,
    );

    run_from_json(r#"{"type": "GeometryCollection", "geometries": []}"#);
    run_from_json(
        r#"{"type": "GeometryCollection", "geometries": [{"type": "Point", "coordinates": [99,11]},{"type": "LineString", "coordinates": [[40,60],[50,50],[60,40]]},{"type": "Point", "coordinates": [99,10]}]}"#,
    );
    run_from_json(
        r#"{"type": "GeometryCollection", "geometries": [{"type": "Polygon", "coordinates": [[[-10,0],[0,10],[10,0],[-10,0]]]},{"type": "LineString", "coordinates": [[40,60],[50,50],[60,40]]},{"type": "Point", "coordinates": [99,11]}]}"#,
    );
    run_from_json(
        r#"{"type": "GeometryCollection", "geometries": [{"type": "Polygon", "coordinates": [[[-10,0],[0,10],[10,0],[-10,0]]]},{"type": "GeometryCollection", "geometries": [{"type": "LineString", "coordinates": [[40,60],[50,50],[60,40]]},{"type": "Point", "coordinates": [99,11]}]},{"type": "Point", "coordinates": [50,70]}]}"#,
    );
}

#[test]
fn test_from_feature() {
    run_from_json(
        r#"{"type": "Feature", "properties": {"name": "abc"}, "geometry": {"type": "Point", "coordinates": [173,-40]}}"#,
    );
    run_from_json(
        r#"{"type": "Feature", "properties": {"name": "abc"}, "geometry": {"type": "GeometryCollection", "geometries": [{"type": "Point", "coordinates": [99,11]},{"type": "LineString", "coordinates": [[40,60],[50,50],[60,40]]},{"type": "Point", "coordinates": [99,10]}]}}"#,
    );
    run_from_json(
        r#"{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"Point","coordinates":[173,-40]},"properties":{"name":"abc"}},{"type":"Feature","geometry":{"type":"Point","coordinates":[174,-41]},"properties":{"name":"def"}}]}"#,
    );
    run_from_json(r#"{"type":"FeatureCollection","features":[]}"#);

    // GeoJson not support Feature without geometry
    // run_from_json(
    //     r#"{"type": "Feature", "properties": {"arr": [1,2], "id": "NZL"}, "geometry":null }"#,
    // );
    // GeoJson not support Feature with POINT EMPTY
    // run_from_json(
    //     r#"{"type": "Feature", "properties": {"arr": [1,2], "id": "NZL"}, "geometry": {"type": "Point","coordinates":[]}}"#,
    // );
}

fn run_from_json(want: &str) {
    let geom = Geometry::try_from(GeoJson(want)).unwrap();
    let GeoJson(got) = (&geom).try_into().unwrap();

    let want: Value = serde_json::from_str(want).unwrap();
    let got: Value = serde_json::from_str(&got).unwrap();

    assert_eq!(want, got)
}

#[test]
fn test_json_member_discard() {
    let input = r#"{"type":"Feature","id":2,"aaa":"bbb","bbox":[-122.35,-90,0,37.55],"properties":{"id":"NZL","name":"New Zealand","o":[1,2]},"geometry":{"type":"MultiPoint","coordinates":[[-122.35,37.55],[0,-90]]}}"#;
    let geom = Geometry::try_from(GeoJson(input)).unwrap();
    let GeoJson(got) = (&geom).try_into().unwrap();
    let got: Value = serde_json::from_str(&got).unwrap();

    let want = json!({"type": "Feature", "properties": {"id": "NZL", "name": "New Zealand", "o": [1,2]}, "geometry": {"type": "MultiPoint", "coordinates": [[-122.35,37.55],[0,-90]]}});

    assert_eq!(want, got);
}
