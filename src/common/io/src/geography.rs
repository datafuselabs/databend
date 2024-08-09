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
use databend_common_exception::Result;
use databend_common_geobuf::Ewkb;
use databend_common_geobuf::Ewkt;
use databend_common_geobuf::GeoJson;
use databend_common_geobuf::Geometry;

pub fn parse_geometry(buf: &[u8]) -> Result<Geometry> {
    let geom = if let Ok(geom) = Geometry::try_from(Ewkb(&buf)) {
        geom
    } else {
        let str = std::str::from_utf8(buf).map_err(|e| ErrorCode::GeometryError(e.to_string()))?;

        if let Ok(geom) = Geometry::try_from(GeoJson(&str)) {
            geom
        } else if let Ok(geom) = Geometry::try_from(Ewkt(&str)) {
            geom
        } else {
            return Err(ErrorCode::GeometryError("can not parse geometry"));
        }
    };

    const WGS84: i32 = 4326;
    match geom.as_ref().srid() {
        None => {}
        Some(WGS84) => {}
        _ => {
            return Err(ErrorCode::GeometryError("supports only WGS84"));
        }
    }

    Ok(geom)
}
