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

use databend_common_geobuf::Geometry;
use databend_common_geobuf::Wkt;

#[test]
fn test_serde_serialize() {
    use serde::Deserialize;
    use serde::Serialize;

    let want = "POINT(-122.35 37.55)";
    let geo = Geometry::try_from(Wkt(want)).unwrap();

    let data = geo
        .as_ref()
        .serialize(serde_json::value::Serializer)
        .unwrap();

    let geo = Geometry::deserialize(data).unwrap();
    let Wkt(got) = (&geo).try_into().unwrap();

    assert_eq!(want, got)
}

#[test]
fn test_borsh_serialize() {
    use borsh::BorshDeserialize;
    use borsh::BorshSerialize;

    let want = "POINT(-122.35 37.55)";
    let geo = Geometry::try_from(Wkt(want)).unwrap();

    let mut buffer: Vec<u8> = Vec::new();

    geo.as_ref().serialize(&mut buffer).unwrap();

    let geo = Geometry::deserialize_reader(&mut buffer.as_slice()).unwrap();
    let Wkt(got) = (&geo).try_into().unwrap();

    assert_eq!(want, got)
}
