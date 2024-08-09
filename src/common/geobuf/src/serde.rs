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

use std::io;
use std::io::prelude::Read;
use std::io::prelude::Write;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_arrow::arrow::buffer::Buffer;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;

use crate::Geometry;
use crate::GeometryRef;

#[derive(Serialize, BorshSerialize)]
struct Ser<'a>(&'a [u8], &'a [f64], &'a [f64]);

#[derive(Deserialize, BorshDeserialize)]
struct De(Vec<u8>, Vec<f64>, Vec<f64>);

impl Serialize for GeometryRef<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where S: Serializer {
        Serialize::serialize(&Ser(self.buf, self.column_x, self.column_y), serializer)
    }
}

impl BorshSerialize for GeometryRef<'_> {
    fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        BorshSerialize::serialize(&Ser(self.buf, self.column_x, self.column_y), writer)
    }
}

impl<'de> Deserialize<'de> for Geometry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where D: Deserializer<'de> {
        let De(buf, x, y) = Deserialize::deserialize(deserializer)?;

        Ok(Geometry {
            buf,
            column_x: Buffer::from(x),
            column_y: Buffer::from(y),
        })
    }
}

impl BorshDeserialize for Geometry {
    fn deserialize_reader<R: Read>(reader: &mut R) -> io::Result<Self> {
        let De(buf, x, y) = BorshDeserialize::deserialize_reader(reader)?;

        Ok(Geometry {
            buf,
            column_x: Buffer::from(x),
            column_y: Buffer::from(y),
        })
    }
}
