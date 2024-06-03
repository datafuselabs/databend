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

use bytes::Bytes;
use opendal::Operator;
use orc_rust::reader::ChunkReader;

pub struct OrcFileReader {
    pub operator: Operator,
    pub path: String,
    pub size: u64,
}

impl ChunkReader for OrcFileReader {
    type T = &'static [u8];

    fn len(&self) -> u64 {
        self.size
    }

    fn get_read(&self, _offset_from_start: u64) -> std::io::Result<Self::T> {
        todo!()
    }

    fn get_bytes(&self, offset_from_start: u64, length: u64) -> std::io::Result<Bytes> {
        let buffer = self
            .operator
            .blocking()
            .read_with(&self.path)
            .range(offset_from_start..(offset_from_start + length))
            .call()?;
        Ok(buffer.to_bytes())
    }
}