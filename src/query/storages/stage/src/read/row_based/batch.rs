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

use std::intrinsics::unlikely;

use databend_common_expression::BlockMetaInfo;
use enum_as_inner::EnumAsInner;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Position {
    pub path: String,
    pub rows: usize,
    pub offset: usize,
}

impl Position {
    pub fn new(path: String) -> Self {
        Self {
            path,
            rows: 0,
            offset: 0,
        }
    }

    pub fn from_bytes_batch(batch: &BytesBatch, start_row_id: usize) -> Self {
        Self {
            path: batch.path.clone(),
            rows: start_row_id,
            offset: batch.offset,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BytesBatch {
    pub data: Vec<u8>,

    pub path: String,
    pub offset: usize,
    pub is_eof: bool,
}

impl BytesBatch {
    pub fn meta(&self) -> Self {
        Self {
            data: vec![],
            path: self.path.clone(),
            offset: self.offset,
            is_eof: self.is_eof,
        }
    }
}

#[typetag::serde(name = "raw_batch")]
impl BlockMetaInfo for BytesBatch {
    fn equals(&self, _info: &Box<dyn BlockMetaInfo>) -> bool {
        unreachable!("RawBatch as BlockMetaInfo is not expected to be compared.")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unreachable!("RawBatch as BlockMetaInfo is not expected to be cloned.")
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct RowBatchWithPosition {
    pub data: RowBatch,
    pub start_pos: Position,
}

impl RowBatchWithPosition {
    pub fn new(data: RowBatch, start_pos: Position) -> Self {
        Self { data, start_pos }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, EnumAsInner)]
pub enum RowBatch {
    Csv(CSVRowBatch),
    NDJson(NdjsonRowBatch),
}

impl RowBatch {
    pub fn rows(&self) -> usize {
        match self {
            RowBatch::Csv(b) => b.rows(),
            RowBatch::NDJson(b) => b.rows(),
        }
    }

    pub fn size(&self) -> usize {
        match self {
            RowBatch::Csv(b) => b.size(),
            RowBatch::NDJson(b) => b.size(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Default)]
pub struct CSVRowBatch {
    /// row[i] starts at row_ends[i-1] and ends at row_ends[i]
    /// has num_fields[i] fields
    /// field[j] starts at field_ends[i-1][j] and ends at field_ends[i-1][j]
    pub data: Vec<u8>,
    pub row_ends: Vec<usize>,
    pub field_ends: Vec<usize>,
    pub num_fields: Vec<usize>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Default)]
pub struct NdjsonRowBatch {
    // as the first row of this batch
    pub tail_of_last_batch: Option<Vec<u8>>,

    // designed to use Vec of BytesBatch without realloc
    // should ignore data[..start]
    pub data: Vec<u8>,
    pub start: usize,
    pub row_ends: Vec<usize>,
}
pub struct NdJsonRowBatchIter<'a> {
    first_row: &'a [u8],
    data: &'a [u8],
    row_ends: &'a [usize],
    end_index: i32,
    start: usize,
}

impl<'a> NdjsonRowBatch {
    pub fn iter(&'a self) -> NdJsonRowBatchIter<'a> {
        let (end_index, first_row) = if let Some(row) = &self.tail_of_last_batch {
            (-1, row)
        } else {
            (0, &self.data)
        };
        NdJsonRowBatchIter {
            first_row,
            end_index,
            data: &self.data,
            row_ends: &self.row_ends,
            start: self.start,
        }
    }
}

impl<'a> Iterator for NdJsonRowBatchIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        if unlikely(self.end_index < 0) {
            self.end_index = 0;
            Some(self.first_row)
        } else {
            let end_index = self.end_index as usize;
            if end_index >= self.row_ends.len() {
                None
            } else {
                let end = self.row_ends[end_index];
                let start = self.start;
                self.start = end;
                self.end_index += 1;
                Some(&self.data[start..end])
            }
        }
    }
}

impl NdjsonRowBatch {
    pub fn rows(&self) -> usize {
        self.row_ends.len()
            + if self.tail_of_last_batch.is_none() {
                0
            } else {
                1
            }
    }

    pub fn size(&self) -> usize {
        self.data.len()
            + self
                .tail_of_last_batch
                .as_ref()
                .map(|v| v.len())
                .unwrap_or(0)
            - self.start
    }
}

impl CSVRowBatch {
    pub fn rows(&self) -> usize {
        self.row_ends.len()
    }

    pub fn size(&self) -> usize {
        self.data.len()
    }
}

#[typetag::serde(name = "row_batch")]
impl BlockMetaInfo for RowBatchWithPosition {
    fn equals(&self, _info: &Box<dyn BlockMetaInfo>) -> bool {
        unreachable!("RowBatch as BlockMetaInfo is not expected to be compared.")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unreachable!("RowBatch as BlockMetaInfo is not expected to be cloned.")
    }
}
