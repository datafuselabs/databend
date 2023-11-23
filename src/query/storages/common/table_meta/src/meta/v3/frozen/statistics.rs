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

use std::collections::HashMap;

use common_expression::ColumnId;
use common_expression::Scalar;

use crate::meta::MinMax;

// the following types are supposed to be frozen

#[derive(serde::Serialize, serde::Deserialize)]
pub struct Statistics {
    pub row_count: u64,
    pub block_count: u64,
    pub perfect_block_count: u64,

    pub uncompressed_byte_size: u64,
    pub compressed_byte_size: u64,
    pub index_size: u64,

    pub col_stats: HashMap<ColumnId, ColumnStatistics>,
}

impl From<Statistics> for crate::meta::v6::Statistics {
    fn from(value: Statistics) -> Self {
        Self {
            row_count: value.row_count,
            block_count: value.block_count,
            perfect_block_count: value.perfect_block_count,
            uncompressed_byte_size: value.uncompressed_byte_size,
            compressed_byte_size: value.compressed_byte_size,
            index_size: value.index_size,
            col_stats: value
                .col_stats
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
            cluster_stats: None,
        }
    }
}

impl From<Statistics> for crate::meta::Statistics {
    fn from(value: Statistics) -> Self {
        Self {
            row_count: value.row_count,
            block_count: value.block_count,
            perfect_block_count: value.perfect_block_count,
            uncompressed_byte_size: value.uncompressed_byte_size,
            compressed_byte_size: value.compressed_byte_size,
            index_size: value.index_size,
            col_stats: value
                .col_stats
                .into_iter()
                .map(|(k, v)| (k, v.into()))
                .collect(),
            cluster_stats: None,
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct ColumnStatistics {
    pub min: Scalar,
    pub max: Scalar,

    pub null_count: u64,
    pub in_memory_size: u64,
    pub distinct_of_values: Option<u64>,
}

impl From<ColumnStatistics> for crate::meta::ColumnStatistics {
    fn from(value: ColumnStatistics) -> Self {
        Self {
            min: value.min,
            max: value.max,
            null_count: value.null_count,
            in_memory_size: value.in_memory_size,
            distinct_of_values: value.distinct_of_values,
        }
    }
}

impl From<ColumnStatistics> for crate::meta::v6::ColumnStatistics {
    fn from(value: ColumnStatistics) -> Self {
        Self {
            minmax: MinMax::new(value.min, value.max),
            null_count: value.null_count,
            in_memory_size: value.in_memory_size,
            distinct_of_values: value.distinct_of_values,
        }
    }
}
