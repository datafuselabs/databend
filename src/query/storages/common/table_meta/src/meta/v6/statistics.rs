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

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ColumnStatistics {
    pub minmax: MinMax<Scalar>,

    pub null_count: u64,
    pub in_memory_size: u64,
    pub distinct_of_values: Option<u64>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ClusterStatistics {
    pub cluster_key_id: u32,
    pub minmax: MinMax<Vec<Scalar>>,
    pub level: i32,

    // currently it's only used in native engine
    pub pages: Option<Vec<Scalar>>,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq, Default)]
pub struct Statistics {
    pub row_count: u64,
    pub block_count: u64,
    pub perfect_block_count: u64,

    pub uncompressed_byte_size: u64,
    pub compressed_byte_size: u64,
    pub index_size: u64,

    pub col_stats: HashMap<ColumnId, ColumnStatistics>,
    pub cluster_stats: Option<ClusterStatistics>,
}

impl ColumnStatistics {
    pub fn new(
        min: Scalar,
        max: Scalar,
        null_count: u64,
        in_memory_size: u64,
        distinct_of_values: Option<u64>,
    ) -> Self {
        Self {
            minmax: MinMax::new(min, max),
            null_count,
            in_memory_size,
            distinct_of_values,
        }
    }

    pub fn min(&self) -> &Scalar {
        self.minmax.min()
    }

    pub fn max(&self) -> &Scalar {
        self.minmax.max()
    }

    pub fn from_v2(v2: &crate::meta::v2::statistics::ColumnStatistics) -> Self {
        Self {
            minmax: MinMax::new(v2.min.clone(), v2.max.clone()),
            null_count: v2.null_count,
            in_memory_size: v2.in_memory_size,
            distinct_of_values: v2.distinct_of_values,
        }
    }
}

impl ClusterStatistics {
    pub fn new(
        cluster_key_id: u32,
        min: Vec<Scalar>,
        max: Vec<Scalar>,
        level: i32,
        pages: Option<Vec<Scalar>>,
    ) -> Self {
        Self {
            cluster_key_id,
            minmax: MinMax::new(min, max),
            level,
            pages,
        }
    }

    pub fn min(&self) -> Vec<Scalar> {
        self.minmax.min().clone()
    }

    pub fn max(&self) -> Vec<Scalar> {
        self.minmax.max().clone()
    }

    pub fn is_const(&self) -> bool {
        matches!(self.minmax, MinMax::Point(_))
    }

    pub fn from_v2(v2: crate::meta::v2::statistics::ClusterStatistics) -> Self {
        Self {
            cluster_key_id: v2.cluster_key_id,
            minmax: MinMax::new(v2.min, v2.max),
            level: v2.level,
            pages: v2.pages,
        }
    }
}

impl Statistics {
    pub fn from_v2(v2: crate::meta::v2::statistics::Statistics) -> Self {
        let col_stats = v2
            .col_stats
            .into_iter()
            .map(|(k, v)| (k, ColumnStatistics::from_v2(&v)))
            .collect();
        let cluster_stats = v2.cluster_stats.map(ClusterStatistics::from_v2);
        Self {
            row_count: v2.row_count,
            block_count: v2.block_count,
            perfect_block_count: v2.perfect_block_count,
            uncompressed_byte_size: v2.uncompressed_byte_size,
            compressed_byte_size: v2.compressed_byte_size,
            index_size: v2.index_size,
            col_stats,
            cluster_stats,
        }
    }
}
