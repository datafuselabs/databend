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

use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::ColumnStatistics;

use crate::statistics::column_statistic;

pub struct BlockStatistics {
    // TODO rename to to num_rows
    pub block_rows_size: u64,
    pub block_bytes_size: u64,
    pub block_file_location: String,
    pub block_column_statistics: HashMap<ColumnId, ColumnStatistics>,
    pub block_cluster_statistics: Option<ClusterStatistics>,
}

impl BlockStatistics {
    pub fn from(
        data_block: &DataBlock,
        location: String,
        cluster_stats: Option<ClusterStatistics>,
        column_distinct_count: Option<HashMap<usize, usize>>,
        schema: &TableSchemaRef,
    ) -> databend_common_exception::Result<BlockStatistics> {
        Ok(BlockStatistics {
            block_file_location: location,
            block_rows_size: data_block.num_rows() as u64,
            block_bytes_size: data_block.memory_size() as u64,
            block_column_statistics: column_statistic::gen_columns_statistics(
                &[data_block.clone()],
                column_distinct_count,
                schema,
            )?,
            block_cluster_statistics: cluster_stats,
        })
    }
}
