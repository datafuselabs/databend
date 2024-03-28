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

use std::alloc::Layout;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_functions::aggregates::get_layout_offsets;
use databend_common_functions::aggregates::AggregateFunctionRef;
use databend_common_functions::aggregates::StateAddr;
use databend_common_sql::IndexType;
use itertools::Itertools;

use crate::pipelines::processors::transforms::group_by::Area;

pub struct AggregatorParams {
    pub input_schema: DataSchemaRef,
    pub group_columns: Vec<IndexType>,
    pub group_data_types: Vec<DataType>,

    pub aggregate_functions: Vec<AggregateFunctionRef>,
    pub aggregate_functions_arguments: Vec<Vec<usize>>,

    // about function state memory layout
    // If there is no aggregate function, layout is None
    pub layout: Option<Layout>,
    pub offsets_aggregate_states: Vec<usize>,

    pub enable_experimental_aggregate_hashtable: bool,
    pub in_cluster: bool,
    pub max_block_size: usize,
    // Limit is push down to AggregatorTransform
    pub limit: Option<usize>,
}

impl AggregatorParams {
    pub fn try_create(
        input_schema: DataSchemaRef,
        group_data_types: Vec<DataType>,
        group_columns: &[usize],
        agg_funcs: &[AggregateFunctionRef],
        agg_args: &[Vec<usize>],
        enable_experimental_aggregate_hashtable: bool,
        in_cluster: bool,
        max_block_size: usize,
        limit: Option<usize>,
    ) -> Result<Arc<AggregatorParams>> {
        let mut states_offsets: Vec<usize> = Vec::with_capacity(agg_funcs.len());
        let mut states_layout = None;
        if !agg_funcs.is_empty() {
            states_offsets = Vec::with_capacity(agg_funcs.len());
            states_layout = Some(get_layout_offsets(agg_funcs, &mut states_offsets)?);
        }

        Ok(Arc::new(AggregatorParams {
            input_schema,
            group_columns: group_columns.to_vec(),
            group_data_types,
            aggregate_functions: agg_funcs.to_vec(),
            aggregate_functions_arguments: agg_args.to_vec(),
            layout: states_layout,
            offsets_aggregate_states: states_offsets,
            enable_experimental_aggregate_hashtable,
            in_cluster,
            max_block_size,
            limit,
        }))
    }

    pub fn alloc_layout(&self, area: &mut Area) -> StateAddr {
        let layout = self.layout.unwrap();
        let place = Into::<StateAddr>::into(area.alloc_layout(layout));

        for idx in 0..self.offsets_aggregate_states.len() {
            let aggr_state = self.offsets_aggregate_states[idx];
            let aggr_state_place = place.next(aggr_state);
            self.aggregate_functions[idx].init_state(aggr_state_place);
        }
        place
    }

    pub fn has_distinct_combinator(&self) -> bool {
        self.aggregate_functions
            .iter()
            .any(|f| f.name().contains("DistinctCombinator"))
    }

    pub fn empty_result_block(&self) -> DataBlock {
        let columns = self
            .aggregate_functions
            .iter()
            .map(|f| ColumnBuilder::with_capacity(&f.return_type().unwrap(), 0).build())
            .chain(
                self.group_data_types
                    .iter()
                    .map(|t| ColumnBuilder::with_capacity(t, 0).build()),
            )
            .collect_vec();
        DataBlock::new_from_columns(columns)
    }
}
