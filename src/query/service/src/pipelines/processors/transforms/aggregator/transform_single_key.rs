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
use std::borrow::BorrowMut;
use std::sync::Arc;
use std::time::Instant;
use std::vec;

use bumpalo::Bump;
use databend_common_base::base::convert_byte_size;
use databend_common_base::base::convert_number_size;
use databend_common_catalog::plan::AggIndexMeta;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::InputColumns;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_functions::aggregates::AggregateFunctionRef;
use databend_common_functions::aggregates::StateAddr;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;
use databend_common_pipeline_transforms::processors::AccumulatingTransformer;

use crate::pipelines::processors::transforms::aggregator::AggregatorParams;

/// SELECT COUNT | SUM FROM table;
pub struct PartialSingleStateAggregator {
    #[allow(dead_code)]
    arena: Bump,
    places: Vec<StateAddr>,
    arg_indices: Vec<Vec<usize>>,
    funcs: Vec<AggregateFunctionRef>,

    start: Instant,
    first_block_start: Option<Instant>,
    rows: usize,
    bytes: usize,
}

impl PartialSingleStateAggregator {
    pub fn try_new(params: &Arc<AggregatorParams>) -> Result<Self> {
        assert!(!params.offsets_aggregate_states.is_empty());

        let arena = Bump::new();
        let layout = params
            .layout
            .ok_or_else(|| ErrorCode::LayoutError("layout shouldn't be None"))?;

        let place: StateAddr = arena.alloc_layout(layout).into();
        let temp_place: StateAddr = arena.alloc_layout(layout).into();
        let mut places = Vec::with_capacity(params.offsets_aggregate_states.len());

        for (idx, func) in params.aggregate_functions.iter().enumerate() {
            let arg_place = place.next(params.offsets_aggregate_states[idx]);
            func.init_state(arg_place);
            places.push(arg_place);

            let state_place = temp_place.next(params.offsets_aggregate_states[idx]);
            func.init_state(state_place);
        }

        Ok(PartialSingleStateAggregator {
            arena,
            places,
            funcs: params.aggregate_functions.clone(),
            arg_indices: params.aggregate_functions_arguments.clone(),
            start: Instant::now(),
            first_block_start: None,
            rows: 0,
            bytes: 0,
        })
    }
}

impl AccumulatingTransform for PartialSingleStateAggregator {
    const NAME: &'static str = "AggregatorPartialTransform";

    fn transform(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        if self.first_block_start.is_none() {
            self.first_block_start = Some(Instant::now());
        }

        let is_agg_index_block = block
            .get_meta()
            .and_then(AggIndexMeta::downcast_ref_from)
            .map(|index| index.is_agg)
            .unwrap_or_default();

        let block = block.consume_convert_to_full();

        for (idx, func) in self.funcs.iter().enumerate() {
            let place = self.places[idx];
            if is_agg_index_block {
                // Aggregation states are in the back of the block.
                let agg_index = block.num_columns() - self.funcs.len() + idx;
                let agg_state = block.get_by_offset(agg_index).value.as_column().unwrap();

                func.batch_merge_single(place, agg_state)?;
            } else {
                let columns =
                    InputColumns::new_block_proxy(self.arg_indices[idx].as_slice(), &block);
                func.accumulate(place, columns, None, block.num_rows())?;
            }
        }

        self.rows += block.num_rows();
        self.bytes += block.memory_size();

        Ok(vec![])
    }

    fn on_finish(&mut self, generate_data: bool) -> Result<Vec<DataBlock>> {
        let mut generate_data_block = vec![];

        if generate_data {
            let mut columns = Vec::with_capacity(self.funcs.len());

            for (idx, func) in self.funcs.iter().enumerate() {
                let place = self.places[idx];

                let mut data = Vec::with_capacity(4);
                func.serialize(place, &mut data)?;
                columns.push(BlockEntry::new(
                    DataType::Binary,
                    Value::Scalar(Scalar::Binary(data)),
                ));
            }

            generate_data_block = vec![DataBlock::new(columns, 1)];
        }

        // destroy states
        for (place, func) in self.places.iter().zip(self.funcs.iter()) {
            if func.need_manual_drop_state() {
                unsafe { func.drop_state(*place) }
            }
        }

        log::info!(
            "Aggregated {} to 1 rows in {} sec (real: {}). ({} rows/sec, {}/sec, {})",
            self.rows,
            self.start.elapsed().as_secs_f64(),
            if let Some(t) = &self.first_block_start {
                t.elapsed().as_secs_f64()
            } else {
                self.start.elapsed().as_secs_f64()
            },
            convert_number_size(self.rows as f64 / self.start.elapsed().as_secs_f64()),
            convert_byte_size(self.bytes as f64 / self.start.elapsed().as_secs_f64()),
            convert_byte_size(self.bytes as _),
        );

        Ok(generate_data_block)
    }
}

/// SELECT COUNT | SUM FROM table;
pub struct FinalSingleStateAggregator {
    arena: Bump,
    layout: Layout,
    to_merge_data: Vec<Vec<Column>>,
    funcs: Vec<AggregateFunctionRef>,
    offsets_aggregate_states: Vec<usize>,
}

impl FinalSingleStateAggregator {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        params: &Arc<AggregatorParams>,
    ) -> Result<Box<dyn Processor>> {
        assert!(!params.offsets_aggregate_states.is_empty());

        let arena = Bump::new();
        let layout = params
            .layout
            .ok_or_else(|| ErrorCode::LayoutError("layout shouldn't be None"))?;

        Ok(AccumulatingTransformer::create(
            input,
            output,
            FinalSingleStateAggregator {
                arena,
                layout,
                funcs: params.aggregate_functions.clone(),
                to_merge_data: vec![vec![]; params.aggregate_functions.len()],
                offsets_aggregate_states: params.offsets_aggregate_states.clone(),
            },
        ))
    }

    fn new_places(&self) -> Vec<StateAddr> {
        let place: StateAddr = self.arena.alloc_layout(self.layout).into();
        self.funcs
            .iter()
            .enumerate()
            .map(|(idx, func)| {
                let arg_place = place.next(self.offsets_aggregate_states[idx]);
                func.init_state(arg_place);
                arg_place
            })
            .collect()
    }
}

impl AccumulatingTransform for FinalSingleStateAggregator {
    const NAME: &'static str = "AggregatorFinalTransform";

    fn transform(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        if !block.is_empty() {
            let block = block.consume_convert_to_full();

            for (index, _) in self.funcs.iter().enumerate() {
                let binary_array = block.get_by_offset(index).value.as_column().unwrap();
                self.to_merge_data[index].push(binary_array.clone());
            }
        }

        Ok(vec![])
    }

    fn on_finish(&mut self, generate_data: bool) -> Result<Vec<DataBlock>> {
        let mut generate_data_block = vec![];

        if generate_data {
            let mut aggr_values = {
                let mut builders = vec![];
                for func in &self.funcs {
                    let data_type = func.return_type()?;
                    builders.push(ColumnBuilder::with_capacity(&data_type, 1));
                }
                builders
            };

            let main_places = self.new_places();
            for (index, func) in self.funcs.iter().enumerate() {
                let main_place = main_places[index];
                for col in self.to_merge_data[index].iter() {
                    func.batch_merge_single(main_place, col)?;
                }
                let array = aggr_values[index].borrow_mut();
                func.merge_result(main_place, array)?;
            }

            let mut columns = Vec::with_capacity(self.funcs.len());
            for builder in aggr_values {
                columns.push(builder.build());
            }

            // destroy states
            for (place, func) in main_places.iter().zip(self.funcs.iter()) {
                if func.need_manual_drop_state() {
                    unsafe { func.drop_state(*place) }
                }
            }

            generate_data_block = vec![DataBlock::new_from_columns(columns)];
        }

        Ok(generate_data_block)
    }
}
