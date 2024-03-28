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

use std::sync::Arc;
use std::vec;

use bumpalo::Bump;
use databend_common_base::base::convert_byte_size;
use databend_common_base::base::convert_number_size;
use databend_common_base::runtime::GLOBAL_MEM_STAT;
use databend_common_catalog::plan::AggIndexMeta;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::HashTableConfig;
use databend_common_expression::PayloadFlushState;
use databend_common_expression::ProbeState;
use databend_common_functions::aggregates::StateAddr;
use databend_common_functions::aggregates::StateAddrs;
use databend_common_hashtable::HashtableEntryMutRefLike;
use databend_common_hashtable::HashtableLike;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;
use databend_common_pipeline_transforms::processors::AccumulatingTransformer;
use log::info;

use crate::pipelines::processors::transforms::aggregator::aggregate_cell::AggregateHashTableDropper;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::HashTableCell;
use crate::pipelines::processors::transforms::aggregator::PartitionedHashTableDropper;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::pipelines::processors::transforms::group_by::PartitionedHashMethod;
use crate::pipelines::processors::transforms::group_by::PolymorphicKeysHelper;
use crate::sessions::QueryContext;
#[allow(clippy::enum_variant_names)]
enum HashTable<Method: HashMethodBounds> {
    MovedOut,
    HashTable(HashTableCell<Method, usize>),
    AggregateHashTable(AggregateHashTable),
    PartitionedHashTable(HashTableCell<PartitionedHashMethod<Method>, usize>),
}

impl<Method: HashMethodBounds> Default for HashTable<Method> {
    fn default() -> Self {
        Self::MovedOut
    }
}

struct AggregateSettings {
    convert_threshold: usize,
    max_memory_usage: usize,
    spilling_bytes_threshold_per_proc: usize,
}

impl TryFrom<Arc<QueryContext>> for AggregateSettings {
    type Error = ErrorCode;

    fn try_from(ctx: Arc<QueryContext>) -> std::result::Result<Self, Self::Error> {
        let settings = ctx.get_settings();
        let max_threads = settings.get_max_threads()? as usize;
        let convert_threshold = settings.get_group_by_two_level_threshold()? as usize;
        let mut memory_ratio = settings.get_aggregate_spilling_memory_ratio()? as f64 / 100_f64;

        if memory_ratio > 1_f64 {
            memory_ratio = 1_f64;
        }

        let max_memory_usage = match settings.get_max_memory_usage()? {
            0 => usize::MAX,
            max_memory_usage => match memory_ratio {
                x if x == 0_f64 => usize::MAX,
                memory_ratio => (max_memory_usage as f64 * memory_ratio) as usize,
            },
        };

        Ok(AggregateSettings {
            convert_threshold,
            max_memory_usage,
            spilling_bytes_threshold_per_proc: match settings
                .get_aggregate_spilling_bytes_threshold_per_proc()?
            {
                0 => max_memory_usage / max_threads,
                spilling_bytes_threshold_per_proc => spilling_bytes_threshold_per_proc,
            },
        })
    }
}

// SELECT column_name, agg(xxx) FROM table_name GROUP BY column_name
pub struct TransformPartialAggregate<Method: HashMethodBounds> {
    method: Method,
    settings: AggregateSettings,
    hash_table: HashTable<Method>,
    probe_state: ProbeState,
    params: Arc<AggregatorParams>,
}

impl<Method: HashMethodBounds> TransformPartialAggregate<Method> {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        method: Method,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        params: Arc<AggregatorParams>,
        config: HashTableConfig,
    ) -> Result<Box<dyn Processor>> {
        let hash_table = if !params.enable_experimental_aggregate_hashtable {
            let arena = Arc::new(Bump::new());
            let hashtable = method.create_hash_table(arena)?;
            let _dropper = AggregateHashTableDropper::create(params.clone());
            let hashtable = HashTableCell::create(hashtable, _dropper);

            match !Method::SUPPORT_PARTITIONED || !params.has_distinct_combinator() {
                true => HashTable::HashTable(hashtable),
                false => HashTable::PartitionedHashTable(PartitionedHashMethod::convert_hashtable(
                    &method, hashtable,
                )?),
            }
        } else {
            let arena = Arc::new(Bump::new());
            HashTable::AggregateHashTable(AggregateHashTable::new(
                params.group_data_types.clone(),
                params.aggregate_functions.clone(),
                config,
                arena,
            ))
        };

        Ok(AccumulatingTransformer::create(
            input,
            output,
            TransformPartialAggregate::<Method> {
                method,
                params,
                hash_table,
                probe_state: ProbeState::default(),
                settings: AggregateSettings::try_from(ctx)?,
            },
        ))
    }

    // Block should be `convert_to_full`.
    #[inline(always)]
    fn aggregate_arguments(
        block: &DataBlock,
        params: &Arc<AggregatorParams>,
    ) -> Result<Vec<Vec<Column>>> {
        let aggregate_functions_arguments = &params.aggregate_functions_arguments;
        let mut aggregate_arguments_columns =
            Vec::with_capacity(aggregate_functions_arguments.len());
        for function_arguments in aggregate_functions_arguments {
            let mut function_arguments_column = Vec::with_capacity(function_arguments.len());

            for argument_index in function_arguments {
                // Unwrap safety: chunk has been `convert_to_full`.
                let argument_column = block
                    .get_by_offset(*argument_index)
                    .value
                    .as_column()
                    .unwrap();
                function_arguments_column.push(argument_column.clone());
            }

            aggregate_arguments_columns.push(function_arguments_column);
        }

        Ok(aggregate_arguments_columns)
    }

    #[inline(always)]
    #[allow(clippy::ptr_arg)] // &[StateAddr] slower than &StateAddrs ~20%
    fn execute(
        params: &Arc<AggregatorParams>,
        block: &DataBlock,
        places: &StateAddrs,
    ) -> Result<()> {
        let aggregate_functions = &params.aggregate_functions;
        let offsets_aggregate_states = &params.offsets_aggregate_states;
        let aggregate_arguments_columns = Self::aggregate_arguments(block, params)?;

        // This can beneficial for the case of dereferencing
        // This will help improve the performance ~hundreds of megabits per second
        let aggr_arg_columns_slice = &aggregate_arguments_columns;

        let rows = block.num_rows();
        for index in 0..aggregate_functions.len() {
            let function = &aggregate_functions[index];
            let state_offset = offsets_aggregate_states[index];
            let function_arguments = &aggr_arg_columns_slice[index];
            function.accumulate_keys(places, state_offset, function_arguments, rows)?;
        }

        Ok(())
    }

    #[inline(always)]
    #[allow(clippy::ptr_arg)] // &[StateAddr] slower than &StateAddrs ~20%
    fn execute_agg_index_block(&self, block: &DataBlock, places: &StateAddrs) -> Result<()> {
        let aggregate_functions = &self.params.aggregate_functions;
        let offsets_aggregate_states = &self.params.offsets_aggregate_states;

        for index in 0..aggregate_functions.len() {
            // Aggregation states are in the back of the block.
            let agg_index = block.num_columns() - aggregate_functions.len() + index;
            let function = &aggregate_functions[index];
            let offset = offsets_aggregate_states[index];
            let agg_state = block.get_by_offset(agg_index).value.as_column().unwrap();

            function.batch_merge(places, offset, agg_state)?;
        }

        Ok(())
    }

    fn execute_one_block(&mut self, block: DataBlock) -> Result<()> {
        let is_agg_index_block = block
            .get_meta()
            .and_then(AggIndexMeta::downcast_ref_from)
            .map(|index| index.is_agg)
            .unwrap_or_default();

        let block = block.convert_to_full();

        let group_columns = self
            .params
            .group_columns
            .iter()
            .map(|&index| block.get_by_offset(index))
            .map(|c| (c.value.as_column().unwrap().clone(), c.data_type.clone()))
            .collect::<Vec<_>>();

        unsafe {
            let rows_num = block.num_rows();

            match &mut self.hash_table {
                HashTable::MovedOut => unreachable!(),
                HashTable::HashTable(hashtable) => {
                    let state = self.method.build_keys_state(&group_columns, rows_num)?;
                    let mut places = Vec::with_capacity(rows_num);

                    for key in self.method.build_keys_iter(&state)? {
                        places.push(match hashtable.hashtable.insert_and_entry(key) {
                            Err(entry) => Into::<StateAddr>::into(*entry.get()),
                            Ok(mut entry) => {
                                let place = self.params.alloc_layout(&mut hashtable.arena);
                                *entry.get_mut() = place.addr();
                                place
                            }
                        })
                    }

                    if is_agg_index_block {
                        self.execute_agg_index_block(&block, &places)
                    } else {
                        Self::execute(&self.params, &block, &places)
                    }
                }
                HashTable::PartitionedHashTable(hashtable) => {
                    let state = self.method.build_keys_state(&group_columns, rows_num)?;
                    let mut places = Vec::with_capacity(rows_num);

                    for key in self.method.build_keys_iter(&state)? {
                        places.push(match hashtable.hashtable.insert_and_entry(key) {
                            Err(entry) => Into::<StateAddr>::into(*entry.get()),
                            Ok(mut entry) => {
                                let place = self.params.alloc_layout(&mut hashtable.arena);
                                *entry.get_mut() = place.addr();
                                place
                            }
                        })
                    }

                    if is_agg_index_block {
                        self.execute_agg_index_block(&block, &places)
                    } else {
                        Self::execute(&self.params, &block, &places)
                    }
                }
                HashTable::AggregateHashTable(hashtable) => {
                    let group_columns: Vec<Column> =
                        group_columns.into_iter().map(|c| c.0).collect();

                    let (params_columns, agg_states) = if is_agg_index_block {
                        (
                            vec![],
                            (0..self.params.aggregate_functions.len())
                                .map(|index| {
                                    block
                                        .get_by_offset(
                                            block.num_columns()
                                                - self.params.aggregate_functions.len()
                                                + index,
                                        )
                                        .value
                                        .as_column()
                                        .cloned()
                                        .unwrap()
                                })
                                .collect(),
                        )
                    } else {
                        (Self::aggregate_arguments(&block, &self.params)?, vec![])
                    };

                    let _ = hashtable.add_groups(
                        &mut self.probe_state,
                        &group_columns,
                        &params_columns,
                        &agg_states,
                        rows_num,
                    )?;
                    Ok(())
                }
            }
        }
    }
}

impl<Method: HashMethodBounds> AccumulatingTransform for TransformPartialAggregate<Method> {
    const NAME: &'static str = "TransformPartialAggregate";

    fn transform(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        self.execute_one_block(block)?;

        let is_new_agg = self.params.enable_experimental_aggregate_hashtable;
        #[allow(clippy::collapsible_if)]
        if Method::SUPPORT_PARTITIONED {
            if !is_new_agg
                && (matches!(&self.hash_table, HashTable::HashTable(cell)
                    if cell.len() >= self.settings.convert_threshold ||
                        cell.allocated_bytes() >= self.settings.spilling_bytes_threshold_per_proc ||
                        GLOBAL_MEM_STAT.get_memory_usage() as usize >= self.settings.max_memory_usage))
            {
                if let HashTable::HashTable(cell) = std::mem::take(&mut self.hash_table) {
                    self.hash_table = HashTable::PartitionedHashTable(
                        PartitionedHashMethod::convert_hashtable(&self.method, cell)?,
                    );
                }
            }

            if !is_new_agg
                && (matches!(&self.hash_table, HashTable::PartitionedHashTable(cell) if cell.allocated_bytes() > self.settings.spilling_bytes_threshold_per_proc)
                    || GLOBAL_MEM_STAT.get_memory_usage() as usize
                        >= self.settings.max_memory_usage)
            {
                if let HashTable::PartitionedHashTable(v) = std::mem::take(&mut self.hash_table) {
                    let _dropper = v._dropper.clone();
                    let blocks = vec![DataBlock::empty_with_meta(
                        AggregateMeta::<Method, usize>::create_spilling(v),
                    )];

                    let arena = Arc::new(Bump::new());
                    let method = PartitionedHashMethod::<Method>::create(self.method.clone());
                    let new_hashtable = method.create_hash_table(arena)?;
                    self.hash_table = HashTable::PartitionedHashTable(HashTableCell::create(
                        new_hashtable,
                        _dropper.unwrap(),
                    ));
                    return Ok(blocks);
                }

                unreachable!()
            }
        }

        if is_new_agg
            && (matches!(&self.hash_table, HashTable::AggregateHashTable(cell) if cell.allocated_bytes() > self.settings.spilling_bytes_threshold_per_proc
            || GLOBAL_MEM_STAT.get_memory_usage() as usize >= self.settings.max_memory_usage))
        {
            if let HashTable::AggregateHashTable(v) = std::mem::take(&mut self.hash_table) {
                let group_types = v.payload.group_types.clone();
                let aggrs = v.payload.aggrs.clone();
                v.config.update_current_max_radix_bits();
                let config = v
                    .config
                    .clone()
                    .with_initial_radix_bits(v.config.max_radix_bits);

                let mut state = PayloadFlushState::default();

                // repartition to max for normalization
                let partitioned_payload = v
                    .payload
                    .repartition(1 << config.max_radix_bits, &mut state);

                let blocks = vec![DataBlock::empty_with_meta(
                    AggregateMeta::<Method, usize>::create_agg_spilling(partitioned_payload),
                )];

                let arena = Arc::new(Bump::new());
                self.hash_table = HashTable::AggregateHashTable(AggregateHashTable::new(
                    group_types,
                    aggrs,
                    config,
                    arena,
                ));
                return Ok(blocks);
            }

            unreachable!()
        }

        Ok(vec![])
    }

    fn on_finish(&mut self, output: bool) -> Result<Vec<DataBlock>> {
        Ok(match std::mem::take(&mut self.hash_table) {
            HashTable::MovedOut => match !output && std::thread::panicking() {
                true => vec![],
                false => unreachable!(),
            },
            HashTable::HashTable(v) => match v.hashtable.len() == 0 {
                true => vec![],
                false => {
                    vec![DataBlock::empty_with_meta(
                        AggregateMeta::<Method, usize>::create_hashtable(-1, v),
                    )]
                }
            },
            HashTable::PartitionedHashTable(v) => {
                info!(
                    "Processed {} different keys, allocated {} memory while in group by.",
                    convert_number_size(v.len() as f64),
                    convert_byte_size(v.allocated_bytes() as f64)
                );

                let cells = PartitionedHashTableDropper::split_cell(v);
                let mut blocks = Vec::with_capacity(cells.len());
                for (bucket, cell) in cells.into_iter().enumerate() {
                    if cell.hashtable.len() != 0 {
                        blocks.push(DataBlock::empty_with_meta(
                            AggregateMeta::<Method, usize>::create_hashtable(bucket as isize, cell),
                        ));
                    }
                }

                blocks
            }
            HashTable::AggregateHashTable(hashtable) => {
                let partition_count = hashtable.payload.partition_count();
                let mut blocks = Vec::with_capacity(partition_count);
                for (bucket, payload) in hashtable.payload.payloads.into_iter().enumerate() {
                    blocks.push(DataBlock::empty_with_meta(
                        AggregateMeta::<Method, usize>::create_agg_payload(
                            bucket as isize,
                            payload,
                            partition_count,
                        ),
                    ));
                }

                blocks
            }
        })
    }
}
