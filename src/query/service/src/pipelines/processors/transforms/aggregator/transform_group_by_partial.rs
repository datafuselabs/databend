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
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::HashTableConfig;
use databend_common_expression::PayloadFlushState;
use databend_common_expression::ProbeState;
use databend_common_hashtable::HashtableLike;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;
use databend_common_pipeline_transforms::processors::AccumulatingTransformer;
use log::info;

use crate::pipelines::processors::transforms::aggregator::aggregate_cell::GroupByHashTableDropper;
use crate::pipelines::processors::transforms::aggregator::aggregate_cell::HashTableCell;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::PartitionedHashTableDropper;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::pipelines::processors::transforms::group_by::PartitionedHashMethod;
use crate::pipelines::processors::transforms::group_by::PolymorphicKeysHelper;
use crate::sessions::QueryContext;

#[allow(clippy::enum_variant_names)]
enum HashTable<Method: HashMethodBounds> {
    MovedOut,
    HashTable(HashTableCell<Method, ()>),
    AggregateHashTable(AggregateHashTable),
    PartitionedHashTable(HashTableCell<PartitionedHashMethod<Method>, ()>),
}

impl<Method: HashMethodBounds> Default for HashTable<Method> {
    fn default() -> Self {
        Self::MovedOut
    }
}

struct GroupBySettings {
    convert_threshold: usize,
    max_memory_usage: usize,
    spilling_bytes_threshold_per_proc: usize,
}

impl TryFrom<Arc<QueryContext>> for GroupBySettings {
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

        Ok(GroupBySettings {
            max_memory_usage,
            convert_threshold,
            spilling_bytes_threshold_per_proc: match settings
                .get_aggregate_spilling_bytes_threshold_per_proc()?
            {
                0 => max_memory_usage / max_threads,
                spilling_bytes_threshold_per_proc => spilling_bytes_threshold_per_proc,
            },
        })
    }
}

// SELECT column_name FROM table_name GROUP BY column_name
pub struct TransformPartialGroupBy<Method: HashMethodBounds> {
    method: Method,
    hash_table: HashTable<Method>,
    probe_state: ProbeState,
    settings: GroupBySettings,
    params: Arc<AggregatorParams>,
}

impl<Method: HashMethodBounds> TransformPartialGroupBy<Method> {
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
            let _dropper = GroupByHashTableDropper::<Method>::create();
            HashTable::HashTable(HashTableCell::create(hashtable, _dropper))
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
            TransformPartialGroupBy::<Method> {
                method,
                hash_table,
                probe_state: ProbeState::default(),
                params,
                settings: GroupBySettings::try_from(ctx)?,
            },
        ))
    }
}

impl<Method: HashMethodBounds> AccumulatingTransform for TransformPartialGroupBy<Method> {
    const NAME: &'static str = "TransformPartialGroupBy";

    fn transform(&mut self, block: DataBlock) -> Result<Vec<DataBlock>> {
        let block = block.convert_to_full();
        let group_columns = self
            .params
            .group_columns
            .iter()
            .map(|&index| block.get_by_offset(index))
            .collect::<Vec<_>>();

        let group_columns = group_columns
            .iter()
            .map(|c| (c.value.as_column().unwrap().clone(), c.data_type.clone()))
            .collect::<Vec<_>>();

        unsafe {
            let rows_num = block.num_rows();

            match &mut self.hash_table {
                HashTable::MovedOut => unreachable!(),
                HashTable::HashTable(cell) => {
                    let state = self.method.build_keys_state(&group_columns, rows_num)?;
                    for key in self.method.build_keys_iter(&state)? {
                        let _ = cell.hashtable.insert_and_entry(key);
                    }
                }
                HashTable::PartitionedHashTable(cell) => {
                    let state = self.method.build_keys_state(&group_columns, rows_num)?;
                    for key in self.method.build_keys_iter(&state)? {
                        let _ = cell.hashtable.insert_and_entry(key);
                    }
                }
                HashTable::AggregateHashTable(hashtable) => {
                    let group_columns: Vec<Column> =
                        group_columns.into_iter().map(|c| c.0).collect();
                    let _ = hashtable.add_groups(
                        &mut self.probe_state,
                        &group_columns,
                        &[vec![]],
                        &[],
                        rows_num,
                    )?;
                }
            };

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
                    if let HashTable::PartitionedHashTable(v) = std::mem::take(&mut self.hash_table)
                    {
                        let _dropper = v._dropper.clone();
                        let blocks = vec![DataBlock::empty_with_meta(
                            AggregateMeta::<Method, ()>::create_spilling(v),
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
                        AggregateMeta::<Method, ()>::create_agg_spilling(partitioned_payload),
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
        }

        Ok(vec![])
    }

    fn on_finish(&mut self, output: bool) -> Result<Vec<DataBlock>> {
        Ok(match std::mem::take(&mut self.hash_table) {
            HashTable::MovedOut => match !output && std::thread::panicking() {
                true => vec![],
                false => unreachable!(),
            },
            HashTable::HashTable(cell) => match cell.hashtable.len() == 0 {
                true => vec![],
                false => {
                    vec![DataBlock::empty_with_meta(
                        AggregateMeta::<Method, ()>::create_hashtable(-1, cell),
                    )]
                }
            },
            HashTable::PartitionedHashTable(v) => {
                info!(
                    "Processed {} different keys, allocated {} memory while in group by.",
                    convert_number_size(v.len() as f64),
                    convert_byte_size(v.allocated_bytes() as f64)
                );

                let _ = v.hashtable.unsize_key_size();
                let cells = PartitionedHashTableDropper::split_cell(v);
                let mut blocks = Vec::with_capacity(cells.len());
                for (bucket, cell) in cells.into_iter().enumerate() {
                    if cell.hashtable.len() != 0 {
                        blocks.push(DataBlock::empty_with_meta(
                            AggregateMeta::<Method, ()>::create_hashtable(bucket as isize, cell),
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
                        AggregateMeta::<Method, ()>::create_agg_payload(
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
