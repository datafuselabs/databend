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

use std::fmt::Debug;
use std::fmt::Formatter;
use std::ops::Range;
use std::sync::Arc;

use bumpalo::Bump;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::AggregateFunction;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::HashTableConfig;
use databend_common_expression::PartitionedPayload;
use databend_common_expression::Payload;
use databend_common_expression::ProbeState;

use crate::pipelines::processors::transforms::aggregator::HashTableCell;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::pipelines::processors::transforms::group_by::PartitionedHashMethod;

pub struct HashTablePayload<T: HashMethodBounds, V: Send + Sync + 'static> {
    pub bucket: isize,
    pub cell: HashTableCell<T, V>,
}

pub struct SerializedPayload {
    pub bucket: isize,
    pub data_block: DataBlock,
    // use for new agg_hashtable
    pub max_partition_count: usize,
}

impl SerializedPayload {
    pub fn get_group_by_column(&self) -> &Column {
        let entry = self.data_block.columns().last().unwrap();
        entry.value.as_column().unwrap()
    }

    pub fn convert_to_aggregate_table(
        &self,
        group_types: Vec<DataType>,
        aggrs: Vec<Arc<dyn AggregateFunction>>,
        radix_bits: u64,
        arena: Arc<Bump>,
        need_init_entry: bool,
    ) -> Result<AggregateHashTable> {
        let rows_num = self.data_block.num_rows();
        let capacity = AggregateHashTable::get_capacity_for_count(rows_num);
        let config = HashTableConfig::default().with_initial_radix_bits(radix_bits);
        let mut state = ProbeState::default();
        let agg_len = aggrs.len();
        let group_len = group_types.len();
        let mut hashtable = AggregateHashTable::new_directly(
            group_types,
            aggrs,
            config,
            capacity,
            arena,
            need_init_entry,
        );

        let agg_states = (0..agg_len)
            .map(|i| {
                self.data_block
                    .get_by_offset(i)
                    .value
                    .as_column()
                    .unwrap()
                    .clone()
            })
            .collect::<Vec<_>>();
        let group_columns = (agg_len..(agg_len + group_len))
            .map(|i| {
                self.data_block
                    .get_by_offset(i)
                    .value
                    .as_column()
                    .unwrap()
                    .clone()
            })
            .collect::<Vec<_>>();

        let _ =
            hashtable.add_groups(&mut state, &group_columns, &[vec![]], &agg_states, rows_num)?;

        hashtable.payload.mark_min_cardinality();
        Ok(hashtable)
    }

    pub fn convert_to_partitioned_payload(
        &self,
        group_types: Vec<DataType>,
        aggrs: Vec<Arc<dyn AggregateFunction>>,
        radix_bits: u64,
        arena: Arc<Bump>,
    ) -> Result<PartitionedPayload> {
        let hashtable =
            self.convert_to_aggregate_table(group_types, aggrs, radix_bits, arena, false)?;
        Ok(hashtable.payload)
    }
}

pub struct BucketSpilledPayload {
    pub bucket: isize,
    pub location: String,
    pub data_range: Range<u64>,
    pub columns_layout: Vec<u64>,
    pub max_partition_count: usize,
}

pub struct AggregatePayload {
    pub bucket: isize,
    pub payload: Payload,
    // use for new agg_hashtable
    pub max_partition_count: usize,
}

pub enum AggregateMeta<Method: HashMethodBounds, V: Send + Sync + 'static> {
    Serialized(SerializedPayload),
    HashTable(HashTablePayload<Method, V>),
    AggregatePayload(AggregatePayload),
    AggregateSpilling(PartitionedPayload),
    BucketSpilled(BucketSpilledPayload),
    Spilled(Vec<BucketSpilledPayload>),
    Spilling(HashTablePayload<PartitionedHashMethod<Method>, V>),

    Partitioned { bucket: isize, data: Vec<Self> },
}

impl<Method: HashMethodBounds, V: Send + Sync + 'static> AggregateMeta<Method, V> {
    pub fn create_hashtable(bucket: isize, cell: HashTableCell<Method, V>) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::<Method, V>::HashTable(HashTablePayload {
            cell,
            bucket,
        }))
    }

    pub fn create_agg_payload(
        bucket: isize,
        payload: Payload,
        max_partition_count: usize,
    ) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::<Method, V>::AggregatePayload(
            AggregatePayload {
                bucket,
                payload,
                max_partition_count,
            },
        ))
    }

    pub fn create_agg_spilling(payload: PartitionedPayload) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::<Method, V>::AggregateSpilling(payload))
    }

    pub fn create_serialized(
        bucket: isize,
        block: DataBlock,
        max_partition_count: usize,
    ) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::<Method, V>::Serialized(SerializedPayload {
            bucket,
            data_block: block,
            max_partition_count,
        }))
    }

    pub fn create_spilling(
        cell: HashTableCell<PartitionedHashMethod<Method>, V>,
    ) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::<Method, V>::Spilling(HashTablePayload {
            cell,
            bucket: 0,
        }))
    }

    pub fn create_spilled(buckets_payload: Vec<BucketSpilledPayload>) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::<Method, V>::Spilled(buckets_payload))
    }

    pub fn create_bucket_spilled(payload: BucketSpilledPayload) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::<Method, V>::BucketSpilled(payload))
    }

    pub fn create_partitioned(bucket: isize, data: Vec<Self>) -> BlockMetaInfoPtr {
        Box::new(AggregateMeta::<Method, V>::Partitioned { data, bucket })
    }
}

impl<Method: HashMethodBounds, V: Send + Sync + 'static> serde::Serialize
    for AggregateMeta<Method, V>
{
    fn serialize<S>(&self, _: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        unreachable!("AggregateMeta does not support exchanging between multiple nodes")
    }
}

impl<'de, Method: HashMethodBounds, V: Send + Sync + 'static> serde::Deserialize<'de>
    for AggregateMeta<Method, V>
{
    fn deserialize<D>(_: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        unreachable!("AggregateMeta does not support exchanging between multiple nodes")
    }
}

impl<Method: HashMethodBounds, V: Send + Sync + 'static> Debug for AggregateMeta<Method, V> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AggregateMeta::HashTable(_) => f.debug_struct("AggregateMeta::HashTable").finish(),
            AggregateMeta::Partitioned { .. } => {
                f.debug_struct("AggregateMeta::Partitioned").finish()
            }
            AggregateMeta::Serialized { .. } => {
                f.debug_struct("AggregateMeta::Serialized").finish()
            }
            AggregateMeta::Spilling(_) => f.debug_struct("Aggregate::Spilling").finish(),
            AggregateMeta::Spilled(_) => f.debug_struct("Aggregate::Spilling").finish(),
            AggregateMeta::BucketSpilled(_) => f.debug_struct("Aggregate::BucketSpilled").finish(),
            AggregateMeta::AggregatePayload(_) => {
                f.debug_struct("AggregateMeta:AggregatePayload").finish()
            }
            AggregateMeta::AggregateSpilling(_) => {
                f.debug_struct("AggregateMeta:AggregateSpilling").finish()
            }
        }
    }
}

impl<Method: HashMethodBounds, V: Send + Sync + 'static> BlockMetaInfo
    for AggregateMeta<Method, V>
{
    fn typetag_deserialize(&self) {
        unimplemented!("AggregateMeta does not support exchanging between multiple nodes")
    }

    fn typetag_name(&self) -> &'static str {
        unimplemented!("AggregateMeta does not support exchanging between multiple nodes")
    }

    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals for AggregateMeta")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unimplemented!("Unimplemented clone for AggregateMeta")
    }
}
