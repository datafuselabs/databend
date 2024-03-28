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

use bumpalo::Bump;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggregateHashTable;
use databend_common_expression::DataBlock;
use databend_common_expression::HashTableConfig;
use databend_common_expression::PayloadFlushState;
use databend_common_hashtable::HashtableEntryRefLike;
use databend_common_hashtable::HashtableLike;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_transforms::processors::BlockMetaTransform;
use databend_common_pipeline_transforms::processors::BlockMetaTransformer;

use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::estimated_key_size;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::group_by::GroupColumnsBuilder;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::pipelines::processors::transforms::group_by::KeysColumnIter;

pub struct TransformFinalGroupBy<Method: HashMethodBounds> {
    method: Method,
    params: Arc<AggregatorParams>,
    flush_state: PayloadFlushState,
    reach_limit: bool,
}

impl<Method: HashMethodBounds> TransformFinalGroupBy<Method> {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        method: Method,
        params: Arc<AggregatorParams>,
    ) -> Result<Box<dyn Processor>> {
        Ok(Box::new(BlockMetaTransformer::create(
            input,
            output,
            TransformFinalGroupBy::<Method> {
                method,
                params,
                flush_state: PayloadFlushState::default(),
                reach_limit: false,
            },
        )))
    }

    fn transform_agg_hashtable(&mut self, meta: AggregateMeta<Method, ()>) -> Result<DataBlock> {
        let mut agg_hashtable: Option<AggregateHashTable> = None;
        if let AggregateMeta::Partitioned { bucket, data } = meta {
            for bucket_data in data {
                match bucket_data {
                    AggregateMeta::Serialized(payload) => match agg_hashtable.as_mut() {
                        Some(ht) => {
                            debug_assert!(bucket == payload.bucket);
                            let payload = payload.convert_to_partitioned_payload(
                                self.params.group_data_types.clone(),
                                self.params.aggregate_functions.clone(),
                                0,
                                Arc::new(Bump::new()),
                            )?;
                            ht.combine_payloads(&payload, &mut self.flush_state)?;
                        }
                        None => {
                            debug_assert!(bucket == payload.bucket);
                            agg_hashtable = Some(payload.convert_to_aggregate_table(
                                self.params.group_data_types.clone(),
                                self.params.aggregate_functions.clone(),
                                0,
                                Arc::new(Bump::new()),
                                true,
                            )?);
                        }
                    },
                    AggregateMeta::AggregatePayload(payload) => match agg_hashtable.as_mut() {
                        Some(ht) => {
                            debug_assert!(bucket == payload.bucket);
                            ht.combine_payload(&payload.payload, &mut self.flush_state)?;
                        }
                        None => {
                            debug_assert!(bucket == payload.bucket);
                            let capacity =
                                AggregateHashTable::get_capacity_for_count(payload.payload.len());
                            let mut hashtable = AggregateHashTable::new_with_capacity(
                                self.params.group_data_types.clone(),
                                self.params.aggregate_functions.clone(),
                                HashTableConfig::default().with_initial_radix_bits(0),
                                capacity,
                                Arc::new(Bump::new()),
                            );
                            hashtable.combine_payload(&payload.payload, &mut self.flush_state)?;
                            agg_hashtable = Some(hashtable);
                        }
                    },
                    _ => unreachable!(),
                }
            }
        }

        if let Some(mut ht) = agg_hashtable {
            let mut blocks = vec![];
            self.flush_state.clear();

            let mut rows = 0;
            loop {
                if ht.merge_result(&mut self.flush_state)? {
                    let cols = self.flush_state.take_group_columns();
                    rows += cols[0].len();
                    blocks.push(DataBlock::new_from_columns(cols));

                    if rows >= self.params.limit.unwrap_or(usize::MAX) {
                        log::info!(
                            "reach limit optimization in flush agg hashtable, current {}, total {}",
                            rows,
                            ht.len(),
                        );
                        self.reach_limit = true;
                        break;
                    }
                } else {
                    break;
                }
            }

            if blocks.is_empty() {
                return Ok(self.params.empty_result_block());
            }

            return DataBlock::concat(&blocks);
        }
        Ok(self.params.empty_result_block())
    }
}

impl<Method> BlockMetaTransform<AggregateMeta<Method, ()>> for TransformFinalGroupBy<Method>
where Method: HashMethodBounds
{
    const NAME: &'static str = "TransformFinalGroupBy";

    fn transform(&mut self, meta: AggregateMeta<Method, ()>) -> Result<DataBlock> {
        if self.reach_limit {
            return Ok(self.params.empty_result_block());
        }

        if self.params.enable_experimental_aggregate_hashtable {
            return self.transform_agg_hashtable(meta);
        }

        if let AggregateMeta::Partitioned { bucket, data } = meta {
            let arena = Arc::new(Bump::new());
            let mut hashtable = self.method.create_hash_table::<()>(arena)?;

            'merge_hashtable: for bucket_data in data {
                match bucket_data {
                    AggregateMeta::Spilled(_) => unreachable!(),
                    AggregateMeta::BucketSpilled(_) => unreachable!(),
                    AggregateMeta::Spilling(_) => unreachable!(),
                    AggregateMeta::Partitioned { .. } => unreachable!(),
                    AggregateMeta::Serialized(payload) => {
                        debug_assert!(bucket == payload.bucket);
                        let column = payload.get_group_by_column();
                        let keys_iter = self.method.keys_iter_from_column(column)?;

                        unsafe {
                            for key in keys_iter.iter() {
                                let _ = hashtable.insert_and_entry(key);
                            }

                            if let Some(limit) = self.params.limit {
                                if hashtable.len() >= limit {
                                    self.reach_limit = true;
                                    break 'merge_hashtable;
                                }
                            }
                        }
                    }
                    AggregateMeta::HashTable(payload) => unsafe {
                        debug_assert!(bucket == payload.bucket);

                        for key in payload.cell.hashtable.iter() {
                            let _ = hashtable.insert_and_entry(key.key());
                        }

                        if let Some(limit) = self.params.limit {
                            if hashtable.len() >= limit {
                                break 'merge_hashtable;
                            }
                        }
                    },
                    AggregateMeta::AggregatePayload(_) => unreachable!(),
                    AggregateMeta::AggregateSpilling(_) => unreachable!(),
                }
            }

            let value_size = estimated_key_size(&hashtable);
            let keys_len = hashtable.len();

            let mut group_columns_builder =
                self.method
                    .group_columns_builder(keys_len, value_size, &self.params);

            for group_entity in hashtable.iter() {
                group_columns_builder.append_value(group_entity.key());
            }

            return Ok(DataBlock::new_from_columns(group_columns_builder.finish()?));
        }

        Err(ErrorCode::Internal(
            "TransformFinalGroupBy only recv AggregateMeta::Partitioned",
        ))
    }
}
