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

use std::any::Any;
use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::marker::PhantomData;
use std::mem::take;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_hashtable::hash2bucket;
use databend_common_hashtable::HashtableLike;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipe;
use databend_common_pipeline_core::PipeItem;
use databend_common_pipeline_core::Pipeline;
use databend_common_storage::DataOperator;

use crate::pipelines::processors::transforms::aggregator::aggregate_meta::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::HashTablePayload;
use crate::pipelines::processors::transforms::aggregator::aggregate_meta::SerializedPayload;
use crate::pipelines::processors::transforms::aggregator::new_transform_partition_bucket::NewTransformPartitionBucket;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::aggregator::PartitionedHashTableDropper;
use crate::pipelines::processors::transforms::aggregator::TransformAggregateSpillReader;
use crate::pipelines::processors::transforms::aggregator::TransformFinalAggregate;
use crate::pipelines::processors::transforms::aggregator::TransformFinalGroupBy;
use crate::pipelines::processors::transforms::aggregator::TransformGroupBySpillReader;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::pipelines::processors::transforms::group_by::KeysColumnIter;
use crate::pipelines::processors::transforms::group_by::PartitionedHashMethod;

static SINGLE_LEVEL_BUCKET_NUM: isize = -1;

struct InputPortState {
    port: Arc<InputPort>,
    bucket: isize,
}

pub struct TransformPartitionBucket<Method: HashMethodBounds, V: Copy + Send + Sync + 'static> {
    output: Arc<OutputPort>,
    inputs: Vec<InputPortState>,
    method: Method,
    working_bucket: isize,
    pushing_bucket: isize,
    initialized_all_inputs: bool,
    buckets_blocks: BTreeMap<isize, Vec<DataBlock>>,
    unsplitted_blocks: Vec<DataBlock>,
    _phantom: PhantomData<V>,
}

impl<Method: HashMethodBounds, V: Copy + Send + Sync + 'static>
    TransformPartitionBucket<Method, V>
{
    pub fn create(method: Method, input_nums: usize) -> Result<Self> {
        let mut inputs = Vec::with_capacity(input_nums);

        for _index in 0..input_nums {
            inputs.push(InputPortState {
                bucket: -1,
                port: InputPort::create(),
            });
        }

        Ok(TransformPartitionBucket {
            method,
            inputs,
            working_bucket: 0,
            pushing_bucket: 0,
            output: OutputPort::create(),
            buckets_blocks: BTreeMap::new(),
            unsplitted_blocks: vec![],
            initialized_all_inputs: false,
            _phantom: Default::default(),
        })
    }

    pub fn get_inputs(&self) -> Vec<Arc<InputPort>> {
        let mut inputs = Vec::with_capacity(self.inputs.len());

        for input_state in &self.inputs {
            inputs.push(input_state.port.clone());
        }

        inputs
    }

    pub fn get_output(&self) -> Arc<OutputPort> {
        self.output.clone()
    }

    fn initialize_all_inputs(&mut self) -> Result<bool> {
        self.initialized_all_inputs = true;

        for index in 0..self.inputs.len() {
            if self.inputs[index].port.is_finished() {
                continue;
            }

            // We pull the first unsplitted data block
            if self.inputs[index].bucket > SINGLE_LEVEL_BUCKET_NUM {
                continue;
            }

            if !self.inputs[index].port.has_data() {
                self.inputs[index].port.set_need_data();
                self.initialized_all_inputs = false;
                continue;
            }

            let data_block = self.inputs[index].port.pull_data().unwrap()?;
            self.inputs[index].bucket = self.add_bucket(data_block)?;

            if self.inputs[index].bucket <= SINGLE_LEVEL_BUCKET_NUM {
                self.inputs[index].port.set_need_data();
                self.initialized_all_inputs = false;
            }
        }

        Ok(self.initialized_all_inputs)
    }

    fn add_bucket(&mut self, mut data_block: DataBlock) -> Result<isize> {
        if let Some(block_meta) = data_block.get_meta() {
            if let Some(block_meta) = AggregateMeta::<Method, V>::downcast_ref_from(block_meta) {
                let (bucket, res) = match block_meta {
                    AggregateMeta::Spilling(_) => unreachable!(),
                    AggregateMeta::Partitioned { .. } => unreachable!(),
                    AggregateMeta::AggregatePayload(_) => unreachable!(),
                    AggregateMeta::AggregateSpilling(_) => unreachable!(),
                    AggregateMeta::BucketSpilled(payload) => {
                        (payload.bucket, SINGLE_LEVEL_BUCKET_NUM)
                    }
                    AggregateMeta::Serialized(payload) => (payload.bucket, payload.bucket),
                    AggregateMeta::HashTable(payload) => (payload.bucket, payload.bucket),
                    AggregateMeta::Spilled(_) => {
                        let meta = data_block.take_meta().unwrap();

                        if let Some(AggregateMeta::Spilled(buckets_payload)) =
                            AggregateMeta::<Method, V>::downcast_from(meta)
                        {
                            for bucket_payload in buckets_payload {
                                match self.buckets_blocks.entry(bucket_payload.bucket) {
                                    Entry::Vacant(v) => {
                                        v.insert(vec![DataBlock::empty_with_meta(
                                            AggregateMeta::<Method, V>::create_bucket_spilled(
                                                bucket_payload,
                                            ),
                                        )]);
                                    }
                                    Entry::Occupied(mut v) => {
                                        v.get_mut().push(DataBlock::empty_with_meta(
                                            AggregateMeta::<Method, V>::create_bucket_spilled(
                                                bucket_payload,
                                            ),
                                        ));
                                    }
                                };
                            }

                            return Ok(SINGLE_LEVEL_BUCKET_NUM);
                        }

                        unreachable!()
                    }
                };

                if bucket > SINGLE_LEVEL_BUCKET_NUM {
                    match self.buckets_blocks.entry(bucket) {
                        Entry::Vacant(v) => {
                            v.insert(vec![data_block]);
                        }
                        Entry::Occupied(mut v) => {
                            v.get_mut().push(data_block);
                        }
                    };

                    return Ok(res);
                }
            }
        }

        self.unsplitted_blocks.push(data_block);
        Ok(SINGLE_LEVEL_BUCKET_NUM)
    }

    fn try_push_data_block(&mut self) -> bool {
        match self.buckets_blocks.is_empty() {
            true => self.try_push_single_level(),
            false => self.try_push_two_level(),
        }
    }

    fn try_push_two_level(&mut self) -> bool {
        while self.pushing_bucket < self.working_bucket {
            if let Some(bucket_blocks) = self.buckets_blocks.remove(&self.pushing_bucket) {
                let data_block = Self::convert_blocks(self.pushing_bucket, bucket_blocks);
                self.output.push_data(Ok(data_block));
                self.pushing_bucket += 1;
                return true;
            }

            self.pushing_bucket += 1;
        }

        false
    }

    fn try_push_single_level(&mut self) -> bool {
        if !self.unsplitted_blocks.is_empty() {
            let data_blocks = take(&mut self.unsplitted_blocks);
            self.output.push_data(Ok(Self::convert_blocks(
                SINGLE_LEVEL_BUCKET_NUM,
                data_blocks,
            )));
            return true;
        }

        false
    }

    fn convert_blocks(bucket: isize, data_blocks: Vec<DataBlock>) -> DataBlock {
        let mut data = Vec::with_capacity(data_blocks.len());
        for mut data_block in data_blocks.into_iter() {
            if let Some(block_meta) = data_block.take_meta() {
                if let Some(block_meta) = AggregateMeta::<Method, V>::downcast_from(block_meta) {
                    data.push(block_meta);
                }
            }
        }

        DataBlock::empty_with_meta(AggregateMeta::<Method, V>::create_partitioned(bucket, data))
    }

    fn partition_block(&self, payload: SerializedPayload) -> Result<Vec<Option<DataBlock>>> {
        let column = payload.get_group_by_column();
        let keys_iter = self.method.keys_iter_from_column(column)?;

        let mut indices = Vec::with_capacity(payload.data_block.num_rows());

        for key_item in keys_iter.iter() {
            let hash = self.method.get_hash(key_item);
            indices.push(hash2bucket::<8, true>(hash as usize) as u16);
        }

        let scatter_blocks = DataBlock::scatter(&payload.data_block, &indices, 1 << 8)?;

        let mut blocks = Vec::with_capacity(scatter_blocks.len());
        for (bucket, data_block) in scatter_blocks.into_iter().enumerate() {
            blocks.push(match data_block.is_empty() {
                true => None,
                false => Some(DataBlock::empty_with_meta(
                    AggregateMeta::<Method, V>::create_serialized(bucket as isize, data_block, 0),
                )),
            });
        }

        Ok(blocks)
    }

    fn partition_hashtable(
        &self,
        payload: HashTablePayload<Method, V>,
    ) -> Result<Vec<Option<DataBlock>>> {
        let temp = PartitionedHashMethod::convert_hashtable(&self.method, payload.cell)?;
        let cells = PartitionedHashTableDropper::split_cell(temp);

        let mut data_blocks = Vec::with_capacity(cells.len());
        for (bucket, cell) in cells.into_iter().enumerate() {
            data_blocks.push(match cell.hashtable.len() == 0 {
                true => None,
                false => Some(DataBlock::empty_with_meta(
                    AggregateMeta::<Method, V>::create_hashtable(bucket as isize, cell),
                )),
            })
        }

        Ok(data_blocks)
    }
}

#[async_trait::async_trait]
impl<Method: HashMethodBounds, V: Copy + Send + Sync + 'static> Processor
    for TransformPartitionBucket<Method, V>
{
    fn name(&self) -> String {
        String::from("TransformPartitionBucket")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            for input_state in &self.inputs {
                input_state.port.finish();
            }

            self.buckets_blocks.clear();
            return Ok(Event::Finished);
        }

        // We pull the first unsplitted data block
        if !self.initialized_all_inputs && !self.initialize_all_inputs()? {
            return Ok(Event::NeedData);
        }

        if !self.buckets_blocks.is_empty() && !self.unsplitted_blocks.is_empty() {
            // Split data blocks if it's unsplitted.
            return Ok(Event::Sync);
        }

        if !self.output.can_push() {
            for input_state in &self.inputs {
                input_state.port.set_not_need_data();
            }

            return Ok(Event::NeedConsume);
        }

        let pushed_data_block = self.try_push_data_block();

        loop {
            // Try to pull the next data or until the port is closed
            let mut all_inputs_is_finished = true;
            let mut all_port_prepared_data = true;

            for index in 0..self.inputs.len() {
                if self.inputs[index].port.is_finished() {
                    continue;
                }

                all_inputs_is_finished = false;
                if self.inputs[index].bucket > self.working_bucket {
                    continue;
                }

                if !self.inputs[index].port.has_data() {
                    all_port_prepared_data = false;
                    self.inputs[index].port.set_need_data();
                    continue;
                }

                let data_block = self.inputs[index].port.pull_data().unwrap()?;
                self.inputs[index].bucket = self.add_bucket(data_block)?;
                debug_assert!(self.unsplitted_blocks.is_empty());

                if self.inputs[index].bucket <= self.working_bucket {
                    all_port_prepared_data = false;
                    self.inputs[index].port.set_need_data();
                }
            }

            if all_inputs_is_finished {
                break;
            }

            if !all_port_prepared_data {
                return Ok(Event::NeedData);
            }

            self.working_bucket += 1;
        }

        if pushed_data_block || self.try_push_data_block() {
            return Ok(Event::NeedConsume);
        }

        if let Some((bucket, bucket_blocks)) = self.buckets_blocks.pop_first() {
            let data_block = Self::convert_blocks(bucket, bucket_blocks);
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        self.output.finish();
        Ok(Event::Finished)
    }

    fn process(&mut self) -> Result<()> {
        let block_meta = self
            .unsplitted_blocks
            .pop()
            .and_then(|mut block| block.take_meta())
            .and_then(AggregateMeta::<Method, V>::downcast_from);

        match block_meta {
            None => Err(ErrorCode::Internal(
                "Internal error, TransformPartitionBucket only recv AggregateMeta.",
            )),
            Some(agg_block_meta) => {
                let data_blocks = match agg_block_meta {
                    AggregateMeta::Spilled(_) => unreachable!(),
                    AggregateMeta::BucketSpilled(_) => unreachable!(),
                    AggregateMeta::Spilling(_) => unreachable!(),
                    AggregateMeta::Partitioned { .. } => unreachable!(),
                    AggregateMeta::Serialized(payload) => self.partition_block(payload)?,
                    AggregateMeta::HashTable(payload) => self.partition_hashtable(payload)?,
                    AggregateMeta::AggregatePayload(_) => unreachable!(),
                    AggregateMeta::AggregateSpilling(_) => unreachable!(),
                };

                for (bucket, block) in data_blocks.into_iter().enumerate() {
                    if let Some(data_block) = block {
                        match self.buckets_blocks.entry(bucket as isize) {
                            Entry::Vacant(v) => {
                                v.insert(vec![data_block]);
                            }
                            Entry::Occupied(mut v) => {
                                v.get_mut().push(data_block);
                            }
                        };
                    }
                }

                Ok(())
            }
        }
    }
}

pub fn build_partition_bucket<Method: HashMethodBounds, V: Copy + Send + Sync + 'static>(
    method: Method,
    pipeline: &mut Pipeline,
    params: Arc<AggregatorParams>,
) -> Result<()> {
    if params.enable_experimental_aggregate_hashtable {
        let input_nums = pipeline.output_len();
        let transform =
            NewTransformPartitionBucket::<Method, V>::create(input_nums, params.clone())?;

        let output = transform.get_output();
        let inputs_port = transform.get_inputs();

        pipeline.add_pipe(Pipe::create(inputs_port.len(), 1, vec![PipeItem::create(
            ProcessorPtr::create(Box::new(transform)),
            inputs_port,
            vec![output],
        )]));

        pipeline.try_resize(input_nums)?;

        let operator = DataOperator::instance().operator();
        pipeline.add_transform(|input, output| {
            let operator = operator.clone();
            match params.aggregate_functions.is_empty() {
                true => TransformGroupBySpillReader::<Method>::create(input, output, operator),
                false => TransformAggregateSpillReader::<Method>::create(input, output, operator),
            }
        })?;

        pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(
                match params.aggregate_functions.is_empty() {
                    true => TransformFinalGroupBy::try_create(
                        input,
                        output,
                        method.clone(),
                        params.clone(),
                    )?,
                    false => TransformFinalAggregate::try_create(
                        input,
                        output,
                        method.clone(),
                        params.clone(),
                    )?,
                },
            ))
        })?;
    } else {
        let input_nums = pipeline.output_len();
        let transform = TransformPartitionBucket::<Method, V>::create(method.clone(), input_nums)?;

        let output = transform.get_output();
        let inputs_port = transform.get_inputs();

        pipeline.add_pipe(Pipe::create(inputs_port.len(), 1, vec![PipeItem::create(
            ProcessorPtr::create(Box::new(transform)),
            inputs_port,
            vec![output],
        )]));

        pipeline.try_resize(input_nums)?;

        let operator = DataOperator::instance().operator();
        pipeline.add_transform(|input, output| {
            let operator = operator.clone();
            match params.aggregate_functions.is_empty() {
                true => TransformGroupBySpillReader::<Method>::create(input, output, operator),
                false => TransformAggregateSpillReader::<Method>::create(input, output, operator),
            }
        })?;

        pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(
                match params.aggregate_functions.is_empty() {
                    true => TransformFinalGroupBy::try_create(
                        input,
                        output,
                        method.clone(),
                        params.clone(),
                    )?,
                    false => TransformFinalAggregate::try_create(
                        input,
                        output,
                        method.clone(),
                        params.clone(),
                    )?,
                },
            ))
        })?;
    }

    Ok(())
}
