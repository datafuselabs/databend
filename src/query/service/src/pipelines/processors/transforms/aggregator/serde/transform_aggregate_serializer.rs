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
use std::pin::Pin;
use std::ptr::NonNull;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::binary::BinaryColumnBuilder;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::PayloadFlushState;
use databend_common_functions::aggregates::StateAddr;
use databend_common_hashtable::HashtableEntryRefLike;
use databend_common_hashtable::HashtableLike;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;

use super::SerializePayload;
use crate::pipelines::processors::transforms::aggregator::create_state_serializer;
use crate::pipelines::processors::transforms::aggregator::estimated_key_size;
use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregateSerdeMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatorParams;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::pipelines::processors::transforms::group_by::KeysColumnBuilder;
pub struct TransformAggregateSerializer<Method: HashMethodBounds> {
    method: Method,
    params: Arc<AggregatorParams>,

    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,
    input_data: Option<SerializeAggregateStream<Method>>,
}

impl<Method: HashMethodBounds> TransformAggregateSerializer<Method> {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        method: Method,
        params: Arc<AggregatorParams>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(
            TransformAggregateSerializer {
                input,
                output,
                method,
                params,
                input_data: None,
                output_data: None,
            },
        )))
    }
}

impl<Method: HashMethodBounds> Processor for TransformAggregateSerializer<Method> {
    fn name(&self) -> String {
        String::from("TransformAggregateSerializer")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(output_data) = self.output_data.take() {
            self.output.push_data(Ok(output_data));
            return Ok(Event::NeedConsume);
        }

        if self.input_data.is_some() {
            return Ok(Event::Sync);
        }

        if self.input.has_data() {
            let data_block = self.input.pull_data().unwrap()?;
            return self.transform_input_data(data_block);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some(stream) = &mut self.input_data {
            self.output_data = Option::transpose(stream.next())?;

            if self.output_data.is_none() {
                self.input_data = None;
            }
        }

        Ok(())
    }
}

impl<Method: HashMethodBounds> TransformAggregateSerializer<Method> {
    fn transform_input_data(&mut self, mut data_block: DataBlock) -> Result<Event> {
        debug_assert!(data_block.is_empty());
        if let Some(block_meta) = data_block.take_meta() {
            if let Some(block_meta) = AggregateMeta::<Method, usize>::downcast_from(block_meta) {
                match block_meta {
                    AggregateMeta::Spilled(_) => unreachable!(),
                    AggregateMeta::Spilling(_) => unreachable!(),
                    AggregateMeta::Serialized(_) => unreachable!(),
                    AggregateMeta::BucketSpilled(_) => unreachable!(),
                    AggregateMeta::Partitioned { .. } => unreachable!(),
                    AggregateMeta::AggregateSpilling(_) => unreachable!(),
                    AggregateMeta::HashTable(payload) => {
                        self.input_data = Some(SerializeAggregateStream::create(
                            &self.method,
                            &self.params,
                            SerializePayload::<Method, usize>::HashTablePayload(payload),
                        ));
                        return Ok(Event::Sync);
                    }
                    AggregateMeta::AggregatePayload(p) => {
                        self.input_data = Some(SerializeAggregateStream::create(
                            &self.method,
                            &self.params,
                            SerializePayload::<Method, usize>::AggregatePayload(p),
                        ));
                        return Ok(Event::Sync);
                    }
                }
            }
        }

        unreachable!()
    }
}

pub fn serialize_aggregate<Method: HashMethodBounds>(
    method: &Method,
    params: &Arc<AggregatorParams>,
    hashtable: &Method::HashTable<usize>,
) -> Result<DataBlock> {
    let keys_len = hashtable.len();
    let value_size = estimated_key_size(hashtable);

    let funcs = &params.aggregate_functions;
    let offsets_aggregate_states = &params.offsets_aggregate_states;

    // Builders.
    let mut state_builders: Vec<BinaryColumnBuilder> = funcs
        .iter()
        .map(|func| create_state_serializer(func, keys_len))
        .collect();

    let mut group_key_builder = method.keys_column_builder(keys_len, value_size);

    let mut places = Vec::with_capacity(keys_len);
    for group_entity in hashtable.iter() {
        places.push(Into::<StateAddr>::into(*group_entity.get()));
        group_key_builder.append_value(group_entity.key());
    }

    let mut columns = Vec::with_capacity(state_builders.len() + 1);
    for (idx, func) in funcs.iter().enumerate() {
        func.batch_serialize(
            &places,
            offsets_aggregate_states[idx],
            &mut state_builders[idx],
        )?;
    }

    for builder in state_builders.into_iter() {
        columns.push(Column::Binary(builder.build()));
    }
    columns.push(group_key_builder.finish());
    Ok(DataBlock::new_from_columns(columns))
}

pub struct SerializeAggregateStream<Method: HashMethodBounds> {
    method: Method,
    params: Arc<AggregatorParams>,
    pub payload: Pin<Box<SerializePayload<Method, usize>>>,
    // old hashtable' iter
    iter: Option<<Method::HashTable<usize> as HashtableLike>::Iterator<'static>>,
    flush_state: Option<PayloadFlushState>,
    end_iter: bool,
}

unsafe impl<Method: HashMethodBounds> Send for SerializeAggregateStream<Method> {}

unsafe impl<Method: HashMethodBounds> Sync for SerializeAggregateStream<Method> {}

impl<Method: HashMethodBounds> SerializeAggregateStream<Method> {
    pub fn create(
        method: &Method,
        params: &Arc<AggregatorParams>,
        payload: SerializePayload<Method, usize>,
    ) -> Self {
        unsafe {
            let payload = Box::pin(payload);

            let iter = if let SerializePayload::HashTablePayload(p) = payload.as_ref().get_ref() {
                Some(NonNull::from(&p.cell.hashtable).as_ref().iter())
            } else {
                None
            };

            let flush_state =
                if let SerializePayload::AggregatePayload(_) = payload.as_ref().get_ref() {
                    Some(PayloadFlushState::default())
                } else {
                    None
                };

            SerializeAggregateStream::<Method> {
                iter,
                payload,
                end_iter: false,
                flush_state,
                method: method.clone(),
                params: params.clone(),
            }
        }
    }
}

impl<Method: HashMethodBounds> Iterator for SerializeAggregateStream<Method> {
    type Item = Result<DataBlock>;

    fn next(&mut self) -> Option<Self::Item> {
        Result::transpose(self.next_impl())
    }
}

impl<Method: HashMethodBounds> SerializeAggregateStream<Method> {
    fn next_impl(&mut self) -> Result<Option<DataBlock>> {
        if self.end_iter {
            return Ok(None);
        }

        match self.payload.as_ref().get_ref() {
            SerializePayload::HashTablePayload(p) => {
                let max_block_rows = std::cmp::min(8192, p.cell.hashtable.len());
                let max_block_bytes = std::cmp::min(
                    8 * 1024 * 1024 + 1024,
                    p.cell.hashtable.unsize_key_size().unwrap_or(usize::MAX),
                );

                let funcs = &self.params.aggregate_functions;
                let offsets_aggregate_states = &self.params.offsets_aggregate_states;

                let mut state_builders: Vec<BinaryColumnBuilder> = funcs
                    .iter()
                    .map(|func| create_state_serializer(func, max_block_rows))
                    .collect();

                let mut group_key_builder = self
                    .method
                    .keys_column_builder(max_block_rows, max_block_bytes);

                let mut bytes = 0;

                #[allow(clippy::while_let_on_iterator)]
                while let Some(group_entity) = self.iter.as_mut().and_then(|iter| iter.next()) {
                    let place = Into::<StateAddr>::into(*group_entity.get());

                    for (idx, func) in funcs.iter().enumerate() {
                        let arg_place = place.next(offsets_aggregate_states[idx]);
                        func.serialize(arg_place, &mut state_builders[idx].data)?;
                        state_builders[idx].commit_row();
                        bytes += state_builders[idx].memory_size();
                    }

                    group_key_builder.append_value(group_entity.key());

                    if bytes + group_key_builder.bytes_size() >= 8 * 1024 * 1024 {
                        return self.finish(state_builders, group_key_builder);
                    }
                }

                self.end_iter = true;
                self.finish(state_builders, group_key_builder)
            }
            SerializePayload::AggregatePayload(p) => {
                let state = self.flush_state.as_mut().unwrap();
                let block = p.payload.aggregate_flush(state)?;

                if block.is_none() {
                    self.end_iter = true;
                }

                match block {
                    Some(block) => Ok(Some(block.add_meta(Some(
                        AggregateSerdeMeta::create_agg_payload(p.bucket, p.max_partition_count),
                    ))?)),
                    None => Ok(None),
                }
            }
        }
    }

    fn finish(
        &self,
        state_builders: Vec<BinaryColumnBuilder>,
        group_key_builder: Method::ColumnBuilder<'_>,
    ) -> Result<Option<DataBlock>> {
        let mut columns = Vec::with_capacity(state_builders.len() + 1);

        for builder in state_builders.into_iter() {
            columns.push(Column::Binary(builder.build()));
        }

        let bucket = if let SerializePayload::HashTablePayload(p) = self.payload.as_ref().get_ref()
        {
            p.bucket
        } else {
            0
        };
        columns.push(group_key_builder.finish());
        let block = DataBlock::new_from_columns(columns);
        Ok(Some(
            block.add_meta(Some(AggregateSerdeMeta::create(bucket)))?,
        ))
    }
}
