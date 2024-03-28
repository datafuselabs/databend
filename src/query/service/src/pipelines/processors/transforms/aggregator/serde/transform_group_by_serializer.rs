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
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::PayloadFlushState;
use databend_common_hashtable::HashtableEntryRefLike;
use databend_common_hashtable::HashtableLike;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;

use crate::pipelines::processors::transforms::aggregator::estimated_key_size;
use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregatePayload;
use crate::pipelines::processors::transforms::aggregator::AggregateSerdeMeta;
use crate::pipelines::processors::transforms::aggregator::HashTablePayload;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::pipelines::processors::transforms::group_by::KeysColumnBuilder;

pub struct TransformGroupBySerializer<Method: HashMethodBounds> {
    method: Method,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,
    input_data: Option<SerializeGroupByStream<Method>>,
}

impl<Method: HashMethodBounds> TransformGroupBySerializer<Method> {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        method: Method,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(TransformGroupBySerializer {
            method,
            input,
            output,
            output_data: None,
            input_data: None,
        })))
    }
}

impl<Method: HashMethodBounds> Processor for TransformGroupBySerializer<Method> {
    fn name(&self) -> String {
        String::from("TransformGroupBySerializer")
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

impl<Method: HashMethodBounds> TransformGroupBySerializer<Method> {
    fn transform_input_data(&mut self, mut data_block: DataBlock) -> Result<Event> {
        debug_assert!(data_block.is_empty());
        if let Some(block_meta) = data_block.take_meta() {
            if let Some(block_meta) = AggregateMeta::<Method, ()>::downcast_from(block_meta) {
                match block_meta {
                    AggregateMeta::Spilled(_) => unreachable!(),
                    AggregateMeta::Spilling(_) => unreachable!(),
                    AggregateMeta::Serialized(_) => unreachable!(),
                    AggregateMeta::BucketSpilled(_) => unreachable!(),
                    AggregateMeta::Partitioned { .. } => unreachable!(),
                    AggregateMeta::AggregateSpilling(_) => unreachable!(),
                    AggregateMeta::AggregatePayload(p) => {
                        self.input_data = Some(SerializeGroupByStream::create(
                            &self.method,
                            SerializePayload::<Method, ()>::AggregatePayload(p),
                        ));
                        return Ok(Event::Sync);
                    }
                    AggregateMeta::HashTable(payload) => {
                        self.input_data = Some(SerializeGroupByStream::create(
                            &self.method,
                            SerializePayload::<Method, ()>::HashTablePayload(payload),
                        ));
                        return Ok(Event::Sync);
                    }
                }
            }
        }

        unreachable!()
    }
}

pub fn serialize_group_by<Method: HashMethodBounds>(
    method: &Method,
    hashtable: &Method::HashTable<()>,
) -> Result<DataBlock> {
    let keys_len = hashtable.len();
    let value_size = estimated_key_size(hashtable);
    let mut group_key_builder = method.keys_column_builder(keys_len, value_size);

    for group_entity in hashtable.iter() {
        group_key_builder.append_value(group_entity.key());
    }

    Ok(DataBlock::new_from_columns(vec![
        group_key_builder.finish(),
    ]))
}

pub enum SerializePayload<Method: HashMethodBounds, V: Send + Sync + 'static> {
    HashTablePayload(HashTablePayload<Method, V>),
    AggregatePayload(AggregatePayload),
}

pub struct SerializeGroupByStream<Method: HashMethodBounds> {
    method: Method,
    pub payload: Pin<Box<SerializePayload<Method, ()>>>,
    // old hashtable' iter
    iter: Option<<Method::HashTable<()> as HashtableLike>::Iterator<'static>>,
    flush_state: Option<PayloadFlushState>,
    end_iter: bool,
}

unsafe impl<Method: HashMethodBounds> Send for SerializeGroupByStream<Method> {}

unsafe impl<Method: HashMethodBounds> Sync for SerializeGroupByStream<Method> {}

impl<Method: HashMethodBounds> SerializeGroupByStream<Method> {
    pub fn create(method: &Method, payload: SerializePayload<Method, ()>) -> Self {
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

            SerializeGroupByStream::<Method> {
                iter,
                payload,
                flush_state,
                method: method.clone(),
                end_iter: false,
            }
        }
    }
}

impl<Method: HashMethodBounds> Iterator for SerializeGroupByStream<Method> {
    type Item = Result<DataBlock>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.end_iter {
            return None;
        }

        match self.payload.as_ref().get_ref() {
            SerializePayload::HashTablePayload(p) => {
                let max_block_rows = std::cmp::min(8192, p.cell.hashtable.len());
                let max_block_bytes = std::cmp::min(
                    8 * 1024 * 1024 + 1024,
                    p.cell.hashtable.unsize_key_size().unwrap_or(usize::MAX),
                );

                let mut group_key_builder = self
                    .method
                    .keys_column_builder(max_block_rows, max_block_bytes);

                #[allow(clippy::while_let_on_iterator)]
                while let Some(group_entity) = self.iter.as_mut()?.next() {
                    group_key_builder.append_value(group_entity.key());

                    if group_key_builder.bytes_size() >= 8 * 1024 * 1024 {
                        let bucket = p.bucket;
                        let data_block =
                            DataBlock::new_from_columns(vec![group_key_builder.finish()]);
                        return Some(data_block.add_meta(Some(AggregateSerdeMeta::create(bucket))));
                    }
                }

                self.end_iter = true;
                let bucket = p.bucket;
                let data_block = DataBlock::new_from_columns(vec![group_key_builder.finish()]);
                Some(data_block.add_meta(Some(AggregateSerdeMeta::create(bucket))))
            }
            SerializePayload::AggregatePayload(p) => {
                let state = self.flush_state.as_mut().unwrap();
                let block = p.payload.aggregate_flush(state).unwrap();

                if block.is_none() {
                    self.end_iter = true;
                }

                block.map(|block| {
                    block.add_meta(Some(AggregateSerdeMeta::create_agg_payload(
                        p.bucket,
                        p.max_partition_count,
                    )))
                })
            }
        }
    }
}
