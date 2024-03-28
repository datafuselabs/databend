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
use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::arrow::deserialize_column;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use itertools::Itertools;
use log::info;
use opendal::Operator;

use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::BucketSpilledPayload;
use crate::pipelines::processors::transforms::aggregator::SerializedPayload;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;

type DeserializingMeta<Method, V> = (AggregateMeta<Method, V>, VecDeque<Vec<u8>>);

pub struct TransformSpillReader<Method: HashMethodBounds, V: Send + Sync + 'static> {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    operator: Operator,
    deserialized_meta: Option<BlockMetaInfoPtr>,
    reading_meta: Option<AggregateMeta<Method, V>>,
    deserializing_meta: Option<DeserializingMeta<Method, V>>,
}

#[async_trait::async_trait]
impl<Method: HashMethodBounds, V: Send + Sync + 'static> Processor
    for TransformSpillReader<Method, V>
{
    fn name(&self) -> String {
        String::from("TransformSpillReader")
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

        if let Some(deserialized_meta) = self.deserialized_meta.take() {
            self.output
                .push_data(Ok(DataBlock::empty_with_meta(deserialized_meta)));
            return Ok(Event::NeedConsume);
        }

        if self.deserializing_meta.is_some() {
            self.input.set_not_need_data();
            return Ok(Event::Sync);
        }

        if self.reading_meta.is_some() {
            self.input.set_not_need_data();
            return Ok(Event::Async);
        }

        if self.input.has_data() {
            let mut data_block = self.input.pull_data().unwrap()?;

            if let Some(block_meta) = data_block
                .get_meta()
                .and_then(AggregateMeta::<Method, V>::downcast_ref_from)
            {
                if matches!(block_meta, AggregateMeta::BucketSpilled(_)) {
                    self.input.set_not_need_data();
                    let block_meta = data_block.take_meta().unwrap();
                    self.reading_meta = AggregateMeta::<Method, V>::downcast_from(block_meta);
                    return Ok(Event::Async);
                }

                if let AggregateMeta::Partitioned { data, .. } = block_meta {
                    for meta in data {
                        if matches!(meta, AggregateMeta::BucketSpilled(_)) {
                            self.input.set_not_need_data();
                            let block_meta = data_block.take_meta().unwrap();
                            self.reading_meta =
                                AggregateMeta::<Method, V>::downcast_from(block_meta);
                            return Ok(Event::Async);
                        }
                    }
                }
            }

            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        if let Some((meta, mut read_data)) = self.deserializing_meta.take() {
            match meta {
                AggregateMeta::Spilled(_) => unreachable!(),
                AggregateMeta::Spilling(_) => unreachable!(),
                AggregateMeta::AggregatePayload(_) => unreachable!(),
                AggregateMeta::AggregateSpilling(_) => unreachable!(),
                AggregateMeta::HashTable(_) => unreachable!(),
                AggregateMeta::Serialized(_) => unreachable!(),
                AggregateMeta::BucketSpilled(payload) => {
                    debug_assert!(read_data.len() == 1);
                    let data = read_data.pop_front().unwrap();

                    self.deserialized_meta = Some(Box::new(Self::deserialize(payload, data)));
                }
                AggregateMeta::Partitioned { bucket, data } => {
                    let mut new_data = Vec::with_capacity(data.len());

                    for meta in data {
                        if matches!(&meta, AggregateMeta::BucketSpilled(_)) {
                            if let AggregateMeta::BucketSpilled(payload) = meta {
                                let data = read_data.pop_front().unwrap();
                                new_data.push(Self::deserialize(payload, data));
                            }

                            continue;
                        }

                        new_data.push(meta);
                    }

                    self.deserialized_meta = Some(AggregateMeta::<Method, V>::create_partitioned(
                        bucket, new_data,
                    ));
                }
            }
        }

        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if let Some(block_meta) = self.reading_meta.take() {
            match &block_meta {
                AggregateMeta::Spilled(_) => unreachable!(),
                AggregateMeta::Spilling(_) => unreachable!(),
                AggregateMeta::HashTable(_) => unreachable!(),
                AggregateMeta::AggregatePayload(_) => unreachable!(),
                AggregateMeta::AggregateSpilling(_) => unreachable!(),
                AggregateMeta::Serialized(_) => unreachable!(),
                AggregateMeta::BucketSpilled(payload) => {
                    let instant = Instant::now();
                    let data = self
                        .operator
                        .read_with(&payload.location)
                        .range(payload.data_range.clone())
                        .await?;

                    info!(
                        "Read aggregate spill {} successfully, elapsed: {:?}",
                        &payload.location,
                        instant.elapsed()
                    );

                    self.deserializing_meta = Some((block_meta, VecDeque::from(vec![data])));
                }
                AggregateMeta::Partitioned { data, .. } => {
                    let mut read_data = Vec::with_capacity(data.len());
                    for meta in data {
                        if let AggregateMeta::BucketSpilled(payload) = meta {
                            let location = payload.location.clone();
                            let operator = self.operator.clone();
                            let data_range = payload.data_range.clone();
                            read_data.push(databend_common_base::runtime::spawn(async move {
                                let instant = Instant::now();
                                let data = operator.read_with(&location).range(data_range).await?;

                                // perf
                                {
                                    Profile::record_usize_profile(
                                        ProfileStatisticsName::SpillReadCount,
                                        1,
                                    );
                                    Profile::record_usize_profile(
                                        ProfileStatisticsName::SpillReadBytes,
                                        data.len(),
                                    );
                                    Profile::record_usize_profile(
                                        ProfileStatisticsName::SpillReadTime,
                                        instant.elapsed().as_millis() as usize,
                                    );
                                }

                                info!(
                                    "Read aggregate spill {} successfully, elapsed: {:?}",
                                    location,
                                    instant.elapsed()
                                );

                                Ok(data)
                            }));
                        }
                    }

                    match futures::future::try_join_all(read_data).await {
                        Err(_) => {
                            return Err(ErrorCode::TokioError("Cannot join tokio job"));
                        }
                        Ok(read_data) => {
                            let read_data: Result<VecDeque<Vec<u8>>, opendal::Error> =
                                read_data.into_iter().try_collect();

                            self.deserializing_meta = Some((block_meta, read_data?));
                        }
                    };
                }
            }
        }

        Ok(())
    }
}

impl<Method: HashMethodBounds, V: Send + Sync + 'static> TransformSpillReader<Method, V> {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        operator: Operator,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(TransformSpillReader::<
            Method,
            V,
        > {
            input,
            output,
            operator,
            deserialized_meta: None,
            reading_meta: None,
            deserializing_meta: None,
        })))
    }

    fn deserialize(payload: BucketSpilledPayload, data: Vec<u8>) -> AggregateMeta<Method, V> {
        let mut begin = 0;
        let mut columns = Vec::with_capacity(payload.columns_layout.len());

        for column_layout in payload.columns_layout {
            columns.push(deserialize_column(&data[begin..begin + column_layout as usize]).unwrap());
            begin += column_layout as usize;
        }

        AggregateMeta::<Method, V>::Serialized(SerializedPayload {
            bucket: payload.bucket,
            data_block: DataBlock::new_from_columns(columns),
            max_partition_count: payload.max_partition_count,
        })
    }
}

pub type TransformGroupBySpillReader<Method> = TransformSpillReader<Method, ()>;
pub type TransformAggregateSpillReader<Method> = TransformSpillReader<Method, usize>;
