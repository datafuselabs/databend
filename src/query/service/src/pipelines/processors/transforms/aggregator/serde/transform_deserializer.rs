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

use std::marker::PhantomData;
use std::sync::Arc;

use databend_common_arrow::arrow::datatypes::Field;
use databend_common_arrow::arrow::datatypes::Schema as ArrowSchema;
use databend_common_arrow::arrow::io::flight::default_ipc_fields;
use databend_common_arrow::arrow::io::flight::deserialize_batch;
use databend_common_arrow::arrow::io::flight::deserialize_dictionary;
use databend_common_arrow::arrow::io::ipc::read::Dictionaries;
use databend_common_arrow::arrow::io::ipc::IpcSchema;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::ValueType;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_io::prelude::bincode_deserialize_from_slice;
use databend_common_io::prelude::BinaryRead;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_transforms::processors::BlockMetaTransform;
use databend_common_pipeline_transforms::processors::BlockMetaTransformer;
use databend_common_pipeline_transforms::processors::UnknownMode;

use crate::pipelines::processors::transforms::aggregator::exchange_defines;
use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregateSerdeMeta;
use crate::pipelines::processors::transforms::aggregator::BucketSpilledPayload;
use crate::pipelines::processors::transforms::aggregator::BUCKET_TYPE;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::servers::flight::v1::exchange::serde::ExchangeDeserializeMeta;
use crate::servers::flight::v1::packets::DataPacket;
use crate::servers::flight::v1::packets::FragmentData;

pub struct TransformDeserializer<Method: HashMethodBounds, V: Send + Sync + 'static> {
    schema: DataSchemaRef,
    ipc_schema: IpcSchema,
    arrow_schema: Arc<ArrowSchema>,
    _phantom: PhantomData<(Method, V)>,
}

impl<Method: HashMethodBounds, V: Send + Sync + 'static> TransformDeserializer<Method, V> {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        schema: &DataSchemaRef,
    ) -> Result<ProcessorPtr> {
        let arrow_schema = ArrowSchema::from(schema.as_ref());
        let ipc_fields = default_ipc_fields(&arrow_schema.fields);
        let ipc_schema = IpcSchema {
            fields: ipc_fields,
            is_little_endian: true,
        };

        Ok(ProcessorPtr::create(BlockMetaTransformer::create(
            input,
            output,
            TransformDeserializer::<Method, V> {
                ipc_schema,
                arrow_schema: Arc::new(arrow_schema),
                schema: schema.clone(),
                _phantom: Default::default(),
            },
        )))
    }

    fn recv_data(&self, dict: Vec<DataPacket>, fragment_data: FragmentData) -> Result<DataBlock> {
        const ROW_HEADER_SIZE: usize = std::mem::size_of::<u32>();

        let meta = bincode_deserialize_from_slice(&fragment_data.get_meta()[ROW_HEADER_SIZE..])
            .map_err(|_| ErrorCode::BadBytes("block meta deserialize error when exchange"))?;

        let mut row_count_meta = &fragment_data.get_meta()[..ROW_HEADER_SIZE];
        let row_count: u32 = row_count_meta.read_scalar()?;

        if row_count == 0 {
            return Ok(DataBlock::new_with_meta(vec![], 0, meta));
        }

        let fields = &self.arrow_schema.fields;
        let schema = &self.ipc_schema;

        let data_block = match &meta {
            None => {
                self.deserialize_data_block(dict, &fragment_data, fields, schema, &self.schema)?
            }
            Some(meta) => match AggregateSerdeMeta::downcast_ref_from(meta) {
                None => {
                    self.deserialize_data_block(dict, &fragment_data, fields, schema, &self.schema)?
                }
                Some(meta) => {
                    return match meta.typ == BUCKET_TYPE {
                        true => Ok(DataBlock::empty_with_meta(
                            AggregateMeta::<Method, V>::create_serialized(
                                meta.bucket,
                                self.deserialize_data_block(
                                    dict,
                                    &fragment_data,
                                    fields,
                                    schema,
                                    &self.schema,
                                )?,
                                meta.max_partition_count,
                            ),
                        )),
                        false => {
                            let fields = exchange_defines::spilled_fields();
                            let schema = exchange_defines::spilled_ipc_schema();
                            let data_schema = Arc::new(exchange_defines::spilled_schema());
                            let data_block = self.deserialize_data_block(
                                dict,
                                &fragment_data,
                                fields,
                                schema,
                                &data_schema,
                            )?;

                            let columns = data_block
                                .columns()
                                .iter()
                                .map(|c| c.value.clone().into_column())
                                .try_collect::<Vec<_>>()
                                .unwrap();

                            let buckets =
                                NumberType::<i64>::try_downcast_column(&columns[0]).unwrap();
                            let data_range_start =
                                NumberType::<u64>::try_downcast_column(&columns[1]).unwrap();
                            let data_range_end =
                                NumberType::<u64>::try_downcast_column(&columns[2]).unwrap();
                            let columns_layout =
                                ArrayType::<UInt64Type>::try_downcast_column(&columns[3]).unwrap();

                            let columns_layout_data = columns_layout.values.as_slice();

                            let mut buckets_payload = Vec::with_capacity(data_block.num_rows());
                            for index in 0..data_block.num_rows() {
                                unsafe {
                                    buckets_payload.push(BucketSpilledPayload {
                                        bucket: *buckets.get_unchecked(index) as isize,
                                        location: meta.location.clone().unwrap(),
                                        data_range: *data_range_start.get_unchecked(index)
                                            ..*data_range_end.get_unchecked(index),
                                        columns_layout: columns_layout_data[columns_layout.offsets
                                            [index]
                                            as usize
                                            ..columns_layout.offsets[index + 1] as usize]
                                            .to_vec(),
                                        max_partition_count: meta.max_partition_count,
                                    });
                                }
                            }

                            Ok(DataBlock::empty_with_meta(
                                AggregateMeta::<Method, V>::create_spilled(buckets_payload),
                            ))
                        }
                    };
                }
            },
        };

        match data_block.num_columns() == 0 {
            true => Ok(DataBlock::new_with_meta(vec![], row_count as usize, meta)),
            false => data_block.add_meta(meta),
        }
    }

    fn deserialize_data_block(
        &self,
        dict: Vec<DataPacket>,
        fragment_data: &FragmentData,
        arrow_fields: &[Field],
        ipc_schema: &IpcSchema,
        data_schema: &DataSchemaRef,
    ) -> Result<DataBlock> {
        let mut dictionaries = Dictionaries::new();

        for dict_packet in dict {
            if let DataPacket::Dictionary(flight_data) = dict_packet {
                deserialize_dictionary(&flight_data, arrow_fields, ipc_schema, &mut dictionaries)?;
            }
        }

        let batch =
            deserialize_batch(&fragment_data.data, arrow_fields, ipc_schema, &dictionaries)?;

        DataBlock::from_arrow_chunk(&batch, data_schema)
    }
}

impl<M, V> BlockMetaTransform<ExchangeDeserializeMeta> for TransformDeserializer<M, V>
where
    M: HashMethodBounds,
    V: Send + Sync + 'static,
{
    const UNKNOWN_MODE: UnknownMode = UnknownMode::Pass;
    const NAME: &'static str = "TransformDeserializer";

    fn transform(&mut self, mut meta: ExchangeDeserializeMeta) -> Result<Vec<DataBlock>> {
        match meta.packet.pop().unwrap() {
            DataPacket::ErrorCode(v) => Err(v),
            DataPacket::Dictionary(_) => unreachable!(),
            DataPacket::QueryProfiles(_) => unreachable!(),
            DataPacket::SerializeProgress { .. } => unreachable!(),
            DataPacket::CopyStatus { .. } => unreachable!(),
            DataPacket::MergeStatus { .. } => unreachable!(),
            DataPacket::DataCacheMetrics(_) => unreachable!(),
            DataPacket::FragmentData(v) => Ok(vec![self.recv_data(meta.packet, v)?]),
        }
    }
}

pub type TransformGroupByDeserializer<Method> = TransformDeserializer<Method, ()>;
pub type TransformAggregateDeserializer<Method> = TransformDeserializer<Method, usize>;
