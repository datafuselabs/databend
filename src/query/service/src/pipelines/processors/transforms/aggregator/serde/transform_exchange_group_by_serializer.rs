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
use std::sync::Arc;
use std::time::Instant;

use databend_common_arrow::arrow::datatypes::Schema as ArrowSchema;
use databend_common_arrow::arrow::io::flight::default_ipc_fields;
use databend_common_arrow::arrow::io::flight::WriteOptions;
use databend_common_arrow::arrow::io::ipc::write::Compression;
use databend_common_arrow::arrow::io::ipc::IpcField;
use databend_common_base::base::GlobalUniqName;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::arrow::serialize_column;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::ValueType;
use databend_common_expression::BlockEntry;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::BlockMetaInfoPtr;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FromData;
use databend_common_expression::PartitionedPayload;
use databend_common_hashtable::HashtableLike;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_transforms::processors::BlockMetaTransform;
use databend_common_pipeline_transforms::processors::BlockMetaTransformer;
use databend_common_pipeline_transforms::processors::UnknownMode;
use databend_common_settings::FlightCompression;
use futures_util::future::BoxFuture;
use log::info;
use opendal::Operator;

use super::SerializePayload;
use crate::api::serialize_block;
use crate::api::ExchangeShuffleMeta;
use crate::pipelines::processors::transforms::aggregator::agg_spilling_group_by_payload as local_agg_spilling_group_by_payload;
use crate::pipelines::processors::transforms::aggregator::aggregate_exchange_injector::compute_block_number;
use crate::pipelines::processors::transforms::aggregator::exchange_defines;
use crate::pipelines::processors::transforms::aggregator::serialize_group_by;
use crate::pipelines::processors::transforms::aggregator::spilling_group_by_payload as local_spilling_group_by_payload;
use crate::pipelines::processors::transforms::aggregator::AggregateMeta;
use crate::pipelines::processors::transforms::aggregator::AggregateSerdeMeta;
use crate::pipelines::processors::transforms::aggregator::HashTablePayload;
use crate::pipelines::processors::transforms::aggregator::SerializeGroupByStream;
use crate::pipelines::processors::transforms::group_by::HashMethodBounds;
use crate::pipelines::processors::transforms::group_by::PartitionedHashMethod;
use crate::sessions::QueryContext;

pub struct TransformExchangeGroupBySerializer<Method: HashMethodBounds> {
    ctx: Arc<QueryContext>,
    method: Method,
    local_pos: usize,
    options: WriteOptions,
    ipc_fields: Vec<IpcField>,

    operator: Operator,
    location_prefix: String,
}

impl<Method: HashMethodBounds> TransformExchangeGroupBySerializer<Method> {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        ctx: Arc<QueryContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        method: Method,
        operator: Operator,
        location_prefix: String,
        schema: DataSchemaRef,
        local_pos: usize,
        compression: Option<FlightCompression>,
    ) -> Box<dyn Processor> {
        let arrow_schema = ArrowSchema::from(schema.as_ref());
        let ipc_fields = default_ipc_fields(&arrow_schema.fields);
        let compression = match compression {
            None => None,
            Some(compression) => match compression {
                FlightCompression::Lz4 => Some(Compression::LZ4),
                FlightCompression::Zstd => Some(Compression::ZSTD),
            },
        };

        BlockMetaTransformer::create(
            input,
            output,
            TransformExchangeGroupBySerializer::<Method> {
                ctx,
                method,
                operator,
                local_pos,
                ipc_fields,
                location_prefix,
                options: WriteOptions { compression },
            },
        )
    }
}

pub enum FlightSerialized {
    DataBlock(DataBlock),
    Future(BoxFuture<'static, Result<DataBlock>>),
}

unsafe impl Sync for FlightSerialized {}

pub struct FlightSerializedMeta {
    pub serialized_blocks: Vec<FlightSerialized>,
}

impl FlightSerializedMeta {
    pub fn create(blocks: Vec<FlightSerialized>) -> BlockMetaInfoPtr {
        Box::new(FlightSerializedMeta {
            serialized_blocks: blocks,
        })
    }
}

impl Debug for FlightSerializedMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlightSerializedMeta").finish()
    }
}

impl serde::Serialize for FlightSerializedMeta {
    fn serialize<S>(&self, _: S) -> Result<S::Ok, S::Error>
    where S: serde::Serializer {
        unimplemented!("Unimplemented serialize FlightSerializedMeta")
    }
}

impl<'de> serde::Deserialize<'de> for FlightSerializedMeta {
    fn deserialize<D>(_: D) -> Result<Self, D::Error>
    where D: serde::Deserializer<'de> {
        unimplemented!("Unimplemented deserialize FlightSerializedMeta")
    }
}

#[typetag::serde(name = "exchange_shuffle")]
impl BlockMetaInfo for FlightSerializedMeta {
    fn equals(&self, _: &Box<dyn BlockMetaInfo>) -> bool {
        unimplemented!("Unimplemented equals FlightSerializedMeta")
    }

    fn clone_self(&self) -> Box<dyn BlockMetaInfo> {
        unimplemented!("Unimplemented clone FlightSerializedMeta")
    }
}

impl<Method: HashMethodBounds> BlockMetaTransform<ExchangeShuffleMeta>
    for TransformExchangeGroupBySerializer<Method>
{
    const UNKNOWN_MODE: UnknownMode = UnknownMode::Error;
    const NAME: &'static str = "TransformExchangeGroupBySerializer";

    fn transform(&mut self, meta: ExchangeShuffleMeta) -> Result<DataBlock> {
        let mut serialized_blocks = Vec::with_capacity(meta.blocks.len());
        for (index, mut block) in meta.blocks.into_iter().enumerate() {
            if block.is_empty() && block.get_meta().is_none() {
                serialized_blocks.push(FlightSerialized::DataBlock(block));
                continue;
            }

            match AggregateMeta::<Method, ()>::downcast_from(block.take_meta().unwrap()) {
                None => unreachable!(),
                Some(AggregateMeta::Spilled(_)) => unreachable!(),
                Some(AggregateMeta::BucketSpilled(_)) => unreachable!(),
                Some(AggregateMeta::Serialized(_)) => unreachable!(),
                Some(AggregateMeta::Partitioned { .. }) => unreachable!(),
                Some(AggregateMeta::Spilling(payload)) => {
                    serialized_blocks.push(FlightSerialized::Future(
                        match index == self.local_pos {
                            true => local_spilling_group_by_payload(
                                self.ctx.clone(),
                                self.operator.clone(),
                                &self.method,
                                &self.location_prefix,
                                payload,
                            )?,
                            false => spilling_group_by_payload(
                                self.ctx.clone(),
                                self.operator.clone(),
                                &self.method,
                                &self.location_prefix,
                                payload,
                            )?,
                        },
                    ));
                }
                Some(AggregateMeta::AggregateSpilling(payload)) => {
                    serialized_blocks.push(FlightSerialized::Future(
                        match index == self.local_pos {
                            true => local_agg_spilling_group_by_payload::<Method>(
                                self.ctx.clone(),
                                self.operator.clone(),
                                &self.location_prefix,
                                payload,
                            )?,
                            false => agg_spilling_group_by_payload::<Method>(
                                self.ctx.clone(),
                                self.operator.clone(),
                                &self.location_prefix,
                                payload,
                            )?,
                        },
                    ));
                }
                Some(AggregateMeta::HashTable(payload)) => {
                    if index == self.local_pos {
                        serialized_blocks.push(FlightSerialized::DataBlock(block.add_meta(
                            Some(Box::new(AggregateMeta::<Method, ()>::HashTable(payload))),
                        )?));
                        continue;
                    }

                    let bucket = payload.bucket;
                    let stream = SerializeGroupByStream::create(
                        &self.method,
                        SerializePayload::<Method, ()>::HashTablePayload(payload),
                    );

                    let mut stream_blocks = stream.into_iter().collect::<Result<Vec<_>>>()?;

                    if stream_blocks.is_empty() {
                        serialized_blocks.push(FlightSerialized::DataBlock(DataBlock::empty()));
                    } else {
                        let mut c = DataBlock::concat(&stream_blocks)?;
                        if let Some(meta) = stream_blocks[0].take_meta() {
                            c.replace_meta(meta);
                        }

                        let c = serialize_block(bucket, c, &self.ipc_fields, &self.options)?;
                        serialized_blocks.push(FlightSerialized::DataBlock(c));
                    }
                }
                Some(AggregateMeta::AggregatePayload(p)) => {
                    if index == self.local_pos {
                        serialized_blocks.push(FlightSerialized::DataBlock(block.add_meta(
                            Some(Box::new(AggregateMeta::<Method, ()>::AggregatePayload(p))),
                        )?));
                        continue;
                    }

                    let bucket = compute_block_number(p.bucket, p.max_partition_count)?;
                    let stream = SerializeGroupByStream::create(
                        &self.method,
                        SerializePayload::<Method, ()>::AggregatePayload(p),
                    );

                    let mut stream_blocks = stream.into_iter().collect::<Result<Vec<_>>>()?;

                    if stream_blocks.is_empty() {
                        serialized_blocks.push(FlightSerialized::DataBlock(DataBlock::empty()));
                    } else {
                        let mut c = DataBlock::concat(&stream_blocks)?;
                        if let Some(meta) = stream_blocks[0].take_meta() {
                            c.replace_meta(meta);
                        }

                        let c = serialize_block(bucket, c, &self.ipc_fields, &self.options)?;
                        serialized_blocks.push(FlightSerialized::DataBlock(c));
                    }
                }
            };
        }

        Ok(DataBlock::empty_with_meta(FlightSerializedMeta::create(
            serialized_blocks,
        )))
    }
}

fn get_columns(data_block: DataBlock) -> Vec<BlockEntry> {
    data_block.columns().to_vec()
}

fn agg_spilling_group_by_payload<Method: HashMethodBounds>(
    ctx: Arc<QueryContext>,
    operator: Operator,
    location_prefix: &str,
    partitioned_payload: PartitionedPayload,
) -> Result<BoxFuture<'static, Result<DataBlock>>> {
    let unique_name = GlobalUniqName::unique();
    let location = format!("{}/{}", location_prefix, unique_name);

    let mut write_size = 0;
    let partition_count = partitioned_payload.partition_count();
    let mut write_data = Vec::with_capacity(partition_count);
    let mut buckets_column_data = Vec::with_capacity(partition_count);
    let mut data_range_start_column_data = Vec::with_capacity(partition_count);
    let mut data_range_end_column_data = Vec::with_capacity(partition_count);
    let mut columns_layout_column_data = Vec::with_capacity(partition_count);
    // Record how many rows are spilled
    let mut rows = 0;

    for (bucket, payload) in partitioned_payload.payloads.into_iter().enumerate() {
        if payload.len() == 0 {
            continue;
        }

        let data_block = payload.group_by_flush_all()?;
        rows += data_block.num_rows();

        let old_write_size = write_size;
        let columns = get_columns(data_block);
        let mut columns_data = Vec::with_capacity(columns.len());
        let mut columns_layout = Vec::with_capacity(columns.len());

        for column in columns.into_iter() {
            let column = column.value.as_column().unwrap();
            let column_data = serialize_column(column);
            write_size += column_data.len() as u64;
            columns_layout.push(column_data.len() as u64);
            columns_data.push(column_data);
        }

        write_data.push(columns_data);
        buckets_column_data.push(bucket as i64);
        data_range_end_column_data.push(write_size);
        columns_layout_column_data.push(columns_layout);
        data_range_start_column_data.push(old_write_size);
    }

    Ok(Box::pin(async move {
        let instant = Instant::now();

        if !write_data.is_empty() {
            let mut write_bytes = 0;
            let mut writer = operator
                .writer_with(&location)
                .buffer(8 * 1024 * 1024)
                .await?;
            for write_bucket_data in write_data.into_iter() {
                for data in write_bucket_data.into_iter() {
                    write_bytes += data.len();
                    writer.write(data).await?;
                }
            }

            writer.close().await?;

            // perf
            {
                Profile::record_usize_profile(ProfileStatisticsName::SpillWriteCount, 1);
                Profile::record_usize_profile(ProfileStatisticsName::SpillWriteBytes, write_bytes);
                Profile::record_usize_profile(
                    ProfileStatisticsName::SpillWriteTime,
                    instant.elapsed().as_millis() as usize,
                );
            }

            {
                let progress_val = ProgressValues {
                    rows,
                    bytes: write_bytes,
                };
                ctx.get_group_by_spill_progress().incr(&progress_val);
            }

            info!(
                "Write aggregate spill {} successfully, elapsed: {:?}",
                location,
                instant.elapsed()
            );

            let data_block = DataBlock::new_from_columns(vec![
                Int64Type::from_data(buckets_column_data),
                UInt64Type::from_data(data_range_start_column_data),
                UInt64Type::from_data(data_range_end_column_data),
                ArrayType::upcast_column(ArrayType::<UInt64Type>::column_from_iter(
                    columns_layout_column_data
                        .into_iter()
                        .map(|x| UInt64Type::column_from_iter(x.into_iter(), &[])),
                    &[],
                )),
            ]);

            let data_block = data_block.add_meta(Some(AggregateSerdeMeta::create_agg_spilled(
                -1,
                location.clone(),
                0..0,
                vec![],
                partition_count,
            )))?;

            let ipc_fields = exchange_defines::spilled_ipc_fields();
            let write_options = exchange_defines::spilled_write_options();
            return serialize_block(-1, data_block, ipc_fields, write_options);
        }

        Ok(DataBlock::empty())
    }))
}

fn spilling_group_by_payload<Method: HashMethodBounds>(
    ctx: Arc<QueryContext>,
    operator: Operator,
    method: &Method,
    location_prefix: &str,
    mut payload: HashTablePayload<PartitionedHashMethod<Method>, ()>,
) -> Result<BoxFuture<'static, Result<DataBlock>>> {
    let unique_name = GlobalUniqName::unique();
    let location = format!("{}/{}", location_prefix, unique_name);

    let mut write_size = 0;
    let mut write_data = Vec::with_capacity(256);
    let mut buckets_column_data = Vec::with_capacity(256);
    let mut data_range_start_column_data = Vec::with_capacity(256);
    let mut data_range_end_column_data = Vec::with_capacity(256);
    let mut columns_layout_column_data = Vec::with_capacity(256);
    // Record how many rows are spilled
    let mut rows = 0;

    for (bucket, inner_table) in payload.cell.hashtable.iter_tables_mut().enumerate() {
        if inner_table.len() == 0 {
            continue;
        }

        let data_block = serialize_group_by(method, inner_table)?;
        rows += 0;

        let old_write_size = write_size;
        let columns = get_columns(data_block);
        let mut columns_data = Vec::with_capacity(columns.len());
        let mut columns_layout = Vec::with_capacity(columns.len());

        for column in columns.into_iter() {
            let column = column.value.as_column().unwrap();
            let column_data = serialize_column(column);
            write_size += column_data.len() as u64;
            columns_layout.push(column_data.len() as u64);
            columns_data.push(column_data);
        }

        write_data.push(columns_data);
        buckets_column_data.push(bucket as i64);
        data_range_end_column_data.push(write_size);
        columns_layout_column_data.push(columns_layout);
        data_range_start_column_data.push(old_write_size);
    }

    Ok(Box::pin(async move {
        let instant = Instant::now();

        if !write_data.is_empty() {
            let mut write_bytes = 0;
            let mut writer = operator
                .writer_with(&location)
                .buffer(8 * 1024 * 1024)
                .await?;
            for write_bucket_data in write_data.into_iter() {
                for data in write_bucket_data.into_iter() {
                    write_bytes += data.len();
                    writer.write(data).await?;
                }
            }

            writer.close().await?;

            // perf
            {
                Profile::record_usize_profile(ProfileStatisticsName::SpillWriteCount, 1);
                Profile::record_usize_profile(ProfileStatisticsName::SpillWriteBytes, write_bytes);
                Profile::record_usize_profile(
                    ProfileStatisticsName::SpillWriteTime,
                    instant.elapsed().as_millis() as usize,
                );
            }

            {
                let progress_val = ProgressValues {
                    rows,
                    bytes: write_bytes,
                };
                ctx.get_group_by_spill_progress().incr(&progress_val);
            }

            info!(
                "Write aggregate spill {} successfully, elapsed: {:?}",
                location,
                instant.elapsed()
            );

            let data_block = DataBlock::new_from_columns(vec![
                Int64Type::from_data(buckets_column_data),
                UInt64Type::from_data(data_range_start_column_data),
                UInt64Type::from_data(data_range_end_column_data),
                ArrayType::upcast_column(ArrayType::<UInt64Type>::column_from_iter(
                    columns_layout_column_data
                        .into_iter()
                        .map(|x| UInt64Type::column_from_iter(x.into_iter(), &[])),
                    &[],
                )),
            ]);

            let data_block = data_block.add_meta(Some(AggregateSerdeMeta::create_spilled(
                -1,
                location.clone(),
                0..0,
                vec![],
            )))?;

            let ipc_fields = exchange_defines::spilled_ipc_fields();
            let write_options = exchange_defines::spilled_write_options();
            return serialize_block(-1, data_block, ipc_fields, write_options);
        }

        Ok(DataBlock::empty())
    }))
}
