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
use std::sync::Arc;

use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::plan::StealablePartitions;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_expression::TableSchema;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sources::SyncSource;
use databend_common_pipeline_sources::SyncSourcer;
use databend_common_sql::IndexType;
use log::debug;

use super::parquet_data_source::ParquetDataSource;
use super::util::build_merge_into_source_build_bloom_info;
use super::util::MergeIntoSourceBuildBloomInfo;
use crate::fuse_part::FuseBlockPartInfo;
use crate::io::AggIndexReader;
use crate::io::BlockReader;
use crate::io::ReadSettings;
use crate::io::TableMetaLocationGenerator;
use crate::io::VirtualColumnReader;
use crate::operations::read::data_source_with_meta::DataSourceWithMeta;
use crate::operations::read::runtime_filter_prunner::runtime_filter_pruner;

pub struct ReadParquetDataSource<const BLOCKING_IO: bool> {
    func_ctx: FunctionContext,
    id: usize,
    // build table index, not probe side table index
    table_index: IndexType,
    finished: bool,
    batch_size: usize,
    block_reader: Arc<BlockReader>,

    output: Arc<OutputPort>,
    output_data: Option<(Vec<PartInfoPtr>, Vec<ParquetDataSource>)>,
    partitions: StealablePartitions,

    index_reader: Arc<Option<AggIndexReader>>,
    virtual_reader: Arc<Option<VirtualColumnReader>>,

    table_schema: Arc<TableSchema>,

    merge_into_source_build_bloom_info: MergeIntoSourceBuildBloomInfo,
}

impl<const BLOCKING_IO: bool> ReadParquetDataSource<BLOCKING_IO> {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        id: usize,
        table_index: IndexType,
        ctx: Arc<dyn TableContext>,
        table_schema: Arc<TableSchema>,
        output: Arc<OutputPort>,
        block_reader: Arc<BlockReader>,
        partitions: StealablePartitions,
        index_reader: Arc<Option<AggIndexReader>>,
        virtual_reader: Arc<Option<VirtualColumnReader>>,
    ) -> Result<ProcessorPtr> {
        let batch_size = ctx.get_settings().get_storage_fetch_part_num()? as usize;
        let func_ctx = ctx.get_function_context()?;
        if BLOCKING_IO {
            let merge_into_join = ctx.get_merge_into_join();
            SyncSourcer::create(ctx.clone(), output.clone(), ReadParquetDataSource::<true> {
                func_ctx,
                id,
                table_index,
                output,
                batch_size,
                block_reader,
                finished: false,
                output_data: None,
                partitions,
                index_reader,
                virtual_reader,
                table_schema,
                merge_into_source_build_bloom_info: build_merge_into_source_build_bloom_info(
                    ctx,
                    table_index,
                    merge_into_join,
                )?,
            })
        } else {
            let merge_into_join = ctx.get_merge_into_join();
            Ok(ProcessorPtr::create(Box::new(ReadParquetDataSource::<
                false,
            > {
                func_ctx,
                id,
                table_index,
                output,
                batch_size,
                block_reader,
                finished: false,
                output_data: None,
                partitions,
                index_reader,
                virtual_reader,
                table_schema,
                merge_into_source_build_bloom_info: build_merge_into_source_build_bloom_info(
                    ctx,
                    table_index,
                    merge_into_join,
                )?,
            })))
        }
    }
}

impl SyncSource for ReadParquetDataSource<true> {
    const NAME: &'static str = "SyncReadParquetDataSource";

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        match self.partitions.steal_one(self.id) {
            None => Ok(None),
            Some(part) => {
                let mut filters = self
                    .partitions
                    .ctx
                    .get_inlist_runtime_filter_with_id(self.table_index);
                filters.extend(
                    self.partitions
                        .ctx
                        .get_min_max_runtime_filter_with_id(self.table_index),
                );
                if runtime_filter_pruner(
                    self.table_schema.clone(),
                    &part,
                    &filters,
                    &self.func_ctx,
                    self.merge_into_source_build_bloom_info
                        .can_do_merge_into_runtime_filter_bloom,
                    self.partitions.ctx.clone(),
                    self.table_index,
                    &mut self.merge_into_source_build_bloom_info,
                )? {
                    return Ok(Some(DataBlock::empty()));
                }

                if let Some(index_reader) = self.index_reader.as_ref() {
                    let fuse_part = FuseBlockPartInfo::from_part(&part)?;
                    let loc =
                        TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
                            &fuse_part.location,
                            index_reader.index_id(),
                        );
                    if let Some(data) = index_reader.sync_read_parquet_data_by_merge_io(
                        &ReadSettings::from_ctx(&self.partitions.ctx)?,
                        &loc,
                    ) {
                        // Read from aggregating index.
                        return Ok(Some(DataBlock::empty_with_meta(
                            DataSourceWithMeta::create(vec![part.clone()], vec![
                                ParquetDataSource::AggIndex(data),
                            ]),
                        )));
                    }
                }

                // If virtual column file exists, read the data from the virtual columns directly.
                let virtual_source = if let Some(virtual_reader) = self.virtual_reader.as_ref() {
                    let fuse_part = FuseBlockPartInfo::from_part(&part)?;
                    let loc =
                        TableMetaLocationGenerator::gen_virtual_block_location(&fuse_part.location);

                    virtual_reader.sync_read_parquet_data_by_merge_io(
                        &ReadSettings::from_ctx(&self.partitions.ctx)?,
                        &loc,
                    )
                } else {
                    None
                };

                let ignore_column_ids = if let Some(virtual_source) = &virtual_source {
                    &virtual_source.ignore_column_ids
                } else {
                    &None
                };

                let source = self.block_reader.sync_read_columns_data_by_merge_io(
                    &ReadSettings::from_ctx(&self.partitions.ctx)?,
                    &part,
                    ignore_column_ids,
                )?;

                Ok(Some(DataBlock::empty_with_meta(
                    DataSourceWithMeta::create(vec![part], vec![ParquetDataSource::Normal((
                        source,
                        virtual_source,
                    ))]),
                )))
            }
        }
    }
}

#[async_trait::async_trait]
impl Processor for ReadParquetDataSource<false> {
    fn name(&self) -> String {
        String::from("AsyncReadParquetDataSource")
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.finished {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if let Some((part, data)) = self.output_data.take() {
            let output = DataBlock::empty_with_meta(DataSourceWithMeta::create(part, data));

            self.output.push_data(Ok(output));
            // return Ok(Event::NeedConsume);
        }

        Ok(Event::Async)
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        let parts = self.partitions.steal(self.id, self.batch_size);
        if !parts.is_empty() {
            let mut chunks = Vec::with_capacity(parts.len());
            let mut filters = self
                .partitions
                .ctx
                .get_inlist_runtime_filter_with_id(self.table_index);
            filters.extend(
                self.partitions
                    .ctx
                    .get_min_max_runtime_filter_with_id(self.table_index),
            );
            let mut fuse_part_infos = Vec::with_capacity(parts.len());
            for part in parts.into_iter() {
                if runtime_filter_pruner(
                    self.table_schema.clone(),
                    &part,
                    &filters,
                    &self.func_ctx,
                    self.merge_into_source_build_bloom_info
                        .can_do_merge_into_runtime_filter_bloom,
                    self.partitions.ctx.clone(),
                    self.table_index,
                    &mut self.merge_into_source_build_bloom_info,
                )? {
                    continue;
                }

                fuse_part_infos.push(part.clone());
                let block_reader = self.block_reader.clone();
                let settings = ReadSettings::from_ctx(&self.partitions.ctx)?;
                let index_reader = self.index_reader.clone();
                let virtual_reader = self.virtual_reader.clone();

                chunks.push(async move {
                    databend_common_base::runtime::spawn(async move {
                        let part = FuseBlockPartInfo::from_part(&part)?;

                        if let Some(index_reader) = index_reader.as_ref() {
                            let loc =
                                TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
                                    &part.location,
                                    index_reader.index_id(),
                                );
                            if let Some(data) = index_reader
                                .read_parquet_data_by_merge_io(&settings, &loc)
                                .await
                            {
                                // Read from aggregating index.
                                return Ok::<_, ErrorCode>(ParquetDataSource::AggIndex(data));
                            }
                        }

                        // If virtual column file exists, read the data from the virtual columns directly.
                        let virtual_source = if let Some(virtual_reader) = virtual_reader.as_ref() {
                            let loc = TableMetaLocationGenerator::gen_virtual_block_location(
                                &part.location,
                            );

                            virtual_reader
                                .read_parquet_data_by_merge_io(&settings, &loc)
                                .await
                        } else {
                            None
                        };

                        let ignore_column_ids = if let Some(virtual_source) = &virtual_source {
                            &virtual_source.ignore_column_ids
                        } else {
                            &None
                        };

                        let source = block_reader
                            .read_columns_data_by_merge_io(
                                &settings,
                                &part.location,
                                &part.columns_meta,
                                ignore_column_ids,
                            )
                            .await?;

                        Ok(ParquetDataSource::Normal((source, virtual_source)))
                    })
                        .await
                        .unwrap()
                });
            }

            debug!("ReadParquetDataSource parts: {}", chunks.len());
            self.output_data = Some((
                fuse_part_infos,
                futures::future::try_join_all(chunks).await?,
            ));
            return Ok(());
        }

        self.finished = true;
        Ok(())
    }
}
