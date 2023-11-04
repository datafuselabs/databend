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

use common_base::runtime::Runtime;
use common_catalog::plan::PartInfoPtr;
use common_catalog::plan::StealablePartitions;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_pipeline_sources::SyncSource;
use common_pipeline_sources::SyncSourcer;

use super::parquet_data_source::DataSource;
use crate::fuse_part::FusePartInfo;
use crate::io::AggIndexReader;
use crate::io::BlockReader;
use crate::io::ReadSettings;
use crate::io::TableMetaLocationGenerator;
use crate::io::VirtualColumnReader;
use crate::operations::read::parquet_data_source::DataSourceMeta;

pub struct ReadParquetDataSource<const BLOCKING_IO: bool> {
    id: usize,
    finished: bool,
    batch_size: usize,
    block_reader: Arc<BlockReader>,

    output: Arc<OutputPort>,
    output_data: Option<(Vec<PartInfoPtr>, Vec<DataSource>)>,
    partitions: StealablePartitions,

    index_reader: Arc<Option<AggIndexReader>>,
    virtual_reader: Arc<Option<VirtualColumnReader>>,
}

impl<const BLOCKING_IO: bool> ReadParquetDataSource<BLOCKING_IO> {
    pub fn create(
        id: usize,
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        block_reader: Arc<BlockReader>,
        partitions: StealablePartitions,
        index_reader: Arc<Option<AggIndexReader>>,
        virtual_reader: Arc<Option<VirtualColumnReader>>,
    ) -> Result<ProcessorPtr> {
        let batch_size = ctx.get_settings().get_storage_fetch_part_num()? as usize;

        if BLOCKING_IO {
            SyncSourcer::create(ctx.clone(), output.clone(), ReadParquetDataSource::<true> {
                id,
                output,
                batch_size,
                block_reader,
                finished: false,
                output_data: None,
                partitions,
                index_reader,
                virtual_reader,
            })
        } else {
            Ok(ProcessorPtr::create(Box::new(ReadParquetDataSource::<
                false,
            > {
                id,
                output,
                batch_size,
                block_reader,
                finished: false,
                output_data: None,
                partitions,
                index_reader,
                virtual_reader,
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
                if let Some(index_reader) = self.index_reader.as_ref() {
                    let fuse_part = FusePartInfo::from_part(&part)?;
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
                        return Ok(Some(DataBlock::empty_with_meta(DataSourceMeta::create(
                            vec![part.clone()],
                            vec![DataSource::AggIndex(data)],
                        ))));
                    }
                }

                // If virtual column file exists, read the data from the virtual columns directly.
                let virtual_source = if let Some(virtual_reader) = self.virtual_reader.as_ref() {
                    let fuse_part = FusePartInfo::from_part(&part)?;
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

                Ok(Some(DataBlock::empty_with_meta(DataSourceMeta::create(
                    vec![part],
                    vec![DataSource::Normal((source, virtual_source))],
                ))))
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
            let output = DataBlock::empty_with_meta(DataSourceMeta::create(part, data));

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
            for part in &parts {
                let part = part.clone();
                let query_id = self.partitions.ctx.get_id();
                let block_reader = self.block_reader.clone();
                let settings = ReadSettings::from_ctx(&self.partitions.ctx)?;
                let index_reader = self.index_reader.clone();
                let virtual_reader = self.virtual_reader.clone();

                chunks.push(async move {
                    Runtime::spawn_current_runtime(&query_id, async move {
                        let part = FusePartInfo::from_part(&part)?;

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
                                return Ok::<_, ErrorCode>(DataSource::AggIndex(data));
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

                        Ok(DataSource::Normal((source, virtual_source)))
                    })
                        .await
                        .unwrap()
                });
            }

            self.output_data = Some((parts, futures::future::try_join_all(chunks).await?));
            return Ok(());
        }

        self.finished = true;
        Ok(())
    }
}
