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

use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_transforms::processors::AsyncTransform;
use databend_common_pipeline_transforms::processors::AsyncTransformer;
use databend_common_pipeline_transforms::processors::Transform;
use databend_common_pipeline_transforms::processors::Transformer;

use super::parquet_data_source::DataSource;
use crate::fuse_part::FusePartInfo;
use crate::io::AggIndexReader;
use crate::io::BlockReader;
use crate::io::ReadSettings;
use crate::io::TableMetaLocationGenerator;
use crate::io::VirtualColumnReader;
use crate::operations::read::parquet_data_source::DataSourceMeta;

pub struct ReadParquetDataSource<const BLOCKING_IO: bool> {
    block_reader: Arc<BlockReader>,
    index_reader: Arc<Option<AggIndexReader>>,
    virtual_reader: Arc<Option<VirtualColumnReader>>,
    ctx: Arc<dyn TableContext>,
}

impl<const BLOCKING_IO: bool> ReadParquetDataSource<BLOCKING_IO> {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        ctx: Arc<dyn TableContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        block_reader: Arc<BlockReader>,
        index_reader: Arc<Option<AggIndexReader>>,
        virtual_reader: Arc<Option<VirtualColumnReader>>,
    ) -> Result<ProcessorPtr> {
        let transformer = if BLOCKING_IO {
            Transformer::create(
                input.clone(),
                output.clone(),
                ReadParquetDataSource::<true> {
                    block_reader,
                    index_reader,
                    virtual_reader,
                    ctx,
                },
            )
        } else {
            AsyncTransformer::create(input.clone(), output.clone(), ReadParquetDataSource::<
                false,
            > {
                block_reader,
                index_reader,
                virtual_reader,
                ctx,
            })
        };
        Ok(ProcessorPtr::create(transformer))
    }
}

impl Transform for ReadParquetDataSource<true> {
    const NAME: &'static str = "SyncReadParquetDataSource";

    fn transform(&mut self, mut data: DataBlock) -> Result<DataBlock> {
        let part: PartInfoPtr = Arc::new(Box::new(
            FusePartInfo::downcast_from(data.take_meta().unwrap()).unwrap(),
        ));
        let fuse_part = FusePartInfo::from_part(&part)?;
        if let Some(index_reader) = self.index_reader.as_ref() {
            let loc = TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
                &fuse_part.location,
                index_reader.index_id(),
            );
            if let Some(data) = index_reader
                .sync_read_parquet_data_by_merge_io(&ReadSettings::from_ctx(&self.ctx)?, &loc)
            {
                // Read from aggregating index.
                return Ok(DataBlock::empty_with_meta(DataSourceMeta::create(
                    vec![part],
                    vec![DataSource::AggIndex(data)],
                )));
            }
        }
        // If virtual column file exists, read the data from the virtual columns directly.
        let virtual_source = if let Some(virtual_reader) = self.virtual_reader.as_ref() {
            let loc = TableMetaLocationGenerator::gen_virtual_block_location(&fuse_part.location);
            virtual_reader
                .sync_read_parquet_data_by_merge_io(&ReadSettings::from_ctx(&self.ctx)?, &loc)
        } else {
            None
        };

        let ignore_column_ids = if let Some(virtual_source) = &virtual_source {
            &virtual_source.ignore_column_ids
        } else {
            &None
        };

        let source = self.block_reader.sync_read_columns_data_by_merge_io(
            &ReadSettings::from_ctx(&self.ctx)?,
            &part,
            ignore_column_ids,
        )?;

        Ok(DataBlock::empty_with_meta(DataSourceMeta::create(
            vec![part],
            vec![DataSource::Normal((source, virtual_source))],
        )))
    }
}

#[async_trait::async_trait]
impl AsyncTransform for ReadParquetDataSource<false> {
    const NAME: &'static str = "AsyncReadParquetDataSource";

    #[async_backtrace::framed]
    async fn transform(&mut self, mut data: DataBlock) -> Result<DataBlock> {
        let part: PartInfoPtr = Arc::new(Box::new(
            FusePartInfo::downcast_from(data.take_meta().unwrap()).unwrap(),
        ));
        let fuse_part = FusePartInfo::from_part(&part)?;
        let settings = ReadSettings::from_ctx(&self.ctx)?;
        if let Some(index_reader) = self.index_reader.as_ref() {
            let loc = TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
                &fuse_part.location,
                index_reader.index_id(),
            );
            if let Some(data) = index_reader
                .read_parquet_data_by_merge_io(&settings, &loc)
                .await
            {
                // Read from aggregating index.
                return Ok::<_, ErrorCode>(DataBlock::empty_with_meta(DataSourceMeta::create(
                    vec![part],
                    vec![DataSource::AggIndex(data)],
                )));
            }
        }

        // If virtual column file exists, read the data from the virtual columns directly.
        let virtual_source = if let Some(virtual_reader) = self.virtual_reader.as_ref() {
            let loc = TableMetaLocationGenerator::gen_virtual_block_location(&fuse_part.location);

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

        let source = self
            .block_reader
            .read_columns_data_by_merge_io(
                &settings,
                &fuse_part.location,
                &fuse_part.columns_meta,
                ignore_column_ids,
            )
            .await?;

        Ok(DataBlock::empty_with_meta(DataSourceMeta::create(
            vec![part],
            vec![DataSource::Normal((source, virtual_source))],
        )))
    }
}
