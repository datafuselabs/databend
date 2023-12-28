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

use super::native_data_source::DataSource;
use crate::io::AggIndexReader;
use crate::io::BlockReader;
use crate::io::TableMetaLocationGenerator;
use crate::io::VirtualColumnReader;
use crate::operations::read::native_data_source::NativeDataSourceMeta;
use crate::FusePartInfo;

pub struct ReadNativeDataSource<const BLOCKING_IO: bool> {
    block_reader: Arc<BlockReader>,
    index_reader: Arc<Option<AggIndexReader>>,
    virtual_reader: Arc<Option<VirtualColumnReader>>,
    ctx: Arc<dyn TableContext>,
}

impl ReadNativeDataSource<true> {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        block_reader: Arc<BlockReader>,
        index_reader: Arc<Option<AggIndexReader>>,
        virtual_reader: Arc<Option<VirtualColumnReader>>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Transformer::create(
            input.clone(),
            output.clone(),
            ReadNativeDataSource::<true> {
                block_reader,
                index_reader,
                virtual_reader,
                ctx,
            },
        )))
    }
}

impl ReadNativeDataSource<false> {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        block_reader: Arc<BlockReader>,
        index_reader: Arc<Option<AggIndexReader>>,
        virtual_reader: Arc<Option<VirtualColumnReader>>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(AsyncTransformer::create(
            input.clone(),
            output.clone(),
            ReadNativeDataSource::<false> {
                block_reader,
                index_reader,
                virtual_reader,
                ctx,
            },
        )))
    }
}

impl Transform for ReadNativeDataSource<true> {
    const NAME: &'static str = "SyncReadNativeDataSource";

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
            if let Some(data) = index_reader.sync_read_native_data(&loc) {
                // Read from aggregating index.
                return Ok(DataBlock::empty_with_meta(NativeDataSourceMeta::create(
                    vec![part],
                    vec![DataSource::AggIndex(data)],
                )));
            }
        }
        if let Some(virtual_reader) = self.virtual_reader.as_ref() {
            let fuse_part = FusePartInfo::from_part(&part)?;
            let loc = TableMetaLocationGenerator::gen_virtual_block_location(&fuse_part.location);

            // If virtual column file exists, read the data from the virtual columns directly.
            if let Some((mut virtual_source_data, ignore_column_ids)) =
                virtual_reader.sync_read_native_data(&loc)
            {
                let mut source_data = self
                    .block_reader
                    .sync_read_native_columns_data(&part, &ignore_column_ids)?;
                source_data.append(&mut virtual_source_data);
                return Ok(DataBlock::empty_with_meta(NativeDataSourceMeta::create(
                    vec![part.clone()],
                    vec![DataSource::Normal(source_data)],
                )));
            }
        }

        Ok(DataBlock::empty_with_meta(NativeDataSourceMeta::create(
            vec![part.clone()],
            vec![DataSource::Normal(
                self.block_reader
                    .sync_read_native_columns_data(&part, &None)?,
            )],
        )))
    }
}

#[async_trait::async_trait]
impl AsyncTransform for ReadNativeDataSource<false> {
    const NAME: &'static str = "AsyncReadNativeDataSource";

    #[async_backtrace::framed]
    async fn transform(&mut self, mut data: DataBlock) -> Result<DataBlock> {
        let part: PartInfoPtr = Arc::new(Box::new(
            FusePartInfo::downcast_from(data.take_meta().unwrap()).unwrap(),
        ));
        let fuse_part = FusePartInfo::from_part(&part)?;
        if let Some(index_reader) = self.index_reader.as_ref() {
            let loc = TableMetaLocationGenerator::gen_agg_index_location_from_block_location(
                &fuse_part.location,
                index_reader.index_id(),
            );
            if let Some(data) = index_reader.read_native_data(&loc).await {
                // Read from aggregating index.
                return Ok::<_, ErrorCode>(DataBlock::empty_with_meta(
                    NativeDataSourceMeta::create(vec![part], vec![DataSource::AggIndex(data)]),
                ));
            }
        }

        if let Some(virtual_reader) = self.virtual_reader.as_ref() {
            let loc = TableMetaLocationGenerator::gen_virtual_block_location(&fuse_part.location);

            // If virtual column file exists, read the data from the virtual columns directly.
            if let Some((mut virtual_source_data, ignore_column_ids)) =
                virtual_reader.read_native_data(&loc).await
            {
                let mut source_data = self
                    .block_reader
                    .async_read_native_columns_data(&part, &self.ctx, &ignore_column_ids)
                    .await?;
                source_data.append(&mut virtual_source_data);
                return Ok(DataBlock::empty_with_meta(NativeDataSourceMeta::create(
                    vec![part.clone()],
                    vec![DataSource::Normal(source_data)],
                )));
            }
        }

        Ok(DataBlock::empty_with_meta(NativeDataSourceMeta::create(
            vec![part.clone()],
            vec![DataSource::Normal(
                self.block_reader
                    .async_read_native_columns_data(&part, &self.ctx, &None)
                    .await?,
            )],
        )))
    }
}
