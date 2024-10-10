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

use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::TableSchemaRef;
use databend_common_io::constants::DEFAULT_BLOCK_INDEX_BUFFER_SIZE;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_bytes;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_nums;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sinks::AsyncSink;
use databend_common_pipeline_sinks::AsyncSinker;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;
use databend_common_pipeline_transforms::processors::AsyncTransform;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::Location;
use opendal::Operator;

use crate::io::block_to_inverted_index;
use crate::io::write_data;
use crate::io::BlockReader;
use crate::io::InvertedIndexWriter;
use crate::io::MetaReaders;
use crate::io::TableMetaLocationGenerator;
use crate::FuseStorageFormat;
use crate::FuseTable;

impl FuseTable {
    // The big picture of refresh inverted index into pipeline:
    //
    //                                    ┌─────────────────────────┐
    //                             ┌────> │ InvertedIndexTransform1 │ ────┐
    //                             │      └─────────────────────────┘     │
    //                             │                  ...                 │
    // ┌─────────────────────┐     │      ┌─────────────────────────┐     │      ┌───────────────────┐
    // │ InvertedIndexSource │ ────┼────> │ InvertedIndexTransformN │ ────┼────> │ InvertedIndexSink │
    // └─────────────────────┘     │      └─────────────────────────┘     │      └───────────────────┘
    //                             │                  ...                 │
    //                             │      ┌─────────────────────────┐     │
    //                             └────> │ InvertedIndexTransformZ │ ────┘
    //                                    └─────────────────────────┘
    //
    #[inline]
    #[async_backtrace::framed]
    pub async fn do_refresh_inverted_index(
        &self,
        ctx: Arc<dyn TableContext>,
        index_name: String,
        index_version: String,
        index_options: &BTreeMap<String, String>,
        index_schema: TableSchemaRef,
        segment_locs: Option<Vec<Location>>,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let Some(snapshot) = self.read_table_snapshot().await? else {
            return Ok(());
        };

        let table_schema = &self.get_table_info().meta.schema;
        // Collect field indices used by inverted index.
        let mut field_indices = Vec::new();
        for field in &index_schema.fields {
            let field_index = table_schema.index_of(field.name())?;
            field_indices.push(field_index);
        }

        // Read data here to keep the order of blocks in segment.
        let projection = Projection::Columns(field_indices);

        let block_reader =
            self.create_block_reader(ctx.clone(), projection, false, false, false)?;

        let segment_reader =
            MetaReaders::segment_info_reader(self.get_operator(), table_schema.clone());

        // If no segment locations are specified, iterates through all segments
        let segment_locs = if let Some(segment_locs) = segment_locs {
            segment_locs
                .into_iter()
                .filter(|s| snapshot.segments.contains(s))
                .collect()
        } else {
            snapshot.segments.clone()
        };

        if segment_locs.is_empty() {
            return Ok(());
        }
        let operator = self.get_operator_ref();

        // Read the segment infos and collect the block metas that need to generate the index.
        let mut block_metas = VecDeque::new();
        for (segment_loc, ver) in &segment_locs {
            let segment_info = segment_reader
                .read(&LoadParams {
                    location: segment_loc.to_string(),
                    len_hint: None,
                    ver: *ver,
                    put_cache: false,
                })
                .await?;

            for block_meta in segment_info.block_metas()? {
                let index_location =
                    TableMetaLocationGenerator::gen_inverted_index_location_from_block_location(
                        &block_meta.location.0,
                        &index_name,
                        &index_version,
                    );
                // only generate inverted index if it is not exist.
                if (operator.stat(&index_location).await).is_err() {
                    block_metas.push_back(block_meta);
                }
            }
        }
        if block_metas.is_empty() {
            return Ok(());
        }

        let data_schema = Arc::new(DataSchema::from(index_schema.as_ref()));
        let settings = ReadSettings::from_ctx(&ctx)?;
        let write_settings = self.get_write_settings();
        let storage_format = write_settings.storage_format;

        pipeline.add_source(
            |output| {
                let inner = InvertedIndexSource::new(
                    settings,
                    storage_format,
                    block_reader.clone(),
                    block_metas.clone(),
                );
                AsyncSourcer::create(ctx.clone(), output, inner)
            },
            1,
        )?;

        let block_nums = block_metas.len();
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let max_threads = std::cmp::min(block_nums, max_threads);
        pipeline.try_resize(max_threads)?;
        pipeline.add_async_transformer(|| {
            InvertedIndexTransform::new(
                index_name.clone(),
                index_version.clone(),
                index_options.clone(),
                data_schema.clone(),
                index_schema.clone(),
                operator.clone(),
            )
        });

        pipeline.try_resize(1)?;
        pipeline.add_sink(|input| InvertedIndexSink::try_create(input, block_nums))?;

        Ok(())
    }
}

/// `InvertedIndexSource` is used to read data blocks that need generate inverted indexes.
pub struct InvertedIndexSource {
    settings: ReadSettings,
    storage_format: FuseStorageFormat,
    block_reader: Arc<BlockReader>,
    block_metas: VecDeque<Arc<BlockMeta>>,
    is_finished: bool,
}

impl InvertedIndexSource {
    pub fn new(
        settings: ReadSettings,
        storage_format: FuseStorageFormat,
        block_reader: Arc<BlockReader>,
        block_metas: VecDeque<Arc<BlockMeta>>,
    ) -> Self {
        Self {
            settings,
            storage_format,
            block_reader,
            block_metas,
            is_finished: false,
        }
    }
}

#[async_trait::async_trait]
impl AsyncSource for InvertedIndexSource {
    const NAME: &'static str = "InvertedIndexSource";

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.is_finished {
            return Ok(None);
        }

        match self.block_metas.pop_front() {
            Some(block_meta) => {
                let block = self
                    .block_reader
                    .read_by_meta(&self.settings, &block_meta, &self.storage_format)
                    .await?;
                let block = block.add_meta(Some(Box::new(Arc::unwrap_or_clone(block_meta))))?;
                Ok(Some(block))
            }
            None => {
                self.is_finished = true;
                Ok(None)
            }
        }
    }
}

/// `InvertedIndexTransform` is used to generate inverted index for each blocks.
pub struct InvertedIndexTransform {
    index_name: String,
    index_version: String,
    index_options: BTreeMap<String, String>,
    data_schema: DataSchemaRef,
    source_schema: TableSchemaRef,
    operator: Operator,
}

impl InvertedIndexTransform {
    pub fn new(
        index_name: String,
        index_version: String,
        index_options: BTreeMap<String, String>,
        data_schema: DataSchemaRef,
        source_schema: TableSchemaRef,
        operator: Operator,
    ) -> Self {
        Self {
            index_name,
            index_version,
            index_options,
            data_schema,
            source_schema,
            operator,
        }
    }
}

#[async_trait::async_trait]
impl AsyncTransform for InvertedIndexTransform {
    const NAME: &'static str = "InvertedIndexTransform";

    #[async_backtrace::framed]
    async fn transform(&mut self, data_block: DataBlock) -> Result<DataBlock> {
        let block_meta = data_block
            .get_meta()
            .and_then(BlockMeta::downcast_ref_from)
            .unwrap();

        let index_location =
            TableMetaLocationGenerator::gen_inverted_index_location_from_block_location(
                &block_meta.location.0,
                &self.index_name,
                &self.index_version,
            );

        let start = Instant::now();
        let mut writer =
            InvertedIndexWriter::try_create(self.data_schema.clone(), &self.index_options)?;
        writer.add_block(&self.source_schema, &data_block)?;

        let (index_schema, index_block) = writer.finalize()?;
        let mut data = Vec::with_capacity(DEFAULT_BLOCK_INDEX_BUFFER_SIZE);
        let _ = block_to_inverted_index(&index_schema, index_block, &mut data)?;
        let index_size = data.len() as u64;
        write_data(data, &self.operator, &index_location).await?;

        // Perf.
        {
            metrics_inc_block_inverted_index_write_nums(1);
            metrics_inc_block_inverted_index_write_bytes(index_size);
            metrics_inc_block_inverted_index_write_milliseconds(start.elapsed().as_millis() as u64);
        }

        let new_block = DataBlock::new(vec![], 0);
        Ok(new_block)
    }
}

/// `InvertedIndexSink` is used to build inverted index.
pub struct InvertedIndexSink {
    block_nums: AtomicUsize,
}

impl InvertedIndexSink {
    pub fn try_create(input: Arc<InputPort>, block_nums: usize) -> Result<ProcessorPtr> {
        let sinker = AsyncSinker::create(input, InvertedIndexSink {
            block_nums: AtomicUsize::new(block_nums),
        });
        Ok(ProcessorPtr::create(sinker))
    }
}

#[async_trait]
impl AsyncSink for InvertedIndexSink {
    const NAME: &'static str = "InvertedIndexSink";

    #[async_backtrace::framed]
    async fn consume(&mut self, _data_block: DataBlock) -> Result<bool> {
        let num = self.block_nums.fetch_sub(1, Ordering::SeqCst);
        if num > 1 {
            return Ok(false);
        }

        Ok(true)
    }
}
