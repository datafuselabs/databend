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
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use chrono::Utc;
use databend_common_arrow::arrow::chunk::Chunk as ArrowChunk;
use databend_common_arrow::native::write::NativeWriter;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_io::constants::DEFAULT_BLOCK_BUFFER_SIZE;
use databend_common_io::constants::DEFAULT_BLOCK_INDEX_BUFFER_SIZE;
use databend_common_meta_app::schema::TableMeta;
use databend_common_metrics::storage::metrics_inc_block_index_write_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_index_write_nums;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_generate_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_bytes;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_write_nums;
use databend_common_metrics::storage::metrics_inc_block_write_milliseconds;
use databend_common_metrics::storage::metrics_inc_block_write_nums;
use databend_storages_common_blocks::blocks_to_parquet;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::table::TableCompression;
use log::info;
use opendal::Operator;

use crate::io::write::WriteSettings;
use crate::io::BlockReader;
use crate::io::InvertedIndexWriter;
use crate::io::ReadSettings;
use crate::io::TableMetaLocationGenerator;
use crate::operations::column_parquet_metas;
use crate::statistics::gen_columns_statistics;
use crate::statistics::ClusterStatsGenerator;
use crate::FuseStorageFormat;

// TODO rename this, it is serialization, or pass in a writer(if not rename)
pub fn serialize_block(
    write_settings: &WriteSettings,
    schema: &TableSchemaRef,
    block: DataBlock,
    buf: &mut Vec<u8>,
) -> Result<HashMap<ColumnId, ColumnMeta>> {
    let schema = Arc::new(schema.remove_virtual_computed_fields());
    match write_settings.storage_format {
        FuseStorageFormat::Parquet => {
            let result =
                blocks_to_parquet(&schema, vec![block], buf, write_settings.table_compression)?;
            let meta = column_parquet_metas(&result, &schema)?;
            Ok(meta)
        }
        FuseStorageFormat::Native => {
            let arrow_schema = schema.as_ref().into();
            let leaf_column_ids = schema.to_leaf_column_ids();

            let mut default_compress_ratio = Some(2.10f64);
            if matches!(write_settings.table_compression, TableCompression::Zstd) {
                default_compress_ratio = Some(3.72f64);
            }

            let mut writer = NativeWriter::new(
                buf,
                arrow_schema,
                databend_common_arrow::native::write::WriteOptions {
                    default_compression: write_settings.table_compression.into(),
                    max_page_size: Some(write_settings.max_page_size),
                    default_compress_ratio,
                    forbidden_compressions: vec![],
                },
            );

            let batch = ArrowChunk::try_from(block)?;

            writer.start()?;
            writer.write(&batch)?;
            writer.finish()?;

            let mut metas = HashMap::with_capacity(writer.metas.len());
            for (idx, meta) in writer.metas.iter().enumerate() {
                // use column id as key instead of index
                let column_id = leaf_column_ids.get(idx).unwrap();
                metas.insert(*column_id, ColumnMeta::Native(meta.clone()));
            }

            Ok(metas)
        }
    }
}

/// Take ownership here to avoid extra copy.
#[async_backtrace::framed]
pub async fn write_data(data: Vec<u8>, data_accessor: &Operator, location: &str) -> Result<()> {
    data_accessor.write(location, data).await?;

    Ok(())
}

pub struct BloomIndexState {
    pub(crate) data: Vec<u8>,
    pub(crate) size: u64,
    pub(crate) location: Location,
    pub(crate) column_distinct_count: HashMap<FieldIndex, usize>,
}

pub struct BloomIndexBuilder {
    pub table_ctx: Arc<dyn TableContext>,
    pub table_schema: TableSchemaRef,
    pub table_dal: Operator,
    pub storage_format: FuseStorageFormat,
    pub bloom_columns_map: BTreeMap<FieldIndex, TableField>,
}

impl BloomIndexBuilder {
    pub fn bloom_index_state_from_data_block(
        &self,
        block: &DataBlock,
        bloom_location: Location,
    ) -> Result<Option<(BloomIndexState, BloomIndex)>> {
        let maybe_bloom_index = BloomIndex::try_create(
            self.table_ctx.get_function_context()?,
            bloom_location.1,
            block,
            self.bloom_columns_map.clone(),
        )?;

        match maybe_bloom_index {
            None => Ok(None),
            Some(bloom_index) => Ok(Some((
                BloomIndexState::from_bloom_index(&bloom_index, bloom_location)?,
                bloom_index,
            ))),
        }
    }

    pub async fn bloom_index_state_from_block_meta(
        &self,
        block_meta: &BlockMeta,
    ) -> Result<Option<(BloomIndexState, BloomIndex)>> {
        let ctx = self.table_ctx.clone();

        // the caller should not pass a block meta without a bloom index location here.
        assert!(block_meta.bloom_filter_index_location.is_some());

        let projection =
            Projection::Columns((0..self.table_schema.fields().len()).collect::<Vec<usize>>());

        let block_reader = BlockReader::create(
            ctx,
            self.table_dal.clone(),
            self.table_schema.clone(),
            projection,
            false,
            false,
            false,
        )?;

        let settings = ReadSettings::from_ctx(&self.table_ctx)?;

        let data_block = block_reader
            .read_by_meta(&settings, block_meta, &self.storage_format)
            .await?;

        self.bloom_index_state_from_data_block(
            &data_block,
            block_meta.bloom_filter_index_location.clone().unwrap(),
        )
    }
}

impl BloomIndexState {
    pub fn from_bloom_index(bloom_index: &BloomIndex, location: Location) -> Result<Self> {
        let index_block = bloom_index.serialize_to_data_block()?;
        let filter_schema = &bloom_index.filter_schema;
        let column_distinct_count = bloom_index.column_distinct_count.clone();
        let mut data = Vec::with_capacity(DEFAULT_BLOCK_INDEX_BUFFER_SIZE);
        let index_block_schema = filter_schema;
        let _ = blocks_to_parquet(
            index_block_schema,
            vec![index_block],
            &mut data,
            TableCompression::None,
        )?;
        let data_size = data.len() as u64;
        Ok(Self {
            data,
            size: data_size,
            location,
            column_distinct_count,
        })
    }
    pub fn from_data_block(
        ctx: Arc<dyn TableContext>,
        block: &DataBlock,
        location: Location,
        bloom_columns_map: BTreeMap<FieldIndex, TableField>,
    ) -> Result<Option<Self>> {
        // write index
        let maybe_bloom_index = BloomIndex::try_create(
            ctx.get_function_context()?,
            location.1,
            block,
            bloom_columns_map,
        )?;
        if let Some(bloom_index) = maybe_bloom_index {
            Ok(Some(Self::from_bloom_index(&bloom_index, location)?))
        } else {
            Ok(None)
        }
    }
}

#[derive(Clone)]
pub struct InvertedIndexBuilder {
    pub(crate) name: String,
    pub(crate) version: String,
    pub(crate) schema: DataSchema,
    pub(crate) options: BTreeMap<String, String>,
}

pub fn create_inverted_index_builders(table_meta: &TableMeta) -> Vec<InvertedIndexBuilder> {
    let mut inverted_index_builders = Vec::with_capacity(table_meta.indexes.len());
    for index in table_meta.indexes.values() {
        if !index.sync_creation {
            continue;
        }
        let mut index_fields = Vec::with_capacity(index.column_ids.len());
        for column_id in &index.column_ids {
            for field in &table_meta.schema.fields {
                if field.column_id() == *column_id {
                    index_fields.push(DataField::from(field));
                    break;
                }
            }
        }
        // ignore invalid index
        if index_fields.len() != index.column_ids.len() {
            continue;
        }
        let index_schema = DataSchema::new(index_fields);

        let inverted_index_builder = InvertedIndexBuilder {
            name: index.name.clone(),
            version: index.version.clone(),
            schema: index_schema,
            options: index.options.clone(),
        };
        inverted_index_builders.push(inverted_index_builder);
    }
    inverted_index_builders
}

pub struct InvertedIndexState {
    pub(crate) data: Vec<u8>,
    pub(crate) size: u64,
    pub(crate) location: Location,
}

impl InvertedIndexState {
    pub fn try_create(
        source_schema: &TableSchemaRef,
        block: &DataBlock,
        block_location: &Location,
        inverted_index_builder: &InvertedIndexBuilder,
    ) -> Result<Self> {
        let start = Instant::now();
        let mut writer = InvertedIndexWriter::try_create(
            Arc::new(inverted_index_builder.schema.clone()),
            &inverted_index_builder.options,
        )?;
        writer.add_block(source_schema, block)?;
        let data = writer.finalize()?;
        let size = data.len() as u64;

        // Perf.
        {
            metrics_inc_block_inverted_index_generate_milliseconds(
                start.elapsed().as_millis() as u64
            );
        }

        let inverted_index_location =
            TableMetaLocationGenerator::gen_inverted_index_location_from_block_location(
                &block_location.0,
                &inverted_index_builder.name,
                &inverted_index_builder.version,
            );

        Ok(Self {
            data,
            size,
            location: (inverted_index_location, 0),
        })
    }
}

pub struct BlockSerialization {
    pub block_raw_data: Vec<u8>,
    pub size: u64, // TODO redundancy
    pub block_meta: BlockMeta,
    pub bloom_index_state: Option<BloomIndexState>,
    pub inverted_index_states: Vec<InvertedIndexState>,
}

#[derive(Clone)]
pub struct BlockBuilder {
    pub ctx: Arc<dyn TableContext>,
    pub meta_locations: TableMetaLocationGenerator,
    pub source_schema: TableSchemaRef,
    pub write_settings: WriteSettings,
    pub cluster_stats_gen: ClusterStatsGenerator,
    pub bloom_columns_map: BTreeMap<FieldIndex, TableField>,
    pub inverted_index_builders: Vec<InvertedIndexBuilder>,
    pub table_meta_timestamps: TableMetaTimestamps,
}

impl BlockBuilder {
    pub fn build<F>(&self, data_block: DataBlock, f: F) -> Result<BlockSerialization>
    where F: Fn(DataBlock, &ClusterStatsGenerator) -> Result<(Option<ClusterStatistics>, DataBlock)>
    {
        let (cluster_stats, data_block) = f(data_block, &self.cluster_stats_gen)?;
        let (block_location, block_id) = self
            .meta_locations
            .gen_block_location(self.table_meta_timestamps);

        let bloom_index_location = self.meta_locations.block_bloom_index_location(&block_id);
        let bloom_index_state = BloomIndexState::from_data_block(
            self.ctx.clone(),
            &data_block,
            bloom_index_location,
            self.bloom_columns_map.clone(),
        )?;
        let column_distinct_count = bloom_index_state
            .as_ref()
            .map(|i| i.column_distinct_count.clone());

        let mut inverted_index_states = Vec::with_capacity(self.inverted_index_builders.len());
        for inverted_index_builder in &self.inverted_index_builders {
            let inverted_index_state = InvertedIndexState::try_create(
                &self.source_schema,
                &data_block,
                &block_location,
                inverted_index_builder,
            )?;
            inverted_index_states.push(inverted_index_state);
        }

        let row_count = data_block.num_rows() as u64;
        let block_size = data_block.memory_size() as u64;
        let col_stats =
            gen_columns_statistics(&data_block, column_distinct_count, &self.source_schema)?;

        let mut buffer = Vec::with_capacity(DEFAULT_BLOCK_BUFFER_SIZE);
        let col_metas = serialize_block(
            &self.write_settings,
            &self.source_schema,
            data_block,
            &mut buffer,
        )?;
        let file_size = buffer.len() as u64;
        let inverted_index_size = if !inverted_index_states.is_empty() {
            let size = inverted_index_states.iter().map(|v| v.size).sum();
            Some(size)
        } else {
            None
        };
        let block_meta = BlockMeta {
            row_count,
            block_size,
            file_size,
            col_stats,
            col_metas,
            cluster_stats,
            location: block_location,
            bloom_filter_index_location: bloom_index_state.as_ref().map(|v| v.location.clone()),
            bloom_filter_index_size: bloom_index_state
                .as_ref()
                .map(|v| v.size)
                .unwrap_or_default(),
            compression: self.write_settings.table_compression.into(),
            inverted_index_size,
            create_on: Some(Utc::now()),
        };

        let serialized = BlockSerialization {
            block_raw_data: buffer,
            size: file_size,
            block_meta,
            bloom_index_state,
            inverted_index_states,
        };
        Ok(serialized)
    }
}

pub struct BlockWriter;

impl BlockWriter {
    pub async fn write_down(dal: &Operator, serialized: BlockSerialization) -> Result<BlockMeta> {
        let block_meta = serialized.block_meta;

        Self::write_down_data_block(dal, serialized.block_raw_data, &block_meta.location.0).await?;
        Self::write_down_bloom_index_state(dal, serialized.bloom_index_state).await?;
        Self::write_down_inverted_index_state(dal, serialized.inverted_index_states).await?;

        Ok(block_meta)
    }

    pub async fn write_down_data_block(
        dal: &Operator,
        raw_block_data: Vec<u8>,
        block_location: &str,
    ) -> Result<()> {
        let start = Instant::now();
        let size = raw_block_data.len();

        write_data(raw_block_data, dal, block_location).await?;

        metrics_inc_block_write_nums(1);
        metrics_inc_block_write_nums(size as u64);
        metrics_inc_block_write_milliseconds(start.elapsed().as_millis() as u64);

        Ok(())
    }

    pub async fn write_down_bloom_index_state(
        dal: &Operator,
        bloom_index_state: Option<BloomIndexState>,
    ) -> Result<()> {
        if let Some(index_state) = bloom_index_state {
            let start = Instant::now();

            let location = &index_state.location.0;
            write_data(index_state.data, dal, location).await?;

            metrics_inc_block_index_write_nums(1);
            metrics_inc_block_index_write_nums(index_state.size);
            metrics_inc_block_index_write_milliseconds(start.elapsed().as_millis() as u64);

            info!(
                "wrote down bloom index: {}, use {} secs",
                location,
                start.elapsed().as_secs_f32()
            );
        }
        Ok(())
    }

    pub async fn write_down_inverted_index_state(
        dal: &Operator,
        inverted_index_states: Vec<InvertedIndexState>,
    ) -> Result<()> {
        for inverted_index_state in inverted_index_states {
            let start = Instant::now();

            let location = &inverted_index_state.location.0;
            let index_size = inverted_index_state.size;
            write_data(inverted_index_state.data, dal, location).await?;
            metrics_inc_block_inverted_index_write_nums(1);
            metrics_inc_block_inverted_index_write_bytes(index_size);
            metrics_inc_block_inverted_index_write_milliseconds(start.elapsed().as_millis() as u64);

            info!("wrote down inverted index: {}", location);
        }
        Ok(())
    }
}
