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

use chrono::Utc;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::FunctionContext;
use databend_common_expression::TableSchemaRef;
use databend_common_io::constants::DEFAULT_BLOCK_BUFFER_SIZE;
use databend_common_io::constants::DEFAULT_BLOCK_INDEX_BUFFER_SIZE;
use databend_common_sql::BloomIndexColumns;
use databend_common_storages_fuse::io::serialize_block;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::io::WriteSettings;
use databend_common_storages_fuse::FuseStorageFormat;
use databend_storages_common_blocks::blocks_to_parquet;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::Compression;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::table::TableCompression;
use opendal::Operator;
use parquet::format::FileMetaData;
use uuid::Uuid;

use super::old_version_generator;
pub struct BlockWriter<'a> {
    location_generator: &'a TableMetaLocationGenerator,
    data_accessor: &'a Operator,
    table_meta_timestamps: TableMetaTimestamps,
    is_greater_than_v5: bool,
}

impl<'a> BlockWriter<'a> {
    pub fn new(
        data_accessor: &'a Operator,
        location_generator: &'a TableMetaLocationGenerator,
        table_meta_timestamps: TableMetaTimestamps,
        is_greater_than_v5: bool,
    ) -> Self {
        Self {
            location_generator,
            data_accessor,
            table_meta_timestamps,
            is_greater_than_v5,
        }
    }

    pub async fn write(
        &self,
        storage_format: FuseStorageFormat,
        schema: &TableSchemaRef,
        block: DataBlock,
        col_stats: StatisticsOfColumns,
        cluster_stats: Option<ClusterStatistics>,
    ) -> Result<(BlockMeta, Option<FileMetaData>)> {
        let (location, block_id) = if !self.is_greater_than_v5 {
            let location_generator = old_version_generator::TableMetaLocationGenerator::with_prefix(
                self.location_generator.prefix().to_string(),
            );
            location_generator.gen_block_location(self.table_meta_timestamps)
        } else {
            self.location_generator
                .gen_block_location(self.table_meta_timestamps)
        };

        let data_accessor = &self.data_accessor;
        let row_count = block.num_rows() as u64;
        let block_size = block.memory_size() as u64;
        let (bloom_filter_index_size, bloom_filter_index_location, meta) = self
            .build_block_index(data_accessor, schema.clone(), &block, block_id)
            .await?;

        let write_settings = WriteSettings {
            storage_format,
            ..Default::default()
        };

        let mut buf = Vec::with_capacity(DEFAULT_BLOCK_BUFFER_SIZE);
        let col_metas = serialize_block(&write_settings, schema, block, &mut buf)?;
        let file_size = buf.len() as u64;

        data_accessor.write(&location.0, buf).await?;

        let block_meta = BlockMeta::new(
            row_count,
            block_size,
            file_size,
            col_stats,
            col_metas,
            cluster_stats,
            location,
            bloom_filter_index_location,
            bloom_filter_index_size,
            None,
            Compression::Lz4Raw,
            Some(Utc::now()),
        );
        Ok((block_meta, meta))
    }

    pub async fn build_block_index(
        &self,
        data_accessor: &Operator,
        schema: TableSchemaRef,
        block: &DataBlock,
        block_id: Uuid,
    ) -> Result<(u64, Option<Location>, Option<FileMetaData>)> {
        let location = self
            .location_generator
            .block_bloom_index_location(&block_id);

        let bloom_index_cols = BloomIndexColumns::All;
        let bloom_columns_map =
            bloom_index_cols.bloom_index_fields(schema.clone(), BloomIndex::supported_type)?;
        let maybe_bloom_index = BloomIndex::try_create(
            FunctionContext::default(),
            location.1,
            block,
            bloom_columns_map,
        )?;
        if let Some(bloom_index) = maybe_bloom_index {
            let index_block = bloom_index.serialize_to_data_block()?;
            let filter_schema = bloom_index.filter_schema;
            let mut data = Vec::with_capacity(DEFAULT_BLOCK_INDEX_BUFFER_SIZE);
            let index_block_schema = &filter_schema;
            let meta = blocks_to_parquet(
                index_block_schema,
                vec![index_block],
                &mut data,
                TableCompression::None,
            )?;
            let size = data.len() as u64;
            data_accessor.write(&location.0, data).await?;
            Ok((size, Some(location), Some(meta)))
        } else {
            Ok((0u64, None, None))
        }
    }
}
