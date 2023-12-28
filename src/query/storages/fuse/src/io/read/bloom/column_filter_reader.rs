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

use databend_common_arrow::arrow::datatypes::DataType;
use databend_common_arrow::arrow::datatypes::Field as ArrowField;
use databend_common_arrow::arrow::io::parquet::read::column_iter_to_arrays;
use databend_common_arrow::parquet::compression::Compression;
use databend_common_arrow::parquet::metadata::ColumnDescriptor;
use databend_common_arrow::parquet::metadata::Descriptor;
use databend_common_arrow::parquet::read::BasicDecompressor;
use databend_common_arrow::parquet::read::PageMetaData;
use databend_common_arrow::parquet::read::PageReader;
use databend_common_arrow::parquet::schema::types::FieldInfo;
use databend_common_arrow::parquet::schema::types::ParquetType;
use databend_common_arrow::parquet::schema::types::PhysicalType;
use databend_common_arrow::parquet::schema::types::PrimitiveType;
use databend_common_arrow::parquet::schema::Repetition;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_metrics::storage::*;
use databend_storages_common_cache::CacheKey;
use databend_storages_common_cache::InMemoryCacheReader;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_cache::Loader;
use databend_storages_common_cache_manager::BloomIndexFilterMeter;
use databend_storages_common_cache_manager::CachedObject;
use databend_storages_common_index::filters::Filter;
use databend_storages_common_index::filters::Xor8Filter;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use opendal::Operator;

type CachedReader = InMemoryCacheReader<Xor8Filter, Xor8FilterLoader, BloomIndexFilterMeter>;

/// Load the filter of a given bloom index column. Also
/// - generates the proper cache key
/// - takes cares of getting the correct cache instance from [CacheManager]
pub struct BloomColumnFilterReader {
    cached_reader: CachedReader,
    param: LoadParams,
}

impl BloomColumnFilterReader {
    pub fn new(
        index_path: String,
        column_id: ColumnId,
        column_name: String,
        column_chunk_meta: &SingleColumnMeta,
        operator: Operator,
    ) -> Self {
        let meta = column_chunk_meta;
        let cache_key = format!("{index_path}-{column_id}");

        // the schema of bloom filter block is fixed, as following
        let base_type = PrimitiveType {
            field_info: FieldInfo {
                name: column_name.clone(),
                repetition: Repetition::Required,
                id: None,
            },
            logical_type: None,
            converted_type: None,
            physical_type: PhysicalType::ByteArray,
        };

        let descriptor = Descriptor {
            primitive_type: base_type.clone(),
            max_def_level: 0,
            max_rep_level: 0,
        };

        let base_parquet_type = ParquetType::PrimitiveType(base_type);
        let loader = Xor8FilterLoader {
            offset: meta.offset,
            len: meta.len,
            cache_key,
            operator,
            column_descriptor: ColumnDescriptor::new(
                descriptor,
                vec![column_name],
                base_parquet_type,
            ),
        };

        let cached_reader = CachedReader::new(Xor8Filter::cache(), loader);

        let param = LoadParams {
            location: index_path,
            len_hint: None,
            ver: 0,
            put_cache: true,
        };

        BloomColumnFilterReader {
            cached_reader,
            param,
        }
    }

    #[async_backtrace::framed]
    pub async fn read(&self) -> Result<Arc<Xor8Filter>> {
        self.cached_reader.read(&self.param).await
    }

    pub async fn load_without_deserialize(&self) -> Result<Vec<u8>> {
        self.cached_reader
            .loader
            .load_without_deserialize(&self.param).await
    }
}

/// Loader that fetch range of the target object with customized cache key
pub struct Xor8FilterLoader {
    pub offset: u64,
    pub len: u64,
    pub cache_key: String,
    pub operator: Operator,
    pub column_descriptor: ColumnDescriptor,
}

impl Xor8FilterLoader {
    pub async fn load_without_deserialize(&self, params: &LoadParams) -> Result<Vec<u8>> {
        let bytes = self
            .operator
            .read_with(&params.location)
            .range(self.offset..self.offset + self.len)
            .await?;
        Ok(bytes)
    }
}

#[async_trait::async_trait]
impl Loader<Xor8Filter> for Xor8FilterLoader {
    #[async_backtrace::framed]
    async fn load(&self, params: &LoadParams) -> Result<Xor8Filter> {
        let bytes = self
            .operator
            .read_with(&params.location)
            .range(self.offset..self.offset + self.len)
            .await?;

        let page_meta_data = PageMetaData {
            column_start: 0,
            num_values: 1,
            compression: Compression::Uncompressed,
            descriptor: self.column_descriptor.descriptor.clone(),
        };

        let page_reader = PageReader::new_with_page_meta(
            std::io::Cursor::new(bytes),
            page_meta_data,
            Arc::new(|_, _| true),
            vec![],
            usize::MAX,
        );

        let decompressor = BasicDecompressor::new(page_reader, vec![]);
        let column_type = self.column_descriptor.descriptor.primitive_type.clone();
        let field_name = self.column_descriptor.path_in_schema[0].to_owned();
        let field = ArrowField::new(field_name, DataType::Binary, false);
        let mut array_iter =
            column_iter_to_arrays(vec![decompressor], vec![&column_type], field, None, 1)?;
        if let Some(array) = array_iter.next() {
            let array = array?;
            let col = Column::from_arrow(
                array.as_ref(),
                &databend_common_expression::types::DataType::String,
            );

            let filter_bytes = col
                .as_string()
                .map(|str| unsafe { str.index_unchecked(0) })
                .ok_or_else(|| {
                    // BloomPruner will log and handle this exception
                    ErrorCode::Internal(
                        "unexpected exception: load bloom filter raw data as string failed",
                    )
                })?;
            metrics_inc_block_index_read_bytes(filter_bytes.len() as u64);
            let (filter, _size) = Xor8Filter::from_bytes(filter_bytes)?;
            Ok(filter)
        } else {
            Err(ErrorCode::StorageOther(
                "bloom index data not available as expected",
            ))
        }
    }

    fn cache_key(&self, _params: &LoadParams) -> CacheKey {
        self.cache_key.clone()
    }
}
