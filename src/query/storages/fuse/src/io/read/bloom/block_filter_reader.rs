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

use std::collections::HashSet;
use std::future::Future;
use std::sync::Arc;

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::TrySpawn;
use databend_common_base::GLOBAL_TASK;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_index::filters::Xor8Filter;
use databend_storages_common_index::BloomIndexMeta;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use futures_util::future::try_join_all;
use opendal::Operator;

use crate::index::filters::BlockBloomFilterIndexVersion;
use crate::index::filters::BlockFilter;
use crate::io::read::bloom::column_filter_reader::BloomColumnFilterReader;
use crate::io::MetaReaders;

#[async_trait::async_trait]
pub trait BloomBlockFilterReader {
    async fn read_block_filter(
        &self,
        dal: Operator,
        columns: &[String],
        index_length: u64,
    ) -> Result<BlockFilter>;
}

#[async_trait::async_trait]
impl BloomBlockFilterReader for Location {
    // NOTE that the `columns` will be de-duplicated first,
    // and the filters contained by the returned `BlockFilter` may not order by `columns`
    #[async_backtrace::framed]
    async fn read_block_filter(
        &self,
        dal: Operator,
        columns: &[String],
        index_length: u64,
    ) -> Result<BlockFilter> {
        let (path, ver) = &self;
        let index_version = BlockBloomFilterIndexVersion::try_from(*ver)?;
        match index_version {
            BlockBloomFilterIndexVersion::V0(_) => Err(ErrorCode::DeprecatedIndexFormat(
                "bloom filter index version(v0) is deprecated",
            )),
            BlockBloomFilterIndexVersion::V2(_)
            | BlockBloomFilterIndexVersion::V3(_)
            | BlockBloomFilterIndexVersion::V4(_) => {
                let res = load_bloom_filter_by_columns(dal, columns, path, index_length).await?;
                Ok(res)
            }
        }
    }
}

/// load index column data
#[minitrace::trace]
async fn load_bloom_filter_by_columns<'a>(
    dal: Operator,
    column_needed: &'a [String],
    index_path: &'a str,
    index_length: u64,
) -> Result<BlockFilter> {
    // 1. load index meta
    let bloom_index_meta = load_index_meta(dal.clone(), index_path, index_length).await?;

    // 2. filter out columns that needed and exist in the index
    // 2.1 dedup the columns
    let column_needed: HashSet<&String> = HashSet::from_iter(column_needed);
    // 2.2 collects the column metas and their column ids
    let index_column_chunk_metas = &bloom_index_meta.columns;
    let mut col_metas = Vec::with_capacity(column_needed.len());
    for column_name in column_needed {
        for (idx, (name, column_meta)) in index_column_chunk_metas.iter().enumerate() {
            if name == column_name {
                col_metas.push((idx as ColumnId, (name, column_meta)))
            }
        }
    }

    // 3. load filters
    let futs = col_metas
        .iter()
        .map(|(idx, (name, col_chunk_meta))| {
            load_column_xor8_filter(*idx, (*name).to_owned(), col_chunk_meta, index_path, &dal)
        })
        .collect::<Vec<_>>();

    let filters = try_join_all(futs).await?.into_iter().collect();

    // 4. build index schema
    let fields = col_metas
        .iter()
        .map(|(_, (name, _col_chunk_mea))| TableField::new(name, TableDataType::String))
        .collect();

    let filter_schema = TableSchema::new(fields);

    Ok(BlockFilter {
        filter_schema: Arc::new(filter_schema),
        filters,
    })
}

/// load index column data
#[minitrace::trace]
pub async fn load_bloom_filter_by_columns_without_deserialize<'a>(
    dal: Operator,
    column_needed: &'a [String],
    index_path: &'a str,
    index_length: u64,
) -> Result<Vec<Vec<u8>>> {
    // 1. load index meta
    let bloom_index_meta = load_index_meta(dal.clone(), index_path, index_length).await?;

    // 2. filter out columns that needed and exist in the index
    // 2.1 dedup the columns
    let column_needed: HashSet<&String> = HashSet::from_iter(column_needed);
    // 2.2 collects the column metas and their column ids
    let index_column_chunk_metas = &bloom_index_meta.columns;
    let mut col_metas = Vec::with_capacity(column_needed.len());
    for column_name in column_needed {
        for (idx, (name, column_meta)) in index_column_chunk_metas.iter().enumerate() {
            if name == column_name {
                col_metas.push((idx as ColumnId, (name, column_meta)))
            }
        }
    }
    // 3. load filters
    let futs = col_metas
        .iter()
        .map(|(idx, (name, col_chunk_meta))| {
            load_column_xor8_filter_without_deseralize(
                *idx,
                (*name).to_owned(),
                col_chunk_meta,
                index_path,
                &dal,
            )
        })
        .collect::<Vec<_>>();

    let filters = try_join_all(futs).await?.into_iter().collect();
    Ok(filters)
}

/// Loads bytes and index of the given column.
/// read data from cache, or populate cache items if possible
#[minitrace::trace]
async fn load_column_xor8_filter<'a>(
    idx: ColumnId,
    column_name: String,
    col_chunk_meta: &'a SingleColumnMeta,
    index_path: &'a str,
    dal: &'a Operator,
) -> Result<Arc<Xor8Filter>> {
    let storage_runtime = GlobalIORuntime::instance();
    let bytes = {
        let column_data_reader = BloomColumnFilterReader::new(
            index_path.to_owned(),
            idx,
            column_name,
            col_chunk_meta,
            dal.clone(),
        );
        async move { column_data_reader.read().await }
    }
    .execute_in_runtime(&storage_runtime)
    .await??;
    Ok(bytes)
}

/// Loads bytes and index of the given column.
/// read data from cache, or populate cache items if possible
#[minitrace::trace]
async fn load_column_xor8_filter_without_deseralize<'a>(
    idx: ColumnId,
    column_name: String,
    col_chunk_meta: &'a SingleColumnMeta,
    index_path: &'a str,
    dal: &'a Operator,
) -> Result<Vec<u8>> {
    let storage_runtime = GlobalIORuntime::instance();
    let bytes = {
        let column_data_reader = BloomColumnFilterReader::new(
            index_path.to_owned(),
            idx,
            column_name,
            col_chunk_meta,
            dal.clone(),
        );
        async move { column_data_reader.load_without_deserialize().await }
    }
    .execute_in_runtime(&storage_runtime)
    .await??;
    Ok(bytes)
}

/// Loads index meta data
/// read data from cache, or populate cache items if possible
#[minitrace::trace]
async fn load_index_meta(dal: Operator, path: &str, length: u64) -> Result<Arc<BloomIndexMeta>> {
    let path_owned = path.to_owned();
    async move {
        let reader = MetaReaders::bloom_index_meta_reader(dal);
        // Format of FileMetaData is not versioned, version argument is ignored by the underlying reader,
        // so we just pass a zero to reader
        let version = 0;

        let load_params = LoadParams {
            location: path_owned,
            len_hint: Some(length),
            ver: version,
            put_cache: true,
        };

        reader.read(&load_params).await
    }
    .execute_in_runtime(&GlobalIORuntime::instance())
    .await?
}

#[async_trait::async_trait]
trait InRuntime
where Self: Future
{
    async fn execute_in_runtime(self, runtime: &Runtime) -> Result<Self::Output>;
}

#[async_trait::async_trait]
impl<T> InRuntime for T
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    #[async_backtrace::framed]
    async fn execute_in_runtime(self, runtime: &Runtime) -> Result<T::Output> {
        runtime
            .try_spawn(GLOBAL_TASK, self)?
            .await
            .map_err(|e| ErrorCode::TokioError(format!("runtime join error. {}", e)))
    }
}
