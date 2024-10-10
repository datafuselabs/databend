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

use std::collections::HashMap;
use std::future::Future;
use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::TrySpawn;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_metrics::storage::metrics_inc_block_inverted_index_read_milliseconds;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheManager;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_index::InvertedIndexDirectory;
use databend_storages_common_index::InvertedIndexMeta;
use databend_storages_common_io::MergeIOReader;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use opendal::Operator;

use crate::index::InvertedIndexFile;
use crate::io::MetaReaders;

const INDEX_COLUMN_NAMES: [&str; 8] = [
    "fast",
    "store",
    "fieldnorm",
    "pos",
    "idx",
    "term",
    "meta.json",
    ".managed.json",
];

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
            .try_spawn(self)?
            .await
            .map_err(|e| ErrorCode::TokioError(format!("runtime join error. {}", e)))
    }
}

/// Loads inverted index meta data
/// read data from cache, or populate cache items if possible
#[fastrace::trace]
pub(crate) async fn load_inverted_index_meta(
    dal: Operator,
    path: &str,
) -> Result<Arc<InvertedIndexMeta>> {
    let path_owned = path.to_owned();
    async move {
        let reader = MetaReaders::inverted_index_meta_reader(dal);
        let version = 0;

        let load_params = LoadParams {
            location: path_owned,
            len_hint: None,
            ver: version,
            put_cache: true,
        };

        reader.read(&load_params).await
    }
    .execute_in_runtime(&GlobalIORuntime::instance())
    .await?
}

/// Loads bytes of each inverted index files
/// read data from cache, or populate cache items if possible
#[fastrace::trace]
pub(crate) async fn load_inverted_index_files<'a>(
    settings: &ReadSettings,
    columns: Vec<(String, Range<u64>)>,
    location: &'a str,
    operator: &'a Operator,
) -> Result<Vec<Arc<InvertedIndexFile>>> {
    let start = Instant::now();

    let mut files = vec![];
    let mut ranges = vec![];
    let mut column_id = 0;
    let mut names_map = HashMap::new();
    let inverted_index_file_cache = CacheManager::instance().get_inverted_index_file_cache();
    for (name, range) in columns.into_iter() {
        let cache_key = cache_key_of_column(location, &name);
        if let Some(cache_file) =
            inverted_index_file_cache.get_sized(&cache_key, range.end - range.start)
        {
            files.push(cache_file);
            continue;
        }

        // if cache missed, prepare the ranges to be read
        ranges.push((column_id, range));
        names_map.insert(column_id, (name, cache_key));
        column_id += 1;
    }

    if !ranges.is_empty() {
        let merge_io_result =
            MergeIOReader::merge_io_read(settings, operator.clone(), location, &ranges).await?;

        // merge column data fetched from object storage
        for (column_id, (chunk_idx, range)) in &merge_io_result.columns_chunk_offsets {
            let chunk = merge_io_result
                .owner_memory
                .get_chunk(*chunk_idx, &merge_io_result.block_path)?;
            let data = chunk.slice(range.clone());

            let (name, cache_key) = names_map.remove(column_id).unwrap();
            let file = InvertedIndexFile::create(name, data.into());

            // add index file to cache
            inverted_index_file_cache.insert(cache_key, file.clone());
            files.push(file.into());
        }
    }

    // Perf.
    {
        metrics_inc_block_inverted_index_read_milliseconds(start.elapsed().as_millis() as u64);
    }

    Ok(files)
}

/// load inverted index directory
#[fastrace::trace]
pub(crate) async fn load_inverted_index_directory<'a>(
    settings: &ReadSettings,
    location: &'a str,
    operator: &'a Operator,
    inverted_index_meta_map: HashMap<String, SingleColumnMeta>,
) -> Result<InvertedIndexDirectory> {
    // load inverted index files, usually including following eight files:
    // 1. fast file
    // 2. store file
    // 3. fieldnorm file
    // 4. position file
    // 5. idx file
    // 6. term file
    // 7. meta.json file
    // 8. .managed.json file
    let mut columns = Vec::with_capacity(inverted_index_meta_map.len());
    for (col_name, col_meta) in inverted_index_meta_map {
        let col_range = col_meta.offset..(col_meta.offset + col_meta.len);
        columns.push((col_name, col_range));
    }

    let files = load_inverted_index_files(settings, columns, location, operator).await?;
    // use those files to create inverted index directory
    let directory = InvertedIndexDirectory::try_create(files)?;

    Ok(directory)
}

fn cache_key_of_column(index_path: &str, index_column_name: &str) -> String {
    format!("{index_path}-{index_column_name}")
}

pub(crate) fn cache_key_of_index_columns(index_path: &str) -> Vec<String> {
    INDEX_COLUMN_NAMES
        .iter()
        .map(|column_name| cache_key_of_column(index_path, column_name))
        .collect()
}
