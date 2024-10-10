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

use databend_common_arrow::parquet::metadata::FileMetaData;
use databend_common_cache::MemSized;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_storages_common_index::filters::Xor8Filter;
use databend_storages_common_index::BloomIndexMeta;
use databend_storages_common_index::InvertedIndexFile;
use databend_storages_common_index::InvertedIndexMeta;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ColumnarSegmentInfo;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::TableSnapshotStatistics;

use crate::manager::CacheManager;
use crate::CacheAccessor;
use crate::InMemoryLruCache;

/// In memory object cache of SegmentInfo
pub type CompactSegmentInfoCache = InMemoryLruCache<CompactSegmentInfo>;
pub type ColumnarSegmentInfoCache = InMemoryLruCache<ColumnarSegmentInfo>;

pub type BlockMetaCache = InMemoryLruCache<Vec<Arc<BlockMeta>>>;

/// In memory object cache of TableSnapshot
pub type TableSnapshotCache = InMemoryLruCache<TableSnapshot>;
/// In memory object cache of TableSnapshotStatistics
pub type TableSnapshotStatisticCache = InMemoryLruCache<TableSnapshotStatistics>;
/// In memory object cache of bloom filter.
/// For each indexed data block, the bloom xor8 filter of column is cached individually
pub type BloomIndexFilterCache = InMemoryLruCache<Xor8Filter>;
/// In memory object cache of parquet FileMetaData of bloom index data
pub type BloomIndexMetaCache = InMemoryLruCache<BloomIndexMeta>;

pub type InvertedIndexMetaCache = InMemoryLruCache<InvertedIndexMeta>;
pub type InvertedIndexFileCache = InMemoryLruCache<InvertedIndexFile>;

/// In memory object cache of parquet FileMetaData of external parquet files
pub type FileMetaDataCache = InMemoryLruCache<FileMetaData>;

pub type PrunePartitionsCache = InMemoryLruCache<(PartStatistics, Partitions)>;

/// In memory object cache of table column array
pub type ColumnArrayCache = InMemoryLruCache<SizedColumnArray>;
pub type ArrayRawDataUncompressedSize = usize;
pub type SizedColumnArray = (
    Box<dyn databend_common_arrow::arrow::array::Array>,
    ArrayRawDataUncompressedSize,
);

// Bind Type of cached objects to Caches
//
// The `Cache` should return
// - cache item of Type `T`
// - and implement `CacheAccessor` properly
pub trait CachedObject<T> {
    type Cache: CacheAccessor<V = T>;
    fn cache() -> Option<Self::Cache>;
}

impl CachedObject<CompactSegmentInfo> for SegmentInfo {
    type Cache = CompactSegmentInfoCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_table_segment_cache()
    }
}

impl CachedObject<TableSnapshot> for TableSnapshot {
    type Cache = TableSnapshotCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_table_snapshot_cache()
    }
}

impl CachedObject<Vec<Arc<BlockMeta>>> for Vec<Arc<BlockMeta>> {
    type Cache = BlockMetaCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_block_meta_cache()
    }
}

impl CachedObject<TableSnapshotStatistics> for TableSnapshotStatistics {
    type Cache = TableSnapshotStatisticCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_table_snapshot_statistics_cache()
    }
}

impl CachedObject<BloomIndexMeta> for BloomIndexMeta {
    type Cache = BloomIndexMetaCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_bloom_index_meta_cache()
    }
}

impl CachedObject<(PartStatistics, Partitions)> for (PartStatistics, Partitions) {
    type Cache = PrunePartitionsCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_prune_partitions_cache()
    }
}

impl CachedObject<Xor8Filter> for Xor8Filter {
    type Cache = BloomIndexFilterCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_bloom_index_filter_cache()
    }
}

impl CachedObject<FileMetaData> for FileMetaData {
    type Cache = FileMetaDataCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_file_meta_data_cache()
    }
}

impl CachedObject<InvertedIndexFile> for InvertedIndexFile {
    type Cache = InvertedIndexFileCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_inverted_index_file_cache()
    }
}

impl CachedObject<InvertedIndexMeta> for InvertedIndexMeta {
    type Cache = InvertedIndexMetaCache;
    fn cache() -> Option<Self::Cache> {
        CacheManager::instance().get_inverted_index_meta_cache()
    }
}

pub struct CacheValue<T> {
    inner: Arc<T>,
    mem_bytes: usize,
}

impl<T> CacheValue<T> {
    pub fn new(inner: T, mem_bytes: usize) -> Self {
        Self {
            inner: Arc::new(inner),
            mem_bytes,
        }
    }

    pub fn get_inner(&self) -> Arc<T> {
        self.inner.clone()
    }
}

impl From<CompactSegmentInfo> for CacheValue<CompactSegmentInfo> {
    fn from(value: CompactSegmentInfo) -> Self {
        CacheValue {
            mem_bytes: std::mem::size_of::<CompactSegmentInfo>()
                + value.raw_block_metas.bytes.len(),
            inner: Arc::new(value),
        }
    }
}

impl From<SegmentInfo> for CacheValue<SegmentInfo> {
    fn from(value: SegmentInfo) -> Self {
        CacheValue {
            mem_bytes: 0, // TODO
            inner: Arc::new(value),
        }
    }
}

impl From<ColumnarSegmentInfo> for CacheValue<ColumnarSegmentInfo> {
    fn from(value: ColumnarSegmentInfo) -> Self {
        CacheValue {
            mem_bytes: 0, // TODO
            inner: Arc::new(value),
        }
    }
}

impl From<Vec<Arc<BlockMeta>>> for CacheValue<Vec<Arc<BlockMeta>>> {
    fn from(value: Vec<Arc<BlockMeta>>) -> Self {
        CacheValue {
            inner: Arc::new(value),
            mem_bytes: 0,
        }
    }
}

impl From<TableSnapshot> for CacheValue<TableSnapshot> {
    fn from(value: TableSnapshot) -> Self {
        CacheValue {
            inner: Arc::new(value),
            mem_bytes: 0,
        }
    }
}

impl From<TableSnapshotStatistics> for CacheValue<TableSnapshotStatistics> {
    fn from(value: TableSnapshotStatistics) -> Self {
        CacheValue {
            inner: Arc::new(value),
            mem_bytes: 0,
        }
    }
}

impl From<Xor8Filter> for CacheValue<Xor8Filter> {
    fn from(value: Xor8Filter) -> Self {
        CacheValue {
            mem_bytes: std::mem::size_of::<Xor8Filter>() + value.filter.finger_prints.len(),
            inner: Arc::new(value),
        }
    }
}

impl From<BloomIndexMeta> for CacheValue<BloomIndexMeta> {
    fn from(value: BloomIndexMeta) -> Self {
        CacheValue {
            inner: Arc::new(value),
            mem_bytes: 0,
        }
    }
}

impl From<InvertedIndexMeta> for CacheValue<InvertedIndexMeta> {
    fn from(value: InvertedIndexMeta) -> Self {
        CacheValue {
            inner: Arc::new(value),
            mem_bytes: 0,
        }
    }
}

impl From<InvertedIndexFile> for CacheValue<InvertedIndexFile> {
    fn from(value: InvertedIndexFile) -> Self {
        CacheValue {
            mem_bytes: std::mem::size_of::<InvertedIndexFile>() + value.data.len(),
            inner: Arc::new(value),
        }
    }
}

impl From<FileMetaData> for CacheValue<FileMetaData> {
    fn from(value: FileMetaData) -> Self {
        CacheValue {
            inner: Arc::new(value),
            mem_bytes: 0,
        }
    }
}

impl From<(PartStatistics, Partitions)> for CacheValue<(PartStatistics, Partitions)> {
    fn from(value: (PartStatistics, Partitions)) -> Self {
        CacheValue {
            inner: Arc::new(value),
            mem_bytes: 0,
        }
    }
}

impl From<SizedColumnArray> for CacheValue<SizedColumnArray> {
    fn from(value: SizedColumnArray) -> Self {
        CacheValue {
            mem_bytes: value.1,
            inner: Arc::new(value),
        }
    }
}

pub struct FileSize(pub u64);

impl From<FileSize> for CacheValue<FileSize> {
    fn from(value: FileSize) -> Self {
        CacheValue {
            mem_bytes: value.0 as usize,
            inner: Arc::new(value),
        }
    }
}

impl<T> MemSized for CacheValue<T> {
    fn mem_bytes(&self) -> usize {
        self.mem_bytes
    }
}
