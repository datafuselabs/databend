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

use std::path::PathBuf;
use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_common_config::CacheConfig;
use databend_common_config::CacheStorageTypeInnerConfig;
use databend_common_config::DiskCacheKeyReloadPolicy;
use databend_common_exception::Result;
use log::info;

use crate::caches::BlockMetaCache;
use crate::caches::BloomIndexFilterCache;
use crate::caches::BloomIndexMetaCache;
use crate::caches::CacheValue;
use crate::caches::ColumnArrayCache;
use crate::caches::ColumnarSegmentInfoCache;
use crate::caches::CompactSegmentInfoCache;
use crate::caches::FileMetaDataCache;
use crate::caches::InvertedIndexFileCache;
use crate::caches::InvertedIndexMetaCache;
use crate::caches::PrunePartitionsCache;
use crate::caches::TableSnapshotCache;
use crate::caches::TableSnapshotStatisticCache;
use crate::InMemoryLruCache;
use crate::TableDataCache;
use crate::TableDataCacheBuilder;

static DEFAULT_FILE_META_DATA_CACHE_ITEMS: usize = 3000;

/// Where all the caches reside
pub struct CacheManager {
    table_snapshot_cache: Option<TableSnapshotCache>,
    table_statistic_cache: Option<TableSnapshotStatisticCache>,
    compact_segment_info_cache: Option<CompactSegmentInfoCache>,
    columnar_segment_info_cache: Option<ColumnarSegmentInfoCache>,
    bloom_index_filter_cache: Option<BloomIndexFilterCache>,
    bloom_index_meta_cache: Option<BloomIndexMetaCache>,
    inverted_index_meta_cache: Option<InvertedIndexMetaCache>,
    inverted_index_file_cache: Option<InvertedIndexFileCache>,
    prune_partitions_cache: Option<PrunePartitionsCache>,
    parquet_file_meta_data_cache: Option<FileMetaDataCache>,
    table_data_cache: Option<TableDataCache>,
    in_memory_table_data_cache: Option<ColumnArrayCache>,
    block_meta_cache: Option<BlockMetaCache>,
}

impl CacheManager {
    /// Initialize the caches according to the relevant configurations.
    pub fn init(
        config: &CacheConfig,
        max_server_memory_usage: &u64,
        tenant_id: impl Into<String>,
    ) -> Result<()> {
        // setup table data cache
        let table_data_cache = {
            match config.data_cache_storage {
                CacheStorageTypeInnerConfig::None => None,
                CacheStorageTypeInnerConfig::Disk => {
                    let real_disk_cache_root = PathBuf::from(&config.disk_cache_config.path)
                        .join(tenant_id.into())
                        .join("v1");

                    let queue_size: u32 = if config.table_data_cache_population_queue_size > 0 {
                        config.table_data_cache_population_queue_size
                    } else {
                        std::cmp::max(
                            1,
                            std::thread::available_parallelism()
                                .expect("Cannot get thread count")
                                .get() as u32,
                        ) * 5
                    };

                    info!(
                        "disk cache enabled, cache population queue size {}",
                        queue_size
                    );

                    Self::new_block_data_cache(
                        &real_disk_cache_root,
                        queue_size,
                        config.disk_cache_config.max_bytes as usize,
                        config.data_cache_key_reload_policy.clone(),
                        config.disk_cache_config.sync_data,
                    )?
                }
            }
        };

        // setup in-memory table column cache
        let memory_cache_capacity = if config.table_data_deserialized_data_bytes != 0 {
            config.table_data_deserialized_data_bytes as usize
        } else {
            (*max_server_memory_usage as usize)
                * config.table_data_deserialized_memory_ratio as usize
                / 100
        };

        // Cache of deserialized table data
        let in_memory_table_data_cache =
            Self::new_named_bytes_cache(MEMORY_CACHE_TABLE_DATA, memory_cache_capacity);

        // setup in-memory table meta cache
        if !config.enable_table_meta_cache {
            GlobalInstance::set(Arc::new(Self {
                table_snapshot_cache: None,
                compact_segment_info_cache: None,
                bloom_index_filter_cache: None,
                bloom_index_meta_cache: None,
                inverted_index_meta_cache: None,
                inverted_index_file_cache: None,
                prune_partitions_cache: None,
                parquet_file_meta_data_cache: None,
                table_statistic_cache: None,
                table_data_cache,
                in_memory_table_data_cache,
                columnar_segment_info_cache: None,
                block_meta_cache: None,
            }));
        } else {
            let table_snapshot_cache = Self::new_named_items_cache(
                config.table_meta_snapshot_count as usize,
                MEMORY_CACHE_TABLE_SNAPSHOT,
            );
            let table_statistic_cache = Self::new_named_items_cache(
                config.table_meta_statistic_count as usize,
                MEMORY_CACHE_TABLE_STATISTICS,
            );
            let compact_segment_info_cache = Self::new_named_bytes_cache(
                MEMORY_CACHE_COMPACT_SEGMENT_INFO,
                config.table_meta_segment_bytes as usize,
            );
            let columnar_segment_info_cache = Self::new_named_bytes_cache(
                MEMORY_CACHE_COLUMNAR_SEGMENT_INFO,
                config.table_meta_segment_bytes as usize,
            );
            let bloom_index_filter_cache = Self::new_named_bytes_cache(
                MEMORY_CACHE_BLOOM_INDEX_FILTER,
                config.table_bloom_index_filter_size as usize,
            );
            let bloom_index_meta_cache = Self::new_named_items_cache(
                config.table_bloom_index_meta_count as usize,
                MEMORY_CACHE_BLOOM_INDEX_FILE_META_DATA,
            );
            let inverted_index_meta_cache = Self::new_named_items_cache(
                config.inverted_index_meta_count as usize,
                MEMORY_CACHE_INVERTED_INDEX_FILE_META_DATA,
            );

            // setup in-memory inverted index filter cache
            let inverted_index_file_size = if config.inverted_index_filter_memory_ratio != 0 {
                (*max_server_memory_usage as usize)
                    * config.inverted_index_filter_memory_ratio as usize
                    / 100
            } else {
                config.inverted_index_filter_size as usize
            };
            let inverted_index_file_cache = Self::new_named_bytes_cache(
                MEMORY_CACHE_INVERTED_INDEX_FILE,
                inverted_index_file_size,
            );
            let prune_partitions_cache = Self::new_named_items_cache(
                config.table_prune_partitions_count as usize,
                MEMORY_CACHE_PRUNE_PARTITIONS,
            );

            let parquet_file_meta_data_cache = Self::new_named_items_cache(
                DEFAULT_FILE_META_DATA_CACHE_ITEMS,
                MEMORY_CACHE_PARQUET_FILE_META,
            );

            let block_meta_cache = Self::new_named_items_cache(
                config.block_meta_count as usize,
                MEMORY_CACHE_BLOCK_META,
            );

            GlobalInstance::set(Arc::new(Self {
                table_snapshot_cache,
                compact_segment_info_cache,
                bloom_index_filter_cache,
                bloom_index_meta_cache,
                inverted_index_meta_cache,
                inverted_index_file_cache,
                prune_partitions_cache,
                parquet_file_meta_data_cache,
                table_statistic_cache,
                table_data_cache,
                in_memory_table_data_cache,
                block_meta_cache,
                columnar_segment_info_cache,
            }));
        }

        Ok(())
    }

    pub fn instance() -> Arc<CacheManager> {
        GlobalInstance::get()
    }

    pub fn get_table_snapshot_cache(&self) -> Option<TableSnapshotCache> {
        self.table_snapshot_cache.clone()
    }

    pub fn get_block_meta_cache(&self) -> Option<BlockMetaCache> {
        self.block_meta_cache.clone()
    }

    pub fn get_table_snapshot_statistics_cache(&self) -> Option<TableSnapshotStatisticCache> {
        self.table_statistic_cache.clone()
    }

    pub fn get_table_segment_cache(&self) -> Option<CompactSegmentInfoCache> {
        self.compact_segment_info_cache.clone()
    }

    pub fn get_columnar_segment_cache(&self) -> Option<ColumnarSegmentInfoCache> {
        self.columnar_segment_info_cache.clone()
    }

    pub fn get_bloom_index_filter_cache(&self) -> Option<BloomIndexFilterCache> {
        self.bloom_index_filter_cache.clone()
    }

    pub fn get_bloom_index_meta_cache(&self) -> Option<BloomIndexMetaCache> {
        self.bloom_index_meta_cache.clone()
    }

    pub fn get_inverted_index_meta_cache(&self) -> Option<InvertedIndexMetaCache> {
        self.inverted_index_meta_cache.clone()
    }

    pub fn get_inverted_index_file_cache(&self) -> Option<InvertedIndexFileCache> {
        self.inverted_index_file_cache.clone()
    }

    pub fn get_prune_partitions_cache(&self) -> Option<PrunePartitionsCache> {
        self.prune_partitions_cache.clone()
    }

    pub fn get_file_meta_data_cache(&self) -> Option<FileMetaDataCache> {
        self.parquet_file_meta_data_cache.clone()
    }

    pub fn get_table_data_cache(&self) -> Option<TableDataCache> {
        self.table_data_cache.clone()
    }

    pub fn get_table_data_array_cache(&self) -> Option<ColumnArrayCache> {
        self.in_memory_table_data_cache.clone()
    }

    pub fn new_named_items_cache<V: Into<CacheValue<V>>>(
        capacity: usize,
        name: impl Into<String>,
    ) -> Option<InMemoryLruCache<V>> {
        match capacity {
            0 => None,
            _ => Some(InMemoryLruCache::with_items_capacity(name.into(), capacity)),
        }
    }

    fn new_named_bytes_cache<V: Into<CacheValue<V>>>(
        name: impl Into<String>,
        bytes_capacity: usize,
    ) -> Option<InMemoryLruCache<V>> {
        match bytes_capacity {
            0 => None,
            _ => Some(InMemoryLruCache::with_bytes_capacity(
                name.into(),
                bytes_capacity,
            )),
        }
    }

    fn new_block_data_cache(
        path: &PathBuf,
        population_queue_size: u32,
        disk_cache_bytes_size: usize,
        disk_cache_key_reload_policy: DiskCacheKeyReloadPolicy,
        sync_data: bool,
    ) -> Result<Option<TableDataCache>> {
        if disk_cache_bytes_size > 0 {
            let cache_holder = TableDataCacheBuilder::new_table_data_disk_cache(
                path,
                population_queue_size,
                disk_cache_bytes_size,
                disk_cache_key_reload_policy,
                sync_data,
            )?;
            Ok(Some(cache_holder))
        } else {
            Ok(None)
        }
    }
}

const MEMORY_CACHE_TABLE_DATA: &str = "memory_cache_table_data";
const MEMORY_CACHE_PARQUET_FILE_META: &str = "memory_cache_parquet_file_meta";
const MEMORY_CACHE_PRUNE_PARTITIONS: &str = "memory_cache_prune_partitions";
const MEMORY_CACHE_INVERTED_INDEX_FILE: &str = "memory_cache_inverted_index_file";
const MEMORY_CACHE_INVERTED_INDEX_FILE_META_DATA: &str =
    "memory_cache_inverted_index_file_meta_data";

const MEMORY_CACHE_BLOOM_INDEX_FILE_META_DATA: &str = "memory_cache_bloom_index_file_meta_data";
const MEMORY_CACHE_BLOOM_INDEX_FILTER: &str = "memory_cache_bloom_index_filter";
const MEMORY_CACHE_COMPACT_SEGMENT_INFO: &str = "memory_cache_compact_segment_info";
const MEMORY_CACHE_COLUMNAR_SEGMENT_INFO: &str = "memory_cache_columnar_segment_info";
const MEMORY_CACHE_TABLE_STATISTICS: &str = "memory_cache_table_statistics";
const MEMORY_CACHE_TABLE_SNAPSHOT: &str = "memory_cache_table_snapshot";
const MEMORY_CACHE_BLOCK_META: &str = "memory_cache_block_meta";
