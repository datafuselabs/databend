//  Copyright 2023 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use chrono::Utc;
use databend_common_base::base::tokio;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::statistics::gen_columns_statistics;
use databend_common_storages_fuse::statistics::STATS_STRING_PREFIX_LEN;
use databend_common_storages_fuse::FuseStorageFormat;
use databend_query::test_kits::*;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CacheValue;
use databend_storages_common_cache::InMemoryLruCache;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::Compression;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::SingleColumnMeta;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::Versioned;
use opendal::Operator;
use parquet::format::FileMetaData;
use sysinfo::get_current_pid;
use sysinfo::System;
use uuid::Uuid;

// NOTE:
//
// usage of memory is observed at *process* level, please do not combine them into
// one test function.
//
// by default, these cases are ignored (in CI).
//
// please run the following two cases individually (in different process)

// #[tokio::test(flavor = "multi_thread")]
// #[ignore]
// async fn test_index_meta_cache_size_file_meta_data() -> databend_common_exception::Result<()> {
//     let thrift_file_meta = setup().await?;

//     let cache_number = 300_000;

//     let meta: FileMetaData = FileMetaData::try_from_thrift(thrift_file_meta)?;

//     let sys = System::new_all();
//     let pid = get_current_pid().unwrap();
//     let process = sys.process(pid).unwrap();
//     let base_memory_usage = process.memory();
//     let scenario = "FileMetaData";

//     eprintln!(
//         "scenario {}, pid {}, base memory {}",
//         scenario, pid, base_memory_usage
//     );

//     let cache = InMemoryCacheBuilder::new_item_cache::<FileMetaData>(cache_number as u64);

//     populate_cache(&cache, meta, cache_number);
//     show_memory_usage(scenario, base_memory_usage, cache_number);

//     drop(cache);

//     Ok(())
// }

// #[tokio::test(flavor = "multi_thread")]
// #[ignore]
// async fn test_index_meta_cache_size_bloom_meta() -> databend_common_exception::Result<()> {
//     let thrift_file_meta = setup().await?;

//     let cache_number = 300_000;

//     let bloom_index_meta = BloomIndexMeta::try_from(thrift_file_meta)?;

//     let sys = System::new_all();
//     let pid = get_current_pid().unwrap();
//     let process = sys.process(pid).unwrap();
//     let base_memory_usage = process.memory();

//     let scenario = "BloomIndexMeta(mini)";
//     eprintln!(
//         "scenario {}, pid {}, base memory {}",
//         scenario, pid, base_memory_usage
//     );

//     let cache = InMemoryCacheBuilder::new_item_cache::<BloomIndexMeta>(cache_number as u64);
//     populate_cache(&cache, bloom_index_meta, cache_number);
//     show_memory_usage("BloomIndexMeta(Mini)", base_memory_usage, cache_number);

//     drop(cache);

//     Ok(())
// }

// cargo test --test it storages::fuse::bloom_index_meta_size::test_random_location_memory_size --no-fail-fast -- --ignored --exact -Z unstable-options --show-output
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_random_location_memory_size() -> databend_common_exception::Result<()> {
    // generate random location of Type Location
    let location_gen = TableMetaLocationGenerator::with_prefix("/root".to_string());

    let num_segments = 5_000_000;
    let sys = System::new_all();
    let pid = get_current_pid().unwrap();
    let process = sys.process(pid).unwrap();
    let base_memory_usage = process.memory();
    let scenario = format!("{} segment locations", num_segments);

    eprintln!(
        "scenario {}, pid {}, base memory {}",
        scenario, pid, base_memory_usage
    );

    let mut locations: HashSet<Location, _> = HashSet::new();
    for _ in 0..num_segments {
        let segment_path = location_gen.gen_segment_info_location(Default::default());
        let segment_location = (segment_path, SegmentInfo::VERSION);
        locations.insert(segment_location);
    }

    show_memory_usage(&scenario, base_memory_usage, num_segments);

    drop(locations);

    Ok(())
}

// cargo test --test it storages::fuse::bloom_index_meta_size::test_segment_info_size --no-fail-fast -- --ignored --exact -Z unstable-options --show-output
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_segment_info_size() -> databend_common_exception::Result<()> {
    let cache_number = 3000;
    let num_block_per_seg = 1000;

    let segment_info = build_test_segment_info(num_block_per_seg)?;

    let sys = System::new_all();
    let pid = get_current_pid().unwrap();
    let process = sys.process(pid).unwrap();
    let base_memory_usage = process.memory();
    let scenario = format!(
        "{} SegmentInfo, {} block per seg ",
        cache_number, num_block_per_seg
    );

    eprintln!(
        "scenario {}, pid {}, base memory {}",
        scenario, pid, base_memory_usage
    );

    let cache = InMemoryLruCache::with_items_capacity(String::from(""), cache_number);
    {
        for _ in 0..cache_number {
            let uuid = Uuid::new_v4();
            let block_metas = segment_info
                .blocks
                .iter()
                .map(|b: &Arc<BlockMeta>| Arc::new(b.as_ref().clone()))
                .collect::<Vec<_>>();
            let statistics = segment_info.summary.clone();
            let segment_info = SegmentInfo::new(block_metas, statistics);
            cache.insert(
                format!("{}", uuid.simple()),
                CompactSegmentInfo::try_from(segment_info)?,
            );
        }
    }
    show_memory_usage("SegmentInfoCache", base_memory_usage, cache_number);

    Ok(())
}

// cargo test --test it storages::fuse::bloom_index_meta_size::test_segment_raw_bytes_size --no-fail-fast -- --ignored --exact -Z unstable-options --show-output
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_segment_raw_bytes_size() -> databend_common_exception::Result<()> {
    let cache_number = 3000;
    let num_block_per_seg = 1000;

    let segment_info = build_test_segment_info(num_block_per_seg)?;
    let segment_info_bytes = CompactSegmentInfo::try_from(segment_info)?;

    let sys = System::new_all();
    let pid = get_current_pid().unwrap();
    let process = sys.process(pid).unwrap();
    let base_memory_usage = process.memory();
    let scenario = format!(
        "{} SegmentInfo(raw bytes, Vec<u8>), {} block per seg ",
        cache_number, num_block_per_seg
    );

    eprintln!(
        "scenario {}, pid {}, base memory {}",
        scenario, pid, base_memory_usage
    );

    let cache = InMemoryLruCache::with_items_capacity(String::from(""), cache_number);

    for _ in 0..cache_number {
        let uuid = Uuid::new_v4();
        cache.insert(format!("{}", uuid.simple()), segment_info_bytes.clone());
    }

    show_memory_usage(
        "SegmentInfoCache(raw bytes Vec<u8>)",
        base_memory_usage,
        cache_number,
    );

    Ok(())
}

// cargo test --test it storages::fuse::bloom_index_meta_size::test_segment_raw_repr_bytes_size --no-fail-fast -- --ignored --exact -Z unstable-options --show-output
#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_segment_raw_repr_bytes_size() -> databend_common_exception::Result<()> {
    let cache_number = 3000;
    let num_block_per_seg = 1000;

    let segment_info = build_test_segment_info(num_block_per_seg)?;
    let segment_raw = CompactSegmentInfo::try_from(&segment_info)?;

    let sys = System::new_all();
    let pid = get_current_pid().unwrap();
    let process = sys.process(pid).unwrap();
    let base_memory_usage = process.memory();

    let scenario = format!(
        "{} SegmentInfo(compact repr), {} block per seg ",
        cache_number, num_block_per_seg
    );

    eprintln!(
        "scenario {}, pid {}, base memory {}",
        scenario, pid, base_memory_usage
    );

    let cache = InMemoryLruCache::with_items_capacity(String::from(""), cache_number);

    for _ in 0..cache_number {
        let uuid = Uuid::new_v4();
        cache.insert(format!("{}", uuid.simple()), segment_raw.clone());
    }
    show_memory_usage(
        "SegmentInfoCache (compact repr)",
        base_memory_usage,
        cache_number,
    );

    Ok(())
}

fn build_test_segment_info(
    num_blocks_per_seg: usize,
) -> databend_common_exception::Result<SegmentInfo> {
    let col_meta = ColumnMeta::Parquet(SingleColumnMeta {
        offset: 0,
        len: 0,
        num_values: 0,
    });

    let col_stat = ColumnStatistics::new(
        Scalar::String(String::from_utf8(vec![b'a'; STATS_STRING_PREFIX_LEN])?),
        Scalar::String(String::from_utf8(vec![b'a'; STATS_STRING_PREFIX_LEN])?),
        0,
        0,
        None,
    );

    let number_col_stat = ColumnStatistics::new(
        Scalar::Number(NumberScalar::Int32(0)),
        Scalar::Number(NumberScalar::Int32(0)),
        0,
        0,
        None,
    );

    // 20 string columns, 5 number columns
    let num_string_columns = 20;
    let num_number_columns = 5;
    let col_metas = (0..num_string_columns + num_number_columns)
        .map(|id| (id as ColumnId, col_meta.clone()))
        .collect::<HashMap<_, _>>();

    assert_eq!(num_number_columns + num_string_columns, col_metas.len());

    let mut col_stats = (0..num_string_columns)
        .map(|id| (id as ColumnId, col_stat.clone()))
        .collect::<HashMap<_, _>>();
    for idx in num_string_columns..num_string_columns + num_number_columns {
        col_stats.insert(idx as ColumnId, number_col_stat.clone());
    }
    assert_eq!(num_number_columns + num_string_columns, col_stats.len());

    let location_gen = TableMetaLocationGenerator::with_prefix("/root/12345/67890".to_owned());

    let (block_location, block_uuid) = location_gen.gen_block_location(Default::default());
    let block_meta = BlockMeta {
        row_count: 0,
        block_size: 0,
        file_size: 0,
        col_stats: col_stats.clone(),
        col_metas,
        cluster_stats: None,
        location: block_location,
        bloom_filter_index_location: Some(location_gen.block_bloom_index_location(&block_uuid)),
        bloom_filter_index_size: 0,
        inverted_index_size: None,
        compression: Compression::Lz4,
        create_on: Some(Utc::now()),
    };

    let block_metas = (0..num_blocks_per_seg)
        .map(|_| Arc::new(block_meta.clone()))
        .collect::<Vec<_>>();

    let statistics = Statistics {
        row_count: 0,
        block_count: 0,
        perfect_block_count: 0,
        uncompressed_byte_size: 0,
        compressed_byte_size: 0,
        index_size: 0,
        col_stats: col_stats.clone(),
        cluster_stats: None,
    };

    Ok(SegmentInfo::new(block_metas, statistics))
}

#[allow(dead_code)]
fn populate_cache<T>(cache: &InMemoryLruCache<T>, item: T, num_cache: usize)
where T: Clone + Into<CacheValue<T>> {
    for _ in 0..num_cache {
        let uuid = Uuid::new_v4();
        cache.insert(format!("{}", uuid.simple()), item.clone());
    }
}

#[allow(dead_code)]
async fn setup() -> databend_common_exception::Result<FileMetaData> {
    let fields = (0..23)
        .map(|_| TableField::new("id", TableDataType::Number(NumberDataType::Int32)))
        .collect::<Vec<_>>();

    let schema = TableSchemaRefExt::create(fields);

    let mut columns = vec![];
    for _ in 0..schema.fields().len() {
        // values do not matter
        let column = Int32Type::from_data(vec![1]);
        columns.push(column)
    }

    let block = DataBlock::new_from_columns(columns);
    let operator = Operator::new(opendal::services::Memory::default())?.finish();
    let loc_generator = TableMetaLocationGenerator::with_prefix("/".to_owned());
    let col_stats = gen_columns_statistics(&block, None, &schema)?;
    let block_writer = BlockWriter::new(&operator, &loc_generator, Default::default(), true);
    let (_block_meta, thrift_file_meta) = block_writer
        .write(FuseStorageFormat::Parquet, &schema, block, col_stats, None)
        .await?;

    Ok(thrift_file_meta.unwrap())
}

fn show_memory_usage(case: &str, base_memory_usage: u64, num_cache_items: usize) {
    let sys = System::new_all();
    let pid = get_current_pid().unwrap();
    let process = sys.process(pid).unwrap();
    {
        let memory_after = process.memory();
        let delta = memory_after - base_memory_usage;
        let delta_gb = (delta as f64) / 1024.0 / 1024.0 / 1024.0;
        eprintln!(
            "
            cache item type : {},
            number of cached items {},
            mem usage(B):{:+},
            mem usage(GB){:+}
            ",
            case, num_cache_items, delta, delta_gb
        );
    }
}
