//  Copyright 2021 Datafuse Labs.
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
use std::sync::Arc;

use chrono::Utc;
use databend_common_base::base::tokio;
use databend_common_expression::type_check::check;
use databend_common_expression::types::number::Int32Type;
use databend_common_expression::types::number::NumberScalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::BlockThresholds;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::RawExpr;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_functions::aggregates::eval_aggr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_storages_fuse::statistics::reducers::reduce_block_metas;
use databend_common_storages_fuse::statistics::Trim;
use databend_common_storages_fuse::statistics::STATS_REPLACEMENT_CHAR;
use databend_common_storages_fuse::statistics::STATS_STRING_PREFIX_LEN;
use databend_common_storages_fuse::FuseStorageFormat;
use databend_query::storages::fuse::io::TableMetaLocationGenerator;
use databend_query::storages::fuse::statistics::gen_columns_statistics;
use databend_query::storages::fuse::statistics::reducers;
use databend_query::storages::fuse::statistics::ClusterStatsGenerator;
use databend_query::storages::fuse::statistics::StatisticsAccumulator;
use databend_query::test_kits::*;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::Compression;
use databend_storages_common_table_meta::meta::Statistics;
use opendal::Operator;
use rand::Rng;

#[test]
fn test_ft_stats_block_stats() -> databend_common_exception::Result<()> {
    let schema = Arc::new(TableSchema::new(vec![
        TableField::new("a", TableDataType::Number(NumberDataType::Int32)),
        TableField::new("b", TableDataType::String),
    ]));
    let block = DataBlock::new_from_columns(vec![
        Int32Type::from_data(vec![1, 2, 3]),
        StringType::from_data(vec!["aa", "aa", "bb"]),
    ]);

    let r = gen_columns_statistics(&block, None, &schema)?;
    assert_eq!(2, r.len());
    let col_stats = r.get(&0).unwrap();
    assert_eq!(col_stats.min(), &Scalar::Number(NumberScalar::Int32(1)));
    assert_eq!(col_stats.max(), &Scalar::Number(NumberScalar::Int32(3)));
    assert_eq!(col_stats.distinct_of_values, Some(3));
    let col_stats = r.get(&1).unwrap();
    assert_eq!(col_stats.min(), &Scalar::String("aa".to_string()));
    assert_eq!(col_stats.max(), &Scalar::String("bb".to_string()));
    assert_eq!(col_stats.distinct_of_values, Some(2));
    Ok(())
}

#[test]
fn test_ft_stats_block_stats_with_column_distinct_count() -> databend_common_exception::Result<()> {
    let schema = Arc::new(TableSchema::new(vec![
        TableField::new("a", TableDataType::Number(NumberDataType::Int32)),
        TableField::new("b", TableDataType::String),
    ]));

    let block = DataBlock::new_from_columns(vec![
        Int32Type::from_data(vec![1, 2, 3]),
        StringType::from_data(vec!["aa", "aa", "bb"]),
    ]);
    let mut column_distinct_count = HashMap::new();
    column_distinct_count.insert(0, 3);
    column_distinct_count.insert(1, 2);
    let r = gen_columns_statistics(&block, Some(column_distinct_count), &schema)?;
    assert_eq!(2, r.len());
    let col_stats = r.get(&0).unwrap();
    assert_eq!(col_stats.min(), &Scalar::Number(NumberScalar::Int32(1)));
    assert_eq!(col_stats.max(), &Scalar::Number(NumberScalar::Int32(3)));
    assert_eq!(col_stats.distinct_of_values, Some(3));
    let col_stats = r.get(&1).unwrap();
    assert_eq!(col_stats.min(), &Scalar::String("aa".to_string()));
    assert_eq!(col_stats.max(), &Scalar::String("bb".to_string()));
    assert_eq!(col_stats.distinct_of_values, Some(2));
    Ok(())
}

#[test]
fn test_ft_tuple_stats_block_stats() -> databend_common_exception::Result<()> {
    let schema = Arc::new(TableSchema::new(vec![TableField::new(
        "a",
        TableDataType::Tuple {
            fields_name: vec!["a11".to_string(), "a12".to_string()],
            fields_type: vec![
                TableDataType::Number(NumberDataType::Int32),
                TableDataType::Number(NumberDataType::Int32),
            ],
        },
    )]));

    let inner_columns = vec![
        Int32Type::from_data(vec![1, 2, 3]),
        Int32Type::from_data(vec![4, 5, 6]),
    ];
    let column = Column::Tuple(inner_columns);

    let block = DataBlock::new_from_columns(vec![column]);

    let r = gen_columns_statistics(&block, None, &schema)?;
    assert_eq!(2, r.len());
    let col0_stats = r.get(&0).unwrap();
    assert_eq!(col0_stats.min(), &Scalar::Number(NumberScalar::Int32(1)));
    assert_eq!(col0_stats.max(), &Scalar::Number(NumberScalar::Int32(3)));

    let col1_stats = r.get(&1).unwrap();
    assert_eq!(col1_stats.min(), &Scalar::Number(NumberScalar::Int32(4)));
    assert_eq!(col1_stats.max(), &Scalar::Number(NumberScalar::Int32(6)));
    Ok(())
}

#[test]
fn test_ft_stats_col_stats_reduce() -> databend_common_exception::Result<()> {
    let num_of_blocks = 10;
    let rows_per_block = 3;
    let val_start_with = 1;

    let (schema, blocks) =
        TestFixture::gen_sample_blocks_ex(num_of_blocks, rows_per_block, val_start_with);
    let col_stats = blocks
        .iter()
        .map(|b| gen_columns_statistics(&b.clone().unwrap(), None, &schema))
        .collect::<databend_common_exception::Result<Vec<_>>>()?;
    let r = reducers::reduce_block_statistics(&col_stats);
    assert_eq!(3, r.len());
    let col0_stats = r.get(&0).unwrap();
    assert_eq!(
        col0_stats.min(),
        &Scalar::Number(NumberScalar::Int32(val_start_with))
    );
    assert_eq!(
        col0_stats.max(),
        &Scalar::Number(NumberScalar::Int32(num_of_blocks as i32))
    );

    let col1_stats = r.get(&1).unwrap();
    assert_eq!(
        col1_stats.min(),
        &Scalar::Number(NumberScalar::Int32(val_start_with * 2))
    );

    assert_eq!(
        col1_stats.max(),
        &Scalar::Number(NumberScalar::Int32((num_of_blocks * 2) as i32))
    );
    let col2_stats = r.get(&2).unwrap();
    assert_eq!(
        col2_stats.min(),
        &Scalar::Number(NumberScalar::Int32(val_start_with * 3))
    );
    assert_eq!(
        col2_stats.max(),
        &Scalar::Number(NumberScalar::Int32((num_of_blocks * 3) as i32))
    );
    Ok(())
}

#[test]
fn test_reduce_block_statistics_in_memory_size() -> databend_common_exception::Result<()> {
    let iter = |mut idx| {
        std::iter::from_fn(move || {
            idx += 1;
            Some((
                idx,
                ColumnStatistics::new(Scalar::Null, Scalar::Null, 1, 1, Some(1)),
            ))
        })
    };

    let num_of_cols = 100;
    // combine two statistics
    let col_stats_left = HashMap::from_iter(iter(0).take(num_of_cols));
    let col_stats_right = HashMap::from_iter(iter(0).take(num_of_cols));
    let r = reducers::reduce_block_statistics(&[col_stats_left, col_stats_right]);
    assert_eq!(num_of_cols, r.len());
    // there should be 100 columns in the result
    for idx in 1..=100 {
        let col_stats = r.get(&idx);
        assert!(col_stats.is_some());
        let col_stats = col_stats.unwrap();
        // for each column, the reduced value of in_memory_size should be 1 + 1
        assert_eq!(col_stats.in_memory_size, 2);
        // for each column, the reduced value of null_count should be 1 + 1
        assert_eq!(col_stats.null_count, 2);
    }
    Ok(())
}

#[test]
fn test_reduce_cluster_statistics() -> databend_common_exception::Result<()> {
    let default_cluster_key_id = Some(0);
    let cluster_stats_0 = Some(ClusterStatistics::new(
        0,
        vec![Scalar::from(2i64)],
        vec![Scalar::from(4i64)],
        0,
        None,
    ));

    let cluster_stats_1 = Some(ClusterStatistics::new(
        0,
        vec![Scalar::from(1i64)],
        vec![Scalar::from(3i64)],
        1,
        None,
    ));

    let cluster_stats_2 = Some(ClusterStatistics::new(
        0,
        vec![Scalar::Null],
        vec![Scalar::Null],
        0,
        None,
    ));

    let res_0 = reducers::reduce_cluster_statistics(
        &[cluster_stats_0.clone(), cluster_stats_1.clone()],
        default_cluster_key_id,
    );
    let expect = Some(ClusterStatistics::new(
        0,
        vec![Scalar::from(1i64)],
        vec![Scalar::from(4i64)],
        1,
        None,
    ));
    assert_eq!(res_0, expect);

    let res_1 = reducers::reduce_cluster_statistics(
        &[cluster_stats_2, cluster_stats_0.clone()],
        default_cluster_key_id,
    );
    let expect = Some(ClusterStatistics::new(
        0,
        vec![Scalar::from(2i64)],
        vec![Scalar::Null],
        0,
        None,
    ));
    assert_eq!(res_1, expect);

    let res_2 = reducers::reduce_cluster_statistics(
        &[cluster_stats_0.clone(), None],
        default_cluster_key_id,
    );
    assert_eq!(res_2, None);

    let res_3 = reducers::reduce_cluster_statistics(&[cluster_stats_0, cluster_stats_1], Some(1));
    assert_eq!(res_3, None);

    // multi cluster keys.
    let multi_cluster_stats_0 = Some(ClusterStatistics::new(
        0,
        vec![Scalar::from(1i64), Scalar::from(4i64)],
        vec![Scalar::from(1i64), Scalar::from(4i64)],
        0,
        None,
    ));
    let multi_cluster_stats_2 = Some(ClusterStatistics::new(
        0,
        vec![Scalar::from(3i64), Scalar::from(2i64)],
        vec![Scalar::from(3i64), Scalar::from(2i64)],
        0,
        None,
    ));
    let res_4 = reducers::reduce_cluster_statistics(
        &[multi_cluster_stats_0, multi_cluster_stats_2],
        default_cluster_key_id,
    );
    let expect = Some(ClusterStatistics::new(
        0,
        vec![Scalar::from(1i64), Scalar::from(4i64)],
        vec![Scalar::from(3i64), Scalar::from(2i64)],
        0,
        None,
    ));
    assert_eq!(res_4, expect);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_accumulator() -> databend_common_exception::Result<()> {
    let (schema, blocks) = TestFixture::gen_sample_blocks(10, 1);
    let mut stats_acc = StatisticsAccumulator::default();

    let operator = Operator::new(opendal::services::Memory::default())?.finish();
    let loc_generator = TableMetaLocationGenerator::with_prefix("/".to_owned());
    for item in blocks {
        let block = item?;
        let col_stats = gen_columns_statistics(&block, None, &schema)?;
        let block_writer = BlockWriter::new(&operator, &loc_generator, Default::default(), true);
        let (block_meta, _index_meta) = block_writer
            .write(FuseStorageFormat::Parquet, &schema, block, col_stats, None)
            .await?;
        stats_acc.add_with_block_meta(block_meta);
    }

    assert_eq!(10, stats_acc.blocks_metas.len());
    assert!(stats_acc.summary_row_count > 0);
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_ft_cluster_stats_with_stats() -> databend_common_exception::Result<()> {
    let schema = DataSchemaRefExt::create(vec![DataField::new(
        "a",
        DataType::Number(NumberDataType::Int32),
    )]);

    let columns = vec![Int32Type::from_data(vec![1i32, 2, 3])];

    let blocks = DataBlock::new_from_columns(columns);
    let origin = Some(ClusterStatistics::new(
        0,
        vec![Scalar::Number(NumberScalar::Int32(1))],
        vec![Scalar::Number(NumberScalar::Int32(5))],
        0,
        None,
    ));

    let block_compactor = BlockThresholds::new(1_000_000, 800_000, 100 * 1024 * 1024);
    let stats_gen = ClusterStatsGenerator::new(
        0,
        vec![0],
        0,
        None,
        0,
        block_compactor,
        vec![],
        vec![],
        FunctionContext::default(),
    );
    let stats = stats_gen.gen_with_origin_stats(&blocks, origin.clone())?;
    assert!(stats.is_some());
    let stats = stats.unwrap();
    assert_eq!(&vec![Scalar::Number(NumberScalar::Int32(1))], stats.min());
    assert_eq!(&vec![Scalar::Number(NumberScalar::Int32(3))], stats.max());

    // add expression executor.
    let expr = RawExpr::FunctionCall {
        span: None,
        name: "plus".to_string(),
        params: vec![],
        args: vec![
            RawExpr::ColumnRef {
                span: None,
                id: 0usize,
                data_type: schema.field(0).data_type().clone(),
                display_name: schema.field(0).name().clone(),
            },
            RawExpr::Constant {
                span: None,
                scalar: Scalar::Number(NumberScalar::UInt64(1)),
            },
        ],
    };
    let expr = check(&expr, &BUILTIN_FUNCTIONS).unwrap();
    let operators = vec![BlockOperator::Map {
        exprs: vec![expr],
        projections: None,
    }];

    let stats_gen = ClusterStatsGenerator::new(
        0,
        vec![1],
        0,
        None,
        0,
        block_compactor,
        operators,
        vec![],
        FunctionContext::default(),
    );
    let stats = stats_gen.gen_with_origin_stats(&blocks, origin.clone())?;
    assert!(stats.is_some());
    let stats = stats.unwrap();
    assert_eq!(&vec![Scalar::Number(NumberScalar::Int64(2))], stats.min());
    assert_eq!(&vec![Scalar::Number(NumberScalar::Int64(4))], stats.max());

    // different cluster_key_id.
    let stats_gen = ClusterStatsGenerator::new(
        1,
        vec![0],
        0,
        None,
        0,
        block_compactor,
        vec![],
        vec![],
        FunctionContext::default(),
    );
    let stats = stats_gen.gen_with_origin_stats(&blocks, origin)?;
    assert!(stats.is_none());

    Ok(())
}

#[test]
fn test_ft_stats_block_stats_string_columns_trimming() -> databend_common_exception::Result<()> {
    let suite = || -> databend_common_exception::Result<()> {
        // prepare random strings
        // 100 string, length ranges from 0 to 100 (chars)
        let mut rand_strings: Vec<String> = vec![];
        for _ in 0..100 {
            let mut rnd = rand::thread_rng();
            let rand_string: String = rand::thread_rng()
                .sample_iter::<char, _>(rand::distributions::Standard)
                .take(rnd.gen_range(0..1000))
                .collect();

            rand_strings.push(rand_string);
        }

        let min_expr = rand_strings.iter().min().unwrap();
        let max_expr = rand_strings.iter().max().unwrap();

        let data_value_min = Scalar::String(min_expr.clone());
        let data_value_max = Scalar::String(max_expr.clone());

        let trimmed_min = data_value_min.clone().trim_min();
        let trimmed_max = data_value_max.clone().trim_max();

        let meaningless_to_collect_max = is_degenerated_case(max_expr.as_str());

        if meaningless_to_collect_max {
            assert!(trimmed_max.is_none());
        } else {
            assert!(trimmed_max.is_some());
            let trimmed = trimmed_max.unwrap().as_string().unwrap().clone();
            assert!(char_len(&trimmed) <= STATS_STRING_PREFIX_LEN);
            assert!(Scalar::String(trimmed) >= data_value_max)
        }

        {
            assert!(trimmed_min.is_some());
            let trimmed = trimmed_min.unwrap().as_string().unwrap().clone();
            assert!(char_len(&trimmed) <= STATS_STRING_PREFIX_LEN);
            assert!(Scalar::String(trimmed) <= data_value_min);
        }
        Ok(())
    };

    // let runs = 0..1000;  // use this setting at home
    let runs = 0..100;
    for _ in runs {
        suite()?
    }
    Ok(())
}

#[test]
fn test_ft_stats_block_stats_string_columns_trimming_using_eval()
-> databend_common_exception::Result<()> {
    // verifies (randomly) the following assumptions:
    //
    // https://github.com/datafuselabs/databend/issues/7829
    // > ...
    // > in a way that preserves the property of min/max statistics:
    // > the trimmed max should be larger than the non-trimmed one, and the trimmed min
    // > should be lesser than the non-trimmed one.

    let suite = || -> databend_common_exception::Result<()> {
        // prepare random strings
        // 100 string, length ranges from 0 to 100 (chars)
        let mut rand_strings: Vec<String> = vec![];
        let rows = 100;
        for _ in 0..rows {
            let mut rnd = rand::thread_rng();
            let rand_string: String = rand::thread_rng()
                .sample_iter::<char, _>(rand::distributions::Standard)
                .take(rnd.gen_range(0..1000))
                .collect();

            rand_strings.push(rand_string);
        }

        let schema = Arc::new(TableSchema::new(vec![TableField::new(
            "a",
            TableDataType::String,
        )]));

        // build test data block, which has only on column, of String type
        let data_col = StringType::from_data(
            rand_strings
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<&str>>(),
        );
        let block = DataBlock::new_from_columns(vec![data_col.clone()]);

        let min_col = eval_aggr("min", vec![], &[data_col.clone()], rows)?;
        let max_col = eval_aggr("max", vec![], &[data_col], rows)?;

        let min_expr = min_col.0.index(0).unwrap();
        let max_expr = max_col.0.index(0).unwrap();

        // generate the statistics of column
        let stats_of_columns = gen_columns_statistics(&block, None, &schema).unwrap();

        // check if the max value (untrimmed) is in degenerated condition:
        // - the length of string value is larger or equal than STRING_PREFIX_LEN
        // - AND the string has a prefix of length STRING_PREFIX_LEN, for all the char C in prefix,
        //   C > REPLACEMENT_CHAR; which means we can not replace any of them.
        let string_max_expr = max_expr.as_string().unwrap().to_string();
        let meaningless_to_collect_max = is_degenerated_case(string_max_expr.as_str());

        if meaningless_to_collect_max {
            // no stats will be collected
            assert!(!stats_of_columns.contains_key(&0))
        } else {
            // Finally:
            // check that, trimmed "col_stats.max" always large than or equal to the untrimmed "max_expr"
            let col_stats = stats_of_columns.get(&0).unwrap();
            assert!(
                col_stats.max().clone() >= max_expr.to_owned(),
                "left [{}]\nright [{}]",
                col_stats.max().clone(),
                max_expr
            );
            // check that, trimmed "col_stats.min always less than or equal to the untrimmed "mn_expr"
            assert!(
                col_stats.min().clone() <= min_expr.to_owned(),
                "left [{}]\nright [{}]",
                col_stats.min().clone(),
                min_expr
            );
        }
        Ok(())
    };

    // let runs = 0..1000;  // use this at home

    let runs = 0..100;
    for _ in runs {
        suite()?
    }
    Ok(())
}

fn is_degenerated_case(value: &str) -> bool {
    // check if the value (untrimmed) is in degenerated condition:
    // - the length of string value is larger or equal than STRING_PREFIX_LEN
    // - AND the string has a prefix of length STRING_PREFIX_LEN, for all the char C in prefix,
    //   C > REPLACEMENT_CHAR; which means we can not replace any of them.
    let larger_than_prefix_len = value.chars().count() > STATS_STRING_PREFIX_LEN;
    let prefixed_with_irreplaceable_chars = value
        .chars()
        .take(STATS_STRING_PREFIX_LEN)
        .all(|c| c >= STATS_REPLACEMENT_CHAR);

    larger_than_prefix_len && prefixed_with_irreplaceable_chars
}

fn char_len(value: &str) -> usize {
    value.chars().count()
}

#[test]
fn test_reduce_block_meta() -> databend_common_exception::Result<()> {
    // case 1: empty input should return the default statistics
    let block_metas: Vec<BlockMeta> = vec![];
    let reduced = reduce_block_metas(&block_metas, BlockThresholds::default(), None);
    assert_eq!(Statistics::default(), reduced);

    // case 2: accumulated variants of size index should be as expected
    // reduction of ColumnStatistics, ClusterStatistics already covered by other cases
    let mut rng = rand::thread_rng();
    let size = 100_u64;
    let location = ("".to_owned(), 0);
    let mut blocks = vec![];
    let mut acc_row_count = 0;
    let mut acc_block_size = 0;
    let mut acc_file_size = 0;
    let mut acc_bloom_filter_index_size = 0;
    for _ in 0..size {
        let row_count = rng.gen::<u64>() / size;
        let block_size = rng.gen::<u64>() / size;
        let file_size = rng.gen::<u64>() / size;
        let bloom_filter_index_size = rng.gen::<u64>() / size;
        acc_row_count += row_count;
        acc_block_size += block_size;
        acc_file_size += file_size;
        acc_bloom_filter_index_size += bloom_filter_index_size;
        let block_meta = BlockMeta::new(
            row_count,
            block_size,
            file_size,
            HashMap::new(),
            HashMap::new(),
            None,
            location.clone(),
            None,
            bloom_filter_index_size,
            None,
            Compression::Lz4Raw,
            Some(Utc::now()),
        );
        blocks.push(block_meta);
    }

    let stats = reduce_block_metas(&blocks, BlockThresholds::default(), None);

    assert_eq!(acc_row_count, stats.row_count);
    assert_eq!(acc_block_size, stats.uncompressed_byte_size);
    assert_eq!(acc_file_size, stats.compressed_byte_size);
    assert_eq!(acc_bloom_filter_index_size, stats.index_size);

    Ok(())
}
