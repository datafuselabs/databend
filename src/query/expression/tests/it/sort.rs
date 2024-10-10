// Copyright 2022 Datafuse Labs.
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

use std::vec;

use databend_common_exception::Result;
use databend_common_expression::types::decimal::*;
use databend_common_expression::types::number::*;
use databend_common_expression::types::StringType;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::SortColumnDescription;

use crate::common::new_block;
use crate::rand_block_for_all_types;

#[test]
fn test_block_sort() -> Result<()> {
    let block = new_block(&[
        Int64Type::from_data(vec![6i64, 4, 3, 2, 1, 1, 7]),
        StringType::from_data(vec!["b1", "b2", "b3", "b4", "b5", "b6", "b7"]),
    ]);

    // test cast:
    // - sort descriptions
    // - limit
    // - expected cols
    let test_cases: Vec<(Vec<SortColumnDescription>, Option<usize>, Vec<Column>)> = vec![
        (
            vec![SortColumnDescription {
                offset: 0,
                asc: true,
                nulls_first: false,
                is_nullable: false,
            }],
            None,
            vec![
                Int64Type::from_data(vec![1_i64, 1, 2, 3, 4, 6, 7]),
                StringType::from_data(vec!["b5", "b6", "b4", "b3", "b2", "b1", "b7"]),
            ],
        ),
        (
            vec![SortColumnDescription {
                offset: 0,
                asc: true,
                nulls_first: false,
                is_nullable: false,
            }],
            Some(4),
            vec![
                Int64Type::from_data(vec![1_i64, 1, 2, 3]),
                StringType::from_data(vec!["b5", "b6", "b4", "b3"]),
            ],
        ),
        (
            vec![SortColumnDescription {
                offset: 1,
                asc: false,
                nulls_first: false,
                is_nullable: false,
            }],
            None,
            vec![
                Int64Type::from_data(vec![7_i64, 1, 1, 2, 3, 4, 6]),
                StringType::from_data(vec!["b7", "b6", "b5", "b4", "b3", "b2", "b1"]),
            ],
        ),
        (
            vec![
                SortColumnDescription {
                    offset: 0,
                    asc: true,
                    nulls_first: false,
                    is_nullable: false,
                },
                SortColumnDescription {
                    offset: 1,
                    asc: false,
                    nulls_first: false,
                    is_nullable: false,
                },
            ],
            None,
            vec![
                Int64Type::from_data(vec![1_i64, 1, 2, 3, 4, 6, 7]),
                StringType::from_data(vec!["b6", "b5", "b4", "b3", "b2", "b1", "b7"]),
            ],
        ),
    ];

    for (sort_descs, limit, expected) in test_cases {
        let res = DataBlock::sort(&block, &sort_descs, limit)?;

        for (entry, expect) in res.columns().iter().zip(expected.iter()) {
            assert_eq!(
                entry.value.as_column().unwrap(),
                expect,
                "the column after sort is wrong, expect: {:?}, got: {:?}",
                expect,
                entry.value
            );
        }
    }

    let decimal_block = new_block(&[
        Decimal128Type::from_data(vec![6i128, 4, 3, 2, 1, 1, 7]),
        StringType::from_data(vec!["b1", "b2", "b3", "b4", "b5", "b6", "b7"]),
    ]);

    // test cast:
    // - sort descriptions
    // - limit
    // - expected cols
    let test_cases: Vec<(Vec<SortColumnDescription>, Option<usize>, Vec<Column>)> = vec![
        (
            vec![SortColumnDescription {
                offset: 0,
                asc: true,
                nulls_first: false,
                is_nullable: false,
            }],
            None,
            vec![
                Decimal128Type::from_data(vec![1_i128, 1, 2, 3, 4, 6, 7]),
                StringType::from_data(vec!["b5", "b6", "b4", "b3", "b2", "b1", "b7"]),
            ],
        ),
        (
            vec![SortColumnDescription {
                offset: 0,
                asc: true,
                nulls_first: false,
                is_nullable: false,
            }],
            Some(4),
            vec![
                Decimal128Type::from_data(vec![1_i128, 1, 2, 3]),
                StringType::from_data(vec!["b5", "b6", "b4", "b3"]),
            ],
        ),
        (
            vec![SortColumnDescription {
                offset: 1,
                asc: false,
                nulls_first: false,
                is_nullable: false,
            }],
            None,
            vec![
                Decimal128Type::from_data(vec![7_i128, 1, 1, 2, 3, 4, 6]),
                StringType::from_data(vec!["b7", "b6", "b5", "b4", "b3", "b2", "b1"]),
            ],
        ),
        (
            vec![
                SortColumnDescription {
                    offset: 0,
                    asc: true,
                    nulls_first: false,
                    is_nullable: false,
                },
                SortColumnDescription {
                    offset: 1,
                    asc: false,
                    nulls_first: false,
                    is_nullable: false,
                },
            ],
            None,
            vec![
                Decimal128Type::from_data(vec![1_i128, 1, 2, 3, 4, 6, 7]),
                StringType::from_data(vec!["b6", "b5", "b4", "b3", "b2", "b1", "b7"]),
            ],
        ),
    ];

    for (sort_descs, limit, expected) in test_cases {
        let res = DataBlock::sort(&decimal_block, &sort_descs, limit)?;

        for (entry, expect) in res.columns().iter().zip(expected.iter()) {
            assert_eq!(
                entry.value.as_column().unwrap(),
                expect,
                "the column after sort is wrong, expect: {:?}, got: {:?}",
                expect,
                entry.value
            );
        }
    }

    Ok(())
}

#[test]
fn sort_concat() {
    // Sort(Sort A || Sort B)  =   Sort (A || B)
    use databend_common_expression::DataBlock;
    use itertools::Itertools;
    use rand::seq::SliceRandom;
    use rand::Rng;

    let mut rng = rand::thread_rng();
    let num_blocks = 100;

    for _i in 0..num_blocks {
        let block_a = rand_block_for_all_types(rng.gen_range(0..100));
        let block_b = rand_block_for_all_types(rng.gen_range(0..100));

        let mut sort_index: Vec<usize> = (0..block_a.num_columns()).collect();
        sort_index.shuffle(&mut rng);

        let sort_desc = sort_index
            .iter()
            .map(|i| SortColumnDescription {
                offset: *i,
                asc: rng.gen_bool(0.5),
                nulls_first: rng.gen_bool(0.5),
                is_nullable: rng.gen_bool(0.5),
            })
            .collect_vec();

        let concat_ab_0 = DataBlock::concat(&[block_a.clone(), block_b.clone()]).unwrap();

        let sort_a = DataBlock::sort(&block_a, &sort_desc, None).unwrap();
        let sort_b = DataBlock::sort(&block_b, &sort_desc, None).unwrap();
        let concat_ab_1 = DataBlock::concat(&[sort_a, sort_b]).unwrap();

        let block_1 = DataBlock::sort(&concat_ab_0, &sort_desc, None).unwrap();
        let block_2 = DataBlock::sort(&concat_ab_1, &sort_desc, None).unwrap();

        assert_eq!(block_1.num_columns(), block_2.num_columns());
        assert_eq!(block_1.num_rows(), block_2.num_rows());

        let columns_1 = block_1.columns();
        let columns_2 = block_2.columns();
        for idx in 0..columns_1.len() {
            assert_eq!(columns_1[idx].data_type, columns_2[idx].data_type);
            assert_eq!(columns_1[idx].value, columns_2[idx].value);
        }
    }
}
