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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_exception::Result;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::array::ArrayColumn;
use databend_common_expression::types::map::KvColumn;
use databend_common_expression::types::map::KvPair;
use databend_common_expression::types::number::NumberScalar;
use databend_common_expression::types::number::UInt8Type;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::VariantType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataBlock;
use databend_common_expression::Expr;
use databend_common_expression::FieldIndex;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::Value;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_index::filters::BlockFilter as LatestBloom;
use databend_storages_common_index::filters::Xor8Filter;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_index::FilterEvalResult;
use databend_storages_common_index::Index;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::Versioned;

#[test]
fn test_bloom_filter() -> Result<()> {
    let schema = Arc::new(TableSchema::new(vec![
        TableField::new("0", TableDataType::Number(NumberDataType::UInt8)),
        TableField::new("1", TableDataType::String),
        TableField::new(
            "2",
            TableDataType::Map(Box::new(TableDataType::Tuple {
                fields_name: vec!["key".to_string(), "value".to_string()],
                fields_type: vec![
                    TableDataType::Number(NumberDataType::UInt8),
                    TableDataType::String,
                ],
            })),
        ),
        TableField::new(
            "3",
            TableDataType::Map(Box::new(TableDataType::Tuple {
                fields_name: vec!["key".to_string(), "value".to_string()],
                fields_type: vec![TableDataType::String, TableDataType::Variant],
            })),
        ),
    ]));

    let map_ty1 = DataType::Map(Box::new(DataType::Tuple(vec![
        DataType::Number(NumberDataType::UInt8),
        DataType::String,
    ])));
    let map_ty2 = DataType::Map(Box::new(DataType::Tuple(vec![
        DataType::String,
        DataType::Variant,
    ])));

    let blocks = [
        DataBlock::new(
            vec![
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt8),
                    Value::Scalar(Scalar::Number(NumberScalar::UInt8(1))),
                ),
                BlockEntry::new(
                    DataType::String,
                    Value::Scalar(Scalar::String("a".to_string())),
                ),
                BlockEntry::new(
                    map_ty1.clone(),
                    Value::Scalar(Scalar::Map(Column::Tuple(vec![
                        UInt8Type::from_data(vec![1, 2]),
                        StringType::from_data(vec!["a", "b"]),
                    ]))),
                ),
                BlockEntry::new(
                    map_ty2.clone(),
                    Value::Scalar(Scalar::Map(Column::Tuple(vec![
                        StringType::from_data(vec!["a", "b"]),
                        VariantType::from_data(vec![
                            jsonb::parse_value(r#""abc""#.as_bytes()).unwrap().to_vec(),
                            jsonb::parse_value(r#"100"#.as_bytes()).unwrap().to_vec(),
                        ]),
                    ]))),
                ),
            ],
            2,
        ),
        DataBlock::new_from_columns(vec![
            UInt8Type::from_data(vec![2, 3]),
            StringType::from_data(vec!["b", "c"]),
            Column::Map(Box::new(
                ArrayColumn::<KvPair<AnyType, AnyType>> {
                    values: KvColumn {
                        keys: UInt8Type::from_data(vec![1, 2, 3]),
                        values: StringType::from_data(vec!["b", "c", "d"]),
                    },
                    offsets: Buffer::<u64>::from(vec![0, 2, 3]),
                }
                .upcast(),
            )),
            Column::Map(Box::new(
                ArrayColumn::<KvPair<AnyType, AnyType>> {
                    values: KvColumn {
                        keys: StringType::from_data(vec!["b", "c", "d"]),
                        values: VariantType::from_data(vec![
                            jsonb::parse_value(r#""def""#.as_bytes()).unwrap().to_vec(),
                            jsonb::parse_value(r#"true"#.as_bytes()).unwrap().to_vec(),
                            jsonb::parse_value(r#""xyz""#.as_bytes()).unwrap().to_vec(),
                        ]),
                    },
                    offsets: Buffer::<u64>::from(vec![0, 2, 3]),
                }
                .upcast(),
            )),
        ]),
    ];

    let bloom_columns = bloom_columns_map(schema.clone(), vec![0, 1, 2, 3]);
    let bloom_fields = bloom_columns.values().cloned().collect::<Vec<_>>();
    let index = BloomIndex::try_create(
        FunctionContext::default(),
        LatestBloom::VERSION,
        &blocks,
        bloom_columns,
    )?
    .unwrap();

    assert_eq!(
        FilterEvalResult::MustFalse,
        eval_index(
            &index,
            "0",
            bloom_fields.clone(),
            schema.clone(),
            Scalar::Number(NumberScalar::UInt8(0)),
            DataType::Number(NumberDataType::UInt8)
        )
    );
    assert_eq!(
        FilterEvalResult::Uncertain,
        eval_index(
            &index,
            "0",
            bloom_fields.clone(),
            schema.clone(),
            Scalar::Number(NumberScalar::UInt8(1)),
            DataType::Number(NumberDataType::UInt8)
        )
    );
    assert_eq!(
        FilterEvalResult::Uncertain,
        eval_index(
            &index,
            "0",
            bloom_fields.clone(),
            schema.clone(),
            Scalar::Number(NumberScalar::UInt8(2)),
            DataType::Number(NumberDataType::UInt8)
        )
    );
    assert_eq!(
        FilterEvalResult::Uncertain,
        eval_index(
            &index,
            "1",
            bloom_fields.clone(),
            schema.clone(),
            Scalar::String("a".to_string()),
            DataType::String
        )
    );
    assert_eq!(
        FilterEvalResult::Uncertain,
        eval_index(
            &index,
            "1",
            bloom_fields.clone(),
            schema.clone(),
            Scalar::String("b".to_string()),
            DataType::String
        )
    );
    assert_eq!(
        FilterEvalResult::MustFalse,
        eval_index(
            &index,
            "1",
            bloom_fields,
            schema.clone(),
            Scalar::String("d".to_string()),
            DataType::String
        )
    );

    assert_eq!(
        FilterEvalResult::Uncertain,
        eval_map_index(
            &index,
            2,
            schema.clone(),
            map_ty1.clone(),
            Scalar::Number(NumberScalar::UInt8(1)),
            DataType::Number(NumberDataType::UInt8),
            Scalar::String("a".to_string()),
            DataType::String
        )
    );
    assert_eq!(
        FilterEvalResult::Uncertain,
        eval_map_index(
            &index,
            2,
            schema.clone(),
            map_ty1.clone(),
            Scalar::Number(NumberScalar::UInt8(2)),
            DataType::Number(NumberDataType::UInt8),
            Scalar::String("b".to_string()),
            DataType::String
        )
    );
    assert_eq!(
        FilterEvalResult::MustFalse,
        eval_map_index(
            &index,
            2,
            schema.clone(),
            map_ty1,
            Scalar::Number(NumberScalar::UInt8(3)),
            DataType::Number(NumberDataType::UInt8),
            Scalar::String("x".to_string()),
            DataType::String
        )
    );
    assert_eq!(
        FilterEvalResult::Uncertain,
        eval_map_index(
            &index,
            3,
            schema.clone(),
            map_ty2.clone(),
            Scalar::String("b".to_string()),
            DataType::String,
            Scalar::String("def".to_string()),
            DataType::String
        )
    );
    assert_eq!(
        FilterEvalResult::MustFalse,
        eval_map_index(
            &index,
            3,
            schema.clone(),
            map_ty2.clone(),
            Scalar::String("d".to_string()),
            DataType::String,
            Scalar::String("xxx".to_string()),
            DataType::String
        )
    );
    assert_eq!(
        FilterEvalResult::Uncertain,
        eval_map_index(
            &index,
            3,
            schema,
            map_ty2,
            Scalar::String("c".to_string()),
            DataType::String,
            Scalar::Boolean(true),
            DataType::Boolean,
        )
    );

    Ok(())
}

#[test]
fn test_specify_bloom_filter() -> Result<()> {
    let schema = Arc::new(TableSchema::new(vec![
        TableField::new("0", TableDataType::Number(NumberDataType::UInt8)),
        TableField::new("1", TableDataType::String),
    ]));

    let blocks = [DataBlock::new_from_columns(vec![
        UInt8Type::from_data(vec![1, 2]),
        StringType::from_data(vec!["a", "b"]),
    ])];

    let bloom_columns = bloom_columns_map(schema.clone(), vec![0]);
    let fields = bloom_columns.values().cloned().collect::<Vec<_>>();
    let specify_index = BloomIndex::try_create(
        FunctionContext::default(),
        LatestBloom::VERSION,
        &blocks,
        bloom_columns,
    )?
    .unwrap();

    assert_eq!(
        FilterEvalResult::Uncertain,
        eval_index(
            &specify_index,
            "1",
            fields,
            schema,
            Scalar::String("d".to_string()),
            DataType::String
        )
    );

    Ok(())
}

#[test]
fn test_string_bloom_filter() -> Result<()> {
    let schema = Arc::new(TableSchema::new(vec![
        TableField::new("0", TableDataType::Number(NumberDataType::UInt8)),
        TableField::new("1", TableDataType::String),
    ]));

    let val: String = (0..512).map(|_| 'a').collect();
    let blocks = [DataBlock::new_from_columns(vec![
        UInt8Type::from_data(vec![1, 2]),
        StringType::from_data(vec![&val, "bc"]),
    ])];

    // The average length of the string column exceeds 256 bytes.
    let bloom_columns = bloom_columns_map(schema.clone(), vec![0, 1]);
    let fields = bloom_columns.values().cloned().collect::<Vec<_>>();
    let index = BloomIndex::try_create(
        FunctionContext::default(),
        LatestBloom::VERSION,
        &blocks,
        bloom_columns,
    )?
    .unwrap();

    assert_eq!(
        FilterEvalResult::Uncertain,
        eval_index(
            &index,
            "1",
            fields,
            schema,
            Scalar::String("d".to_string()),
            DataType::String
        )
    );

    Ok(())
}

fn eval_index(
    index: &BloomIndex,
    col_name: &str,
    fields: Vec<TableField>,
    schema: Arc<TableSchema>,
    val: Scalar,
    ty: DataType,
) -> FilterEvalResult {
    let expr = check_function(
        None,
        "eq",
        &[],
        &[
            Expr::ColumnRef {
                span: None,
                id: col_name.to_string(),
                data_type: ty.clone(),
                display_name: col_name.to_string(),
            },
            Expr::Constant {
                span: None,
                scalar: val,
                data_type: ty,
            },
        ],
        &BUILTIN_FUNCTIONS,
    )
    .unwrap();

    let point_query_cols = BloomIndex::find_eq_columns(&expr, fields).unwrap();

    let mut scalar_map = HashMap::<Scalar, u64>::new();
    let func_ctx = FunctionContext::default();
    for (_, scalar, ty) in point_query_cols.iter() {
        if !scalar_map.contains_key(scalar) {
            let digest = BloomIndex::calculate_scalar_digest(&func_ctx, scalar, ty).unwrap();
            scalar_map.insert(scalar.clone(), digest);
        }
    }
    let column_stats = StatisticsOfColumns::new();
    index
        .apply(expr, &scalar_map, &column_stats, schema)
        .unwrap()
}

#[allow(clippy::too_many_arguments)]
fn eval_map_index(
    index: &BloomIndex,
    i: FieldIndex,
    schema: Arc<TableSchema>,
    map_ty: DataType,
    key: Scalar,
    key_ty: DataType,
    val: Scalar,
    ty: DataType,
) -> FilterEvalResult {
    let fields = schema.fields.clone();
    let col_name = &fields[i].name;
    let func_ctx = FunctionContext::default();
    let get_expr = check_function(
        None,
        "get",
        &[],
        &[
            Expr::ColumnRef {
                span: None,
                id: col_name.to_string(),
                data_type: map_ty,
                display_name: col_name.to_string(),
            },
            Expr::Constant {
                span: None,
                scalar: key,
                data_type: key_ty,
            },
        ],
        &BUILTIN_FUNCTIONS,
    )
    .unwrap();

    let const_expr = Expr::Constant {
        span: None,
        scalar: val,
        data_type: ty,
    };

    let eq_expr =
        check_function(None, "eq", &[], &[get_expr, const_expr], &BUILTIN_FUNCTIONS).unwrap();
    let expr = check_function(None, "is_true", &[], &[eq_expr], &BUILTIN_FUNCTIONS).unwrap();

    let (expr, _) = ConstantFolder::fold(&expr, &func_ctx, &BUILTIN_FUNCTIONS);
    let point_query_cols = BloomIndex::find_eq_columns(&expr, fields).unwrap();

    let mut scalar_map = HashMap::<Scalar, u64>::new();
    for (_, scalar, ty) in point_query_cols.iter() {
        if !scalar_map.contains_key(scalar) {
            let digest = BloomIndex::calculate_scalar_digest(&func_ctx, scalar, ty).unwrap();
            scalar_map.insert(scalar.clone(), digest);
        }
    }
    let column_stats = StatisticsOfColumns::new();
    index
        .apply(expr, &scalar_map, &column_stats, schema)
        .unwrap()
}

fn bloom_columns_map(
    schema: TableSchemaRef,
    cols: Vec<FieldIndex>,
) -> BTreeMap<FieldIndex, TableField> {
    let mut bloom_columns_map = BTreeMap::new();
    for i in cols {
        let field_type = schema.field(i).data_type();
        let data_type = DataType::from(field_type);
        if Xor8Filter::supported_type(&data_type) {
            bloom_columns_map.insert(i, schema.field(i).clone());
        }
    }
    bloom_columns_map
}
