// Copyright 2023 Datafuse Labs.
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

use chrono::TimeZone;
use chrono::Utc;
use databend_common_expression as ce;
use databend_common_expression::types::decimal::DecimalSize;
use databend_common_expression::types::DecimalDataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_meta_app::schema as mt;
use fastrace::func_name;
use maplit::btreemap;
use maplit::btreeset;

use crate::common;

// These bytes are built when a new version in introduced,
// and are kept for backward compatibility test.
//
// *************************************************************
// * These messages should never be updated,                   *
// * only be added when a new version is added,                *
// * or be removed when an old version is no longer supported. *
// *************************************************************
//
// The message bytes are built from the output of `test_pb_from_to()`
#[test]
fn test_decode_v105_schema() -> anyhow::Result<()> {
    let table_schema_v105 = vec![
        10, 28, 10, 1, 97, 26, 17, 154, 2, 8, 34, 0, 160, 6, 105, 168, 6, 24, 160, 6, 105, 168, 6,
        24, 160, 6, 105, 168, 6, 24, 10, 104, 10, 1, 98, 26, 91, 202, 2, 82, 10, 2, 98, 49, 10, 2,
        98, 50, 18, 47, 202, 2, 38, 10, 3, 98, 49, 49, 10, 3, 98, 49, 50, 18, 9, 138, 2, 0, 160, 6,
        105, 168, 6, 24, 18, 9, 146, 2, 0, 160, 6, 105, 168, 6, 24, 160, 6, 105, 168, 6, 24, 160,
        6, 105, 168, 6, 24, 18, 17, 154, 2, 8, 66, 0, 160, 6, 105, 168, 6, 24, 160, 6, 105, 168, 6,
        24, 160, 6, 105, 168, 6, 24, 160, 6, 105, 168, 6, 24, 32, 1, 160, 6, 105, 168, 6, 24, 10,
        30, 10, 1, 99, 26, 17, 154, 2, 8, 34, 0, 160, 6, 105, 168, 6, 24, 160, 6, 105, 168, 6, 24,
        32, 4, 160, 6, 105, 168, 6, 24, 10, 49, 10, 10, 100, 101, 99, 105, 109, 97, 108, 49, 50,
        56, 26, 27, 218, 2, 18, 10, 10, 8, 18, 16, 3, 160, 6, 105, 168, 6, 24, 160, 6, 105, 168, 6,
        24, 160, 6, 105, 168, 6, 24, 32, 5, 160, 6, 105, 168, 6, 24, 10, 49, 10, 10, 100, 101, 99,
        105, 109, 97, 108, 50, 53, 54, 26, 27, 218, 2, 18, 18, 10, 8, 46, 16, 6, 160, 6, 105, 168,
        6, 24, 160, 6, 105, 168, 6, 24, 160, 6, 105, 168, 6, 24, 32, 6, 160, 6, 105, 168, 6, 24,
        10, 30, 10, 9, 101, 109, 112, 116, 121, 95, 109, 97, 112, 26, 9, 226, 2, 0, 160, 6, 105,
        168, 6, 24, 32, 7, 160, 6, 105, 168, 6, 24, 10, 27, 10, 6, 98, 105, 116, 109, 97, 112, 26,
        9, 234, 2, 0, 160, 6, 105, 168, 6, 24, 32, 8, 160, 6, 105, 168, 6, 24, 10, 25, 10, 4, 103,
        101, 111, 109, 26, 9, 250, 2, 0, 160, 6, 105, 168, 6, 24, 32, 9, 160, 6, 105, 168, 6, 24,
        10, 25, 10, 4, 103, 101, 111, 103, 26, 9, 130, 3, 0, 160, 6, 105, 168, 6, 24, 32, 10, 160,
        6, 105, 168, 6, 24, 24, 11, 160, 6, 105, 168, 6, 24,
    ];

    let b1 = TableDataType::Tuple {
        fields_name: vec!["b11".to_string(), "b12".to_string()],
        fields_type: vec![TableDataType::Boolean, TableDataType::String],
    };
    let b = TableDataType::Tuple {
        fields_name: vec!["b1".to_string(), "b2".to_string()],
        fields_type: vec![b1, TableDataType::Number(NumberDataType::Int64)],
    };
    let fields = vec![
        TableField::new("a", TableDataType::Number(NumberDataType::UInt64)),
        TableField::new("b", b),
        TableField::new("c", TableDataType::Number(NumberDataType::UInt64)),
        TableField::new(
            "decimal128",
            TableDataType::Decimal(DecimalDataType::Decimal128(DecimalSize {
                precision: 18,
                scale: 3,
            })),
        ),
        TableField::new(
            "decimal256",
            TableDataType::Decimal(DecimalDataType::Decimal256(DecimalSize {
                precision: 46,
                scale: 6,
            })),
        ),
        TableField::new("empty_map", TableDataType::EmptyMap),
        TableField::new("bitmap", TableDataType::Bitmap),
        TableField::new("geom", TableDataType::Geometry),
        TableField::new("geog", TableDataType::Geography),
    ];

    let want = || TableSchema::new(fields.clone());
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), table_schema_v105.as_slice(), 105, want())?;
    Ok(())
}

#[test]
fn test_decode_v105_table_meta() -> anyhow::Result<()> {
    let table_meta_v105 = vec![
        10, 231, 6, 10, 51, 10, 8, 110, 117, 108, 108, 97, 98, 108, 101, 18, 5, 97, 32, 43, 32, 51,
        26, 26, 178, 2, 17, 154, 2, 8, 42, 0, 160, 6, 105, 168, 6, 24, 160, 6, 105, 168, 6, 24,
        160, 6, 105, 168, 6, 24, 160, 6, 105, 168, 6, 24, 10, 25, 10, 4, 98, 111, 111, 108, 26, 9,
        138, 2, 0, 160, 6, 105, 168, 6, 24, 32, 1, 160, 6, 105, 168, 6, 24, 10, 33, 10, 4, 105,
        110, 116, 56, 26, 17, 154, 2, 8, 42, 0, 160, 6, 105, 168, 6, 24, 160, 6, 105, 168, 6, 24,
        32, 2, 160, 6, 105, 168, 6, 24, 10, 34, 10, 5, 105, 110, 116, 49, 54, 26, 17, 154, 2, 8,
        50, 0, 160, 6, 105, 168, 6, 24, 160, 6, 105, 168, 6, 24, 32, 3, 160, 6, 105, 168, 6, 24,
        10, 34, 10, 5, 105, 110, 116, 51, 50, 26, 17, 154, 2, 8, 58, 0, 160, 6, 105, 168, 6, 24,
        160, 6, 105, 168, 6, 24, 32, 4, 160, 6, 105, 168, 6, 24, 10, 34, 10, 5, 105, 110, 116, 54,
        52, 26, 17, 154, 2, 8, 66, 0, 160, 6, 105, 168, 6, 24, 160, 6, 105, 168, 6, 24, 32, 5, 160,
        6, 105, 168, 6, 24, 10, 34, 10, 5, 117, 105, 110, 116, 56, 26, 17, 154, 2, 8, 10, 0, 160,
        6, 105, 168, 6, 24, 160, 6, 105, 168, 6, 24, 32, 6, 160, 6, 105, 168, 6, 24, 10, 35, 10, 6,
        117, 105, 110, 116, 49, 54, 26, 17, 154, 2, 8, 18, 0, 160, 6, 105, 168, 6, 24, 160, 6, 105,
        168, 6, 24, 32, 7, 160, 6, 105, 168, 6, 24, 10, 35, 10, 6, 117, 105, 110, 116, 51, 50, 26,
        17, 154, 2, 8, 26, 0, 160, 6, 105, 168, 6, 24, 160, 6, 105, 168, 6, 24, 32, 8, 160, 6, 105,
        168, 6, 24, 10, 35, 10, 6, 117, 105, 110, 116, 54, 52, 26, 17, 154, 2, 8, 34, 0, 160, 6,
        105, 168, 6, 24, 160, 6, 105, 168, 6, 24, 32, 9, 160, 6, 105, 168, 6, 24, 10, 36, 10, 7,
        102, 108, 111, 97, 116, 51, 50, 26, 17, 154, 2, 8, 74, 0, 160, 6, 105, 168, 6, 24, 160, 6,
        105, 168, 6, 24, 32, 10, 160, 6, 105, 168, 6, 24, 10, 36, 10, 7, 102, 108, 111, 97, 116,
        54, 52, 26, 17, 154, 2, 8, 82, 0, 160, 6, 105, 168, 6, 24, 160, 6, 105, 168, 6, 24, 32, 11,
        160, 6, 105, 168, 6, 24, 10, 25, 10, 4, 100, 97, 116, 101, 26, 9, 170, 2, 0, 160, 6, 105,
        168, 6, 24, 32, 12, 160, 6, 105, 168, 6, 24, 10, 30, 10, 9, 116, 105, 109, 101, 115, 116,
        97, 109, 112, 26, 9, 162, 2, 0, 160, 6, 105, 168, 6, 24, 32, 13, 160, 6, 105, 168, 6, 24,
        10, 27, 10, 6, 115, 116, 114, 105, 110, 103, 26, 9, 146, 2, 0, 160, 6, 105, 168, 6, 24, 32,
        14, 160, 6, 105, 168, 6, 24, 10, 65, 10, 6, 115, 116, 114, 117, 99, 116, 26, 47, 202, 2,
        38, 10, 3, 102, 111, 111, 10, 3, 98, 97, 114, 18, 9, 138, 2, 0, 160, 6, 105, 168, 6, 24,
        18, 9, 146, 2, 0, 160, 6, 105, 168, 6, 24, 160, 6, 105, 168, 6, 24, 160, 6, 105, 168, 6,
        24, 32, 15, 160, 6, 105, 168, 6, 24, 10, 35, 10, 5, 97, 114, 114, 97, 121, 26, 18, 186, 2,
        9, 138, 2, 0, 160, 6, 105, 168, 6, 24, 160, 6, 105, 168, 6, 24, 32, 17, 160, 6, 105, 168,
        6, 24, 10, 28, 10, 7, 118, 97, 114, 105, 97, 110, 116, 26, 9, 210, 2, 0, 160, 6, 105, 168,
        6, 24, 32, 18, 160, 6, 105, 168, 6, 24, 10, 34, 10, 13, 118, 97, 114, 105, 97, 110, 116,
        95, 97, 114, 114, 97, 121, 26, 9, 210, 2, 0, 160, 6, 105, 168, 6, 24, 32, 19, 160, 6, 105,
        168, 6, 24, 10, 35, 10, 14, 118, 97, 114, 105, 97, 110, 116, 95, 111, 98, 106, 101, 99,
        116, 26, 9, 210, 2, 0, 160, 6, 105, 168, 6, 24, 32, 20, 160, 6, 105, 168, 6, 24, 10, 29,
        10, 8, 105, 110, 116, 101, 114, 118, 97, 108, 26, 9, 250, 1, 0, 160, 6, 105, 168, 6, 24,
        32, 21, 160, 6, 105, 168, 6, 24, 10, 27, 10, 6, 98, 105, 116, 109, 97, 112, 26, 9, 234, 2,
        0, 160, 6, 105, 168, 6, 24, 32, 22, 160, 6, 105, 168, 6, 24, 10, 25, 10, 4, 103, 101, 111,
        109, 26, 9, 250, 2, 0, 160, 6, 105, 168, 6, 24, 32, 23, 160, 6, 105, 168, 6, 24, 10, 25,
        10, 4, 103, 101, 111, 103, 26, 9, 130, 3, 0, 160, 6, 105, 168, 6, 24, 32, 24, 160, 6, 105,
        168, 6, 24, 18, 6, 10, 1, 97, 18, 1, 98, 24, 25, 160, 6, 105, 168, 6, 24, 34, 10, 40, 97,
        32, 43, 32, 50, 44, 32, 98, 41, 42, 10, 10, 3, 120, 121, 122, 18, 3, 102, 111, 111, 50, 2,
        52, 52, 58, 10, 10, 3, 97, 98, 99, 18, 3, 100, 101, 102, 64, 0, 74, 10, 40, 97, 32, 43, 32,
        50, 44, 32, 98, 41, 162, 1, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50, 56, 32, 49, 50, 58, 48,
        48, 58, 48, 57, 32, 85, 84, 67, 170, 1, 23, 50, 48, 49, 52, 45, 49, 49, 45, 50, 57, 32, 49,
        50, 58, 48, 48, 58, 49, 48, 32, 85, 84, 67, 178, 1, 13, 116, 97, 98, 108, 101, 95, 99, 111,
        109, 109, 101, 110, 116, 186, 1, 6, 160, 6, 105, 168, 6, 24, 202, 1, 1, 99, 202, 1, 1, 99,
        202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99,
        202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99,
        202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99, 202, 1, 1, 99,
        202, 1, 1, 99, 226, 1, 1, 1, 234, 1, 6, 10, 1, 97, 18, 1, 98, 160, 6, 105, 168, 6, 24,
    ];

    let want = || mt::TableMeta {
        schema: Arc::new(ce::TableSchema::new_from(
            vec![
                ce::TableField::new(
                    "nullable",
                    ce::TableDataType::Nullable(Box::new(ce::TableDataType::Number(
                        NumberDataType::Int8,
                    ))),
                )
                .with_default_expr(Some("a + 3".to_string())),
                ce::TableField::new("bool", ce::TableDataType::Boolean),
                ce::TableField::new("int8", ce::TableDataType::Number(NumberDataType::Int8)),
                ce::TableField::new("int16", ce::TableDataType::Number(NumberDataType::Int16)),
                ce::TableField::new("int32", ce::TableDataType::Number(NumberDataType::Int32)),
                ce::TableField::new("int64", ce::TableDataType::Number(NumberDataType::Int64)),
                ce::TableField::new("uint8", ce::TableDataType::Number(NumberDataType::UInt8)),
                ce::TableField::new("uint16", ce::TableDataType::Number(NumberDataType::UInt16)),
                ce::TableField::new("uint32", ce::TableDataType::Number(NumberDataType::UInt32)),
                ce::TableField::new("uint64", ce::TableDataType::Number(NumberDataType::UInt64)),
                ce::TableField::new(
                    "float32",
                    ce::TableDataType::Number(NumberDataType::Float32),
                ),
                ce::TableField::new(
                    "float64",
                    ce::TableDataType::Number(NumberDataType::Float64),
                ),
                ce::TableField::new("date", ce::TableDataType::Date),
                ce::TableField::new("timestamp", ce::TableDataType::Timestamp),
                ce::TableField::new("string", ce::TableDataType::String),
                ce::TableField::new("struct", ce::TableDataType::Tuple {
                    fields_name: vec![s("foo"), s("bar")],
                    fields_type: vec![ce::TableDataType::Boolean, ce::TableDataType::String],
                }),
                ce::TableField::new(
                    "array",
                    ce::TableDataType::Array(Box::new(ce::TableDataType::Boolean)),
                ),
                ce::TableField::new("variant", ce::TableDataType::Variant),
                ce::TableField::new("variant_array", ce::TableDataType::Variant),
                ce::TableField::new("variant_object", ce::TableDataType::Variant),
                // NOTE: It is safe to convert Interval to NULL, because `Interval` is never really used.
                ce::TableField::new("interval", ce::TableDataType::Null),
                ce::TableField::new("bitmap", ce::TableDataType::Bitmap),
                ce::TableField::new("geom", ce::TableDataType::Geometry),
                ce::TableField::new("geog", ce::TableDataType::Geography),
            ],
            btreemap! {s("a") => s("b")},
        )),
        engine: "44".to_string(),
        storage_params: None,
        part_prefix: "".to_string(),
        engine_options: btreemap! {s("abc") => s("def")},
        options: btreemap! {s("xyz") => s("foo")},
        default_cluster_key: Some("(a + 2, b)".to_string()),
        cluster_keys: vec!["(a + 2, b)".to_string()],
        default_cluster_key_id: Some(0),
        created_on: Utc.with_ymd_and_hms(2014, 11, 28, 12, 0, 9).unwrap(),
        updated_on: Utc.with_ymd_and_hms(2014, 11, 29, 12, 0, 10).unwrap(),
        comment: s("table_comment"),
        field_comments: vec!["c".to_string(); 21],
        drop_on: None,
        statistics: Default::default(),
        shared_by: btreeset! {1},
        column_mask_policy: Some(btreemap! {s("a") => s("b")}),
        indexes: btreemap! {},
    };
    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), table_meta_v105.as_slice(), 105, want())?;
    Ok(())
}

fn s(ss: impl ToString) -> String {
    ss.to_string()
}
