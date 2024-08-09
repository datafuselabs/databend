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

use chrono::TimeZone;
use chrono::Utc;
use databend_common_expression as ce;
use databend_common_expression::types::NumberDataType;
use databend_common_meta_app::schema as mt;
use databend_common_meta_types::anyerror::func_name;
use maplit::btreemap;

use crate::common;

#[test]
fn test_decode_v105_dictionary_meta() -> anyhow::Result<()> {
    let bytes = vec![
        10, 5, 77, 121, 83, 81, 76, 18, 17, 10, 8, 100, 97, 116, 97, 98, 97, 115, 101, 18, 5, 109,
        121, 95, 100, 98, 18, 17, 10, 4, 104, 111, 115, 116, 18, 9, 108, 111, 99, 97, 108, 104,
        111, 115, 116, 18, 16, 10, 8, 112, 97, 115, 115, 119, 111, 114, 100, 18, 4, 49, 50, 51, 52,
        18, 12, 10, 4, 112, 111, 114, 116, 18, 4, 51, 51, 48, 54, 18, 16, 10, 8, 117, 115, 101,
        114, 110, 97, 109, 101, 18, 4, 114, 111, 111, 116, 26, 123, 10, 43, 10, 7, 117, 115, 101,
        114, 95, 105, 100, 26, 26, 178, 2, 17, 154, 2, 8, 66, 0, 160, 6, 105, 168, 6, 24, 160, 6,
        105, 168, 6, 24, 160, 6, 105, 168, 6, 24, 160, 6, 105, 168, 6, 24, 10, 30, 10, 9, 117, 115,
        101, 114, 95, 110, 97, 109, 101, 26, 9, 146, 2, 0, 160, 6, 105, 168, 6, 24, 32, 1, 160, 6,
        105, 168, 6, 24, 10, 28, 10, 7, 97, 100, 100, 114, 101, 115, 115, 26, 9, 146, 2, 0, 160, 6,
        105, 168, 6, 24, 32, 2, 160, 6, 105, 168, 6, 24, 18, 6, 10, 1, 97, 18, 1, 98, 24, 3, 160,
        6, 105, 168, 6, 24, 34, 7, 117, 115, 101, 114, 32, 105, 100, 34, 9, 117, 115, 101, 114, 32,
        110, 97, 109, 101, 34, 7, 97, 100, 100, 114, 101, 115, 115, 42, 1, 0, 50, 15, 99, 111, 109,
        109, 101, 110, 116, 95, 101, 120, 97, 109, 112, 108, 101, 58, 23, 50, 48, 50, 52, 45, 48,
        56, 45, 48, 53, 32, 48, 55, 58, 48, 48, 58, 48, 48, 32, 85, 84, 67, 160, 6, 105, 168, 6,
        24,
    ];

    let want = || mt::DictionaryMeta {
        source: "MySQL".to_string(),
        options: btreemap! {
            s("host") => s("localhost"),
            s("username") => s("root"),
            s("password") => s("1234"),
            s("port") => s("3306"),
            s("database") => s("my_db"),
        },
        schema: Arc::new(ce::TableSchema::new_from(
            vec![
                ce::TableField::new(
                    "user_id",
                    ce::TableDataType::Nullable(Box::new(ce::TableDataType::Number(
                        NumberDataType::Int64,
                    ))),
                ),
                ce::TableField::new("user_name", ce::TableDataType::String),
                ce::TableField::new("address", ce::TableDataType::String),
            ],
            btreemap! {s("a") => s("b")},
        )),
        field_comments: vec![
            "user id".to_string(),
            "user name".to_string(),
            "address".to_string(),
        ],
        primary_column_ids: vec![0],
        comment: "comment_example".to_string(),
        created_on: Utc.with_ymd_and_hms(2024, 8, 5, 7, 0, 0).unwrap(),
        updated_on: None,
    };

    common::test_pb_from_to(func_name!(), want())?;
    common::test_load_old(func_name!(), bytes.as_slice(), 105, want())?;

    Ok(())
}

fn s(ss: impl ToString) -> String {
    ss.to_string()
}
