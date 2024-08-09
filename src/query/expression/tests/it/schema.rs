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

use std::collections::BTreeMap;

use databend_common_arrow::arrow::datatypes::DataType as ArrowDataType;
use databend_common_arrow::arrow::datatypes::Field as ArrowField;
use databend_common_exception::Result;
use databend_common_expression::create_test_complex_schema;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::ColumnId;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use pretty_assertions::assert_eq;

#[test]
fn test_from_arrow_field_to_table_field() -> Result<()> {
    let extension_data_type =
        ArrowDataType::Extension("a".to_string(), Box::new(ArrowDataType::Int8), None);
    let arrow_field = ArrowField::new("".to_string(), extension_data_type, false);
    let _: TableField = (&arrow_field).try_into().unwrap();
    Ok(())
}

#[test]
fn test_project_schema_from_tuple() -> Result<()> {
    let b1 = TableDataType::Tuple {
        fields_name: vec!["b11".to_string(), "b12".to_string()],
        fields_type: vec![TableDataType::Boolean, TableDataType::String],
    };
    let b = TableDataType::Tuple {
        fields_name: vec!["b1".to_string(), "b2".to_string()],
        fields_type: vec![b1.clone(), TableDataType::Number(NumberDataType::Int64)],
    };
    let d = TableDataType::Tuple {
        fields_name: vec!["d1".to_string(), "d2".to_string()],
        fields_type: vec![
            TableDataType::Number(NumberDataType::Int64),
            TableDataType::Number(NumberDataType::Int64),
        ],
    };
    let fields = vec![
        TableField::new("a", TableDataType::Number(NumberDataType::UInt64)),
        TableField::new("b", b.clone()),
        TableField::new("c", TableDataType::Number(NumberDataType::UInt64)),
    ];
    let mut schema = TableSchema::new(fields);

    // project schema
    {
        let expect_fields = vec![
            TableField::new_from_column_id("a", TableDataType::Number(NumberDataType::UInt64), 0),
            TableField::new_from_column_id("b:b1:b11", TableDataType::Boolean, 1),
            TableField::new_from_column_id("b:b1:b12", TableDataType::String, 2),
            TableField::new_from_column_id("b:b2", TableDataType::Number(NumberDataType::Int64), 3),
            TableField::new_from_column_id("b:b1", b1.clone(), 1),
            TableField::new_from_column_id("b", b.clone(), 1),
        ];

        let mut path_indices = BTreeMap::new();
        path_indices.insert(0, vec![0]); // a
        path_indices.insert(1, vec![1, 0, 0]); // b:b1:b11
        path_indices.insert(2, vec![1, 0, 1]); // b:b1:b12
        path_indices.insert(3, vec![1, 1]); // b:b2
        path_indices.insert(4, vec![1, 0]); // b:b1
        path_indices.insert(5, vec![1]); // b
        let project_schema = schema.inner_project(&path_indices);

        for (i, field) in project_schema.fields().iter().enumerate() {
            assert_eq!(*field, expect_fields[i]);
        }
        assert_eq!(project_schema.next_column_id(), schema.next_column_id());

        // check leaf fields
        {
            let expected_column_id_field = vec![
                (0, "a"),
                (1, "b:b1:b11"),
                (2, "b:b1:b12"),
                (3, "b:b2"),
                (1, "b:b1:b11"),
                (2, "b:b1:b12"),
                (1, "b:b1:b11"),
                (2, "b:b1:b12"),
                (3, "b:b2"),
            ];
            let leaf_fields = project_schema.leaf_fields();
            for (i, leaf_field) in leaf_fields.iter().enumerate() {
                assert_eq!(expected_column_id_field[i].0, leaf_field.column_id());
                assert_eq!(expected_column_id_field[i].1, leaf_field.name());
            }

            // verify leaf column ids of projected schema are as expected
            assert_eq!(
                leaf_fields
                    .into_iter()
                    .flat_map(|f| f.leaf_column_ids())
                    .collect::<Vec<_>>(),
                project_schema.to_leaf_column_ids()
            );
        }
    };

    // drop column
    {
        schema.drop_column("b")?;
        let mut path_indices = BTreeMap::new();
        path_indices.insert(0, vec![0]);
        path_indices.insert(1, vec![1]);
        let project_schema = schema.inner_project(&path_indices);

        let expect_fields = vec![
            TableField::new_from_column_id("a", TableDataType::Number(NumberDataType::UInt64), 0),
            TableField::new_from_column_id("c", TableDataType::Number(NumberDataType::UInt64), 4),
        ];

        for (i, field) in project_schema.fields().iter().enumerate() {
            assert_eq!(*field, expect_fields[i]);
        }
        assert_eq!(project_schema.next_column_id(), schema.next_column_id());
    }

    // add column
    {
        schema.add_columns(&[TableField::new("b", b.clone())])?;

        let mut path_indices = BTreeMap::new();
        path_indices.insert(0, vec![0]);
        path_indices.insert(1, vec![1]);
        path_indices.insert(2, vec![2, 0, 0]);
        path_indices.insert(3, vec![2, 0, 1]);
        path_indices.insert(4, vec![2, 1]);
        path_indices.insert(5, vec![2, 0]);
        path_indices.insert(6, vec![2]);

        let expect_fields = vec![
            TableField::new_from_column_id("a", TableDataType::Number(NumberDataType::UInt64), 0),
            TableField::new_from_column_id("c", TableDataType::Number(NumberDataType::UInt64), 4),
            TableField::new_from_column_id("b:b1:b11", TableDataType::Boolean, 5),
            TableField::new_from_column_id("b:b1:b12", TableDataType::String, 6),
            TableField::new_from_column_id("b:b2", TableDataType::Number(NumberDataType::Int64), 7),
            TableField::new_from_column_id("b:b1", b1.clone(), 5),
            TableField::new_from_column_id("b", b.clone(), 5),
        ];
        let project_schema = schema.inner_project(&path_indices);

        for (i, field) in project_schema.fields().iter().enumerate() {
            assert_eq!(*field, expect_fields[i]);
        }
        assert_eq!(project_schema.next_column_id(), schema.next_column_id());
    }

    // add column
    {
        schema.add_columns(&[TableField::new("d", d.clone())])?;

        let mut path_indices = BTreeMap::new();
        path_indices.insert(0, vec![0]);
        path_indices.insert(1, vec![1]);
        path_indices.insert(2, vec![2, 0, 0]);
        path_indices.insert(3, vec![2, 0, 1]);
        path_indices.insert(4, vec![2, 1]);
        path_indices.insert(5, vec![2, 0]);
        path_indices.insert(6, vec![2]);
        path_indices.insert(7, vec![3, 0]);
        path_indices.insert(8, vec![3, 1]);
        path_indices.insert(9, vec![3]);

        let expect_fields = vec![
            TableField::new_from_column_id("a", TableDataType::Number(NumberDataType::UInt64), 0),
            TableField::new_from_column_id("c", TableDataType::Number(NumberDataType::UInt64), 4),
            TableField::new_from_column_id("b:b1:b11", TableDataType::Boolean, 5),
            TableField::new_from_column_id("b:b1:b12", TableDataType::String, 6),
            TableField::new_from_column_id("b:b2", TableDataType::Number(NumberDataType::Int64), 7),
            TableField::new_from_column_id("b:b1", b1, 5),
            TableField::new_from_column_id("b", b, 5),
            TableField::new_from_column_id("d:d1", TableDataType::Number(NumberDataType::Int64), 8),
            TableField::new_from_column_id("d:d2", TableDataType::Number(NumberDataType::Int64), 9),
            TableField::new_from_column_id("d", d, 8),
        ];
        let project_schema = schema.inner_project(&path_indices);

        for (i, field) in project_schema.fields().iter().enumerate() {
            assert_eq!(*field, expect_fields[i]);
        }
        assert_eq!(project_schema.next_column_id(), schema.next_column_id());
    }

    Ok(())
}

#[test]
fn test_schema_from_simple_type() -> Result<()> {
    let field1 = TableField::new("a", TableDataType::Number(NumberDataType::UInt64));
    let field2 = TableField::new("b", TableDataType::Number(NumberDataType::UInt64));
    let field3 = TableField::new(
        "c",
        TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
    );

    let schema = TableSchema::new(vec![field1, field2, field3]);
    assert_eq!(schema.to_column_ids(), vec![0, 1, 2]);
    assert_eq!(schema.to_leaf_column_ids(), vec![0, 1, 2]);
    assert_eq!(schema.next_column_id(), 3);

    let leaf_fields = schema.leaf_fields();
    let leaf_field_names = ["a", "b", "c"];
    let leaf_column_ids = [0, 1, 2];
    for (i, field) in leaf_fields.iter().enumerate() {
        assert_eq!(field.name(), leaf_field_names[i]);
        assert_eq!(field.column_id(), leaf_column_ids[i]);
    }

    // verify leaf column ids are as expected
    assert_eq!(
        leaf_fields
            .iter()
            .flat_map(|f| f.leaf_column_ids())
            .collect::<Vec<_>>(),
        schema.to_leaf_column_ids()
    );

    Ok(())
}

#[test]
fn test_field_leaf_default_values() -> Result<()> {
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
    ];
    let schema = TableSchema::new(fields);

    let default_values = vec![
        Scalar::Number(databend_common_expression::types::number::NumberScalar::UInt64(1)),
        Scalar::Tuple(vec![
            Scalar::Tuple(vec![
                Scalar::Boolean(true),
                Scalar::String("ab".to_string()),
            ]),
            Scalar::Number(databend_common_expression::types::number::NumberScalar::Int64(2)),
        ]),
        Scalar::Number(databend_common_expression::types::number::NumberScalar::UInt64(10)),
    ];

    let leaf_default_values = schema.field_leaf_default_values(&default_values);
    let expected_leaf_default_values: Vec<(ColumnId, Scalar)> = vec![
        (
            0,
            Scalar::Number(databend_common_expression::types::number::NumberScalar::UInt64(1)),
        ),
        (1, Scalar::Boolean(true)),
        (2, Scalar::String("ab".to_string())),
        (
            3,
            Scalar::Number(databend_common_expression::types::number::NumberScalar::Int64(2)),
        ),
        (
            4,
            Scalar::Number(databend_common_expression::types::number::NumberScalar::UInt64(10)),
        ),
    ];
    expected_leaf_default_values
        .iter()
        .for_each(|(col_id, default_value)| {
            assert_eq!(leaf_default_values.get(col_id).unwrap(), default_value)
        });
    Ok(())
}

#[test]
fn test_schema_from_struct() -> Result<()> {
    let schema = create_test_complex_schema();
    let flat_column_ids = schema.to_leaf_column_ids();

    let leaf_fields = schema.leaf_fields();

    // verify leaf column ids are as expected
    assert_eq!(
        leaf_fields
            .iter()
            .flat_map(|f| f.leaf_column_ids())
            .collect::<Vec<_>>(),
        schema.to_leaf_column_ids()
    );

    let expected_fields = vec![
        ("u64", TableDataType::Number(NumberDataType::UInt64)),
        (
            "tuplearray:\"0\":\"0\"",
            TableDataType::Number(NumberDataType::UInt64),
        ),
        (
            "tuplearray:\"0\":\"1\"",
            TableDataType::Number(NumberDataType::UInt64),
        ),
        (
            "tuplearray:\"1\":0",
            TableDataType::Number(NumberDataType::UInt64),
        ),
        (
            "arraytuple:0:\"0\"",
            TableDataType::Number(NumberDataType::UInt64),
        ),
        (
            "arraytuple:0:\"1\"",
            TableDataType::Number(NumberDataType::UInt64),
        ),
        ("nullarray:0", TableDataType::Number(NumberDataType::UInt64)),
        (
            "maparray:key",
            TableDataType::Number(NumberDataType::UInt64),
        ),
        ("maparray:value", TableDataType::String),
        (
            "nullu64",
            TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
        ),
        ("u64array:0", TableDataType::Number(NumberDataType::UInt64)),
        (
            "tuplesimple:a",
            TableDataType::Number(NumberDataType::Int32),
        ),
        (
            "tuplesimple:b",
            TableDataType::Number(NumberDataType::Int32),
        ),
    ];

    for (i, field) in leaf_fields.iter().enumerate() {
        let expected_field = &expected_fields[i];
        assert_eq!(field.name(), expected_field.0);
        assert_eq!(field.data_type().to_owned(), expected_field.1);
        assert_eq!(field.column_id(), i as u32);
    }

    let expected_column_ids = vec![
        ("u64", vec![0]),
        ("tuplearray", vec![1, 1, 1, 2, 3, 3]),
        ("arraytuple", vec![4, 4, 4, 5]),
        ("nullarray", vec![6, 6]),
        ("maparray", vec![7, 7, 7, 8]),
        ("nullu64", vec![9]),
        ("u64array", vec![10, 10]),
        ("tuplesimple", vec![11, 11, 12]),
    ];

    for (i, column_id) in schema.field_column_ids().iter().enumerate() {
        let (field_name, ids) = &expected_column_ids[i];
        assert_eq!(
            field_name.to_string(),
            schema.fields()[i].name().to_string()
        );
        assert_eq!(ids, column_id);
    }

    let expected_flat_column_ids = vec![
        ("u64", vec![0]),
        ("tuplearray", vec![1, 2, 3]),
        ("arraytuple", vec![4, 5]),
        ("nullarray", vec![6]),
        ("maparray", vec![7, 8]),
        ("nullu64", vec![9]),
        ("u64array", vec![10]),
        ("tuplesimple", vec![11, 12]),
    ];

    for (i, field) in schema.fields().iter().enumerate() {
        let expected_column_id = &expected_flat_column_ids[i];
        assert_eq!(expected_column_id.0.to_string(), field.name().to_string());
        assert_eq!(expected_column_id.1, field.leaf_column_ids());
    }

    assert_eq!(schema.next_column_id(), 13);

    // make sure column ids is adjacent integers(in case there is no add or drop column operations)
    assert_eq!(flat_column_ids.len(), schema.next_column_id() as usize);
    for i in 1..flat_column_ids.len() {
        assert_eq!(flat_column_ids[i], flat_column_ids[i - 1] + 1);
    }

    // check leaf fields
    {
        let expected_column_id_field = vec![
            (0, "u64"),
            (1, "tuplearray:\"0\":\"0\""),
            (2, "tuplearray:\"0\":\"1\""),
            (3, "tuplearray:\"1\":0"),
            (4, "arraytuple:0:\"0\""),
            (5, "arraytuple:0:\"1\""),
            (6, "nullarray:0"),
            (7, "maparray:key"),
            (8, "maparray:value"),
            (9, "nullu64"),
            (10, "u64array:0"),
            (11, "tuplesimple:a"),
            (12, "tuplesimple:b"),
        ];
        let leaf_fields = schema.leaf_fields();
        for (i, leaf_field) in leaf_fields.iter().enumerate() {
            assert_eq!(expected_column_id_field[i].0, leaf_field.column_id());
            assert_eq!(expected_column_id_field[i].1, leaf_field.name());
        }
    }

    Ok(())
}

#[test]
fn test_schema_modify_field() -> Result<()> {
    let field1 = TableField::new("a", TableDataType::Number(NumberDataType::UInt64));
    let field2 = TableField::new("b", TableDataType::Number(NumberDataType::UInt64));
    let field3 = TableField::new("c", TableDataType::Number(NumberDataType::UInt64));

    let mut schema = TableSchema::new(vec![field1.clone()]);

    let expected_field1 =
        TableField::new_from_column_id("a", TableDataType::Number(NumberDataType::UInt64), 0);
    let expected_field2 =
        TableField::new_from_column_id("b", TableDataType::Number(NumberDataType::UInt64), 1);
    let expected_field3 =
        TableField::new_from_column_id("c", TableDataType::Number(NumberDataType::UInt64), 2);

    assert_eq!(schema.fields().to_owned(), vec![expected_field1.clone()]);
    assert_eq!(schema.column_id_of("a").unwrap(), 0);
    assert_eq!(schema.is_column_deleted(0), false);
    assert_eq!(schema.to_column_ids(), vec![0]);
    assert_eq!(schema.to_leaf_column_ids(), vec![0]);
    assert_eq!(schema.next_column_id(), 1);

    // add column b
    schema.add_columns(&[field2.clone()])?;
    assert_eq!(schema.fields().to_owned(), vec![
        expected_field1.clone(),
        expected_field2,
    ]);
    assert_eq!(schema.column_id_of("a").unwrap(), 0);
    assert_eq!(schema.column_id_of("b").unwrap(), 1);
    assert_eq!(schema.is_column_deleted(0), false);
    assert_eq!(schema.is_column_deleted(1), false);
    assert_eq!(schema.to_column_ids(), vec![0, 1]);
    assert_eq!(schema.to_leaf_column_ids(), vec![0, 1]);
    assert_eq!(schema.next_column_id(), 2);

    // drop column b
    schema.drop_column("b")?;
    assert_eq!(schema.fields().to_owned(), vec![expected_field1.clone(),]);
    assert_eq!(schema.column_id_of("a").unwrap(), 0);
    assert_eq!(schema.is_column_deleted(0), false);
    assert_eq!(schema.is_column_deleted(1), true);
    assert_eq!(schema.to_column_ids(), vec![0]);
    assert_eq!(schema.to_leaf_column_ids(), vec![0]);
    assert_eq!(schema.next_column_id(), 2);

    // add column c
    schema.add_columns(&[field3.clone()])?;
    assert_eq!(schema.fields().to_owned(), vec![
        expected_field1,
        expected_field3
    ]);
    assert_eq!(schema.column_id_of("a").unwrap(), 0);
    assert_eq!(schema.column_id_of("c").unwrap(), 2);
    assert_eq!(schema.is_column_deleted(0), false);
    assert_eq!(schema.is_column_deleted(1), true);
    assert_eq!(schema.is_column_deleted(2), false);
    assert_eq!(schema.to_column_ids(), vec![0, 2]);
    assert_eq!(schema.to_leaf_column_ids(), vec![0, 2]);
    assert_eq!(schema.next_column_id(), 3);

    // add struct column
    let child_field11 = TableDataType::Number(NumberDataType::UInt64);
    let child_field12 = TableDataType::Number(NumberDataType::UInt64);
    let child_field22 = TableDataType::Number(NumberDataType::UInt64);
    let s = TableDataType::Tuple {
        fields_name: vec!["0".to_string(), "1".to_string()],
        fields_type: vec![child_field11.clone(), child_field12.clone()],
    };
    let s2 = TableDataType::Tuple {
        fields_name: vec!["0".to_string(), "1".to_string()],
        fields_type: vec![s.clone(), child_field22.clone()],
    };
    schema.add_columns(&[TableField::new("s", s2.clone())])?;
    assert_eq!(schema.column_id_of("s").unwrap(), 3);
    assert_eq!(schema.is_column_deleted(0), false);
    assert_eq!(schema.is_column_deleted(1), true);
    assert_eq!(schema.is_column_deleted(2), false);
    assert_eq!(schema.is_column_deleted(3), false);
    assert_eq!(schema.to_column_ids(), vec![0, 2, 3, 3, 3, 4, 5]);
    assert_eq!(schema.to_leaf_column_ids(), vec![0, 2, 3, 4, 5]);
    assert_eq!(schema.next_column_id(), 6);

    // add array column
    let ary = TableDataType::Array(Box::new(TableDataType::Array(Box::new(
        TableDataType::Number(NumberDataType::UInt64),
    ))));
    schema.add_columns(&[TableField::new("ary", ary.clone())])?;
    assert_eq!(schema.column_id_of("ary").unwrap(), 6);
    assert_eq!(schema.is_column_deleted(0), false);
    assert_eq!(schema.is_column_deleted(1), true);
    assert_eq!(schema.is_column_deleted(2), false);
    assert_eq!(schema.is_column_deleted(3), false);
    assert_eq!(schema.is_column_deleted(6), false);
    assert_eq!(schema.to_column_ids(), vec![0, 2, 3, 3, 3, 4, 5, 6, 6, 6]);
    assert_eq!(schema.to_leaf_column_ids(), vec![0, 2, 3, 4, 5, 6]);
    assert_eq!(schema.next_column_id(), 7);

    // check leaf fields
    {
        let expected_column_id_field = [
            (0, "a"),
            (2, "c"),
            (3, "s:\"0\":\"0\""),
            (4, "s:\"0\":\"1\""),
            (5, "s:\"1\""),
            (6, "ary:0:0"),
        ];
        let leaf_fields = schema.leaf_fields();
        for (i, leaf_field) in leaf_fields.iter().enumerate() {
            assert_eq!(expected_column_id_field[i].0, leaf_field.column_id());
            assert_eq!(expected_column_id_field[i].1, leaf_field.name());
        }
    }

    // check project fields
    {
        let mut project_fields = BTreeMap::new();
        project_fields.insert(0, field1);
        project_fields.insert(2, TableField::new("s", s2));
        project_fields.insert(3, TableField::new("0", s));
        project_fields.insert(4, TableField::new("0", child_field11));
        project_fields.insert(5, TableField::new("1", child_field12));
        project_fields.insert(6, TableField::new("1", child_field22));
        project_fields.insert(7, TableField::new("ary", ary));
        project_fields.insert(
            8,
            TableField::new(
                "ary:0",
                TableDataType::Array(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
        );
        project_fields.insert(
            9,
            TableField::new("0", TableDataType::Number(NumberDataType::UInt64)),
        );
        let project_schema = schema.project_by_fields(&project_fields);
        let expected_column_ids = vec![
            (0, vec![0]),
            (2, vec![3, 3, 3, 4, 5]),
            (3, vec![3, 3, 4]),
            (4, vec![3]),
            (5, vec![4]),
            (6, vec![5]),
            (7, vec![6, 6, 6]),
            (8, vec![6, 6]),
            (9, vec![6]),
        ];
        for (project_schema_index, (_i, column_ids)) in expected_column_ids.into_iter().enumerate()
        {
            let field = &project_schema.fields()[project_schema_index];
            assert_eq!(field.column_ids(), column_ids);
        }
    }

    // drop tuple column
    schema.drop_column("s")?;
    assert_eq!(schema.is_column_deleted(0), false);
    assert_eq!(schema.is_column_deleted(1), true);
    assert_eq!(schema.is_column_deleted(2), false);
    assert_eq!(schema.is_column_deleted(3), true);
    assert_eq!(schema.is_column_deleted(6), false);
    assert_eq!(schema.to_column_ids(), vec![0, 2, 6, 6, 6]);
    assert_eq!(schema.to_leaf_column_ids(), vec![0, 2, 6]);
    assert!(schema.column_id_of("s").is_err());

    // add column with index
    schema.add_column(&field2, 1)?;
    assert_eq!(schema.column_id_of("b").unwrap(), 7);
    let field_index = schema.index_of("b")?;
    assert_eq!(field_index, 1);

    let err = schema.add_column(&field3, 1).unwrap_err();
    assert_eq!(err.message(), "add column c already exist");

    Ok(())
}

#[test]
fn test_leaf_columns_of() -> Result<()> {
    let fields = vec![
        TableField::new("a", TableDataType::Number(NumberDataType::UInt64)),
        TableField::new("b", TableDataType::Tuple {
            fields_name: vec!["b1".to_string(), "b2".to_string()],
            fields_type: vec![
                TableDataType::Tuple {
                    fields_name: vec!["b11".to_string(), "b12".to_string()],
                    fields_type: vec![TableDataType::Boolean, TableDataType::String],
                },
                TableDataType::Number(NumberDataType::UInt64),
            ],
        }),
        TableField::new(
            "c",
            TableDataType::Array(Box::new(TableDataType::Number(NumberDataType::UInt64))),
        ),
        TableField::new(
            "d",
            TableDataType::Map(Box::new(TableDataType::Tuple {
                fields_name: vec!["key".to_string(), "value".to_string()],
                fields_type: vec![TableDataType::String, TableDataType::String],
            })),
        ),
        TableField::new("e", TableDataType::String),
    ];
    let schema = TableSchema::new(fields);

    assert_eq!(schema.leaf_columns_of(&"a".to_string()), vec![0]);
    assert_eq!(schema.leaf_columns_of(&"b".to_string()), vec![1, 2, 3]);
    assert_eq!(schema.leaf_columns_of(&"b:b1".to_string()), vec![1, 2]);
    assert_eq!(schema.leaf_columns_of(&"b:1".to_string()), vec![1, 2]);
    assert_eq!(schema.leaf_columns_of(&"b:b1:b11".to_string()), vec![1]);
    assert_eq!(schema.leaf_columns_of(&"b:1:1".to_string()), vec![1]);
    assert_eq!(schema.leaf_columns_of(&"b:b1:b12".to_string()), vec![2]);
    assert_eq!(schema.leaf_columns_of(&"b:1:2".to_string()), vec![2]);
    assert_eq!(schema.leaf_columns_of(&"b:b2".to_string()), vec![3]);
    assert_eq!(schema.leaf_columns_of(&"b:2".to_string()), vec![3]);
    assert_eq!(schema.leaf_columns_of(&"c".to_string()), vec![4]);
    assert_eq!(schema.leaf_columns_of(&"d".to_string()), vec![5, 6]);
    assert_eq!(schema.leaf_columns_of(&"e".to_string()), vec![7]);
    Ok(())
}

#[test]
fn test_geography_as_arrow() {
    use databend_common_expression::types::geography::GeographyColumnBuilder;
    use databend_common_expression::types::GeographyType;
    use databend_common_expression::Column;

    let mut builder = GeographyColumnBuilder::with_capacity(3, 0);
    builder.push(GeographyType::point(1.0, 2.0).as_ref());
    builder.push(GeographyType::point(2.0, 3.0).as_ref());
    builder.push(GeographyType::point(4.0, 5.0).as_ref());
    let col = Column::Geography(builder.build());

    let got = col.as_arrow();
    assert_eq!(
        "StructArray[{buf: [1], points: [{x: 1, y: 2}]}, {buf: [1], points: [{x: 2, y: 3}]}, {buf: [1], points: [{x: 4, y: 5}]}]",
        format!("{:?}", got)
    );
}
