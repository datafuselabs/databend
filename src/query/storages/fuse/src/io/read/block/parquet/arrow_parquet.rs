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

//! This module makes a read fix to read old parquet incompat formats which take decimal256 as 32 fixed bytes

use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::TimeUnit;
use parquet::basic::ConvertedType;
use parquet::basic::LogicalType;
use parquet::basic::Repetition;
use parquet::basic::TimeUnit as ParquetTimeUnit;
use parquet::basic::Type as PhysicalType;
use parquet::errors::ParquetError;
use parquet::errors::Result;
use parquet::schema::types::SchemaDescriptor;
use parquet::schema::types::Type;

pub fn arrow_to_parquet_schema_fix(schema: &Schema) -> Result<SchemaDescriptor> {
    arrow_to_parquet_schema_with_root_fix(schema, "arrow_schema")
}

/// Convert arrow schema to parquet schema specifying the name of the root schema element
pub fn arrow_to_parquet_schema_with_root_fix(
    schema: &Schema,
    root: &str,
) -> Result<SchemaDescriptor> {
    let fields = schema
        .fields()
        .iter()
        .map(|field| arrow_to_parquet_type_fix(field).map(Arc::new))
        .collect::<Result<_>>()?;
    let group = Type::group_type_builder(root).with_fields(fields).build()?;
    Ok(SchemaDescriptor::new(Arc::new(group)))
}

fn decimal_length_from_precision(precision: u8) -> usize {
    // digits = floor(log_10(2^(8*n - 1) - 1))
    // ceil(digits) = log10(2^(8*n - 1) - 1)
    // 10^ceil(digits) = 2^(8*n - 1) - 1
    // 10^ceil(digits) + 1 = 2^(8*n - 1)
    // log2(10^ceil(digits) + 1) = (8*n - 1)
    // log2(10^ceil(digits) + 1) + 1 = 8*n
    // (log2(10^ceil(a) + 1) + 1) / 8 = n
    (((10.0_f64.powi(precision as i32) + 1.0).log2() + 1.0) / 8.0).ceil() as usize
}

/// Convert an arrow field to a parquet `Type`
pub(crate) fn arrow_to_parquet_type_fix(field: &Field) -> Result<Type> {
    let name = field.name().as_str();
    let repetition = if field.is_nullable() {
        Repetition::OPTIONAL
    } else {
        Repetition::REQUIRED
    };
    let id = field_id(field);
    // create type from field
    match field.data_type() {
        DataType::Null => Type::primitive_type_builder(name, PhysicalType::INT32)
            .with_logical_type(Some(LogicalType::Unknown))
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::Boolean => Type::primitive_type_builder(name, PhysicalType::BOOLEAN)
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::Int8 => Type::primitive_type_builder(name, PhysicalType::INT32)
            .with_logical_type(Some(LogicalType::Integer {
                bit_width: 8,
                is_signed: true,
            }))
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::Int16 => Type::primitive_type_builder(name, PhysicalType::INT32)
            .with_logical_type(Some(LogicalType::Integer {
                bit_width: 16,
                is_signed: true,
            }))
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::Int32 => Type::primitive_type_builder(name, PhysicalType::INT32)
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::Int64 => Type::primitive_type_builder(name, PhysicalType::INT64)
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::UInt8 => Type::primitive_type_builder(name, PhysicalType::INT32)
            .with_logical_type(Some(LogicalType::Integer {
                bit_width: 8,
                is_signed: false,
            }))
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::UInt16 => Type::primitive_type_builder(name, PhysicalType::INT32)
            .with_logical_type(Some(LogicalType::Integer {
                bit_width: 16,
                is_signed: false,
            }))
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::UInt32 => Type::primitive_type_builder(name, PhysicalType::INT32)
            .with_logical_type(Some(LogicalType::Integer {
                bit_width: 32,
                is_signed: false,
            }))
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::UInt64 => Type::primitive_type_builder(name, PhysicalType::INT64)
            .with_logical_type(Some(LogicalType::Integer {
                bit_width: 64,
                is_signed: false,
            }))
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::Float16 => Type::primitive_type_builder(name, PhysicalType::FIXED_LEN_BYTE_ARRAY)
            .with_repetition(repetition)
            .with_id(id)
            .with_logical_type(Some(LogicalType::Float16))
            .with_length(2)
            .build(),
        DataType::Float32 => Type::primitive_type_builder(name, PhysicalType::FLOAT)
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::Float64 => Type::primitive_type_builder(name, PhysicalType::DOUBLE)
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::Timestamp(TimeUnit::Second, _) => {
            // Cannot represent seconds in LogicalType
            Type::primitive_type_builder(name, PhysicalType::INT64)
                .with_repetition(repetition)
                .with_id(id)
                .build()
        }
        DataType::Timestamp(time_unit, tz) => {
            Type::primitive_type_builder(name, PhysicalType::INT64)
                .with_logical_type(Some(LogicalType::Timestamp {
                    // If timezone set, values are normalized to UTC timezone
                    is_adjusted_to_u_t_c: matches!(tz, Some(z) if !z.as_ref().is_empty()),
                    unit: match time_unit {
                        TimeUnit::Second => unreachable!(),
                        TimeUnit::Millisecond => ParquetTimeUnit::MILLIS(Default::default()),
                        TimeUnit::Microsecond => ParquetTimeUnit::MICROS(Default::default()),
                        TimeUnit::Nanosecond => ParquetTimeUnit::NANOS(Default::default()),
                    },
                }))
                .with_repetition(repetition)
                .with_id(id)
                .build()
        }
        DataType::Date32 => Type::primitive_type_builder(name, PhysicalType::INT32)
            .with_logical_type(Some(LogicalType::Date))
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        // date64 is cast to date32 (#1666)
        DataType::Date64 => Type::primitive_type_builder(name, PhysicalType::INT32)
            .with_logical_type(Some(LogicalType::Date))
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::Time32(TimeUnit::Second) => {
            // Cannot represent seconds in LogicalType
            Type::primitive_type_builder(name, PhysicalType::INT32)
                .with_repetition(repetition)
                .with_id(id)
                .build()
        }
        DataType::Time32(unit) => Type::primitive_type_builder(name, PhysicalType::INT32)
            .with_logical_type(Some(LogicalType::Time {
                is_adjusted_to_u_t_c: false,
                unit: match unit {
                    TimeUnit::Millisecond => ParquetTimeUnit::MILLIS(Default::default()),
                    u => unreachable!("Invalid unit for Time32: {:?}", u),
                },
            }))
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::Time64(unit) => Type::primitive_type_builder(name, PhysicalType::INT64)
            .with_logical_type(Some(LogicalType::Time {
                is_adjusted_to_u_t_c: false,
                unit: match unit {
                    TimeUnit::Microsecond => ParquetTimeUnit::MICROS(Default::default()),
                    TimeUnit::Nanosecond => ParquetTimeUnit::NANOS(Default::default()),
                    u => unreachable!("Invalid unit for Time64: {:?}", u),
                },
            }))
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::Duration(_) => Err(ParquetError::ArrowError(
            "Converting Duration to parquet not supported".to_string(),
        )),
        DataType::Interval(_) => {
            Type::primitive_type_builder(name, PhysicalType::FIXED_LEN_BYTE_ARRAY)
                .with_converted_type(ConvertedType::INTERVAL)
                .with_repetition(repetition)
                .with_id(id)
                .with_length(12)
                .build()
        }
        DataType::Binary | DataType::LargeBinary => {
            Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
                .with_repetition(repetition)
                .with_id(id)
                .build()
        }
        DataType::FixedSizeBinary(length) => {
            Type::primitive_type_builder(name, PhysicalType::FIXED_LEN_BYTE_ARRAY)
                .with_repetition(repetition)
                .with_id(id)
                .with_length(*length)
                .build()
        }
        DataType::BinaryView => Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            // Decimal precision determines the Parquet physical type to use.
            // Following the: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#decimal
            let (physical_type, length) = if *precision > 1 && *precision <= 9 {
                (PhysicalType::INT32, -1)
            } else if *precision <= 18 {
                (PhysicalType::INT64, -1)
            } else if *precision > 38 {
                // For fix read
                (PhysicalType::FIXED_LEN_BYTE_ARRAY, 32)
            } else {
                (
                    PhysicalType::FIXED_LEN_BYTE_ARRAY,
                    decimal_length_from_precision(*precision) as i32,
                )
            };
            Type::primitive_type_builder(name, physical_type)
                .with_repetition(repetition)
                .with_id(id)
                .with_length(length)
                .with_logical_type(Some(LogicalType::Decimal {
                    scale: *scale as i32,
                    precision: *precision as i32,
                }))
                .with_precision(*precision as i32)
                .with_scale(*scale as i32)
                .build()
        }
        DataType::Utf8 | DataType::LargeUtf8 => {
            Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
                .with_logical_type(Some(LogicalType::String))
                .with_repetition(repetition)
                .with_id(id)
                .build()
        }
        DataType::Utf8View => Type::primitive_type_builder(name, PhysicalType::BYTE_ARRAY)
            .with_logical_type(Some(LogicalType::String))
            .with_repetition(repetition)
            .with_id(id)
            .build(),
        DataType::List(f) | DataType::FixedSizeList(f, _) | DataType::LargeList(f) => {
            Type::group_type_builder(name)
                .with_fields(vec![Arc::new(
                    Type::group_type_builder("list")
                        .with_fields(vec![Arc::new(arrow_to_parquet_type_fix(f)?)])
                        .with_repetition(Repetition::REPEATED)
                        .build()?,
                )])
                .with_logical_type(Some(LogicalType::List))
                .with_repetition(repetition)
                .with_id(id)
                .build()
        }
        DataType::ListView(_) | DataType::LargeListView(_) => {
            unimplemented!("ListView/LargeListView not implemented")
        }
        DataType::Struct(fields) => {
            if fields.is_empty() {
                return Err(ParquetError::ArrowError(
                    "Parquet does not support writing empty structs".to_string(),
                ));
            }
            // recursively convert children to types/nodes
            let fields = fields
                .iter()
                .map(|f| arrow_to_parquet_type_fix(f).map(Arc::new))
                .collect::<Result<_>>()?;
            Type::group_type_builder(name)
                .with_fields(fields)
                .with_repetition(repetition)
                .with_id(id)
                .build()
        }
        DataType::Map(field, _) => {
            if let DataType::Struct(struct_fields) = field.data_type() {
                Type::group_type_builder(name)
                    .with_fields(vec![Arc::new(
                        Type::group_type_builder(field.name())
                            .with_fields(vec![
                                Arc::new(arrow_to_parquet_type_fix(&struct_fields[0])?),
                                Arc::new(arrow_to_parquet_type_fix(&struct_fields[1])?),
                            ])
                            .with_repetition(Repetition::REPEATED)
                            .build()?,
                    )])
                    .with_logical_type(Some(LogicalType::Map))
                    .with_repetition(repetition)
                    .with_id(id)
                    .build()
            } else {
                Err(ParquetError::ArrowError(
                    "DataType::Map should contain a struct field child".to_string(),
                ))
            }
        }
        DataType::Union(_, _) => unimplemented!("See ARROW-8817."),
        DataType::Dictionary(_, ref value) => {
            // Dictionary encoding not handled at the schema level
            let dict_field = field.clone().with_data_type(value.as_ref().clone());
            arrow_to_parquet_type_fix(&dict_field)
        }
        DataType::RunEndEncoded(_, _) => Err(ParquetError::ArrowError(
            "Converting RunEndEncodedType to parquet not supported".to_string(),
        )),
    }
}

fn field_id(field: &Field) -> Option<i32> {
    let value = field
        .metadata()
        .get(parquet::arrow::PARQUET_FIELD_ID_META_KEY)?;
    value.parse().ok() // Fail quietly if not a valid integer
}
