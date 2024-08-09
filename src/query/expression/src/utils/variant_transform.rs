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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use jsonb::parse_value;
use jsonb::to_string;

use crate::types::AnyType;
use crate::types::DataType;
use crate::values::Column;
use crate::values::Scalar;
use crate::values::Value;
use crate::ColumnBuilder;
use crate::ScalarRef;

pub fn contains_variant(data_type: &DataType) -> bool {
    match data_type {
        DataType::Variant => true,
        DataType::Null
        | DataType::EmptyArray
        | DataType::EmptyMap
        | DataType::Boolean
        | DataType::Binary
        | DataType::String
        | DataType::Number(_)
        | DataType::Decimal(_)
        | DataType::Timestamp
        | DataType::Date
        | DataType::Bitmap
        | DataType::Geometry
        | DataType::Geography
        | DataType::Generic(_) => false,
        DataType::Nullable(ty) => contains_variant(ty.as_ref()),
        DataType::Array(ty) => contains_variant(ty.as_ref()),
        DataType::Map(ty) => contains_variant(ty.as_ref()),
        DataType::Tuple(types) => types.iter().any(contains_variant),
    }
}

/// This function decodes variant data into string or parses the string into variant data.
/// When `decode` is true, decoding the variant data into string so that UDF Server can handle the variant data.
/// Otherwise parsing the string into variant data.
pub fn transform_variant(value: &Value<AnyType>, decode: bool) -> Result<Value<AnyType>> {
    let value = match value {
        Value::Scalar(scalar) => Value::Scalar(transform_scalar(scalar.as_ref(), decode)?),
        Value::Column(col) => Value::Column(transform_column(col, decode)?),
    };
    Ok(value)
}

fn transform_column(col: &Column, decode: bool) -> Result<Column> {
    let mut builder = ColumnBuilder::with_capacity(&col.data_type(), col.len());
    for scalar in col.iter() {
        builder.push(transform_scalar(scalar, decode)?.as_ref());
    }
    Ok(builder.build())
}

fn transform_scalar(scalar: ScalarRef<'_>, decode: bool) -> Result<Scalar> {
    let scalar = match scalar {
        ScalarRef::Null
        | ScalarRef::EmptyArray
        | ScalarRef::EmptyMap
        | ScalarRef::Number(_)
        | ScalarRef::Decimal(_)
        | ScalarRef::Timestamp(_)
        | ScalarRef::Date(_)
        | ScalarRef::Boolean(_)
        | ScalarRef::Binary(_)
        | ScalarRef::String(_)
        | ScalarRef::Bitmap(_)
        | ScalarRef::Geometry(_)
        | ScalarRef::Geography(_) => scalar.to_owned(),
        ScalarRef::Array(col) => Scalar::Array(transform_column(&col, decode)?),
        ScalarRef::Map(col) => Scalar::Map(transform_column(&col, decode)?),
        ScalarRef::Tuple(scalars) => {
            let scalars = scalars
                .into_iter()
                .map(|scalar| transform_scalar(scalar, decode))
                .collect::<Result<Vec<_>>>()?;
            Scalar::Tuple(scalars)
        }
        ScalarRef::Variant(data) => {
            if decode {
                Scalar::Variant(to_string(data).into_bytes())
            } else {
                let value = parse_value(data).map_err(|err| {
                    ErrorCode::UDFDataError(format!("parse json value error: {err}"))
                })?;
                Scalar::Variant(value.to_vec())
            }
        }
    };
    Ok(scalar)
}
