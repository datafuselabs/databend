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

use databend_common_datavalues::DataValue;
use ordered_float::OrderedFloat;

use crate::Scalar;

pub fn scalar_to_datavalue(scalar: &Scalar) -> DataValue {
    match scalar {
        Scalar::Null => DataValue::Null,
        Scalar::EmptyArray => DataValue::Null,
        Scalar::Number(ty) => match ty {
            crate::types::number::NumberScalar::UInt8(x) => DataValue::UInt64(*x as u64),
            crate::types::number::NumberScalar::UInt16(x) => DataValue::UInt64(*x as u64),
            crate::types::number::NumberScalar::UInt32(x) => DataValue::UInt64(*x as u64),
            crate::types::number::NumberScalar::UInt64(x) => DataValue::UInt64(*x),
            crate::types::number::NumberScalar::Int8(x) => DataValue::Int64(*x as i64),
            crate::types::number::NumberScalar::Int16(x) => DataValue::Int64(*x as i64),
            crate::types::number::NumberScalar::Int32(x) => DataValue::Int64(*x as i64),
            crate::types::number::NumberScalar::Int64(x) => DataValue::Int64(*x),
            crate::types::number::NumberScalar::Float32(x) => {
                DataValue::Float64(<OrderedFloat<f32> as Into<f32>>::into(*x) as f64)
            }
            crate::types::number::NumberScalar::Float64(x) => DataValue::Float64((*x).into()),
        },
        Scalar::Decimal(_) => unimplemented!("decimal type is not supported"),
        Scalar::Timestamp(x) => DataValue::Int64(*x),
        Scalar::Date(x) => DataValue::Int64(*x as i64),
        Scalar::Boolean(x) => DataValue::Boolean(*x),
        Scalar::Variant(x) => DataValue::String(x.clone()),
        Scalar::Geometry(x) => DataValue::String(x.clone()),
        Scalar::String(x) => DataValue::String(x.as_bytes().to_vec()),
        Scalar::Array(x) => {
            let values = (0..x.len())
                .map(|idx| scalar_to_datavalue(&x.index(idx).unwrap().to_owned()))
                .collect();
            DataValue::Array(values)
        }
        Scalar::Tuple(x) => {
            let values = x.iter().map(scalar_to_datavalue).collect();
            DataValue::Struct(values)
        }
        Scalar::EmptyMap
        | Scalar::Binary(_)
        | Scalar::Map(_)
        | Scalar::Bitmap(_)
        | Scalar::Geography(_) => {
            unimplemented!()
        }
    }
}
