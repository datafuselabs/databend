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

//! This mod is the key point about compatibility.
//! Everytime update anything in this file, update the `VER` and let the tests pass.

use databend_common_expression as ex;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::TableDataType;
use databend_common_protos::pb;
use databend_common_protos::pb::data_type::Dt;
use databend_common_protos::pb::data_type::Dt24;
use databend_common_protos::pb::number::Num;

use crate::reader_check_msg;
use crate::FromToProto;
use crate::Incompatible;
use crate::MIN_READER_VER;
use crate::VER;

impl FromToProto for ex::TableSchema {
    type PB = pb::DataSchema;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::DataSchema) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let mut fs = Vec::with_capacity(p.fields.len());
        for f in p.fields {
            fs.push(ex::TableField::from_pb(f)?);
        }

        let v = Self::new_from_column_ids(fs, p.metadata, p.next_column_id);
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::DataSchema, Incompatible> {
        let mut fs = Vec::with_capacity(self.fields().len());
        for f in self.fields() {
            fs.push(f.to_pb()?);
        }

        let p = pb::DataSchema {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            fields: fs,
            metadata: self.meta().clone(),
            next_column_id: self.next_column_id(),
        };
        Ok(p)
    }
}

impl FromToProto for ex::TableField {
    type PB = pb::DataField;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::DataField) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let computed_expr = match p.computed_expr {
            Some(computed_expr) => Some(ex::ComputedExpr::from_pb(computed_expr)?),
            None => None,
        };

        let v = ex::TableField::new_from_column_id(
            &p.name,
            ex::TableDataType::from_pb(p.data_type.ok_or_else(|| Incompatible {
                reason: "DataField.data_type can not be None".to_string(),
            })?)?,
            p.column_id,
        )
        .with_default_expr(p.default_expr)
        .with_computed_expr(computed_expr);
        Ok(v)
    }

    fn to_pb(&self) -> Result<pb::DataField, Incompatible> {
        let computed_expr = match self.computed_expr() {
            Some(computed_expr) => Some(computed_expr.to_pb()?),
            None => None,
        };
        let p = pb::DataField {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            name: self.name().clone(),
            default_expr: self.default_expr().cloned(),
            data_type: Some(self.data_type().to_pb()?),
            column_id: self.column_id(),
            computed_expr,
        };
        Ok(p)
    }
}

impl FromToProto for ex::ComputedExpr {
    type PB = pb::ComputedExpr;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: pb::ComputedExpr) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let computed_expr = p.computed_expr.ok_or_else(|| Incompatible {
            reason: "Invalid ComputedExpr: .computed_expr can not be None".to_string(),
        })?;

        let x = match computed_expr {
            pb::computed_expr::ComputedExpr::Virtual(expr) => Self::Virtual(expr),
            pb::computed_expr::ComputedExpr::Stored(expr) => Self::Stored(expr),
        };
        Ok(x)
    }

    fn to_pb(&self) -> Result<pb::ComputedExpr, Incompatible> {
        let x = match self {
            ex::ComputedExpr::Virtual(expr) => {
                pb::computed_expr::ComputedExpr::Virtual(expr.clone())
            }
            ex::ComputedExpr::Stored(expr) => pb::computed_expr::ComputedExpr::Stored(expr.clone()),
        };

        Ok(pb::ComputedExpr {
            ver: VER,
            min_reader_ver: MIN_READER_VER,

            computed_expr: Some(x),
        })
    }
}

impl FromToProto for ex::TableDataType {
    type PB = pb::DataType;
    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }
    fn from_pb(p: pb::DataType) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        match (&p.dt, &p.dt24) {
            (None, None) => Err(Incompatible {
                reason: "DataType .dt and .dt24 are both None".to_string(),
            }),
            (Some(_), None) => {
                // Convert from version 23 or lower:
                let x = match p.dt.unwrap() {
                    Dt::NullType(_) => ex::TableDataType::Null,
                    Dt::NullableType(nullable_type) => {
                        //
                        reader_check_msg(nullable_type.ver, nullable_type.min_reader_ver)?;

                        let inner = Box::into_inner(nullable_type).inner;
                        let inner = inner.ok_or_else(|| Incompatible {
                            reason: "NullableType.inner can not be None".to_string(),
                        })?;
                        let inner = Box::into_inner(inner);
                        ex::TableDataType::Nullable(Box::new(ex::TableDataType::from_pb(inner)?))
                    }
                    Dt::BoolType(_) => ex::TableDataType::Boolean,
                    Dt::Int8Type(_) => ex::TableDataType::Number(NumberDataType::Int8),
                    Dt::Int16Type(_) => ex::TableDataType::Number(NumberDataType::Int16),
                    Dt::Int32Type(_) => ex::TableDataType::Number(NumberDataType::Int32),
                    Dt::Int64Type(_) => ex::TableDataType::Number(NumberDataType::Int64),
                    Dt::Uint8Type(_) => ex::TableDataType::Number(NumberDataType::UInt8),
                    Dt::Uint16Type(_) => ex::TableDataType::Number(NumberDataType::UInt16),
                    Dt::Uint32Type(_) => ex::TableDataType::Number(NumberDataType::UInt32),
                    Dt::Uint64Type(_) => ex::TableDataType::Number(NumberDataType::UInt64),
                    Dt::Float32Type(_) => ex::TableDataType::Number(NumberDataType::Float32),
                    Dt::Float64Type(_) => ex::TableDataType::Number(NumberDataType::Float64),
                    Dt::DateType(_) => ex::TableDataType::Date,
                    Dt::TimestampType(_) => ex::TableDataType::Timestamp,
                    Dt::StringType(_) => ex::TableDataType::String,
                    Dt::StructType(stt) => {
                        reader_check_msg(stt.ver, stt.min_reader_ver)?;

                        let mut types = vec![];
                        for x in stt.types {
                            let vv = ex::TableDataType::from_pb(x)?;
                            types.push(vv);
                        }

                        ex::TableDataType::Tuple {
                            fields_name: stt.names,
                            fields_type: types,
                        }
                    }
                    Dt::ArrayType(a) => {
                        reader_check_msg(a.ver, a.min_reader_ver)?;

                        let inner = Box::into_inner(a).inner;
                        let inner = inner.ok_or_else(|| Incompatible {
                            reason: "Array.inner can not be None".to_string(),
                        })?;
                        let inner = Box::into_inner(inner);
                        ex::TableDataType::Array(Box::new(ex::TableDataType::from_pb(inner)?))
                    }
                    Dt::VariantType(_) => ex::TableDataType::Variant,
                    Dt::VariantArrayType(_) => ex::TableDataType::Variant,
                    Dt::VariantObjectType(_) => ex::TableDataType::Variant,
                    // NOTE: No Interval type is ever stored in meta-service.
                    //       This variant should never be matched.
                    //       Thus it is safe for this conversion to map it to any type.
                    Dt::IntervalType(_) => ex::TableDataType::Null,
                };
                Ok(x)
            }
            (None, Some(_)) => {
                // Convert from version 24 or higher:
                let x = match p.dt24.unwrap() {
                    Dt24::NullT(_) => ex::TableDataType::Null,
                    Dt24::EmptyArrayT(_) => ex::TableDataType::EmptyArray,
                    Dt24::BoolT(_) => ex::TableDataType::Boolean,
                    Dt24::BinaryT(_) => ex::TableDataType::Binary,
                    Dt24::StringT(_) => ex::TableDataType::String,
                    Dt24::NumberT(n) => {
                        ex::TableDataType::Number(ex::types::NumberDataType::from_pb(n)?)
                    }
                    Dt24::TimestampT(_) => ex::TableDataType::Timestamp,
                    Dt24::DateT(_) => ex::TableDataType::Date,
                    Dt24::NullableT(x) => ex::TableDataType::Nullable(Box::new(
                        ex::TableDataType::from_pb(Box::into_inner(x))?,
                    )),
                    Dt24::ArrayT(x) => ex::TableDataType::Array(Box::new(
                        ex::TableDataType::from_pb(Box::into_inner(x))?,
                    )),
                    Dt24::MapT(x) => ex::TableDataType::Map(Box::new(ex::TableDataType::from_pb(
                        Box::into_inner(x),
                    )?)),
                    Dt24::BitmapT(_) => ex::TableDataType::Bitmap,
                    Dt24::TupleT(t) => {
                        reader_check_msg(t.ver, t.min_reader_ver)?;

                        let mut types = vec![];
                        for x in t.field_types {
                            let vv = ex::TableDataType::from_pb(x)?;
                            types.push(vv);
                        }

                        ex::TableDataType::Tuple {
                            fields_name: t.field_names,
                            fields_type: types,
                        }
                    }
                    Dt24::VariantT(_) => ex::TableDataType::Variant,
                    Dt24::GeometryT(_) => ex::TableDataType::Geometry,
                    Dt24::GeographyT(_) => ex::TableDataType::Geography,
                    Dt24::DecimalT(x) => {
                        ex::TableDataType::Decimal(ex::types::decimal::DecimalDataType::from_pb(x)?)
                    }
                    Dt24::EmptyMapT(_) => ex::TableDataType::EmptyMap,
                };
                Ok(x)
            }
            (Some(_), Some(_)) => Err(Incompatible {
                reason: "Invalid DataType: at most only one of .dt and .dt23 can be Some"
                    .to_string(),
            }),
        }
    }

    fn to_pb(&self) -> Result<pb::DataType, Incompatible> {
        let x = match self {
            TableDataType::Null => new_pb_dt24(Dt24::NullT(pb::Empty {})),
            TableDataType::EmptyArray => new_pb_dt24(Dt24::EmptyArrayT(pb::Empty {})),
            TableDataType::EmptyMap => new_pb_dt24(Dt24::EmptyMapT(pb::Empty {})),
            TableDataType::Boolean => new_pb_dt24(Dt24::BoolT(pb::Empty {})),
            TableDataType::Binary => new_pb_dt24(Dt24::BinaryT(pb::Empty {})),
            TableDataType::String => new_pb_dt24(Dt24::StringT(pb::Empty {})),
            TableDataType::Number(n) => {
                let x = n.to_pb()?;
                new_pb_dt24(Dt24::NumberT(x))
            }
            TableDataType::Decimal(n) => {
                let x = n.to_pb()?;
                new_pb_dt24(Dt24::DecimalT(x))
            }
            TableDataType::Timestamp => new_pb_dt24(Dt24::TimestampT(pb::Empty {})),
            TableDataType::Date => new_pb_dt24(Dt24::DateT(pb::Empty {})),
            TableDataType::Nullable(v) => {
                let x = v.to_pb()?;
                new_pb_dt24(Dt24::NullableT(Box::new(x)))
            }
            TableDataType::Array(v) => {
                let x = v.to_pb()?;
                new_pb_dt24(Dt24::ArrayT(Box::new(x)))
            }
            TableDataType::Map(v) => {
                let x = v.to_pb()?;
                new_pb_dt24(Dt24::MapT(Box::new(x)))
            }
            TableDataType::Bitmap => new_pb_dt24(Dt24::BitmapT(pb::Empty {})),
            TableDataType::Tuple {
                fields_name,
                fields_type,
            } => {
                //
                let mut types = vec![];
                for t in fields_type {
                    let p = t.to_pb()?;
                    types.push(p);
                }

                let x = pb::Tuple {
                    ver: VER,
                    min_reader_ver: MIN_READER_VER,
                    field_names: fields_name.clone(),
                    field_types: types,
                };
                new_pb_dt24(Dt24::TupleT(x))
            }
            TableDataType::Variant => new_pb_dt24(Dt24::VariantT(pb::Empty {})),
            TableDataType::Geometry => new_pb_dt24(Dt24::GeometryT(pb::Empty {})),
            TableDataType::Geography => new_pb_dt24(Dt24::GeographyT(pb::Empty {})),
        };
        Ok(x)
    }
}

impl FromToProto for ex::types::NumberDataType {
    type PB = pb::Number;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: pb::Number) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let num = p.num.ok_or_else(|| Incompatible {
            reason: "Invalid Number: .num can not be None".to_string(),
        })?;

        let x = match num {
            Num::Uint8Type(_) => Self::UInt8,
            Num::Uint16Type(_) => Self::UInt16,
            Num::Uint32Type(_) => Self::UInt32,
            Num::Uint64Type(_) => Self::UInt64,
            Num::Int8Type(_) => Self::Int8,
            Num::Int16Type(_) => Self::Int16,
            Num::Int32Type(_) => Self::Int32,
            Num::Int64Type(_) => Self::Int64,
            Num::Float32Type(_) => Self::Float32,
            Num::Float64Type(_) => Self::Float64,
        };
        Ok(x)
    }

    fn to_pb(&self) -> Result<pb::Number, Incompatible> {
        let x = match self {
            ex::types::NumberDataType::UInt8 => Num::Uint8Type(pb::Empty {}),
            ex::types::NumberDataType::UInt16 => Num::Uint16Type(pb::Empty {}),
            ex::types::NumberDataType::UInt32 => Num::Uint32Type(pb::Empty {}),
            ex::types::NumberDataType::UInt64 => Num::Uint64Type(pb::Empty {}),
            ex::types::NumberDataType::Int8 => Num::Int8Type(pb::Empty {}),
            ex::types::NumberDataType::Int16 => Num::Int16Type(pb::Empty {}),
            ex::types::NumberDataType::Int32 => Num::Int32Type(pb::Empty {}),
            ex::types::NumberDataType::Int64 => Num::Int64Type(pb::Empty {}),
            ex::types::NumberDataType::Float32 => Num::Float32Type(pb::Empty {}),
            ex::types::NumberDataType::Float64 => Num::Float64Type(pb::Empty {}),
        };
        Ok(pb::Number {
            ver: VER,
            min_reader_ver: MIN_READER_VER,

            num: Some(x),
        })
    }
}

impl FromToProto for ex::types::DecimalDataType {
    type PB = pb::Decimal;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: pb::Decimal) -> Result<Self, Incompatible> {
        reader_check_msg(p.ver, p.min_reader_ver)?;

        let num = p.decimal.ok_or_else(|| Incompatible {
            reason: "Invalid Decimal: .decimal can not be None".to_string(),
        })?;

        let x = match num {
            pb::decimal::Decimal::Decimal128(x) => {
                ex::types::DecimalDataType::Decimal128(ex::types::decimal::DecimalSize::from_pb(x)?)
            }
            pb::decimal::Decimal::Decimal256(x) => {
                ex::types::DecimalDataType::Decimal256(ex::types::decimal::DecimalSize::from_pb(x)?)
            }
        };
        Ok(x)
    }

    fn to_pb(&self) -> Result<pb::Decimal, Incompatible> {
        let x = match self {
            ex::types::DecimalDataType::Decimal128(x) => {
                pb::decimal::Decimal::Decimal128(ex::types::decimal::DecimalSize::to_pb(x)?)
            }
            ex::types::DecimalDataType::Decimal256(x) => {
                pb::decimal::Decimal::Decimal256(ex::types::decimal::DecimalSize::to_pb(x)?)
            }
        };
        Ok(pb::Decimal {
            ver: VER,
            min_reader_ver: MIN_READER_VER,

            decimal: Some(x),
        })
    }
}

impl FromToProto for ex::types::decimal::DecimalSize {
    type PB = pb::DecimalSize;

    fn get_pb_ver(p: &Self::PB) -> u64 {
        p.ver
    }

    fn from_pb(p: Self::PB) -> Result<Self, Incompatible>
    where Self: Sized {
        reader_check_msg(p.ver, p.min_reader_ver)?;
        Ok(ex::types::decimal::DecimalSize {
            precision: p.precision as u8,
            scale: p.scale as u8,
        })
    }

    fn to_pb(&self) -> Result<Self::PB, Incompatible> {
        Ok(pb::DecimalSize {
            ver: VER,
            min_reader_ver: MIN_READER_VER,
            precision: self.precision as i32,
            scale: self.scale as i32,
        })
    }
}

/// Create a pb::DataType with version-24 data type schema
fn new_pb_dt24(dt24: Dt24) -> pb::DataType {
    pb::DataType {
        ver: VER,
        min_reader_ver: MIN_READER_VER,
        dt: None,
        dt24: Some(dt24),
    }
}
