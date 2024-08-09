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

use enum_as_inner::EnumAsInner;

use crate::types::boolean::BooleanDomain;
use crate::types::decimal::Decimal128Type;
use crate::types::decimal::Decimal256Type;
use crate::types::decimal::DecimalDomain;
use crate::types::decimal::DecimalScalar;
use crate::types::nullable::NullableDomain;
use crate::types::number::NumberDomain;
use crate::types::number::NumberScalar;
use crate::types::number::SimpleDomain;
use crate::types::number::F32;
use crate::types::number::F64;
use crate::types::string::StringDomain;
use crate::types::AnyType;
use crate::types::ArgType;
use crate::types::BooleanType;
use crate::types::DataType;
use crate::types::DateType;
use crate::types::DecimalDataType;
use crate::types::NumberDataType;
use crate::types::NumberType;
use crate::types::StringType;
use crate::types::TimestampType;
use crate::types::ValueType;
use crate::with_decimal_type;
use crate::with_number_type;
use crate::Scalar;

#[derive(Debug, Clone, Copy)]
pub struct FunctionProperty {
    pub non_deterministic: bool,
    pub kind: FunctionKind,
}

impl FunctionProperty {
    pub fn non_deterministic(mut self) -> Self {
        self.non_deterministic = true;
        self
    }

    pub fn kind(mut self, kind: FunctionKind) -> Self {
        self.kind = kind;
        self
    }
}

impl Default for FunctionProperty {
    fn default() -> Self {
        FunctionProperty {
            non_deterministic: false,
            kind: FunctionKind::Scalar,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionKind {
    Scalar,
    SRF,
}

/// Describe the behavior of a function to eliminate the runtime
/// evaluation of the function if possible.
#[derive(Debug, Clone, PartialEq, EnumAsInner)]
pub enum FunctionDomain<T: ValueType> {
    /// The function may return error.
    MayThrow,
    /// The function must not return error, and the return value can be
    /// any valid value the type can represent.
    Full,
    /// The function must not return error, and have further information
    /// about the range of the output value.
    Domain(T::Domain),
}

/// The range of the possible values that a scalar or the scalars in a
/// column can take. We can assume the values outside the range are not
/// possible, but we cannot assume the values inside the range must exist.
#[derive(Debug, Clone, PartialEq, EnumAsInner)]
pub enum Domain {
    Number(NumberDomain),
    Decimal(DecimalDomain),
    Boolean(BooleanDomain),
    String(StringDomain),
    Timestamp(SimpleDomain<i64>),
    Date(SimpleDomain<i32>),
    Nullable(NullableDomain<AnyType>),
    /// `Array(None)` means that the array is empty, thus there is no inner domain information.
    Array(Option<Box<Domain>>),
    /// `Map(None)` means that the map is empty, thus there is no inner domain information.
    Map(Option<Box<Domain>>),
    Tuple(Vec<Domain>),
    /// For certain types, like `Variant`, the domain is useless therefore is not defined.
    Undefined,
}

impl<T: ValueType> FunctionDomain<T> {
    pub fn map<U: ValueType>(self, f: impl Fn(T::Domain) -> U::Domain) -> FunctionDomain<U> {
        match self {
            FunctionDomain::MayThrow => FunctionDomain::MayThrow,
            FunctionDomain::Full => FunctionDomain::Full,
            FunctionDomain::Domain(domain) => FunctionDomain::Domain(f(domain)),
        }
    }
}

impl<T: ArgType> FunctionDomain<T> {
    /// Return the range of the output value.
    ///
    /// Return `None` if the function may return error.
    pub fn normalize(self) -> Option<T::Domain> {
        match self {
            FunctionDomain::MayThrow => None,
            FunctionDomain::Full => Some(T::full_domain()),
            FunctionDomain::Domain(domain) => Some(domain),
        }
    }
}

impl Domain {
    pub fn full(data_type: &DataType) -> Self {
        match data_type {
            DataType::Boolean => Domain::Boolean(BooleanType::full_domain()),
            DataType::String => Domain::String(StringType::full_domain()),
            DataType::Number(NumberDataType::UInt8) => {
                Domain::Number(NumberDomain::UInt8(NumberType::<u8>::full_domain()))
            }
            DataType::Number(NumberDataType::UInt16) => {
                Domain::Number(NumberDomain::UInt16(NumberType::<u16>::full_domain()))
            }
            DataType::Number(NumberDataType::UInt32) => {
                Domain::Number(NumberDomain::UInt32(NumberType::<u32>::full_domain()))
            }
            DataType::Number(NumberDataType::UInt64) => {
                Domain::Number(NumberDomain::UInt64(NumberType::<u64>::full_domain()))
            }
            DataType::Number(NumberDataType::Int8) => {
                Domain::Number(NumberDomain::Int8(NumberType::<i8>::full_domain()))
            }
            DataType::Number(NumberDataType::Int16) => {
                Domain::Number(NumberDomain::Int16(NumberType::<i16>::full_domain()))
            }
            DataType::Number(NumberDataType::Int32) => {
                Domain::Number(NumberDomain::Int32(NumberType::<i32>::full_domain()))
            }
            DataType::Number(NumberDataType::Int64) => {
                Domain::Number(NumberDomain::Int64(NumberType::<i64>::full_domain()))
            }
            DataType::Number(NumberDataType::Float32) => {
                Domain::Number(NumberDomain::Float32(NumberType::<F32>::full_domain()))
            }
            DataType::Number(NumberDataType::Float64) => {
                Domain::Number(NumberDomain::Float64(NumberType::<F64>::full_domain()))
            }
            DataType::Decimal(x) => match x {
                DecimalDataType::Decimal128(x) => {
                    Domain::Decimal(DecimalDomain::Decimal128(Decimal128Type::full_domain(), *x))
                }
                DecimalDataType::Decimal256(x) => {
                    Domain::Decimal(DecimalDomain::Decimal256(Decimal256Type::full_domain(), *x))
                }
            },
            DataType::Timestamp => Domain::Timestamp(TimestampType::full_domain()),
            DataType::Date => Domain::Date(DateType::full_domain()),
            DataType::Null => Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            }),
            DataType::Nullable(ty) => Domain::Nullable(NullableDomain {
                has_null: true,
                value: Some(Box::new(Domain::full(ty))),
            }),
            DataType::Tuple(fields_ty) => {
                Domain::Tuple(fields_ty.iter().map(Domain::full).collect())
            }
            DataType::EmptyArray => Domain::Array(None),
            DataType::Array(ty) => Domain::Array(Some(Box::new(Domain::full(ty)))),
            DataType::EmptyMap => Domain::Map(None),
            DataType::Map(ty) => Domain::Map(Some(Box::new(Domain::full(ty)))),
            DataType::Binary
            | DataType::Bitmap
            | DataType::Variant
            | DataType::Geometry
            | DataType::Geography => Domain::Undefined,
            DataType::Generic(_) => unreachable!(),
        }
    }

    pub fn merge(&self, other: &Domain) -> Domain {
        match (self, other) {
            (Domain::Number(this), Domain::Number(other)) => {
                with_number_type!(|TYPE| match (this, other) {
                    (NumberDomain::TYPE(this), NumberDomain::TYPE(other)) =>
                        Domain::Number(NumberDomain::TYPE(SimpleDomain {
                            min: this.min.min(other.min),
                            max: this.max.max(other.max),
                        })),
                    _ => unreachable!("unable to merge {this:?} with {other:?}"),
                })
            }
            (Domain::Decimal(this), Domain::Decimal(other)) => {
                with_decimal_type!(|TYPE| match (this, other) {
                    (DecimalDomain::TYPE(x, size), DecimalDomain::TYPE(y, _)) =>
                        Domain::Decimal(DecimalDomain::TYPE(
                            SimpleDomain {
                                min: x.min.min(y.min),
                                max: x.max.max(y.max),
                            },
                            *size
                        ),),
                    _ => unreachable!("unable to merge {this:?} with {other:?}"),
                })
            }
            (Domain::Boolean(this), Domain::Boolean(other)) => Domain::Boolean(BooleanDomain {
                has_false: this.has_false || other.has_false,
                has_true: this.has_true || other.has_true,
            }),
            (Domain::String(this), Domain::String(other)) => Domain::String(StringDomain {
                min: this.min.as_str().min(&other.min).to_string(),
                max: this
                    .max
                    .as_ref()
                    .zip(other.max.as_ref())
                    .map(|(self_max, other_max)| self_max.max(other_max).to_string()),
            }),
            (Domain::Timestamp(this), Domain::Timestamp(other)) => {
                Domain::Timestamp(SimpleDomain {
                    min: this.min.min(other.min),
                    max: this.max.max(other.max),
                })
            }
            (Domain::Date(this), Domain::Date(other)) => Domain::Date(SimpleDomain {
                min: this.min.min(other.min),
                max: this.max.max(other.max),
            }),
            (
                Domain::Nullable(NullableDomain {
                    has_null: true,
                    value: None,
                }),
                Domain::Nullable(NullableDomain {
                    has_null: true,
                    value: None,
                }),
            ) => Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            }),
            (
                Domain::Nullable(NullableDomain {
                    has_null: _,
                    value: Some(self_value),
                }),
                Domain::Nullable(NullableDomain {
                    has_null: true,
                    value: None,
                }),
            ) => Domain::Nullable(NullableDomain {
                has_null: true,
                value: Some(self_value.clone()),
            }),
            (
                Domain::Nullable(NullableDomain {
                    has_null: true,
                    value: None,
                }),
                Domain::Nullable(NullableDomain {
                    has_null: _,
                    value: Some(other_value),
                }),
            ) => Domain::Nullable(NullableDomain {
                has_null: true,
                value: Some(other_value.clone()),
            }),
            (
                Domain::Nullable(NullableDomain {
                    has_null: self_has_null,
                    value: Some(self_value),
                }),
                Domain::Nullable(NullableDomain {
                    has_null: other_has_null,
                    value: Some(other_value),
                }),
            ) => Domain::Nullable(NullableDomain {
                has_null: *self_has_null || *other_has_null,
                value: Some(Box::new(self_value.merge(other_value))),
            }),
            (Domain::Array(None), Domain::Array(None)) => Domain::Array(None),
            (Domain::Array(Some(_)), Domain::Array(None)) => self.clone(),
            (Domain::Array(None), Domain::Array(Some(_))) => other.clone(),
            (Domain::Array(Some(self_arr)), Domain::Array(Some(other_arr))) => {
                Domain::Array(Some(Box::new(self_arr.merge(other_arr))))
            }
            (Domain::Map(None), Domain::Map(None)) => Domain::Map(None),
            (Domain::Map(Some(_)), Domain::Map(None)) => self.clone(),
            (Domain::Map(None), Domain::Map(Some(_))) => other.clone(),
            (Domain::Map(Some(self_arr)), Domain::Map(Some(other_arr))) => {
                Domain::Map(Some(Box::new(self_arr.merge(other_arr))))
            }
            (Domain::Tuple(self_tup), Domain::Tuple(other_tup)) => Domain::Tuple(
                self_tup
                    .iter()
                    .zip(other_tup.iter())
                    .map(|(self_tup, other_tup)| self_tup.merge(other_tup))
                    .collect(),
            ),
            (Domain::Undefined, Domain::Undefined) => Domain::Undefined,
            (this, other) => unreachable!("unable to merge {this:?} with {other:?}"),
        }
    }

    pub fn as_singleton(&self) -> Option<Scalar> {
        match self {
            Domain::Number(NumberDomain::Int8(SimpleDomain { min, max })) if min == max => {
                Some(Scalar::Number(NumberScalar::Int8(*min)))
            }
            Domain::Number(NumberDomain::Int16(SimpleDomain { min, max })) if min == max => {
                Some(Scalar::Number(NumberScalar::Int16(*min)))
            }
            Domain::Number(NumberDomain::Int32(SimpleDomain { min, max })) if min == max => {
                Some(Scalar::Number(NumberScalar::Int32(*min)))
            }
            Domain::Number(NumberDomain::Int64(SimpleDomain { min, max })) if min == max => {
                Some(Scalar::Number(NumberScalar::Int64(*min)))
            }
            Domain::Number(NumberDomain::UInt8(SimpleDomain { min, max })) if min == max => {
                Some(Scalar::Number(NumberScalar::UInt8(*min)))
            }
            Domain::Number(NumberDomain::UInt16(SimpleDomain { min, max })) if min == max => {
                Some(Scalar::Number(NumberScalar::UInt16(*min)))
            }
            Domain::Number(NumberDomain::UInt32(SimpleDomain { min, max })) if min == max => {
                Some(Scalar::Number(NumberScalar::UInt32(*min)))
            }
            Domain::Number(NumberDomain::UInt64(SimpleDomain { min, max })) if min == max => {
                Some(Scalar::Number(NumberScalar::UInt64(*min)))
            }
            Domain::Number(NumberDomain::Float32(SimpleDomain { min, max }))
                if min == max && !min.is_nan() =>
            {
                Some(Scalar::Number(NumberScalar::Float32(*min)))
            }
            Domain::Number(NumberDomain::Float64(SimpleDomain { min, max }))
                if min == max && !min.is_nan() =>
            {
                Some(Scalar::Number(NumberScalar::Float64(*min)))
            }
            Domain::Decimal(DecimalDomain::Decimal128(SimpleDomain { min, max }, sz))
                if min == max =>
            {
                Some(Scalar::Decimal(DecimalScalar::Decimal128(*min, *sz)))
            }
            Domain::Decimal(DecimalDomain::Decimal256(SimpleDomain { min, max }, sz))
                if min == max =>
            {
                Some(Scalar::Decimal(DecimalScalar::Decimal256(*min, *sz)))
            }
            Domain::Boolean(BooleanDomain {
                has_false: true,
                has_true: false,
            }) => Some(Scalar::Boolean(false)),
            Domain::Boolean(BooleanDomain {
                has_false: false,
                has_true: true,
            }) => Some(Scalar::Boolean(true)),
            Domain::String(StringDomain { min, max }) if Some(min) == max.as_ref() => {
                Some(Scalar::String(min.clone()))
            }
            Domain::Timestamp(SimpleDomain { min, max }) if min == max => {
                Some(Scalar::Timestamp(*min))
            }
            Domain::Date(SimpleDomain { min, max }) if min == max => Some(Scalar::Date(*min)),
            Domain::Nullable(NullableDomain {
                has_null: true,
                value: None,
            }) => Some(Scalar::Null),
            Domain::Nullable(NullableDomain {
                has_null: false,
                value: Some(value),
            }) => value.as_singleton(),
            Domain::Tuple(fields) => Some(Scalar::Tuple(
                fields
                    .iter()
                    .map(|field| field.as_singleton())
                    .collect::<Option<Vec<_>>>()?,
            )),
            _ => None,
        }
    }
}

pub trait SimpleDomainCmp {
    fn domain_eq(&self, other: &Self) -> FunctionDomain<BooleanType>;
    fn domain_noteq(&self, other: &Self) -> FunctionDomain<BooleanType>;
    fn domain_gt(&self, other: &Self) -> FunctionDomain<BooleanType>;
    fn domain_gte(&self, other: &Self) -> FunctionDomain<BooleanType>;
    fn domain_lt(&self, other: &Self) -> FunctionDomain<BooleanType>;
    fn domain_lte(&self, other: &Self) -> FunctionDomain<BooleanType>;
    fn domain_contains(&self, other: &Self) -> FunctionDomain<BooleanType>;
}

const ALL_TRUE_DOMAIN: BooleanDomain = BooleanDomain {
    has_true: true,
    has_false: false,
};

const ALL_FALSE_DOMAIN: BooleanDomain = BooleanDomain {
    has_true: false,
    has_false: true,
};

impl<T: Ord + PartialOrd> SimpleDomainCmp for SimpleDomain<T> {
    fn domain_eq(&self, other: &Self) -> FunctionDomain<BooleanType> {
        if self.min > other.max || self.max < other.min {
            FunctionDomain::Domain(ALL_FALSE_DOMAIN)
        } else {
            FunctionDomain::Full
        }
    }

    fn domain_noteq(&self, other: &Self) -> FunctionDomain<BooleanType> {
        if self.min > other.max || self.max < other.min {
            FunctionDomain::Domain(ALL_TRUE_DOMAIN)
        } else {
            FunctionDomain::Full
        }
    }

    fn domain_gt(&self, other: &Self) -> FunctionDomain<BooleanType> {
        if self.min > other.max {
            FunctionDomain::Domain(ALL_TRUE_DOMAIN)
        } else if self.max <= other.min {
            FunctionDomain::Domain(ALL_FALSE_DOMAIN)
        } else {
            FunctionDomain::Full
        }
    }

    fn domain_gte(&self, other: &Self) -> FunctionDomain<BooleanType> {
        if self.min >= other.max {
            FunctionDomain::Domain(ALL_TRUE_DOMAIN)
        } else if self.max < other.min {
            FunctionDomain::Domain(ALL_FALSE_DOMAIN)
        } else {
            FunctionDomain::Full
        }
    }

    fn domain_lt(&self, other: &Self) -> FunctionDomain<BooleanType> {
        if self.max < other.min {
            FunctionDomain::Domain(ALL_TRUE_DOMAIN)
        } else if self.min >= other.max {
            FunctionDomain::Domain(ALL_FALSE_DOMAIN)
        } else {
            FunctionDomain::Full
        }
    }

    fn domain_lte(&self, other: &Self) -> FunctionDomain<BooleanType> {
        if self.max <= other.min {
            FunctionDomain::Domain(ALL_TRUE_DOMAIN)
        } else if self.min > other.max {
            FunctionDomain::Domain(ALL_FALSE_DOMAIN)
        } else {
            FunctionDomain::Full
        }
    }

    fn domain_contains(&self, other: &Self) -> FunctionDomain<BooleanType> {
        if self.min > other.max || self.max < other.min {
            FunctionDomain::Domain(ALL_FALSE_DOMAIN)
        } else {
            FunctionDomain::Full
        }
    }
}

impl SimpleDomainCmp for StringDomain {
    fn domain_eq(&self, other: &Self) -> FunctionDomain<BooleanType> {
        let (d1, d2) = unify_string(self, other);
        d1.domain_eq(&d2)
    }

    fn domain_noteq(&self, other: &Self) -> FunctionDomain<BooleanType> {
        let (d1, d2) = unify_string(self, other);
        d1.domain_noteq(&d2)
    }

    fn domain_gt(&self, other: &Self) -> FunctionDomain<BooleanType> {
        let (d1, d2) = unify_string(self, other);
        d1.domain_gt(&d2)
    }

    fn domain_gte(&self, other: &Self) -> FunctionDomain<BooleanType> {
        let (d1, d2) = unify_string(self, other);
        d1.domain_gte(&d2)
    }

    fn domain_lt(&self, other: &Self) -> FunctionDomain<BooleanType> {
        let (d1, d2) = unify_string(self, other);
        d1.domain_lt(&d2)
    }

    fn domain_lte(&self, other: &Self) -> FunctionDomain<BooleanType> {
        let (d1, d2) = unify_string(self, other);
        d1.domain_lte(&d2)
    }

    fn domain_contains(&self, other: &Self) -> FunctionDomain<BooleanType> {
        let (d1, d2) = unify_string(self, other);
        d1.domain_contains(&d2)
    }
}

pub fn unify_string(
    lhs: &StringDomain,
    rhs: &StringDomain,
) -> (SimpleDomain<String>, SimpleDomain<String>) {
    let mut max = lhs.min.as_str().max(&rhs.min);
    if let Some(lhs_max) = &lhs.max {
        max = max.max(lhs_max);
    }
    if let Some(rhs_max) = &rhs.max {
        max = max.max(rhs_max);
    }

    let mut max = max.to_string();
    max.push('\0');

    (
        SimpleDomain {
            min: lhs.min.clone(),
            max: lhs.max.clone().unwrap_or_else(|| max.clone()),
        },
        SimpleDomain {
            min: rhs.min.clone(),
            max: rhs.max.clone().unwrap_or_else(|| max.clone()),
        },
    )
}
