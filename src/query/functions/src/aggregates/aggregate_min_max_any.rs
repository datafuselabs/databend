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

use std::marker::PhantomData;
use std::sync::Arc;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::decimal::*;
use databend_common_expression::types::number::*;
use databend_common_expression::types::*;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::Scalar;
use ethnum::i256;

use super::aggregate_function_factory::AggregateFunctionDescription;
use super::aggregate_scalar_state::need_manual_drop_state;
use super::aggregate_scalar_state::ChangeIf;
use super::aggregate_scalar_state::CmpAny;
use super::aggregate_scalar_state::CmpMax;
use super::aggregate_scalar_state::CmpMin;
use super::aggregate_scalar_state::TYPE_ANY;
use super::aggregate_scalar_state::TYPE_MAX;
use super::aggregate_scalar_state::TYPE_MIN;
use super::AggregateUnaryFunction;
use super::FunctionData;
use super::UnaryState;
use crate::aggregates::assert_unary_arguments;
use crate::aggregates::AggregateFunction;
use crate::with_compare_mapped_type;
use crate::with_simple_no_number_mapped_type;

#[derive(BorshSerialize, BorshDeserialize)]
pub struct MinMaxAnyState<T, C>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
{
    pub value: Option<T::Scalar>,
    #[borsh(skip)]
    _c: PhantomData<C>,
}

impl<T, C> Default for MinMaxAnyState<T, C>
where
    T: Send + Sync + ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize + Send + Sync,
    C: ChangeIf<T> + Default,
{
    fn default() -> Self {
        Self {
            value: None,
            _c: PhantomData,
        }
    }
}

impl<T, C> UnaryState<T, T> for MinMaxAnyState<T, C>
where
    T: ValueType + Send + Sync,
    T::Scalar: BorshSerialize + BorshDeserialize + Send + Sync,
    C: ChangeIf<T> + Default,
{
    fn add(
        &mut self,
        other: T::ScalarRef<'_>,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        match &self.value {
            Some(v) => {
                if C::change_if(&T::to_scalar_ref(v), &other) {
                    self.value = Some(T::to_owned_scalar(other));
                }
            }
            None => {
                self.value = Some(T::to_owned_scalar(other));
            }
        }
        Ok(())
    }

    fn add_batch(
        &mut self,
        other: T::Column,
        validity: Option<&Bitmap>,
        function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        let column_len = T::column_len(&other);
        if column_len == 0 {
            return Ok(());
        }

        let column_iter = T::iter_column(&other);
        if let Some(validity) = validity {
            if validity.unset_bits() == column_len {
                return Ok(());
            }
            for (data, valid) in column_iter.zip(validity.iter()) {
                if valid {
                    let _ = self.add(data, function_data);
                }
            }
        } else {
            let v = column_iter.reduce(|l, r| if !C::change_if(&l, &r) { l } else { r });
            if let Some(v) = v {
                let _ = self.add(v, function_data);
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        if let Some(v) = &rhs.value {
            self.add(T::to_scalar_ref(v), None)?;
        }
        Ok(())
    }

    fn merge_result(
        &mut self,
        builder: &mut T::ColumnBuilder,
        _function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        if let Some(v) = &self.value {
            T::push_item(builder, T::to_scalar_ref(v));
        } else {
            T::push_default(builder);
        }

        Ok(())
    }
}

pub fn try_create_aggregate_min_max_any_function<const CMP_TYPE: u8>(
    display_name: &str,
    params: Vec<Scalar>,
    argument_types: Vec<DataType>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_unary_arguments(display_name, argument_types.len())?;
    let mut data_type = argument_types[0].clone();
    let need_drop = need_manual_drop_state(&data_type);

    // null use dummy func, it's already covered in `AggregateNullResultFunction`
    if data_type.is_null() {
        data_type = DataType::String;
    }

    with_compare_mapped_type!(|CMP| match CMP_TYPE {
        CMP => {
            with_simple_no_number_mapped_type!(|T| match data_type {
                DataType::T => {
                    let return_type = data_type.clone();
                    let func = AggregateUnaryFunction::<MinMaxAnyState<T, CMP>, T, T>::try_create(
                        display_name,
                        return_type,
                        params,
                        data_type,
                    )
                    .with_need_drop(need_drop);

                    Ok(Arc::new(func))
                }
                DataType::Number(num_type) => {
                    with_number_mapped_type!(|NUM| match num_type {
                        NumberDataType::NUM => {
                            let return_type = data_type.clone();
                            AggregateUnaryFunction::<
                                MinMaxAnyState<NumberType<NUM>, CMP>,
                                NumberType<NUM>,
                                NumberType<NUM>,
                            >::try_create_unary(
                                display_name, return_type, params, data_type
                            )
                        }
                    })
                }
                DataType::Decimal(DecimalDataType::Decimal128(s)) => {
                    let decimal_size = DecimalSize {
                        precision: s.precision,
                        scale: s.scale,
                    };
                    let return_type = DataType::Decimal(DecimalDataType::from_size(decimal_size)?);
                    AggregateUnaryFunction::<
                        MinMaxAnyState<DecimalType<i128>, CMP>,
                        DecimalType<i128>,
                        DecimalType<i128>,
                    >::try_create_unary(
                        display_name, return_type, params, data_type
                    )
                }
                DataType::Decimal(DecimalDataType::Decimal256(s)) => {
                    let decimal_size = DecimalSize {
                        precision: s.precision,
                        scale: s.scale,
                    };
                    let return_type = DataType::Decimal(DecimalDataType::from_size(decimal_size)?);
                    AggregateUnaryFunction::<
                        MinMaxAnyState<DecimalType<i256>, CMP>,
                        DecimalType<i256>,
                        DecimalType<i256>,
                    >::try_create_unary(
                        display_name, return_type, params, data_type
                    )
                }
                _ => {
                    let return_type = data_type.clone();
                    let func = AggregateUnaryFunction::<
                        MinMaxAnyState<AnyType, CMP>,
                        AnyType,
                        AnyType,
                    >::try_create(
                        display_name, return_type, params, data_type
                    )
                    .with_need_drop(need_drop);

                    Ok(Arc::new(func))
                }
            })
        }
        _ => Err(ErrorCode::BadDataValueType(format!(
            "Unsupported compare type for aggregate function {} (type number: {})",
            display_name, CMP_TYPE
        ))),
    })
}

pub fn aggregate_min_function_desc() -> AggregateFunctionDescription {
    let features = super::aggregate_function_factory::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_min_max_any_function::<TYPE_MIN>),
        features,
    )
}

pub fn aggregate_max_function_desc() -> AggregateFunctionDescription {
    let features = super::aggregate_function_factory::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_min_max_any_function::<TYPE_MAX>),
        features,
    )
}

pub fn aggregate_any_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(
        try_create_aggregate_min_max_any_function::<TYPE_ANY>,
    ))
}
