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

use std::alloc::Layout;
use std::any::Any;
use std::fmt::Display;
use std::fmt::Formatter;
use std::marker::PhantomData;
use std::sync::Arc;

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_base::base::take_mut;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::ValueType;
use databend_common_expression::AggregateFunction;
use databend_common_expression::AggregateFunctionRef;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::InputColumns;
use databend_common_expression::Scalar;
use databend_common_expression::StateAddr;

pub trait UnaryState<T, R>:
    Send + Sync + Default + borsh::BorshSerialize + borsh::BorshDeserialize
where
    T: ValueType,
    R: ValueType,
{
    fn add(
        &mut self,
        other: T::ScalarRef<'_>,
        function_data: Option<&dyn FunctionData>,
    ) -> Result<()>;

    fn add_batch(
        &mut self,
        other: T::Column,
        validity: Option<&Bitmap>,
        function_data: Option<&dyn FunctionData>,
    ) -> Result<()> {
        match validity {
            Some(validity) => {
                for (data, valid) in T::iter_column(&other).zip(validity.iter()) {
                    if valid {
                        self.add(data, function_data)?;
                    }
                }
            }
            None => {
                for value in T::iter_column(&other) {
                    self.add(value, function_data)?;
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()>;

    fn merge_result(
        &mut self,
        builder: &mut R::ColumnBuilder,
        function_data: Option<&dyn FunctionData>,
    ) -> Result<()>;
}

pub trait FunctionData: Send + Sync {
    fn as_any(&self) -> &dyn Any;
}

pub struct AggregateUnaryFunction<S, T, R>
where
    S: UnaryState<T, R>,
    T: ValueType,
    R: ValueType,
{
    display_name: String,
    _params: Vec<Scalar>,
    _argument: DataType,
    return_type: DataType,
    function_data: Option<Box<dyn FunctionData>>,
    need_drop: bool,
    _phantom: PhantomData<(S, T, R)>,
}

impl<S, T, R> Display for AggregateUnaryFunction<S, T, R>
where
    S: UnaryState<T, R>,
    T: ValueType,
    R: ValueType,
{
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<S, T, R> AggregateUnaryFunction<S, T, R>
where
    S: UnaryState<T, R> + 'static,
    T: Send + Sync + ValueType,
    R: Send + Sync + ValueType,
{
    pub(crate) fn try_create_unary(
        display_name: &str,
        return_type: DataType,
        _params: Vec<Scalar>,
        _argument: DataType,
    ) -> Result<AggregateFunctionRef> {
        Ok(Arc::new(Self::try_create(
            display_name,
            return_type,
            _params,
            _argument,
        )))
    }

    pub(crate) fn try_create(
        display_name: &str,
        return_type: DataType,
        _params: Vec<Scalar>,
        _argument: DataType,
    ) -> AggregateUnaryFunction<S, T, R> {
        AggregateUnaryFunction {
            display_name: display_name.to_string(),
            return_type,
            _params,
            _argument,
            function_data: None,
            need_drop: false,
            _phantom: Default::default(),
        }
    }

    pub(crate) fn with_function_data(
        mut self,
        function_data: Box<dyn FunctionData>,
    ) -> AggregateUnaryFunction<S, T, R> {
        self.function_data = Some(function_data);
        self
    }

    pub(crate) fn with_need_drop(mut self, need_drop: bool) -> AggregateUnaryFunction<S, T, R> {
        self.need_drop = need_drop;
        self
    }

    fn do_merge_result(&self, state: &mut S, builder: &mut ColumnBuilder) -> Result<()> {
        let decimal_size = check_decimal(builder);
        // some `ValueType` like `NullableType` need ownership to downcast builder,
        // so here we using an unsafe way to take the ownership of builder.
        // See [`take_mut`] for details.
        if let Some(builder) = R::try_downcast_builder(builder) {
            state.merge_result(builder, self.function_data.as_deref())
        } else {
            take_mut(builder, |builder| {
                let mut builder = R::try_downcast_owned_builder(builder).unwrap();
                let res = state.merge_result(&mut builder, self.function_data.as_deref());

                (
                    res,
                    R::try_upcast_column_builder(builder, decimal_size).unwrap(),
                )
            })
        }
    }
}

fn check_decimal(builder: &ColumnBuilder) -> Option<DecimalSize> {
    match builder {
        ColumnBuilder::Decimal(b) => Some(b.decimal_size()),
        ColumnBuilder::Array(box b) => check_decimal(&b.builder),
        ColumnBuilder::Nullable(box b) => check_decimal(&b.builder),
        ColumnBuilder::Map(box b) => check_decimal(&b.builder),
        _ => None,
    }
}

impl<S, T, R> AggregateFunction for AggregateUnaryFunction<S, T, R>
where
    S: UnaryState<T, R> + 'static,
    T: Send + Sync + ValueType,
    R: Send + Sync + ValueType,
{
    fn name(&self) -> &str {
        &self.display_name
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn init_state(&self, place: StateAddr) {
        place.write_state(S::default())
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<S>()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: InputColumns,
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let column = T::try_downcast_column(&columns[0]).unwrap();
        let state: &mut S = place.get::<S>();

        state.add_batch(column, validity, self.function_data.as_deref())
    }

    fn accumulate_row(&self, place: StateAddr, columns: InputColumns, row: usize) -> Result<()> {
        let column = T::try_downcast_column(&columns[0]).unwrap();
        let value = T::index_column(&column, row);

        let state: &mut S = place.get::<S>();
        state.add(value.unwrap(), self.function_data.as_deref())?;
        Ok(())
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: InputColumns,
        _input_rows: usize,
    ) -> Result<()> {
        let column = T::try_downcast_column(&columns[0]).unwrap();

        for (i, place) in places.iter().enumerate() {
            let state: &mut S = place.next(offset).get::<S>();
            state.add(
                T::index_column(&column, i).unwrap(),
                self.function_data.as_deref(),
            )?;
        }

        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state: &mut S = place.get::<S>();
        Ok(borsh::to_writer(writer, state)?)
    }

    fn merge(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state: &mut S = place.get::<S>();
        let rhs = S::deserialize_reader(reader)?;
        state.merge(&rhs)
    }

    fn merge_states(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state: &mut S = place.get::<S>();
        let other: &mut S = rhs.get::<S>();
        state.merge(other)
    }

    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        let state: &mut S = place.get::<S>();
        self.do_merge_result(state, builder)
    }

    fn batch_merge_result(
        &self,
        places: &[StateAddr],
        offset: usize,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        for place in places {
            let state: &mut S = place.next(offset).get::<S>();
            self.do_merge_result(state, builder)?;
        }
        Ok(())
    }

    fn need_manual_drop_state(&self) -> bool {
        self.need_drop
    }

    unsafe fn drop_state(&self, place: StateAddr) {
        let state = place.get::<S>();
        std::ptr::drop_in_place(state);
    }
}
