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

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_exception::Result;

use crate::kernels::utils::copy_advance_aligned;
use crate::kernels::utils::set_vec_len_by_ptr;
use crate::types::array::ArrayColumn;
use crate::types::array::ArrayColumnBuilder;
use crate::types::binary::BinaryColumn;
use crate::types::bitmap::BitmapType;
use crate::types::decimal::DecimalColumn;
use crate::types::geometry::GeometryType;
use crate::types::map::KvColumnBuilder;
use crate::types::nullable::NullableColumn;
use crate::types::number::NumberColumn;
use crate::types::string::StringColumn;
use crate::types::AnyType;
use crate::types::ArgType;
use crate::types::ArrayType;
use crate::types::BinaryType;
use crate::types::BooleanType;
use crate::types::GeographyType;
use crate::types::MapType;
use crate::types::NumberType;
use crate::types::StringType;
use crate::types::ValueType;
use crate::types::VariantType;
use crate::with_decimal_type;
use crate::with_number_mapped_type;
use crate::BlockEntry;
use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::Value;

pub const BIT_MASK: [u8; 8] = [1, 2, 4, 8, 16, 32, 64, 128];

impl DataBlock {
    pub fn take<I>(
        &self,
        indices: &[I],
        string_items_buf: &mut Option<Vec<(u64, usize)>>,
    ) -> Result<Self>
    where
        I: databend_common_arrow::arrow::types::Index,
    {
        if indices.is_empty() {
            return Ok(self.slice(0..0));
        }

        let after_columns = self
            .columns()
            .iter()
            .map(|entry| match &entry.value {
                Value::Scalar(s) => {
                    BlockEntry::new(entry.data_type.clone(), Value::Scalar(s.clone()))
                }
                Value::Column(c) => BlockEntry::new(
                    entry.data_type.clone(),
                    Value::Column(Column::take(c, indices, string_items_buf)),
                ),
            })
            .collect();

        Ok(DataBlock::new_with_meta(
            after_columns,
            indices.len(),
            self.get_meta().cloned(),
        ))
    }
}

impl Column {
    pub fn take<I>(&self, indices: &[I], string_items_buf: &mut Option<Vec<(u64, usize)>>) -> Self
    where I: databend_common_arrow::arrow::types::Index {
        match self {
            Column::Null { .. } => Column::Null { len: indices.len() },
            Column::EmptyArray { .. } => Column::EmptyArray { len: indices.len() },
            Column::EmptyMap { .. } => Column::EmptyMap { len: indices.len() },
            Column::Number(column) => with_number_mapped_type!(|NUM_TYPE| match column {
                NumberColumn::NUM_TYPE(values) => {
                    let builder = Self::take_primitive_types(values, indices);
                    <NumberType<NUM_TYPE>>::upcast_column(<NumberType<NUM_TYPE>>::column_from_vec(
                        builder,
                        &[],
                    ))
                }
            }),
            Column::Decimal(column) => with_decimal_type!(|DECIMAL_TYPE| match column {
                DecimalColumn::DECIMAL_TYPE(values, size) => {
                    let builder = Self::take_primitive_types(values, indices);
                    Column::Decimal(DecimalColumn::DECIMAL_TYPE(builder.into(), *size))
                }
            }),
            Column::Boolean(bm) => Column::Boolean(Self::take_boolean_types(bm, indices)),
            Column::Binary(column) => BinaryType::upcast_column(Self::take_binary_types(
                column,
                indices,
                string_items_buf.as_mut(),
            )),
            Column::String(column) => StringType::upcast_column(Self::take_string_types(
                column,
                indices,
                string_items_buf.as_mut(),
            )),
            Column::Timestamp(column) => {
                let builder = Self::take_primitive_types(column, indices);
                let ts = <NumberType<i64>>::upcast_column(<NumberType<i64>>::column_from_vec(
                    builder,
                    &[],
                ))
                .into_number()
                .unwrap()
                .into_int64()
                .unwrap();
                Column::Timestamp(ts)
            }
            Column::Date(column) => {
                let builder = Self::take_primitive_types(column, indices);
                let d = <NumberType<i32>>::upcast_column(<NumberType<i32>>::column_from_vec(
                    builder,
                    &[],
                ))
                .into_number()
                .unwrap()
                .into_int32()
                .unwrap();
                Column::Date(d)
            }
            Column::Array(column) => {
                let mut offsets = Vec::with_capacity(indices.len() + 1);
                offsets.push(0);
                let builder = ColumnBuilder::with_capacity(&column.values.data_type(), self.len());
                let builder = ArrayColumnBuilder { builder, offsets };
                Self::take_value_types::<ArrayType<AnyType>, _>(column, builder, indices)
            }
            Column::Map(column) => {
                let mut offsets = Vec::with_capacity(indices.len() + 1);
                offsets.push(0);
                let builder = ColumnBuilder::from_column(
                    ColumnBuilder::with_capacity(&column.values.data_type(), self.len()).build(),
                );
                let (key_builder, val_builder) = match builder {
                    ColumnBuilder::Tuple(fields) => (fields[0].clone(), fields[1].clone()),
                    _ => unreachable!(),
                };
                let builder = KvColumnBuilder {
                    keys: key_builder,
                    values: val_builder,
                };
                let builder = ArrayColumnBuilder { builder, offsets };
                let column = ArrayColumn::try_downcast(column).unwrap();
                Self::take_value_types::<MapType<AnyType, AnyType>, _>(&column, builder, indices)
            }
            Column::Bitmap(column) => BitmapType::upcast_column(Self::take_binary_types(
                column,
                indices,
                string_items_buf.as_mut(),
            )),
            Column::Nullable(c) => {
                let column = c.column.take(indices, string_items_buf);
                let validity = Column::Boolean(Self::take_boolean_types(&c.validity, indices));
                Column::Nullable(Box::new(NullableColumn {
                    column,
                    validity: BooleanType::try_downcast_column(&validity).unwrap(),
                }))
            }
            Column::Tuple(fields) => {
                let fields = fields
                    .iter()
                    .map(|c| c.take(indices, string_items_buf))
                    .collect();
                Column::Tuple(fields)
            }
            Column::Variant(column) => VariantType::upcast_column(Self::take_binary_types(
                column,
                indices,
                string_items_buf.as_mut(),
            )),
            Column::Geometry(column) => GeometryType::upcast_column(Self::take_binary_types(
                column,
                indices,
                string_items_buf.as_mut(),
            )),
            Column::Geography(column) => {
                let num_rows = indices.len();
                let mut builder = GeographyType::create_builder(num_rows, &[]);
                for index in indices.iter() {
                    let data = unsafe { column.index_unchecked(index.to_usize()) };
                    builder.push(data);
                }
                GeographyType::upcast_column(GeographyType::build_column(builder))
            }
        }
    }

    pub fn take_primitive_types<T, I>(col: &Buffer<T>, indices: &[I]) -> Vec<T>
    where
        T: Copy,
        I: databend_common_arrow::arrow::types::Index,
    {
        let num_rows = indices.len();
        let mut builder: Vec<T> = Vec::with_capacity(num_rows);
        let col = col.as_slice();
        builder.extend(
            indices
                .iter()
                .map(|index| unsafe { *col.get_unchecked(index.to_usize()) }),
        );
        builder
    }

    pub fn take_binary_types<I>(
        col: &BinaryColumn,
        indices: &[I],
        string_items_buf: Option<&mut Vec<(u64, usize)>>,
    ) -> BinaryColumn
    where
        I: databend_common_arrow::arrow::types::Index,
    {
        let num_rows = indices.len();

        // Each element of `items` is (string pointer(u64), string length), if `string_items_buf`
        // can be reused, we will not re-allocate memory.
        let mut items: Option<Vec<(u64, usize)>> = match &string_items_buf {
            Some(string_items_buf) if string_items_buf.capacity() >= num_rows => None,
            _ => Some(Vec::with_capacity(num_rows)),
        };
        let items = match items.is_some() {
            true => items.as_mut().unwrap(),
            false => string_items_buf.unwrap(),
        };

        // [`BinaryColumn`] consists of [`data`] and [`offset`], we build [`data`] and [`offset`] respectively,
        // and then call `BinaryColumn::new(data.into(), offsets.into())` to create [`BinaryColumn`].
        let col_offset = col.offsets().as_slice();
        let col_data_ptr = col.data().as_slice().as_ptr();
        let mut offsets: Vec<u64> = Vec::with_capacity(num_rows + 1);
        let mut data_size = 0;

        // Build [`offset`] and calculate `data_size` required by [`data`].
        unsafe {
            items.set_len(num_rows);
            offsets.set_len(num_rows + 1);
            *offsets.get_unchecked_mut(0) = 0;
            for (i, index) in indices.iter().enumerate() {
                let start = *col_offset.get_unchecked(index.to_usize()) as usize;
                let len = *col_offset.get_unchecked(index.to_usize() + 1) as usize - start;
                data_size += len as u64;
                *items.get_unchecked_mut(i) = (col_data_ptr.add(start) as u64, len);
                *offsets.get_unchecked_mut(i + 1) = data_size;
            }
        }

        // Build [`data`].
        let mut data: Vec<u8> = Vec::with_capacity(data_size as usize);
        let mut data_ptr = data.as_mut_ptr();

        unsafe {
            for (str_ptr, len) in items.iter() {
                copy_advance_aligned(*str_ptr as *const u8, &mut data_ptr, *len);
            }
            set_vec_len_by_ptr(&mut data, data_ptr);
        }

        BinaryColumn::new(data.into(), offsets.into())
    }

    pub fn take_string_types<I>(
        col: &StringColumn,
        indices: &[I],
        string_items_buf: Option<&mut Vec<(u64, usize)>>,
    ) -> StringColumn
    where
        I: databend_common_arrow::arrow::types::Index,
    {
        unsafe {
            StringColumn::from_binary_unchecked(Self::take_binary_types(
                &col.clone().into(),
                indices,
                string_items_buf,
            ))
        }
    }

    pub fn take_boolean_types<I>(col: &Bitmap, indices: &[I]) -> Bitmap
    where I: databend_common_arrow::arrow::types::Index {
        let num_rows = indices.len();
        // Fast path: avoid iterating column to generate a new bitmap.
        // If this [`Bitmap`] is all true or all false and `num_rows <= bitmap.len()``,
        // we can just slice it.
        if num_rows <= col.len() && (col.unset_bits() == 0 || col.unset_bits() == col.len()) {
            let mut bitmap = col.clone();
            bitmap.slice(0, num_rows);
            return bitmap;
        }

        let capacity = num_rows.saturating_add(7) / 8;
        let mut builder: Vec<u8> = Vec::with_capacity(capacity);
        let mut unset_bits = 0;
        let mut value = 0;
        let mut i = 0;

        for index in indices.iter() {
            if col.get_bit(index.to_usize()) {
                value |= BIT_MASK[i % 8];
            } else {
                unset_bits += 1;
            }
            i += 1;
            if i % 8 == 0 {
                builder.push(value);
                value = 0;
            }
        }
        if i % 8 != 0 {
            builder.push(value);
        }

        unsafe {
            Bitmap::from_inner(Arc::new(builder.into()), 0, num_rows, unset_bits)
                .ok()
                .unwrap()
        }
    }

    fn take_value_types<T: ValueType, I>(
        col: &T::Column,
        mut builder: T::ColumnBuilder,
        indices: &[I],
    ) -> Column
    where
        I: databend_common_arrow::arrow::types::Index,
    {
        for index in indices {
            T::push_item(&mut builder, unsafe {
                T::index_column_unchecked(col, index.to_usize())
            });
        }
        T::upcast_column(T::build_column(builder))
    }
}
