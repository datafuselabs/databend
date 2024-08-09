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

use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_exception::Result;

use crate::kernels::utils::copy_advance_aligned;
use crate::kernels::utils::set_vec_len_by_ptr;
use crate::kernels::utils::store_advance_aligned;
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

impl DataBlock {
    pub fn take_compacted_indices(&self, indices: &[(u32, u32)], num_rows: usize) -> Result<Self> {
        if indices.is_empty() {
            return Ok(self.slice(0..0));
        }

        // Each item in the `indices` consists of an `index` and a `cnt`, the sum
        // of the `cnt` must be equal to the `num_rows`.
        debug_assert_eq!(
            indices.iter().fold(0, |acc, &(_, x)| acc + x as usize),
            num_rows
        );

        let after_columns = self
            .columns()
            .iter()
            .map(|entry| match &entry.value {
                Value::Scalar(s) => BlockEntry {
                    data_type: entry.data_type.clone(),
                    value: Value::Scalar(s.clone()),
                },
                Value::Column(c) => BlockEntry {
                    data_type: entry.data_type.clone(),
                    value: Value::Column(Column::take_compacted_indices(c, indices, num_rows)),
                },
            })
            .collect();

        Ok(DataBlock::new_with_meta(
            after_columns,
            num_rows,
            self.get_meta().cloned(),
        ))
    }
}

impl Column {
    pub fn take_compacted_indices(&self, indices: &[(u32, u32)], num_rows: usize) -> Self {
        match self {
            Column::Null { .. } => Column::Null { len: num_rows },
            Column::EmptyArray { .. } => Column::EmptyArray { len: num_rows },
            Column::EmptyMap { .. } => Column::EmptyMap { len: num_rows },
            Column::Number(column) => with_number_mapped_type!(|NUM_TYPE| match column {
                NumberColumn::NUM_TYPE(values) => {
                    let builder = Self::take_compacted_primitive_types(values, indices, num_rows);
                    <NumberType<NUM_TYPE>>::upcast_column(<NumberType<NUM_TYPE>>::column_from_vec(
                        builder,
                        &[],
                    ))
                }
            }),
            Column::Decimal(column) => with_decimal_type!(|DECIMAL_TYPE| match column {
                DecimalColumn::DECIMAL_TYPE(values, size) => {
                    let builder = Self::take_compacted_primitive_types(values, indices, num_rows);
                    Column::Decimal(DecimalColumn::DECIMAL_TYPE(builder.into(), *size))
                }
            }),
            Column::Boolean(bm) => {
                Self::take_compacted_arg_types::<BooleanType>(bm, indices, num_rows)
            }
            Column::Binary(column) => BinaryType::upcast_column(Self::take_compact_binary_types(
                column, indices, num_rows,
            )),
            Column::String(column) => StringType::upcast_column(Self::take_compact_string_types(
                column, indices, num_rows,
            )),
            Column::Timestamp(column) => {
                let builder = Self::take_compacted_primitive_types(column, indices, num_rows);
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
                let builder = Self::take_compacted_primitive_types(column, indices, num_rows);
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
            Column::Geography(column) => {
                Self::take_compacted_arg_types::<GeographyType>(column, indices, num_rows)
            }
            Column::Array(column) => {
                let mut offsets = Vec::with_capacity(num_rows + 1);
                offsets.push(0);
                let builder = ColumnBuilder::with_capacity(&column.values.data_type(), num_rows);
                let builder = ArrayColumnBuilder { builder, offsets };
                Self::take_compacted_value_types::<ArrayType<AnyType>>(column, builder, indices)
            }
            Column::Map(column) => {
                let mut offsets = Vec::with_capacity(num_rows + 1);
                offsets.push(0);
                let builder = ColumnBuilder::from_column(
                    ColumnBuilder::with_capacity(&column.values.data_type(), num_rows).build(),
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
                Self::take_compacted_value_types::<MapType<AnyType, AnyType>>(
                    &column, builder, indices,
                )
            }
            Column::Bitmap(column) => BitmapType::upcast_column(Self::take_compact_binary_types(
                column, indices, num_rows,
            )),
            Column::Nullable(c) => {
                let column = c.column.take_compacted_indices(indices, num_rows);
                let validity =
                    Self::take_compacted_arg_types::<BooleanType>(&c.validity, indices, num_rows);
                Column::Nullable(Box::new(NullableColumn {
                    column,
                    validity: BooleanType::try_downcast_column(&validity).unwrap(),
                }))
            }
            Column::Tuple(fields) => {
                let fields = fields
                    .iter()
                    .map(|c| c.take_compacted_indices(indices, num_rows))
                    .collect();
                Column::Tuple(fields)
            }
            Column::Variant(column) => VariantType::upcast_column(Self::take_compact_binary_types(
                column, indices, num_rows,
            )),
            Column::Geometry(column) => GeometryType::upcast_column(
                Self::take_compact_binary_types(column, indices, num_rows),
            ),
        }
    }

    pub fn take_compacted_primitive_types<T>(
        col: &Buffer<T>,
        indices: &[(u32, u32)],
        num_rows: usize,
    ) -> Vec<T>
    where
        T: Copy,
    {
        let col_ptr = col.as_slice().as_ptr();
        let mut builder: Vec<T> = Vec::with_capacity(num_rows);
        let mut ptr = builder.as_mut_ptr();
        let mut remain;

        unsafe {
            for (index, cnt) in indices.iter() {
                if *cnt == 1 {
                    copy_advance_aligned(col_ptr.add(*index as usize), &mut ptr, 1);
                    continue;
                }

                // Using the doubling method to copy the max segment memory.
                // [___________] => [x__________] => [xx_________] => [xxxx_______] => [xxxxxxxx___]
                // Since cnt > 0, then 31 - cnt.leading_zeros() >= 0.
                let max_segment = 1 << (31 - cnt.leading_zeros());
                let base_ptr = ptr;
                copy_advance_aligned(col_ptr.add(*index as usize), &mut ptr, 1);
                let mut cur_segment = 1;
                while cur_segment < max_segment {
                    copy_advance_aligned(base_ptr, &mut ptr, cur_segment);
                    cur_segment <<= 1;
                }

                // Copy the remaining memory directly.
                // [xxxxxxxxxx____] => [xxxxxxxxxxxxxx]
                //  ^^^^ ---> ^^^^
                remain = *cnt as usize - max_segment;
                if remain > 0 {
                    copy_advance_aligned(base_ptr, &mut ptr, remain);
                }
            }
            set_vec_len_by_ptr(&mut builder, ptr);
        }

        builder
    }

    pub fn take_compact_binary_types(
        col: &BinaryColumn,
        indices: &[(u32, u32)],
        num_rows: usize,
    ) -> BinaryColumn {
        // Each element of `items` is (string(&[u8]), repeat times).
        let mut items = Vec::with_capacity(indices.len());
        let mut items_ptr = items.as_mut_ptr();

        // [`BinaryColumn`] consists of [`data`] and [`offset`], we build [`data`] and [`offset`] respectively,
        // and then call `BinaryColumn::new(data.into(), offsets.into())` to create [`BinaryColumn`].
        let mut offsets = Vec::with_capacity(num_rows + 1);
        let mut offsets_ptr = offsets.as_mut_ptr();
        let mut data_size = 0;

        // Build [`offset`] and calculate `data_size` required by [`data`].
        unsafe {
            store_advance_aligned::<u64>(0, &mut offsets_ptr);
            for (index, cnt) in indices.iter() {
                let item = col.index_unchecked(*index as usize);
                store_advance_aligned((item, *cnt), &mut items_ptr);
                for _ in 0..*cnt {
                    data_size += item.len() as u64;
                    store_advance_aligned(data_size, &mut offsets_ptr);
                }
            }
            set_vec_len_by_ptr(&mut offsets, offsets_ptr);
            set_vec_len_by_ptr(&mut items, items_ptr);
        }

        // Build [`data`].
        let mut data: Vec<u8> = Vec::with_capacity(data_size as usize);
        let mut data_ptr = data.as_mut_ptr();
        let mut remain;

        unsafe {
            for (item, cnt) in items {
                let len = item.len();
                if cnt == 1 {
                    copy_advance_aligned(item.as_ptr(), &mut data_ptr, len);
                    continue;
                }

                // Using the doubling method to copy the max segment memory.
                // [___________] => [x__________] => [xx_________] => [xxxx_______] => [xxxxxxxx___]
                // Since cnt > 0, then 31 - cnt.leading_zeros() >= 0.
                let max_bit_num = 1 << (31 - cnt.leading_zeros());
                let max_segment = max_bit_num * len;
                let base_data_ptr = data_ptr;
                copy_advance_aligned(item.as_ptr(), &mut data_ptr, len);
                let mut cur_segment = len;
                while cur_segment < max_segment {
                    copy_advance_aligned(base_data_ptr, &mut data_ptr, cur_segment);
                    cur_segment <<= 1;
                }

                // Copy the remaining memory directly.
                // [xxxxxxxxxx____] => [xxxxxxxxxxxxxx]
                //  ^^^^ ---> ^^^^
                remain = cnt as usize - max_bit_num;
                if remain > 0 {
                    copy_advance_aligned(base_data_ptr, &mut data_ptr, remain * len);
                }
            }
            set_vec_len_by_ptr(&mut data, data_ptr);
        }

        BinaryColumn::new(data.into(), offsets.into())
    }

    pub fn take_compact_string_types(
        col: &StringColumn,
        indices: &[(u32, u32)],
        num_rows: usize,
    ) -> StringColumn {
        unsafe {
            StringColumn::from_binary_unchecked(Self::take_compact_binary_types(
                &col.clone().into(),
                indices,
                num_rows,
            ))
        }
    }

    fn take_compacted_arg_types<T: ArgType>(
        col: &T::Column,
        indices: &[(u32, u32)],
        num_rows: usize,
    ) -> Column {
        let mut builder = T::create_builder(num_rows, &[]);
        for (index, cnt) in indices {
            for _ in 0..*cnt {
                T::push_item(&mut builder, unsafe {
                    T::index_column_unchecked(col, *index as usize)
                });
            }
        }
        let column = T::build_column(builder);
        T::upcast_column(column)
    }

    fn take_compacted_value_types<T: ValueType>(
        col: &T::Column,
        mut builder: T::ColumnBuilder,
        indices: &[(u32, u32)],
    ) -> Column {
        for (index, cnt) in indices {
            for _ in 0..*cnt {
                T::push_item(&mut builder, unsafe {
                    T::index_column_unchecked(col, *index as usize)
                });
            }
        }
        T::upcast_column(T::build_column(builder))
    }
}
