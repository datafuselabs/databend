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

use core::ops::Range;
use std::sync::Arc;

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_exception::Result;

use crate::kernels::take::BIT_MASK;
use crate::kernels::utils::copy_advance_aligned;
use crate::kernels::utils::set_vec_len_by_ptr;
use crate::kernels::utils::store_advance_aligned;
use crate::types::array::ArrayColumn;
use crate::types::array::ArrayColumnBuilder;
use crate::types::binary::BinaryColumn;
use crate::types::decimal::DecimalColumn;
use crate::types::map::KvColumnBuilder;
use crate::types::nullable::NullableColumn;
use crate::types::number::NumberColumn;
use crate::types::string::StringColumn;
use crate::types::AnyType;
use crate::types::ArgType;
use crate::types::ArrayType;
use crate::types::GeographyType;
use crate::types::MapType;
use crate::types::ValueType;
use crate::with_decimal_type;
use crate::with_number_type;
use crate::BlockEntry;
use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::Value;

impl DataBlock {
    // Generate a new `DataBlock` by the specified indices ranges.
    pub fn take_ranges(self, ranges: &[Range<u32>], num_rows: usize) -> Result<DataBlock> {
        debug_assert_eq!(
            ranges
                .iter()
                .map(|range| range.end - range.start)
                .sum::<u32>() as usize,
            num_rows
        );

        let columns = self
            .columns()
            .iter()
            .map(|entry| match &entry.value {
                Value::Column(c) => {
                    let value = Value::Column(Column::take_ranges(c, ranges, num_rows));
                    BlockEntry::new(entry.data_type.clone(), value)
                }
                _ => entry.clone(),
            })
            .collect();
        Ok(DataBlock::new(columns, num_rows))
    }
}

impl Column {
    // Generate a new `Column` by the specified indices ranges.
    fn take_ranges(&self, ranges: &[Range<u32>], num_rows: usize) -> Column {
        match self {
            Column::Null { .. } => Column::Null { len: num_rows },
            Column::EmptyArray { .. } => Column::EmptyArray { len: num_rows },
            Column::EmptyMap { .. } => Column::EmptyMap { len: num_rows },
            Column::Number(column) => with_number_type!(|NUM_TYPE| match column {
                NumberColumn::NUM_TYPE(values) => {
                    Column::Number(NumberColumn::NUM_TYPE(Self::take_ranges_primitive_types(
                        values, ranges, num_rows,
                    )))
                }
            }),
            Column::Decimal(column) => with_decimal_type!(|DECIMAL_TYPE| match column {
                DecimalColumn::DECIMAL_TYPE(values, size) => {
                    Column::Decimal(DecimalColumn::DECIMAL_TYPE(
                        Self::take_ranges_primitive_types(values, ranges, num_rows),
                        *size,
                    ))
                }
            }),
            Column::Boolean(bm) => {
                let column = Self::take_ranges_boolean_types(bm, ranges, num_rows);
                Column::Boolean(column)
            }
            Column::Binary(column) => {
                let column = Self::take_ranges_binary_types(column, ranges, num_rows);
                Column::Binary(column)
            }
            Column::String(column) => {
                let column = Self::take_ranges_string_types(column, ranges, num_rows);
                Column::String(column)
            }
            Column::Timestamp(column) => {
                let ts = Self::take_ranges_primitive_types(column, ranges, num_rows);
                Column::Timestamp(ts)
            }
            Column::Date(column) => {
                let d = Self::take_ranges_primitive_types(column, ranges, num_rows);
                Column::Date(d)
            }
            Column::Array(column) => {
                let mut offsets = Vec::with_capacity(num_rows + 1);
                offsets.push(0);
                let builder = ColumnBuilder::with_capacity(&column.values.data_type(), num_rows);
                let builder = ArrayColumnBuilder { builder, offsets };
                Self::take_ranges_scalar_types::<ArrayType<AnyType>>(
                    column, builder, ranges, num_rows,
                )
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
                Self::take_ranges_scalar_types::<MapType<AnyType, AnyType>>(
                    &column, builder, ranges, num_rows,
                )
            }
            Column::Bitmap(column) => {
                let column = Self::take_ranges_binary_types(column, ranges, num_rows);
                Column::Bitmap(column)
            }

            Column::Nullable(c) => {
                let column = Self::take_ranges(&c.column, ranges, num_rows);
                let validity = Self::take_ranges_boolean_types(&c.validity, ranges, num_rows);
                Column::Nullable(Box::new(NullableColumn { column, validity }))
            }
            Column::Tuple(fields) => {
                let fields = fields
                    .iter()
                    .map(|c| c.take_ranges(ranges, num_rows))
                    .collect();
                Column::Tuple(fields)
            }
            Column::Variant(column) => {
                let column = Self::take_ranges_binary_types(column, ranges, num_rows);
                Column::Variant(column)
            }
            Column::Geometry(column) => {
                let column = Self::take_ranges_binary_types(column, ranges, num_rows);
                Column::Geometry(column)
            }
            Column::Geography(column) => {
                let mut builder = GeographyType::create_builder(num_rows, &[]);
                for range in ranges {
                    GeographyType::append_column(
                        &mut builder,
                        &column.slice(range.start as usize..range.end as usize),
                    )
                }
                GeographyType::upcast_column(GeographyType::build_column(builder))
            }
        }
    }

    fn take_ranges_scalar_types<T: ValueType>(
        col: &T::Column,
        mut builder: T::ColumnBuilder,
        ranges: &[Range<u32>],
        _num_rows: usize,
    ) -> Column {
        for range in ranges {
            for index in range.start as usize..range.end as usize {
                T::push_item(&mut builder, unsafe {
                    T::index_column_unchecked(col, index)
                });
            }
        }
        T::upcast_column(T::build_column(builder))
    }

    fn take_ranges_primitive_types<T: Copy>(
        values: &Buffer<T>,
        ranges: &[Range<u32>],
        num_rows: usize,
    ) -> Buffer<T> {
        let mut builder: Vec<T> = Vec::with_capacity(num_rows);
        for range in ranges {
            builder.extend(&values[range.start as usize..range.end as usize]);
        }
        builder.into()
    }

    fn take_ranges_binary_types(
        values: &BinaryColumn,
        ranges: &[Range<u32>],
        num_rows: usize,
    ) -> BinaryColumn {
        let mut offsets: Vec<u64> = Vec::with_capacity(num_rows + 1);
        let mut data_size = 0;

        let value_data = values.data().as_slice();
        let values_offset = values.offsets().as_slice();
        // Build [`offset`] and calculate `data_size` required by [`data`].
        offsets.push(0);
        for range in ranges {
            let mut offset_start = values_offset[range.start as usize];
            for offset_end in values_offset[range.start as usize + 1..range.end as usize + 1].iter()
            {
                data_size += offset_end - offset_start;
                offset_start = *offset_end;
                offsets.push(data_size);
            }
        }

        // Build [`data`].
        let mut data: Vec<u8> = Vec::with_capacity(data_size as usize);
        let mut data_ptr = data.as_mut_ptr();

        unsafe {
            for range in ranges {
                let col_data = &value_data[values_offset[range.start as usize] as usize
                    ..values_offset[range.end as usize] as usize];
                copy_advance_aligned(col_data.as_ptr(), &mut data_ptr, col_data.len());
            }
            set_vec_len_by_ptr(&mut data, data_ptr);
        }

        BinaryColumn::new(data.into(), offsets.into())
    }

    fn take_ranges_string_types(
        values: &StringColumn,
        ranges: &[Range<u32>],
        num_rows: usize,
    ) -> StringColumn {
        unsafe {
            StringColumn::from_binary_unchecked(Self::take_ranges_binary_types(
                &values.clone().into(),
                ranges,
                num_rows,
            ))
        }
    }

    fn take_ranges_boolean_types(
        bitmap: &Bitmap,
        ranges: &[Range<u32>],
        num_rows: usize,
    ) -> Bitmap {
        let capacity = num_rows.saturating_add(7) / 8;
        let mut builder: Vec<u8> = Vec::with_capacity(capacity);
        let mut builder_ptr = builder.as_mut_ptr();
        let mut builder_idx = 0;
        let mut unset_bits = 0;
        let mut buf = 0;

        let (bitmap_slice, bitmap_offset, _) = bitmap.as_slice();
        unsafe {
            for range in ranges {
                let mut start = range.start as usize;
                let end = range.end as usize;
                if builder_idx % 8 != 0 {
                    while start < end {
                        if bitmap.get_bit_unchecked(start) {
                            buf |= BIT_MASK[builder_idx % 8];
                        } else {
                            unset_bits += 1;
                        }
                        builder_idx += 1;
                        start += 1;
                        if builder_idx % 8 == 0 {
                            store_advance_aligned(buf, &mut builder_ptr);
                            buf = 0;
                            break;
                        }
                    }
                }
                let remaining = end - start;
                if remaining > 0 {
                    let (cur_buf, cur_unset_bits) = Self::copy_continuous_bits(
                        &mut builder_ptr,
                        bitmap_slice,
                        builder_idx,
                        start + bitmap_offset,
                        remaining,
                    );
                    builder_idx += remaining;
                    unset_bits += cur_unset_bits;
                    buf = cur_buf;
                }
            }

            if builder_idx % 8 != 0 {
                store_advance_aligned(buf, &mut builder_ptr);
            }

            set_vec_len_by_ptr(&mut builder, builder_ptr);
            Bitmap::from_inner(Arc::new(builder.into()), 0, num_rows, unset_bits)
                .ok()
                .unwrap()
        }
    }
}
