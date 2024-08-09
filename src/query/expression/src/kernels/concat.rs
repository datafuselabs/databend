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

use std::iter::TrustedLen;
use std::sync::Arc;

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::buffer::Buffer;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use itertools::Itertools;

use crate::kernels::take::BIT_MASK;
use crate::kernels::utils::copy_advance_aligned;
use crate::kernels::utils::set_vec_len_by_ptr;
use crate::store_advance_aligned;
use crate::types::array::ArrayColumnBuilder;
use crate::types::binary::BinaryColumn;
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
use crate::types::BitmapType;
use crate::types::BooleanType;
use crate::types::GeographyType;
use crate::types::MapType;
use crate::types::NumberType;
use crate::types::StringType;
use crate::types::ValueType;
use crate::types::VariantType;
use crate::types::F32;
use crate::types::F64;
use crate::with_decimal_type;
use crate::with_number_mapped_type;
use crate::BlockEntry;
use crate::Column;
use crate::ColumnBuilder;
use crate::DataBlock;
use crate::Value;

impl DataBlock {
    pub fn concat(blocks: &[DataBlock]) -> Result<DataBlock> {
        if blocks.is_empty() {
            return Err(ErrorCode::EmptyData("Can't concat empty blocks"));
        }
        let block_refs = blocks.iter().collect::<Vec<_>>();

        if blocks.len() == 1 {
            return Ok(blocks[0].clone());
        }

        let num_columns = blocks[0].num_columns();
        let mut concat_columns = Vec::with_capacity(num_columns);
        for i in 0..num_columns {
            concat_columns.push(BlockEntry::new(
                blocks[0].get_by_offset(i).data_type.clone(),
                Self::concat_columns(&block_refs, i)?,
            ))
        }

        let num_rows = blocks.iter().map(|c| c.num_rows()).sum();

        Ok(DataBlock::new(concat_columns, num_rows))
    }

    pub fn concat_columns(blocks: &[&DataBlock], column_index: usize) -> Result<Value<AnyType>> {
        debug_assert!(
            blocks
                .iter()
                .map(|block| &block.get_by_offset(column_index).data_type)
                .all_equal()
        );

        let entry0 = blocks[0].get_by_offset(column_index);
        if matches!(entry0.value, Value::Scalar(_))
            && blocks
                .iter()
                .all(|b| b.get_by_offset(column_index) == entry0)
        {
            return Ok(entry0.value.clone());
        }

        let columns_iter = blocks.iter().map(|block| {
            let entry = &block.get_by_offset(column_index);
            match &entry.value {
                Value::Scalar(s) => {
                    ColumnBuilder::repeat(&s.as_ref(), block.num_rows(), &entry.data_type).build()
                }
                Value::Column(c) => c.clone(),
            }
        });
        Ok(Value::Column(Column::concat_columns(columns_iter)?))
    }
}

impl Column {
    pub fn concat_columns<I: Iterator<Item = Column> + TrustedLen + Clone>(
        columns: I,
    ) -> Result<Column> {
        let (_, size) = columns.size_hint();
        match size {
            Some(0) => Err(ErrorCode::EmptyData("Can't concat empty columns")),
            _ => Self::concat_columns_impl(columns),
        }
    }

    pub fn concat_columns_impl<I: Iterator<Item = Column> + TrustedLen + Clone>(
        columns: I,
    ) -> Result<Column> {
        let mut columns_iter_clone = columns.clone();
        let first_column = match columns_iter_clone.next() {
            // Even if `columns.size_hint()`'s upper bound is `Some(a)` (a != 0),
            // it's possible that `columns`'s len is 0.
            None => return Err(ErrorCode::EmptyData("Can't concat empty columns")),
            Some(col) => col,
        };
        let capacity = columns_iter_clone.fold(first_column.len(), |acc, x| acc + x.len());
        let column = match first_column {
            Column::Null { .. } => Column::Null { len: capacity },
            Column::EmptyArray { .. } => Column::EmptyArray { len: capacity },
            Column::EmptyMap { .. } => Column::EmptyMap { len: capacity },
            Column::Number(col) => with_number_mapped_type!(|NUM_TYPE| match col {
                NumberColumn::UInt8(_) => {
                    let builder = Self::concat_primitive_types(
                        columns.map(|col| col.into_number().unwrap().into_u_int8().unwrap()),
                        capacity,
                    );
                    <NumberType<u8>>::upcast_column(<NumberType<u8>>::column_from_vec(builder, &[]))
                }
                NumberColumn::UInt16(_) => {
                    let builder = Self::concat_primitive_types(
                        columns.map(|col| col.into_number().unwrap().into_u_int16().unwrap()),
                        capacity,
                    );
                    <NumberType<u16>>::upcast_column(<NumberType<u16>>::column_from_vec(
                        builder,
                        &[],
                    ))
                }
                NumberColumn::UInt32(_) => {
                    let builder = Self::concat_primitive_types(
                        columns.map(|col| col.into_number().unwrap().into_u_int32().unwrap()),
                        capacity,
                    );
                    <NumberType<u32>>::upcast_column(<NumberType<u32>>::column_from_vec(
                        builder,
                        &[],
                    ))
                }
                NumberColumn::UInt64(_) => {
                    let builder = Self::concat_primitive_types(
                        columns.map(|col| col.into_number().unwrap().into_u_int64().unwrap()),
                        capacity,
                    );
                    <NumberType<u64>>::upcast_column(<NumberType<u64>>::column_from_vec(
                        builder,
                        &[],
                    ))
                }
                NumberColumn::Int8(_) => {
                    let builder = Self::concat_primitive_types(
                        columns.map(|col| col.into_number().unwrap().into_int8().unwrap()),
                        capacity,
                    );
                    <NumberType<i8>>::upcast_column(<NumberType<i8>>::column_from_vec(builder, &[]))
                }
                NumberColumn::Int16(_) => {
                    let builder = Self::concat_primitive_types(
                        columns.map(|col| col.into_number().unwrap().into_int16().unwrap()),
                        capacity,
                    );
                    <NumberType<i16>>::upcast_column(<NumberType<i16>>::column_from_vec(
                        builder,
                        &[],
                    ))
                }
                NumberColumn::Int32(_) => {
                    let builder = Self::concat_primitive_types(
                        columns.map(|col| col.into_number().unwrap().into_int32().unwrap()),
                        capacity,
                    );
                    <NumberType<i32>>::upcast_column(<NumberType<i32>>::column_from_vec(
                        builder,
                        &[],
                    ))
                }
                NumberColumn::Int64(_) => {
                    let builder = Self::concat_primitive_types(
                        columns.map(|col| col.into_number().unwrap().into_int64().unwrap()),
                        capacity,
                    );
                    <NumberType<i64>>::upcast_column(<NumberType<i64>>::column_from_vec(
                        builder,
                        &[],
                    ))
                }
                NumberColumn::Float32(_) => {
                    let builder = Self::concat_primitive_types(
                        columns.map(|col| col.into_number().unwrap().into_float32().unwrap()),
                        capacity,
                    );
                    <NumberType<F32>>::upcast_column(<NumberType<F32>>::column_from_vec(
                        builder,
                        &[],
                    ))
                }
                NumberColumn::Float64(_) => {
                    let builder = Self::concat_primitive_types(
                        columns.map(|col| col.into_number().unwrap().into_float64().unwrap()),
                        capacity,
                    );
                    <NumberType<F64>>::upcast_column(<NumberType<F64>>::column_from_vec(
                        builder,
                        &[],
                    ))
                }
            }),
            Column::Decimal(col) => with_decimal_type!(|DECIMAL_TYPE| match col {
                DecimalColumn::Decimal128(_, size) => {
                    let builder = Self::concat_primitive_types(
                        columns.map(|col| match col {
                            Column::Decimal(DecimalColumn::Decimal128(col, _)) => col,
                            _ => unreachable!(),
                        }),
                        capacity,
                    );
                    Column::Decimal(DecimalColumn::Decimal128(builder.into(), size))
                }
                DecimalColumn::Decimal256(_, size) => {
                    let builder = Self::concat_primitive_types(
                        columns.map(|col| match col {
                            Column::Decimal(DecimalColumn::Decimal256(col, _)) => col,
                            _ => unreachable!(),
                        }),
                        capacity,
                    );
                    Column::Decimal(DecimalColumn::Decimal256(builder.into(), size))
                }
            }),
            Column::Boolean(_) => Column::Boolean(Self::concat_boolean_types(
                columns.map(|col| col.into_boolean().unwrap()),
                capacity,
            )),
            Column::Binary(_) => BinaryType::upcast_column(Self::concat_binary_types(
                columns.map(|col| col.into_binary().unwrap()),
                capacity,
            )),
            Column::String(_) => StringType::upcast_column(Self::concat_string_types(
                columns.map(|col| col.into_string().unwrap()),
                capacity,
            )),
            Column::Timestamp(_) => {
                let builder = Self::concat_primitive_types(
                    columns.map(|col| col.into_timestamp().unwrap()),
                    capacity,
                );
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
            Column::Date(_) => {
                let builder = Self::concat_primitive_types(
                    columns.map(|col| col.into_date().unwrap()),
                    capacity,
                );
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
            Column::Geography(_) => {
                let mut builder = GeographyType::create_builder(capacity, &[]);
                for col in columns.map(|col| col.into_geography().unwrap()) {
                    GeographyType::append_column(&mut builder, &col)
                }
                Column::Geography(GeographyType::build_column(builder))
            }
            Column::Array(col) => {
                let mut offsets = Vec::with_capacity(capacity + 1);
                offsets.push(0);
                let builder = ColumnBuilder::with_capacity(&col.values.data_type(), capacity);
                let builder = ArrayColumnBuilder { builder, offsets };
                Self::concat_value_types::<ArrayType<AnyType>>(builder, columns)
            }
            Column::Map(col) => {
                let mut offsets = Vec::with_capacity(capacity + 1);
                offsets.push(0);
                let builder = ColumnBuilder::from_column(
                    ColumnBuilder::with_capacity(&col.values.data_type(), capacity).build(),
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
                Self::concat_value_types::<MapType<AnyType, AnyType>>(builder, columns)
            }
            Column::Bitmap(_) => BitmapType::upcast_column(Self::concat_binary_types(
                columns.map(|col| col.into_bitmap().unwrap()),
                capacity,
            )),
            Column::Nullable(_) => {
                let column: Vec<Column> = columns
                    .clone()
                    .map(|col| col.into_nullable().unwrap().column)
                    .collect();
                let column = Self::concat_columns_impl(column.into_iter())?;
                let validity = Column::Boolean(Self::concat_boolean_types(
                    columns.map(|col| col.into_nullable().unwrap().validity),
                    capacity,
                ));
                let validity = BooleanType::try_downcast_column(&validity).unwrap();
                Column::Nullable(Box::new(NullableColumn { column, validity }))
            }
            Column::Tuple(fields) => {
                let fields = (0..fields.len())
                    .map(|idx| {
                        let column: Vec<Column> = columns
                            .clone()
                            .map(|col| col.into_tuple().unwrap()[idx].clone())
                            .collect();
                        Self::concat_columns_impl(column.into_iter())
                    })
                    .collect::<Result<_>>()?;
                Column::Tuple(fields)
            }
            Column::Variant(_) => VariantType::upcast_column(Self::concat_binary_types(
                columns.map(|col| col.into_variant().unwrap()),
                capacity,
            )),
            Column::Geometry(_) => GeometryType::upcast_column(Self::concat_binary_types(
                columns.map(|col| col.into_geometry().unwrap()),
                capacity,
            )),
        };
        Ok(column)
    }

    pub fn concat_primitive_types<T>(
        cols: impl Iterator<Item = Buffer<T>>,
        num_rows: usize,
    ) -> Vec<T>
    where
        T: Copy,
    {
        let mut builder: Vec<T> = Vec::with_capacity(num_rows);
        for col in cols {
            builder.extend(col.iter());
        }
        builder
    }

    pub fn concat_binary_types(
        cols: impl Iterator<Item = BinaryColumn> + Clone,
        num_rows: usize,
    ) -> BinaryColumn {
        // [`BinaryColumn`] consists of [`data`] and [`offset`], we build [`data`] and [`offset`] respectively,
        // and then call `BinaryColumn::new(data.into(), offsets.into())` to create [`BinaryColumn`].
        let mut offsets: Vec<u64> = Vec::with_capacity(num_rows + 1);
        let mut data_size = 0;

        // Build [`offset`] and calculate `data_size` required by [`data`].
        offsets.push(0);
        for col in cols.clone() {
            let mut start = col.offsets()[0];
            for end in col.offsets()[1..].iter() {
                data_size += end - start;
                start = *end;
                offsets.push(data_size);
            }
        }

        // Build [`data`].
        let mut data: Vec<u8> = Vec::with_capacity(data_size as usize);
        let mut data_ptr = data.as_mut_ptr();

        unsafe {
            for col in cols {
                let offsets = col.offsets();
                let col_data = &(col.data().as_slice())
                    [offsets[0] as usize..offsets[offsets.len() - 1] as usize];
                copy_advance_aligned(col_data.as_ptr(), &mut data_ptr, col_data.len());
            }
            set_vec_len_by_ptr(&mut data, data_ptr);
        }

        BinaryColumn::new(data.into(), offsets.into())
    }

    pub fn concat_string_types(
        cols: impl Iterator<Item = StringColumn> + Clone,
        num_rows: usize,
    ) -> StringColumn {
        unsafe {
            StringColumn::from_binary_unchecked(Self::concat_binary_types(
                cols.map(Into::into),
                num_rows,
            ))
        }
    }

    pub fn concat_boolean_types(bitmaps: impl Iterator<Item = Bitmap>, num_rows: usize) -> Bitmap {
        let capacity = num_rows.saturating_add(7) / 8;
        let mut builder: Vec<u8> = Vec::with_capacity(capacity);
        let mut builder_ptr = builder.as_mut_ptr();
        let mut builder_idx = 0;
        let mut unset_bits = 0;
        let mut buf = 0;

        unsafe {
            for bitmap in bitmaps {
                let (bitmap_slice, bitmap_offset, _) = bitmap.as_slice();
                let mut idx = 0;
                let len = bitmap.len();
                if builder_idx % 8 != 0 {
                    while idx < len {
                        if bitmap.get_bit_unchecked(idx) {
                            buf |= BIT_MASK[builder_idx % 8];
                        } else {
                            unset_bits += 1;
                        }
                        builder_idx += 1;
                        idx += 1;
                        if builder_idx % 8 == 0 {
                            store_advance_aligned(buf, &mut builder_ptr);
                            buf = 0;
                            break;
                        }
                    }
                }
                let remaining = len - idx;
                if remaining > 0 {
                    let (cur_buf, cur_unset_bits) = Self::copy_continuous_bits(
                        &mut builder_ptr,
                        bitmap_slice,
                        builder_idx,
                        idx + bitmap_offset,
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

    fn concat_value_types<T: ValueType>(
        mut builder: T::ColumnBuilder,
        columns: impl Iterator<Item = Column>,
    ) -> Column {
        let columns = columns.map(|c| T::try_downcast_column(&c).unwrap());
        for col in columns {
            T::append_column(&mut builder, &col);
        }
        T::upcast_column(T::build_column(builder))
    }
}
