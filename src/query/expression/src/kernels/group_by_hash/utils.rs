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

use ethnum::i256;

use crate::kernels::utils::copy_advance_aligned;
use crate::kernels::utils::set_vec_len_by_ptr;
use crate::kernels::utils::store_advance;
use crate::kernels::utils::store_advance_aligned;
use crate::types::binary::BinaryColumn;
use crate::types::decimal::DecimalColumn;
use crate::types::NumberColumn;
use crate::with_decimal_mapped_type;
use crate::with_number_mapped_type;
use crate::Column;
use crate::InputColumns;

/// The serialize_size is equal to the number of bytes required by serialization.
pub fn serialize_group_columns(
    columns: InputColumns,
    num_rows: usize,
    serialize_size: usize,
) -> BinaryColumn {
    // [`BinaryColumn`] consists of [`data`] and [`offset`], we build [`data`] and [`offset`] respectively,
    // and then call `BinaryColumn::new(data.into(), offsets.into())` to create [`BinaryColumn`].
    let mut data: Vec<u8> = Vec::with_capacity(serialize_size);
    let mut offsets: Vec<u64> = Vec::with_capacity(num_rows + 1);
    let mut data_ptr = data.as_mut_ptr();
    let mut offsets_ptr = offsets.as_mut_ptr();
    let mut offset = 0;

    unsafe {
        store_advance_aligned::<u64>(0, &mut offsets_ptr);
        for i in 0..num_rows {
            let old_ptr = data_ptr;
            for col in columns.iter() {
                serialize_column_binary(col, i, &mut data_ptr);
            }
            offset += data_ptr as u64 - old_ptr as u64;
            store_advance_aligned::<u64>(offset, &mut offsets_ptr);
        }
        set_vec_len_by_ptr(&mut data, data_ptr);
        set_vec_len_by_ptr(&mut offsets, offsets_ptr);
    }

    BinaryColumn::new(data.into(), offsets.into())
}

/// This function must be consistent with the `push_binary` function of `src/query/expression/src/values.rs`.
/// # Safety
///
/// * The size of the memory pointed by `row_space` is equal to the number of bytes required by serialization.
pub unsafe fn serialize_column_binary(column: &Column, row: usize, row_space: &mut *mut u8) {
    match column {
        Column::Null { .. } | Column::EmptyArray { .. } | Column::EmptyMap { .. } => {}
        Column::Number(v) => with_number_mapped_type!(|NUM_TYPE| match v {
            NumberColumn::NUM_TYPE(v) => {
                store_advance::<NUM_TYPE>(&v[row], row_space);
            }
        }),
        Column::Decimal(v) => {
            with_decimal_mapped_type!(|DECIMAL_TYPE| match v {
                DecimalColumn::DECIMAL_TYPE(v, _) => {
                    store_advance::<DECIMAL_TYPE>(&v[row], row_space);
                }
            })
        }
        Column::Boolean(v) => store_advance::<bool>(&v.get_bit(row), row_space),
        Column::Binary(v) | Column::Bitmap(v) | Column::Variant(v) | Column::Geometry(v) => {
            let value = unsafe { v.index_unchecked(row) };
            let len = value.len();
            store_advance::<u64>(&(len as u64), row_space);
            copy_advance_aligned::<u8>(value.as_ptr(), row_space, len);
        }
        Column::String(v) => {
            let value = unsafe { v.index_unchecked(row) };
            let len = value.len();
            store_advance::<u64>(&(len as u64), row_space);
            copy_advance_aligned::<u8>(value.as_ptr(), row_space, len);
        }
        Column::Timestamp(v) => store_advance::<i64>(&v[row], row_space),
        Column::Date(v) => store_advance::<i32>(&v[row], row_space),
        Column::Geography(v) => {
            let value = unsafe { v.index_unchecked(row) };
            let value = borsh::to_vec(&value.0).unwrap();
            let len = value.len();
            store_advance::<u64>(&(len as u64), row_space);
            copy_advance_aligned::<u8>(value.as_ptr(), row_space, len);
        }
        Column::Array(array) | Column::Map(array) => {
            let data = array.index(row).unwrap();
            store_advance::<u64>(&(data.len() as u64), row_space);
            for i in 0..data.len() {
                serialize_column_binary(&data, i, row_space);
            }
        }
        Column::Nullable(c) => {
            let valid = c.validity.get_bit(row);
            store_advance::<bool>(&valid, row_space);
            if valid {
                serialize_column_binary(&c.column, row, row_space);
            }
        }
        Column::Tuple(fields) => {
            for inner_col in fields.iter() {
                serialize_column_binary(inner_col, row, row_space);
            }
        }
    }
}
