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

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_arrow::arrow::bitmap::MutableBitmap;
use databend_common_exception::Result;
use databend_common_expression::arrow::or_validities;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::AnyType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FilterExecutor;
use databend_common_expression::FunctionContext;
use databend_common_expression::InputColumns;
use databend_common_expression::Value;
use databend_common_functions::BUILTIN_FUNCTIONS;

use super::desc::MARKER_KIND_FALSE;
use super::desc::MARKER_KIND_NULL;
use super::desc::MARKER_KIND_TRUE;
use super::HashJoinState;
use crate::pipelines::processors::transforms::hash_join::HashJoinProbeState;

/// Some common methods for hash join.

impl HashJoinProbeState {
    // Merge build chunk and probe chunk that have the same number of rows
    pub(crate) fn merge_eq_block(
        &self,
        probe_block: Option<DataBlock>,
        build_block: Option<DataBlock>,
        num_rows: usize,
    ) -> DataBlock {
        match (probe_block, build_block) {
            (Some(mut probe_block), Some(build_block)) => {
                probe_block.merge_block(build_block);
                probe_block
            }
            (Some(probe_block), None) => probe_block,
            (None, Some(build_block)) => build_block,
            (None, None) => DataBlock::new(vec![], num_rows),
        }
    }

    pub(crate) fn create_marker_block(
        &self,
        has_null: bool,
        markers: &mut [u8],
        num_rows: usize,
    ) -> Result<DataBlock> {
        let mut validity = MutableBitmap::with_capacity(num_rows);
        let mut boolean_bit_map = MutableBitmap::with_capacity(num_rows);
        let mut row_index = 0;
        while row_index < num_rows {
            let marker = if markers[row_index] == MARKER_KIND_FALSE && has_null {
                MARKER_KIND_NULL
            } else {
                markers[row_index]
            };
            if marker == MARKER_KIND_NULL {
                validity.push(false);
            } else {
                validity.push(true);
            }
            if marker == MARKER_KIND_TRUE {
                boolean_bit_map.push(true);
            } else {
                boolean_bit_map.push(false);
            }
            markers[row_index] = MARKER_KIND_FALSE;
            row_index += 1;
        }
        let boolean_column = Column::Boolean(boolean_bit_map.into());
        let marker_column = Column::Nullable(Box::new(NullableColumn {
            column: boolean_column,
            validity: validity.into(),
        }));
        Ok(DataBlock::new_from_columns(vec![marker_column]))
    }

    // return (result data block, filtered indices, all_true, all_false).
    pub(crate) fn get_other_predicate_result_block<'a>(
        &self,
        filter_executor: &'a mut FilterExecutor,
        data_block: DataBlock,
    ) -> Result<(DataBlock, &'a [u32], bool, bool)> {
        let origin_count = data_block.num_rows();
        let result_block = filter_executor.filter(data_block)?;
        let result_count = result_block.num_rows();
        Ok((
            result_block,
            &filter_executor.true_selection()[0..result_count],
            result_count == origin_count,
            result_count == 0,
        ))
    }

    // return (result data block, filtered indices, all_true, all_false).
    pub(crate) fn get_other_predicate_selection<'a>(
        &self,
        filter_executor: &'a mut FilterExecutor,
        data_block: &DataBlock,
    ) -> Result<(&'a [u32], bool, bool)> {
        let origin_count = data_block.num_rows();
        let result_count = filter_executor.select(data_block)?;
        Ok((
            &filter_executor.true_selection()[0..result_count],
            result_count == origin_count,
            result_count == 0,
        ))
    }

    pub(crate) fn get_nullable_filter_column(
        &self,
        merged_block: &DataBlock,
        filter: &Expr,
        func_ctx: &FunctionContext,
    ) -> Result<Column> {
        let evaluator = Evaluator::new(merged_block, func_ctx, &BUILTIN_FUNCTIONS);
        let filter_vector: Value<AnyType> = evaluator.run(filter)?;
        let filter_vector =
            filter_vector.convert_to_full_column(filter.data_type(), merged_block.num_rows());

        match filter_vector {
            Column::Nullable(_) => Ok(filter_vector),
            other => Ok(Column::Nullable(Box::new(NullableColumn {
                validity: Bitmap::new_constant(true, other.len()),
                column: other,
            }))),
        }
    }
}

impl HashJoinState {
    /// if all cols in the same row are all null, we mark this row as null.
    pub(crate) fn init_markers(&self, cols: InputColumns, num_rows: usize, markers: &mut [u8]) {
        if cols
            .iter()
            .any(|c| matches!(c, Column::Null { .. } | Column::Nullable(_)))
        {
            let mut valids = None;
            for col in cols.iter() {
                match col {
                    Column::Nullable(c) => {
                        let bitmap = &c.validity;
                        if bitmap.unset_bits() == 0 {
                            valids = Some(Bitmap::new_constant(true, num_rows));
                            break;
                        } else {
                            valids = or_validities(valids, Some(bitmap.clone()));
                        }
                    }
                    Column::Null { .. } => {}
                    _c => {
                        valids = Some(Bitmap::new_constant(true, num_rows));
                        break;
                    }
                }
            }
            if let Some(v) = valids {
                let mut idx = 0;
                while idx < num_rows {
                    if !v.get_bit(idx) {
                        markers[idx] = MARKER_KIND_NULL;
                    }
                    idx += 1;
                }
            }
        }
    }
}

pub fn wrap_true_validity(
    column: &BlockEntry,
    num_rows: usize,
    true_validity: &Bitmap,
) -> BlockEntry {
    let (value, data_type) = (&column.value, &column.data_type);
    let col = value.convert_to_full_column(data_type, num_rows);
    if matches!(col, Column::Null { .. }) || col.as_nullable().is_some() {
        column.clone()
    } else {
        let mut validity = true_validity.clone();
        validity.slice(0, num_rows);
        let col = Column::Nullable(Box::new(NullableColumn {
            column: col,
            validity,
        }));
        BlockEntry::new(data_type.wrap_nullable(), Value::Column(col))
    }
}
