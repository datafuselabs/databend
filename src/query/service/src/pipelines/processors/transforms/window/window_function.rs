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

use databend_common_base::runtime::drop_guard;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::InputColumns;
use databend_common_functions::aggregates::get_layout_offsets;
use databend_common_functions::aggregates::AggregateFunction;
use databend_common_functions::aggregates::AggregateFunctionFactory;
use databend_common_functions::aggregates::StateAddr;
use databend_common_sql::executor::physical_plans::LagLeadDefault;
use databend_common_sql::executor::physical_plans::WindowFunction;

use crate::pipelines::processors::transforms::group_by::Area;

#[derive(Clone)]
pub enum WindowFunctionInfo {
    // (func instance, argument offsets)
    Aggregate(Arc<dyn AggregateFunction>, Vec<usize>),
    RowNumber,
    Rank,
    DenseRank,
    PercentRank,
    LagLead(WindowFuncLagLeadImpl),
    NthValue(WindowFuncNthValueImpl),
    Ntile(WindowFuncNtileImpl),
    CumeDist,
}

pub struct WindowFuncAggImpl {
    // Need to hold arena until `drop`.
    _arena: Area,
    agg: Arc<dyn AggregateFunction>,
    place: StateAddr,
    args: Vec<usize>,
}

impl WindowFuncAggImpl {
    #[inline]
    pub fn reset(&self) {
        self.agg.init_state(self.place);
    }

    #[inline]
    pub fn arg_columns<'a>(&'a self, data: &'a DataBlock) -> InputColumns {
        InputColumns::new_block_proxy(&self.args, data)
    }

    #[inline]
    pub fn accumulate_row(&self, args: InputColumns, row: usize) -> Result<()> {
        self.agg.accumulate_row(self.place, args, row)
    }

    #[inline]
    pub fn merge_result(&self, builder: &mut ColumnBuilder) -> Result<()> {
        self.agg.merge_result(self.place, builder)
    }
}

impl Drop for WindowFuncAggImpl {
    fn drop(&mut self) {
        drop_guard(move || {
            if self.agg.need_manual_drop_state() {
                unsafe {
                    self.agg.drop_state(self.place);
                }
            }
        })
    }
}

#[derive(Clone, Debug)]
pub struct WindowFuncLagLeadImpl {
    pub arg: usize,
    pub default: LagLeadDefault,
    pub return_type: DataType,
}

#[derive(Clone)]
pub struct WindowFuncNthValueImpl {
    pub n: Option<u64>,
    pub arg: usize,
    pub return_type: DataType,
}

#[derive(Clone)]
pub struct WindowFuncNtileImpl {
    /// number of buckets
    pub n: usize,
    pub return_type: DataType,
}

impl WindowFuncNtileImpl {
    pub(crate) fn compute_nitle(
        &self,
        current_row_in_partition: usize,
        num_partition_rows: usize,
    ) -> usize {
        if self.n > num_partition_rows {
            // buckets more than partition rows
            current_row_in_partition
        } else {
            NtileBucket::new(self.n, num_partition_rows)
                .compute_bucket_value(current_row_in_partition - 1)
        }
    }
}

struct NtileBucket {
    // number of rows in a bucket: `(number of rows in partition) / (number of buckets)`.
    rows_per_bucket: usize,
    // partition rows might not be exactly divisible by number of buckets,
    // so some buckets could have `rows_per_bucket + 1` rows
    // `extra_buckets`: (number of rows in partition) % (number of buckets).
    extra_buckets: usize,
    // the first extra_buckets will have (rows_per_bucket + 1) rows.
    // this row number in partition at this boundary is extra_buckets_boundary.
    // `extra_buckets_boundary`: extra_buckets * (rows_per_bucket + 1).
    // for each rows in partition beyond this row number,
    // their belong to bucket have only `rows_per_bucket` number of rows.
    // this field is used to computing the bucket value.
    extra_buckets_boundary: usize,
}

impl NtileBucket {
    fn new(num_buckets: usize, num_partition_rows: usize) -> Self {
        let rows_per_bucket = num_partition_rows / num_buckets;
        let extra_buckets = num_partition_rows % num_buckets;
        let extra_buckets_boundary = (rows_per_bucket + 1) * extra_buckets;
        Self {
            rows_per_bucket,
            extra_buckets,
            extra_buckets_boundary,
        }
    }

    fn compute_bucket_value(&self, row_number: usize) -> usize {
        if row_number < self.extra_buckets_boundary {
            return row_number / (self.rows_per_bucket + 1) + 1;
        }
        (row_number - self.extra_buckets) / self.rows_per_bucket + 1
    }
}

pub enum WindowFunctionImpl {
    Aggregate(WindowFuncAggImpl),
    RowNumber,
    Rank,
    DenseRank,
    PercentRank,
    LagLead(WindowFuncLagLeadImpl),
    NthValue(WindowFuncNthValueImpl),
    Ntile(WindowFuncNtileImpl),
    CumeDist,
}

impl WindowFunctionInfo {
    pub fn try_create(window: &WindowFunction, schema: &DataSchema) -> Result<Self> {
        Ok(match window {
            WindowFunction::Aggregate(agg) => {
                let agg_func = AggregateFunctionFactory::instance().get(
                    agg.sig.name.as_str(),
                    agg.sig.params.clone(),
                    agg.sig.args.clone(),
                )?;
                let args = agg
                    .arg_indices
                    .iter()
                    .map(|p| {
                        let offset = schema.index_of(&p.to_string())?;
                        Ok(offset)
                    })
                    .collect::<Result<Vec<_>>>()?;
                Self::Aggregate(agg_func, args)
            }
            WindowFunction::RowNumber => Self::RowNumber,
            WindowFunction::Rank => Self::Rank,
            WindowFunction::DenseRank => Self::DenseRank,
            WindowFunction::PercentRank => Self::PercentRank,
            WindowFunction::LagLead(ll) => {
                let new_arg = schema.index_of(&ll.arg.to_string())?;
                let new_default = match &ll.default {
                    LagLeadDefault::Null => LagLeadDefault::Null,
                    LagLeadDefault::Index(i) => {
                        let offset = schema.index_of(&i.to_string())?;
                        LagLeadDefault::Index(offset)
                    }
                };
                Self::LagLead(WindowFuncLagLeadImpl {
                    arg: new_arg,
                    default: new_default,
                    return_type: ll.return_type.clone(),
                })
            }
            WindowFunction::NthValue(func) => {
                let new_arg = schema.index_of(&func.arg.to_string())?;
                Self::NthValue(WindowFuncNthValueImpl {
                    n: func.n,
                    arg: new_arg,
                    return_type: func.return_type.clone(),
                })
            }
            WindowFunction::Ntile(func) => Self::Ntile(WindowFuncNtileImpl {
                n: func.n as usize,
                return_type: func.return_type.clone(),
            }),
            WindowFunction::CumeDist => Self::CumeDist,
        })
    }
}

impl WindowFunctionImpl {
    pub(crate) fn try_create(window: Vec<WindowFunctionInfo>) -> Result<Vec<Self>> {
        let mut window_func_impl = vec![];
        for window in window {
            window_func_impl.push(match window {
                WindowFunctionInfo::Aggregate(agg, args) => {
                    let mut arena = Area::create();
                    let mut state_offset = Vec::with_capacity(1);
                    let layout = get_layout_offsets(&[agg.clone()], &mut state_offset)?;
                    let place: StateAddr = arena.alloc_layout(layout).into();
                    let place = place.next(state_offset[0]);
                    let agg = WindowFuncAggImpl {
                        _arena: arena,
                        agg,
                        place,
                        args,
                    };
                    agg.reset();
                    Self::Aggregate(agg)
                }
                WindowFunctionInfo::RowNumber => Self::RowNumber,
                WindowFunctionInfo::Rank => Self::Rank,
                WindowFunctionInfo::DenseRank => Self::DenseRank,
                WindowFunctionInfo::PercentRank => Self::PercentRank,
                WindowFunctionInfo::LagLead(ll) => Self::LagLead(ll),
                WindowFunctionInfo::NthValue(func) => Self::NthValue(func),
                WindowFunctionInfo::Ntile(func) => Self::Ntile(func),
                WindowFunctionInfo::CumeDist => Self::CumeDist,
            })
        }
        Ok(window_func_impl)
    }

    pub fn return_type(&self) -> Result<DataType> {
        Ok(match self {
            Self::Aggregate(agg) => agg.agg.return_type()?,
            Self::RowNumber | Self::Rank | Self::DenseRank => {
                DataType::Number(NumberDataType::UInt64)
            }
            Self::PercentRank | Self::CumeDist => DataType::Number(NumberDataType::Float64),
            Self::LagLead(f) => f.return_type.clone(),
            Self::NthValue(f) => f.return_type.clone(),
            Self::Ntile(f) => f.return_type.clone(),
        })
    }

    #[inline]
    pub fn reset(&self) {
        if let Self::Aggregate(agg) = self {
            agg.reset();
        }
    }
}
