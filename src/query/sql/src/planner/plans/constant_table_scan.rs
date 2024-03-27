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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::ValueType;
use databend_common_expression::Column;
use databend_common_expression::DataSchemaRef;
use databend_common_functions::aggregates::eval_aggr;
use databend_common_storage::Datum;
use itertools::Itertools;

use crate::optimizer::histogram_from_ndv;
use crate::optimizer::ColumnSet;
use crate::optimizer::ColumnStat;
use crate::optimizer::ColumnStatSet;
use crate::optimizer::Distribution;
use crate::optimizer::PhysicalProperty;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::RequiredProperty;
use crate::optimizer::StatInfo;
use crate::optimizer::Statistics;
use crate::optimizer::DEFAULT_HISTOGRAM_BUCKETS;
use crate::plans::Operator;
use crate::plans::RelOp;

// Constant table is a table with constant values.
#[derive(Clone, Debug)]
pub struct ConstantTableScan {
    pub values: Vec<Column>,
    pub num_rows: usize,
    pub schema: DataSchemaRef,
    pub columns: ColumnSet,
}

impl ConstantTableScan {
    pub fn prune_columns(&self, columns: ColumnSet) -> Self {
        let mut projection = columns
            .iter()
            .map(|index| self.schema.index_of(&index.to_string()).unwrap())
            .collect::<Vec<_>>();
        projection.sort();

        let schema = self.schema.project(&projection);
        let values = projection
            .iter()
            .map(|idx| self.values[*idx].clone())
            .collect();

        ConstantTableScan {
            values,
            schema: Arc::new(schema),
            columns,
            num_rows: self.num_rows,
        }
    }

    pub fn used_columns(&self) -> Result<ColumnSet> {
        Ok(self.columns.clone())
    }
}

impl PartialEq for ConstantTableScan {
    fn eq(&self, other: &Self) -> bool {
        self.num_rows == other.num_rows
            && self.values == other.values
            && self.columns == other.columns
    }
}

impl Eq for ConstantTableScan {}

impl std::hash::Hash for ConstantTableScan {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.num_rows.hash(state);
        for value in self.values.iter() {
            for i in 0..value.len() {
                let v = unsafe { value.index_unchecked(i) };
                v.hash(state);
            }
        }
        for column in self.columns.iter().sorted() {
            column.hash(state);
        }
    }
}

impl Operator for ConstantTableScan {
    fn rel_op(&self) -> RelOp {
        RelOp::ConstantTableScan
    }

    fn arity(&self) -> usize {
        0
    }

    fn derive_relational_prop(&self, _rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        Ok(Arc::new(RelationalProperty {
            output_columns: self.columns.clone(),
            outer_columns: Default::default(),
            used_columns: self.columns.clone(),
            orderings: vec![],
        }))
    }

    fn derive_physical_prop(&self, _rel_expr: &RelExpr) -> Result<PhysicalProperty> {
        Ok(PhysicalProperty {
            distribution: Distribution::Random,
        })
    }

    fn derive_stats(&self, _rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        let mut column_stats: ColumnStatSet = Default::default();
        for (index, value) in self.columns.iter().zip(self.values.iter()) {
            let (mins, _) = eval_aggr("min", vec![], &[value.clone()], self.num_rows)?;
            let min = if let Some(v) = mins.index(0) {
                match Datum::from_scalar(v.to_owned()) {
                    Some(val) => val,
                    None => {
                        continue;
                    }
                }
            } else {
                continue;
            };
            let (maxs, _) = eval_aggr("max", vec![], &[value.clone()], self.num_rows)?;
            let max = if let Some(v) = maxs.index(0) {
                match Datum::from_scalar(v.to_owned()) {
                    Some(val) => val,
                    None => {
                        continue;
                    }
                }
            } else {
                continue;
            };

            let distinct_values = eval_aggr(
                "approx_count_distinct",
                vec![],
                &[value.clone()],
                self.num_rows,
            )?;
            let ndv = NumberType::<u64>::try_downcast_column(&distinct_values.0).unwrap()[0];

            let (is_all_null, bitmap) = value.validity();
            let null_count = match (is_all_null, bitmap) {
                (true, _) => self.num_rows as u64,
                (false, Some(bitmap)) => bitmap.unset_bits() as u64,
                (false, None) => 0,
            };

            let histogram = histogram_from_ndv(
                ndv,
                self.num_rows as u64,
                Some((min.clone(), max.clone())),
                DEFAULT_HISTOGRAM_BUCKETS,
            )
            .ok();
            let column_stat = ColumnStat {
                min,
                max,
                ndv: ndv as f64,
                null_count,
                histogram,
            };
            column_stats.insert(*index, column_stat);
        }
        Ok(Arc::new(StatInfo {
            cardinality: self.num_rows as f64,
            statistics: Statistics {
                precise_cardinality: Some(self.num_rows as u64),
                column_stats,
            },
        }))
    }

    fn compute_required_prop_child(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        _child_index: usize,
        _required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        Err(ErrorCode::Internal(
            "ConstantTableScan cannot compute required property for children".to_string(),
        ))
    }
}
