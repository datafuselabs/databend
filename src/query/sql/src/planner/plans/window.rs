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

use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use databend_common_ast::Span;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::Scalar;
use educe::Educe;
use enum_as_inner::EnumAsInner;
use serde::Deserialize;
use serde::Serialize;

use super::AggregateFunction;
use super::NthValueFunction;
use crate::binder::WindowOrderByInfo;
use crate::optimizer::ColumnSet;
use crate::optimizer::Distribution;
use crate::optimizer::PhysicalProperty;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::RequiredProperty;
use crate::optimizer::StatInfo;
use crate::optimizer::Statistics;
use crate::plans::LagLeadFunction;
use crate::plans::NtileFunction;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::ScalarItem;
use crate::IndexType;

#[derive(Clone, Debug, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct Window {
    #[educe(PartialEq(ignore), Eq(ignore), Hash(ignore))]
    pub span: Span,

    // aggregate scalar expressions, such as: sum(col1), count(*);
    // or general window functions, such as: row_number(), rank();
    pub index: Vec<IndexType>,
    pub function: Vec<WindowFuncType>,
    pub arguments: Vec<Vec<ScalarItem>>,

    // partition by scalar expressions
    pub partition_by: Vec<ScalarItem>,
    // order by
    pub order_by: Vec<WindowOrderByInfo>,
    // window frames
    pub frame: WindowFuncFrame,
    // limit for potentially possible push-down
    pub limit: Option<usize>,
}

impl Window {
    pub fn used_columns(&self) -> Result<ColumnSet> {
        let mut used_columns = ColumnSet::new();

        for index in &self.index {
            used_columns.insert(*index);
        }
        for func in &self.function {
            used_columns.extend(func.used_columns());
        }

        for args in self.arguments.iter() {
            for arg in args {
                used_columns.insert(arg.index);
                used_columns.extend(arg.scalar.used_columns())
            }
        }

        for part in self.partition_by.iter() {
            used_columns.insert(part.index);
            used_columns.extend(part.scalar.used_columns())
        }

        for sort in self.order_by.iter() {
            used_columns.insert(sort.order_by_item.index);
            used_columns.extend(sort.order_by_item.scalar.used_columns())
        }

        Ok(used_columns)
    }

    // `Window.partition_by_columns` used in `RulePushDownFilterWindow` only consider `partition_by` field,
    // like `Aggregate.group_columns` only consider `group_items` field.
    pub fn partition_by_columns(&self) -> Result<ColumnSet> {
        let mut col_set = ColumnSet::new();
        for part in self.partition_by.iter() {
            col_set.insert(part.index);
            col_set.extend(part.scalar.used_columns())
        }
        Ok(col_set)
    }
}

impl Operator for Window {
    fn rel_op(&self) -> RelOp {
        RelOp::Window
    }

    fn arity(&self) -> usize {
        1
    }

    fn derive_physical_prop(&self, rel_expr: &RelExpr) -> Result<PhysicalProperty> {
        rel_expr.derive_physical_prop_child(0)
    }

    fn compute_required_prop_child(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        _child_index: usize,
        required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        let mut required = required.clone();
        if self.partition_by.is_empty() {
            required.distribution = Distribution::Serial;
        }
        Ok(required.clone())
    }

    fn compute_required_prop_children(
        &self,
        _ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        required: &RequiredProperty,
    ) -> Result<Vec<Vec<RequiredProperty>>> {
        let mut required = required.clone();
        if self.partition_by.is_empty() {
            required.distribution = Distribution::Serial;
        }
        Ok(vec![vec![required.clone()]])
    }

    fn derive_relational_prop(&self, rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        let input_prop = rel_expr.derive_relational_prop_child(0)?;

        // Derive output columns
        let mut output_columns = input_prop.output_columns.clone();
        for index in &self.index {
            output_columns.insert(*index);
        }

        // Derive outer columns
        let outer_columns = input_prop
            .outer_columns
            .difference(&output_columns)
            .cloned()
            .collect();

        // Derive used columns
        let mut used_columns = self.used_columns()?;
        used_columns.extend(input_prop.used_columns.clone());

        // Derive orderings
        let orderings = input_prop.orderings.clone();
        let partition_orderings = input_prop.partition_orderings.clone();

        Ok(Arc::new(RelationalProperty {
            output_columns,
            outer_columns,
            used_columns,
            orderings,
            partition_orderings,
        }))
    }

    fn derive_stats(&self, rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        let input_stat_info = rel_expr.derive_cardinality_child(0)?;
        let cardinality = if self.partition_by.is_empty() {
            // Scalar aggregation
            1.0
        } else if self.partition_by.iter().any(|item| {
            input_stat_info
                .statistics
                .column_stats
                .get(&item.index)
                .is_none()
        }) {
            input_stat_info.cardinality
        } else {
            // A upper bound
            let res = self.partition_by.iter().fold(1.0, |acc, item| {
                let item_stat = input_stat_info
                    .statistics
                    .column_stats
                    .get(&item.index)
                    .unwrap();
                acc * item_stat.ndv
            });
            // To avoid res is very large
            f64::min(res, input_stat_info.cardinality)
        };

        let precise_cardinality = if self.partition_by.is_empty() {
            Some(1)
        } else {
            None
        };
        Ok(Arc::new(StatInfo {
            cardinality,
            statistics: Statistics {
                precise_cardinality,
                column_stats: input_stat_info.statistics.column_stats.clone(),
            },
        }))
    }
}

#[derive(Default, Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub struct WindowFuncFrame {
    pub units: WindowFuncFrameUnits,
    pub start_bound: WindowFuncFrameBound,
    pub end_bound: WindowFuncFrameBound,
}

impl Display for WindowFuncFrame {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "{:?}: {:?} ~ {:?}",
            self.units, self.start_bound, self.end_bound
        )
    }
}

#[derive(Default, Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, EnumAsInner)]
pub enum WindowFuncFrameUnits {
    #[default]
    Rows,
    Range,
}

#[derive(Default, Clone, PartialEq, Eq, Hash, Debug, Serialize, Deserialize)]
pub enum WindowFuncFrameBound {
    /// `CURRENT ROW`
    #[default]
    CurrentRow,
    /// `<N> PRECEDING` or `UNBOUNDED PRECEDING`
    Preceding(Option<Scalar>),
    /// `<N> FOLLOWING` or `UNBOUNDED FOLLOWING`.
    Following(Option<Scalar>),
}

#[derive(Clone, PartialEq, Eq, Hash, Debug)]
pub enum WindowFuncType {
    Aggregate(AggregateFunction),
    RowNumber,
    Rank,
    DenseRank,
    PercentRank,
    LagLead(LagLeadFunction),
    NthValue(NthValueFunction),
    Ntile(NtileFunction),
    CumeDist,
}

impl WindowFuncType {
    pub fn from_name(name: &str) -> Result<WindowFuncType> {
        match name {
            "row_number" => Ok(WindowFuncType::RowNumber),
            "rank" => Ok(WindowFuncType::Rank),
            "dense_rank" => Ok(WindowFuncType::DenseRank),
            "percent_rank" => Ok(WindowFuncType::PercentRank),
            "cume_dist" => Ok(WindowFuncType::CumeDist),
            _ => Err(ErrorCode::UnknownFunction(format!(
                "Unknown window function: {}",
                name
            ))),
        }
    }

    pub fn func_name(&self) -> String {
        match self {
            WindowFuncType::Aggregate(agg) => agg.func_name.to_string(),
            WindowFuncType::RowNumber => "row_number".to_string(),
            WindowFuncType::Rank => "rank".to_string(),
            WindowFuncType::DenseRank => "dense_rank".to_string(),
            WindowFuncType::PercentRank => "percent_rank".to_string(),
            WindowFuncType::LagLead(lag_lead) if lag_lead.is_lag => "lag".to_string(),
            WindowFuncType::LagLead(_) => "lead".to_string(),
            WindowFuncType::NthValue(_) => "nth_value".to_string(),
            WindowFuncType::Ntile(_) => "ntile".to_string(),
            WindowFuncType::CumeDist => "cume_dist".to_string(),
        }
    }

    pub fn used_columns(&self) -> ColumnSet {
        match self {
            WindowFuncType::Aggregate(agg) => {
                agg.args.iter().flat_map(|arg| arg.used_columns()).collect()
            }
            WindowFuncType::LagLead(func) => match &func.default {
                None => func.arg.used_columns(),
                Some(d) => func
                    .arg
                    .used_columns()
                    .union(&d.used_columns())
                    .cloned()
                    .collect(),
            },
            WindowFuncType::NthValue(func) => func.arg.used_columns(),
            _ => ColumnSet::new(),
        }
    }

    pub fn return_type(&self) -> DataType {
        match self {
            WindowFuncType::Aggregate(agg) => *agg.return_type.clone(),
            WindowFuncType::RowNumber | WindowFuncType::Rank | WindowFuncType::DenseRank => {
                DataType::Number(NumberDataType::UInt64)
            }
            WindowFuncType::PercentRank | WindowFuncType::CumeDist => {
                DataType::Number(NumberDataType::Float64)
            }
            WindowFuncType::LagLead(lag_lead) => *lag_lead.return_type.clone(),
            WindowFuncType::NthValue(nth_value) => *nth_value.return_type.clone(),
            WindowFuncType::Ntile(buckets) => *buckets.return_type.clone(),
        }
    }
}
