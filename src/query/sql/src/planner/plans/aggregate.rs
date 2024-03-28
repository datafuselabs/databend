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
use databend_common_expression::types::DataType;

use crate::optimizer::ColumnSet;
use crate::optimizer::Distribution;
use crate::optimizer::PhysicalProperty;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::RequiredProperty;
use crate::optimizer::StatInfo;
use crate::optimizer::Statistics;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::ScalarItem;
use crate::IndexType;

#[derive(Clone, Debug, PartialEq, Eq, Hash, Copy)]
pub enum AggregateMode {
    Partial,
    Final,

    // TODO(leiysky): this mode is only used for preventing recursion of
    // RuleSplitAggregate, find a better way.
    Initial,
}

/// Information for `GROUPING SETS`.
/// See the comment of [`crate::planner::binder::aggregate::GroupingSetsInfo`].
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub struct GroupingSets {
    /// The index of the virtual column `_grouping_id`. It's valid only if `grouping_sets` is not empty.
    pub grouping_id_index: IndexType,
    /// See the comment in `GroupingSetsInfo`.
    pub sets: Vec<Vec<IndexType>>,
    /// See the comment in `GroupingSetsInfo`.
    pub dup_group_items: Vec<(IndexType, DataType)>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Aggregate {
    pub mode: AggregateMode,
    // group by scalar expressions, such as: group by col1, col2;
    pub group_items: Vec<ScalarItem>,
    // aggregate scalar expressions, such as: sum(col1), count(*);
    pub aggregate_functions: Vec<ScalarItem>,
    // True if the plan is generated from distinct, else the plan is a normal aggregate;
    pub from_distinct: bool,
    pub limit: Option<usize>,
    pub grouping_sets: Option<GroupingSets>,
}

impl Aggregate {
    pub fn used_columns(&self) -> Result<ColumnSet> {
        let mut used_columns = ColumnSet::new();
        for group_item in self.group_items.iter() {
            used_columns.insert(group_item.index);
            used_columns.extend(group_item.scalar.used_columns())
        }
        for agg in self.aggregate_functions.iter() {
            used_columns.insert(agg.index);
            used_columns.extend(agg.scalar.used_columns())
        }
        Ok(used_columns)
    }

    pub fn group_columns(&self) -> Result<ColumnSet> {
        let mut col_set = ColumnSet::new();
        for group_item in self.group_items.iter() {
            col_set.insert(group_item.index);
            col_set.extend(group_item.scalar.used_columns())
        }
        Ok(col_set)
    }
}

impl Operator for Aggregate {
    fn rel_op(&self) -> RelOp {
        RelOp::Aggregate
    }

    fn arity(&self) -> usize {
        1
    }

    fn derive_physical_prop(&self, rel_expr: &RelExpr) -> Result<PhysicalProperty> {
        let input_physical_prop = rel_expr.derive_physical_prop_child(0)?;

        if input_physical_prop.distribution == Distribution::Serial {
            return Ok(input_physical_prop);
        }

        match self.mode {
            AggregateMode::Partial => {
                // TODO(leiysky): in current implementation we are using the `_group_by_key` produced
                // by partial aggregation as the distribution key, which is not exactly the same with
                // `Hash(group_items)`. Because of this we cannot leverage the distribution key to
                // satisfy the required hash distribution from parent node. We should fix this in the future.
                Ok(PhysicalProperty {
                    distribution: Distribution::Random,
                })
            }

            AggregateMode::Final => {
                if self.group_items.is_empty() {
                    // Scalar aggregation
                    Ok(PhysicalProperty {
                        distribution: Distribution::Serial,
                    })
                } else {
                    // The distribution should have been derived by partial aggregation
                    Ok(PhysicalProperty {
                        distribution: input_physical_prop.distribution,
                    })
                }
            }

            AggregateMode::Initial => Err(ErrorCode::Internal(
                "Cannot derive physical property for initial aggregate".to_string(),
            ))?,
        }
    }

    fn compute_required_prop_child(
        &self,
        ctx: Arc<dyn TableContext>,
        rel_expr: &RelExpr,
        _child_index: usize,
        required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        let mut required = required.clone();
        let child_physical_prop = rel_expr.derive_physical_prop_child(0)?;

        if child_physical_prop.distribution == Distribution::Serial {
            return Ok(required);
        }

        match self.mode {
            AggregateMode::Partial => {
                if self.group_items.is_empty() {
                    // Scalar aggregation
                    required.distribution = Distribution::Any;
                } else {
                    let settings = ctx.get_settings();

                    // Group aggregation, enforce `Hash` distribution
                    required.distribution = match settings.get_group_by_shuffle_mode()?.as_str() {
                        "before_partial" => Ok(Distribution::Hash(
                            self.group_items
                                .iter()
                                .map(|item| item.scalar.clone())
                                .collect(),
                        )),
                        "before_merge" => {
                            Ok(Distribution::Hash(vec![self.group_items[0].scalar.clone()]))
                        }
                        value => Err(ErrorCode::Internal(format!(
                            "Bad settings value group_by_shuffle_mode = {:?}",
                            value
                        ))),
                    }?;
                }
            }

            AggregateMode::Final => {
                if self.group_items.is_empty() {
                    // Scalar aggregation
                    required.distribution = Distribution::Serial;
                } else {
                    // The distribution should have been derived by partial aggregation
                    required.distribution = Distribution::Any;
                }
            }

            AggregateMode::Initial => unreachable!(),
        }
        Ok(required)
    }

    fn derive_relational_prop(&self, rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        let input_prop = rel_expr.derive_relational_prop_child(0)?;

        // Derive output columns
        let mut output_columns = ColumnSet::new();
        for group_item in self.group_items.iter() {
            output_columns.insert(group_item.index);
        }
        for agg in self.aggregate_functions.iter() {
            output_columns.insert(agg.index);
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

        Ok(Arc::new(RelationalProperty {
            output_columns,
            outer_columns,
            used_columns,
            orderings: vec![],
        }))
    }

    fn derive_stats(&self, rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        if self.mode == AggregateMode::Final {
            return rel_expr.derive_cardinality_child(0);
        }
        let stat_info = rel_expr.derive_cardinality_child(0)?;
        let (cardinality, mut statistics) = (stat_info.cardinality, stat_info.statistics.clone());
        let cardinality = if self.group_items.is_empty() {
            // Scalar aggregation
            1.0
        } else if self
            .group_items
            .iter()
            .any(|item| statistics.column_stats.get(&item.index).is_none())
        {
            cardinality
        } else {
            // A upper bound
            let res = self.group_items.iter().fold(1.0, |acc, item| {
                let item_stat = statistics.column_stats.get(&item.index).unwrap();
                acc * item_stat.ndv
            });
            for item in self.group_items.iter() {
                let item_stat = statistics.column_stats.get_mut(&item.index).unwrap();
                if let Some(histogram) = &mut item_stat.histogram {
                    let mut num_values = 0.0;
                    let mut num_distinct = 0.0;
                    for bucket in histogram.buckets.iter() {
                        num_distinct += bucket.num_distinct();
                        num_values += bucket.num_values();
                    }
                    // When there is a high probability that eager aggregation
                    // is better, we will update the histogram.
                    if num_values / num_distinct >= 10.0 {
                        for bucket in histogram.buckets.iter_mut() {
                            bucket.aggregate_values();
                        }
                    }
                }
            }
            // To avoid res is very large
            f64::min(res, cardinality)
        };

        let precise_cardinality = if self.group_items.is_empty() {
            Some(1)
        } else {
            None
        };
        Ok(Arc::new(StatInfo {
            cardinality,
            statistics: Statistics {
                precise_cardinality,
                column_stats: statistics.column_stats,
            },
        }))
    }

    fn compute_required_prop_children(
        &self,
        ctx: Arc<dyn TableContext>,
        _rel_expr: &RelExpr,
        _required: &RequiredProperty,
    ) -> Result<Vec<Vec<RequiredProperty>>> {
        let mut children_required = vec![];

        match self.mode {
            AggregateMode::Partial => {
                if self.group_items.is_empty() {
                    // Scalar aggregation
                    children_required.push(vec![RequiredProperty {
                        distribution: Distribution::Any,
                    }]);

                    children_required.push(vec![RequiredProperty {
                        distribution: Distribution::Serial,
                    }]);
                } else {
                    let settings = ctx.get_settings();

                    // Group aggregation, enforce `Hash` distribution
                    match settings.get_group_by_shuffle_mode()?.as_str() {
                        "before_partial" => {
                            children_required.push(vec![RequiredProperty {
                                distribution: Distribution::Hash(
                                    self.group_items
                                        .iter()
                                        .map(|item| item.scalar.clone())
                                        .collect(),
                                ),
                            }]);
                        }
                        "before_merge" => {
                            children_required.push(vec![RequiredProperty {
                                distribution: Distribution::Hash(vec![
                                    self.group_items[0].scalar.clone(),
                                ]),
                            }]);
                        }
                        value => {
                            return Err(ErrorCode::Internal(format!(
                                "Bad settings value group_by_shuffle_mode = {:?}",
                                value
                            )));
                        }
                    }
                }
            }

            AggregateMode::Final => {
                if self.group_items.is_empty() {
                    // Scalar aggregation
                    children_required.push(vec![RequiredProperty {
                        distribution: Distribution::Serial,
                    }]);
                } else {
                    // The distribution should have been derived by partial aggregation
                    children_required.push(vec![RequiredProperty {
                        distribution: Distribution::Any,
                    }]);
                }
            }

            AggregateMode::Initial => Err(ErrorCode::Internal(
                "Cannot compute required property for initial aggregate".to_string(),
            ))?,
        }

        Ok(children_required)
    }
}
