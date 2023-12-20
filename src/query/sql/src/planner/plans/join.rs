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

use std::cmp::max;
use std::collections::HashMap;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::F64;
use databend_common_storage::Datum;

use crate::optimizer::histogram_from_ndv;
use crate::optimizer::ColumnSet;
use crate::optimizer::ColumnStat;
use crate::optimizer::Distribution;
use crate::optimizer::Histogram;
use crate::optimizer::NewStatistic;
use crate::optimizer::PhysicalProperty;
use crate::optimizer::RelExpr;
use crate::optimizer::RelationalProperty;
use crate::optimizer::RequiredProperty;
use crate::optimizer::StatInfo;
use crate::optimizer::Statistics;
use crate::optimizer::UniformSampleSet;
use crate::optimizer::DEFAULT_HISTOGRAM_BUCKETS;
use crate::plans::Operator;
use crate::plans::RelOp;
use crate::plans::ScalarExpr;
use crate::IndexType;

#[derive(Clone, Debug, Eq, PartialEq, Hash, serde::Serialize, serde::Deserialize)]
pub enum JoinType {
    Cross,
    Inner,
    Left,
    Right,
    Full,
    LeftSemi,
    RightSemi,
    LeftAnti,
    RightAnti,
    /// Mark Join is a special case of join that is used to process Any subquery and correlated Exists subquery.
    /// Left Mark Join use subquery as probe side, it's blocked at `mark_join_blocks`
    LeftMark,
    /// Right Mark Join use subquery as build side, it's executed by streaming.
    RightMark,
    /// Single Join is a special kind of join that is used to process correlated scalar subquery.
    LeftSingle,
    RightSingle,
}

impl JoinType {
    pub fn opposite(&self) -> JoinType {
        match self {
            JoinType::Left => JoinType::Right,
            JoinType::Right => JoinType::Left,
            JoinType::LeftSingle => JoinType::RightSingle,
            JoinType::RightSingle => JoinType::LeftSingle,
            JoinType::LeftSemi => JoinType::RightSemi,
            JoinType::RightSemi => JoinType::LeftSemi,
            JoinType::LeftAnti => JoinType::RightAnti,
            JoinType::RightAnti => JoinType::LeftAnti,
            JoinType::LeftMark => JoinType::RightMark,
            JoinType::RightMark => JoinType::LeftMark,
            _ => self.clone(),
        }
    }

    pub fn is_outer_join(&self) -> bool {
        matches!(self, JoinType::Left | JoinType::Right | JoinType::Full)
    }

    pub fn is_mark_join(&self) -> bool {
        matches!(self, JoinType::LeftMark | JoinType::RightMark)
    }
}

impl Display for JoinType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            JoinType::Inner => {
                write!(f, "INNER")
            }
            JoinType::Left => {
                write!(f, "LEFT OUTER")
            }
            JoinType::Right => {
                write!(f, "RIGHT OUTER")
            }
            JoinType::Full => {
                write!(f, "FULL OUTER")
            }
            JoinType::LeftSemi => {
                write!(f, "LEFT SEMI")
            }
            JoinType::LeftAnti => {
                write!(f, "LEFT ANTI")
            }
            JoinType::RightSemi => {
                write!(f, "RIGHT SEMI")
            }
            JoinType::RightAnti => {
                write!(f, "RIGHT ANTI")
            }
            JoinType::Cross => {
                write!(f, "CROSS")
            }
            JoinType::LeftMark => {
                write!(f, "LEFT MARK")
            }
            JoinType::RightMark => {
                write!(f, "RIGHT MARK")
            }
            JoinType::LeftSingle => {
                write!(f, "LEFT SINGLE")
            }
            JoinType::RightSingle => {
                write!(f, "RIGHT SINGLE")
            }
        }
    }
}

/// Join operator. We will choose hash join by default.
/// In the case that using hash join, the right child
/// is always the build side, and the left child is always
/// the probe side.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Join {
    pub left_conditions: Vec<ScalarExpr>,
    pub right_conditions: Vec<ScalarExpr>,
    pub non_equi_conditions: Vec<ScalarExpr>,
    pub join_type: JoinType,
    // marker_index is for MarkJoin only.
    pub marker_index: Option<IndexType>,
    pub from_correlated_subquery: bool,
    // if we execute distributed merge into, we need to hold the
    // hash table to get not match data from source.
    pub need_hold_hash_table: bool,
    // Under cluster, mark if the join is broadcast join.
    pub broadcast: bool,
}

impl Default for Join {
    fn default() -> Self {
        Self {
            left_conditions: Default::default(),
            right_conditions: Default::default(),
            non_equi_conditions: Default::default(),
            join_type: JoinType::Cross,
            marker_index: Default::default(),
            from_correlated_subquery: Default::default(),
            need_hold_hash_table: false,
            broadcast: false,
        }
    }
}

impl Join {
    pub fn used_columns(&self) -> Result<ColumnSet> {
        let mut used_columns = ColumnSet::new();
        for cond in self
            .left_conditions
            .iter()
            .chain(self.right_conditions.iter())
            .chain(self.non_equi_conditions.iter())
        {
            used_columns = used_columns.union(&cond.used_columns()).cloned().collect();
        }
        Ok(used_columns)
    }

    fn inner_join_cardinality(
        &self,
        left_cardinality: &mut f64,
        right_cardinality: &mut f64,
        left_statistics: &mut Statistics,
        right_statistics: &mut Statistics,
    ) -> Result<f64> {
        let mut join_card = *left_cardinality * *right_cardinality;
        let mut join_card_updated = false;
        let mut left_column_index = 0;
        let mut right_column_index = 0;
        for (left_condition, right_condition) in self
            .left_conditions
            .iter()
            .zip(self.right_conditions.iter())
        {
            if join_card == 0 as f64 {
                break;
            }
            // Currently don't consider the case such as: `t1.a + t1.b = t2.a`
            if left_condition.used_columns().len() != 1 || right_condition.used_columns().len() != 1
            {
                continue;
            }
            let left_col_stat = left_statistics
                .column_stats
                .get(left_condition.used_columns().iter().next().unwrap());
            let right_col_stat = right_statistics
                .column_stats
                .get(right_condition.used_columns().iter().next().unwrap());
            match (left_col_stat, right_col_stat) {
                (Some(left_col_stat), Some(right_col_stat)) => {
                    if !left_col_stat.min.type_comparable(&right_col_stat.min) {
                        continue;
                    }
                    let left_interval =
                        UniformSampleSet::new(left_col_stat.min.clone(), left_col_stat.max.clone());
                    let right_interval = UniformSampleSet::new(
                        right_col_stat.min.clone(),
                        right_col_stat.max.clone(),
                    );
                    if !left_interval.has_intersection(&right_interval)? {
                        join_card = 0.0;
                        continue;
                    }

                    // Update column min and max value
                    let mut new_ndv = None;
                    let (new_min, new_max) = left_interval.intersection(&right_interval)?;

                    if let Datum::Bytes(_) | Datum::Bool(_) = left_col_stat.min {
                        let card = evaluate_by_ndv(
                            left_col_stat,
                            right_col_stat,
                            *left_cardinality,
                            *right_cardinality,
                            &mut new_ndv,
                        );
                        let (left_index, right_index) = update_statistic(
                            left_statistics,
                            right_statistics,
                            left_condition,
                            right_condition,
                            NewStatistic {
                                min: new_min,
                                max: new_max,
                                ndv: new_ndv,
                            },
                        );
                        if card < join_card {
                            join_card = card;
                            join_card_updated = true;
                            left_column_index = left_index;
                            right_column_index = right_index;
                        }
                        continue;
                    }
                    let card = match (&left_col_stat.histogram, &right_col_stat.histogram) {
                        (Some(left_hist), Some(right_hist)) => {
                            // Evaluate join cardinality by histogram.
                            evaluate_by_histogram(left_hist, right_hist, &mut new_ndv)?
                        }
                        _ => evaluate_by_ndv(
                            left_col_stat,
                            right_col_stat,
                            *left_cardinality,
                            *right_cardinality,
                            &mut new_ndv,
                        ),
                    };
                    let (left_index, right_index) = update_statistic(
                        left_statistics,
                        right_statistics,
                        left_condition,
                        right_condition,
                        NewStatistic {
                            min: new_min,
                            max: new_max,
                            ndv: new_ndv,
                        },
                    );
                    if card < join_card {
                        join_card = card;
                        join_card_updated = true;
                        left_column_index = left_index;
                        right_column_index = right_index;
                    }
                }
                _ => continue,
            }
        }

        if join_card_updated {
            for (idx, left) in left_statistics.column_stats.iter_mut() {
                if *idx == left_column_index {
                    if left.histogram.is_some() {
                        left.histogram = if left.ndv as u64 <= 2 {
                            None
                        } else {
                            if matches!(left.min, Datum::Int(_) | Datum::UInt(_) | Datum::Float(_))
                            {
                                left.min = Datum::Float(F64::from(left.min.to_double()?));
                                left.max = Datum::Float(F64::from(left.max.to_double()?));
                            }
                            Some(histogram_from_ndv(
                                left.ndv as u64,
                                max(join_card as u64, left.ndv as u64),
                                Some((left.min.clone(), left.max.clone())),
                                DEFAULT_HISTOGRAM_BUCKETS,
                            )?)
                        }
                    }
                    continue;
                }
                // Other columns' histograms are inaccurate, so make them None
                left.histogram = None;
            }
            for (idx, right) in right_statistics.column_stats.iter_mut() {
                if *idx == right_column_index {
                    if right.histogram.is_some() {
                        right.histogram = if right.ndv as u64 <= 2 {
                            None
                        } else {
                            if matches!(right.min, Datum::Int(_) | Datum::UInt(_) | Datum::Float(_))
                            {
                                right.min = Datum::Float(F64::from(right.min.to_double()?));
                                right.max = Datum::Float(F64::from(right.max.to_double()?));
                            }
                            Some(histogram_from_ndv(
                                right.ndv as u64,
                                max(join_card as u64, right.ndv as u64),
                                Some((right.min.clone(), right.max.clone())),
                                DEFAULT_HISTOGRAM_BUCKETS,
                            )?)
                        }
                    }
                    continue;
                }
                right.histogram = None;
            }
        }

        Ok(join_card)
    }
}

impl Operator for Join {
    fn rel_op(&self) -> RelOp {
        RelOp::Join
    }

    fn derive_relational_prop(&self, rel_expr: &RelExpr) -> Result<Arc<RelationalProperty>> {
        let left_prop = rel_expr.derive_relational_prop_child(0)?;
        let right_prop = rel_expr.derive_relational_prop_child(1)?;
        // Derive output columns
        let mut output_columns = left_prop.output_columns.clone();
        if let Some(mark_index) = self.marker_index {
            output_columns.insert(mark_index);
        }
        output_columns = output_columns
            .union(&right_prop.output_columns)
            .cloned()
            .collect();

        // Derive outer columns
        let mut outer_columns = left_prop.outer_columns.clone();
        outer_columns = outer_columns
            .union(&right_prop.outer_columns)
            .cloned()
            .collect();
        for cond in self
            .left_conditions
            .iter()
            .chain(self.right_conditions.iter())
        {
            let used_columns = cond.used_columns();
            let outer = used_columns.difference(&output_columns).cloned().collect();
            outer_columns = outer_columns.union(&outer).cloned().collect();
        }
        outer_columns = outer_columns.difference(&output_columns).cloned().collect();

        // Derive used columns
        let mut used_columns = self.used_columns()?;
        used_columns.extend(left_prop.used_columns.clone());
        used_columns.extend(right_prop.used_columns.clone());

        // Derive orderings
        let orderings = vec![];

        Ok(Arc::new(RelationalProperty {
            output_columns,
            outer_columns,
            used_columns,
            orderings,
        }))
    }

    fn derive_physical_prop(&self, rel_expr: &RelExpr) -> Result<PhysicalProperty> {
        let probe_prop = rel_expr.derive_physical_prop_child(0)?;
        let build_prop = rel_expr.derive_physical_prop_child(1)?;

        match (&probe_prop.distribution, &build_prop.distribution) {
            // If the distribution of probe side is Random, we will pass through
            // the distribution of build side.
            (Distribution::Random, _) => Ok(PhysicalProperty {
                distribution: build_prop.distribution.clone(),
            }),
            // If both sides are broadcast, which means broadcast join is enabled, to make sure the current join is broadcast, should return Random.
            // Then required proper is broadcast, and the join will be broadcast.
            (Distribution::Broadcast, Distribution::Broadcast) => Ok(PhysicalProperty {
                distribution: Distribution::Random,
            }),
            // Otherwise pass through probe side.
            _ => Ok(PhysicalProperty {
                distribution: probe_prop.distribution.clone(),
            }),
        }
    }

    fn derive_cardinality(&self, rel_expr: &RelExpr) -> Result<Arc<StatInfo>> {
        let left_stat_info = rel_expr.derive_cardinality_child(0)?;
        let right_stat_info = rel_expr.derive_cardinality_child(1)?;
        let (mut left_cardinality, mut left_statistics) = (
            left_stat_info.cardinality,
            left_stat_info.statistics.clone(),
        );
        let (mut right_cardinality, mut right_statistics) = (
            right_stat_info.cardinality,
            right_stat_info.statistics.clone(),
        );
        // Evaluating join cardinality using histograms.
        // If histogram is None, will evaluate using NDV.
        let inner_join_cardinality = self.inner_join_cardinality(
            &mut left_cardinality,
            &mut right_cardinality,
            &mut left_statistics,
            &mut right_statistics,
        )?;
        let cardinality = match self.join_type {
            JoinType::Inner | JoinType::Cross => inner_join_cardinality,
            JoinType::Left => f64::max(left_cardinality, inner_join_cardinality),
            JoinType::Right => f64::max(right_cardinality, inner_join_cardinality),
            JoinType::Full => {
                f64::max(left_cardinality, inner_join_cardinality)
                    + f64::max(right_cardinality, inner_join_cardinality)
                    - inner_join_cardinality
            }
            JoinType::LeftSemi => f64::min(left_cardinality, inner_join_cardinality),
            JoinType::RightSemi => f64::min(right_cardinality, inner_join_cardinality),
            JoinType::LeftSingle | JoinType::RightMark | JoinType::LeftAnti => left_cardinality,
            JoinType::RightSingle | JoinType::LeftMark | JoinType::RightAnti => right_cardinality,
        };
        // Derive column statistics
        let column_stats = if cardinality == 0.0 {
            HashMap::new()
        } else {
            let mut column_stats = HashMap::new();
            column_stats.extend(left_statistics.column_stats);
            column_stats.extend(right_statistics.column_stats);
            column_stats
        };
        Ok(Arc::new(StatInfo {
            cardinality,
            statistics: Statistics {
                precise_cardinality: None,
                column_stats,
            },
        }))
    }

    fn compute_required_prop_child(
        &self,
        ctx: Arc<dyn TableContext>,
        rel_expr: &RelExpr,
        child_index: usize,
        required: &RequiredProperty,
    ) -> Result<RequiredProperty> {
        let mut required = required.clone();

        let probe_physical_prop = rel_expr.derive_physical_prop_child(0)?;
        let build_physical_prop = rel_expr.derive_physical_prop_child(1)?;

        // if join/probe side is Serial or join key is empty, we use Serial distribution
        if probe_physical_prop.distribution == Distribution::Serial
            || build_physical_prop.distribution == Distribution::Serial
        {
            // TODO(leiysky): we can enforce redistribution here
            required.distribution = Distribution::Serial;
            return Ok(required);
        } else if ctx.get_settings().get_prefer_broadcast_join()?
            && !matches!(
                self.join_type,
                JoinType::Right
                    | JoinType::Full
                    | JoinType::RightAnti
                    | JoinType::RightSemi
                    | JoinType::RightMark
            )
        {
            let left_stat_info = rel_expr.derive_cardinality_child(0)?;
            let right_stat_info = rel_expr.derive_cardinality_child(1)?;
            if right_stat_info.cardinality * 10.0 <= left_stat_info.cardinality {
                required.distribution = Distribution::Broadcast;
                return Ok(required);
            }
        }
        if child_index == 0 {
            required.distribution = Distribution::Hash(self.left_conditions.clone());
        } else {
            required.distribution = Distribution::Hash(self.right_conditions.clone());
        }

        Ok(required)
    }
}

fn evaluate_by_histogram(
    left_hist: &Histogram,
    right_hist: &Histogram,
    new_ndv: &mut Option<f64>,
) -> Result<f64> {
    let mut card = 0.0;
    let mut all_ndv = 0.0;
    for (left_idx, left_bucket) in left_hist.buckets.iter().enumerate() {
        if left_idx == 0 {
            continue;
        }
        let mut has_intersection = false;
        let left_num_rows = left_bucket.num_values();
        let left_ndv = left_bucket.num_distinct();
        let left_bucket_min = left_hist.buckets[left_idx - 1].upper_bound().to_double()?;
        let left_bucket_max = left_bucket.upper_bound().to_double()?;
        for (right_idx, right_bucket) in right_hist.buckets.iter().enumerate() {
            if right_idx == 0 {
                continue;
            }
            let right_bucket_min = right_hist.buckets[right_idx - 1]
                .upper_bound()
                .to_double()?;
            let right_bucket_max = right_bucket.upper_bound().to_double()?;
            if left_bucket_min < right_bucket_max && left_bucket_max > right_bucket_min {
                has_intersection = true;
                let right_num_rows = right_bucket.num_values();
                let right_ndv = right_bucket.num_distinct();

                // There are four cases for interleaving
                // 1. left bucket contains right bucket
                // ---left_min---right_min---right_max---left_max---
                if right_bucket_min >= left_bucket_min && right_bucket_max <= left_bucket_max {
                    let percentage =
                        (right_bucket_max - right_bucket_min) / (left_bucket_max - left_bucket_min);

                    let left_ndv = left_ndv * percentage;
                    let left_num_rows = left_num_rows * percentage;

                    let max_ndv = f64::max(left_ndv, right_ndv);
                    if max_ndv > 0.0 {
                        all_ndv += left_ndv.min(right_ndv);
                        card += left_num_rows * right_num_rows / max_ndv;
                    }
                } else if left_bucket_min >= right_bucket_min && left_bucket_max <= right_bucket_max
                {
                    // 2. right bucket contains left bucket
                    // ---right_min---left_min---left_max---right_max---
                    let percentage =
                        (left_bucket_max - left_bucket_min) / (right_bucket_max - right_bucket_min);

                    let right_ndv = right_ndv * percentage;
                    let right_num_rows = right_num_rows * percentage;

                    let max_ndv = f64::max(left_ndv, right_ndv);
                    if max_ndv > 0.0 {
                        all_ndv += left_ndv.min(right_ndv);
                        card += left_num_rows * right_num_rows / max_ndv;
                    }
                } else if left_bucket_min <= right_bucket_min && left_bucket_max <= right_bucket_max
                {
                    // 3. left bucket intersects with right bucket on the left
                    // ---left_min---right_min---left_max---right_max---
                    if left_bucket_max == right_bucket_min {
                        continue;
                    }
                    let left_percentage =
                        (left_bucket_max - right_bucket_min) / (left_bucket_max - left_bucket_min);
                    let right_percentage = (left_bucket_max - right_bucket_min)
                        / (right_bucket_max - right_bucket_min);

                    let left_ndv = left_ndv * left_percentage;
                    let left_num_rows = left_num_rows * left_percentage;
                    let right_ndv = right_ndv * right_percentage;
                    let right_num_rows = right_num_rows * right_percentage;

                    let max_ndv = f64::max(left_ndv, right_ndv);
                    if max_ndv > 0.0 {
                        all_ndv += left_ndv.min(right_ndv);
                        card += left_num_rows * right_num_rows / max_ndv;
                    }
                } else if left_bucket_min >= right_bucket_min && left_bucket_max >= right_bucket_max
                {
                    // 4. left bucket intersects with right bucket on the right
                    // ---right_min---left_min---right_max---left_max---
                    if right_bucket_max == left_bucket_min {
                        continue;
                    }
                    let left_percentage =
                        (right_bucket_max - left_bucket_min) / (left_bucket_max - left_bucket_min);
                    let right_percentage = (right_bucket_max - left_bucket_min)
                        / (right_bucket_max - right_bucket_min);

                    let left_ndv = left_ndv * left_percentage;
                    let left_num_rows = left_num_rows * left_percentage;
                    let right_ndv = right_ndv * right_percentage;
                    let right_num_rows = right_num_rows * right_percentage;

                    let max_ndv = f64::max(left_ndv, right_ndv);
                    if max_ndv > 0.0 {
                        all_ndv += left_ndv.min(right_ndv);
                        card += left_num_rows * right_num_rows / max_ndv;
                    }
                }
            } else if has_intersection {
                break;
            }
        }
    }
    *new_ndv = Some(all_ndv.ceil());
    Ok(card)
}

fn evaluate_by_ndv(
    left_stat: &ColumnStat,
    right_stat: &ColumnStat,
    left_cardinality: f64,
    right_cardinality: f64,
    new_ndv: &mut Option<f64>,
) -> f64 {
    // Update column ndv
    *new_ndv = Some(left_stat.ndv.min(right_stat.ndv));

    let max_ndv = f64::max(left_stat.ndv, right_stat.ndv);
    if max_ndv == 0.0 {
        0.0
    } else {
        left_cardinality * right_cardinality / max_ndv
    }
}

fn update_statistic(
    left_statistics: &mut Statistics,
    right_statistics: &mut Statistics,
    left_condition: &ScalarExpr,
    right_condition: &ScalarExpr,
    new_stat: NewStatistic,
) -> (usize, usize) {
    let left_index = *left_condition.used_columns().iter().next().unwrap();
    let right_index = *right_condition.used_columns().iter().next().unwrap();
    let left_col_stat = left_statistics.column_stats.get_mut(&left_index).unwrap();
    let right_col_stat = right_statistics.column_stats.get_mut(&right_index).unwrap();
    if let Some(new_min) = new_stat.min {
        left_col_stat.min = new_min.clone();
        right_col_stat.min = new_min;
    }
    if let Some(new_max) = new_stat.max {
        left_col_stat.max = new_max.clone();
        right_col_stat.max = new_max;
    }
    if let Some(new_ndv) = new_stat.ndv {
        left_col_stat.ndv = new_ndv;
        right_col_stat.ndv = new_ndv;
    }
    (left_index, right_index)
}
