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

use std::cmp::Ordering;
use std::collections::HashMap;

use common_expression::ColumnId;

use crate::meta::ColumnStatistics;

pub type FormatVersion = u64;

pub type Location = (String, FormatVersion);
pub type ClusterKey = (u32, String);
pub type StatisticsOfColumns = HashMap<ColumnId, ColumnStatistics>;

// Assigned to executors, describes that which blocks of given segment, an executor should take care of
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
pub struct BlockSlotDescription {
    // number of slots
    pub num_slots: usize,
    // index of slot that current executor should take care of:
    // let `block_index` be the index of block in segment,
    // `block_index` mod `num_slots` == `slot_index` indicates that the block should be taken care of by current executor
    // otherwise, the block should be taken care of by other executors
    pub slot: u32,
}

#[derive(serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum MinMax<T> {
    // min eq max
    Point(T),
    // inclusive on both sides, min < max
    Range(T, T),
}

impl<T: Ord + Clone> MinMax<T> {
    pub fn new(min: T, max: T) -> Self {
        if min == max {
            Self::Point(min)
        } else {
            Self::Range(min, max)
        }
    }

    pub fn update(&mut self, value: T) {
        match self {
            MinMax::Point(v) => {
                let v = v.clone();
                match v.cmp(&value) {
                    Ordering::Less => *self = MinMax::Range(v, value),
                    Ordering::Greater => *self = MinMax::Range(value, v),
                    // value cannot be null
                    Ordering::Equal => (),
                }
            }
            MinMax::Range(min, max) => {
                if value > *max {
                    *max = value
                } else if value < *min {
                    *min = value
                }
            }
        }
    }

    pub fn min(&self) -> &T {
        match self {
            MinMax::Point(v) => v,
            MinMax::Range(v, _) => v,
        }
    }

    pub fn max(&self) -> &T {
        match self {
            MinMax::Point(v) => v,
            MinMax::Range(_, v) => v,
        }
    }
}

#[cfg(test)]
mod tests {
    use common_exception::Result;
    use common_expression::types::NumberScalar;
    use common_expression::Scalar;

    use crate::meta::MinMax;

    #[test]
    fn test_minmax() -> Result<()> {
        let a = Scalar::Number(NumberScalar::Int16(1));
        let b = Scalar::Number(NumberScalar::Int16(2));
        let mut t1 = MinMax::new(b.clone(), b.clone());
        assert_eq!(t1, MinMax::Point(Scalar::Number(NumberScalar::Int16(2))));
        t1.update(a.clone());
        assert_eq!(&Scalar::Number(NumberScalar::Int16(1)), t1.min());
        assert_eq!(&Scalar::Number(NumberScalar::Int16(2)), t1.max());

        let mut t2 = MinMax::new(a, Scalar::Null);
        assert_eq!(
            t2,
            MinMax::Range(Scalar::Number(NumberScalar::Int16(1)), Scalar::Null)
        );
        t2.update(b);
        assert_eq!(&Scalar::Number(NumberScalar::Int16(1)), t2.min());
        assert_eq!(&Scalar::Null, t2.max());

        Ok(())
    }
}
