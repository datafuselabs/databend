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

use databend_storages_common_table_meta::meta::Location;

use crate::table::Table;

#[derive(Clone)]
pub enum MergeIntoJoinType {
    Left,
    Right,
    Inner,
    LeftAnti,
    RightAnti,
    // it means this join is not a merge into join
    NormalJoin,
}

pub type MergeIntoSourceBuildSegments = Arc<Vec<Location>>;

// MergeIntoJoin is used in two cases:
// I. target build optimization:
// we should support MergeIntoJoinType::Left(need to support LeftInner,LeftAnti) to use MergeIntoBlockInfoHashTable in two situations:
// 1. distributed broadcast join and target table as build side (don't support this now).
// 2. in standalone mode and target table as build side (supported).
// 3. native(not-supported) and parquet(supported) format
// for the `target build optimization`, merge_into_join_type is only Left for current implementation in fact.And we
// don't support distributed mode and parquet. So only if it is a LeftJoin and non-dirtributed-merge-into(even if in distributed environment
// but the merge_into_optimizer can also give a non-dirtributed-merge-into) and uses parquet, the `target build optimization` can be enabled.
// II. source build runtime bloom filter.
// only if it's a RightJoin((need to support RightInner,RightAnti)),target_tbl_idx is not invalid. We can make sure it enables
// `source build runtime bloom filter`. And in this case the table_info and catalog_info must be some. We give an `assert` for this judge.
//
// So let's do a summary:
// In most cases, the MergeIntoJoin's `merge_into_join_type` is NormalJoin (even this is a real merge into join),because
// MergeIntoJoin is used to judge whether we enable target_build_optimizations or source build runtime bloom filter.
// Both of these optimizations all will check  `merge_into_join_type` to judge if the correlated optimization is enabled.
// So we only `set_merge_into_join` when we use these two optimziations (of course there is an exception, the `merge_into_optimzier` can't distinct
// parquet and native, so it will think it's using parquet format in default, so if it's a native format in fact, the `interpreter_merge_into` will rollback it.
// But the modification is a little tricky. Because the `MergeIntoJoinType` doesn't contain any info about parquet or native,we set the target_table_index as
// DUMMY_TABLE_INDEX, but in fact it has a target_table_index. However it's ok to do this. Firstly we can do this to make sure the `target build optimization` is
// closed. And it won't cause a conflict with `source build runtime bloom filter`. Because for the `target build optimization`, it's target build, and for
// `source build runtime bloom filter`, it's source build).
pub struct MergeIntoJoin {
    pub merge_into_join_type: MergeIntoJoinType,
    pub is_distributed: bool,
    pub target_tbl_idx: usize,
    pub table: Option<Arc<dyn Table>>,
}

impl Default for MergeIntoJoin {
    fn default() -> Self {
        Self {
            merge_into_join_type: MergeIntoJoinType::NormalJoin,
            is_distributed: false,
            // Invalid Index
            target_tbl_idx: usize::MAX,
            table: None,
        }
    }
}
