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

use databend_common_expression::BlockThresholds;
use databend_common_expression::FieldIndex;
use databend_common_meta_app::schema::TableInfo;
use databend_storages_common_table_meta::meta::BlockSlotDescription;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;

use crate::executor::physical_plans::common::OnConflictField;
use crate::executor::PhysicalPlan;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ReplaceInto {
    /// A unique id of operator in a `PhysicalPlan` tree.
    pub plan_id: u32,

    pub input: Box<PhysicalPlan>,
    pub block_thresholds: BlockThresholds,
    pub table_info: TableInfo,
    pub on_conflicts: Vec<OnConflictField>,
    pub bloom_filter_column_indexes: Vec<FieldIndex>,
    pub segments: Vec<(usize, Location)>,
    pub block_slots: Option<BlockSlotDescription>,
    pub need_insert: bool,
    pub table_meta_timestamps: TableMetaTimestamps,
}
