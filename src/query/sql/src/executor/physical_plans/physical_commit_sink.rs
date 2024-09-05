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

use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;

use crate::executor::physical_plans::common::MutationKind;
use crate::executor::PhysicalPlan;

// serde is required by `PhysicalPlan`
/// The commit sink is used to commit the data to the table.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct CommitSink {
    pub plan_id: u32,
    pub input: Box<PhysicalPlan>,
    pub snapshot: Option<Arc<TableSnapshot>>,
    pub table_info: TableInfo,
    pub mutation_kind: MutationKind,
    pub update_stream_meta: Vec<UpdateStreamMetaReq>,
    pub merge_meta: bool,
    pub deduplicated_label: Option<String>,
    pub table_meta_timestamps: TableMetaTimestamps,

    // Used for recluster.
    pub recluster_info: Option<ReclusterInfoSideCar>,
}

// TODO refine this
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default)]
pub struct ReclusterInfoSideCar {
    pub merged_blocks: Vec<Arc<BlockMeta>>,
    pub removed_segment_indexes: Vec<usize>,
    pub removed_statistics: Statistics,
}
