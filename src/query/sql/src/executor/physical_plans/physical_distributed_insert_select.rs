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

use databend_common_expression::DataSchemaRef;
use databend_common_meta_app::schema::TableInfo;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;

use crate::executor::PhysicalPlan;
use crate::ColumnBinding;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct DistributedInsertSelect {
    /// A unique id of operator in a `PhysicalPlan` tree.
    pub plan_id: u32,

    pub input: Box<PhysicalPlan>,
    pub table_info: TableInfo,
    pub insert_schema: DataSchemaRef,
    pub select_schema: DataSchemaRef,
    pub select_column_bindings: Vec<ColumnBinding>,
    pub cast_needed: bool,
    pub table_meta_timestamps: TableMetaTimestamps,
}
