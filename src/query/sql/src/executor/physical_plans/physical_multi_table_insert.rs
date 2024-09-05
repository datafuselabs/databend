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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::RemoteExpr;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;

use crate::executor::PhysicalPlan;
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Duplicate {
    pub plan_id: u32,
    pub input: Box<PhysicalPlan>,
    pub n: usize,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Shuffle {
    pub plan_id: u32,
    pub input: Box<PhysicalPlan>,
    pub strategy: ShuffleStrategy,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub enum ShuffleStrategy {
    Transpose(usize),
}

impl ShuffleStrategy {
    pub fn shuffle(&self, total: usize) -> Result<Vec<usize>> {
        match self {
            ShuffleStrategy::Transpose(n) => {
                if total % n != 0 {
                    return Err(ErrorCode::Internal(format!(
                        "total rows {} is not divisible by n {}",
                        total, n
                    )));
                }
                let mut result = vec![0; total];
                for i in 0..*n {
                    for j in 0..total / n {
                        result[i + j * n] = i * (total / n) + j;
                    }
                }
                Ok(result)
            }
        }
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ChunkFilter {
    pub plan_id: u32,
    pub input: Box<PhysicalPlan>,
    pub predicates: Vec<Option<RemoteExpr>>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ChunkEvalScalar {
    pub plan_id: u32,
    pub input: Box<PhysicalPlan>,
    pub eval_scalars: Vec<Option<MultiInsertEvalScalar>>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct MultiInsertEvalScalar {
    pub remote_exprs: Vec<RemoteExpr>,
    pub projection: HashSet<usize>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ChunkCastSchema {
    pub plan_id: u32,
    pub input: Box<PhysicalPlan>,
    pub cast_schemas: Vec<Option<CastSchema>>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CastSchema {
    pub source_schema: DataSchemaRef,
    pub target_schema: DataSchemaRef,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ChunkFillAndReorder {
    pub plan_id: u32,
    pub input: Box<PhysicalPlan>,
    pub fill_and_reorders: Vec<Option<FillAndReorder>>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct FillAndReorder {
    pub source_schema: DataSchemaRef,
    pub target_table_info: TableInfo,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ChunkAppendData {
    pub plan_id: u32,
    pub input: Box<PhysicalPlan>,
    pub target_tables: Vec<SerializableTable>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct SerializableTable {
    pub target_catalog_info: Arc<CatalogInfo>,
    pub target_table_info: TableInfo,
    pub table_meta_timestamps: TableMetaTimestamps,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ChunkMerge {
    pub plan_id: u32,
    pub input: Box<PhysicalPlan>,
    pub group_ids: Vec<u64>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct ChunkCommitInsert {
    pub plan_id: u32,
    pub input: Box<PhysicalPlan>,
    pub update_stream_meta: Vec<UpdateStreamMetaReq>,
    pub overwrite: bool,
    pub deduplicated_label: Option<String>,
    pub targets: Vec<SerializableTable>,
}
