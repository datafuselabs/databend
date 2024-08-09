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

use databend_common_catalog::plan::PartInfoType;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::ReclusterParts;
use databend_common_catalog::plan::ReclusterTask;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::TableInfo;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;

use crate::executor::physical_plans::physical_commit_sink::ReclusterInfoSideCar;
use crate::executor::physical_plans::CommitSink;
use crate::executor::physical_plans::CompactSource;
use crate::executor::physical_plans::Exchange;
use crate::executor::physical_plans::FragmentKind;
use crate::executor::physical_plans::MutationKind;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Recluster {
    pub plan_id: u32,
    pub tasks: Vec<ReclusterTask>,
    pub table_info: TableInfo,
    pub table_meta_timestamps: TableMetaTimestamps,
}

impl PhysicalPlanBuilder {
    /// The flow of Pipeline is as follows:
    // ┌──────────┐     ┌───────────────┐     ┌─────────┐
    // │FuseSource├────►│CompoundBlockOp├────►│SortMerge├────┐
    // └──────────┘     └───────────────┘     └─────────┘    │
    // ┌──────────┐     ┌───────────────┐     ┌─────────┐    │     ┌──────────────┐     ┌─────────┐
    // │FuseSource├────►│CompoundBlockOp├────►│SortMerge├────┤────►│MultiSortMerge├────►│Resize(N)├───┐
    // └──────────┘     └───────────────┘     └─────────┘    │     └──────────────┘     └─────────┘   │
    // ┌──────────┐     ┌───────────────┐     ┌─────────┐    │                                        │
    // │FuseSource├────►│CompoundBlockOp├────►│SortMerge├────┘                                        │
    // └──────────┘     └───────────────┘     └─────────┘                                             │
    // ┌──────────────────────────────────────────────────────────────────────────────────────────────┘
    // │         ┌──────────────┐
    // │    ┌───►│SerializeBlock├───┐
    // │    │    └──────────────┘   │
    // │    │    ┌──────────────┐   │    ┌─────────┐    ┌────────────────┐     ┌─────────────┐     ┌──────────┐
    // └───►│───►│SerializeBlock├───┤───►│Resize(1)├───►│SerializeSegment├────►│ReclusterAggr├────►│CommitSink│
    //      │    └──────────────┘   │    └─────────┘    └────────────────┘     └─────────────┘     └──────────┘
    //      │    ┌──────────────┐   │
    //      └───►│SerializeBlock├───┘
    //           └──────────────┘
    pub async fn build_recluster(
        &mut self,
        recluster: &crate::plans::Recluster,
    ) -> Result<PhysicalPlan> {
        let crate::plans::Recluster {
            catalog,
            database,
            table,
            filters,
            limit,
        } = recluster;

        let tenant = self.ctx.get_tenant();
        let catalog = self.ctx.get_catalog(catalog).await?;
        let tbl = catalog.get_table(&tenant, database, table).await?;
        // check mutability
        tbl.check_mutable()?;

        let push_downs = filters.clone().map(|v| PushDownInfo {
            filters: Some(v),
            ..PushDownInfo::default()
        });
        let Some((parts, snapshot)) = tbl.recluster(self.ctx.clone(), push_downs, *limit).await?
        else {
            return Err(ErrorCode::NoNeedToRecluster(format!(
                "No need to do recluster for '{database}'.'{table}'"
            )));
        };
        let table_meta_timestamps = self
            .ctx
            .get_table_meta_timestamps(tbl.get_id(), Some(snapshot.clone()))?;
        if parts.is_empty() {
            return Err(ErrorCode::NoNeedToRecluster(format!(
                "No need to do recluster for '{database}'.'{table}'"
            )));
        }

        let is_distributed = parts.is_distributed(self.ctx.clone());
        let table_info = tbl.get_table_info().clone();
        let mut plan = match parts {
            ReclusterParts::Recluster {
                tasks,
                remained_blocks,
                removed_segment_indexes,
                removed_segment_summary,
            } => {
                let mut root = PhysicalPlan::Recluster(Box::new(Recluster {
                    tasks,
                    table_info: table_info.clone(),
                    plan_id: u32::MAX,
                    table_meta_timestamps,
                }));

                if is_distributed {
                    root = PhysicalPlan::Exchange(Exchange {
                        plan_id: 0,
                        input: Box::new(root),
                        kind: FragmentKind::Merge,
                        keys: vec![],
                        allow_adjust_parallelism: true,
                        ignore_exchange: false,
                    });
                }
                PhysicalPlan::CommitSink(Box::new(CommitSink {
                    input: Box::new(root),
                    table_info,
                    snapshot: Some(snapshot),
                    mutation_kind: MutationKind::Recluster,
                    update_stream_meta: vec![],
                    merge_meta: false,
                    deduplicated_label: None,
                    plan_id: u32::MAX,
                    table_meta_timestamps,
                    recluster_info: Some(ReclusterInfoSideCar {
                        merged_blocks: remained_blocks,
                        removed_segment_indexes,
                        removed_statistics: removed_segment_summary,
                    }),
                }))
            }
            ReclusterParts::Compact(parts) => {
                let merge_meta = parts.partitions_type() == PartInfoType::LazyLevel;
                let mut root = PhysicalPlan::CompactSource(Box::new(CompactSource {
                    parts,
                    table_info: table_info.clone(),
                    column_ids: snapshot.schema.to_leaf_column_id_set(),
                    plan_id: u32::MAX,
                    table_meta_timestamps,
                }));

                if is_distributed {
                    root = PhysicalPlan::Exchange(Exchange {
                        plan_id: 0,
                        input: Box::new(root),
                        kind: FragmentKind::Merge,
                        keys: vec![],
                        allow_adjust_parallelism: true,
                        ignore_exchange: false,
                    });
                }

                PhysicalPlan::CommitSink(Box::new(CommitSink {
                    input: Box::new(root),
                    table_info,
                    snapshot: Some(snapshot),
                    mutation_kind: MutationKind::Compact,
                    update_stream_meta: vec![],
                    merge_meta,
                    deduplicated_label: None,
                    plan_id: u32::MAX,
                    table_meta_timestamps,
                    recluster_info: None,
                }))
            }
        };
        plan.adjust_plan_id(&mut 0);
        Ok(plan)
    }
}
