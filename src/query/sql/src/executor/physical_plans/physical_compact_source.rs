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

use databend_common_catalog::plan::PartInfoType;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_meta_app::schema::TableInfo;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;

use crate::executor::physical_plans::CommitSink;
use crate::executor::physical_plans::Exchange;
use crate::executor::physical_plans::FragmentKind;
use crate::executor::physical_plans::MutationKind;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CompactSource {
    pub plan_id: u32,
    pub parts: Partitions,
    pub table_info: TableInfo,
    pub column_ids: HashSet<ColumnId>,
    pub table_meta_timestamps: TableMetaTimestamps,
}

impl PhysicalPlanBuilder {
    pub async fn build_compact_block(
        &mut self,
        compact_block: &crate::plans::OptimizeCompactBlock,
    ) -> Result<PhysicalPlan> {
        let crate::plans::OptimizeCompactBlock {
            catalog,
            database,
            table,
            limit,
        } = compact_block;

        let tenant = self.ctx.get_tenant();
        let catalog = self.ctx.get_catalog(catalog).await?;
        let tbl = catalog.get_table(&tenant, database, table).await?;
        // check mutability
        tbl.check_mutable()?;

        let table_info = tbl.get_table_info().clone();

        let Some((parts, snapshot)) = tbl.compact_blocks(self.ctx.clone(), limit.clone()).await?
        else {
            return Err(ErrorCode::NoNeedToCompact(format!(
                "No need to do compact for '{database}'.'{table}'"
            )));
        };

        let table_meta_timestamps = self
            .ctx
            .get_table_meta_timestamps(table_info.ident.table_id, Some(snapshot.clone()))?;

        let merge_meta = parts.partitions_type() == PartInfoType::LazyLevel;
        let mut root = PhysicalPlan::CompactSource(Box::new(CompactSource {
            parts,
            table_info: table_info.clone(),
            column_ids: snapshot.schema.to_leaf_column_id_set(),
            plan_id: u32::MAX,
            table_meta_timestamps,
        }));

        let is_distributed = (!self.ctx.get_cluster().is_empty())
            && self.ctx.get_settings().get_enable_distributed_compact()?;
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

        root = PhysicalPlan::CommitSink(Box::new(CommitSink {
            input: Box::new(root),
            table_info,
            snapshot: Some(snapshot),
            mutation_kind: MutationKind::Compact,
            update_stream_meta: vec![],
            merge_meta,
            deduplicated_label: None,
            plan_id: u32::MAX,
            recluster_info: None,
            table_meta_timestamps,
        }));

        root.adjust_plan_id(&mut 0);
        Ok(root)
    }
}
