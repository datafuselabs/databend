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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::Runtime;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;

use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::optimizer::StatInfo;
use crate::plans::RelOperator;
use crate::plans::Statistics;
use crate::BaseTableColumn;
use crate::ColumnEntry;
use crate::IndexType;
use crate::MetadataRef;

// The CollectStatisticsOptimizer will collect statistics for each leaf node in SExpr.
pub struct CollectStatisticsOptimizer {
    table_ctx: Arc<dyn TableContext>,
    metadata: MetadataRef,
    cte_statistics: HashMap<IndexType, Arc<StatInfo>>,
    async_runtime: Arc<Runtime>,
}

impl CollectStatisticsOptimizer {
    pub fn new(table_ctx: Arc<dyn TableContext>, metadata: MetadataRef) -> Self {
        CollectStatisticsOptimizer {
            table_ctx,
            metadata,
            cte_statistics: HashMap::new(),
            async_runtime: GlobalIORuntime::instance(),
        }
    }

    pub fn run(mut self, s_expr: &SExpr) -> Result<SExpr> {
        let s_expr = self.collect(s_expr)?;
        Ok(s_expr)
    }

    pub fn collect(&mut self, s_expr: &SExpr) -> Result<SExpr> {
        match s_expr.plan.as_ref() {
            RelOperator::Scan(scan) => {
                let table = self.metadata.read().table(scan.table_index).clone();
                let table = table.table();
                let columns = self
                    .metadata
                    .read()
                    .columns_by_table_index(scan.table_index);

                let table_ctx = self.table_ctx.clone();
                let change_type = scan.change_type.clone();
                let (column_statistics_provider, table_stats) =
                    self.async_runtime.block_on(async move {
                        let column_statistics_provider =
                            table.column_statistics_provider(table_ctx.clone()).await?;
                        let table_stats = table
                            .table_statistics(table_ctx.clone(), change_type.clone())
                            .await?;
                        Result::<_, ErrorCode>::Ok((column_statistics_provider, table_stats))
                    })?;

                let mut column_stats = HashMap::new();
                for column in columns.iter() {
                    if let ColumnEntry::BaseTableColumn(BaseTableColumn {
                        column_index,
                        path_indices,
                        leaf_index,
                        virtual_computed_expr,
                        ..
                    }) = column
                    {
                        if path_indices.is_none() && virtual_computed_expr.is_none() {
                            if let Some(col_id) = *leaf_index {
                                let col_stat = column_statistics_provider
                                    .column_statistics(col_id as ColumnId);
                                column_stats.insert(*column_index, col_stat.cloned());
                            }
                        }
                    }
                }

                let mut scan = scan.clone();
                scan.statistics = Arc::new(Statistics {
                    table_stats,
                    column_stats,
                });

                Ok(s_expr.replace_plan(Arc::new(RelOperator::Scan(scan))))
            }
            RelOperator::MaterializedCte(materialized_cte) => {
                let left = self.collect(s_expr.child(0)?)?;
                let cte_stat_info = RelExpr::with_s_expr(&left).derive_cardinality_child(0)?;
                self.cte_statistics
                    .insert(materialized_cte.cte_idx, cte_stat_info);
                let right = self.collect(s_expr.child(1)?)?;
                Ok(s_expr.replace_children(vec![Arc::new(left), Arc::new(right)]))
            }
            RelOperator::CteScan(cte_scan) => {
                let stat_info = self
                    .cte_statistics
                    .get(&cte_scan.cte_idx.0)
                    .unwrap()
                    .clone();
                let mut cte_scan = cte_scan.clone();
                cte_scan.stat = stat_info;
                Ok(s_expr.replace_plan(Arc::new(RelOperator::CteScan(cte_scan))))
            }
            _ => {
                let mut children = Vec::with_capacity(s_expr.arity());
                for child in s_expr.children() {
                    let child = self.collect(child)?;
                    children.push(Arc::new(child));
                }
                Ok(s_expr.replace_children(children))
            }
        }
    }
}
