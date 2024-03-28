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

use std::any::Any;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchema;
use databend_common_metrics::storage::*;
use databend_storages_common_table_meta::meta::ClusterKey;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::TableSnapshot;
use log::info;
use uuid::Uuid;

use crate::operations::common::ConflictResolveContext;
use crate::operations::common::SnapshotGenerator;
use crate::statistics::merge_statistics;
use crate::statistics::reducers::deduct_statistics_mut;

#[derive(Clone)]
pub struct MutationGenerator {
    base_snapshot: Arc<TableSnapshot>,
    conflict_resolve_ctx: ConflictResolveContext,
    replace_snapshots: bool,
}

impl MutationGenerator {
    pub fn new(base_snapshot: Arc<TableSnapshot>, replace_snapshots: bool) -> Self {
        MutationGenerator {
            base_snapshot,
            conflict_resolve_ctx: ConflictResolveContext::None,
            replace_snapshots,
        }
    }
}

impl SnapshotGenerator for MutationGenerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn set_conflict_resolve_context(&mut self, ctx: ConflictResolveContext) {
        self.conflict_resolve_ctx = ctx;
    }

    fn generate_new_snapshot(
        &self,
        schema: TableSchema,
        cluster_key_meta: Option<ClusterKey>,
        previous: Option<Arc<TableSnapshot>>,
        prev_table_seq: Option<u64>,
    ) -> Result<TableSnapshot> {
        let default_cluster_key_id = cluster_key_meta.clone().map(|v| v.0);

        let previous = previous.unwrap_or_else(|| {
            Arc::new(TableSnapshot::new_empty_snapshot(
                schema.clone(),
                prev_table_seq,
            ))
        });
        match &self.conflict_resolve_ctx {
            ConflictResolveContext::ModifiedSegmentExistsInLatest(ctx) => {
                if let Some((removed, replaced)) =
                    ConflictResolveContext::is_modified_segments_exists_in_latest(
                        &self.base_snapshot,
                        &previous,
                        &ctx.replaced_segments,
                        &ctx.removed_segment_indexes,
                    )
                {
                    info!("resolvable conflicts detected");
                    metrics_inc_commit_mutation_modified_segment_exists_in_latest();
                    let new_segments = ConflictResolveContext::merge_segments(
                        if self.replace_snapshots {
                            vec![]
                        } else {
                            previous.segments.clone()
                        },
                        ctx.appended_segments.clone(),
                        replaced,
                        removed,
                    );
                    let dummy_stats = Statistics::default();
                    let mut new_summary = merge_statistics(
                        &ctx.merged_statistics,
                        if self.replace_snapshots {
                            &dummy_stats
                        } else {
                            &previous.summary
                        },
                        default_cluster_key_id,
                    );
                    deduct_statistics_mut(&mut new_summary, &ctx.removed_statistics);
                    let new_snapshot = TableSnapshot::new(
                        Uuid::new_v4(),
                        prev_table_seq,
                        &previous.timestamp,
                        Some((previous.snapshot_id, previous.format_version)),
                        schema,
                        new_summary,
                        new_segments,
                        cluster_key_meta,
                        previous.table_statistics_location.clone(),
                        previous.index_info_locations.clone(),
                    );
                    Ok(new_snapshot)
                } else {
                    metrics_inc_commit_mutation_unresolvable_conflict();
                    Err(ErrorCode::UnresolvableConflict(format!(
                        "conflict resolve context:{:?}",
                        self.conflict_resolve_ctx
                    )))
                }
            }
            _ => Err(ErrorCode::Internal(
                "conflict_resolve_ctx should not be AppendOnly in MutationGenerator",
            )),
        }
    }
}
