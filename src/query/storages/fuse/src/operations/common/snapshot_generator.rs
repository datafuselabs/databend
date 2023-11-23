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
use std::collections::HashSet;
use std::ops::Range;
use std::sync::Arc;

use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::ColumnId;
use common_expression::Scalar;
use common_expression::TableSchema;
use common_expression::TableSchemaRef;
use common_metrics::storage::*;
use common_sql::field_default_value;
use log::info;
use storages_common_table_meta::meta::ClusterKey;
use storages_common_table_meta::meta::ColumnStatistics;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::Statistics;
use storages_common_table_meta::meta::TableSnapshot;
use storages_common_table_meta::meta::TableVersion;

use crate::statistics::merge_statistics;
use crate::statistics::reducers::deduct_statistics_mut;
use crate::statistics::reducers::merge_statistics_mut;

#[async_trait::async_trait]
pub trait SnapshotGenerator {
    fn set_conflict_resolve_context(&mut self, ctx: ConflictResolveContext);

    async fn fill_default_values(
        &mut self,
        _schema: TableSchema,
        _snapshot: &Option<Arc<TableSnapshot>>,
    ) -> Result<()> {
        Ok(())
    }

    fn generate_new_snapshot(
        &self,
        schema: TableSchema,
        cluster_key_meta: Option<ClusterKey>,
        previous: Option<Arc<TableSnapshot>>,
    ) -> Result<TableSnapshot>;
}

#[derive(Clone, serde::Serialize, serde::Deserialize, Debug, PartialEq, Default)]
pub struct SnapshotChanges {
    pub appended_segments: Vec<Location>,
    pub replaced_segments: HashMap<usize, Location>,
    pub removed_segment_indexes: Vec<usize>,

    pub merged_statistics: Statistics,
    pub removed_statistics: Statistics,
}

impl SnapshotChanges {
    pub fn check_intersect(&self, other: &SnapshotChanges) -> bool {
        if Self::is_slice_intersect(&self.appended_segments, &other.appended_segments) {
            return true;
        }
        for o in &other.replaced_segments {
            if self.replaced_segments.contains_key(o.0) {
                return true;
            }
        }
        if Self::is_slice_intersect(
            &self.removed_segment_indexes,
            &other.removed_segment_indexes,
        ) {
            return true;
        }
        false
    }

    fn is_slice_intersect<T: Eq + std::hash::Hash>(l: &[T], r: &[T]) -> bool {
        let (l, r) = if l.len() > r.len() { (l, r) } else { (r, l) };
        let l = l.iter().collect::<HashSet<_>>();
        for x in r {
            if l.contains(x) {
                return true;
            }
        }
        false
    }
}

#[derive(Clone, serde::Serialize, serde::Deserialize, Debug, PartialEq)]
pub struct SnapshotMerged {
    pub merged_segments: Vec<Location>,
    pub merged_statistics: Statistics,
}

#[derive(Clone, serde::Serialize, serde::Deserialize, Debug, PartialEq)]
pub enum ConflictResolveContext {
    AppendOnly((SnapshotMerged, TableSchemaRef)),
    ModifiedSegmentExistsInLatest(SnapshotChanges),
}

impl ConflictResolveContext {
    pub fn is_latest_snapshot_append_only(
        base: &TableSnapshot,
        latest: &TableSnapshot,
    ) -> Option<Range<usize>> {
        let base_segments = &base.segments;
        let latest_segments = &latest.segments;

        let base_segments_len = base_segments.len();
        let latest_segments_len = latest_segments.len();

        if latest_segments_len >= base_segments_len
            && base_segments[0..base_segments_len]
                == latest_segments[(latest_segments_len - base_segments_len)..latest_segments_len]
        {
            Some(0..(latest_segments_len - base_segments_len))
        } else {
            None
        }
    }

    pub fn is_modified_segments_exists_in_latest(
        base: &TableSnapshot,
        latest: &TableSnapshot,
        replaced_segments: &HashMap<usize, Location>,
        removed_segments: &[usize],
    ) -> Option<(Vec<usize>, HashMap<usize, Location>)> {
        let latest_segments = latest
            .segments
            .iter()
            .enumerate()
            .map(|(i, x)| (x, i))
            .collect::<HashMap<_, usize>>();
        let mut removed = Vec::with_capacity(removed_segments.len());
        for removed_segment in removed_segments {
            let removed_segment = &base.segments[*removed_segment];
            if let Some(position) = latest_segments.get(removed_segment) {
                removed.push(*position);
            } else {
                return None;
            }
        }

        let mut replaced = HashMap::with_capacity(replaced_segments.len());
        for (position, location) in replaced_segments {
            let origin_segment = &base.segments[*position];
            if let Some(position) = latest_segments.get(origin_segment) {
                replaced.insert(*position, location.clone());
            } else {
                return None;
            }
        }
        Some((removed, replaced))
    }

    pub fn merge_segments(
        mut base_segments: Vec<Location>,
        appended_segments: Vec<Location>,
        replaced_segments: HashMap<usize, Location>,
        removed_segment_indexes: Vec<usize>,
    ) -> Vec<Location> {
        replaced_segments
            .into_iter()
            .for_each(|(k, v)| base_segments[k] = v);

        let mut blanks = removed_segment_indexes;
        blanks.sort_unstable();
        let mut merged_segments =
            Vec::with_capacity(base_segments.len() + appended_segments.len() - blanks.len());
        if !blanks.is_empty() {
            let mut last = 0;
            for blank in blanks {
                merged_segments.extend_from_slice(&base_segments[last..blank]);
                last = blank + 1;
            }
            merged_segments.extend_from_slice(&base_segments[last..]);
        } else {
            merged_segments = base_segments;
        }

        appended_segments
            .into_iter()
            .chain(merged_segments)
            .collect()
    }
}

#[derive(Clone)]
pub struct MutationGenerator {
    base_snapshot: Arc<TableSnapshot>,
    conflict_resolve_ctx: Option<ConflictResolveContext>,
    table_version: Option<TableVersion>,
}

impl MutationGenerator {
    pub fn new(base_snapshot: Arc<TableSnapshot>, table_version: Option<TableVersion>) -> Self {
        MutationGenerator {
            base_snapshot,
            conflict_resolve_ctx: None,
            table_version,
        }
    }
}

impl SnapshotGenerator for MutationGenerator {
    fn generate_new_snapshot(
        &self,
        schema: TableSchema,
        cluster_key_meta: Option<ClusterKey>,
        previous: Option<Arc<TableSnapshot>>,
    ) -> Result<TableSnapshot> {
        let default_cluster_key_id = cluster_key_meta.clone().map(|v| v.0);

        let previous =
            previous.unwrap_or_else(|| Arc::new(TableSnapshot::new_empty_snapshot(schema.clone())));
        let ctx = self
            .conflict_resolve_ctx
            .as_ref()
            .ok_or(ErrorCode::Internal("conflict_solve_ctx not set"))?;
        match ctx {
            ConflictResolveContext::AppendOnly(_) => {
                return Err(ErrorCode::Internal(
                    "conflict_resolve_ctx should not be AppendOnly in MutationGenerator",
                ));
            }
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
                        previous.segments.clone(),
                        ctx.appended_segments.clone(),
                        replaced,
                        removed,
                    );
                    let mut new_summary = merge_statistics(
                        &ctx.merged_statistics,
                        &previous.summary,
                        default_cluster_key_id,
                    );
                    deduct_statistics_mut(&mut new_summary, &ctx.removed_statistics);
                    let new_snapshot = TableSnapshot::new(
                        &previous.timestamp,
                        Some((
                            previous.snapshot_id,
                            previous.format_version,
                            previous.table_version,
                        )),
                        self.table_version,
                        schema,
                        new_summary,
                        new_segments,
                        cluster_key_meta,
                        previous.table_statistics_location.clone(),
                    );
                    return Ok(new_snapshot);
                }
            }
        }
        metrics_inc_commit_mutation_unresolvable_conflict();
        Err(ErrorCode::UnresolvableConflict(format!(
            "conflict resolve context:{:?}",
            ctx
        )))
    }

    fn set_conflict_resolve_context(&mut self, ctx: ConflictResolveContext) {
        self.conflict_resolve_ctx = Some(ctx);
    }
}

#[derive(Clone)]
pub struct AppendGenerator {
    ctx: Arc<dyn TableContext>,
    leaf_default_values: HashMap<ColumnId, Scalar>,
    overwrite: bool,
    conflict_resolve_ctx: Option<ConflictResolveContext>,
    table_version: Option<TableVersion>,
}

impl AppendGenerator {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        overwrite: bool,
        table_version: Option<TableVersion>,
    ) -> Self {
        AppendGenerator {
            ctx,
            leaf_default_values: HashMap::new(),
            overwrite,
            conflict_resolve_ctx: None,
            table_version,
        }
    }

    fn check_fill_default(&self, summary: &Statistics) -> Result<bool> {
        let mut fill_default_values = false;
        // check if need to fill default value in statistics
        for column_id in self
            .conflict_resolve_ctx()?
            .0
            .merged_statistics
            .col_stats
            .keys()
        {
            if !summary.col_stats.contains_key(column_id) {
                fill_default_values = true;
                break;
            }
        }
        Ok(fill_default_values)
    }
}

impl AppendGenerator {
    fn conflict_resolve_ctx(&self) -> Result<(&SnapshotMerged, &TableSchema)> {
        let ctx = self
            .conflict_resolve_ctx
            .as_ref()
            .ok_or(ErrorCode::Internal("conflict_solve_ctx not set"))?;
        match ctx {
            ConflictResolveContext::AppendOnly((ctx, schema)) => Ok((ctx, schema.as_ref())),
            _ => Err(ErrorCode::Internal(
                "conflict_resolve_ctx should only be Appendonly in AppendGenerator",
            )),
        }
    }
}

#[async_trait::async_trait]
impl SnapshotGenerator for AppendGenerator {
    fn set_conflict_resolve_context(&mut self, ctx: ConflictResolveContext) {
        self.conflict_resolve_ctx = Some(ctx);
    }

    async fn fill_default_values(
        &mut self,
        schema: TableSchema,
        previous: &Option<Arc<TableSnapshot>>,
    ) -> Result<()> {
        if let Some(snapshot) = previous {
            if !self.overwrite && self.check_fill_default(&snapshot.summary)? {
                let mut default_values = Vec::with_capacity(schema.num_fields());
                for field in schema.fields() {
                    default_values.push(field_default_value(self.ctx.clone(), field)?);
                }
                self.leaf_default_values = schema.field_leaf_default_values(&default_values);
            }
        }
        Ok(())
    }

    fn generate_new_snapshot(
        &self,
        schema: TableSchema,
        cluster_key_meta: Option<ClusterKey>,
        previous: Option<Arc<TableSnapshot>>,
    ) -> Result<TableSnapshot> {
        let (snapshot_merged, expected_schema) = self.conflict_resolve_ctx()?;
        if is_column_type_modified(&schema, expected_schema) {
            return Err(ErrorCode::UnresolvableConflict(format!(
                "schema was changed during insert, expected:{:?}, actual:{:?}",
                expected_schema, schema
            )));
        }
        let mut prev_timestamp = None;
        let mut prev_snapshot_id = None;
        let mut table_statistics_location = None;
        let mut new_segments = snapshot_merged.merged_segments.clone();
        let mut new_summary = snapshot_merged.merged_statistics.clone();

        if let Some(snapshot) = &previous {
            prev_timestamp = snapshot.timestamp;
            prev_snapshot_id = Some((
                snapshot.snapshot_id,
                snapshot.format_version,
                snapshot.table_version,
            ));
            table_statistics_location = snapshot.table_statistics_location.clone();

            if !self.overwrite {
                let mut summary = snapshot.summary.clone();
                if self.check_fill_default(&summary)? {
                    self.leaf_default_values
                        .iter()
                        .for_each(|(col_id, default_value)| {
                            if !summary.col_stats.contains_key(col_id) {
                                let (null_count, distinct_of_values) = if default_value.is_null() {
                                    (summary.row_count, Some(0))
                                } else {
                                    (0, Some(1))
                                };
                                let col_stat = ColumnStatistics::new(
                                    default_value.to_owned(),
                                    default_value.to_owned(),
                                    null_count,
                                    0,
                                    distinct_of_values,
                                );
                                summary.col_stats.insert(*col_id, col_stat);
                            }
                        });
                }

                new_segments = snapshot_merged
                    .merged_segments
                    .iter()
                    .chain(snapshot.segments.iter())
                    .cloned()
                    .collect();

                merge_statistics_mut(
                    &mut new_summary,
                    &summary,
                    cluster_key_meta.clone().map(|v| v.0),
                );
            }
        }

        Ok(TableSnapshot::new(
            &prev_timestamp,
            prev_snapshot_id,
            self.table_version,
            schema,
            new_summary,
            new_segments,
            cluster_key_meta,
            table_statistics_location,
        ))
    }
}

fn is_column_type_modified(schema: &TableSchema, expected_schema: &TableSchema) -> bool {
    let expected: HashMap<_, _> = expected_schema
        .fields()
        .iter()
        .map(|f| (f.column_id, &f.data_type))
        .collect();
    schema.fields().iter().any(|f| {
        expected
            .get(&f.column_id)
            .is_some_and(|ty| **ty != f.data_type)
    })
}
