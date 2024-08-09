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
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::ColumnId;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableSchema;
use databend_common_sql::field_default_value;
use databend_storages_common_table_meta::meta::ClusterKey;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;
use log::info;
use log::warn;

use crate::operations::common::ConflictResolveContext;
use crate::operations::common::SnapshotGenerator;
use crate::operations::common::SnapshotMerged;
use crate::statistics::reducers::merge_statistics_mut;

#[derive(Clone)]
pub struct AppendGenerator {
    ctx: Arc<dyn TableContext>,
    leaf_default_values: HashMap<ColumnId, Scalar>,
    overwrite: bool,
    conflict_resolve_ctx: ConflictResolveContext,
}

impl AppendGenerator {
    pub fn new(ctx: Arc<dyn TableContext>, overwrite: bool) -> Self {
        AppendGenerator {
            ctx,
            leaf_default_values: HashMap::new(),
            overwrite,
            conflict_resolve_ctx: ConflictResolveContext::None,
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
        match &self.conflict_resolve_ctx {
            ConflictResolveContext::AppendOnly((ctx, schema)) => Ok((ctx, schema.as_ref())),
            _ => Err(ErrorCode::Internal(
                "conflict_resolve_ctx should only be Appendonly in AppendGenerator",
            )),
        }
    }
}

#[async_trait::async_trait]
impl SnapshotGenerator for AppendGenerator {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn set_conflict_resolve_context(&mut self, ctx: ConflictResolveContext) {
        self.conflict_resolve_ctx = ctx;
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

    fn do_generate_new_snapshot(
        &self,
        schema: TableSchema,
        cluster_key_meta: Option<ClusterKey>,
        previous: &Option<Arc<TableSnapshot>>,
        prev_table_seq: Option<u64>,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<TableSnapshot> {
        let (snapshot_merged, expected_schema) = self.conflict_resolve_ctx()?;
        if is_column_type_modified(&schema, expected_schema) {
            return Err(ErrorCode::UnresolvableConflict(format!(
                "schema was changed during insert, expected:{:?}, actual:{:?}",
                expected_schema, schema
            )));
        }
        let mut table_statistics_location = None;
        let mut new_segments = snapshot_merged.merged_segments.clone();
        let mut new_summary = snapshot_merged.merged_statistics.clone();

        if let Some(snapshot) = previous {
            table_statistics_location = snapshot.table_statistics_location.clone();

            if !self.overwrite {
                let mut summary = snapshot.summary.clone();

                let leaf_fields = schema.leaf_fields();
                let column_data_types: HashMap<ColumnId, &TableDataType> =
                    HashMap::from_iter(leaf_fields.iter().map(|f| (f.column_id, &f.data_type)));

                if self.check_fill_default(&summary)? {
                    self.leaf_default_values
                        .iter()
                        .for_each(|(col_id, default_value)| {
                            if let Some(data_type) = column_data_types.get(col_id) {
                                if !summary.col_stats.contains_key(col_id) {
                                    assert!(
                                        default_value
                                            .as_ref()
                                            .is_value_of_type(&DataType::from(*data_type)),
                                        "default value: {:?} is not of type: {:?}",
                                        default_value,
                                        data_type
                                    );
                                    if let Some((min, max)) = crate::statistics::scalar_min_max(
                                        &DataType::from(*data_type),
                                        default_value.clone(),
                                    ) {
                                        let (null_count, distinct_of_values) =
                                            if default_value.is_null() {
                                                (summary.row_count, Some(0))
                                            } else {
                                                (0, Some(1))
                                            };
                                        let col_stat = ColumnStatistics::new(
                                            min,
                                            max,
                                            null_count,
                                            0,
                                            distinct_of_values,
                                        );
                                        summary.col_stats.insert(*col_id, col_stat);
                                    }
                                }
                            } else {
                                warn!("column id:{} not found in schema, while populating min/max values. the schema is {:?}", col_id, schema);
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

        // check if need to auto compact
        // the algorithm is: if the number of imperfect blocks is greater than the threshold, then auto compact.
        // the threshold is set by the setting `auto_compaction_imperfect_blocks_threshold`, default is 25.
        let imperfect_count = new_summary.block_count - new_summary.perfect_block_count;
        let auto_compaction_imperfect_blocks_threshold = self
            .ctx
            .get_settings()
            .get_auto_compaction_imperfect_blocks_threshold()?;

        if imperfect_count >= auto_compaction_imperfect_blocks_threshold {
            // If imperfect_count is larger, SLIGHTLY increase the number of blocks
            // eligible for auto-compaction, this adjustment is intended to help reduce
            // fragmentation over time.
            //
            // To prevent the off-by-one mistake, we need to add 1 to it;
            // this way, the potentially previously left non-compacted segment will
            // also be included.
            let compact_num_block_hint = std::cmp::min(
                imperfect_count,
                (auto_compaction_imperfect_blocks_threshold as f64 * 1.5).ceil() as u64,
            ) + 1;
            info!("set compact_num_block_hint to {compact_num_block_hint }");
            self.ctx
                .set_compaction_num_block_hint(compact_num_block_hint);
        }

        TableSnapshot::try_new(
            prev_table_seq,
            previous.clone(),
            schema,
            new_summary,
            new_segments,
            cluster_key_meta,
            table_statistics_location,
            table_meta_timestamps,
        )
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
