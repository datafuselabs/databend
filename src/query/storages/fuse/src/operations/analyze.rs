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
use std::time::Instant;

use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use log::warn;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::TableSnapshot;
use storages_common_table_meta::meta::TableSnapshotStatistics;

use crate::io::SegmentsIO;
use crate::statistics::reduce_block_statistics;
use crate::statistics::reduce_cluster_statistics;
use crate::FuseTable;

impl FuseTable {
    #[async_backtrace::framed]
    pub async fn do_analyze(&self, ctx: &Arc<dyn TableContext>) -> Result<()> {
        // 1. Read table snapshot.
        let r = self.read_table_snapshot().await;
        let snapshot_opt = match r {
            Err(e) if e.code() == ErrorCode::STORAGE_NOT_FOUND => {
                warn!(
                    "concurrent statistic: snapshot {:?} already collected. table: {}, ident {}",
                    self.snapshot_loc().await?,
                    self.table_info.desc,
                    self.table_info.ident,
                );
                return Ok(());
            }
            Err(e) => return Err(e),
            Ok(v) => v,
        };

        let default_cluster_key_id = self.cluster_key_id();

        if let Some(snapshot) = snapshot_opt {
            // 2. Iterator segments and blocks to estimate statistics.
            let mut sum_map = HashMap::new();
            let mut row_count_sum = 0;
            let mut block_count_sum: u64 = 0;
            let mut read_segment_count = 0;
            let mut col_stats = HashMap::new();
            let mut cluster_stats = None;

            let start = Instant::now();
            let segments_io = SegmentsIO::create(ctx.clone(), self.operator.clone(), self.schema());
            let chunk_size = ctx.get_settings().get_max_threads()? as usize * 4;
            let number_segments = snapshot.segments.len();
            for chunk in snapshot.segments.chunks(chunk_size) {
                let mut stats_of_columns = Vec::new();
                let mut blocks_cluster_stats = Vec::new();
                if !col_stats.is_empty() {
                    stats_of_columns.push(col_stats.clone());
                    blocks_cluster_stats.push(cluster_stats.clone());
                }

                let segments = segments_io
                    .read_segments::<SegmentInfo>(chunk, true)
                    .await?;
                for segment in segments {
                    let segment = segment?;
                    stats_of_columns.push(segment.summary.col_stats.clone());
                    blocks_cluster_stats.push(segment.summary.cluster_stats.clone());
                    segment.blocks.iter().for_each(|block| {
                        let block = block.as_ref();
                        let row_count = block.row_count;
                        if row_count != 0 {
                            block_count_sum += 1;
                            row_count_sum += row_count;
                            for (i, col_stat) in block.col_stats.iter() {
                                let density = col_stat
                                    .distinct_of_values
                                    .map_or(0.0, |ndv| ndv as f64 / row_count as f64);

                                match sum_map.get_mut(i) {
                                    Some(sum) => {
                                        *sum += density;
                                    }
                                    None => {
                                        let _ = sum_map.insert(*i, density);
                                    }
                                }
                            }
                        }
                    });
                }

                // Generate new column statistics for snapshot
                col_stats = reduce_block_statistics(&stats_of_columns);
                cluster_stats =
                    reduce_cluster_statistics(&blocks_cluster_stats, default_cluster_key_id);

                // Status.
                {
                    read_segment_count += chunk.len();
                    let status = format!(
                        "analyze: read segment files:{}/{}, cost:{} sec",
                        read_segment_count,
                        number_segments,
                        start.elapsed().as_secs()
                    );
                    ctx.set_status_info(&status);
                }
            }

            let mut ndv_map = HashMap::new();
            for (i, sum) in sum_map.iter() {
                let density_avg = *sum / block_count_sum as f64;
                ndv_map.insert(*i, (density_avg * row_count_sum as f64) as u64);
            }

            // 3. Generate new table statistics
            let table_statistics = TableSnapshotStatistics::new(ndv_map);
            let table_statistics_location = self
                .meta_location_generator
                .snapshot_statistics_location_from_uuid(
                    &table_statistics.snapshot_id,
                    table_statistics.format_version(),
                    Some(self.current_table_version()),
                )?;

            // 4. Save table statistics
            let mut new_snapshot =
                TableSnapshot::from_previous(&snapshot, Some(self.current_table_version()));
            new_snapshot.summary.col_stats = col_stats;
            new_snapshot.summary.cluster_stats = cluster_stats;
            new_snapshot.table_statistics_location = Some(table_statistics_location);
            self.commit_to_meta_server(
                ctx.as_ref(),
                &self.table_info,
                new_snapshot,
                Some(table_statistics),
                &None,
            )
            .await?;
        }

        Ok(())
    }
}
