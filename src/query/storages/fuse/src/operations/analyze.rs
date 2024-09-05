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
use std::time::Instant;

use async_channel::Receiver;
use async_channel::Sender;
use async_trait::async_trait;
use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_io::prelude::borsh_deserialize_from_slice;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sinks::AsyncSink;
use databend_common_pipeline_sinks::AsyncSinker;
use databend_common_storage::Datum;
use databend_common_storage::Histogram;
use databend_common_storage::HistogramBucket;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::MetaHLL;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::SnapshotId;
use databend_storages_common_table_meta::meta::StatisticsOfColumns;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::TableSnapshotStatistics;

use crate::io::SegmentsIO;
use crate::statistics::reduce_block_statistics;
use crate::statistics::reduce_cluster_statistics;
use crate::FuseTable;

#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
enum AnalyzeStep {
    CollectNDV,
    CollectHistogram,
    CommitStatistics,
}

impl FuseTable {
    #[allow(clippy::too_many_arguments)]
    pub fn do_analyze(
        ctx: Arc<dyn TableContext>,
        output_schema: Arc<DataSchema>,
        catalog: &str,
        database: &str,
        table: &str,
        snapshot_id: SnapshotId,
        pipeline: &mut Pipeline,
        histogram_info_receivers: HashMap<u32, Receiver<DataBlock>>,
    ) -> Result<()> {
        pipeline.add_sink(|input| {
            SinkAnalyzeState::create(
                ctx.clone(),
                output_schema.clone(),
                catalog,
                database,
                table,
                snapshot_id,
                input,
                histogram_info_receivers.clone(),
            )
        })?;
        Ok(())
    }
}

struct SinkAnalyzeState {
    ctx: Arc<dyn TableContext>,
    output_schema: Arc<DataSchema>,
    input_port: Arc<InputPort>,

    catalog: String,
    database: String,
    table: String,
    snapshot_id: SnapshotId,
    histogram_info_receivers: HashMap<u32, Receiver<DataBlock>>,
    input_data: Option<DataBlock>,
    committed: bool,
    ndv_states: HashMap<ColumnId, MetaHLL>,
    histograms: HashMap<ColumnId, Histogram>,
    step: AnalyzeStep,
}

impl SinkAnalyzeState {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output_schema: Arc<DataSchema>,
        catalog: &str,
        database: &str,
        table: &str,
        snapshot_id: SnapshotId,
        input: Arc<InputPort>,
        histogram_info_receivers: HashMap<u32, Receiver<DataBlock>>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(SinkAnalyzeState {
            ctx,
            output_schema,
            input_port: input,
            catalog: catalog.to_string(),
            database: database.to_string(),
            table: table.to_string(),
            snapshot_id,
            histogram_info_receivers,
            input_data: None,
            committed: false,
            ndv_states: Default::default(),
            histograms: Default::default(),
            step: AnalyzeStep::CollectNDV,
        })))
    }

    async fn get_table(&self) -> Result<Arc<dyn Table>> {
        // always use the latest table
        let tenant = self.ctx.get_tenant();
        let catalog = CatalogManager::instance()
            .get_catalog(
                tenant.tenant_name(),
                &self.catalog,
                self.ctx.session_state(),
            )
            .await?;
        let table = catalog
            .get_table(&tenant, &self.database, &self.table)
            .await?;
        Ok(table)
    }

    #[async_backtrace::framed]
    pub async fn merge_analyze_states(&mut self, data_block: DataBlock) -> Result<()> {
        if data_block.num_rows() == 0 {
            return Ok(());
        }
        let table = self.get_table().await?;
        let table = FuseTable::try_from_table(table.as_ref())?;
        let snapshot = table.read_table_snapshot().await?;

        if snapshot.is_none() {
            return Ok(());
        }
        let table_statistics = table
            .read_table_snapshot_statistics(snapshot.as_ref())
            .await?;

        let is_full = data_block.columns()[self.output_schema.num_fields() - 1]
            .value
            .index(0)
            .unwrap();

        let is_full = is_full.as_boolean().unwrap();

        let mut ndv_states = table_statistics.map(|s| s.hll.clone()).unwrap_or_default();

        let index_num = self.output_schema.num_fields() - 1;

        for (f, col) in self
            .output_schema
            .fields()
            .iter()
            .take(index_num)
            .zip(data_block.columns())
        {
            let name = f.name();
            let index: u32 = name.strip_prefix("ndv_").unwrap().parse().unwrap();

            let col = col.value.index(0).unwrap();
            let col = col.as_binary().unwrap();
            let hll: MetaHLL = borsh_deserialize_from_slice(col)?;

            if !is_full {
                ndv_states
                    .entry(index)
                    .and_modify(|c| c.merge(&hll))
                    .or_insert(hll);
            } else {
                ndv_states.insert(index, hll);
            }
        }

        self.ndv_states = ndv_states;
        Ok(())
    }

    #[async_backtrace::framed]
    async fn create_histogram(&mut self, col_id: u32, data_block: DataBlock) -> Result<()> {
        if data_block.num_rows() == 0 {
            return Ok(());
        }
        for row in 0..data_block.num_rows() {
            let column = &data_block.columns()[1];
            let value = column.value.index(row).clone().unwrap();
            let number = value.as_number().unwrap();
            let ndv = number.as_u_int64().unwrap();
            let upper_bound =
                Datum::from_scalar(data_block.columns()[2].value.index(row).unwrap().to_owned())
                    .ok_or_else(|| {
                        ErrorCode::Internal("Don't support the type to generate histogram")
                    })?;
            let lower_bound =
                Datum::from_scalar(data_block.columns()[3].value.index(row).unwrap().to_owned())
                    .ok_or_else(|| {
                        ErrorCode::Internal("Don't support the type to generate histogram")
                    })?;
            let count_col = &data_block.columns()[4];
            let val = count_col.value.index(row).clone().unwrap();
            let number = val.as_number().unwrap();
            let count = number.as_u_int64().unwrap();
            let bucket = HistogramBucket::new(lower_bound, upper_bound, *count as f64, *ndv as f64);
            self.histograms
                .entry(col_id)
                .and_modify(|histogram| histogram.add_bucket(bucket.clone()))
                .or_insert(Histogram::new(vec![bucket], true));
        }
        Ok(())
    }

    async fn commit_statistics(&self) -> Result<()> {
        let table = self.get_table().await?;
        let table = FuseTable::try_from_table(table.as_ref())?;
        let snapshot = table.read_table_snapshot().await?;
        if snapshot.is_none() {
            return Ok(());
        }
        let snapshot = snapshot.unwrap();
        // 3. Generate new table statistics
        let table_statistics = TableSnapshotStatistics::new(
            self.ndv_states.clone(),
            self.histograms.clone(),
            self.snapshot_id,
        );
        let table_statistics_location = table
            .meta_location_generator
            .snapshot_statistics_location_from_uuid(
                &table_statistics.snapshot_id,
                table_statistics.format_version(),
            )?;

        let (col_stats, cluster_stats) =
            regenerate_statistics(table, snapshot.as_ref(), &self.ctx).await?;
        // 4. Save table statistics
        let mut new_snapshot = TableSnapshot::try_from_previous(
            snapshot.clone(),
            Some(table.get_table_info().ident.seq),
            self.ctx
                .get_table_meta_timestamps(table.get_id(), Some(snapshot.clone()))?,
        )?;
        new_snapshot.summary.col_stats = col_stats;
        new_snapshot.summary.cluster_stats = cluster_stats;
        new_snapshot.table_statistics_location = Some(table_statistics_location);
        FuseTable::commit_to_meta_server(
            self.ctx.as_ref(),
            &table.table_info,
            &table.meta_location_generator,
            new_snapshot,
            Some(table_statistics),
            &None,
            &table.operator,
        )
        .await
    }
}

#[async_trait::async_trait]
impl Processor for SinkAnalyzeState {
    fn name(&self) -> String {
        "SinkAnalyzeState".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.input_data.is_some() {
            if !self.input_port.has_data() {
                self.input_port.set_need_data();
            }
            return Ok(Event::Async);
        }

        if self.input_port.is_finished() {
            match self.step {
                AnalyzeStep::CollectNDV => {
                    self.step = AnalyzeStep::CollectHistogram;
                    return Ok(Event::Async);
                }
                AnalyzeStep::CollectHistogram => {
                    return Ok(Event::Async);
                }
                AnalyzeStep::CommitStatistics => {
                    if self.committed {
                        return Ok(Event::Finished);
                    } else {
                        return Ok(Event::Async);
                    }
                }
            }
        }

        if self.input_port.has_data() {
            self.input_data = Some(self.input_port.pull_data().unwrap()?);
            return Ok(Event::Async);
        }

        self.input_port.set_need_data();
        Ok(Event::NeedData)
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match self.step {
            AnalyzeStep::CollectNDV => {
                if let Some(data_block) = self.input_data.take() {
                    self.merge_analyze_states(data_block.clone()).await?;
                }
            }
            AnalyzeStep::CollectHistogram => {
                let mut finished_count = 0;
                let receivers = self.histogram_info_receivers.clone();
                for (id, receiver) in receivers.iter() {
                    if let Ok(res) = receiver.recv().await {
                        self.create_histogram(*id, res).await?;
                    } else {
                        finished_count += 1;
                    }
                }
                if finished_count == self.histogram_info_receivers.len() {
                    self.step = AnalyzeStep::CommitStatistics;
                }
            }
            AnalyzeStep::CommitStatistics => {
                let mismatch_code = ErrorCode::TABLE_VERSION_MISMATCHED;
                loop {
                    if let Err(e) = self.commit_statistics().await {
                        if e.code() == mismatch_code {
                            log::warn!("Retry after got TableVersionMismatched");
                            continue;
                        } else {
                            return Err(e);
                        }
                    }
                    break;
                }
                self.committed = true;
            }
        }
        return Ok(());
    }
}

pub async fn regenerate_statistics(
    table: &FuseTable,
    snapshot: &TableSnapshot,
    ctx: &Arc<dyn TableContext>,
) -> Result<(StatisticsOfColumns, Option<ClusterStatistics>)> {
    // 1. Read table snapshot.
    let default_cluster_key_id = table.cluster_key_id();

    // 2. Iterator segments and blocks to estimate statistics.
    let mut read_segment_count = 0;
    let mut col_stats = HashMap::new();
    let mut cluster_stats = None;

    let start = Instant::now();
    let segments_io = SegmentsIO::create(ctx.clone(), table.operator.clone(), table.schema());
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
        }

        // Generate new column statistics for snapshot
        col_stats = reduce_block_statistics(&stats_of_columns);
        cluster_stats = reduce_cluster_statistics(&blocks_cluster_stats, default_cluster_key_id);

        // Status.
        {
            read_segment_count += chunk.len();
            let status = format!(
                "analyze: read segment files:{}/{}, cost:{:?}",
                read_segment_count,
                number_segments,
                start.elapsed()
            );
            ctx.set_status_info(&status);
        }
    }

    Ok((col_stats, cluster_stats))
}

pub struct HistogramInfoSink {
    sender: Option<Sender<DataBlock>>,
}

impl HistogramInfoSink {
    pub fn create(tx: Option<Sender<DataBlock>>, input: Arc<InputPort>) -> Box<dyn Processor> {
        AsyncSinker::create(input, HistogramInfoSink { sender: tx })
    }
}

#[async_trait]
impl AsyncSink for HistogramInfoSink {
    const NAME: &'static str = "HistogramInfoSink";

    #[async_backtrace::framed]
    async fn on_finish(&mut self) -> Result<()> {
        drop(self.sender.take());
        Ok(())
    }

    #[async_backtrace::framed]
    async fn consume(&mut self, data_block: DataBlock) -> Result<bool> {
        if let Some(sender) = self.sender.as_ref() {
            if sender.send(data_block).await.is_err() {
                return Err(ErrorCode::Internal("HistogramInfoSink sender failed"));
            };
        }
        Ok(false)
    }
}
