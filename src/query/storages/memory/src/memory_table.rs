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
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::LazyLock;

use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_catalog::catalog::StorageDescription;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::AppendMode;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::Value;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_common_meta_app::schema::UpsertTableCopiedFileReq;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sinks::Sink;
use databend_common_pipeline_sinks::Sinker;
use databend_common_pipeline_sources::SyncSource;
use databend_common_pipeline_sources::SyncSourcer;
use databend_common_storage::StorageMetrics;
use databend_storages_common_table_meta::meta::SnapshotId;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use parking_lot::Mutex;
use parking_lot::RwLock;

use crate::memory_part::MemoryPartInfo;

/// Shared store to support memory tables.
///
/// Indexed by table id etc.
pub type InMemoryData<K> = HashMap<K, Arc<RwLock<Vec<DataBlock>>>>;

static IN_MEMORY_DATA: LazyLock<Arc<RwLock<InMemoryData<u64>>>> =
    LazyLock::new(|| Arc::new(Default::default()));

#[derive(Clone)]
pub struct MemoryTable {
    table_info: TableInfo,
    blocks: Arc<RwLock<Vec<DataBlock>>>,

    data_metrics: Arc<StorageMetrics>,
}

impl MemoryTable {
    pub fn try_create(table_info: TableInfo) -> Result<Box<dyn Table>> {
        let table_id = &table_info.ident.table_id;
        let blocks = {
            let mut in_mem_data = IN_MEMORY_DATA.write();
            let x = in_mem_data.get(table_id);
            x.cloned().unwrap_or_else(|| {
                let blocks = Arc::new(RwLock::new(vec![]));
                in_mem_data.insert(*table_id, blocks.clone());
                blocks
            })
        };

        let table = Self {
            table_info,
            blocks,
            data_metrics: Arc::new(StorageMetrics::default()),
        };
        Ok(Box::new(table))
    }

    pub fn description() -> StorageDescription {
        StorageDescription {
            engine_name: "MEMORY".to_string(),
            comment: "MEMORY Storage Engine".to_string(),
            ..Default::default()
        }
    }

    pub fn get_blocks(&self) -> Vec<DataBlock> {
        let data_blocks = self.blocks.read();
        data_blocks.iter().cloned().collect()
    }

    pub fn truncate(&self) {
        let mut blocks = self.blocks.write();
        blocks.clear();
    }

    pub fn update(&self, new_blocks: Vec<DataBlock>) {
        let mut blocks = self.blocks.write();
        *blocks = new_blocks;
    }

    fn get_read_data_blocks(&self) -> Arc<Mutex<VecDeque<DataBlock>>> {
        let data_blocks = self.blocks.read();
        let mut read_data_blocks = VecDeque::with_capacity(data_blocks.len());

        for data_block in data_blocks.iter() {
            read_data_blocks.push_back(data_block.clone());
        }

        Arc::new(Mutex::new(read_data_blocks))
    }

    pub fn generate_memory_parts(start: usize, workers: usize, total: usize) -> Partitions {
        let part_size = total / workers;
        let part_remain = total % workers;

        let mut partitions = Vec::with_capacity(workers);
        if part_size == 0 {
            partitions.push(MemoryPartInfo::create(start, total, total));
        } else {
            for part in 0..workers {
                let mut part_begin = part * part_size;
                if part == 0 && start > 0 {
                    part_begin = start;
                }
                let mut part_end = (part + 1) * part_size;
                if part == (workers - 1) && part_remain > 0 {
                    part_end += part_remain;
                }

                partitions.push(MemoryPartInfo::create(part_begin, part_end, total));
            }
        }

        Partitions::create(PartitionsShuffleKind::Seq, partitions)
    }
}

#[async_trait::async_trait]
impl Table for MemoryTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn support_column_projection(&self) -> bool {
        true
    }

    fn get_data_metrics(&self) -> Option<Arc<StorageMetrics>> {
        Some(self.data_metrics.clone())
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        let blocks = self.blocks.read();

        let statistics = match push_downs {
            Some(push_downs) => {
                let projection_filter: Box<dyn Fn(usize) -> bool> = match push_downs.projection {
                    Some(prj) => {
                        let col_ids = match prj {
                            Projection::Columns(indices) => indices,
                            Projection::InnerColumns(path_indices) => {
                                path_indices.values().map(|i| i[0]).collect()
                            }
                        };
                        let proj_cols = HashSet::<usize>::from_iter(col_ids);
                        Box::new(move |column_id: usize| proj_cols.contains(&column_id))
                    }
                    None => Box::new(|_: usize| true),
                };

                blocks
                    .iter()
                    .fold(PartStatistics::default(), |mut stats, block| {
                        stats.read_rows += block.num_rows();
                        stats.read_bytes += (0..block.num_columns())
                            .collect::<Vec<usize>>()
                            .iter()
                            .filter(|cid| projection_filter(**cid))
                            .map(|cid| block.get_by_offset(*cid).memory_size())
                            .sum::<usize>();
                        stats
                    })
            }
            None => {
                let rows = blocks.iter().map(|block| block.num_rows()).sum();
                let bytes = blocks.iter().map(|block| block.memory_size()).sum();

                PartStatistics::new_exact(rows, bytes, blocks.len(), blocks.len())
            }
        };

        let parts = Self::generate_memory_parts(
            0,
            ctx.get_settings().get_max_threads()? as usize,
            blocks.len(),
        );
        Ok((statistics, parts))
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        let numbers = ctx.get_settings().get_max_threads()? as usize;
        let read_data_blocks = self.get_read_data_blocks();

        // Add source pipe.
        pipeline.add_source(
            |output| {
                MemoryTableSource::create(
                    ctx.clone(),
                    output,
                    read_data_blocks.clone(),
                    plan.push_downs.clone(),
                )
            },
            numbers,
        )
    }

    fn append_data(
        &self,
        _ctx: Arc<dyn TableContext>,
        _pipeline: &mut Pipeline,
        _: AppendMode,
        _table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<()> {
        Ok(())
    }

    fn commit_insertion(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        _copied_files: Option<UpsertTableCopiedFileReq>,
        _update_stream_meta: Vec<UpdateStreamMetaReq>,
        overwrite: bool,
        _prev_snapshot_id: Option<SnapshotId>,
        _deduplicated_label: Option<String>,
        _table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<()> {
        pipeline.try_resize(1)?;

        pipeline.add_sink(|input| {
            Ok(ProcessorPtr::create(MemoryTableSink::create(
                input,
                ctx.clone(),
                Arc::new(self.clone()),
                overwrite,
            )))
        })
    }

    #[async_backtrace::framed]
    async fn truncate(&self, _ctx: Arc<dyn TableContext>, _pipeline: &mut Pipeline) -> Result<()> {
        self.truncate();
        Ok(())
    }
}

struct MemoryTableSource {
    extras: Option<PushDownInfo>,
    data_blocks: Arc<Mutex<VecDeque<DataBlock>>>,
}

impl MemoryTableSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        data_blocks: Arc<Mutex<VecDeque<DataBlock>>>,
        extras: Option<PushDownInfo>,
    ) -> Result<ProcessorPtr> {
        SyncSourcer::create(ctx, output, MemoryTableSource {
            extras,
            data_blocks,
        })
    }

    fn projection(&self, data_block: DataBlock) -> Result<Option<DataBlock>> {
        if let Some(extras) = &self.extras {
            if let Some(projection) = &extras.projection {
                let num_rows = data_block.num_rows();
                let pruned_data_block = match projection {
                    Projection::Columns(indices) => {
                        let columns = indices
                            .iter()
                            .map(|idx| data_block.get_by_offset(*idx).clone())
                            .collect();
                        DataBlock::new(columns, num_rows)
                    }
                    Projection::InnerColumns(path_indices) => {
                        let mut columns = Vec::with_capacity(path_indices.len());
                        let paths: Vec<&Vec<usize>> = path_indices.values().collect();
                        for path in paths {
                            let entry = Self::traverse_paths(data_block.columns(), path)?;
                            columns.push(entry);
                        }
                        DataBlock::new(columns, num_rows)
                    }
                };
                return Ok(Some(pruned_data_block));
            }
        }

        Ok(Some(data_block))
    }

    fn traverse_paths(columns: &[BlockEntry], path: &[usize]) -> Result<BlockEntry> {
        if path.is_empty() {
            return Err(ErrorCode::BadArguments("path should not be empty"));
        }
        let entry = &columns[path[0]];
        if path.len() == 1 {
            return Ok(entry.clone());
        }

        match &entry.data_type {
            DataType::Tuple(inner_tys) => {
                let col = entry.value.clone().into_column().unwrap();
                let inner_columns = col.into_tuple().unwrap();
                let mut values = Vec::with_capacity(inner_tys.len());
                for (col, ty) in inner_columns.iter().zip(inner_tys.iter()) {
                    values.push(BlockEntry::new(ty.clone(), Value::Column(col.clone())));
                }
                Self::traverse_paths(&values[..], &path[1..])
            }
            _ => Err(ErrorCode::BadArguments(format!(
                "Unable to get column by paths: {:?}",
                path
            ))),
        }
    }
}

impl SyncSource for MemoryTableSource {
    const NAME: &'static str = "MemoryTable";

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        let mut blocks_guard = self.data_blocks.lock();
        match blocks_guard.pop_front() {
            None => Ok(None),
            Some(data_block) => self.projection(data_block),
        }
    }
}

struct MemoryTableSink {
    table: Arc<MemoryTable>,
    write_progress: Arc<Progress>,
    operations: Vec<DataBlock>,
    overwrite: bool,
}

impl MemoryTableSink {
    pub fn create(
        input: Arc<InputPort>,
        ctx: Arc<dyn TableContext>,
        table: Arc<MemoryTable>,
        overwrite: bool,
    ) -> Box<dyn Processor> {
        Sinker::create(input, MemoryTableSink {
            table,
            write_progress: ctx.get_write_progress(),
            operations: vec![],
            overwrite,
        })
    }
}

impl Sink for MemoryTableSink {
    const NAME: &'static str = "MemoryTableSink";

    fn consume(&mut self, block: DataBlock) -> Result<()> {
        self.operations.push(block);
        Ok(())
    }

    fn on_finish(&mut self) -> Result<()> {
        let operations = std::mem::take(&mut self.operations);

        let bytes: usize = operations.iter().map(|b| b.memory_size()).sum();
        let rows: usize = operations.iter().map(|b| b.num_rows()).sum();
        let progress_values = ProgressValues { rows, bytes };
        self.write_progress.incr(&progress_values);
        self.table.data_metrics.inc_write_bytes(bytes);

        let mut blocks = self.table.blocks.write();
        if self.overwrite {
            blocks.clear();
        }
        for block in operations {
            blocks.push(block);
        }

        Ok(())
    }
}
