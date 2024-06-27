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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use ahash::AHashMap;
use databend_common_arrow::arrow::bitmap::MutableBitmap;
use databend_common_base::base::tokio::sync::Semaphore;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::TrySpawn;
use databend_common_catalog::plan::build_origin_block_row_num;
use databend_common_catalog::plan::gen_mutation_stream_meta;
use databend_common_catalog::plan::split_prefix;
use databend_common_catalog::plan::split_row_id;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_metrics::storage::*;
use databend_common_sql::StreamContext;
use databend_common_storage::MergeStatus;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SegmentInfo;
use itertools::Itertools;
use log::info;
use opendal::Operator;

use crate::io::BlockBuilder;
use crate::io::BlockReader;
use crate::io::BlockWriter;
use crate::io::CompactSegmentInfoReader;
use crate::io::MetaReaders;
use crate::io::ReadSettings;
use crate::io::WriteSettings;
use crate::operations::acquire_task_permit;
use crate::operations::common::MutationLogEntry;
use crate::operations::common::MutationLogs;
use crate::operations::merge_into::processors::RowIdKind;
use crate::operations::mutation::BlockIndex;
use crate::operations::mutation::SegmentIndex;
use crate::operations::read_block;
use crate::operations::BlockMetaIndex;
use crate::FuseTable;

struct AggregationContext {
    ctx: Arc<dyn TableContext>,
    data_accessor: Operator,
    write_settings: WriteSettings,
    read_settings: ReadSettings,
    block_builder: BlockBuilder,
    block_reader: Arc<BlockReader>,
    stream_ctx: Option<StreamContext>,
}

type UpdateOffset = HashSet<usize>;
type DeleteOffset = HashSet<usize>;
pub struct MatchedAggregator {
    ctx: Arc<dyn TableContext>,
    io_request_semaphore: Arc<Semaphore>,
    segment_reader: CompactSegmentInfoReader,
    segment_locations: AHashMap<SegmentIndex, Location>,
    block_mutation_row_offset: HashMap<u64, (UpdateOffset, DeleteOffset)>,
    aggregation_ctx: Arc<AggregationContext>,
    target_build_optimization: bool,
    meta_indexes: HashSet<(SegmentIndex, BlockIndex)>,
}

impl MatchedAggregator {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        table: &FuseTable,
        block_builder: BlockBuilder,
        io_request_semaphore: Arc<Semaphore>,
        segment_locations: Vec<(SegmentIndex, Location)>,
        target_build_optimization: bool,
    ) -> Result<Self> {
        let target_table_schema = table.schema_with_stream();
        let data_accessor = table.get_operator();
        let write_settings = table.get_write_settings();
        let update_stream_columns = table.change_tracking_enabled();
        let read_settings = ReadSettings::from_ctx(&ctx)?;

        let segment_reader =
            MetaReaders::segment_info_reader(data_accessor.clone(), target_table_schema.clone());

        let block_reader = {
            let projection =
                Projection::Columns((0..target_table_schema.num_fields()).collect_vec());
            BlockReader::create(
                ctx.clone(),
                data_accessor.clone(),
                target_table_schema.clone(),
                projection,
                false,
                update_stream_columns,
                false,
            )
        }?;

        let stream_ctx = if update_stream_columns {
            Some(StreamContext::try_create(
                ctx.get_function_context()?,
                target_table_schema,
                table.get_table_info().ident.seq,
                true,
            )?)
        } else {
            None
        };

        Ok(Self {
            aggregation_ctx: Arc::new(AggregationContext {
                ctx: ctx.clone(),
                write_settings,
                read_settings,
                data_accessor,
                block_builder,
                block_reader,
                stream_ctx,
            }),
            io_request_semaphore,
            segment_reader,
            block_mutation_row_offset: HashMap::new(),
            segment_locations: AHashMap::from_iter(segment_locations),
            ctx: ctx.clone(),
            target_build_optimization,
            meta_indexes: HashSet::new(),
        })
    }

    #[async_backtrace::framed]
    pub async fn accumulate(&mut self, data_block: DataBlock) -> Result<()> {
        // An optimization: If we use target table as build side, the deduplicate will be done
        // in hashtable probe phase.In this case, We don't support delete for now, so we
        // don't to add MergeStatus here.
        if data_block.get_meta().is_some() && data_block.is_empty() {
            let meta_index = BlockMetaIndex::downcast_ref_from(data_block.get_meta().unwrap());
            if meta_index.is_some() {
                let meta_index = meta_index.unwrap();
                if !self
                    .meta_indexes
                    .insert((meta_index.segment_idx, meta_index.block_idx))
                {
                    // we can get duplicated partial unmodified blocks,this is not an error
                    // |----------------------------block----------------------------------------|
                    // |----partial-unmodified----|-----matched------|----partial-unmodified-----|
                    info!(
                        "duplicated block: segment_idx: {}, block_idx: {}",
                        meta_index.segment_idx, meta_index.block_idx
                    );
                }
            }
            return Ok(());
        }
        if data_block.is_empty() {
            return Ok(());
        }
        let start = Instant::now();
        // data_block is from matched_split, so there is only one column.
        // that's row_id
        let row_id_col = data_block.get_by_offset(0);
        debug_assert!(
            row_id_col.data_type.remove_nullable() == DataType::Number(NumberDataType::UInt64)
        );
        let row_ids = row_id_col
            .value
            .convert_to_full_column(&row_id_col.data_type, data_block.num_rows());
        let row_id_kind = RowIdKind::downcast_ref_from(data_block.get_meta().unwrap()).unwrap();
        match row_id_kind {
            RowIdKind::Update => {
                for row_id in row_ids.iter() {
                    let (prefix, offset) =
                        split_row_id(row_id.as_number().unwrap().into_u_int64().unwrap());
                    if !self
                        .block_mutation_row_offset
                        .entry(prefix)
                        .or_insert_with(|| (HashSet::new(), HashSet::new()))
                        .0
                        .insert(offset as usize)
                    {
                        return Err(ErrorCode::UnresolvableConflict(
                            "multi rows from source match one and the same row in the target_table multi times",
                        ));
                    }
                }
            }
            RowIdKind::Delete => {
                for row_id in row_ids.iter() {
                    let (prefix, offset) =
                        split_row_id(row_id.as_number().unwrap().into_u_int64().unwrap());
                    let value = self.block_mutation_row_offset.get(&prefix);
                    if value.is_none() {
                        self.ctx.add_merge_status(MergeStatus {
                            insert_rows: 0,
                            update_rows: 0,
                            deleted_rows: 1,
                        });
                    } else {
                        let s = value.unwrap();
                        if !s.1.contains(&(offset as usize)) {
                            self.ctx.add_merge_status(MergeStatus {
                                insert_rows: 0,
                                update_rows: 0,
                                deleted_rows: 1,
                            });
                        }
                    }
                    // support idempotent delete
                    self.block_mutation_row_offset
                        .entry(prefix)
                        .or_insert_with(|| (HashSet::new(), HashSet::new()))
                        .1
                        .insert(offset as usize);
                }
            }
        };
        let elapsed_time = start.elapsed().as_millis() as u64;
        metrics_inc_merge_into_accumulate_milliseconds(elapsed_time);
        Ok(())
    }

    #[async_backtrace::framed]
    pub async fn apply(&mut self) -> Result<Option<MutationLogs>> {
        let start = Instant::now();
        // 1.get modified segments
        let mut segment_infos = HashMap::<SegmentIndex, SegmentInfo>::new();
        let segment_indexes = if self.target_build_optimization {
            let mut vecs = Vec::with_capacity(self.meta_indexes.len());
            for prefix in &self.meta_indexes {
                vecs.push(prefix.0);
            }
            vecs
        } else {
            let mut vecs = Vec::with_capacity(self.block_mutation_row_offset.len());
            for prefix in self.block_mutation_row_offset.keys() {
                let (segment_idx, _) = split_prefix(*prefix);
                let segment_idx = segment_idx as usize;
                vecs.push(segment_idx);
            }
            vecs
        };

        for segment_idx in segment_indexes {
            if let Entry::Vacant(e) = segment_infos.entry(segment_idx) {
                let (path, ver) = self.segment_locations.get(&segment_idx).ok_or_else(|| {
                    ErrorCode::Internal(format!(
                        "unexpected, segment (idx {}) not found, during applying mutation log",
                        segment_idx
                    ))
                })?;

                let load_param = LoadParams {
                    location: path.clone(),
                    len_hint: None,
                    ver: *ver,
                    put_cache: true,
                };

                let compact_segment_info = self.segment_reader.read(&load_param).await?;
                let segment_info: SegmentInfo = compact_segment_info.try_into()?;

                e.insert(segment_info);
            }
        }

        if self.target_build_optimization {
            let mut mutation_logs = Vec::with_capacity(self.meta_indexes.len());
            for item in &self.meta_indexes {
                let segment_idx = item.0;
                let block_idx = item.1;
                let segment_info = segment_infos.get(&item.0).unwrap();
                let block_idx = segment_info.blocks.len() - block_idx - 1;
                info!(
                    "target_build_optimization, merge into apply: segment_idx:{},blk_idx:{}",
                    segment_idx, block_idx
                );
                mutation_logs.push(MutationLogEntry::DeletedBlock {
                    index: BlockMetaIndex {
                        segment_idx,
                        block_idx,
                    },
                })
            }
            return Ok(Some(MutationLogs {
                entries: mutation_logs,
            }));
        }

        let io_runtime = GlobalIORuntime::instance();
        let mut mutation_log_handlers = Vec::with_capacity(self.block_mutation_row_offset.len());

        for item in &self.block_mutation_row_offset {
            let (segment_idx, block_idx) = split_prefix(*item.0);
            let segment_idx = segment_idx as usize;
            let permit = acquire_task_permit(self.io_request_semaphore.clone()).await?;
            let aggregation_ctx = self.aggregation_ctx.clone();
            let segment_info = segment_infos.get(&segment_idx).unwrap();
            let block_idx = segment_info.blocks.len() - block_idx as usize - 1;
            assert!(block_idx < segment_info.blocks.len());
            info!(
                "merge into apply: segment_idx:{},blk_idx:{}",
                segment_idx, block_idx
            );
            // the row_id is generated by block_id, not block_idx,reference to fill_internal_column_meta()
            let block_meta = segment_info.blocks[block_idx].clone();

            let update_modified_offsets = &item.1.0;
            let delete_modified_offsets = &item.1.1;
            let modified_offsets: HashSet<usize> = update_modified_offsets
                .union(delete_modified_offsets)
                .cloned()
                .collect();

            if modified_offsets.len()
                < update_modified_offsets.len() + delete_modified_offsets.len()
            {
                return Err(ErrorCode::UnresolvableConflict(
                    "multi rows from source match one and the same row in the target_table multi times",
                ));
            }

            let handle = io_runtime.spawn(async move {
                let mutation_log_entry = aggregation_ctx
                    .apply_update_and_deletion_to_data_block(
                        segment_idx,
                        block_idx,
                        &block_meta,
                        modified_offsets,
                    )
                    .await?;

                drop(permit);
                Ok::<_, ErrorCode>(mutation_log_entry)
            });
            mutation_log_handlers.push(handle);
        }

        let log_entries = futures::future::try_join_all(mutation_log_handlers)
            .await
            .map_err(|e| {
                ErrorCode::Internal(
                    "unexpected, failed to join apply update and delete tasks for merge into.",
                )
                .add_message_back(e.to_string())
            })?;
        let mut mutation_logs = Vec::new();
        for maybe_log_entry in log_entries {
            if let Some(segment_mutation_log) = maybe_log_entry? {
                mutation_logs.push(segment_mutation_log);
            }
        }
        let elapsed_time = start.elapsed().as_millis() as u64;
        metrics_inc_merge_into_apply_milliseconds(elapsed_time);
        Ok(Some(MutationLogs {
            entries: mutation_logs,
        }))
    }
}

impl AggregationContext {
    #[async_backtrace::framed]
    async fn apply_update_and_deletion_to_data_block(
        &self,
        segment_idx: SegmentIndex,
        block_idx: BlockIndex,
        block_meta: &BlockMeta,
        modified_offsets: HashSet<usize>,
    ) -> Result<Option<MutationLogEntry>> {
        info!(
            "apply update and delete to segment idx {}, block idx {}",
            segment_idx, block_idx,
        );
        let progress_values = ProgressValues {
            rows: modified_offsets.len(),
            bytes: 0,
        };
        self.ctx.get_write_progress().incr(&progress_values);
        let mut origin_data_block = read_block(
            self.write_settings.storage_format,
            &self.block_reader,
            block_meta,
            &self.read_settings,
        )
        .await?;
        let origin_num_rows = origin_data_block.num_rows();
        if self.stream_ctx.is_some() {
            let row_num = build_origin_block_row_num(origin_num_rows);
            origin_data_block.add_column(row_num);
        }

        // apply delete
        let mut bitmap = MutableBitmap::new();
        for row in 0..origin_num_rows {
            if modified_offsets.contains(&row) {
                bitmap.push(false);
            } else {
                bitmap.push(true);
            }
        }
        let mut res_block = origin_data_block.filter_with_bitmap(&bitmap.into())?;

        if res_block.is_empty() {
            metrics_inc_merge_into_deleted_blocks_counter(1);
            metrics_inc_merge_into_deleted_blocks_rows_counter(origin_num_rows as u32);
            return Ok(Some(MutationLogEntry::DeletedBlock {
                index: BlockMetaIndex {
                    segment_idx,
                    block_idx,
                },
            }));
        }

        if let Some(stream_ctx) = &self.stream_ctx {
            let stream_meta = gen_mutation_stream_meta(None, &block_meta.location.0)?;
            res_block = stream_ctx.apply(res_block, &stream_meta)?;
        }

        // serialization and compression is cpu intensive, send them to dedicated thread pool
        // and wait (asyncly, which will NOT block the executor thread)
        let block_builder = self.block_builder.clone();
        let origin_stats = block_meta.cluster_stats.clone();

        let serialized = GlobalIORuntime::instance()
            .spawn(async move {
                            block_builder.build(vec![res_block], |block, generator| {
                                let cluster_stats =
                                    generator.gen_with_origin_stats(&block, origin_stats.clone())?;
                                info!(
                                    "serialize block after get cluster_stats:\n {:?}",
                                    cluster_stats
                                );
                                Ok((cluster_stats, block))
                            })
                        })
            .await
            .map_err(|e| {
                ErrorCode::Internal(
                    "unexpected, failed to serialize block when apply update and delete to data block for merge into",
                )
                .add_message_back(e.to_string())
            })??;

        // persistent data

        let new_block_meta = BlockWriter::write_down(&self.data_accessor, serialized).await?;

        metrics_inc_merge_into_replace_blocks_counter(1);
        metrics_inc_merge_into_replace_blocks_rows_counter(origin_num_rows as u32);
        // generate log
        let mutation = MutationLogEntry::ReplacedBlock {
            index: BlockMetaIndex {
                segment_idx,
                block_idx,
            },
            block_meta: Arc::new(new_block_meta),
        };

        Ok(Some(mutation))
    }
}
