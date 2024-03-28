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

use ahash::AHashMap;
use databend_common_arrow::arrow::bitmap::MutableBitmap;
use databend_common_base::base::tokio::sync::Semaphore;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_base::runtime::TrySpawn;
use databend_common_catalog::plan::gen_mutation_stream_meta;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnId;
use databend_common_expression::ComputedExpr;
use databend_common_expression::DataBlock;
use databend_common_expression::FieldIndex;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::Value;
use databend_common_metrics::storage::*;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::executor::physical_plans::OnConflictField;
use databend_common_sql::StreamContext;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_index::filters::Filter;
use databend_storages_common_index::filters::Xor8Filter;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::BlockSlotDescription;
use databend_storages_common_table_meta::meta::ColumnStatistics;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SegmentInfo;
use log::info;
use log::warn;
use opendal::Operator;

use crate::io::read::bloom::block_filter_reader::BloomBlockFilterReader;
use crate::io::write_data;
use crate::io::BlockBuilder;
use crate::io::BlockReader;
use crate::io::CompactSegmentInfoReader;
use crate::io::MetaReaders;
use crate::io::ReadSettings;
use crate::io::WriteSettings;
use crate::operations::acquire_task_permit;
use crate::operations::common::BlockMetaIndex;
use crate::operations::common::MutationLogEntry;
use crate::operations::common::MutationLogs;
use crate::operations::mutation::BlockIndex;
use crate::operations::mutation::SegmentIndex;
use crate::operations::read_block;
use crate::operations::replace_into::meta::DeletionByColumn;
use crate::operations::replace_into::meta::MergeIntoOperation;
use crate::operations::replace_into::meta::UniqueKeyDigest;
use crate::operations::replace_into::mutator::row_hash_of_columns;
use crate::operations::replace_into::mutator::DeletionAccumulator;
use crate::FuseTable;

struct AggregationContext {
    segment_locations: AHashMap<SegmentIndex, Location>,
    block_slots_in_charge: Option<BlockSlotDescription>,
    // the fields specified in ON CONFLICT clause
    on_conflict_fields: Vec<OnConflictField>,
    // the field indexes of `on_conflict_fields`
    // which we should apply bloom filtering, if any
    bloom_filter_column_indexes: Vec<FieldIndex>,
    // table fields excludes `on_conflict_fields`
    remain_column_field_ids: Vec<FieldIndex>,
    // reader that reads the ON CONFLICT key fields
    key_column_reader: Arc<BlockReader>,
    // reader that reads the `remain_column_field_ids`
    remain_column_reader: Option<Arc<BlockReader>>,
    data_accessor: Operator,
    write_settings: WriteSettings,
    read_settings: ReadSettings,
    segment_reader: CompactSegmentInfoReader,
    block_builder: BlockBuilder,
    io_request_semaphore: Arc<Semaphore>,
    query_id: String,
    // generate stream columns if necessary
    stream_ctx: Option<StreamContext>,
}

// Apply MergeIntoOperations to segments
pub struct MergeIntoOperationAggregator {
    ctx: Arc<dyn TableContext>,
    deletion_accumulator: DeletionAccumulator,
    aggregation_ctx: Arc<AggregationContext>,
}

impl MergeIntoOperationAggregator {
    #[allow(clippy::too_many_arguments)] // TODO fix this
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        on_conflict_fields: Vec<OnConflictField>,
        bloom_filter_column_indexes: Vec<FieldIndex>,
        segment_locations: Vec<(SegmentIndex, Location)>,
        block_slots: Option<BlockSlotDescription>,
        table: &FuseTable,
        read_settings: ReadSettings,
        block_builder: BlockBuilder,
        io_request_semaphore: Arc<Semaphore>,
    ) -> Result<Self> {
        let data_accessor = table.get_operator();
        let table_schema = table.schema_with_stream();
        let write_settings = table.get_write_settings();
        let update_stream_columns = table.change_tracking_enabled();

        let deletion_accumulator = DeletionAccumulator::default();
        let segment_reader =
            MetaReaders::segment_info_reader(data_accessor.clone(), table_schema.clone());

        // order matters, later the projection that used by block readers depends on the order
        let key_column_field_indexes: Vec<FieldIndex> =
            on_conflict_fields.iter().map(|i| i.field_index).collect();

        let remain_column_field_ids: Vec<FieldIndex> = {
            let all_field_indexes = table_schema
                .fields()
                .iter()
                .enumerate()
                .filter(|(_, f)| !matches!(f.computed_expr(), Some(ComputedExpr::Virtual(_))))
                .map(|(i, _)| i)
                .collect::<Vec<FieldIndex>>();

            all_field_indexes
                .into_iter()
                .filter(|index| !key_column_field_indexes.contains(index))
                .collect()
        };

        let key_column_reader = {
            let projection = Projection::Columns(key_column_field_indexes);
            BlockReader::create(
                ctx.clone(),
                data_accessor.clone(),
                table_schema.clone(),
                projection,
                false,
                update_stream_columns,
                false,
            )
        }?;

        let remain_column_reader = {
            if remain_column_field_ids.is_empty() {
                None
            } else {
                let projection = Projection::Columns(remain_column_field_ids.clone());
                let reader = BlockReader::create(
                    ctx.clone(),
                    data_accessor.clone(),
                    table_schema.clone(),
                    projection,
                    false,
                    update_stream_columns,
                    false,
                )?;
                Some(reader)
            }
        };
        let query_id = ctx.get_id();

        let stream_ctx = if update_stream_columns {
            Some(StreamContext::try_create(
                ctx.get_function_context()?,
                table_schema,
                table.get_table_info().ident.seq,
                true,
            )?)
        } else {
            None
        };

        Ok(Self {
            ctx,
            deletion_accumulator,
            aggregation_ctx: Arc::new(AggregationContext {
                segment_locations: AHashMap::from_iter(segment_locations),
                block_slots_in_charge: block_slots,
                on_conflict_fields,
                bloom_filter_column_indexes,
                remain_column_field_ids,
                key_column_reader,
                remain_column_reader,
                data_accessor,
                write_settings,
                read_settings,
                segment_reader,
                block_builder,
                io_request_semaphore,
                query_id,
                stream_ctx,
            }),
        })
    }
}

// aggregate mutations (currently, deletion only)
impl MergeIntoOperationAggregator {
    #[async_backtrace::framed]
    pub async fn accumulate(&mut self, merge_into_operation: MergeIntoOperation) -> Result<()> {
        let aggregation_ctx = &self.aggregation_ctx;
        metrics_inc_replace_number_accumulated_merge_action();

        let start = Instant::now();
        match merge_into_operation {
            MergeIntoOperation::Delete(partitions) => {
                for (segment_index, (path, ver)) in &aggregation_ctx.segment_locations {
                    // segment level
                    let load_param = LoadParams {
                        location: path.clone(),
                        len_hint: None,
                        ver: *ver,
                        put_cache: true,
                    };
                    let compact_segment_info =
                        aggregation_ctx.segment_reader.read(&load_param).await?;
                    let mut segment_info: Option<SegmentInfo> = None;

                    for DeletionByColumn {
                        columns_min_max,
                        key_hashes,
                        bloom_hashes,
                    } in &partitions
                    {
                        if aggregation_ctx
                            .overlapped(&compact_segment_info.summary.col_stats, columns_min_max)
                        {
                            let seg = match &segment_info {
                                None => {
                                    // un-compact the segment if necessary
                                    segment_info = Some(compact_segment_info.clone().try_into()?);
                                    segment_info.as_ref().unwrap()
                                }
                                Some(v) => v,
                            };

                            // block level pruning, using range index
                            for (block_index, block_meta) in seg.blocks.iter().enumerate() {
                                if let Some(BlockSlotDescription { num_slots, slot }) =
                                    &aggregation_ctx.block_slots_in_charge
                                {
                                    if block_index % num_slots != *slot as usize {
                                        // skip this block
                                        continue;
                                    }
                                }
                                if aggregation_ctx
                                    .overlapped(&block_meta.col_stats, columns_min_max)
                                {
                                    self.deletion_accumulator.add_block_deletion(
                                        *segment_index,
                                        block_index,
                                        key_hashes,
                                        bloom_hashes,
                                    )
                                }
                            }
                        }
                    }
                }
            }
            MergeIntoOperation::None => {}
        }

        metrics_inc_replace_accumulated_merge_action_time_ms(start.elapsed().as_millis() as u64);
        Ok(())
    }
}

// apply the mutations and generate mutation log
impl MergeIntoOperationAggregator {
    #[async_backtrace::framed]
    pub async fn apply(&mut self) -> Result<Option<MutationLogs>> {
        metrics_inc_replace_number_apply_deletion();

        // track number of segments and blocks after pruning (per merge action application)
        {
            metrics_inc_replace_segment_number_after_pruning(
                self.deletion_accumulator.deletions.len() as u64,
            );

            let num_blocks_mutated = self
                .deletion_accumulator
                .deletions
                .values()
                .fold(0, |acc, blocks_may_have_row_deletion| {
                    acc + blocks_may_have_row_deletion.len()
                });

            metrics_inc_replace_block_number_after_pruning(num_blocks_mutated as u64);
        }

        let start = Instant::now();
        let mut mutation_logs = Vec::new();
        let aggregation_ctx = &self.aggregation_ctx;
        let io_runtime = GlobalIORuntime::instance();
        let mut mutation_log_handlers = Vec::new();
        let mut num_rows_mutated = 0;
        for (segment_idx, block_deletion) in self.deletion_accumulator.deletions.drain() {
            let (path, ver) = self
                .aggregation_ctx
                .segment_locations
                .get(&segment_idx)
                .ok_or_else(|| {
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

            let compact_segment_info = aggregation_ctx.segment_reader.read(&load_param).await?;
            let segment_info: SegmentInfo = compact_segment_info.try_into()?;

            for (block_index, keys) in block_deletion {
                let permit =
                    acquire_task_permit(aggregation_ctx.io_request_semaphore.clone()).await?;
                let block_meta = segment_info.blocks[block_index].clone();
                let aggregation_ctx = aggregation_ctx.clone();
                num_rows_mutated += block_meta.row_count;
                // self.aggregation_ctx.
                let handle = io_runtime.spawn(self.ctx.get_id(), async move {
                    let mutation_log_entry = aggregation_ctx
                        .apply_deletion_to_data_block(segment_idx, block_index, &block_meta, &keys)
                        .await?;
                    drop(permit);
                    Ok::<_, ErrorCode>(mutation_log_entry)
                });
                mutation_log_handlers.push(handle)
            }
        }
        if num_rows_mutated > 0 {
            metrics_inc_replace_row_number_after_pruning(num_rows_mutated);
        }

        let log_entries = futures::future::try_join_all(mutation_log_handlers)
            .await
            .map_err(|e| {
                ErrorCode::Internal("unexpected, failed to join apply-deletion tasks.")
                    .add_message_back(e.to_string())
            })?;

        for maybe_log_entry in log_entries {
            if let Some(segment_mutation_log) = maybe_log_entry? {
                mutation_logs.push(segment_mutation_log);
            }
        }

        metrics_inc_replace_apply_deletion_time_ms(start.elapsed().as_millis() as u64);

        Ok(Some(MutationLogs {
            entries: mutation_logs,
        }))
    }
}

impl AggregationContext {
    #[async_backtrace::framed]
    async fn apply_deletion_to_data_block(
        &self,
        segment_index: SegmentIndex,
        block_index: BlockIndex,
        block_meta: &BlockMeta,
        deleted_key_hashes: &(ahash::HashSet<UniqueKeyDigest>, Vec<Vec<u64>>),
    ) -> Result<Option<MutationLogEntry>> {
        let (deleted_key_hashes, bloom_hashes) = deleted_key_hashes;
        info!(
            "apply delete to segment idx {}, block idx {}, num of deletion key hashes: {}",
            segment_index,
            block_index,
            deleted_key_hashes.len()
        );

        if block_meta.row_count == 0 {
            return Ok(None);
        }

        // apply bloom filter pruning if possible
        let pruned = self
            .apply_bloom_pruning(block_meta, bloom_hashes, &self.bloom_filter_column_indexes)
            .await;

        if pruned {
            // skip this block
            metrics_inc_replace_block_number_bloom_pruned(1);
            return Ok(None);
        }

        let key_columns_data = read_block(
            self.write_settings.storage_format,
            &self.key_column_reader,
            block_meta,
            &self.read_settings,
            self.query_id.clone(),
        )
        .await?;

        let num_rows = key_columns_data.num_rows();

        let on_conflict_fields = &self.on_conflict_fields;
        let mut columns = Vec::with_capacity(on_conflict_fields.len());
        for (field, _) in on_conflict_fields.iter().enumerate() {
            let on_conflict_field_index = field;
            columns.push(&key_columns_data
                .columns()
                .get(on_conflict_field_index)
                .ok_or_else(|| {
                    ErrorCode::Internal(format!(
                        "unexpected, block entry (index {}) not found. segment index {}, block index {}",
                        on_conflict_field_index, segment_index, block_index
                    ))
                })?
                .value);
        }

        let mut bitmap = MutableBitmap::new();
        for row in 0..num_rows {
            if let Some(hash) = row_hash_of_columns(&columns, row)? {
                // some row hash means on-conflict columns of this row contains non-null values
                // let's check it out
                bitmap.push(!deleted_key_hashes.contains(&hash));
            } else {
                // otherwise, keep this row
                bitmap.push(true);
            }
        }

        let delete_nums = bitmap.unset_bits();
        info!("number of row deleted: {}", delete_nums);

        // shortcut: nothing to be deleted
        if delete_nums == 0 {
            info!("nothing deleted");
            metrics_inc_replace_block_of_zero_row_deleted(1);
            // nothing to be deleted
            return Ok(None);
        }

        let progress_values = ProgressValues {
            rows: delete_nums,
            // ignore bytes.
            bytes: 0,
        };

        self.block_builder
            .ctx
            .get_write_progress()
            .incr(&progress_values);

        // shortcut: whole block deletion
        if delete_nums == block_meta.row_count as usize {
            info!("whole block deletion");
            metrics_inc_replace_whole_block_deletion(1);
            metrics_inc_replace_deleted_blocks_rows(num_rows as u64);
            // whole block deletion
            // NOTE that if deletion marker is enabled, check the real meaning of `row_count`
            let mutation = MutationLogEntry::DeletedBlock {
                index: BlockMetaIndex {
                    segment_idx: segment_index,
                    block_idx: block_index,
                },
            };

            return Ok(Some(mutation));
        }

        let bitmap = bitmap.into();
        let mut key_columns_data_after_deletion = key_columns_data.filter_with_bitmap(&bitmap)?;

        let mut new_block = match &self.remain_column_reader {
            None => key_columns_data_after_deletion,
            Some(remain_columns_reader) => {
                metrics_inc_replace_block_number_totally_loaded(1);
                metrics_inc_replace_row_number_totally_loaded(block_meta.row_count);

                // read the remaining columns
                let remain_columns_data =
                    self.read_block(remain_columns_reader, block_meta).await?;

                // remove the deleted rows
                let remain_columns_data_after_deletion =
                    remain_columns_data.filter_with_bitmap(&bitmap)?;

                // merge the remaining columns
                for col in remain_columns_data_after_deletion.columns() {
                    key_columns_data_after_deletion.add_column(col.clone());
                }

                // resort the block
                let col_indexes = self
                    .on_conflict_fields
                    .iter()
                    .map(|f| f.field_index)
                    .chain(self.remain_column_field_ids.iter().copied())
                    .collect::<Vec<_>>();
                let mut projection = (0..col_indexes.len()).collect::<Vec<_>>();
                projection.sort_by_key(|&i| col_indexes[i]);
                let func_ctx = self.block_builder.ctx.get_function_context()?;
                BlockOperator::Project { projection }
                    .execute(&func_ctx, key_columns_data_after_deletion)?
            }
        };

        if let Some(stream_ctx) = &self.stream_ctx {
            // generate row id column
            let mut row_ids = Vec::with_capacity(num_rows);
            for i in 0..num_rows {
                row_ids.push(i as u64);
            }
            let value = Value::Column(Column::filter(&UInt64Type::from_data(row_ids), &bitmap));
            let row_num = BlockEntry::new(
                DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt64))),
                value.wrap_nullable(None),
            );
            new_block.add_column(row_num);

            let stream_meta = gen_mutation_stream_meta(None, &block_meta.location.0)?;
            new_block = stream_ctx.apply(new_block, &stream_meta)?;
        }

        // serialization and compression is cpu intensive, send them to dedicated thread pool
        // and wait (asyncly, which will NOT block the executor thread)
        let block_builder = self.block_builder.clone();
        let origin_stats = block_meta.cluster_stats.clone();
        let serialized = GlobalIORuntime::instance()
            .spawn(self.query_id.clone(), async move {
                block_builder.build(new_block, |block, generator| {
                    let cluster_stats =
                        generator.gen_with_origin_stats(&block, origin_stats.clone())?;
                    Ok((cluster_stats, block))
                })
            })
            .await
            .map_err(|e| {
                ErrorCode::Internal(
                    "unexpected, failed to join apply delete tasks for replace into.",
                )
                .add_message_back(e.to_string())
            })??;

        // persistent data
        let new_block_meta = serialized.block_meta;
        let new_block_location = new_block_meta.location.0.clone();
        let new_block_raw_data = serialized.block_raw_data;
        let data_accessor = self.data_accessor.clone();
        write_data(new_block_raw_data, &data_accessor, &new_block_location).await?;

        metrics_inc_replace_block_number_write(1);
        metrics_inc_replace_row_number_write(new_block_meta.row_count);
        metrics_inc_replace_replaced_blocks_rows(num_rows as u64);
        if let Some(index_state) = serialized.bloom_index_state {
            write_data(index_state.data, &data_accessor, &index_state.location.0).await?;
        }

        // generate log
        let mutation = MutationLogEntry::ReplacedBlock {
            index: BlockMetaIndex {
                segment_idx: segment_index,
                block_idx: block_index,
            },
            block_meta: Arc::new(new_block_meta),
        };

        Ok(Some(mutation))
    }

    fn overlapped(
        &self,
        column_stats: &HashMap<ColumnId, ColumnStatistics>,
        columns_min_max: &[(Scalar, Scalar)],
    ) -> bool {
        Self::check_overlap(&self.on_conflict_fields, column_stats, columns_min_max)
    }

    // if any item of `column_min_max` does NOT overlap with the corresponding item of `column_stats`
    // returns false, otherwise returns true.
    fn check_overlap(
        on_conflict_fields: &[OnConflictField],
        column_stats: &HashMap<ColumnId, ColumnStatistics>,
        columns_min_max: &[(Scalar, Scalar)],
    ) -> bool {
        for (idx, field) in on_conflict_fields.iter().enumerate() {
            let column_id = field.table_field.column_id();
            let (min, max) = &columns_min_max[idx];
            if !Self::check_overlapped_by_stats(column_stats.get(&column_id), min, max) {
                return false;
            }
        }
        true
    }

    fn check_overlapped_by_stats(
        column_stats: Option<&ColumnStatistics>,
        key_min: &Scalar,
        key_max: &Scalar,
    ) -> bool {
        if let Some(stats) = column_stats {
            let max = stats.max();
            let min = stats.min();
            std::cmp::min(key_max, max) >= std::cmp::max(key_min,min)
                || // coincide overlap
                (max == key_max && min == key_min)
        } else {
            false
        }
    }

    async fn read_block(&self, reader: &BlockReader, block_meta: &BlockMeta) -> Result<DataBlock> {
        let merged_io_read_result = reader
            .read_columns_data_by_merge_io(
                &self.read_settings,
                &block_meta.location.0,
                &block_meta.col_metas,
                &None,
            )
            .await?;

        // deserialize block data
        // cpu intensive task, send them to dedicated thread pool
        let storage_format = self.write_settings.storage_format;
        let block_meta_ptr = block_meta.clone();
        let reader = reader.clone();
        GlobalIORuntime::instance()
            .spawn(self.query_id.clone(), async move {
                let column_chunks = merged_io_read_result.columns_chunks()?;
                reader.deserialize_chunks(
                    block_meta_ptr.location.0.as_str(),
                    block_meta_ptr.row_count as usize,
                    &block_meta_ptr.compression,
                    &block_meta_ptr.col_metas,
                    column_chunks,
                    &storage_format,
                )
            })
            .await
            .map_err(|e| {
                ErrorCode::Internal(
                    "unexpected, failed to join aggregation context read block tasks for replace into.",
                )
                .add_message_back(e.to_string())
            })?
    }

    // return true if the block is pruned, otherwise false
    pub async fn apply_bloom_pruning(
        &self,
        block_meta: &BlockMeta,
        input_hashes: &[Vec<u64>],
        bloom_on_conflict_field_index: &[FieldIndex],
    ) -> bool {
        if bloom_on_conflict_field_index.is_empty() {
            return false;
        }
        if let Some(loc) = &block_meta.bloom_filter_index_location {
            let filters = load_bloom_filter(
                self.data_accessor.clone(),
                &self.on_conflict_fields,
                loc,
                block_meta.bloom_filter_index_size,
                bloom_on_conflict_field_index,
            )
            .await;
            try_prune_use_bloom_filter(filters, input_hashes)
        } else {
            // no bloom filter, no pruning
            false
        }
    }
}

pub fn try_prune_use_bloom_filter(
    filters: Result<Vec<Option<Arc<Xor8Filter>>>>,
    input_hashes: &[Vec<u64>],
) -> bool {
    match filters {
        Ok(filters) => {
            // the caller ensures that the input_hashes is not empty
            let row_count = input_hashes[0].len();

            // let assume that the target block is prunable
            let mut block_pruned = true;
            for row in 0..row_count {
                // for each row, by default, assume that columns of this row do have conflict with the target block.
                let mut row_not_prunable = true;
                for (col_idx, col_hash) in input_hashes.iter().enumerate() {
                    // For each column of current row, check if the corresponding bloom
                    // filter contains the digest of the column.
                    //
                    // Any one of the columns NOT contains by the corresponding bloom filter,
                    // indicates that the row is prunable(thus, we do not stop on the first column that
                    // the bloom filter contains).

                    // - if bloom filter presents, check if the column is contained
                    // - if bloom filter absents, do nothing(since by default, we assume that the row is not-prunable)
                    if let Some(col_filter) = &filters[col_idx] {
                        let hash = col_hash[row];
                        if hash == 0 || !col_filter.contains_digest(hash) {
                            // - hash == 0 indicates that the column value is null, which equals nothing.
                            // - NOT `contains_digest`, indicates that this column of row does not match
                            row_not_prunable = false;
                            // if one column not match, we do not need to check other columns
                            break;
                        }
                    }
                }
                if row_not_prunable {
                    // any row not prunable indicates that the target block is not prunable
                    block_pruned = false;
                    break;
                }
            }
            block_pruned
        }
        Err(e) => {
            // broken index should not stop us:
            warn!("failed to build bloom index column name: {}", e);
            // failed to load bloom filter, do not prune
            false
        }
    }
}

pub async fn load_bloom_filter(
    data_accessor: Operator,
    on_conflict_fields: &[OnConflictField],
    location: &Location,
    index_len: u64,
    bloom_on_conflict_field_index: &[FieldIndex],
) -> Result<Vec<Option<Arc<Xor8Filter>>>> {
    // different block may have different version of bloom filter index
    let mut col_names = Vec::with_capacity(bloom_on_conflict_field_index.len());

    for idx in bloom_on_conflict_field_index {
        let bloom_column_name = BloomIndex::build_filter_column_name(
            location.1,
            &on_conflict_fields[*idx].table_field,
        )?;
        col_names.push(bloom_column_name);
    }

    // using load_bloom_filter_by_columns is attractive,
    // but it do not care about the version of the bloom filter index
    let block_filter = location
        .read_block_filter(data_accessor.clone(), &col_names, index_len)
        .await?;

    // reorder the filter according to the order of bloom_on_conflict_field
    let mut filters = Vec::with_capacity(bloom_on_conflict_field_index.len());
    for filter_col_name in &col_names {
        match block_filter.filter_schema.index_of(filter_col_name) {
            Ok(idx) => {
                filters.push(Some(block_filter.filters[idx].clone()));
            }
            Err(_) => {
                info!(
                    "bloom filter column {} not found for block {}",
                    filter_col_name, location.0
                );
                filters.push(None);
            }
        }
    }

    Ok(filters)
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;
    use databend_common_expression::TableDataType;
    use databend_common_expression::TableField;
    use databend_common_expression::TableSchema;

    use super::*;

    #[test]
    fn test_check_overlap() -> Result<()> {
        // setup :
        //
        // - on conflict('xx_id', 'xx_type', 'xx_time');
        //
        // - range index of columns
        //   'xx_id' : [1, 10]
        //   'xx_type' : ["a", "z"]
        //   'xx_time' : [100, 200]

        // setup schema
        let field_type_id = TableDataType::Number(NumberDataType::UInt64);
        let field_type_string = TableDataType::String;
        let field_type_time = TableDataType::Number(NumberDataType::UInt32);

        let xx_id = TableField::new("xx_id", field_type_id);
        let xx_type = TableField::new("xx_type", field_type_string);
        let xx_time = TableField::new("xx_time", field_type_time);

        let schema = TableSchema::new(vec![xx_id, xx_type, xx_time]);

        let fields = schema.fields();

        // setup the ON CONFLICT fields
        let on_conflict_fields = fields
            .iter()
            .enumerate()
            .map(|(id, field)| OnConflictField {
                table_field: field.clone(),
                field_index: id,
            })
            .collect::<Vec<_>>();

        // set up range index of columns
        // the null_count/in_memory_size/distinct_of_values do not matter in this case
        let range = |min: Scalar, max: Scalar| ColumnStatistics::new(min, max, 0, 0, None);

        let column_range_indexes = HashMap::from_iter([
            // range of xx_id [1, 10]
            (
                0,
                range(
                    Scalar::Number(NumberScalar::UInt64(1)),
                    Scalar::Number(NumberScalar::UInt64(10)),
                ),
            ),
            // range of xx_type [a, z]
            (
                1,
                range(
                    Scalar::String("a".to_string()),
                    Scalar::String("z".to_string()),
                ),
            ),
            // range of xx_time [100, 200]
            (
                2,
                range(
                    Scalar::Number(NumberScalar::UInt32(100)),
                    Scalar::Number(NumberScalar::UInt32(200)),
                ),
            ),
        ]);

        // case 1:
        //
        // - min/max of input block
        //
        //  'xx_id' : [1, 9]
        //  'xx_type' : ["b", "y"]
        //  'xx_time' : [101, 200]
        //
        // - recall that the range index of columns are:
        //
        //   'xx_id' : [1, 10]
        //   'xx_type' : ["a", "z"]
        //   'xx_time' : [100, 200]
        //
        // - expected : overlap == true
        //   since value of all the ON CONFLICT columns of input block overlap with range index

        let input_column_min_max = [
            // for xx_id column, overlaps
            (
                Scalar::Number(NumberScalar::UInt64(1)),
                Scalar::Number(NumberScalar::UInt64(9)),
            ),
            // for xx_type column, overlaps
            (
                Scalar::String("b".to_string()),
                Scalar::String("y".to_string()),
            ),
            // for xx_time column, overlaps
            (
                Scalar::Number(NumberScalar::UInt32(101)),
                Scalar::Number(NumberScalar::UInt32(200)),
            ),
        ];

        let overlap = super::AggregationContext::check_overlap(
            &on_conflict_fields,
            &column_range_indexes,
            &input_column_min_max,
        );

        assert!(overlap);

        // case 2:
        //
        // - min/max of input block
        //
        //  'xx_id' : [11, 12]
        //  'xx_type' : ["b", "b"]
        //  'xx_time' : [100, 100]
        //
        // - recall that the range index of columns are:
        //
        //   'xx_id' : [1, 10]
        //   'xx_type' : ["a", "z"]
        //   'xx_time' : [100, 200]
        //
        // - expected : overlap == false
        //
        //   although columns 'xx_type' and 'xx_time' do overlap, but 'xx_id' does not overlap,
        //   so the result is NOT overlap

        let input_column_min_max = [
            // for xx_id column, NOT overlaps
            (
                Scalar::Number(NumberScalar::UInt64(11)),
                Scalar::Number(NumberScalar::UInt64(12)),
            ),
            // for xx_type column, overlaps
            (
                Scalar::String("b".to_string()),
                Scalar::String("b".to_string()),
            ),
            // for xx_time column, overlaps
            (
                Scalar::Number(NumberScalar::UInt32(100)),
                Scalar::Number(NumberScalar::UInt32(100)),
            ),
        ];

        let overlap = super::AggregationContext::check_overlap(
            &on_conflict_fields,
            &column_range_indexes,
            &input_column_min_max,
        );

        assert!(!overlap);

        Ok(())
    }
}
