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

use std::sync::LazyLock;

use databend_common_base::runtime::metrics::register_counter;
use databend_common_base::runtime::metrics::register_histogram_in_milliseconds;
use databend_common_base::runtime::metrics::Counter;
use databend_common_base::runtime::metrics::Histogram;

// Common metrics.
static OMIT_FILTER_ROWGROUPS: LazyLock<Counter> =
    LazyLock::new(|| register_counter("omit_filter_rowgroups"));
static OMIT_FILTER_ROWS: LazyLock<Counter> = LazyLock::new(|| register_counter("omit_filter_rows"));

// COPY metrics.
static COPY_PURGE_FILE_COUNTER: LazyLock<Counter> =
    LazyLock::new(|| register_counter("copy_purge_file_counter"));
static COPY_PURGE_FILE_COST_MILLISECONDS: LazyLock<Histogram> =
    LazyLock::new(|| register_histogram_in_milliseconds("copy_purge_file_cost_milliseconds"));
static COPY_READ_PART_COUNTER: LazyLock<Counter> =
    LazyLock::new(|| register_counter("copy_read_part_counter"));
static COPY_READ_SIZE_BYTES: LazyLock<Counter> =
    LazyLock::new(|| register_counter("copy_read_size_bytes"));
static COPY_READ_PART_COST_MILLISECONDS: LazyLock<Histogram> =
    LazyLock::new(|| register_histogram_in_milliseconds("copy_read_part_cost_milliseconds"));
static COPY_FILTER_OUT_COPIED_FILES_REQUEST_MILLISECONDS: LazyLock<Histogram> =
    LazyLock::new(|| {
        register_histogram_in_milliseconds("copy_filter_out_copied_files_request_milliseconds")
    });
static COPY_FILTER_OUT_COPIED_FILES_ENTIRE_MILLISECONDS: LazyLock<Histogram> =
    LazyLock::new(|| {
        register_histogram_in_milliseconds("copy_filter_out_copied_files_entire_milliseconds")
    });
static COPY_COLLECT_FILES_GET_ALL_SOURCE_FILES_MILLISECONDS: LazyLock<Histogram> =
    LazyLock::new(|| {
        register_histogram_in_milliseconds("copy_collect_files_get_all_source_files_milliseconds")
    });

// Merge into metrics.
static MERGE_INTO_REPLACE_BLOCKS_COUNTER: LazyLock<Counter> =
    LazyLock::new(|| register_counter("merge_into_replace_blocks_counter"));
static MERGE_INTO_REPLACE_BLOCKS_ROWS_COUNTER: LazyLock<Counter> =
    LazyLock::new(|| register_counter("merge_into_replace_blocks_rows_counter"));
static MERGE_INTO_DELETED_BLOCKS_COUNTER: LazyLock<Counter> =
    LazyLock::new(|| register_counter("merge_into_deleted_blocks_counter"));
static MERGE_INTO_DELETED_BLOCKS_ROWS_COUNTER: LazyLock<Counter> =
    LazyLock::new(|| register_counter("merge_into_deleted_blocks_rows_counter"));
static MERGE_INTO_APPEND_BLOCKS_COUNTER: LazyLock<Counter> =
    LazyLock::new(|| register_counter("merge_into_append_blocks_counter"));
static MERGE_INTO_DISTRIBUTED_HASHTABLE_FETCH_ROWNUMBER: LazyLock<Counter> =
    LazyLock::new(|| register_counter("merge_into_distributed_hashtable_fetch_row_number"));
static MERGE_INTO_DISTRIBUTED_HASHTABLE_EMPTY_BLOCK: LazyLock<Counter> =
    LazyLock::new(|| register_counter("merge_into_distributed_hashtable_empty_block"));
static MERGE_INTO_DISTRIBUTED_GENERATE_ROW_NUMBERS: LazyLock<Counter> =
    LazyLock::new(|| register_counter("merge_into_distributed_generate_row_numbers"));
static MERGE_INTO_DISTRIBUTED_INIT_UNIQUE_NUMBER: LazyLock<Counter> =
    LazyLock::new(|| register_counter("merge_into_distributed_init_unique_number"));
static MERGE_INTO_DISTRIBUTED_NEW_SET_LEN: LazyLock<Counter> =
    LazyLock::new(|| register_counter("merge_into_distributed_new_set_len"));
static MERGE_INTO_DISTRIBUTED_HASHTABLE_PUSH_EMPTY_NULL_BLOCK: LazyLock<Counter> =
    LazyLock::new(|| register_counter("merge_into_distributed_hashtable_push_empty_null_block"));
static MERGE_INTO_DISTRIBUTED_HASHTABLE_PUSH_NULL_BLOCK: LazyLock<Counter> =
    LazyLock::new(|| register_counter("merge_into_distributed_hashtable_push_null_block"));
static MERGE_INTO_DISTRIBUTED_HASHTABLE_PUSH_NULL_BLOCK_ROWS: LazyLock<Counter> =
    LazyLock::new(|| register_counter("merge_into_distributed_hashtable_push_null_block_rows"));
static MERGE_INTO_APPEND_BLOCKS_ROWS_COUNTER: LazyLock<Counter> =
    LazyLock::new(|| register_counter("merge_into_append_blocks_rows_counter"));
static MERGE_INTO_MATCHED_ROWS: LazyLock<Counter> =
    LazyLock::new(|| register_counter("merge_into_matched_rows"));
static MERGE_INTO_UNMATCHED_ROWS: LazyLock<Counter> =
    LazyLock::new(|| register_counter("merge_into_unmatched_rows"));
static MERGE_INTO_DISTRIBUTED_DEDUPLICATE_ROWNUMBER: LazyLock<Counter> =
    LazyLock::new(|| register_counter("merge_into_distributed_deduplicate_row_number"));
static MERGE_INTO_DISTRIBUTED_EMPTY_ROWNUMBER: LazyLock<Counter> =
    LazyLock::new(|| register_counter("merge_into_distributed_empty_row_number"));
static MERGE_INTO_DISTRIBUTED_APPLY_ROWNUMBER: LazyLock<Counter> =
    LazyLock::new(|| register_counter("merge_into_distributed_apply_row_number"));
static MERGE_INTO_ACCUMULATE_MILLISECONDS: LazyLock<Histogram> =
    LazyLock::new(|| register_histogram_in_milliseconds("merge_into_accumulate_milliseconds"));
static MERGE_INTO_APPLY_MILLISECONDS: LazyLock<Histogram> =
    LazyLock::new(|| register_histogram_in_milliseconds("merge_into_apply_milliseconds"));
static MERGE_INTO_SPLIT_MILLISECONDS: LazyLock<Histogram> =
    LazyLock::new(|| register_histogram_in_milliseconds("merge_into_split_milliseconds"));
static MERGE_INTO_NOT_MATCHED_OPERATION_MILLISECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram_in_milliseconds("merge_into_not_matched_operation_milliseconds")
});
static MERGE_INTO_MATCHED_OPERATION_MILLISECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram_in_milliseconds("merge_into_matched_operation_milliseconds")
});

// Fuse engine metrics.
static COMMIT_MUTATION_UNRESOLVABLE_CONFLICT: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_commit_mutation_unresolvable_conflict"));
static COMMIT_MUTATION_LATEST_SNAPSHOT_APPEND_ONLY: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_commit_mutation_latest_snapshot_append_only"));
static COMMIT_MUTATION_MODIFIED_SEGMENT_EXISTS_IN_LATEST: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_commit_mutation_modified_segment_exists_in_latest"));
static COMMIT_MUTATION_RETRY: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_commit_mutation_retry"));
static COMMIT_MUTATION_SUCCESS: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_commit_mutation_success"));
static COMMIT_COPIED_FILES: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_commit_copied_files"));
static COMMIT_MILLISECONDS: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_commit_milliseconds"));
static COMMIT_ABORTS: LazyLock<Counter> = LazyLock::new(|| register_counter("fuse_commit_aborts"));
static REMOTE_IO_SEEKS: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_remote_io_seeks"));
static REMOTE_IO_SEEKS_AFTER_MERGED: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_remote_io_seeks_after_merged"));
static REMOTE_IO_READ_BYTES: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_remote_io_read_bytes"));
static REMOTE_IO_READ_BYTES_AFTER_MERGED: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_remote_io_read_bytes_after_merged"));
static REMOTE_IO_READ_PARTS: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_remote_io_read_parts"));
static REMOTE_IO_READ_MILLISECONDS: LazyLock<Histogram> =
    LazyLock::new(|| register_histogram_in_milliseconds("fuse_remote_io_read_milliseconds"));
static REMOTE_IO_DESERIALIZE_MILLISECONDS: LazyLock<Histogram> =
    LazyLock::new(|| register_histogram_in_milliseconds("fuse_remote_io_deserialize_milliseconds"));
static BLOCK_WRITE_NUMS: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_block_write_nums"));
static BLOCK_WRITE_BYTES: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_block_write_bytes"));
static BLOCK_WRITE_MILLISECONDS: LazyLock<Histogram> =
    LazyLock::new(|| register_histogram_in_milliseconds("fuse_block_write_millioseconds"));
static BLOCK_INDEX_WRITE_NUMS: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_block_index_write_nums"));
static BLOCK_INDEX_WRITE_BYTES: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_block_index_write_bytes"));
static BLOCK_INDEX_WRITE_MILLISECONDS: LazyLock<Histogram> =
    LazyLock::new(|| register_histogram_in_milliseconds("fuse_block_index_write_milliseconds"));
static BLOCK_INDEX_READ_BYTES: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_block_index_read_bytes"));
static COMPACT_BLOCK_READ_NUMS: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_compact_block_read_nums"));
static COMPACT_BLOCK_READ_BYTES: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_compact_block_read_bytes"));
static COMPACT_BLOCK_READ_MILLISECONDS: LazyLock<Histogram> =
    LazyLock::new(|| register_histogram_in_milliseconds("fuse_compact_block_read_milliseconds"));
static COMPACT_BLOCK_BUILD_TASK_MILLISECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram_in_milliseconds("fuse_compact_block_build_task_milliseconds")
});
static COMPACT_BLOCK_BUILD_LAZY_PART_MILLISECONDS: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram_in_milliseconds("fuse_compact_block_build_lazy_part_milliseconds")
});
static RECLUSTER_BUILD_TASK_MILLISECONDS: LazyLock<Histogram> =
    LazyLock::new(|| register_histogram_in_milliseconds("fuse_recluster_build_task_milliseconds"));
static RECLUSTER_SEGMENT_NUMS_SCHEDULED: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_recluster_segment_nums_scheduled"));
static RECLUSTER_BLOCK_NUMS_TO_READ: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_recluster_block_nums_to_read"));
static RECLUSTER_BLOCK_BYTES_TO_READ: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_recluster_block_bytes_to_read"));
static RECLUSTER_ROW_NUMS_TO_READ: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_recluster_row_nums_to_read"));
static RECLUSTER_WRITE_BLOCK_NUMS: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_recluster_write_block_nums"));
static SEGMENTS_RANGE_PRUNING_BEFORE: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_segments_range_pruning_before"));
static SEGMENTS_RANGE_PRUNING_AFTER: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_segments_range_pruning_after"));
static BYTES_SEGMENT_RANGE_PRUNING_BEFORE: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_bytes_segment_range_pruning_before"));
static BYTES_SEGMENT_RANGE_PRUNING_AFTER: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_bytes_segment_range_pruning_after"));
static BLOCKS_RANGE_PRUNING_BEFORE: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_blocks_range_pruning_before"));
static BLOCKS_RANGE_PRUNING_AFTER: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_blocks_range_pruning_after"));
static BYTES_BLOCK_RANGE_PRUNING_BEFORE: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_bytes_block_range_pruning_before"));
static BYTES_BLOCK_RANGE_PRUNING_AFTER: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_bytes_block_range_pruning_after"));
static BLOCKS_BLOOM_PRUNING_BEFORE: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_blocks_bloom_pruning_before"));
static BLOCKS_BLOOM_PRUNING_AFTER: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_blocks_bloom_pruning_after"));
static BYTES_BLOCK_BLOOM_PRUNING_BEFORE: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_bytes_block_bloom_pruning_before"));
static BYTES_BLOCK_BLOOM_PRUNING_AFTER: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_bytes_block_bloom_pruning_after"));
static PRUNING_PREWHERE_NUMS: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_pruning_prewhere_nums"));
static PRUNING_MILLISECONDS: LazyLock<Histogram> =
    LazyLock::new(|| register_histogram_in_milliseconds("fuse_pruning_milliseconds"));
static DELETION_BLOCK_RANGE_PRUNED_NUMS: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_deletion_block_range_pruned_nums"));
static DELETION_SEGMENT_RANGE_PRUNED_WHOLE_SEGMENT_NUMS: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_deletion_segment_range_pruned_whole_segment_nums"));
static DELETION_BLOCK_RANGE_PRUNED_WHOLE_BLOCK_NUMS: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_deletion_block_range_pruned_whole_block_nums"));
static REPLACE_INTO_BLOCK_NUMBER_AFTER_PRUNING: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_replace_into_block_number_after_pruning"));
static REPLACE_INTO_SEGMENT_NUMBER_AFTER_PRUNING: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_replace_into_segment_number_after_pruning"));
static REPLACE_INTO_ROW_NUMBER_AFTER_PRUNING: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_replace_into_row_number_after_pruning"));
static REPLACE_INTO_BLOCK_NUMBER_TOTALLY_LOADED: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_replace_into_block_number_totally_loaded"));
static REPLACE_INTO_ROW_NUMBER_WRITE: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_replace_into_row_number_write"));
static REPLACE_INTO_BLOCK_NUMBER_WRITE: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_replace_into_block_number_write"));
static REPLACE_INTO_ROW_NUMBER_TOTALLY_LOADED: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_replace_into_row_number_totally_loaded"));
static REPLACE_INTO_BLOCK_NUMBER_WHOLE_BLOCK_DELETION: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_replace_into_block_number_whole_block_deletion"));
static REPLACE_INTO_BLOCK_NUMBER_ZERO_ROW_DELETED: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_replace_into_block_number_zero_row_deleted"));
static REPLACE_INTO_ROW_NUMBER_SOURCE_BLOCK: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_replace_into_row_number_source_block"));
static REPLACE_INTO_ROW_NUMBER_AFTER_TABLE_LEVEL_PRUNING: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_replace_into_row_number_after_table_level_pruning"));
static REPLACE_INTO_PARTITION_NUMBER: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_replace_into_partition_number"));
static REPLACE_INTO_TIME_PROCESS_INPUT_BLOCK_MS: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram_in_milliseconds("fuse_replace_into_time_process_input_block_ms")
});
static REPLACE_INTO_NUMBER_ACCUMULATED_MERGE_ACTION: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_replace_into_number_accumulated_merge_action"));
static REPLACE_INTO_NUMBER_APPLY_DELETION: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_replace_into_number_apply_deletion"));
static REPLACE_INTO_TIME_ACCUMULATED_MERGE_ACTION_MS: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram_in_milliseconds("fuse_replace_into_time_accumulated_merge_action_ms")
});
static REPLACE_INTO_TIME_APPLY_DELETION_MS: LazyLock<Histogram> = LazyLock::new(|| {
    register_histogram_in_milliseconds("fuse_replace_into_time_apply_deletion_ms")
});
static REPLACE_INTO_BLOCK_NUMBER_BLOOM_PRUNED: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_replace_into_block_number_bloom_pruned"));
static REPLACE_INTO_BLOCK_NUMBER_SOURCE: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_replace_into_block_number_source"));
static REPLACE_INTO_REPLACED_BLOCKS_ROWS: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_replace_into_replaced_blocks_rows"));
static REPLACE_INTO_DELETED_BLOCKS_ROWS: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_replace_into_deleted_blocks_rows"));
static REPLACE_INTO_APPEND_BLOCKS_ROWS: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_replace_into_append_blocks_rows"));

// Aggregate index metrics.
static AGG_INDEX_WRITE_NUMS: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_aggregate_index_write_nums"));
static AGG_INDEX_WRITE_BYTES: LazyLock<Counter> =
    LazyLock::new(|| register_counter("fuse_aggregate_index_write_bytes"));
static AGG_INDEX_WRITE_MILLISECONDS: LazyLock<Histogram> =
    LazyLock::new(|| register_histogram_in_milliseconds("fuse_aggregate_index_write_milliseconds"));

/// Common metrics.
pub fn metrics_inc_omit_filter_rowgroups(c: u64) {
    OMIT_FILTER_ROWGROUPS.inc_by(c);
}

pub fn metrics_inc_omit_filter_rows(c: u64) {
    OMIT_FILTER_ROWS.inc_by(c);
}

/// COPY
pub fn metrics_inc_copy_purge_files_counter(c: u32) {
    COPY_PURGE_FILE_COUNTER.inc_by(c as u64);
}

pub fn metrics_inc_copy_purge_files_cost_milliseconds(c: u32) {
    COPY_PURGE_FILE_COST_MILLISECONDS.observe(c as f64);
}

pub fn metrics_inc_copy_read_part_counter() {
    COPY_READ_PART_COUNTER.inc();
}

pub fn metrics_inc_copy_read_size_bytes(c: u64) {
    COPY_READ_SIZE_BYTES.inc_by(c);
}

pub fn metrics_inc_copy_read_part_cost_milliseconds(c: u64) {
    COPY_READ_PART_COST_MILLISECONDS.observe(c as f64);
}

pub fn metrics_inc_copy_filter_out_copied_files_request_milliseconds(c: u64) {
    COPY_FILTER_OUT_COPIED_FILES_REQUEST_MILLISECONDS.observe(c as f64);
}

pub fn metrics_inc_copy_filter_out_copied_files_entire_milliseconds(c: u64) {
    COPY_FILTER_OUT_COPIED_FILES_ENTIRE_MILLISECONDS.observe(c as f64);
}

pub fn metrics_inc_copy_collect_files_get_all_source_files_milliseconds(c: u64) {
    COPY_COLLECT_FILES_GET_ALL_SOURCE_FILES_MILLISECONDS.observe(c as f64);
}

/// Merge into metrics.
pub fn metrics_inc_merge_into_replace_blocks_counter(c: u32) {
    MERGE_INTO_REPLACE_BLOCKS_COUNTER.inc_by(c as u64);
}

pub fn metrics_inc_merge_into_replace_blocks_rows_counter(c: u32) {
    MERGE_INTO_REPLACE_BLOCKS_ROWS_COUNTER.inc_by(c as u64);
}

pub fn metrics_inc_merge_into_deleted_blocks_counter(c: u32) {
    MERGE_INTO_DELETED_BLOCKS_COUNTER.inc_by(c as u64);
}

pub fn metrics_inc_merge_into_deleted_blocks_rows_counter(c: u32) {
    MERGE_INTO_DELETED_BLOCKS_ROWS_COUNTER.inc_by(c as u64);
}

// used to record append new blocks in matched split and not match insert.
pub fn metrics_inc_merge_into_append_blocks_counter(c: u32) {
    MERGE_INTO_APPEND_BLOCKS_COUNTER.inc_by(c as u64);
}

pub fn merge_into_distributed_deduplicate_row_number(c: u32) {
    MERGE_INTO_DISTRIBUTED_DEDUPLICATE_ROWNUMBER.inc_by(c as u64);
}

pub fn merge_into_distributed_empty_row_number(c: u32) {
    MERGE_INTO_DISTRIBUTED_EMPTY_ROWNUMBER.inc_by(c as u64);
}

pub fn merge_into_distributed_apply_row_number(c: u32) {
    MERGE_INTO_DISTRIBUTED_APPLY_ROWNUMBER.inc_by(c as u64);
}

pub fn merge_into_distributed_hashtable_fetch_row_number(c: u32) {
    MERGE_INTO_DISTRIBUTED_HASHTABLE_FETCH_ROWNUMBER.inc_by(c as u64);
}

pub fn merge_into_distributed_hashtable_empty_block(c: u32) {
    MERGE_INTO_DISTRIBUTED_HASHTABLE_EMPTY_BLOCK.inc_by(c as u64);
}

pub fn merge_into_distributed_generate_row_numbers(c: u32) {
    MERGE_INTO_DISTRIBUTED_GENERATE_ROW_NUMBERS.inc_by(c as u64);
}

pub fn merge_into_distributed_init_unique_number(c: u32) {
    MERGE_INTO_DISTRIBUTED_INIT_UNIQUE_NUMBER.inc_by(c as u64);
}

pub fn merge_into_distributed_new_set_len(c: u32) {
    MERGE_INTO_DISTRIBUTED_NEW_SET_LEN.inc_by(c as u64);
}

pub fn merge_into_distributed_hashtable_push_empty_null_block(c: u32) {
    MERGE_INTO_DISTRIBUTED_HASHTABLE_PUSH_EMPTY_NULL_BLOCK.inc_by(c as u64);
}

pub fn merge_into_distributed_hashtable_push_null_block(c: u32) {
    MERGE_INTO_DISTRIBUTED_HASHTABLE_PUSH_NULL_BLOCK.inc_by(c as u64);
}

pub fn merge_into_distributed_hashtable_push_null_block_rows(c: u32) {
    MERGE_INTO_DISTRIBUTED_HASHTABLE_PUSH_NULL_BLOCK_ROWS.inc_by(c as u64);
}

pub fn metrics_inc_merge_into_append_blocks_rows_counter(c: u32) {
    MERGE_INTO_APPEND_BLOCKS_ROWS_COUNTER.inc_by(c as u64);
}

// matched_rows and not unmatched_rows is used in the join phase of merge_source.
pub fn metrics_inc_merge_into_matched_rows(c: u32) {
    MERGE_INTO_MATCHED_ROWS.inc_by(c as u64);
}

pub fn metrics_inc_merge_into_unmatched_rows(c: u32) {
    MERGE_INTO_UNMATCHED_ROWS.inc_by(c as u64);
}

pub fn metrics_inc_merge_into_accumulate_milliseconds(c: u64) {
    MERGE_INTO_ACCUMULATE_MILLISECONDS.observe(c as f64);
}

pub fn metrics_inc_merge_into_apply_milliseconds(c: u64) {
    MERGE_INTO_APPLY_MILLISECONDS.observe(c as f64);
}

// join result data split time
pub fn metrics_inc_merge_into_split_milliseconds(c: u64) {
    MERGE_INTO_SPLIT_MILLISECONDS.observe(c as f64);
}

// after merge_source_split, record the time of not macthed clauses (processor_merge_into_not_matched)
pub fn merge_into_not_matched_operation_milliseconds(c: u64) {
    MERGE_INTO_NOT_MATCHED_OPERATION_MILLISECONDS.observe(c as f64);
}

// after merge_source_split, record the time of macthed clauses (processor_merge_into_matched_and_split)
pub fn merge_into_matched_operation_milliseconds(c: u64) {
    MERGE_INTO_MATCHED_OPERATION_MILLISECONDS.observe(c as f64);
}

// Fuse engine metrics.
pub fn metrics_inc_commit_mutation_unresolvable_conflict() {
    COMMIT_MUTATION_UNRESOLVABLE_CONFLICT.inc();
}

pub fn metrics_inc_commit_mutation_latest_snapshot_append_only() {
    COMMIT_MUTATION_LATEST_SNAPSHOT_APPEND_ONLY.inc();
}

pub fn metrics_inc_commit_mutation_modified_segment_exists_in_latest() {
    COMMIT_MUTATION_MODIFIED_SEGMENT_EXISTS_IN_LATEST.inc();
}

pub fn metrics_inc_commit_mutation_retry() {
    COMMIT_MUTATION_RETRY.inc();
}

pub fn metrics_inc_commit_mutation_success() {
    COMMIT_MUTATION_SUCCESS.inc();
}

pub fn metrics_inc_commit_copied_files(n: u64) {
    COMMIT_COPIED_FILES.inc_by(n);
}

pub fn metrics_inc_commit_milliseconds(c: u128) {
    COMMIT_MILLISECONDS.inc_by(c as u64);
}

pub fn metrics_inc_commit_aborts() {
    COMMIT_ABORTS.inc();
}

pub fn metrics_inc_remote_io_seeks(c: u64) {
    REMOTE_IO_SEEKS.inc_by(c);
}

pub fn metrics_inc_remote_io_seeks_after_merged(c: u64) {
    REMOTE_IO_SEEKS_AFTER_MERGED.inc_by(c);
}

pub fn metrics_inc_remote_io_read_bytes(c: u64) {
    REMOTE_IO_READ_BYTES.inc_by(c);
}

pub fn metrics_inc_remote_io_read_bytes_after_merged(c: u64) {
    REMOTE_IO_READ_BYTES_AFTER_MERGED.inc_by(c);
}

pub fn metrics_inc_remote_io_read_parts(c: u64) {
    REMOTE_IO_READ_PARTS.inc_by(c);
}

pub fn metrics_inc_remote_io_read_milliseconds(c: u64) {
    REMOTE_IO_READ_MILLISECONDS.observe(c as f64);
}

pub fn metrics_inc_remote_io_deserialize_milliseconds(c: u64) {
    REMOTE_IO_DESERIALIZE_MILLISECONDS.observe(c as f64);
}

/// Block metrics.
pub fn metrics_inc_block_write_nums(c: u64) {
    BLOCK_WRITE_NUMS.inc_by(c);
}

pub fn metrics_inc_block_write_bytes(c: u64) {
    BLOCK_WRITE_BYTES.inc_by(c);
}

pub fn metrics_inc_block_write_milliseconds(c: u64) {
    BLOCK_WRITE_MILLISECONDS.observe(c as f64);
}

pub fn metrics_inc_block_index_write_nums(c: u64) {
    BLOCK_INDEX_WRITE_NUMS.inc_by(c);
}

pub fn metrics_inc_block_index_write_bytes(c: u64) {
    BLOCK_INDEX_WRITE_BYTES.inc_by(c);
}

pub fn metrics_inc_block_index_write_milliseconds(c: u64) {
    BLOCK_INDEX_WRITE_MILLISECONDS.observe(c as f64);
}

pub fn metrics_inc_block_index_read_bytes(c: u64) {
    BLOCK_INDEX_READ_BYTES.inc_by(c);
}

/// Compact metrics.
pub fn metrics_inc_compact_block_read_nums(c: u64) {
    COMPACT_BLOCK_READ_NUMS.inc_by(c);
}

pub fn metrics_inc_compact_block_read_bytes(c: u64) {
    COMPACT_BLOCK_READ_BYTES.inc_by(c);
}

pub fn metrics_inc_compact_block_read_milliseconds(c: u64) {
    COMPACT_BLOCK_READ_MILLISECONDS.observe(c as f64);
}

pub fn metrics_inc_compact_block_build_task_milliseconds(c: u64) {
    COMPACT_BLOCK_BUILD_TASK_MILLISECONDS.observe(c as f64);
}

pub fn metrics_inc_compact_block_build_lazy_part_milliseconds(c: u64) {
    COMPACT_BLOCK_BUILD_LAZY_PART_MILLISECONDS.observe(c as f64);
}

/// Pruning metrics.
pub fn metrics_inc_segments_range_pruning_before(c: u64) {
    SEGMENTS_RANGE_PRUNING_BEFORE.inc_by(c);
}

pub fn metrics_inc_segments_range_pruning_after(c: u64) {
    SEGMENTS_RANGE_PRUNING_AFTER.inc_by(c);
}

pub fn metrics_inc_bytes_segment_range_pruning_before(c: u64) {
    BYTES_SEGMENT_RANGE_PRUNING_BEFORE.inc_by(c);
}

pub fn metrics_inc_bytes_segment_range_pruning_after(c: u64) {
    BYTES_SEGMENT_RANGE_PRUNING_AFTER.inc_by(c);
}

pub fn metrics_inc_blocks_range_pruning_before(c: u64) {
    BLOCKS_RANGE_PRUNING_BEFORE.inc_by(c);
}

pub fn metrics_inc_blocks_range_pruning_after(c: u64) {
    BLOCKS_RANGE_PRUNING_AFTER.inc_by(c);
}

pub fn metrics_inc_bytes_block_range_pruning_before(c: u64) {
    BYTES_BLOCK_BLOOM_PRUNING_BEFORE.inc_by(c);
}

pub fn metrics_inc_bytes_block_range_pruning_after(c: u64) {
    BYTES_BLOCK_BLOOM_PRUNING_AFTER.inc_by(c);
}

pub fn metrics_inc_blocks_bloom_pruning_before(c: u64) {
    BLOCKS_BLOOM_PRUNING_BEFORE.inc_by(c);
}

pub fn metrics_inc_blocks_bloom_pruning_after(c: u64) {
    BLOCKS_BLOOM_PRUNING_AFTER.inc_by(c);
}

pub fn metrics_inc_bytes_block_bloom_pruning_before(c: u64) {
    BYTES_BLOCK_BLOOM_PRUNING_BEFORE.inc_by(c);
}

pub fn metrics_inc_bytes_block_bloom_pruning_after(c: u64) {
    BYTES_BLOCK_BLOOM_PRUNING_AFTER.inc_by(c);
}

pub fn metrics_inc_pruning_prewhere_nums(c: u64) {
    PRUNING_PREWHERE_NUMS.inc_by(c);
}

pub fn metrics_inc_pruning_milliseconds(c: u64) {
    PRUNING_MILLISECONDS.observe(c as f64);
}

pub fn metrics_inc_deletion_block_range_pruned_nums(c: u64) {
    DELETION_BLOCK_RANGE_PRUNED_NUMS.inc_by(c);
}

pub fn metrics_inc_deletion_segment_range_purned_whole_segment_nums(c: u64) {
    DELETION_SEGMENT_RANGE_PRUNED_WHOLE_SEGMENT_NUMS.inc_by(c);
}

pub fn metrics_inc_deletion_block_range_pruned_whole_block_nums(c: u64) {
    DELETION_BLOCK_RANGE_PRUNED_WHOLE_BLOCK_NUMS.inc_by(c);
}

pub fn metrics_inc_replace_block_number_after_pruning(c: u64) {
    REPLACE_INTO_BLOCK_NUMBER_AFTER_PRUNING.inc_by(c);
}

pub fn metrics_inc_replace_segment_number_after_pruning(c: u64) {
    REPLACE_INTO_SEGMENT_NUMBER_AFTER_PRUNING.inc_by(c);
}

pub fn metrics_inc_replace_row_number_after_pruning(c: u64) {
    REPLACE_INTO_ROW_NUMBER_AFTER_PRUNING.inc_by(c);
}

pub fn metrics_inc_replace_block_number_totally_loaded(c: u64) {
    REPLACE_INTO_BLOCK_NUMBER_TOTALLY_LOADED.inc_by(c);
}

pub fn metrics_inc_replace_row_number_write(c: u64) {
    REPLACE_INTO_ROW_NUMBER_WRITE.inc_by(c);
}

pub fn metrics_inc_replace_block_number_write(c: u64) {
    REPLACE_INTO_BLOCK_NUMBER_WRITE.inc_by(c);
}

pub fn metrics_inc_replace_row_number_totally_loaded(c: u64) {
    REPLACE_INTO_ROW_NUMBER_TOTALLY_LOADED.inc_by(c);
}

pub fn metrics_inc_replace_whole_block_deletion(c: u64) {
    REPLACE_INTO_BLOCK_NUMBER_WHOLE_BLOCK_DELETION.inc_by(c);
}

pub fn metrics_inc_replace_block_of_zero_row_deleted(c: u64) {
    REPLACE_INTO_BLOCK_NUMBER_ZERO_ROW_DELETED.inc_by(c);
}

pub fn metrics_inc_replace_original_row_number(c: u64) {
    REPLACE_INTO_ROW_NUMBER_SOURCE_BLOCK.inc_by(c);
}

pub fn metrics_inc_replace_row_number_after_table_level_pruning(c: u64) {
    REPLACE_INTO_ROW_NUMBER_AFTER_TABLE_LEVEL_PRUNING.inc_by(c);
}

pub fn metrics_inc_replace_partition_number(c: u64) {
    REPLACE_INTO_PARTITION_NUMBER.inc_by(c);
}

// time used in processing the input block
pub fn metrics_inc_replace_process_input_block_time_ms(c: u64) {
    REPLACE_INTO_TIME_PROCESS_INPUT_BLOCK_MS.observe(c as f64);
}

// the number of accumulate_merge_action operation invoked
pub fn metrics_inc_replace_number_accumulated_merge_action() {
    REPLACE_INTO_NUMBER_ACCUMULATED_MERGE_ACTION.inc();
}

// the number of apply_deletion operation applied
pub fn metrics_inc_replace_number_apply_deletion() {
    REPLACE_INTO_NUMBER_APPLY_DELETION.inc();
}

// time used in executing the accumulated_merge_action operation
pub fn metrics_inc_replace_accumulated_merge_action_time_ms(c: u64) {
    REPLACE_INTO_TIME_ACCUMULATED_MERGE_ACTION_MS.observe(c as f64)
}

// time used in executing the apply_deletion operation
pub fn metrics_inc_replace_apply_deletion_time_ms(c: u64) {
    REPLACE_INTO_TIME_APPLY_DELETION_MS.observe(c as f64);
}

// number of blocks that pruned by bloom filter
pub fn metrics_inc_replace_block_number_bloom_pruned(c: u64) {
    REPLACE_INTO_BLOCK_NUMBER_BLOOM_PRUNED.inc_by(c);
}
// number of blocks from upstream  source
pub fn metrics_inc_replace_block_number_input(c: u64) {
    REPLACE_INTO_BLOCK_NUMBER_SOURCE.inc_by(c);
}

// rows of blocks that are replaced
pub fn metrics_inc_replace_replaced_blocks_rows(c: u64) {
    REPLACE_INTO_REPLACED_BLOCKS_ROWS.inc_by(c);
}

// rows of blocks that are deleted
pub fn metrics_inc_replace_deleted_blocks_rows(c: u64) {
    REPLACE_INTO_DELETED_BLOCKS_ROWS.inc_by(c);
}

// rows of blocks that are appended
pub fn metrics_inc_replace_append_blocks_rows(c: u64) {
    REPLACE_INTO_APPEND_BLOCKS_ROWS.inc_by(c);
}

/// Recluster metrics.
pub fn metrics_inc_recluster_build_task_milliseconds(c: u64) {
    RECLUSTER_BUILD_TASK_MILLISECONDS.observe(c as f64);
}

pub fn metrics_inc_recluster_segment_nums_scheduled(c: u64) {
    RECLUSTER_SEGMENT_NUMS_SCHEDULED.inc_by(c);
}

pub fn metrics_inc_recluster_block_nums_to_read(c: u64) {
    RECLUSTER_BLOCK_NUMS_TO_READ.inc_by(c);
}

pub fn metrics_inc_recluster_block_bytes_to_read(c: u64) {
    RECLUSTER_BLOCK_BYTES_TO_READ.inc_by(c);
}

pub fn metrics_inc_recluster_row_nums_to_read(c: u64) {
    RECLUSTER_ROW_NUMS_TO_READ.inc_by(c);
}

pub fn metrics_inc_recluster_write_block_nums() {
    RECLUSTER_WRITE_BLOCK_NUMS.inc();
}

/// Aggregate index metrics.
pub fn metrics_inc_agg_index_write_nums(c: u64) {
    AGG_INDEX_WRITE_NUMS.inc_by(c);
}

pub fn metrics_inc_agg_index_write_bytes(c: u64) {
    AGG_INDEX_WRITE_BYTES.inc_by(c);
}

pub fn metrics_inc_agg_index_write_milliseconds(c: u64) {
    AGG_INDEX_WRITE_MILLISECONDS.observe(c as f64);
}
