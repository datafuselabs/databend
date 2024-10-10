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
use std::ops::Range;
use std::time::Instant;

use databend_common_base::rangemap::RangeMerger;
use databend_common_base::runtime::UnlimitedFuture;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_metrics::storage::*;
use futures::future::try_join_all;
use opendal::Operator;

use crate::merge_io_result::OwnerMemory;
use crate::MergeIOReadResult;
use crate::ReadSettings;

pub struct MergeIOReader {}

impl MergeIOReader {
    /// If the distance between two IO request ranges to be read is less than storage_io_min_bytes_for_seek(Default is 48Bytes),
    /// will read the range that contains both ranges, thus avoiding extra seek.
    ///
    /// It will *NOT* merge two requests:
    /// if the last io request size is larger than storage_io_page_bytes_for_read(Default is 512KB).
    #[async_backtrace::framed]
    pub async fn merge_io_read(
        read_settings: &ReadSettings,
        op: Operator,
        location: &str,
        raw_ranges: &[(ColumnId, Range<u64>)],
    ) -> Result<MergeIOReadResult> {
        if raw_ranges.is_empty() {
            // shortcut
            let read_res = MergeIOReadResult::create(
                OwnerMemory::create(vec![]),
                HashMap::new(),
                location.to_string(),
            );
            return Ok(read_res);
        }

        // Build merged read ranges.
        let ranges = raw_ranges
            .iter()
            .map(|(_, r)| r.clone())
            .collect::<Vec<_>>();
        let range_merger = RangeMerger::from_iter(
            ranges,
            read_settings.storage_io_min_bytes_for_seek,
            read_settings.storage_io_max_page_bytes_for_read,
        );
        let merged_ranges = range_merger.ranges();

        // Read merged range data.
        let mut read_handlers = Vec::with_capacity(merged_ranges.len());
        for (idx, range) in merged_ranges.iter().enumerate() {
            // Perf.
            {
                metrics_inc_remote_io_seeks_after_merged(1);
                metrics_inc_remote_io_read_bytes_after_merged(range.end - range.start);
            }

            read_handlers.push(UnlimitedFuture::create(Self::read_range(
                op.clone(),
                location,
                idx,
                range.start,
                range.end,
            )));
        }

        let start = Instant::now();
        let owner_memory = OwnerMemory::create(try_join_all(read_handlers).await?);

        // Perf.
        {
            metrics_inc_remote_io_read_milliseconds(start.elapsed().as_millis() as u64);
        }

        let mut columns_chunk_offsets = HashMap::with_capacity(raw_ranges.len());
        for (raw_idx, raw_range) in raw_ranges {
            let column_range = raw_range.start..raw_range.end;

            // Find the range index and Range from merged ranges.
            let (merged_range_idx, merged_range) = range_merger.get(column_range.clone()).ok_or_else(|| ErrorCode::Internal(format!(
                "It's a terrible bug, not found raw range:[{:?}], path:{} from merged ranges\n: {:?}",
                column_range, location, merged_ranges
            )))?;

            // Fetch the raw data for the raw range.
            let start = (column_range.start - merged_range.start) as usize;
            let end = (column_range.end - merged_range.start) as usize;
            let column_id = *raw_idx as ColumnId;
            let range = start..end;
            columns_chunk_offsets.insert(column_id, (merged_range_idx, range));
        }

        let read_res =
            MergeIOReadResult::create(owner_memory, columns_chunk_offsets, location.to_string());

        Ok(read_res)
    }

    #[inline]
    #[async_backtrace::framed]
    async fn read_range(
        op: Operator,
        path: &str,
        index: usize,
        start: u64,
        end: u64,
    ) -> Result<(usize, Vec<u8>)> {
        let chunk = op.read_with(path).range(start..end).await?;
        Ok((index, chunk.to_vec()))
    }

    pub fn sync_merge_io_read(
        read_settings: &ReadSettings,
        op: Operator,
        location: &str,
        raw_ranges: &[(ColumnId, Range<u64>)],
    ) -> Result<MergeIOReadResult> {
        let path = location.to_string();

        // Build merged read ranges.
        let ranges = raw_ranges
            .iter()
            .map(|(_, r)| r.clone())
            .collect::<Vec<_>>();
        let range_merger = RangeMerger::from_iter(
            ranges,
            read_settings.storage_io_min_bytes_for_seek,
            read_settings.storage_io_max_page_bytes_for_read,
        );
        let merged_ranges = range_merger.ranges();

        // Read merged range data.
        let mut io_res = Vec::with_capacity(merged_ranges.len());
        for (idx, range) in merged_ranges.iter().enumerate() {
            io_res.push(Self::sync_read_range(
                op.clone(),
                location,
                idx,
                range.start,
                range.end,
            )?);
        }

        let owner_memory = OwnerMemory::create(io_res);

        let mut columns_chunk_offsets = HashMap::with_capacity(raw_ranges.len());
        for (raw_idx, raw_range) in raw_ranges {
            let column_id = *raw_idx as ColumnId;
            let column_range = raw_range.start..raw_range.end;

            // Find the range index and Range from merged ranges.
            let (merged_range_idx, merged_range) = range_merger.get(column_range.clone()).ok_or_else(|| ErrorCode::Internal(format!(
                "It's a terrible bug, not found raw range:[{:?}], path:{} from merged ranges\n: {:?}",
                column_range, path, merged_ranges
            )))?;

            // Fetch the raw data for the raw range.
            let start = (column_range.start - merged_range.start) as usize;
            let end = (column_range.end - merged_range.start) as usize;
            let range = start..end;
            columns_chunk_offsets.insert(column_id, (merged_range_idx, range));
        }

        let read_res =
            MergeIOReadResult::create(owner_memory, columns_chunk_offsets, location.to_string());

        Ok(read_res)
    }

    #[inline]
    pub fn sync_read_range(
        op: Operator,
        path: &str,
        index: usize,
        start: u64,
        end: u64,
    ) -> Result<(usize, Vec<u8>)> {
        let chunk = op.blocking().read_with(path).range(start..end).call()?;
        Ok((index, chunk.to_vec()))
    }
}
