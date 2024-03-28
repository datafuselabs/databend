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

use databend_common_base::runtime::metrics::register_counter_family;
use databend_common_base::runtime::metrics::register_histogram_family_in_milliseconds;
use databend_common_base::runtime::metrics::FamilyCounter;
use databend_common_base::runtime::metrics::FamilyHistogram;

use crate::VecLabels;

const METRIC_QUERY_START: &str = "query_start";
const METRIC_QUERY_ERROR: &str = "query_error";
const METRIC_QUERY_SUCCESS: &str = "query_success";
const METRIC_QUERY_FAILED: &str = "query_failed";

const METRIC_QUERY_DURATION_MS: &str = "query_duration_ms";
const METRIC_QUERY_WRITE_ROWS: &str = "query_write_rows";
const METRIC_QUERY_WRITE_BYTES: &str = "query_write_bytes";
const METRIC_QUERY_WRITE_IO_BYTES: &str = "query_write_io_bytes";
const METRIC_QUERY_WRITE_IO_BYTES_COST_MS: &str = "query_write_io_bytes_cost_ms";
const METRIC_QUERY_SCAN_ROWS: &str = "query_scan_rows";
const METRIC_QUERY_SCAN_BYTES: &str = "query_scan_bytes";
const METRIC_QUERY_SCAN_IO_BYTES: &str = "query_scan_io_bytes";
const METRIC_QUERY_SCAN_IO_BYTES_COST_MS: &str = "query_scan_io_bytes_cost_ms";
const METRIC_QUERY_SCAN_PARTITIONS: &str = "query_scan_partitions";
const METRIC_QUERY_TOTAL_PARTITIONS: &str = "query_total_partitions";
const METRIC_QUERY_RESULT_ROWS: &str = "query_result_rows";
const METRIC_QUERY_RESULT_BYTES: &str = "query_result_bytes";

pub static QUERY_START: LazyLock<FamilyCounter<VecLabels>> =
    LazyLock::new(|| register_counter_family(METRIC_QUERY_START));
pub static QUERY_ERROR: LazyLock<FamilyCounter<VecLabels>> =
    LazyLock::new(|| register_counter_family(METRIC_QUERY_ERROR));
pub static QUERY_SUCCESS: LazyLock<FamilyCounter<VecLabels>> =
    LazyLock::new(|| register_counter_family(METRIC_QUERY_SUCCESS));
pub static QUERY_FAILED: LazyLock<FamilyCounter<VecLabels>> =
    LazyLock::new(|| register_counter_family(METRIC_QUERY_FAILED));
pub static QUERY_DURATION_MS: LazyLock<FamilyHistogram<VecLabels>> =
    LazyLock::new(|| register_histogram_family_in_milliseconds(METRIC_QUERY_DURATION_MS));
pub static QUERY_WRITE_ROWS: LazyLock<FamilyCounter<VecLabels>> =
    LazyLock::new(|| register_counter_family(METRIC_QUERY_WRITE_ROWS));
pub static QUERY_WRITE_BYTES: LazyLock<FamilyCounter<VecLabels>> =
    LazyLock::new(|| register_counter_family(METRIC_QUERY_WRITE_BYTES));
pub static QUERY_WRITE_IO_BYTES: LazyLock<FamilyCounter<VecLabels>> =
    LazyLock::new(|| register_counter_family(METRIC_QUERY_WRITE_IO_BYTES));
pub static QUERY_WRITE_IO_BYTES_COST_MS: LazyLock<FamilyHistogram<VecLabels>> =
    LazyLock::new(|| {
        register_histogram_family_in_milliseconds(METRIC_QUERY_WRITE_IO_BYTES_COST_MS)
    });
pub static QUERY_SCAN_ROWS: LazyLock<FamilyCounter<VecLabels>> =
    LazyLock::new(|| register_counter_family(METRIC_QUERY_SCAN_ROWS));
pub static QUERY_SCAN_BYTES: LazyLock<FamilyCounter<VecLabels>> =
    LazyLock::new(|| register_counter_family(METRIC_QUERY_SCAN_BYTES));
pub static QUERY_SCAN_IO_BYTES: LazyLock<FamilyCounter<VecLabels>> =
    LazyLock::new(|| register_counter_family(METRIC_QUERY_SCAN_IO_BYTES));
pub static QUERY_SCAN_IO_BYTES_COST_MS: LazyLock<FamilyHistogram<VecLabels>> =
    LazyLock::new(|| register_histogram_family_in_milliseconds(METRIC_QUERY_SCAN_IO_BYTES_COST_MS));
pub static QUERY_SCAN_PARTITIONS: LazyLock<FamilyCounter<VecLabels>> =
    LazyLock::new(|| register_counter_family(METRIC_QUERY_SCAN_PARTITIONS));
pub static QUERY_TOTAL_PARTITIONS: LazyLock<FamilyCounter<VecLabels>> =
    LazyLock::new(|| register_counter_family(METRIC_QUERY_TOTAL_PARTITIONS));
pub static QUERY_RESULT_ROWS: LazyLock<FamilyCounter<VecLabels>> =
    LazyLock::new(|| register_counter_family(METRIC_QUERY_RESULT_ROWS));
pub static QUERY_RESULT_BYTES: LazyLock<FamilyCounter<VecLabels>> =
    LazyLock::new(|| register_counter_family(METRIC_QUERY_RESULT_BYTES));
