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

use databend_common_base::runtime::metrics::register_histogram_family_in_milliseconds;
use databend_common_base::runtime::metrics::FamilyHistogram;

static COMPACT_HOOK_EXECUTION_MS: LazyLock<FamilyHistogram<Vec<(&'static str, String)>>> =
    LazyLock::new(|| register_histogram_family_in_milliseconds("compact_hook_execution_ms"));
static COMPACT_HOOK_COMPACTION_MS: LazyLock<FamilyHistogram<Vec<(&'static str, String)>>> =
    LazyLock::new(|| register_histogram_family_in_milliseconds("compact_hook_compaction_ms"));

// the time used in executing the main operation  (replace-into, copy-into, etc)
// metrics names with pattern `compact_hook_{operation_name}_time_execution_ms`
pub fn metrics_inc_compact_hook_main_operation_time_ms(operation_name: &str, c: u64) {
    let labels = &vec![("operation", operation_name.to_string())];
    COMPACT_HOOK_EXECUTION_MS
        .get_or_create(labels)
        .observe(c as f64);
}

// the time used in executing the compaction
// metrics names with pattern `compact_hook_{operation_name}_time_compaction_ms`
pub fn metrics_inc_compact_hook_compact_time_ms(operation_name: &str, c: u64) {
    let labels = &vec![("operation", operation_name.to_string())];
    COMPACT_HOOK_COMPACTION_MS
        .get_or_create(labels)
        .observe(c as f64);
}
