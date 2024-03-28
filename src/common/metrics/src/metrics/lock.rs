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
use databend_common_base::runtime::metrics::register_counter_family;
use databend_common_base::runtime::metrics::Counter;
use databend_common_base::runtime::metrics::FamilyCounter;

use crate::VecLabels;

const METRIC_CREATED_LOCK_NUMS: &str = "created_lock_nums";
const METRIC_ACQUIRED_LOCK_NUMS: &str = "acquired_lock_nums";
const METRIC_START_LOCK_HOLDER_NUMS: &str = "start_lock_holder_nums";
const METRIC_SHUTDOWN_LOCK_HOLDER_NUMS: &str = "shutdown_lock_holder_nums";

static CREATED_LOCK_NUMS: LazyLock<FamilyCounter<VecLabels>> =
    LazyLock::new(|| register_counter_family(METRIC_CREATED_LOCK_NUMS));
static ACQUIRED_LOCK_NUMS: LazyLock<FamilyCounter<VecLabels>> =
    LazyLock::new(|| register_counter_family(METRIC_ACQUIRED_LOCK_NUMS));
static START_LOCK_HOLDER_NUMS: LazyLock<Counter> =
    LazyLock::new(|| register_counter(METRIC_START_LOCK_HOLDER_NUMS));
static SHUTDOWN_LOCK_HOLDER_NUMS: LazyLock<Counter> =
    LazyLock::new(|| register_counter(METRIC_SHUTDOWN_LOCK_HOLDER_NUMS));

const LABEL_TYPE: &str = "type";
const LABEL_TABLE_ID: &str = "table_id";

pub fn record_created_lock_nums(lock_type: String, table_id: u64, num: u64) {
    let labels = &vec![
        (LABEL_TYPE, lock_type),
        (LABEL_TABLE_ID, table_id.to_string()),
    ];
    CREATED_LOCK_NUMS.get_or_create(labels).inc_by(num);
}

pub fn record_acquired_lock_nums(lock_type: String, table_id: u64, num: u64) {
    let labels = &vec![
        (LABEL_TYPE, lock_type),
        (LABEL_TABLE_ID, table_id.to_string()),
    ];
    ACQUIRED_LOCK_NUMS.get_or_create(labels).inc_by(num);
}

pub fn metrics_inc_start_lock_holder_nums() {
    START_LOCK_HOLDER_NUMS.inc();
}

pub fn metrics_inc_shutdown_lock_holder_nums() {
    SHUTDOWN_LOCK_HOLDER_NUMS.inc();
}
