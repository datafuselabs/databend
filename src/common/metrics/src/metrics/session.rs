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
use std::time::Duration;

use databend_common_base::runtime::metrics::register_counter;
use databend_common_base::runtime::metrics::register_gauge;
use databend_common_base::runtime::metrics::register_histogram_in_milliseconds;
use databend_common_base::runtime::metrics::Counter;
use databend_common_base::runtime::metrics::Gauge;
use databend_common_base::runtime::metrics::Histogram;

pub static SESSION_CONNECT_NUMBERS: LazyLock<Counter> =
    LazyLock::new(|| register_counter("session_connect_numbers"));
pub static SESSION_CLOSE_NUMBERS: LazyLock<Counter> =
    LazyLock::new(|| register_counter("session_close_numbers"));
pub static SESSION_ACTIVE_CONNECTIONS: LazyLock<Gauge> =
    LazyLock::new(|| register_gauge("session_connections"));
pub static SESSION_QUQUED_QUERIES: LazyLock<Gauge> =
    LazyLock::new(|| register_gauge("session_queued_queries"));
pub static SESSION_QUEUE_ABORT_COUNT: LazyLock<Counter> =
    LazyLock::new(|| register_counter("session_queue_abort_count"));
pub static SESSION_QUEUE_ACQUIRE_ERROR_COUNT: LazyLock<Counter> =
    LazyLock::new(|| register_counter("session_queue_acquire_error_count"));
pub static SESSION_QUEUE_ACQUIRE_TIMEOUT_COUNT: LazyLock<Counter> =
    LazyLock::new(|| register_counter("session_queue_acquire_timeout_count"));
pub static SESSION_QUEUE_ACQUIRE_DURATION_MS: LazyLock<Histogram> =
    LazyLock::new(|| register_histogram_in_milliseconds("session_queue_acquire_duration_ms"));

pub fn incr_session_connect_numbers() {
    SESSION_CONNECT_NUMBERS.inc();
}

pub fn incr_session_close_numbers() {
    SESSION_CLOSE_NUMBERS.inc();
}

pub fn set_session_active_connections(num: usize) {
    SESSION_ACTIVE_CONNECTIONS.set(num as i64);
}

pub fn set_session_queued_queries(num: usize) {
    SESSION_QUQUED_QUERIES.set(num as i64);
}

pub fn incr_session_queue_abort_count() {
    SESSION_QUEUE_ABORT_COUNT.inc();
}

pub fn incr_session_queue_acquire_error_count() {
    SESSION_QUEUE_ACQUIRE_ERROR_COUNT.inc();
}

pub fn incr_session_queue_acquire_timeout_count() {
    SESSION_QUEUE_ACQUIRE_TIMEOUT_COUNT.inc();
}

pub fn record_session_queue_acquire_duration_ms(duration: Duration) {
    SESSION_QUEUE_ACQUIRE_DURATION_MS.observe(duration.as_millis() as f64);
}
