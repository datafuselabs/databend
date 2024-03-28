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

use databend_common_base::runtime::metrics::register_histogram_in_milliseconds;
use databend_common_base::runtime::metrics::Histogram;

pub static MYSQL_PROCESSOR_REQUEST_DURATION: LazyLock<Histogram> =
    LazyLock::new(|| register_histogram_in_milliseconds("mysql_process_request_duration_ms"));
pub static MYSQL_INTERPRETER_USEDTIME: LazyLock<Histogram> =
    LazyLock::new(|| register_histogram_in_milliseconds("mysql_interpreter_usedtime_ms"));

pub fn observe_mysql_process_request_duration(duration: Duration) {
    MYSQL_PROCESSOR_REQUEST_DURATION.observe(duration.as_millis() as f64);
}

pub fn observe_mysql_interpreter_used_time(duration: Duration) {
    MYSQL_INTERPRETER_USEDTIME.observe(duration.as_millis() as f64);
}
