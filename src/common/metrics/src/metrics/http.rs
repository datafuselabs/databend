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
use databend_common_base::runtime::metrics::register_counter_family;
use databend_common_base::runtime::metrics::register_histogram_family_in_seconds;
use databend_common_base::runtime::metrics::Counter;
use databend_common_base::runtime::metrics::FamilyCounter;
use databend_common_base::runtime::metrics::FamilyHistogram;

use crate::VecLabels;

static QUERY_HTTP_REQUESTS_COUNT: LazyLock<FamilyCounter<VecLabels>> =
    LazyLock::new(|| register_counter_family("query_http_requests_count"));
static QUERY_HTTP_RESPONSE_DURATION: LazyLock<FamilyHistogram<VecLabels>> =
    LazyLock::new(|| register_histogram_family_in_seconds("query_http_response_duration_seconds"));
static QUERY_HTTP_SLOW_REQUESTS_COUNT: LazyLock<FamilyCounter<VecLabels>> =
    LazyLock::new(|| register_counter_family("query_http_slow_requests_count"));
static QUERY_HTTP_RESPONSE_ERRORS_COUNT: LazyLock<FamilyCounter<VecLabels>> =
    LazyLock::new(|| register_counter_family("query_http_response_errors_count"));
static QUERY_HTTP_RESPONSE_PANICS_COUNT: LazyLock<Counter> =
    LazyLock::new(|| register_counter("query_http_response_panics_count"));

pub fn metrics_incr_http_request_count(method: String, api: String, status: String) {
    let labels = vec![("method", method), ("api", api), ("status", status)];
    QUERY_HTTP_REQUESTS_COUNT.get_or_create(&labels).inc();
}

pub fn metrics_incr_http_slow_request_count(method: String, api: String, status: String) {
    let labels = vec![("method", method), ("api", api), ("status", status)];
    QUERY_HTTP_SLOW_REQUESTS_COUNT.get_or_create(&labels).inc();
}

pub fn metrics_incr_http_response_errors_count(err: String, code: u16) {
    let labels = vec![("err", err), ("code", code.to_string())];
    QUERY_HTTP_RESPONSE_ERRORS_COUNT
        .get_or_create(&labels)
        .inc();
}

pub fn metrics_observe_http_response_duration(method: String, api: String, duration: Duration) {
    let labels = vec![("method", method), ("api", api)];
    QUERY_HTTP_RESPONSE_DURATION
        .get_or_create(&labels)
        .observe(duration.as_secs_f64());
}

pub fn metrics_incr_http_response_panics_count() {
    QUERY_HTTP_RESPONSE_PANICS_COUNT.inc();
}
