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
use prometheus_client::encoding::EncodeLabelSet;

#[derive(Clone, Debug, EncodeLabelSet, Hash, PartialEq, Eq)]
struct CacheLabels {
    cache_name: String,
}

static CACHE_ACCESS_COUNT: LazyLock<FamilyCounter<CacheLabels>> =
    LazyLock::new(|| register_counter_family("cache_access_count"));
static CACHE_MISS_COUNT: LazyLock<FamilyCounter<CacheLabels>> =
    LazyLock::new(|| register_counter_family("cache_miss_count"));
static CACHE_MISS_LOAD_MILLISECOND: LazyLock<FamilyHistogram<CacheLabels>> =
    LazyLock::new(|| register_histogram_family_in_milliseconds("cache_miss_load_millisecond"));
static CACHE_HIT_COUNT: LazyLock<FamilyCounter<CacheLabels>> =
    LazyLock::new(|| register_counter_family("cache_hit_count"));
static CACHE_POPULATION_PENDING_COUNT: LazyLock<FamilyCounter<CacheLabels>> =
    LazyLock::new(|| register_counter_family("cache_population_pending_count"));
static CACHE_POPULATION_OVERFLOW_COUNT: LazyLock<FamilyCounter<CacheLabels>> =
    LazyLock::new(|| register_counter_family("cache_population_overflow_count"));

pub fn metrics_inc_cache_access_count(c: u64, cache_name: &str) {
    CACHE_ACCESS_COUNT
        .get_or_create(&CacheLabels {
            cache_name: cache_name.to_string(),
        })
        .inc_by(c);
}

pub fn metrics_inc_cache_miss_count(c: u64, cache_name: &str) {
    // increment_gauge!(("fuse_memory_miss_count"), c as f64);
    CACHE_MISS_COUNT
        .get_or_create(&CacheLabels {
            cache_name: cache_name.to_string(),
        })
        .inc_by(c);
}

// When cache miss, load time cost.
pub fn metrics_inc_cache_miss_load_millisecond(c: u64, cache_name: &str) {
    CACHE_MISS_LOAD_MILLISECOND
        .get_or_create(&CacheLabels {
            cache_name: cache_name.to_string(),
        })
        .observe(c as f64);
}

pub fn metrics_inc_cache_hit_count(c: u64, cache_name: &str) {
    CACHE_HIT_COUNT
        .get_or_create(&CacheLabels {
            cache_name: cache_name.to_string(),
        })
        .inc_by(c);
}

pub fn metrics_inc_cache_population_pending_count(c: i64, cache_name: &str) {
    CACHE_POPULATION_PENDING_COUNT
        .get_or_create(&CacheLabels {
            cache_name: cache_name.to_string(),
        })
        .inc_by(c as u64);
}

pub fn metrics_inc_cache_population_overflow_count(c: i64, cache_name: &str) {
    CACHE_POPULATION_OVERFLOW_COUNT
        .get_or_create(&CacheLabels {
            cache_name: cache_name.to_string(),
        })
        .inc_by(c as u64);
}
