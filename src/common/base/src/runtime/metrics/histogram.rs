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

use std::iter::once;
use std::sync::Arc;

use parking_lot::MappedRwLockReadGuard;
use parking_lot::RwLock;
use parking_lot::RwLockReadGuard;
use prometheus_client::encoding::EncodeMetric;
use prometheus_client::encoding::MetricEncoder;
use prometheus_client::metrics::MetricType;
use prometheus_client::metrics::TypedMetric;

use crate::runtime::metrics::registry::DatabendMetric;
use crate::runtime::metrics::sample::HistogramCount;
use crate::runtime::metrics::sample::MetricSample;
use crate::runtime::metrics::sample::MetricValue;
use crate::runtime::metrics::ScopedRegistry;

pub static BUCKET_SECONDS: [f64; 15] = [
    0.02, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0, 30.0, 60.0, 300.0, 600.0, 1800.0,
];

pub static BUCKET_MILLISECONDS: [f64; 15] = [
    10.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0, 20000.0, 30000.0, 60000.0,
    300000.0, 600000.0, 1800000.0,
];

/// Histogram is a port of prometheus-client's Histogram. The only difference is that
/// we can reset the histogram.
#[derive(Debug)]
pub struct Histogram {
    index: usize,
    inner: Arc<RwLock<Inner>>,
}

impl Clone for Histogram {
    fn clone(&self) -> Self {
        Histogram {
            index: self.index,
            inner: self.inner.clone(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Inner {
    sum: f64,
    count: u64,
    buckets: Vec<(f64, u64)>,
}

impl Histogram {
    /// Create a new [`Histogram`].
    pub fn new(index: usize, buckets: impl Iterator<Item = f64>) -> Self {
        Self {
            index,
            inner: Arc::new(RwLock::new(Inner {
                sum: Default::default(),
                count: Default::default(),
                buckets: buckets
                    .into_iter()
                    .chain(once(f64::MAX))
                    .map(|upper_bound| (upper_bound, 0))
                    .collect(),
            })),
        }
    }

    /// Observe the given value.
    pub fn observe(&self, v: f64) {
        ScopedRegistry::op(self.index, |m: &Self| {
            m.observe_and_bucket(v);
        });

        self.observe_and_bucket(v);
    }

    pub(crate) fn observe_and_bucket(&self, v: f64) -> Option<usize> {
        let mut inner = self.inner.write();
        inner.sum += v;
        inner.count += 1;

        let first_bucket = inner
            .buckets
            .iter_mut()
            .enumerate()
            .find(|(_i, (upper_bound, _value))| upper_bound >= &v);

        match first_bucket {
            Some((i, (_upper_bound, value))) => {
                *value += 1;
                Some(i)
            }
            None => None,
        }
    }

    pub(crate) fn get(&self) -> (f64, u64, MappedRwLockReadGuard<Vec<(f64, u64)>>) {
        let inner = self.inner.read();
        let sum = inner.sum;
        let count = inner.count;
        let buckets = RwLockReadGuard::map(inner, |inner| &inner.buckets);
        (sum, count, buckets)
    }
}

impl TypedMetric for Histogram {
    const TYPE: MetricType = MetricType::Histogram;
}

impl EncodeMetric for Histogram {
    fn encode(&self, mut encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
        let (sum, count, buckets) = self.get();
        encoder.encode_histogram::<()>(sum, count, &buckets, None)
    }

    fn metric_type(&self) -> MetricType {
        Self::TYPE
    }
}

impl DatabendMetric for Histogram {
    fn reset_metric(&self) {
        let mut inner = self.inner.write();
        inner.sum = 0.0;
        inner.count = 0;
        inner.buckets = inner
            .buckets
            .iter()
            .map(|(upper_bound, _value)| (*upper_bound, 0))
            .collect();
    }

    fn sample(&self, name: &str, samples: &mut Vec<MetricSample>) {
        let (_sum, _count, buckets) = self.get();

        let mut histogram_count = vec![];
        let mut cumulative = 0;
        for (upper_bound, count) in buckets.iter() {
            cumulative += count;

            histogram_count.push(HistogramCount {
                less_than: *upper_bound,
                count: cumulative as f64,
            });
        }

        samples.push(MetricSample {
            name: name.to_string(),
            labels: Default::default(),
            value: MetricValue::Histogram(histogram_count),
        });
    }
}
