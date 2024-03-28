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

use crate::runtime::metrics::family::Family;
use crate::runtime::metrics::family::FamilyLabels;
use crate::runtime::metrics::family::FamilyMetric;
use crate::runtime::metrics::registry::DatabendMetric;
use crate::runtime::metrics::sample::HistogramCount;
use crate::runtime::metrics::sample::MetricSample;
use crate::runtime::metrics::sample::MetricValue;
use crate::runtime::metrics::ScopedRegistry;

/// Histogram is a port of prometheus-client's Histogram. The only difference is that
/// we can reset the histogram.
#[derive(Debug)]
pub struct FamilyHistogram<Labels: FamilyLabels> {
    index: usize,
    labels: Labels,
    inner: Arc<RwLock<Inner>>,
}

impl<Labels: FamilyLabels> Clone for FamilyHistogram<Labels> {
    fn clone(&self) -> Self {
        FamilyHistogram {
            index: self.index,
            inner: self.inner.clone(),
            labels: self.labels.clone(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Inner {
    sum: f64,
    count: u64,
    buckets: Vec<(f64, u64)>,
}

impl<Labels: FamilyLabels> FamilyHistogram<Labels> {
    /// Create a new [`Histogram`].
    pub fn new(index: usize, labels: Labels, buckets: impl Iterator<Item = f64>) -> Self {
        Self {
            index,
            labels,
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
        ScopedRegistry::op(self.index, |m: &Family<Labels, Self>| {
            m.get_or_create(&self.labels).observe_and_bucket(v);
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

impl<Labels: FamilyLabels> TypedMetric for FamilyHistogram<Labels> {
    const TYPE: MetricType = MetricType::Histogram;
}

impl<Labels: FamilyLabels> EncodeMetric for FamilyHistogram<Labels> {
    fn encode(&self, mut encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
        let (sum, count, buckets) = self.get();
        encoder.encode_histogram::<()>(sum, count, &buckets, None)
    }

    fn metric_type(&self) -> MetricType {
        Self::TYPE
    }
}

impl<Labels: FamilyLabels> DatabendMetric for FamilyHistogram<Labels> {
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

impl<Labels: FamilyLabels> FamilyMetric for FamilyHistogram<Labels> {}
