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

use std::fmt::Debug;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use prometheus_client::encoding::EncodeMetric;
use prometheus_client::encoding::MetricEncoder;
use prometheus_client::metrics::MetricType;
use prometheus_client::metrics::TypedMetric;

use crate::runtime::metrics::family::Family;
use crate::runtime::metrics::family::FamilyLabels;
use crate::runtime::metrics::family::FamilyMetric;
use crate::runtime::metrics::registry::DatabendMetric;
use crate::runtime::metrics::MetricSample;
use crate::runtime::metrics::MetricValue;
use crate::runtime::metrics::ScopedRegistry;

#[derive(Debug)]
pub struct FamilyCounter<Labels: FamilyLabels> {
    index: usize,
    labels: Labels,
    value: Arc<AtomicU64>,
}

impl<Labels: FamilyLabels> FamilyCounter<Labels> {
    pub fn create(index: usize, labels: Labels) -> Self {
        FamilyCounter {
            index,
            labels,
            value: Arc::new(Default::default()),
        }
    }

    pub fn inc(&self) -> u64 {
        ScopedRegistry::op(self.index, |m: &Family<Labels, Self>| {
            m.get_or_create(&self.labels)
                .value
                .fetch_add(1, Ordering::Relaxed);
        });

        self.value.fetch_add(1, Ordering::Relaxed)
    }

    pub fn inc_by(&self, v: u64) -> u64 {
        ScopedRegistry::op(self.index, |m: &Family<Labels, Self>| {
            m.get_or_create(&self.labels)
                .value
                .fetch_add(v, Ordering::Relaxed);
        });

        self.value.fetch_add(v, Ordering::Relaxed)
    }

    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    pub fn reset(&self) {
        self.value.store(0, Ordering::Release)
    }
}

impl<Labels: FamilyLabels> Clone for FamilyCounter<Labels> {
    fn clone(&self) -> Self {
        FamilyCounter::<Labels> {
            index: self.index,
            value: self.value.clone(),
            labels: self.labels.clone(),
        }
    }
}

impl<Labels: FamilyLabels> TypedMetric for FamilyCounter<Labels> {
    const TYPE: MetricType = MetricType::Counter;
}

impl<Labels: FamilyLabels> EncodeMetric for FamilyCounter<Labels> {
    fn encode(&self, mut encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
        encoder.encode_counter::<(), _, u64>(&self.get(), None)
    }

    fn metric_type(&self) -> MetricType {
        Self::TYPE
    }
}

impl<Labels: FamilyLabels> FamilyMetric for FamilyCounter<Labels> {}

impl<Labels: FamilyLabels> DatabendMetric for FamilyCounter<Labels> {
    fn reset_metric(&self) {
        self.value.store(0, Ordering::Release);
    }

    fn sample(&self, name: &str, samples: &mut Vec<MetricSample>) {
        samples.push(MetricSample {
            name: name.to_string(),
            labels: Default::default(),
            value: MetricValue::Counter(self.get() as f64),
        });
    }
}
