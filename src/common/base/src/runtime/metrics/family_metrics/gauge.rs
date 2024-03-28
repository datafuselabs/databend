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

use prometheus_client::encoding::EncodeMetric;
use prometheus_client::encoding::MetricEncoder;
use prometheus_client::metrics::gauge::Gauge as PGauge;
use prometheus_client::metrics::MetricType;
use prometheus_client::metrics::TypedMetric;

use crate::runtime::metrics::family::Family;
use crate::runtime::metrics::family::FamilyLabels;
use crate::runtime::metrics::family::FamilyMetric;
use crate::runtime::metrics::registry::DatabendMetric;
use crate::runtime::metrics::sample::MetricSample;
use crate::runtime::metrics::sample::MetricValue;
use crate::runtime::metrics::ScopedRegistry;

#[derive(Debug)]
pub struct FamilyGauge<Labels: FamilyLabels> {
    index: usize,
    inner: PGauge,
    labels: Labels,
}

impl<Labels: FamilyLabels> Clone for FamilyGauge<Labels> {
    fn clone(&self) -> Self {
        FamilyGauge {
            index: self.index,
            inner: self.inner.clone(),
            labels: self.labels.clone(),
        }
    }
}

impl<Labels: FamilyLabels> FamilyGauge<Labels> {
    pub fn create(index: usize, labels: Labels) -> FamilyGauge<Labels> {
        FamilyGauge {
            index,
            labels,
            inner: PGauge::default(),
        }
    }

    pub fn inc(&self) -> i64 {
        ScopedRegistry::op(self.index, |m: &Family<Labels, Self>| {
            m.get_or_create(&self.labels).inner.inc();
        });

        self.inner.inc()
    }

    pub fn inc_by(&self, v: i64) -> i64 {
        ScopedRegistry::op(self.index, |m: &Family<Labels, Self>| {
            m.get_or_create(&self.labels).inner.inc_by(v);
        });

        self.inner.inc_by(v)
    }

    pub fn dec(&self) -> i64 {
        ScopedRegistry::op(self.index, |m: &Family<Labels, Self>| {
            m.get_or_create(&self.labels).inner.dec();
        });

        self.inner.dec()
    }

    pub fn dec_by(&self, v: i64) -> i64 {
        ScopedRegistry::op(self.index, |m: &Family<Labels, Self>| {
            m.get_or_create(&self.labels).inner.dec_by(v);
        });

        self.inner.dec_by(v)
    }

    pub fn set(&self, v: i64) -> i64 {
        ScopedRegistry::op(self.index, |m: &Family<Labels, Self>| {
            m.get_or_create(&self.labels).inner.set(v);
        });

        self.inner.set(v)
    }

    pub fn get(&self) -> i64 {
        self.inner.get()
    }
}

impl<Labels: FamilyLabels> TypedMetric for FamilyGauge<Labels> {
    const TYPE: MetricType = MetricType::Gauge;
}

impl<Labels: FamilyLabels> EncodeMetric for FamilyGauge<Labels> {
    fn encode(&self, encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
        self.inner.encode(encoder)
    }

    fn metric_type(&self) -> MetricType {
        Self::TYPE
    }
}

impl<Labels: FamilyLabels> DatabendMetric for FamilyGauge<Labels> {
    fn reset_metric(&self) {
        self.inner.inc_by(-self.inner.get());
    }

    fn sample(&self, name: &str, samples: &mut Vec<MetricSample>) {
        samples.push(MetricSample {
            name: name.to_string(),
            labels: Default::default(),
            value: MetricValue::Gauge(self.get() as f64),
        })
    }
}

impl<Labels: FamilyLabels> FamilyMetric for FamilyGauge<Labels> {}
