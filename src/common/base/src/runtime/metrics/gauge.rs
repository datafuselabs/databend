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

use crate::runtime::metrics::registry::DatabendMetric;
use crate::runtime::metrics::sample::MetricSample;
use crate::runtime::metrics::sample::MetricValue;
use crate::runtime::metrics::ScopedRegistry;

#[derive(Debug)]
pub struct Gauge {
    inner: PGauge,
    index: usize,
}

impl Clone for Gauge {
    fn clone(&self) -> Self {
        Gauge {
            index: self.index,
            inner: self.inner.clone(),
        }
    }
}

// TODO: Maybe impl deref is better
impl Gauge {
    pub fn create(index: usize) -> Gauge {
        Gauge {
            index,
            inner: PGauge::default(),
        }
    }

    pub fn inc(&self) -> i64 {
        ScopedRegistry::op(self.index, |m: &Self| {
            m.inner.inc();
        });

        self.inner.inc()
    }

    pub fn inc_by(&self, v: i64) -> i64 {
        ScopedRegistry::op(self.index, |m: &Self| {
            m.inner.inc_by(v);
        });

        self.inner.inc_by(v)
    }

    pub fn dec(&self) -> i64 {
        ScopedRegistry::op(self.index, |m: &Self| {
            m.inner.dec();
        });

        self.inner.dec()
    }

    pub fn dec_by(&self, v: i64) -> i64 {
        ScopedRegistry::op(self.index, |m: &Self| {
            m.inner.dec_by(v);
        });

        self.inner.dec_by(v)
    }

    pub fn set(&self, v: i64) -> i64 {
        ScopedRegistry::op(self.index, |m: &Self| {
            m.inner.set(v);
        });

        self.inner.set(v)
    }

    pub fn get(&self) -> i64 {
        self.inner.get()
    }
}

impl TypedMetric for Gauge {
    const TYPE: MetricType = MetricType::Gauge;
}

impl EncodeMetric for Gauge {
    fn encode(&self, encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
        self.inner.encode(encoder)
    }

    fn metric_type(&self) -> MetricType {
        Self::TYPE
    }
}

impl DatabendMetric for Gauge {
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
