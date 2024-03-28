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

use std::sync::Arc;
use std::sync::LazyLock;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use parking_lot::MappedMutexGuard;
use parking_lot::Mutex;
use parking_lot::MutexGuard;
use parking_lot::RwLock;
use prometheus_client::registry::Metric as PMetrics;
use prometheus_client::registry::Registry;

use crate::runtime::metrics::counter::Counter;
use crate::runtime::metrics::family::Family;
use crate::runtime::metrics::family::FamilyLabels;
use crate::runtime::metrics::family_metrics::FamilyCounter as InnerFamilyCounter;
use crate::runtime::metrics::family_metrics::FamilyGauge as InnerFamilyGauge;
use crate::runtime::metrics::family_metrics::FamilyHistogram as InnerFamilyHistogram;
use crate::runtime::metrics::gauge::Gauge;
use crate::runtime::metrics::histogram::Histogram;
use crate::runtime::metrics::histogram::BUCKET_MILLISECONDS;
use crate::runtime::metrics::histogram::BUCKET_SECONDS;
use crate::runtime::metrics::sample::MetricSample;
use crate::runtime::ThreadTracker;

pub static GLOBAL_METRICS_REGISTRY: LazyLock<GlobalRegistry> =
    LazyLock::new(GlobalRegistry::create);

pub trait DatabendMetric {
    fn reset_metric(&self);

    fn sample(&self, name: &str, samples: &mut Vec<MetricSample>);
}

pub trait Metric: PMetrics + DatabendMetric {}

impl<T: PMetrics + DatabendMetric + Clone> Metric for T {}

#[allow(dead_code)]
struct GlobalMetric {
    pub name: String,
    pub help: String,
    pub metric: Box<dyn Metric>,
    pub creator: Box<dyn Fn(usize) -> Box<dyn Metric>>,
}

struct GlobalRegistryInner {
    registry: Registry,
    metrics: Vec<GlobalMetric>,
}

pub struct GlobalRegistry {
    inner: Mutex<GlobalRegistryInner>,
}

unsafe impl Send for GlobalRegistry {}

unsafe impl Sync for GlobalRegistry {}

impl GlobalRegistry {
    pub fn create() -> GlobalRegistry {
        GlobalRegistry {
            inner: Mutex::new(GlobalRegistryInner {
                metrics: vec![],
                registry: Registry::with_prefix("databend"),
            }),
        }
    }

    pub fn register<M, F>(&self, name: &str, help: &str, metric_creator: F) -> M
    where
        M: Metric + Clone,
        F: Fn(usize) -> M + 'static,
    {
        let mut global_registry_inner = self.inner.lock();
        let metric = metric_creator(global_registry_inner.metrics.len());
        global_registry_inner
            .registry
            .register(name, help, metric.clone());
        global_registry_inner.metrics.push(GlobalMetric {
            name: name.to_string(),
            help: help.to_string(),
            metric: Box::new(metric.clone()),
            creator: Box::new(move |index| Box::new(metric_creator(index))),
        });

        metric
    }

    pub(crate) fn new_scoped_metric(&self, index: usize) -> impl Iterator<Item = ScopedMetric> {
        let global_registry = self.inner.lock();
        let mut scoped_metrics = Vec::with_capacity(global_registry.metrics.len() - index);

        for (index, metric) in global_registry.metrics[index..].iter().enumerate() {
            scoped_metrics.push(ScopedMetric {
                name: metric.name.to_string(),
                metric: (metric.creator)(index),
            });
        }

        scoped_metrics.into_iter()
    }

    pub fn inner_mut(&self) -> MappedMutexGuard<'_, Registry> {
        let guard = self.inner.lock();
        MutexGuard::map(guard, |f| &mut f.registry)
    }

    pub fn reset(&self) {
        let global_registry = self.inner.lock();
        for metric in &global_registry.metrics {
            metric.metric.reset_metric();
        }
    }

    pub fn dump_sample(&self) -> Result<Vec<MetricSample>> {
        let global_registry = self.inner.lock();

        let mut samples = Vec::with_capacity(global_registry.metrics.len());

        for metric in global_registry.metrics.iter() {
            metric.metric.sample(&metric.name, &mut samples);
        }

        // TODO:
        // let proc_stats = dump_proc_stats().unwrap_or_default();
        // samples.extend(proc_stats);
        Ok(samples)
    }

    pub fn render_metrics(&self) -> Result<String> {
        let mut text = String::new();
        match prometheus_client::encoding::text::encode(&mut text, &self.inner.lock().registry) {
            Ok(_) => Ok(text),
            Err(err) => Err(ErrorCode::Internal(format!(
                "Failed to encode metrics: {}",
                err
            ))),
        }
    }
}

pub(crate) struct ScopedMetric {
    name: String,
    metric: Box<dyn Metric>,
}

pub struct ScopedRegistry {
    parent: Option<Arc<ScopedRegistry>>,
    metrics: RwLock<Vec<ScopedMetric>>,
}

impl ScopedRegistry {
    pub fn create(parent: Option<Arc<Self>>) -> Arc<Self> {
        Arc::new(ScopedRegistry {
            parent,
            metrics: RwLock::new(vec![]),
        })
    }

    pub(crate) fn op<M: Metric, F: Fn(&M)>(index: usize, f: F) {
        ThreadTracker::with(|x| {
            if let Some(metrics) = x.borrow().payload.metrics.as_ref() {
                metrics.apply(index, f);
            }
        });
    }

    fn apply<M: Metric, F: Fn(&M)>(&self, index: usize, f: F) {
        let metrics = self.metrics.read();

        match metrics.len() > index {
            true => {
                if let Some(metric) = metrics.get(index) {
                    let metric =
                        unsafe { &*(metric.metric.as_ref() as *const dyn Metric as *const M) };
                    // avoid dead lock, is safely
                    drop(metrics);
                    f(metric);
                }
            }
            false => {
                // TODO: may use upgrade read lock is better.
                drop(metrics);
                let mut metrics = self.metrics.write();

                if metrics.len() <= index {
                    let len = metrics.len();
                    metrics.extend(GLOBAL_METRICS_REGISTRY.new_scoped_metric(len));
                }

                if let Some(metric) = metrics.get(index) {
                    let metric =
                        unsafe { &*(metric.metric.as_ref() as *const dyn Metric as *const M) };
                    // avoid dead lock, is safely
                    drop(metrics);
                    f(metric);
                }
            }
        }

        if let Some(parent) = &self.parent {
            parent.apply(index, f);
        }
    }

    pub fn dump_sample(&self) -> Result<Vec<MetricSample>> {
        let metrics = self.metrics.read();
        let mut samples = Vec::with_capacity(metrics.len());
        for metric in metrics.iter() {
            metric.metric.sample(&metric.name, &mut samples);
        }

        Ok(samples)
    }
}

pub fn register_counter(name: &str) -> Counter {
    GLOBAL_METRICS_REGISTRY.register(name, "", Counter::create)
}

pub fn register_gauge(name: &str) -> Gauge {
    GLOBAL_METRICS_REGISTRY.register(name, "", Gauge::create)
}

pub fn register_histogram_in_milliseconds(name: &str) -> Histogram {
    GLOBAL_METRICS_REGISTRY.register(name, "", {
        move |index| Histogram::new(index, BUCKET_MILLISECONDS.iter().copied())
    })
}

pub fn register_histogram_in_seconds(name: &str) -> Histogram {
    GLOBAL_METRICS_REGISTRY.register(name, "", {
        move |index| Histogram::new(index, BUCKET_SECONDS.iter().copied())
    })
}

pub fn register_counter_family<T: FamilyLabels>(name: &str) -> FamilyCounter<T> {
    GLOBAL_METRICS_REGISTRY.register(name, "", |index| {
        FamilyCounter::create(index, InnerFamilyCounter::create)
    })
}

pub fn register_gauge_family<T: FamilyLabels>(name: &str) -> FamilyGauge<T> {
    GLOBAL_METRICS_REGISTRY.register(name, "", |index| {
        FamilyGauge::create(index, InnerFamilyGauge::create)
    })
}

pub fn register_histogram_family_in_seconds<T: FamilyLabels>(name: &str) -> FamilyHistogram<T> {
    GLOBAL_METRICS_REGISTRY.register(name, "", |index| {
        FamilyHistogram::create(index, |index, labels| {
            InnerFamilyHistogram::new(index, labels, BUCKET_SECONDS.iter().copied())
        })
    })
}

pub fn register_histogram_family_in_milliseconds<T>(name: &str) -> FamilyHistogram<T>
where T: FamilyLabels {
    GLOBAL_METRICS_REGISTRY.register(name, "", |index| {
        FamilyHistogram::create(index, |index, labels| {
            InnerFamilyHistogram::new(index, labels, BUCKET_MILLISECONDS.iter().copied())
        })
    })
}

pub type FamilyGauge<T> = Family<T, InnerFamilyGauge<T>>;
pub type FamilyCounter<T> = Family<T, InnerFamilyCounter<T>>;
pub type FamilyHistogram<T> = Family<T, InnerFamilyHistogram<T>>;
