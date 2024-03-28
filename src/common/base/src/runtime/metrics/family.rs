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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Error;
use std::fmt::Formatter;
use std::hash::Hash;
use std::sync::Arc;

use parking_lot::RwLock;
use prometheus_client::collector::Collector;
use prometheus_client::encoding::DescriptorEncoder;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::encoding::EncodeMetric;
use prometheus_client::encoding::MetricEncoder;
use prometheus_client::metrics::MetricType;
use prometheus_client::metrics::TypedMetric;
use prometheus_client::registry::Registry;

use crate::runtime::metrics::registry::DatabendMetric;
use crate::runtime::metrics::registry::Metric;
use crate::runtime::metrics::sample::MetricSample;
use crate::runtime::metrics::ScopedRegistry;

pub trait FamilyMetric: Metric + TypedMetric {}

pub trait FamilyLabels: Clone + Hash + Eq + Debug + EncodeLabelSet + Send + Sync + 'static {}

impl<T: Clone + Hash + Eq + Debug + EncodeLabelSet + Send + Sync + 'static> FamilyLabels for T {}

pub struct Family<S: FamilyLabels, M: FamilyMetric> {
    index: usize,

    metrics: Arc<RwLock<HashMap<S, Arc<M>>>>,

    family_metric_creator: Arc<Box<dyn Fn(usize, S) -> M + Send + Sync + 'static>>,
}

impl<S: FamilyLabels, M: FamilyMetric> Debug for Family<S, M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Family")
            .field("metrics", &self.metrics)
            .finish()
    }
}

impl<S: FamilyLabels, M: FamilyMetric> Clone for Family<S, M> {
    fn clone(&self) -> Self {
        Family {
            index: self.index,
            metrics: self.metrics.clone(),
            family_metric_creator: self.family_metric_creator.clone(),
        }
    }
}

impl<S: FamilyLabels, M: FamilyMetric> Family<S, M> {
    pub fn create<F: Fn(usize, S) -> M + Send + Sync + 'static>(index: usize, f: F) -> Self {
        Self {
            index,
            metrics: Arc::new(RwLock::new(Default::default())),
            family_metric_creator: Arc::new(Box::new(f)),
        }
    }
}

impl<S: FamilyLabels, M: FamilyMetric> Family<S, M> {
    pub fn get_or_create(&self, label_set: &S) -> Arc<M> {
        if let Some(metric) = self.metrics.read().get(label_set) {
            return metric.clone();
        }

        let mut write_guard = self.metrics.write();

        match write_guard.entry(label_set.clone()) {
            Entry::Occupied(v) => v.get().clone(),
            Entry::Vacant(v) => {
                let metric = Arc::new((self.family_metric_creator)(self.index, label_set.clone()));
                v.insert(metric.clone());
                metric
            }
        }
    }

    pub fn remove(&self, label_set: &S) -> bool {
        ScopedRegistry::op(self.index, |m: &Self| {
            m.metrics.write().remove(label_set);
        });

        self.metrics.write().remove(label_set).is_some()
    }

    pub fn clear(&self) {
        ScopedRegistry::op(self.index, |m: &Self| {
            m.metrics.write().clear();
        });

        // TODO: clear scoped registry
        self.metrics.write().clear()
    }
}

impl<S: FamilyLabels, M: FamilyMetric> TypedMetric for Family<S, M> {
    const TYPE: MetricType = <M as TypedMetric>::TYPE;
}

impl<S: FamilyLabels, M: FamilyMetric> EncodeMetric for Family<S, M> {
    fn encode(&self, mut encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
        let guard = self.metrics.read();
        for (label_set, m) in guard.iter() {
            let encoder = encoder.encode_family(label_set)?;
            m.encode(encoder)?;
        }

        Ok(())
    }

    fn metric_type(&self) -> MetricType {
        M::TYPE
    }
}

impl<S: FamilyLabels, M: FamilyMetric> DatabendMetric for Family<S, M> {
    fn reset_metric(&self) {
        self.clear()
    }

    fn sample(&self, name: &str, samples: &mut Vec<MetricSample>) {
        // TODO: hack, using prometheus parser for get metric labels
        let mut registry = Registry::with_labels(vec![].into_iter());
        registry.register_collector(Box::new(DummyCollector {
            name: name.to_string(),
            metrics: self.metrics.clone(),
        }));

        let mut text = String::new();
        if prometheus_client::encoding::text::encode(&mut text, &registry).is_ok() {
            samples.extend(prometheus_parse::parse_at(text.lines()));
        }
    }
}

#[derive(Debug)]
struct DummyCollector<S: FamilyLabels, M: FamilyMetric> {
    name: String,
    metrics: Arc<RwLock<HashMap<S, Arc<M>>>>,
}

impl<S: FamilyLabels, M: FamilyMetric> Collector for DummyCollector<S, M> {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), Error> {
        let mut encoder = encoder.encode_descriptor(&self.name, "", None, M::TYPE)?;

        for (label_set, m) in self.metrics.read().iter() {
            let encoder = encoder.encode_family(label_set)?;
            m.encode(encoder)?;
        }

        Ok(())
    }
}

// Copy from prometheus_parse and fixes some bug
mod prometheus_parse {
    use std::collections::BTreeMap;
    use std::collections::HashMap;

    use prometheus_parse::LineInfo;
    use prometheus_parse::SampleType;

    use crate::runtime::metrics::sample::SummaryCount;
    use crate::runtime::metrics::HistogramCount;
    use crate::runtime::metrics::MetricSample;
    use crate::runtime::metrics::MetricValue;

    fn parse_golang_float(s: &str) -> Result<f64, <f64 as std::str::FromStr>::Err> {
        match s.to_lowercase().as_str() {
            "nan" => Ok(std::f64::NAN), // f64::parse doesn't recognize 'nan'
            s => s.parse::<f64>(),      // f64::parse expects lowercase [+-]inf
        }
    }

    fn parse_bucket(s: &str, label: &str) -> Option<(BTreeMap<String, String>, f64)> {
        let mut labs = BTreeMap::new();

        let mut value = None;
        for kv in s.split(',') {
            let kvpair = kv.split('=').collect::<Vec<_>>();
            if kvpair.len() != 2 || kvpair[0].is_empty() {
                continue;
            }
            let (k, v) = (kvpair[0], kvpair[1].trim_matches('"'));
            if k == label {
                value = match parse_golang_float(v) {
                    Ok(v) => Some(v),
                    Err(_) => return None,
                };
            } else {
                labs.insert(k.to_string(), v.to_string());
            }
        }

        value.map(|v| (labs, v))
    }

    pub fn parse_at<'a>(lines: impl Iterator<Item = &'a str>) -> Vec<MetricSample> {
        let mut types: HashMap<String, SampleType> = HashMap::new();
        let mut buckets: HashMap<(String, BTreeMap<String, String>), MetricSample> = HashMap::new();
        let mut samples: Vec<MetricSample> = vec![];

        for read_line in lines {
            match LineInfo::parse(read_line) {
                LineInfo::Type {
                    ref metric_name,
                    ref sample_type,
                    ..
                } => {
                    match sample_type {
                        SampleType::Counter => {
                            types.insert(format!("{}_total", metric_name), *sample_type)
                        }
                        _ => types.insert(metric_name.to_string(), *sample_type),
                    };
                }
                LineInfo::Sample {
                    metric_name,
                    ref labels,
                    value,
                    ..
                } => {
                    // Parse value or skip
                    let fvalue = if let Ok(v) = parse_golang_float(value) {
                        v
                    } else {
                        continue;
                    };

                    match (types.get(metric_name), labels) {
                        (Some(SampleType::Histogram), Some(labels)) => {
                            if let Some((labels, lt)) = parse_bucket(labels, "le") {
                                let sample = buckets
                                    .entry((metric_name.to_string(), labels.clone()))
                                    .or_insert(MetricSample {
                                        name: metric_name.to_string(),
                                        labels: labels.into_iter().collect(),
                                        value: MetricValue::Histogram(vec![]),
                                    });

                                if let MetricValue::Histogram(histogram_counts) = &mut sample.value
                                {
                                    histogram_counts.push(HistogramCount {
                                        less_than: lt,
                                        count: fvalue,
                                    });
                                }
                            }
                        }
                        (Some(SampleType::Summary), Some(labels)) => {
                            if let Some((labels, q)) = parse_bucket(labels, "quantile") {
                                let sample = buckets
                                    .entry((metric_name.to_string(), labels.clone()))
                                    .or_insert(MetricSample {
                                        name: metric_name.to_string(),
                                        labels: labels.into_iter().collect(),
                                        value: MetricValue::Summary(vec![]),
                                    });

                                if let MetricValue::Summary(summary_count) = &mut sample.value {
                                    summary_count.push(SummaryCount {
                                        quantile: q,
                                        count: fvalue,
                                    });
                                }
                            }
                        }
                        (ty, labels) => {
                            let mut labels_map = HashMap::new();
                            if let Some(labels) = labels {
                                for kv in labels.split(',') {
                                    let kvpair = kv.split('=').collect::<Vec<_>>();
                                    if kvpair.len() != 2 || kvpair[0].is_empty() {
                                        continue;
                                    }
                                    labels_map.insert(
                                        kvpair[0].to_string(),
                                        kvpair[1].trim_matches('"').to_string(),
                                    );
                                }
                            }

                            samples.push(match ty {
                                Some(SampleType::Counter) => MetricSample {
                                    name: metric_name.trim_end_matches("_total").to_string(),
                                    labels: labels_map,
                                    value: MetricValue::Counter(fvalue),
                                },
                                Some(SampleType::Gauge) => MetricSample {
                                    name: metric_name.to_string(),
                                    labels: labels_map,
                                    value: MetricValue::Gauge(fvalue),
                                },
                                _ => MetricSample {
                                    name: metric_name.to_string(),
                                    labels: labels_map,
                                    value: MetricValue::Untyped(fvalue),
                                },
                            });
                        }
                    };
                }
                _ => {}
            }
        }
        samples.extend(buckets.drain().map(|(_k, v)| v).collect::<Vec<_>>());
        samples
    }
}
