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

use std::collections::HashMap;

use databend_common_base::base::GlobalUniqName;
use databend_common_base::runtime::metrics::register_counter;
use databend_common_base::runtime::metrics::register_counter_family;
use databend_common_base::runtime::metrics::register_gauge;
use databend_common_base::runtime::metrics::register_histogram_in_milliseconds;
use databend_common_base::runtime::metrics::register_histogram_in_seconds;
use databend_common_base::runtime::metrics::HistogramCount;
use databend_common_base::runtime::metrics::MetricSample;
use databend_common_base::runtime::metrics::MetricValue;
use databend_common_base::runtime::metrics::ScopedRegistry;
use databend_common_base::runtime::metrics::BUCKET_MILLISECONDS;
use databend_common_base::runtime::metrics::BUCKET_SECONDS;
use databend_common_base::runtime::metrics::GLOBAL_METRICS_REGISTRY;
use databend_common_base::runtime::ThreadTracker;
use databend_common_exception::Result;

fn assert_contain_metric(samples: Vec<MetricSample>, expected: MetricSample) {
    for sample in samples {
        if sample.name == expected.name && sample.labels == expected.labels {
            assert_eq!(sample.value, expected.value);
            return;
        }
    }

    panic!()
}

fn assert_contain_metrics(samples: Vec<MetricSample>, expected: Vec<MetricSample>) {
    for expected in expected {
        assert_contain_metric(samples.clone(), expected);
    }
}

#[test]
fn test_tracking_scoped_counter_metrics() -> Result<()> {
    let uniq_metric_name = GlobalUniqName::unique();
    let counter = register_counter(&uniq_metric_name);
    counter.inc();

    let scoped_registry = ScopedRegistry::create(None);
    {
        // tracking assert
        let mut new_tracking_payload = ThreadTracker::new_tracking_payload();
        new_tracking_payload.metrics = Some(scoped_registry.clone());
        let _guard = ThreadTracker::tracking(new_tracking_payload);

        counter.inc();

        assert_contain_metric(GLOBAL_METRICS_REGISTRY.dump_sample()?, MetricSample {
            name: uniq_metric_name.clone(),
            labels: Default::default(),
            value: MetricValue::Counter(2_f64),
        });

        assert_contain_metric(scoped_registry.dump_sample()?, MetricSample {
            name: uniq_metric_name.clone(),
            labels: Default::default(),
            value: MetricValue::Counter(1_f64),
        });
    }

    // untracking assert
    counter.inc();

    assert_contain_metric(GLOBAL_METRICS_REGISTRY.dump_sample()?, MetricSample {
        name: uniq_metric_name.clone(),
        labels: Default::default(),
        value: MetricValue::Counter(3_f64),
    });

    assert_contain_metric(scoped_registry.dump_sample()?, MetricSample {
        name: uniq_metric_name.clone(),
        labels: Default::default(),
        value: MetricValue::Counter(1_f64),
    });

    Ok(())
}

#[test]
fn test_tracking_scoped_gauge_metrics() -> Result<()> {
    let uniq_metric_name = GlobalUniqName::unique();
    let gauge = register_gauge(&uniq_metric_name);
    gauge.inc();

    let scoped_registry = ScopedRegistry::create(None);
    {
        // tracking assert
        let mut new_tracking_payload = ThreadTracker::new_tracking_payload();
        new_tracking_payload.metrics = Some(scoped_registry.clone());
        let _guard = ThreadTracker::tracking(new_tracking_payload);

        gauge.inc();

        assert_contain_metric(GLOBAL_METRICS_REGISTRY.dump_sample()?, MetricSample {
            name: uniq_metric_name.clone(),
            labels: Default::default(),
            value: MetricValue::Gauge(2_f64),
        });

        assert_contain_metric(scoped_registry.dump_sample()?, MetricSample {
            name: uniq_metric_name.clone(),
            labels: Default::default(),
            value: MetricValue::Gauge(1_f64),
        });
    }

    // untracking assert
    gauge.inc();

    assert_contain_metric(GLOBAL_METRICS_REGISTRY.dump_sample()?, MetricSample {
        name: uniq_metric_name.clone(),
        labels: Default::default(),
        value: MetricValue::Gauge(3_f64),
    });

    assert_contain_metric(scoped_registry.dump_sample()?, MetricSample {
        name: uniq_metric_name.clone(),
        labels: Default::default(),
        value: MetricValue::Gauge(1_f64),
    });

    Ok(())
}

#[test]
fn test_tracking_scoped_histogram_in_seconds_metrics() -> Result<()> {
    let uniq_metric_name = GlobalUniqName::unique();
    let histogram = register_histogram_in_seconds(&uniq_metric_name);
    histogram.observe(1801.0);

    fn seconds_histogram_value(v: f64) -> Vec<HistogramCount> {
        BUCKET_SECONDS
            .iter()
            .map(|lt| HistogramCount {
                less_than: *lt,
                count: 0.0,
            })
            .chain(std::iter::once(HistogramCount {
                less_than: f64::MAX,
                count: v,
            }))
            .collect::<Vec<_>>()
    }

    let scoped_registry = ScopedRegistry::create(None);
    {
        // tracking assert
        let mut new_tracking_payload = ThreadTracker::new_tracking_payload();
        new_tracking_payload.metrics = Some(scoped_registry.clone());
        let _guard = ThreadTracker::tracking(new_tracking_payload);

        histogram.observe(1801.0);

        assert_contain_metric(GLOBAL_METRICS_REGISTRY.dump_sample()?, MetricSample {
            name: uniq_metric_name.clone(),
            labels: Default::default(),
            value: MetricValue::Histogram(seconds_histogram_value(2_f64)),
        });

        assert_contain_metric(scoped_registry.dump_sample()?, MetricSample {
            name: uniq_metric_name.clone(),
            labels: Default::default(),
            value: MetricValue::Histogram(seconds_histogram_value(1_f64)),
        });
    }

    // untracking assert
    histogram.observe(1801.0);

    assert_contain_metric(GLOBAL_METRICS_REGISTRY.dump_sample()?, MetricSample {
        name: uniq_metric_name.clone(),
        labels: Default::default(),
        value: MetricValue::Histogram(seconds_histogram_value(3_f64)),
    });

    assert_contain_metric(scoped_registry.dump_sample()?, MetricSample {
        name: uniq_metric_name.clone(),
        labels: Default::default(),
        value: MetricValue::Histogram(seconds_histogram_value(1_f64)),
    });

    Ok(())
}

#[test]
fn test_tracking_scoped_histogram_in_milliseconds_metrics() -> Result<()> {
    let uniq_metric_name = GlobalUniqName::unique();
    let histogram = register_histogram_in_milliseconds(&uniq_metric_name);
    histogram.observe(1800001.0);

    fn milliseconds_histogram_value(v: f64) -> Vec<HistogramCount> {
        BUCKET_MILLISECONDS
            .iter()
            .map(|lt| HistogramCount {
                less_than: *lt,
                count: 0.0,
            })
            .chain(std::iter::once(HistogramCount {
                less_than: f64::MAX,
                count: v,
            }))
            .collect::<Vec<_>>()
    }

    let scoped_registry = ScopedRegistry::create(None);
    {
        // tracking assert
        let mut new_tracking_payload = ThreadTracker::new_tracking_payload();
        new_tracking_payload.metrics = Some(scoped_registry.clone());
        let _guard = ThreadTracker::tracking(new_tracking_payload);

        histogram.observe(1800001.0);

        assert_contain_metric(GLOBAL_METRICS_REGISTRY.dump_sample()?, MetricSample {
            name: uniq_metric_name.clone(),
            labels: Default::default(),
            value: MetricValue::Histogram(milliseconds_histogram_value(2_f64)),
        });

        assert_contain_metric(scoped_registry.dump_sample()?, MetricSample {
            name: uniq_metric_name.clone(),
            labels: Default::default(),
            value: MetricValue::Histogram(milliseconds_histogram_value(1_f64)),
        });
    }

    // untracking assert
    histogram.observe(1800001.0);

    assert_contain_metric(GLOBAL_METRICS_REGISTRY.dump_sample()?, MetricSample {
        name: uniq_metric_name.clone(),
        labels: Default::default(),
        value: MetricValue::Histogram(milliseconds_histogram_value(3_f64)),
    });

    assert_contain_metric(scoped_registry.dump_sample()?, MetricSample {
        name: uniq_metric_name.clone(),
        labels: Default::default(),
        value: MetricValue::Histogram(milliseconds_histogram_value(1_f64)),
    });

    Ok(())
}

#[test]
fn test_tracking_scoped_family_counter_metrics() -> Result<()> {
    let uniq_metric_name = GlobalUniqName::unique();
    let counter = register_counter_family::<Vec<(&'static str, u64)>>(&uniq_metric_name);
    counter.get_or_create(&vec![("TEST_LABEL1", 1)]).inc();
    counter.get_or_create(&vec![("TEST_LABEL1", 2)]).inc();
    counter.get_or_create(&vec![("TEST_LABEL2", 1)]).inc();
    counter.get_or_create(&vec![("TEST_LABEL3", 1)]).inc();

    let scoped_registry = ScopedRegistry::create(None);
    {
        // tracking assert
        let mut new_tracking_payload = ThreadTracker::new_tracking_payload();
        new_tracking_payload.metrics = Some(scoped_registry.clone());
        let _guard = ThreadTracker::tracking(new_tracking_payload);

        counter.get_or_create(&vec![("TEST_LABEL1", 1)]).inc();
        counter.get_or_create(&vec![("TEST_LABEL1", 2)]).inc();
        counter.get_or_create(&vec![("TEST_LABEL1", 3)]).inc();
        counter.get_or_create(&vec![("TEST_LABEL2", 1)]).inc();
        counter.get_or_create(&vec![("TEST_LABEL4", 1)]).inc();

        assert_contain_metrics(GLOBAL_METRICS_REGISTRY.dump_sample()?, vec![
            MetricSample {
                name: uniq_metric_name.clone(),
                labels: HashMap::from([("TEST_LABEL1".to_string(), "1".to_string())]),
                value: MetricValue::Counter(2_f64),
            },
            MetricSample {
                name: uniq_metric_name.clone(),
                labels: HashMap::from([("TEST_LABEL1".to_string(), "2".to_string())]),
                value: MetricValue::Counter(2_f64),
            },
            MetricSample {
                name: uniq_metric_name.clone(),
                labels: HashMap::from([("TEST_LABEL1".to_string(), "3".to_string())]),
                value: MetricValue::Counter(1_f64),
            },
            MetricSample {
                name: uniq_metric_name.clone(),
                labels: HashMap::from([("TEST_LABEL2".to_string(), "1".to_string())]),
                value: MetricValue::Counter(2_f64),
            },
            MetricSample {
                name: uniq_metric_name.clone(),
                labels: HashMap::from([("TEST_LABEL3".to_string(), "1".to_string())]),
                value: MetricValue::Counter(1_f64),
            },
            MetricSample {
                name: uniq_metric_name.clone(),
                labels: HashMap::from([("TEST_LABEL4".to_string(), "1".to_string())]),
                value: MetricValue::Counter(1_f64),
            },
        ]);

        assert_contain_metrics(scoped_registry.dump_sample()?, vec![
            MetricSample {
                name: uniq_metric_name.clone(),
                labels: HashMap::from([("TEST_LABEL1".to_string(), "1".to_string())]),
                value: MetricValue::Counter(1_f64),
            },
            MetricSample {
                name: uniq_metric_name.clone(),
                labels: HashMap::from([("TEST_LABEL1".to_string(), "2".to_string())]),
                value: MetricValue::Counter(1_f64),
            },
            MetricSample {
                name: uniq_metric_name.clone(),
                labels: HashMap::from([("TEST_LABEL1".to_string(), "3".to_string())]),
                value: MetricValue::Counter(1_f64),
            },
            MetricSample {
                name: uniq_metric_name.clone(),
                labels: HashMap::from([("TEST_LABEL1".to_string(), "2".to_string())]),
                value: MetricValue::Counter(1_f64),
            },
            MetricSample {
                name: uniq_metric_name.clone(),
                labels: HashMap::from([("TEST_LABEL2".to_string(), "1".to_string())]),
                value: MetricValue::Counter(1_f64),
            },
            MetricSample {
                name: uniq_metric_name.clone(),
                labels: HashMap::from([("TEST_LABEL4".to_string(), "1".to_string())]),
                value: MetricValue::Counter(1_f64),
            },
        ]);
    }

    // untracking assert
    counter.get_or_create(&vec![("TEST_LABEL1", 1)]).inc();
    counter.get_or_create(&vec![("TEST_LABEL1", 2)]).inc();
    counter.get_or_create(&vec![("TEST_LABEL2", 1)]).inc();
    counter.get_or_create(&vec![("TEST_LABEL3", 1)]).inc();

    assert_contain_metrics(GLOBAL_METRICS_REGISTRY.dump_sample()?, vec![
        MetricSample {
            name: uniq_metric_name.clone(),
            labels: HashMap::from([("TEST_LABEL1".to_string(), "1".to_string())]),
            value: MetricValue::Counter(3_f64),
        },
        MetricSample {
            name: uniq_metric_name.clone(),
            labels: HashMap::from([("TEST_LABEL1".to_string(), "2".to_string())]),
            value: MetricValue::Counter(3_f64),
        },
        MetricSample {
            name: uniq_metric_name.clone(),
            labels: HashMap::from([("TEST_LABEL1".to_string(), "3".to_string())]),
            value: MetricValue::Counter(1_f64),
        },
        MetricSample {
            name: uniq_metric_name.clone(),
            labels: HashMap::from([("TEST_LABEL2".to_string(), "1".to_string())]),
            value: MetricValue::Counter(3_f64),
        },
        MetricSample {
            name: uniq_metric_name.clone(),
            labels: HashMap::from([("TEST_LABEL3".to_string(), "1".to_string())]),
            value: MetricValue::Counter(2_f64),
        },
        MetricSample {
            name: uniq_metric_name.clone(),
            labels: HashMap::from([("TEST_LABEL4".to_string(), "1".to_string())]),
            value: MetricValue::Counter(1_f64),
        },
    ]);

    assert_contain_metrics(scoped_registry.dump_sample()?, vec![
        MetricSample {
            name: uniq_metric_name.clone(),
            labels: HashMap::from([("TEST_LABEL1".to_string(), "1".to_string())]),
            value: MetricValue::Counter(1_f64),
        },
        MetricSample {
            name: uniq_metric_name.clone(),
            labels: HashMap::from([("TEST_LABEL1".to_string(), "2".to_string())]),
            value: MetricValue::Counter(1_f64),
        },
        MetricSample {
            name: uniq_metric_name.clone(),
            labels: HashMap::from([("TEST_LABEL1".to_string(), "3".to_string())]),
            value: MetricValue::Counter(1_f64),
        },
        MetricSample {
            name: uniq_metric_name.clone(),
            labels: HashMap::from([("TEST_LABEL1".to_string(), "2".to_string())]),
            value: MetricValue::Counter(1_f64),
        },
        MetricSample {
            name: uniq_metric_name.clone(),
            labels: HashMap::from([("TEST_LABEL2".to_string(), "1".to_string())]),
            value: MetricValue::Counter(1_f64),
        },
        MetricSample {
            name: uniq_metric_name.clone(),
            labels: HashMap::from([("TEST_LABEL4".to_string(), "1".to_string())]),
            value: MetricValue::Counter(1_f64),
        },
    ]);

    Ok(())
}
