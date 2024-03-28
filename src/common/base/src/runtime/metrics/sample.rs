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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MetricSample {
    pub name: String,
    pub labels: HashMap<String, String>,
    pub value: MetricValue,
}

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize, Clone)]
pub struct HistogramCount {
    pub less_than: f64,
    pub count: f64,
}

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize, Clone)]
pub struct SummaryCount {
    pub quantile: f64,
    pub count: f64,
}

#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize, Clone)]
pub enum MetricValue {
    Counter(f64),
    Gauge(f64),
    Untyped(f64),
    Histogram(Vec<HistogramCount>),
    Summary(Vec<SummaryCount>),
}

impl MetricValue {
    pub fn kind(&self) -> String {
        match self {
            MetricValue::Counter(_) => "counter",
            MetricValue::Gauge(_) => "gauge",
            MetricValue::Untyped(_) => "untyped",
            MetricValue::Histogram(_) => "histogram",
            MetricValue::Summary(_) => "summary",
        }
        .to_string()
    }
}
