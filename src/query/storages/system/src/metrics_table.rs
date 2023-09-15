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
use std::sync::Arc;

use common_base::runtime::GLOBAL_MEM_STAT;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::StringType;
use common_expression::utils::FromData;
use common_expression::DataBlock;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchemaRefExt;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_metrics::reset_metrics;
use common_metrics::MetricSample;
use common_metrics::MetricValue;

use crate::SyncOneBlockSystemTable;
use crate::SyncSystemTable;

pub struct MetricsTable {
    table_info: TableInfo,
}

impl SyncSystemTable for MetricsTable {
    const NAME: &'static str = "system.metrics";
    // Allow distributed query.
    const IS_LOCAL: bool = false;
    const BROADCAST_TRUNCATE: bool = true;

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    fn get_full_data(&self, ctx: Arc<dyn TableContext>) -> Result<DataBlock> {
        let local_id = ctx.get_cluster().local_id.clone();

        let mut samples = {
            let registry = common_metrics::load_global_prometheus_registry();
            common_metrics::dump_metric_samples(&registry)?
        };
        samples.extend(self.custom_metric_samples()?);

        let mut nodes: Vec<Vec<u8>> = Vec::with_capacity(samples.len());
        let mut metrics: Vec<Vec<u8>> = Vec::with_capacity(samples.len());
        let mut labels: Vec<Vec<u8>> = Vec::with_capacity(samples.len());
        let mut kinds: Vec<Vec<u8>> = Vec::with_capacity(samples.len());
        let mut values: Vec<Vec<u8>> = Vec::with_capacity(samples.len());
        for sample in samples.into_iter() {
            nodes.push(local_id.clone().into_bytes());
            metrics.push(sample.name.clone().into_bytes());
            kinds.push(sample.value.kind().into_bytes());
            labels.push(self.display_sample_labels(&sample.labels)?.into_bytes());
            values.push(self.display_sample_value(&sample.value)?.into_bytes());
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(nodes),
            StringType::from_data(metrics),
            StringType::from_data(kinds),
            StringType::from_data(labels),
            StringType::from_data(values),
        ]))
    }

    fn truncate(&self, _ctx: Arc<dyn TableContext>) -> Result<()> {
        reset_metrics()?;
        Ok(())
    }
}

impl MetricsTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("node", TableDataType::String),
            TableField::new("metric", TableDataType::String),
            TableField::new("kind", TableDataType::String),
            TableField::new("labels", TableDataType::String),
            TableField::new("value", TableDataType::String),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'metrics'".to_string(),
            name: "metrics".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemMetrics".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        SyncOneBlockSystemTable::create(MetricsTable { table_info })
    }

    fn display_sample_labels(&self, labels: &HashMap<String, String>) -> Result<String> {
        serde_json::to_string(labels).map_err(|err| {
            ErrorCode::Internal(format!(
                "Dump prometheus metrics on display labels: {}",
                err
            ))
        })
    }

    fn display_sample_value(&self, value: &MetricValue) -> Result<String> {
        match value {
            MetricValue::Counter(v) => serde_json::to_string(v),
            MetricValue::Gauge(v) => serde_json::to_string(v),
            MetricValue::Untyped(v) => serde_json::to_string(v),
            MetricValue::Histogram(v) => serde_json::to_string(v),
            MetricValue::Summary(v) => serde_json::to_string(v),
        }
        .map_err(|err| {
            ErrorCode::Internal(format!(
                "Dump prometheus metrics failed on display values: {}",
                err
            ))
        })
    }

    /// Custom metrics that are not collected by prometheus.
    fn custom_metric_samples(&self) -> Result<Vec<MetricSample>> {
        let samples = vec![
            MetricSample {
                name: "query_memory_usage_bytes".to_string(),
                value: MetricValue::Counter(GLOBAL_MEM_STAT.get_memory_usage() as f64),
                labels: HashMap::new(),
            },
            MetricSample {
                name: "query_memory_peak_usage_bytes".to_string(),
                value: MetricValue::Counter(GLOBAL_MEM_STAT.get_peak_memory_usage() as f64),
                labels: HashMap::new(),
            },
        ];

        Ok(samples)
    }
}
