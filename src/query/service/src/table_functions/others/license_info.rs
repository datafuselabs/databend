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

use std::any::Any;
use std::ops::Sub;
use std::sync::Arc;
use std::time::Duration;

use chrono::DateTime;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_catalog::table_function::TableFunction;
use databend_common_exception::ErrorCode;
pub use databend_common_exception::Result;
use databend_common_exception::ToErrorCode;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::Value;
use databend_common_license::license::Feature;
use databend_common_license::license::LicenseInfo;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;
use databend_common_storages_factory::Table;
use humantime::Duration as HumanDuration;
use jwt_simple::claims::JWTClaims;
use jwt_simple::prelude::Clock;

pub struct LicenseInfoTable {
    table_info: TableInfo,
}

impl LicenseInfoTable {
    pub fn schema() -> TableSchemaRef {
        TableSchemaRefExt::create(vec![
            TableField::new("license_issuer", TableDataType::String),
            TableField::new("license_type", TableDataType::String),
            TableField::new("organization", TableDataType::String),
            TableField::new("issued_at", TableDataType::Timestamp),
            TableField::new("expire_at", TableDataType::Timestamp),
            // formatted string calculate the available time from now to expiry of license
            TableField::new("available_time_until_expiry", TableDataType::String),
            TableField::new("features", TableDataType::String),
        ])
    }

    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        _table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: String::from("license_info"),
            meta: TableMeta {
                schema: Self::schema(),
                engine: String::from(table_func_name),
                // Assuming that created_on is unnecessary for function table,
                // we could make created_on fixed to pass test_shuffle_action_try_into.
                created_on: DateTime::from_timestamp(0, 0).unwrap(),
                updated_on: DateTime::from_timestamp(0, 0).unwrap(),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(LicenseInfoTable { table_info }))
    }
}

#[async_trait::async_trait]
impl Table for LicenseInfoTable {
    fn is_local(&self) -> bool {
        true
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        // dummy statistics
        Ok((PartStatistics::new_exact(1, 1, 1, 1), Partitions::default()))
    }

    fn table_args(&self) -> Option<TableArgs> {
        None
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        pipeline.add_source(|output| LicenseInfoSource::create(ctx.clone(), output), 1)?;
        Ok(())
    }
}

struct LicenseInfoSource {
    done: bool,
    ctx: Arc<dyn TableContext>,
}

impl LicenseInfoSource {
    pub fn create(ctx: Arc<dyn TableContext>, output: Arc<OutputPort>) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.clone(), output, LicenseInfoSource { ctx, done: false })
    }

    fn to_block(&self, info: &JWTClaims<LicenseInfo>) -> Result<DataBlock> {
        let now = Clock::now_since_epoch();
        let available_time = info.expires_at.unwrap_or_default().sub(now).as_micros();
        let human_readable_available_time =
            HumanDuration::from(Duration::from_micros(available_time)).to_string();

        let feature_str = info.custom.display_features();
        Ok(DataBlock::new(
            vec![
                BlockEntry::new(
                    DataType::String,
                    Value::Scalar(Scalar::String(
                        info.issuer.clone().unwrap_or("".to_string()),
                    )),
                ),
                BlockEntry::new(
                    DataType::String,
                    Value::Scalar(Scalar::String(
                        info.custom.r#type.clone().unwrap_or("".to_string()),
                    )),
                ),
                BlockEntry::new(
                    DataType::String,
                    Value::Scalar(Scalar::String(
                        info.custom.org.clone().unwrap_or("".to_string()),
                    )),
                ),
                BlockEntry::new(
                    DataType::Timestamp,
                    Value::Scalar(Scalar::Timestamp(
                        info.issued_at.unwrap_or_default().as_micros() as i64,
                    )),
                ),
                BlockEntry::new(
                    DataType::Timestamp,
                    Value::Scalar(Scalar::Timestamp(
                        info.expires_at.unwrap_or_default().as_micros() as i64,
                    )),
                ),
                BlockEntry::new(
                    DataType::String,
                    Value::Scalar(Scalar::String(human_readable_available_time)),
                ),
                BlockEntry::new(DataType::String, Value::Scalar(Scalar::String(feature_str))),
            ],
            1,
        ))
    }
}

#[async_trait::async_trait]
impl AsyncSource for LicenseInfoSource {
    const NAME: &'static str = "license_info";

    #[async_trait::unboxed_simple]
    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        let settings = self.ctx.get_settings();
        // sync global changes on distributed node cluster.
        settings.load_changes().await?;
        let license = unsafe {
            settings.get_enterprise_license().map_err_to_code(
                ErrorCode::LicenseKeyInvalid,
                || {
                    format!(
                        "failed to get license for {}",
                        self.ctx.get_tenant().display()
                    )
                },
            )?
        };

        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(license.clone(), Feature::LicenseInfo)?;

        let info = LicenseManagerSwitch::instance()
            .parse_license(license.as_str())
            .map_err_to_code(ErrorCode::LicenseKeyInvalid, || {
                format!(
                    "current license invalid for {}",
                    self.ctx.get_tenant().display()
                )
            })?;
        Ok(Some(self.to_block(&info)?))
    }
}

impl TableFunction for LicenseInfoTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}
