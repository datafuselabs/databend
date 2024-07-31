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
use std::sync::Arc;

use arrow_array::RecordBatch;
use chrono::DateTime;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_catalog::table_function::TableFunction;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::VariantType;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_license::license::Feature;
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
use databend_enterprise_background_service::Suggestion;
use futures_util::StreamExt;
use log::error;
use log::info;

use crate::interpreters::interpreter_plan_sql;
use crate::interpreters::InterpreterFactory;
use crate::sessions::QueryContext;

pub struct SuggestedBackgroundTasksTable {
    table_info: TableInfo,
}

impl SuggestedBackgroundTasksTable {
    pub fn schema() -> TableSchemaRef {
        TableSchemaRefExt::create(vec![
            TableField::new("type", TableDataType::String),
            TableField::new("database_name", TableDataType::String),
            TableField::new("table_name", TableDataType::String),
            TableField::new("table_statistics", TableDataType::Variant),
            TableField::new("should_do_segment_compact", TableDataType::Boolean),
            TableField::new("should_do_compact", TableDataType::Boolean),
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
            name: String::from("suggested_background_tasks"),
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

        Ok(Arc::new(SuggestedBackgroundTasksTable { table_info }))
    }
}

#[async_trait::async_trait]
impl Table for SuggestedBackgroundTasksTable {
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
        pipeline.add_source(
            |output| SuggestedBackgroundTasksSource::create(ctx.clone(), output),
            1,
        )?;
        Ok(())
    }
}

pub struct SuggestedBackgroundTasksSource {
    done: bool,
    ctx: Arc<dyn TableContext>,
}

impl SuggestedBackgroundTasksSource {
    pub fn create(ctx: Arc<dyn TableContext>, output: Arc<OutputPort>) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.clone(), output, SuggestedBackgroundTasksSource {
            ctx,
            done: false,
        })
    }

    #[async_backtrace::framed]
    pub async fn do_execute_sql(
        ctx: Arc<QueryContext>,
        sql: String,
    ) -> Result<Option<RecordBatch>> {
        // Use interpreter_plan_sql, we can write the query log if an error occurs.
        let (plan, _) = interpreter_plan_sql(ctx.clone(), sql.as_str()).await?;

        let data_schema = plan.schema();
        let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let stream = interpreter.execute(ctx.clone()).await?;
        let blocks = stream.map(|v| v).collect::<Vec<_>>().await;

        let mut result = vec![];
        for block in blocks {
            match block {
                Ok(block) => {
                    result.push(block);
                }
                Err(e) => {
                    error!("execute sql error: {:?}", e);
                }
            }
        }
        if result.is_empty() {
            return Ok(None);
        }
        let record = DataBlock::concat(&result)?;
        let record = record
            .to_record_batch_with_dataschema(data_schema.as_ref())
            .map_err(|e| ErrorCode::Internal(format!("{e:?}")))?;

        Ok(Some(record))
    }

    #[async_backtrace::framed]
    pub async fn all_suggestions(ctx: Arc<QueryContext>) -> Result<Vec<Suggestion>> {
        let ctx = ctx.clone();
        info!(
            background = true,
            tenant = ctx.get_tenant().tenant_name().to_string();
            "list all lsuggestions"
        );
        Self::get_suggested_compaction_tasks(ctx).await
    }

    fn to_block(&self, suggestions: Vec<Suggestion>) -> Result<DataBlock> {
        let mut suggestion_type = vec![];
        let mut should_do_segment_compact = vec![];
        let mut should_do_compact = vec![];
        let mut database_name = vec![];
        let mut table_names = vec![];
        let mut table_statistics = vec![];
        for suggestion in suggestions {
            match suggestion {
                Suggestion::Compaction {
                    need_compact_segment,
                    need_compact_block,
                    db_name,
                    table_name,
                    table_stats,
                    ..
                } => {
                    suggestion_type.push("compaction".to_string());
                    should_do_segment_compact.push(Some(need_compact_segment));
                    should_do_compact.push(Some(need_compact_block));
                    database_name.push(db_name);
                    table_names.push(table_name);
                    table_statistics.push(serde_json::to_vec(&table_stats).unwrap());
                }
            }
        }
        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(suggestion_type),
            StringType::from_data(database_name),
            StringType::from_data(table_names),
            VariantType::from_data(table_statistics),
            BooleanType::from_opt_data(should_do_segment_compact),
            BooleanType::from_opt_data(should_do_compact),
        ]))
    }
}

#[async_trait::async_trait]
impl AsyncSource for SuggestedBackgroundTasksSource {
    const NAME: &'static str = "suggested_background_tasks";

    #[async_trait::unboxed_simple]
    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.done {
            return Ok(None);
        }
        self.done = true;

        let ctx = self.ctx.as_any().downcast_ref::<QueryContext>().unwrap();
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(ctx.get_license_key(), Feature::BackgroundService)?;

        let suggestions = Self::all_suggestions(Arc::new(ctx.clone())).await?;
        Ok(Some(self.to_block(suggestions)?))
    }
}

impl TableFunction for SuggestedBackgroundTasksTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}
