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

use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_api::BackgroundApi;
use databend_common_meta_app::background::BackgroundJobType;
use databend_common_meta_app::background::ListBackgroundJobsReq;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_users::UserApiProvider;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

pub struct BackgroundJobTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for BackgroundJobTable {
    const NAME: &'static str = "system.background_jobs";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        let tenant = ctx.get_tenant();
        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let jobs = meta_api
            .list_background_jobs(ListBackgroundJobsReq::new(&tenant))
            .await?;
        let mut names = Vec::with_capacity(jobs.len());
        let mut job_types = Vec::with_capacity(jobs.len());
        let mut scheduled_job_interval_secs = Vec::with_capacity(jobs.len());
        let mut scheduled_job_cron_expression = Vec::with_capacity(jobs.len());
        let mut scheduled_job_cron_timezone = Vec::with_capacity(jobs.len());
        let mut task_types = Vec::with_capacity(jobs.len());
        let mut job_states = Vec::with_capacity(jobs.len());
        let mut last_task_ids = Vec::with_capacity(jobs.len());
        let mut last_task_run_at = Vec::with_capacity(jobs.len());
        let mut next_task_scheduled_time = Vec::with_capacity(jobs.len());
        let mut message = Vec::with_capacity(jobs.len());
        let mut last_updated = Vec::with_capacity(jobs.len());
        let mut creator = Vec::with_capacity(jobs.len());
        let mut create_time = Vec::with_capacity(jobs.len());
        for (_, name, job) in jobs {
            names.push(name);
            let job_type = job.job_params.as_ref().map(|x| x.job_type.clone());
            if let Some(job_type) = job_type {
                job_types.push(Some(job_type.to_string()));
                match job_type {
                    BackgroundJobType::INTERVAL => {
                        scheduled_job_interval_secs.push(Some(
                            job.job_params
                                .as_ref()
                                .unwrap()
                                .scheduled_job_interval
                                .as_secs(),
                        ));
                        scheduled_job_cron_expression.push(None);
                        scheduled_job_cron_timezone.push(None);
                    }
                    BackgroundJobType::CRON => {
                        scheduled_job_interval_secs.push(None);
                        scheduled_job_cron_expression.push(Some(
                            job.job_params.as_ref().unwrap().scheduled_job_cron.clone(),
                        ));
                        scheduled_job_cron_timezone.push(
                            job.job_params
                                .unwrap()
                                .scheduled_job_timezone
                                .map(|tz| tz.to_string()),
                        );
                    }
                    BackgroundJobType::ONESHOT => {
                        scheduled_job_interval_secs.push(None);
                        scheduled_job_cron_expression.push(None);
                        scheduled_job_cron_timezone.push(None);
                    }
                }
            } else {
                job_types.push(None);
                scheduled_job_interval_secs.push(None);
                scheduled_job_cron_expression.push(None);
                scheduled_job_cron_timezone.push(None);
            }
            task_types.push(job.task_type.to_string());
            job_states.push(job.job_status.as_ref().map(|x| x.job_state.to_string()));
            last_task_ids.push(job.job_status.as_ref().and_then(|x| x.last_task_id.clone()));
            last_task_run_at.push(
                job.job_status
                    .as_ref()
                    .and_then(|x| x.last_task_run_at.map(|x| x.timestamp_micros())),
            );
            next_task_scheduled_time.push(
                job.job_status
                    .as_ref()
                    .and_then(|x| x.next_task_scheduled_time.map(|x| x.timestamp_micros())),
            );
            message.push(job.message);
            last_updated.push(job.last_updated.map(|t| t.timestamp_micros()));
            creator.push(job.creator.map(|x| x.identity().to_string()));
            create_time.push(job.created_at.timestamp_micros());
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            StringType::from_opt_data(job_types),
            NumberType::from_opt_data(scheduled_job_interval_secs),
            StringType::from_opt_data(scheduled_job_cron_expression),
            StringType::from_opt_data(scheduled_job_cron_timezone),
            StringType::from_data(task_types),
            StringType::from_opt_data(job_states),
            StringType::from_opt_data(last_task_ids),
            TimestampType::from_opt_data(last_task_run_at),
            TimestampType::from_opt_data(next_task_scheduled_time),
            StringType::from_data(message),
            TimestampType::from_opt_data(last_updated),
            StringType::from_opt_data(creator),
            TimestampType::from_data(create_time),
        ]))
    }
}

impl BackgroundJobTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("name", TableDataType::String),
            TableField::new("job_type", TableDataType::String.wrap_nullable()),
            TableField::new(
                "scheduled_job_interval_secs",
                TableDataType::Number(NumberDataType::UInt64).wrap_nullable(),
            ),
            TableField::new(
                "scheduled_job_cron_expression",
                TableDataType::String.wrap_nullable(),
            ),
            TableField::new(
                "scheduled_job_cron_timezone",
                TableDataType::String.wrap_nullable(),
            ),
            TableField::new("task_type", TableDataType::String),
            TableField::new("job_state", TableDataType::String.wrap_nullable()),
            TableField::new("last_task_id", TableDataType::String.wrap_nullable()),
            TableField::new("last_task_run_at", TableDataType::Timestamp.wrap_nullable()),
            TableField::new(
                "next_task_scheduled_time",
                TableDataType::Timestamp.wrap_nullable(),
            ),
            TableField::new("message", TableDataType::String),
            TableField::new("last_updated", TableDataType::Timestamp.wrap_nullable()),
            TableField::new("creator", TableDataType::String.wrap_nullable()),
            TableField::new("created_on", TableDataType::Timestamp),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'background_jobs'".to_string(),
            name: "background_jobs".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemBackgroundJobs".to_string(),

                ..Default::default()
            },
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(Self { table_info })
    }
}
