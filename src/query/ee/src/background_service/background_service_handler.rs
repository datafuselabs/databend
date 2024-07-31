// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use arrow_array::RecordBatch;
use databend_common_base::base::tokio::sync::mpsc::Sender;
use databend_common_base::base::tokio::sync::Mutex;
use databend_common_base::base::uuid::Uuid;
use databend_common_base::base::GlobalInstance;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_api::BackgroundApi;
use databend_common_meta_app::background::BackgroundJobIdent;
use databend_common_meta_app::background::BackgroundJobInfo;
use databend_common_meta_app::background::BackgroundJobParams;
use databend_common_meta_app::background::BackgroundJobState;
use databend_common_meta_app::background::CreateBackgroundJobReq;
use databend_common_meta_app::background::GetBackgroundJobReq;
use databend_common_meta_app::background::ManualTriggerParams;
use databend_common_meta_app::background::UpdateBackgroundJobParamsReq;
use databend_common_meta_app::background::UpdateBackgroundJobStatusReq;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_store::MetaStore;
use databend_common_users::UserApiProvider;
use databend_enterprise_background_service::background_service::BackgroundServiceHandlerWrapper;
use databend_enterprise_background_service::BackgroundServiceHandler;
use databend_query::sessions::SessionManager;
use databend_query::sessions::SessionType;
use databend_query::table_functions::SuggestedBackgroundTasksSource;
use log::info;
use log::warn;

use crate::background_service::session::create_session;
use crate::background_service::session::get_background_service_user;
use crate::background_service::CompactionJob;
use crate::background_service::JobScheduler;

pub struct RealBackgroundService {
    conf: InnerConfig,
    scheduler: Arc<JobScheduler>,
    pub meta_api: Arc<MetaStore>,
}

#[async_trait::async_trait]
impl BackgroundServiceHandler for RealBackgroundService {
    #[async_backtrace::framed]
    async fn execute_sql(&self, sql: String) -> Result<Option<RecordBatch>> {
        let session = create_session(&self.conf).await?;
        let ctx = session.create_query_context().await?;
        SuggestedBackgroundTasksSource::do_execute_sql(ctx, sql).await
    }

    #[async_backtrace::framed]
    async fn execute_scheduled_job(
        &self,
        tenant: Tenant,
        user: UserIdentity,
        name: String,
    ) -> Result<()> {
        self.check_license().await?;
        // register the trigger to background job on meta store
        // the consistency level is final consistency, which means that
        // when many execute scheduled job requests are sent to the meta store,
        // only one of them will be executed and the others will be ignored.
        let job_ident = BackgroundJobIdent::new(tenant, name);
        let info = self
            .meta_api
            .get_background_job(GetBackgroundJobReq {
                name: job_ident.clone(),
            })
            .await?;
        let mut params = info.info.job_params.clone().unwrap_or_default();
        let id = Uuid::new_v4().to_string();
        let trigger = user;
        params.manual_trigger_params = Some(ManualTriggerParams::new(id, trigger));
        self.meta_api
            .update_background_job_params(UpdateBackgroundJobParamsReq {
                job_name: job_ident.clone(),
                params,
            })
            .await?;
        if self.conf.background.enable {
            return if let Some(job) = self.scheduler.get_scheduled_job(job_ident.name()) {
                JobScheduler::check_and_run_job(job, true).await
            } else {
                Err(ErrorCode::UnknownBackgroundJob(format!(
                    "background job {} not found",
                    job_ident.name()
                )))
            };
        } else {
            Ok(())
        }
    }
    #[async_backtrace::framed]
    async fn start(&self) -> Result<()> {
        if let Err(e) = self.check_license().await {
            warn!(
                "Background service is only available in enterprise edition. error: {}",
                e
            );
        }

        let scheduler = self.scheduler.clone();
        scheduler.start().await?;
        info!("all jobs finished");
        Ok(())
    }
}

impl RealBackgroundService {
    pub async fn new(conf: &InnerConfig) -> Result<Option<Self>> {
        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let user = get_background_service_user(conf);
        if !conf.background.enable {
            // register default jobs if not exists
            Self::create_compactor_job(
                meta_api.clone(),
                conf,
                BackgroundJobParams::new_one_shot_job(),
                user.identity(),
            )
            .await?;
            return Ok(None);
        }
        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let mut scheduler = JobScheduler::new();
        if conf.background.compaction.enable {
            let compactor_job = RealBackgroundService::get_compactor_job(
                meta_api.clone(),
                conf,
                &user.identity(),
                scheduler.finish_tx.clone(),
            )
            .await?;
            scheduler.add_job(compactor_job).await?;
        }

        let rm = RealBackgroundService {
            conf: conf.clone(),
            scheduler: Arc::new(scheduler),
            meta_api,
        };
        Ok(Some(rm))
    }
    pub fn get_compactor_job_name(tenant: String) -> String {
        let name = format!("{}-compactor-job", tenant);
        name
    }
    pub async fn create_compactor_job(
        meta: Arc<MetaStore>,
        conf: &InnerConfig,
        params: BackgroundJobParams,
        creator: UserIdentity,
    ) -> Result<BackgroundJobIdent> {
        let tenant = conf.query.tenant_id.clone();

        let name = RealBackgroundService::get_compactor_job_name(
            conf.query.tenant_id.tenant_name().to_string(),
        );
        let id = BackgroundJobIdent::new(tenant, name);

        let info = BackgroundJobInfo::new_compactor_job(params, creator);
        meta.create_background_job(CreateBackgroundJobReq {
            if_not_exists: true,
            job_name: id.clone(),
            job_info: info,
        })
        .await?;
        Ok(id)
    }

    async fn get_compactor_job(
        meta: Arc<MetaStore>,
        conf: &InnerConfig,
        creator: &UserIdentity,
        finish_tx: Arc<Mutex<Sender<u64>>>,
    ) -> Result<CompactionJob> {
        let id = RealBackgroundService::create_compactor_job(
            meta.clone(),
            conf,
            conf.background.compaction.params.clone(),
            creator.clone(),
        )
        .await?;
        Self::update_compaction_job_params(meta.clone(), &id, conf).await?;
        Self::suspend_job(meta.clone(), &id, false).await?;

        let job = CompactionJob::create(conf, id.name(), finish_tx).await?;
        Ok(job)
    }

    async fn update_compaction_job_params(
        meta: Arc<MetaStore>,
        id: &BackgroundJobIdent,
        conf: &InnerConfig,
    ) -> Result<()> {
        // create job if not exist
        let info = meta
            .get_background_job(GetBackgroundJobReq { name: id.clone() })
            .await?
            .info;
        if info.job_params.is_some() {
            meta.update_background_job_params(UpdateBackgroundJobParamsReq {
                job_name: id.clone(),
                params: conf.background.compaction.params.clone(),
            })
            .await?;
        }
        Ok(())
    }
    async fn suspend_job(
        meta: Arc<MetaStore>,
        id: &BackgroundJobIdent,
        suspend: bool,
    ) -> Result<BackgroundJobInfo> {
        // create job if not exist
        let info = meta
            .get_background_job(GetBackgroundJobReq { name: id.clone() })
            .await?
            .info;
        if info.job_status.is_some() {
            let mut status = info.job_status.clone().unwrap();
            if suspend {
                status.job_state = BackgroundJobState::SUSPENDED;
            } else {
                status.job_state = BackgroundJobState::RUNNING;
            }
            meta.update_background_job_status(UpdateBackgroundJobStatusReq {
                job_name: id.clone(),
                status,
            })
            .await?;
        }
        let info = meta
            .get_background_job(GetBackgroundJobReq { name: id.clone() })
            .await?
            .info;
        Ok(info)
    }

    pub async fn init(conf: &InnerConfig) -> Result<()> {
        let rm = RealBackgroundService::new(conf).await?;
        if let Some(rm) = rm {
            let wrapper = BackgroundServiceHandlerWrapper::new(Box::new(rm));
            GlobalInstance::set(Arc::new(wrapper));
        }
        Ok(())
    }

    async fn check_license(&self) -> Result<()> {
        let session_manager = SessionManager::create(&self.conf);

        let session = session_manager.create_session(SessionType::Dummy).await?;

        let session = session_manager.register_session(session)?;

        let settings = session.get_settings();

        // check for valid license
        LicenseManagerSwitch::instance().check_enterprise_enabled(
            unsafe { settings.get_enterprise_license().unwrap_or_default() },
            Feature::BackgroundService,
        )
    }
}
