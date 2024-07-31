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

use databend_common_base::base::GlobalInstance;
use databend_common_config::InnerConfig;
use databend_common_exception::Result;
use databend_common_license::license::StorageQuota;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_enterprise_storage_quota::StorageQuotaHandler;
use databend_enterprise_storage_quota::StorageQuotaHandlerWrapper;
use databend_query::sessions::SessionManager;
use databend_query::sessions::SessionType;

pub struct RealStorageQuotaHandler {
    cfg: InnerConfig,
}

#[async_trait::async_trait]
impl StorageQuotaHandler for RealStorageQuotaHandler {
    async fn check_license(&self) -> Result<StorageQuota> {
        let session_manager = SessionManager::create(&self.cfg);

        let session = session_manager.create_session(SessionType::Dummy).await?;

        let session = session_manager.register_session(session)?;

        let settings = session.get_settings();
        // check for valid license
        LicenseManagerSwitch::instance()
            .get_storage_quota(unsafe { settings.get_enterprise_license().unwrap_or_default() })
    }
}

impl RealStorageQuotaHandler {
    pub fn init(cfg: &InnerConfig) -> Result<()> {
        let handler = RealStorageQuotaHandler { cfg: cfg.clone() };
        let wrapper = StorageQuotaHandlerWrapper::new(Box::new(handler));
        GlobalInstance::set(Arc::new(wrapper));
        Ok(())
    }
}
