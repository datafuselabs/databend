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
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_enterprise_storage_encryption::StorageEncryptionHandler;
use databend_enterprise_storage_encryption::StorageEncryptionHandlerWrapper;
use databend_query::sessions::SessionManager;
use databend_query::sessions::SessionType;

pub struct RealStorageEncryptionHandler {
    cfg: InnerConfig,
}

#[async_trait::async_trait]
impl StorageEncryptionHandler for RealStorageEncryptionHandler {
    async fn check_license(&self) -> Result<()> {
        let session_manager = SessionManager::create(&self.cfg);

        let session = session_manager.create_session(SessionType::Dummy).await?;

        let session = session_manager.register_session(session)?;

        let settings = session.get_settings();

        // check for valid license
        LicenseManagerSwitch::instance().check_enterprise_enabled(
            unsafe { settings.get_enterprise_license().unwrap_or_default() },
            Feature::StorageEncryption,
        )
    }
}

impl RealStorageEncryptionHandler {
    pub fn init(cfg: &InnerConfig) -> Result<()> {
        let handler = RealStorageEncryptionHandler { cfg: cfg.clone() };
        let wrapper = StorageEncryptionHandlerWrapper::new(Box::new(handler));
        GlobalInstance::set(Arc::new(wrapper));
        Ok(())
    }
}
