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
use databend_common_license::license_manager::LicenseManagerSwitch;

use crate::aggregating_index::RealAggregatingIndexHandler;
use crate::data_mask::RealDatamaskHandler;
use crate::inverted_index::RealInvertedIndexHandler;
use crate::license::RealLicenseManager;
use crate::storages::fuse::operations::RealVacuumHandler;
use crate::stream::RealStreamHandler;
use crate::virtual_column::RealVirtualColumnHandler;

pub struct MockServices;

impl MockServices {
    #[async_backtrace::framed]
    pub async fn init(cfg: &InnerConfig, public_key: String) -> Result<()> {
        let rm = RealLicenseManager::new(cfg.query.tenant_id.tenant_name().to_string(), public_key);
        GlobalInstance::set(Arc::new(LicenseManagerSwitch::create(Box::new(rm))));
        RealVacuumHandler::init()?;
        RealAggregatingIndexHandler::init()?;
        RealDatamaskHandler::init()?;
        RealVirtualColumnHandler::init()?;
        RealStreamHandler::init()?;
        RealInvertedIndexHandler::init()?;
        Ok(())
    }
}
