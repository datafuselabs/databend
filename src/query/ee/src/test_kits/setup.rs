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

use databend_common_config::InnerConfig;
use databend_common_exception::Result;
use databend_common_tracing::set_panic_hook;
use databend_query::clusters::ClusterDiscovery;
use databend_query::GlobalServices;
use log::info;

use crate::test_kits::mock_services::MockServices;

pub struct TestFixture;

impl TestFixture {
    pub async fn setup(config: &InnerConfig, public_key: String) -> Result<()> {
        set_panic_hook();
        std::env::set_var("UNIT_TEST", "TRUE");

        #[cfg(debug_assertions)]
        {
            let thread_name = std::thread::current().name().unwrap().to_string();
            databend_common_base::base::GlobalInstance::init_testing(&thread_name);
        }

        GlobalServices::init_with(config).await?;
        MockServices::init(config, public_key).await?;

        // Cluster register.
        {
            ClusterDiscovery::instance().register_to_metastore().await?;
            info!(
                "Databend query has been registered:{:?} to metasrv:{:?}.",
                config.query.cluster_id, config.meta.endpoints
            );
        }

        Ok(())
    }

    /// Teardown the test environment.
    pub async fn teardown() -> Result<()> {
        // Nothing to do.

        Ok(())
    }
}
