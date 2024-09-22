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

mod config;
mod display;
mod executor;
pub(crate) mod helper;

use std::env;
use std::io::stdin;
use std::io::IsTerminal;
use std::path::Path;

use databend_common_config::Config;
use databend_common_config::InnerConfig;
use databend_common_exception::Result;
use databend_common_license::license_manager::LicenseManager;
use databend_common_license::license_manager::OssLicenseManager;
use databend_common_meta_app::storage::StorageFsConfig;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_embedded::MetaEmbedded;

use crate::clusters::ClusterDiscovery;
use crate::GlobalServices;

pub async fn query_local(query_sql: &str, output_format: &str) -> Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let p = env::var("DATABEND_DATA_PATH");
    let path = match &p {
        Ok(p) => Path::new(p),
        Err(_) => temp_dir.path(),
    };

    env::set_var("META_EMBEDDED_DIR", path.join("_meta"));
    let mut conf: InnerConfig = Config::load(true).unwrap().try_into().unwrap();
    conf.storage.allow_insecure = true;
    conf.storage.params = StorageParams::Fs(StorageFsConfig {
        root: path.join("_data").to_str().unwrap().to_owned(),
    });

    let meta_dir = path.join("_meta");
    MetaEmbedded::init_global_meta_store(meta_dir.to_string_lossy().to_string())
        .await
        .unwrap();

    GlobalServices::init(&conf).await?;
    // init oss license manager
    OssLicenseManager::init(conf.query.tenant_id.tenant_name().to_string()).unwrap();

    // Cluster register.
    ClusterDiscovery::instance().register_to_metastore().await?;

    let is_terminal = stdin().is_terminal();
    let is_repl = is_terminal && query_sql.is_empty();
    let mut executor = executor::SessionExecutor::try_new(is_repl, output_format).await?;

    let query_sql = query_sql.replace("$STDIN", "'fs:///dev/fd/0'");
    executor.handle(&query_sql).await;
    Ok(())
}
