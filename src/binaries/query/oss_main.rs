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

#![allow(clippy::uninlined_format_args)]
#![feature(try_blocks)]

mod entry;

use databend_common_base::mem_allocator::GlobalAllocator;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::ThreadTracker;
use databend_common_config::InnerConfig;
use databend_common_exception::Result;
use databend_common_license::license_manager::LicenseManager;
use databend_common_license::license_manager::OssLicenseManager;

use crate::entry::init_services;
use crate::entry::run_cmd;
use crate::entry::start_services;

#[global_allocator]
pub static GLOBAL_ALLOCATOR: GlobalAllocator = GlobalAllocator;

fn main() {
    ThreadTracker::init();

    match Runtime::with_default_worker_threads() {
        Err(cause) => {
            eprintln!("Databend Query start failure, cause: {:?}", cause);
            std::process::exit(cause.code() as i32);
        }
        Ok(rt) => {
            if let Err(cause) = rt.block_on(main_entrypoint()) {
                eprintln!("Databend Query start failure, cause: {:?}", cause);
                std::process::exit(cause.code() as i32);
            }
        }
    }
}

async fn main_entrypoint() -> Result<()> {
    let conf: InnerConfig = InnerConfig::load().await?;
    if run_cmd(&conf).await? {
        return Ok(());
    }

    init_services(conf.clone()).await?;
    // init oss license manager
    OssLicenseManager::init(conf.query.tenant_id.clone())?;
    start_services().await
}
