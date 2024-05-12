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

use databend_common_base::base::GlobalInstance;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_exception::Result;

use crate::configs::Config;

// hold singleton services.
pub struct SharingServices {}

impl SharingServices {
    #[async_backtrace::framed]
    pub async fn init(config: Config) -> Result<()> {
        // init global instance singleton
        GlobalInstance::init_production();

        GlobalIORuntime::init(config.storage.num_cpus as usize)?;

        Ok(())
    }
}
