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

use crate::background_service::configs::JobConfig;
use crate::background_service::job::Job;
use common_exception::Result;

#[derive(Clone)]
struct CompactionJob {
    config: JobConfig,
}

#[async_trait]
impl Job for CompactionJob {
    async fn run(&self) {
        do_compaction_job().await?;
    }

    fn get_config(&self) -> &JobConfig {
        &self.config
    }
}

//Service
// optimize table limit
// vacuum
impl CompactionJob {
    pub fn new(config: JobConfig) -> Self {
        Self { config }
    }

    async fn do_compaction_job() -> Result<()> {
        Ok(())
    }
}