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

use backoff::backoff::Backoff;
use backoff::ExponentialBackoff;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_storages_fuse::TableContext;
use databend_storages_common_txn::TxnManagerRef;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::PipelineBuilder;
use crate::sessions::QueryContext;
pub struct CommitInterpreter {
    ctx: Arc<QueryContext>,
}

impl CommitInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>) -> Result<Self> {
        Ok(Self { ctx })
    }
}

#[async_trait::async_trait]
impl Interpreter for CommitInterpreter {
    fn name(&self) -> &str {
        "CommitInterpreter"
    }

    fn is_txn_command(&self) -> bool {
        true
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        // After commit statement, current session should be in auto commit mode, no matter update meta success or not.
        // Use this guard to clear txn manager before return.
        let _guard = ClearTxnManagerGuard(self.ctx.txn_mgr().clone());
        let is_active = self.ctx.txn_mgr().lock().is_active();
        if is_active {
            try_commit_repeatedly(self.ctx.clone())?;
            let need_purge_files = self.ctx.txn_mgr().lock().need_purge_files();
            for (stage_info, files) in need_purge_files {
                PipelineBuilder::try_purge_files(self.ctx.clone(), &stage_info, &files).await;
            }
        }
        Ok(PipelineBuildResult::create())
    }
}

struct ClearTxnManagerGuard(TxnManagerRef);

impl Drop for ClearTxnManagerGuard {
    fn drop(&mut self) {
        self.0.lock().clear();
    }
}

fn try_commit_repeatedly(ctx: Arc<QueryContext>) -> Result<()> {
    let catalog = ctx.get_default_catalog()?;
    let req = ctx.txn_mgr().lock().req();
    let backoff = ExponentialBackoff::default();

    loop {
        match catalog.update_multi_table_meta(req).await {
            Ok(_) => break,
            Err(e) if e.code() != ErrorCode::TABLE_VERSION_MISMATCHED => return Err(e),
            Err(_) => {
                todo!()
            }
        }
    }
    Ok(())
}
