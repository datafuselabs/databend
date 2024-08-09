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

use databend_common_exception::Result;
use databend_common_meta_app::schema::LockType;
use databend_common_pipeline_core::LockGuard;

use crate::table_context::TableContext;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LockTableOption {
    NoLock,
    LockWithRetry,
    LockNoRetry,
}

#[async_trait::async_trait]
pub trait Lock: Sync + Send {
    fn lock_type(&self) -> LockType;

    async fn try_lock(
        &self,
        ctx: Arc<dyn TableContext>,
        should_retry: bool,
    ) -> Result<Option<Arc<LockGuard>>>;
}
