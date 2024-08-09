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
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use databend_common_base::base::GlobalInstance;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_storages_fuse::FuseTable;

// (TableName, file, file size)
pub type VacuumDropFileInfo = (String, String, u64);

#[async_trait::async_trait]
pub trait VacuumHandler: Sync + Send {
    async fn do_vacuum(
        &self,
        fuse_table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        retention_time: DateTime<Utc>,
        dry_run: bool,
    ) -> Result<Option<Vec<String>>>;

    async fn do_vacuum2(
        &self,
        fuse_table: &FuseTable,
        ctx: Arc<dyn TableContext>,
    ) -> Result<Vec<String>>;

    async fn do_vacuum_drop_tables(
        &self,
        threads_nums: usize,
        tables: Vec<Arc<dyn Table>>,
        dry_run_limit: Option<usize>,
    ) -> Result<Option<Vec<VacuumDropFileInfo>>>;

    async fn do_vacuum_temporary_files(
        &self,
        temporary_dir: String,
        retain: Option<Duration>,
        vacuum_limit: usize,
    ) -> Result<usize>;
}

pub struct VacuumHandlerWrapper {
    handler: Box<dyn VacuumHandler>,
}

impl VacuumHandlerWrapper {
    pub fn new(handler: Box<dyn VacuumHandler>) -> Self {
        Self { handler }
    }

    #[async_backtrace::framed]
    pub async fn do_vacuum(
        &self,
        fuse_table: &FuseTable,
        ctx: Arc<dyn TableContext>,
        retention_time: DateTime<Utc>,
        dry_run: bool,
    ) -> Result<Option<Vec<String>>> {
        self.handler
            .do_vacuum(fuse_table, ctx, retention_time, dry_run)
            .await
    }

    #[async_backtrace::framed]
    pub async fn do_vacuum2(
        &self,
        fuse_table: &FuseTable,
        ctx: Arc<dyn TableContext>,
    ) -> Result<Vec<String>> {
        self.handler.do_vacuum2(fuse_table, ctx).await
    }

    #[async_backtrace::framed]
    pub async fn do_vacuum_drop_tables(
        &self,
        threads_nums: usize,
        tables: Vec<Arc<dyn Table>>,
        dry_run_limit: Option<usize>,
    ) -> Result<Option<Vec<VacuumDropFileInfo>>> {
        self.handler
            .do_vacuum_drop_tables(threads_nums, tables, dry_run_limit)
            .await
    }

    #[async_backtrace::framed]
    pub async fn do_vacuum_temporary_files(
        &self,
        temporary_dir: String,
        retain: Option<Duration>,
        vacuum_limit: usize,
    ) -> Result<usize> {
        self.handler
            .do_vacuum_temporary_files(temporary_dir, retain, vacuum_limit)
            .await
    }
}

pub fn get_vacuum_handler() -> Arc<VacuumHandlerWrapper> {
    GlobalInstance::get()
}
