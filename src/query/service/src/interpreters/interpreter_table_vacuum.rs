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

use chrono::Duration;
use databend_common_catalog::table::TableExt;
use databend_common_exception::Result;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_license::license::Feature::Vacuum;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_sql::plans::VacuumTablePlan;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::FUSE_TBL_BLOCK_PREFIX;
use databend_common_storages_fuse::FUSE_TBL_SEGMENT_PREFIX;
use databend_common_storages_fuse::FUSE_TBL_SNAPSHOT_PREFIX;
use databend_common_storages_fuse::FUSE_TBL_XOR_BLOOM_INDEX_PREFIX;
use databend_enterprise_vacuum_handler::get_vacuum_handler;
use opendal::Metakey;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

pub struct VacuumTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: VacuumTablePlan,
}

type FileStat = (u64, u64);

#[derive(Debug, Default)]
struct Statistics {
    pub snapshot_files: FileStat,
    pub segment_files: FileStat,
    pub block_files: FileStat,
    pub index_files: FileStat,
}

impl VacuumTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: VacuumTablePlan) -> Result<Self> {
        Ok(VacuumTableInterpreter { ctx, plan })
    }

    async fn get_statistics(&self, fuse_table: &FuseTable) -> Result<Statistics> {
        let operator = fuse_table.get_operator();
        let table_data_prefix = format!("/{}", fuse_table.meta_location_generator().prefix());

        let mut snapshot_files = (0, 0);
        let mut segment_files = (0, 0);
        let mut block_files = (0, 0);
        let mut index_files = (0, 0);

        let prefix_with_stats = vec![
            (FUSE_TBL_SNAPSHOT_PREFIX, &mut snapshot_files),
            (FUSE_TBL_SEGMENT_PREFIX, &mut segment_files),
            (FUSE_TBL_BLOCK_PREFIX, &mut block_files),
            (FUSE_TBL_XOR_BLOOM_INDEX_PREFIX, &mut index_files),
        ];

        for (dir_prefix, stat) in prefix_with_stats {
            for entry in operator
                .list_with(&format!("{}/{}/", table_data_prefix, dir_prefix))
                .metakey(Metakey::ContentLength)
                .await?
            {
                if entry.metadata().is_file() {
                    stat.0 += 1;
                    stat.1 += entry.metadata().content_length();
                }
            }
        }

        Ok(Statistics {
            snapshot_files,
            segment_files,
            block_files,
            index_files,
        })
    }
}

#[async_trait::async_trait]
impl Interpreter for VacuumTableInterpreter {
    fn name(&self) -> &str {
        "VacuumTableInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Vacuum)?;

        let catalog_name = self.plan.catalog.clone();
        let db_name = self.plan.database.clone();
        let tbl_name = self.plan.table.clone();
        let ctx = self.ctx.clone();
        let table = self
            .ctx
            .get_table(&catalog_name, &db_name, &tbl_name)
            .await?;

        // check mutability
        table.check_mutable()?;

        let duration = Duration::days(ctx.get_settings().get_data_retention_time_in_days()? as i64);

        let retention_time = chrono::Utc::now() - duration;
        let ctx = self.ctx.clone();

        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        let handler = get_vacuum_handler();
        let purge_files_opt = handler
            .do_vacuum(
                fuse_table,
                ctx,
                retention_time,
                self.plan.option.dry_run.is_some(),
            )
            .await?;

        match purge_files_opt {
            None => {
                return {
                    let stat = self.get_statistics(fuse_table).await?;
                    let total_files = stat.snapshot_files.0
                        + stat.segment_files.0
                        + stat.block_files.0
                        + stat.index_files.0;
                    let total_size = stat.snapshot_files.1
                        + stat.segment_files.1
                        + stat.block_files.1
                        + stat.index_files.1;
                    PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
                        UInt64Type::from_data(vec![stat.snapshot_files.0]),
                        UInt64Type::from_data(vec![stat.snapshot_files.1]),
                        UInt64Type::from_data(vec![stat.segment_files.0]),
                        UInt64Type::from_data(vec![stat.segment_files.1]),
                        UInt64Type::from_data(vec![stat.block_files.0]),
                        UInt64Type::from_data(vec![stat.block_files.1]),
                        UInt64Type::from_data(vec![stat.index_files.0]),
                        UInt64Type::from_data(vec![stat.index_files.1]),
                        UInt64Type::from_data(vec![total_files]),
                        UInt64Type::from_data(vec![total_size]),
                    ])])
                };
            }
            Some(purge_files) => {
                let mut file_sizes = vec![];
                let operator = fuse_table.get_operator();
                for file in &purge_files {
                    file_sizes.push(operator.stat(file).await?.content_length());
                }

                // when `purge_files_opt` is some, it means `dry_run` is some, so safe to unwrap()
                if self.plan.option.dry_run.unwrap() {
                    PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
                        UInt64Type::from_data(vec![purge_files.len() as u64]),
                        UInt64Type::from_data(vec![file_sizes.into_iter().sum()]),
                    ])])
                } else {
                    PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
                        StringType::from_data(purge_files),
                        UInt64Type::from_data(file_sizes),
                    ])])
                }
            }
        }
    }
}
