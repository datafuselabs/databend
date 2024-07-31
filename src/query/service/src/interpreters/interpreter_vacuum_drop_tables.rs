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

use std::cmp::min;
use std::collections::HashMap;
use std::sync::Arc;

use chrono::Duration;
use databend_common_catalog::catalog::Catalog;
use databend_common_exception::Result;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_license::license::Feature::Vacuum;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::schema::DroppedId;
use databend_common_meta_app::schema::GcDroppedTableReq;
use databend_common_meta_app::schema::ListDroppedTableReq;
use databend_common_meta_app::schema::TableInfoFilter;
use databend_common_sql::plans::VacuumDropTablePlan;
use databend_enterprise_vacuum_handler::get_vacuum_handler;
use log::info;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;

const DRY_RUN_LIMIT: usize = 1000;

pub struct VacuumDropTablesInterpreter {
    ctx: Arc<QueryContext>,
    plan: VacuumDropTablePlan,
}

impl VacuumDropTablesInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: VacuumDropTablePlan) -> Result<Self> {
        Ok(VacuumDropTablesInterpreter { ctx, plan })
    }

    async fn gc_drop_tables(
        &self,
        catalog: Arc<dyn Catalog>,
        drop_ids: Vec<DroppedId>,
    ) -> Result<()> {
        info!(
            "vacuum drop table from db {:?}, gc_drop_tables",
            self.plan.database,
        );

        let mut drop_db_ids = vec![];
        let mut drop_db_table_ids = vec![];
        for drop_id in drop_ids {
            match drop_id {
                DroppedId::Db(db_id, db_name) => {
                    drop_db_ids.push(DroppedId::Db(db_id, db_name));
                }
                DroppedId::Table(db_id, table_id, table_name) => {
                    drop_db_table_ids.push(DroppedId::Table(db_id, table_id, table_name));
                }
            }
        }

        let chunk_size = 50;

        // first gc drop table ids
        for c in drop_db_table_ids.chunks(chunk_size) {
            info!("vacuum drop {} table ids: {:?}", c.len(), c);
            let req = GcDroppedTableReq {
                tenant: self.ctx.get_tenant(),
                drop_ids: c.to_vec(),
            };
            let _ = catalog.gc_drop_tables(req).await?;
        }

        // then gc drop db ids
        for c in drop_db_ids.chunks(chunk_size) {
            info!("vacuum drop {} db ids: {:?}", c.len(), c);
            let req = GcDroppedTableReq {
                tenant: self.ctx.get_tenant(),
                drop_ids: c.to_vec(),
            };
            let _ = catalog.gc_drop_tables(req).await?;
        }

        Ok(())
    }
}

#[async_trait::async_trait]
impl Interpreter for VacuumDropTablesInterpreter {
    fn name(&self) -> &str {
        "VacuumDropTablesInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Vacuum)?;

        let ctx = self.ctx.clone();
        let duration = Duration::days(ctx.get_settings().get_data_retention_time_in_days()? as i64);

        let retention_time = chrono::Utc::now() - duration;
        let catalog = self.ctx.get_catalog(self.plan.catalog.as_str()).await?;
        info!(
            "vacuum drop table from db {:?}, retention_time: {:?}",
            self.plan.database, retention_time
        );
        // if database if empty, vacuum all tables
        let filter = if self.plan.database.is_empty() {
            TableInfoFilter::AllDroppedTables(Some(retention_time))
        } else {
            TableInfoFilter::Dropped(Some(retention_time))
        };

        let tenant = self.ctx.get_tenant();
        let (tables, drop_ids) = catalog
            .get_drop_table_infos(ListDroppedTableReq {
                inner: DatabaseNameIdent::new(&tenant, &self.plan.database),
                filter,
                limit: self.plan.option.limit,
            })
            .await?;

        info!(
            "vacuum drop table from db {:?}, get_drop_table_infos return tables: {:?}, drop_ids: {:?}",
            self.plan.database,
            tables.len(),
            drop_ids.len()
        );

        // TODO buggy, table as catalog obj should be allowed to drop
        // also drop ids
        // filter out read-only tables
        let tables = tables
            .into_iter()
            .filter(|tbl| !tbl.as_ref().is_read_only())
            .collect::<Vec<_>>();

        let handler = get_vacuum_handler();
        let threads_nums = self.ctx.get_settings().get_max_threads()? as usize;
        let files_opt = handler
            .do_vacuum_drop_tables(
                threads_nums,
                tables,
                if self.plan.option.dry_run.is_some() {
                    Some(DRY_RUN_LIMIT)
                } else {
                    None
                },
            )
            .await?;
        // gc meta data only when not dry run
        if self.plan.option.dry_run.is_none() {
            self.gc_drop_tables(catalog, drop_ids).await?;
        }

        match files_opt {
            None => return Ok(PipelineBuildResult::create()),
            Some(purge_files) => {
                let mut len = min(purge_files.len(), DRY_RUN_LIMIT);
                if let Some(limit) = self.plan.option.limit {
                    len = min(len, limit);
                }
                let purge_files = &purge_files[0..len];
                let mut table_file_sizes = HashMap::new();
                for (table_name, file, file_size) in purge_files {
                    table_file_sizes
                        .entry(table_name)
                        .and_modify(|file_sizes: &mut Vec<(String, u64)>| {
                            file_sizes.push((file.to_string(), *file_size))
                        })
                        .or_insert(vec![(file.to_string(), *file_size)]);
                }

                if let Some(summary) = self.plan.option.dry_run {
                    if summary {
                        let mut tables = vec![];
                        let mut total_files = vec![];
                        let mut total_size = vec![];
                        for (table, file_sizes) in table_file_sizes {
                            tables.push(table.to_string());
                            total_files.push(file_sizes.len() as u64);
                            total_size.push(file_sizes.into_iter().map(|(_, num)| num).sum());
                        }

                        PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
                            StringType::from_data(tables),
                            UInt64Type::from_data(total_files),
                            UInt64Type::from_data(total_size),
                        ])])
                    } else {
                        let mut tables = Vec::with_capacity(len);
                        let mut files = Vec::with_capacity(len);
                        let mut file_size = Vec::with_capacity(len);
                        for (table, file_sizes) in table_file_sizes {
                            for (file, size) in file_sizes {
                                tables.push(table.to_string());
                                files.push(file);
                                file_size.push(size);
                            }
                        }

                        PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
                            StringType::from_data(tables),
                            StringType::from_data(files),
                            UInt64Type::from_data(file_size),
                        ])])
                    }
                } else {
                    unreachable!();
                }
            }
        }
    }
}
