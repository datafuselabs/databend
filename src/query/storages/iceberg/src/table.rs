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

use std::any::Any;
use std::sync::Arc;

use arrow_schema::Schema as ArrowSchema;
use async_trait::async_trait;
use chrono::Utc;
use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::catalog::StorageDescription;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartInfo;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_expression::TableSchema;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline_core::Pipeline;
use futures::TryStreamExt;
use tokio::sync::OnceCell;

use crate::partition::IcebergPartInfo;
use crate::table_source::IcebergTableSource;
use crate::IcebergCatalog;

pub const ICEBERG_ENGINE: &str = "ICEBERG";

/// accessor wrapper as a table
#[derive(Clone)]
pub struct IcebergTable {
    info: TableInfo,
    ctl: IcebergCatalog,
    database_name: String,
    table_name: String,

    table: OnceCell<iceberg::table::Table>,
}

impl IcebergTable {
    /// create a new table on the table directory
    #[async_backtrace::framed]
    pub fn try_create(info: TableInfo) -> Result<Box<dyn Table>> {
        let ctl = IcebergCatalog::try_create(info.catalog_info.clone())?;
        let (db_name, table_name) = info.desc.as_str().rsplit_once('.').ok_or_else(|| {
            ErrorCode::BadArguments(format!("Iceberg table desc {} is invalid", &info.desc))
        })?;
        Ok(Box::new(Self {
            info: info.clone(),
            ctl,
            database_name: db_name.to_string(),
            table_name: table_name.to_string(),
            table: OnceCell::new(),
        }))
    }

    pub fn description() -> StorageDescription {
        StorageDescription {
            engine_name: ICEBERG_ENGINE.to_string(),
            comment: "ICEBERG Storage Engine".to_string(),
            support_cluster_key: false,
        }
    }

    pub async fn load_iceberg_table(
        ctl: &IcebergCatalog,
        database: &str,
        table_name: &str,
    ) -> Result<iceberg::table::Table> {
        let db_ident = iceberg::NamespaceIdent::new(database.to_string());
        let table = ctl
            .iceberg_catalog()
            .load_table(&iceberg::TableIdent::new(db_ident, table_name.to_string()))
            .await
            .map_err(|err| {
                ErrorCode::ReadTableDataError(format!("Iceberg catalog load failed: {err:?}"))
            })?;
        Ok(table)
    }

    pub fn get_schema(table: &iceberg::table::Table) -> Result<TableSchema> {
        let meta = table.metadata();

        // Build arrow schema from iceberg metadata.
        let arrow_schema: ArrowSchema = meta.current_schema().as_ref().try_into().map_err(|e| {
            ErrorCode::ReadTableDataError(format!("Cannot convert table metadata: {e:?}"))
        })?;
        TableSchema::try_from(&arrow_schema)
    }

    /// create a new table on the table directory
    #[async_backtrace::framed]
    pub async fn try_create_from_iceberg_catalog(
        ctl: IcebergCatalog,
        database_name: &str,
        table_name: &str,
    ) -> Result<IcebergTable> {
        let table = Self::load_iceberg_table(&ctl, database_name, table_name).await?;
        let table_schema = Self::get_schema(&table)?;

        // construct table info
        let info = TableInfo {
            ident: TableIdent::new(0, 0),
            desc: format!("{database_name}.{table_name}"),
            name: table_name.to_string(),
            meta: TableMeta {
                schema: Arc::new(table_schema),
                engine: "iceberg".to_string(),
                created_on: Utc::now(),
                ..Default::default()
            },
            catalog_info: ctl.info(),
            ..Default::default()
        };

        Ok(Self {
            info,
            ctl,
            database_name: database_name.to_string(),
            table_name: table_name.to_string(),
            table: OnceCell::new_with(Some(table)),
        })
    }

    /// Fetch or init the iceberg table
    pub async fn table(&self) -> Result<&iceberg::table::Table> {
        self.table
            .get_or_try_init(|| async {
                let table =
                    Self::load_iceberg_table(&self.ctl, &self.database_name, &self.table_name)
                        .await
                        .map_err(|err| {
                            ErrorCode::ReadTableDataError(format!(
                                "Iceberg catalog load failed: {err:?}"
                            ))
                        })?;

                Ok(table)
            })
            .await
    }

    pub fn do_read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let parts_len = plan.parts.len();
        let max_threads = ctx.get_settings().get_max_threads()? as usize;
        let max_threads = std::cmp::min(parts_len, max_threads);

        let output_schema = Arc::new(DataSchema::from(plan.schema()));
        pipeline.add_source(
            |output| {
                IcebergTableSource::create(ctx.clone(), output, output_schema.clone(), self.clone())
            },
            max_threads.max(1),
        )
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn do_read_partitions(
        &self,
        _: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        let table = self.table().await?;

        let mut scan = table.scan();

        if let Some(push_downs) = &push_downs {
            if let Some(projection) = &push_downs.projection {
                scan = scan.select(
                    projection
                        .project_schema(&self.schema())
                        .fields
                        .iter()
                        .map(|v| v.name.clone()),
                );
            }
            // TODO: Implement filter based on iceberg-rust's scan builder.
            // if let Some(filter) = &push_downs.filters {}
        }

        let tasks: Vec<_> = scan
            .build()
            .map_err(|err| ErrorCode::Internal(format!("iceberg table scan build: {err:?}")))?
            .plan_files()
            .await
            .map_err(|err| ErrorCode::Internal(format!("iceberg table scan plan: {err:?}")))?
            .try_collect()
            .await
            .map_err(|err| ErrorCode::Internal(format!("iceberg table scan collect: {err:?}")))?;

        let mut read_rows = 0;
        let mut read_bytes = 0;
        let total_files = tasks.len();
        let parts: Vec<_> = tasks
            .into_iter()
            .map(|v: iceberg::scan::FileScanTask| {
                read_rows += v.record_count.unwrap_or_default() as usize;
                read_bytes += v.length as usize;
                Arc::new(Box::new(IcebergPartInfo::new(v)) as Box<dyn PartInfo>)
            })
            .collect();

        Ok((
            PartStatistics::new_estimated(None, read_rows, read_bytes, parts.len(), total_files),
            Partitions::create(PartitionsShuffleKind::Mod, parts),
        ))
    }
}

#[async_trait]
impl Table for IcebergTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn is_local(&self) -> bool {
        false
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.info
    }

    fn name(&self) -> &str {
        &self.get_table_info().name
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        // TODO: we will support dry run later.
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        self.do_read_partitions(ctx, push_downs).await
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        self.do_read_data(ctx, plan, pipeline)
    }

    fn table_args(&self) -> Option<TableArgs> {
        None
    }

    fn support_column_projection(&self) -> bool {
        true
    }

    fn support_prewhere(&self) -> bool {
        true
    }
}
