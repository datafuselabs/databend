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

use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::number::UInt64Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::utils::FromData;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_storages_view::view_table::QUERY;
use databend_common_users::GrantObjectVisibilityChecker;
use databend_common_users::UserApiProvider;
use log::warn;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;
use crate::util::find_eq_filter;

pub struct TablesTable<const WITH_HISTORY: bool> {
    table_info: TableInfo,
}

pub type TablesTableWithHistory = TablesTable<true>;
pub type TablesTableWithoutHistory = TablesTable<false>;

#[async_trait::async_trait]
pub trait HistoryAware {
    const TABLE_NAME: &'static str;
    async fn list_tables(
        catalog: &Arc<dyn Catalog>,
        tenant: &str,
        db_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>>;
}

#[async_trait::async_trait]
impl HistoryAware for TablesTable<true> {
    const TABLE_NAME: &'static str = "tables_with_history";
    #[async_backtrace::framed]
    async fn list_tables(
        catalog: &Arc<dyn Catalog>,
        tenant: &str,
        database_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        catalog.list_tables_history(tenant, database_name).await
    }
}

#[async_trait::async_trait]
impl HistoryAware for TablesTable<false> {
    const TABLE_NAME: &'static str = "tables";
    #[async_backtrace::framed]
    async fn list_tables(
        catalog: &Arc<dyn Catalog>,
        tenant: &str,
        database_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        catalog.list_tables(tenant, database_name).await
    }
}

#[async_trait::async_trait]
impl<const T: bool> AsyncSystemTable for TablesTable<T>
where TablesTable<T>: HistoryAware
{
    const NAME: &'static str = Self::TABLE_NAME;

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        let tenant = ctx.get_tenant();
        let catalog_mgr = CatalogManager::instance();
        let catalogs = catalog_mgr
            .list_catalogs(tenant.name(), ctx.txn_mgr())
            .await?;
        let visibility_checker = ctx.get_visibility_checker().await?;

        Ok(self
            .get_full_data_from_catalogs(ctx, push_downs, catalogs, visibility_checker)
            .await)
    }
}

impl<const T: bool> TablesTable<T>
where TablesTable<T>: HistoryAware
{
    pub fn schema() -> TableSchemaRef {
        TableSchemaRefExt::create(vec![
            TableField::new("catalog", TableDataType::String),
            TableField::new("database", TableDataType::String),
            TableField::new("name", TableDataType::String),
            TableField::new("table_id", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("engine", TableDataType::String),
            TableField::new("engine_full", TableDataType::String),
            TableField::new("cluster_by", TableDataType::String),
            TableField::new("is_transient", TableDataType::String),
            TableField::new("created_on", TableDataType::Timestamp),
            TableField::new(
                "dropped_on",
                TableDataType::Nullable(Box::new(TableDataType::Timestamp)),
            ),
            TableField::new("updated_on", TableDataType::Timestamp),
            TableField::new(
                "num_rows",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "data_size",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "data_compressed_size",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "index_size",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "number_of_segments",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "number_of_blocks",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "owner",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new("comment", TableDataType::String),
            TableField::new("view_query", TableDataType::String),
        ])
    }

    /// dump all the tables from all the catalogs with pushdown, this is used for `SHOW TABLES` command.
    /// please note that this function is intended to not wrapped with Result<>, because we do not want to
    /// break ALL the output on reading ANY of the catalog, database or table failed.
    #[async_backtrace::framed]
    async fn get_full_data_from_catalogs(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
        catalogs: Vec<Arc<dyn Catalog>>,
        visibility_checker: GrantObjectVisibilityChecker,
    ) -> DataBlock {
        let tenant = ctx.get_tenant();

        let ctls: Vec<(String, Arc<dyn Catalog>)> =
            catalogs.iter().map(|e| (e.name(), e.clone())).collect();

        let mut catalogs = vec![];
        let mut databases = vec![];

        let mut database_tables = vec![];
        let mut owner: Vec<Option<String>> = Vec::new();
        let user_api = UserApiProvider::instance();

        for (ctl_name, ctl) in ctls.into_iter() {
            let mut dbs = Vec::new();
            if let Some(push_downs) = &push_downs {
                let mut db_name: Vec<String> = Vec::new();
                if let Some(filter) = push_downs.filters.as_ref().map(|f| &f.filter) {
                    let expr = filter.as_expr(&BUILTIN_FUNCTIONS);
                    find_eq_filter(&expr, &mut |col_name, scalar| {
                        if col_name == "database" {
                            if let Scalar::String(database) = scalar {
                                if !db_name.contains(database) {
                                    db_name.push(database.clone());
                                }
                            }
                        }
                    });
                    for db in db_name {
                        match ctl.get_database(tenant.name(), db.as_str()).await {
                            Ok(database) => dbs.push(database),
                            Err(err) => {
                                let msg = format!("Failed to get database: {}, {}", db, err);
                                warn!("{}", msg);
                                ctx.push_warning(msg);
                            }
                        }
                    }
                }
            }

            if dbs.is_empty() {
                dbs = match ctl.list_databases(&tenant).await {
                    Ok(dbs) => dbs,
                    Err(err) => {
                        let msg =
                            format!("List databases failed on catalog {}: {}", ctl.name(), err);
                        warn!("{}", msg);
                        ctx.push_warning(msg);

                        vec![]
                    }
                }
            }
            let ctl_name: &str = Box::leak(ctl_name.into_boxed_str());

            let final_dbs = dbs
                .into_iter()
                .filter(|db| {
                    visibility_checker.check_database_visibility(
                        ctl_name,
                        db.name(),
                        db.get_db_info().ident.db_id,
                    )
                })
                .collect::<Vec<_>>();

            let ownership = user_api.get_ownerships(&tenant).await.unwrap_or_default();
            for db in final_dbs {
                let name = db.name().to_string().into_boxed_str();
                let db_id = db.get_db_info().ident.db_id;
                let name: &str = Box::leak(name);
                let tables = match Self::list_tables(&ctl, tenant.name(), name).await {
                    Ok(tables) => tables,
                    Err(err) => {
                        // swallow the errors related with remote database or tables, avoid ANY of bad table config corrupt ALL of the results.
                        // these databases might be:
                        // - sharing database
                        // - hive database
                        // - iceberg database
                        // - others
                        // TODO(liyz): return the warnings in the HTTP query protocol.
                        let msg =
                            format!("Failed to list tables in database: {}, {}", db.name(), err);
                        warn!("{}", msg);
                        ctx.push_warning(msg);

                        continue;
                    }
                };

                for table in tables {
                    let table_id = table.get_id();
                    // If db1 is visible, do not means db1.table1 is visible. An user may have a grant about db1.table2, so db1 is visible
                    // for her, but db1.table1 may be not visible. So we need an extra check about table here after db visibility check.
                    if visibility_checker.check_table_visibility(
                        ctl_name,
                        db.name(),
                        table.name(),
                        db_id,
                        table_id,
                    ) && table.engine() != "STREAM"
                    {
                        catalogs.push(ctl_name);
                        databases.push(name);
                        database_tables.push(table);
                        if ownership.is_empty() {
                            owner.push(None);
                        } else {
                            owner.push(
                                ownership
                                    .get(&OwnershipObject::Table {
                                        catalog_name: ctl_name.to_string(),
                                        db_id,
                                        table_id,
                                    })
                                    .map(|role| role.to_string()),
                            );
                        }
                    }
                }
            }
        }

        let mut number_of_blocks: Vec<Option<u64>> = Vec::new();
        let mut number_of_segments: Vec<Option<u64>> = Vec::new();
        let mut num_rows: Vec<Option<u64>> = Vec::new();
        let mut data_size: Vec<Option<u64>> = Vec::new();
        let mut data_compressed_size: Vec<Option<u64>> = Vec::new();
        let mut index_size: Vec<Option<u64>> = Vec::new();

        for tbl in &database_tables {
            let stats = match tbl.table_statistics(ctx.clone(), None).await {
                Ok(stats) => stats,
                Err(err) => {
                    let msg = format!(
                        "Unable to get table statistics on table {}: {}",
                        tbl.name(),
                        err
                    );
                    warn!("{}", msg);
                    ctx.push_warning(msg);

                    None
                }
            };
            num_rows.push(stats.as_ref().and_then(|v| v.num_rows));
            number_of_blocks.push(stats.as_ref().and_then(|v| v.number_of_blocks));
            number_of_segments.push(stats.as_ref().and_then(|v| v.number_of_segments));
            data_size.push(stats.as_ref().and_then(|v| v.data_size));
            data_compressed_size.push(stats.as_ref().and_then(|v| v.data_size_compressed));
            index_size.push(stats.as_ref().and_then(|v| v.index_size));
        }

        let names: Vec<String> = database_tables
            .iter()
            .map(|v| v.name().to_string())
            .collect();
        let table_id: Vec<u64> = database_tables
            .iter()
            .map(|v| v.get_table_info().ident.table_id)
            .collect();
        let engines: Vec<String> = database_tables
            .iter()
            .map(|v| v.engine().to_string())
            .collect();
        let engines_full: Vec<String> = engines.clone();
        let created_on: Vec<i64> = database_tables
            .iter()
            .map(|v| v.get_table_info().meta.created_on.timestamp_micros())
            .collect();
        let dropped_on: Vec<Option<i64>> = database_tables
            .iter()
            .map(|v| {
                v.get_table_info()
                    .meta
                    .drop_on
                    .map(|v| v.timestamp_micros())
            })
            .collect();
        let updated_on = database_tables
            .iter()
            .map(|v| v.get_table_info().meta.updated_on.timestamp_micros())
            .collect::<Vec<_>>();

        let cluster_bys: Vec<String> = database_tables
            .iter()
            .map(|v| {
                v.get_table_info()
                    .meta
                    .default_cluster_key
                    .clone()
                    .unwrap_or_else(|| "".to_owned())
            })
            .collect();
        let is_transient: Vec<String> = database_tables
            .iter()
            .map(|v| {
                if v.options().contains_key("TRANSIENT") {
                    "TRANSIENT".to_string()
                } else {
                    "".to_string()
                }
            })
            .collect();
        let comment: Vec<String> = database_tables
            .iter()
            .map(|v| v.get_table_info().meta.comment.clone())
            .collect();

        let view_query: Vec<String> = database_tables
            .iter()
            .map(|v| -> String {
                let tbl_info = v.get_table_info();
                match tbl_info.engine() {
                    "VIEW" => {
                        let query = tbl_info.options().get(QUERY);
                        match query {
                            Some(query) => query.clone(),
                            None => String::from(""),
                        }
                    }
                    _ => String::from(""),
                }
            })
            .collect();

        DataBlock::new_from_columns(vec![
            StringType::from_data(catalogs),
            StringType::from_data(databases),
            StringType::from_data(names),
            UInt64Type::from_data(table_id),
            StringType::from_data(engines),
            StringType::from_data(engines_full),
            StringType::from_data(cluster_bys),
            StringType::from_data(is_transient),
            TimestampType::from_data(created_on),
            TimestampType::from_opt_data(dropped_on),
            TimestampType::from_data(updated_on),
            UInt64Type::from_opt_data(num_rows),
            UInt64Type::from_opt_data(data_size),
            UInt64Type::from_opt_data(data_compressed_size),
            UInt64Type::from_opt_data(index_size),
            UInt64Type::from_opt_data(number_of_segments),
            UInt64Type::from_opt_data(number_of_blocks),
            StringType::from_opt_data(owner),
            StringType::from_data(comment),
            StringType::from_data(view_query),
        ])
    }

    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let name = Self::TABLE_NAME;
        let table_info = TableInfo {
            desc: format!("'system'.'{name}'"),
            name: Self::NAME.to_owned(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema: TablesTable::<T>::schema(),
                engine: "SystemTables".to_string(),

                ..Default::default()
            },
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(TablesTable::<T> { table_info })
    }
}
