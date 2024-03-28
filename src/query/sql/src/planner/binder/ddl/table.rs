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

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_ast::ast::AddColumnOption as AstAddColumnOption;
use databend_common_ast::ast::AlterTableAction;
use databend_common_ast::ast::AlterTableStmt;
use databend_common_ast::ast::AnalyzeTableStmt;
use databend_common_ast::ast::AttachTableStmt;
use databend_common_ast::ast::ColumnDefinition;
use databend_common_ast::ast::ColumnExpr;
use databend_common_ast::ast::CompactTarget;
use databend_common_ast::ast::CreateTableSource;
use databend_common_ast::ast::CreateTableStmt;
use databend_common_ast::ast::DescribeTableStmt;
use databend_common_ast::ast::DropTableStmt;
use databend_common_ast::ast::Engine;
use databend_common_ast::ast::ExistsTableStmt;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::ModifyColumnAction;
use databend_common_ast::ast::OptimizeTableAction as AstOptimizeTableAction;
use databend_common_ast::ast::OptimizeTableStmt;
use databend_common_ast::ast::RenameTableStmt;
use databend_common_ast::ast::ShowCreateTableStmt;
use databend_common_ast::ast::ShowDropTablesStmt;
use databend_common_ast::ast::ShowLimit;
use databend_common_ast::ast::ShowTablesStatusStmt;
use databend_common_ast::ast::ShowTablesStmt;
use databend_common_ast::ast::Statement;
use databend_common_ast::ast::TableReference;
use databend_common_ast::ast::TruncateTableStmt;
use databend_common_ast::ast::TypeName;
use databend_common_ast::ast::UndropTableStmt;
use databend_common_ast::ast::UriLocation;
use databend_common_ast::ast::VacuumDropTableStmt;
use databend_common_ast::ast::VacuumTableStmt;
use databend_common_ast::ast::VacuumTemporaryFiles;
use databend_common_ast::parser::parse_sql;
use databend_common_ast::parser::tokenize_sql;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::infer_schema_type;
use databend_common_expression::infer_table_schema;
use databend_common_expression::types::DataType;
use databend_common_expression::ComputedExpr;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::storage::StorageParams;
use databend_common_storage::DataOperator;
use databend_common_storages_delta::DeltaTable;
use databend_common_storages_iceberg::IcebergTable;
use databend_common_storages_view::view_table::QUERY;
use databend_common_storages_view::view_table::VIEW_ENGINE;
use databend_storages_common_table_meta::table::is_reserved_opt_key;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use databend_storages_common_table_meta::table::OPT_KEY_ENGINE_META;
use databend_storages_common_table_meta::table::OPT_KEY_STORAGE_FORMAT;
use databend_storages_common_table_meta::table::OPT_KEY_STORAGE_PREFIX;
use databend_storages_common_table_meta::table::OPT_KEY_TABLE_ATTACHED_DATA_URI;
use databend_storages_common_table_meta::table::OPT_KEY_TABLE_COMPRESSION;
use derive_visitor::DriveMut;
use log::debug;
use log::error;

use crate::binder::get_storage_params_from_options;
use crate::binder::parse_storage_params_from_uri;
use crate::binder::scalar::ScalarBinder;
use crate::binder::Binder;
use crate::binder::ColumnBindingBuilder;
use crate::binder::Visibility;
use crate::optimizer::optimize;
use crate::optimizer::OptimizerContext;
use crate::parse_computed_expr_to_string;
use crate::parse_default_expr_to_string;
use crate::planner::semantic::normalize_identifier;
use crate::planner::semantic::resolve_type_name;
use crate::planner::semantic::IdentifierNormalizer;
use crate::plans::AddColumnOption;
use crate::plans::AddTableColumnPlan;
use crate::plans::AlterTableClusterKeyPlan;
use crate::plans::AnalyzeTablePlan;
use crate::plans::CreateTablePlan;
use crate::plans::DescribeTablePlan;
use crate::plans::DropTableClusterKeyPlan;
use crate::plans::DropTableColumnPlan;
use crate::plans::DropTablePlan;
use crate::plans::ExistsTablePlan;
use crate::plans::ModifyColumnAction as ModifyColumnActionInPlan;
use crate::plans::ModifyTableColumnPlan;
use crate::plans::OptimizeTableAction;
use crate::plans::OptimizeTablePlan;
use crate::plans::Plan;
use crate::plans::ReclusterTablePlan;
use crate::plans::RenameTableColumnPlan;
use crate::plans::RenameTablePlan;
use crate::plans::RevertTablePlan;
use crate::plans::RewriteKind;
use crate::plans::SetOptionsPlan;
use crate::plans::ShowCreateTablePlan;
use crate::plans::TruncateTablePlan;
use crate::plans::UndropTablePlan;
use crate::plans::VacuumDropTableOption;
use crate::plans::VacuumDropTablePlan;
use crate::plans::VacuumTableOption;
use crate::plans::VacuumTablePlan;
use crate::plans::VacuumTemporaryFilesPlan;
use crate::BindContext;
use crate::Planner;
use crate::SelectBuilder;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_tables(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &ShowTablesStmt,
    ) -> Result<Plan> {
        let ShowTablesStmt {
            catalog,
            database,
            full,
            limit,
            with_history,
        } = stmt;

        let database = self.check_database_exist(catalog, database).await?;

        let mut select_builder = if stmt.with_history {
            SelectBuilder::from("system.tables_with_history")
        } else {
            SelectBuilder::from("system.tables")
        };

        if *full {
            select_builder
                .with_column("name AS Tables")
                .with_column("'BASE TABLE' AS Table_type")
                .with_column("database AS Database")
                .with_column("catalog AS Catalog")
                .with_column("owner")
                .with_column("engine")
                .with_column("cluster_by AS Cluster_by")
                .with_column("created_on AS create_time");
            if *with_history {
                select_builder.with_column("dropped_on AS drop_time");
            }

            select_builder
                .with_column("num_rows")
                .with_column("data_size")
                .with_column("data_compressed_size")
                .with_column("index_size");
        } else {
            select_builder.with_column(format!("name AS `Tables_in_{database}`"));
            if *with_history {
                select_builder.with_column("dropped_on AS drop_time");
            };
        }

        select_builder
            .with_order_by("catalog")
            .with_order_by("database")
            .with_order_by("name");

        select_builder.with_filter(format!("database = '{database}'"));

        let catalog_name = match catalog {
            None => self.ctx.get_current_catalog(),
            Some(ident) => {
                let catalog = normalize_identifier(ident, &self.name_resolution_ctx).name;
                self.ctx.get_catalog(&catalog).await?;
                catalog
            }
        };

        select_builder.with_filter(format!("catalog = '{catalog_name}'"));
        let query = match limit {
            None => select_builder.build(),
            Some(ShowLimit::Like { pattern }) => {
                select_builder.with_filter(format!("name LIKE '{pattern}'"));
                select_builder.build()
            }
            Some(ShowLimit::Where { selection }) => {
                select_builder.with_filter(format!("({selection})"));
                select_builder.build()
            }
        };
        debug!("show tables rewrite to: {:?}", query);
        self.bind_rewrite_to_query(
            bind_context,
            query.as_str(),
            RewriteKind::ShowTables(catalog_name, database),
        )
        .await
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_create_table(
        &mut self,
        stmt: &ShowCreateTableStmt,
    ) -> Result<Plan> {
        let ShowCreateTableStmt {
            catalog,
            database,
            table,
        } = stmt;

        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        let schema = DataSchemaRefExt::create(vec![
            DataField::new("Table", DataType::String),
            DataField::new("Create Table", DataType::String),
        ]);
        Ok(Plan::ShowCreateTable(Box::new(ShowCreateTablePlan {
            catalog,
            database,
            table,
            schema,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_describe_table(
        &mut self,
        stmt: &DescribeTableStmt,
    ) -> Result<Plan> {
        let DescribeTableStmt {
            catalog,
            database,
            table,
        } = stmt;

        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("Field", DataType::String),
            DataField::new("Type", DataType::String),
            DataField::new("Null", DataType::String),
            DataField::new("Default", DataType::String),
            DataField::new("Extra", DataType::String),
        ]);

        Ok(Plan::DescribeTable(Box::new(DescribeTablePlan {
            catalog,
            database,
            table,
            schema,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_tables_status(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &ShowTablesStatusStmt,
    ) -> Result<Plan> {
        let ShowTablesStatusStmt { database, limit } = stmt;

        let database = self.check_database_exist(&None, database).await?;

        let select_cols = "name AS Name, engine AS Engine, 0 AS Version, \
        NULL AS Row_format, num_rows AS Rows, NULL AS Avg_row_length, data_size AS Data_length, \
        NULL AS Max_data_length, index_size AS Index_length, NULL AS Data_free, NULL AS Auto_increment, \
        created_on AS Create_time, NULL AS Update_time, NULL AS Check_time, NULL AS Collation, \
        NULL AS Checksum, '' AS Comment, cluster_by as Cluster_by"
            .to_string();

        // Use `system.tables` AS the "base" table to construct the result-set of `SHOW TABLE STATUS ..`
        //
        // To constraint the schema of the final result-set,
        //  `(select ${select_cols} from system.tables where ..)`
        // is used AS a derived table.
        // (unlike mysql, alias of derived table is not required in databend).
        let query = match limit {
            None => format!(
                "SELECT {} FROM system.tables WHERE database = '{}' ORDER BY Name",
                select_cols, database
            ),
            Some(ShowLimit::Like { pattern }) => format!(
                "SELECT * from (SELECT {} FROM system.tables WHERE database = '{}') \
            WHERE Name LIKE '{}' ORDER BY Name",
                select_cols, database, pattern
            ),
            Some(ShowLimit::Where { selection }) => format!(
                "SELECT * from (SELECT {} FROM system.tables WHERE database = '{}') \
            WHERE ({}) ORDER BY Name",
                select_cols, database, selection
            ),
        };

        let tokens = tokenize_sql(query.as_str())?;
        let (stmt, _) = parse_sql(&tokens, self.dialect)?;
        self.bind_statement(bind_context, &stmt).await
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_drop_tables(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &ShowDropTablesStmt,
    ) -> Result<Plan> {
        let ShowDropTablesStmt { database, limit } = stmt;

        let database = self.check_database_exist(&None, database).await?;

        let mut select_builder = SelectBuilder::from("system.tables_with_history");

        select_builder
            .with_column("name AS Tables")
            .with_column("'BASE TABLE' AS Table_type")
            .with_column("database AS Database")
            .with_column("catalog AS Catalog")
            .with_column("engine")
            .with_column("created_on AS create_time");
        select_builder.with_column("dropped_on AS drop_time");

        select_builder
            .with_column("num_rows")
            .with_column("data_size")
            .with_column("data_compressed_size")
            .with_column("index_size");

        select_builder
            .with_order_by("catalog")
            .with_order_by("database")
            .with_order_by("name");

        select_builder.with_filter(format!("database = '{database}'"));
        select_builder.with_filter("dropped_on IS NOT NULL".to_string());

        let query = match limit {
            None => select_builder.build(),
            Some(ShowLimit::Like { pattern }) => {
                select_builder.with_filter(format!("name LIKE '{pattern}'"));
                select_builder.build()
            }
            Some(ShowLimit::Where { selection }) => {
                select_builder.with_filter(format!("({selection})"));
                select_builder.build()
            }
        };

        debug!("show drop tables rewrite to: {:?}", query);
        let catalog = self.ctx.get_current_catalog();
        self.bind_rewrite_to_query(
            bind_context,
            query.as_str(),
            RewriteKind::ShowTables(catalog, database),
        )
        .await
    }

    #[async_backtrace::framed]
    pub async fn check_database_exist(
        &mut self,
        catalog: &Option<Identifier>,
        database: &Option<Identifier>,
    ) -> Result<String> {
        let ctl_name = match catalog {
            Some(ctl) => ctl.to_string(),
            None => self.ctx.get_current_catalog(),
        };
        match database {
            None => Ok(self.ctx.get_current_database()),
            Some(ident) => {
                let database = normalize_identifier(ident, &self.name_resolution_ctx).name;
                self.ctx
                    .get_catalog(&ctl_name)
                    .await?
                    .get_database(self.ctx.get_tenant().name(), &database)
                    .await?;
                Ok(database)
            }
        }
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_table(
        &mut self,
        stmt: &CreateTableStmt,
    ) -> Result<Plan> {
        let CreateTableStmt {
            create_option,
            catalog,
            database,
            table,
            source,
            table_options,
            cluster_by,
            as_query,
            transient,
            engine,
            uri_location,
        } = stmt;

        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        // Take FUSE engine AS default engine
        let engine = engine.unwrap_or(Engine::Fuse);
        let mut options: BTreeMap<String, String> = BTreeMap::new();
        let mut engine_options: BTreeMap<String, String> = BTreeMap::new();
        for table_option in table_options.iter() {
            self.insert_table_option_with_validation(
                &mut options,
                table_option.0.to_lowercase(),
                table_option.1.to_string(),
            )?;
        }

        let (mut storage_params, part_prefix) = match (uri_location, engine) {
            (Some(uri), Engine::Fuse) => {
                let mut uri = UriLocation {
                    protocol: uri.protocol.clone(),
                    name: uri.name.clone(),
                    path: uri.path.clone(),
                    part_prefix: uri.part_prefix.clone(),
                    connection: uri.connection.clone(),
                };
                let sp = parse_storage_params_from_uri(
                    &mut uri,
                    Some(self.ctx.as_ref()),
                    "when create TABLE with external location",
                )
                .await?;

                // create a temporary op to check if params is correct
                DataOperator::try_create(&sp).await?;

                // Path ends with "/" means it's a directory.
                let fp = if uri.path.ends_with('/') {
                    uri.part_prefix.clone()
                } else {
                    "".to_string()
                };

                (Some(sp), fp)
            }
            (Some(uri), _) => Err(ErrorCode::BadArguments(format!(
                "Incorrect CREATE query: CREATE TABLE with external location is only supported for FUSE engine, but got {:?} for {:?}",
                engine, uri
            )))?,
            _ => (None, "".to_string()),
        };

        // If table is TRANSIENT, set a flag in table option
        if *transient {
            options.insert("TRANSIENT".to_owned(), "T".to_owned());
        }

        // todo(geometry): remove this when geometry stable.
        if let Some(CreateTableSource::Columns(cols)) = &source {
            if cols
                .iter()
                .any(|col| matches!(col.data_type, TypeName::Geometry))
                && !self.ctx.get_settings().get_enable_geo_create_table()?
            {
                return Err(ErrorCode::GeometryError(
                    "Create table using the geometry type is an experimental feature. \
                    You can `set enable_geo_create_table=1` to use this feature. \
                    We do not guarantee its compatibility until we doc this feature.",
                ));
            }
        }

        // Build table schema
        let (schema, field_comments) = match (&source, &as_query) {
            (Some(source), None) => {
                // `CREATE TABLE` without `AS SELECT ...`
                self.analyze_create_table_schema(source).await?
            }
            (None, Some(query)) => {
                // `CREATE TABLE AS SELECT ...` without column definitions
                let mut init_bind_context = BindContext::new();
                let (_, bind_context) = self.bind_query(&mut init_bind_context, query).await?;
                let fields = bind_context
                    .columns
                    .iter()
                    .map(|column_binding| {
                        Ok(TableField::new(
                            &column_binding.column_name,
                            infer_schema_type(&column_binding.data_type)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let schema = TableSchemaRefExt::create(fields);
                Self::validate_create_table_schema(&schema)?;
                (schema, vec![])
            }
            (Some(source), Some(query)) => {
                // e.g. `CREATE TABLE t (i INT) AS SELECT * from old_t` with columns specified
                let (source_schema, source_comments) =
                    self.analyze_create_table_schema(source).await?;
                let mut init_bind_context = BindContext::new();
                let (_, bind_context) = self.bind_query(&mut init_bind_context, query).await?;
                let query_fields: Vec<TableField> = bind_context
                    .columns
                    .iter()
                    .map(|column_binding| {
                        Ok(TableField::new(
                            &column_binding.column_name,
                            infer_schema_type(&column_binding.data_type)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                if source_schema.fields().len() != query_fields.len() {
                    return Err(ErrorCode::BadArguments("Number of columns does not match"));
                }
                Self::validate_create_table_schema(&source_schema)?;
                (source_schema, source_comments)
            }
            _ => {
                match engine {
                    Engine::Iceberg => {
                        let sp =
                            get_storage_params_from_options(self.ctx.as_ref(), &options).await?;
                        let dop = DataOperator::try_new(&sp)?;
                        let table = IcebergTable::load_iceberg_table(dop).await?;
                        let table_schema = IcebergTable::get_schema(&table).await?;
                        // the first version of current iceberg table do not need to persist the storage_params,
                        // since we get it from table options location and connection when load table each time.
                        // we do this in case we change this idea.
                        storage_params = Some(sp);
                        (Arc::new(table_schema), vec![])
                    }
                    Engine::Delta => {
                        let sp =
                            get_storage_params_from_options(self.ctx.as_ref(), &options).await?;
                        let table = DeltaTable::load(&sp).await?;
                        let (table_schema, meta) = DeltaTable::get_meta(&table).await?;
                        // the first version of current iceberg table do not need to persist the storage_params,
                        // since we get it from table options location and connection when load table each time.
                        // we do this in case we change this idea.
                        storage_params = Some(sp);
                        engine_options.insert(OPT_KEY_ENGINE_META.to_lowercase().to_string(), meta);
                        (Arc::new(table_schema), vec![])
                    }
                    _ => Err(ErrorCode::BadArguments(
                        "Incorrect CREATE query: required list of column descriptions or AS section or SELECT or ICEBERG/DELTA table engine",
                    ))?,
                }
            }
        };

        // for fuse engine, we will insert database_id, so if we check it in execute phase,
        // we can't distinct user key and our internal key.
        if options.contains_key(&OPT_KEY_DATABASE_ID.to_lowercase()) {
            error!("invalid opt for fuse table in create table statement");
            return Err(ErrorCode::TableOptionInvalid(format!(
                "table option {} is invalid for create table statement",
                OPT_KEY_DATABASE_ID
            )));
        }

        if engine == Engine::Fuse {
            // Currently, [Table] can not accesses its database id yet, thus
            // here we keep the db id AS an entry of `table_meta.options`.
            //
            // To make the unit/stateless test cases (`show create ..`) easier,
            // here we care about the FUSE engine only.
            //
            // Later, when database id is kept, let say in `TableInfo`, we can
            // safely eliminate this "FUSE" constant and the table meta option entry.
            let catalog = self.ctx.get_catalog(&catalog).await?;
            let db = catalog
                .get_database(self.ctx.get_tenant().name(), &database)
                .await?;
            let db_id = db.get_db_info().ident.db_id;
            options.insert(OPT_KEY_DATABASE_ID.to_owned(), db_id.to_string());

            let config = GlobalConfig::instance();
            let is_blocking_fs = matches!(
                storage_params.as_ref().unwrap_or(&config.storage.params),
                StorageParams::Fs(_)
            );

            // we should persist the storage format and compression type instead of using the default value in fuse table
            if !options.contains_key(OPT_KEY_STORAGE_FORMAT) {
                let default_storage_format = match config.query.default_storage_format.as_str() {
                    "" | "auto" => {
                        if is_blocking_fs {
                            "native"
                        } else {
                            "parquet"
                        }
                    }
                    _ => config.query.default_storage_format.as_str(),
                };
                options.insert(
                    OPT_KEY_STORAGE_FORMAT.to_owned(),
                    default_storage_format.to_owned(),
                );
            }

            if !options.contains_key(OPT_KEY_TABLE_COMPRESSION) {
                let default_compression = match config.query.default_compression.as_str() {
                    "" | "auto" => {
                        if is_blocking_fs {
                            "lz4"
                        } else {
                            "zstd"
                        }
                    }
                    _ => config.query.default_compression.as_str(),
                };
                options.insert(
                    OPT_KEY_TABLE_COMPRESSION.to_owned(),
                    default_compression.to_owned(),
                );
            }
        }

        let cluster_key = {
            let keys = self
                .analyze_cluster_keys(cluster_by, schema.clone())
                .await?;
            if keys.is_empty() {
                None
            } else {
                Some(format!("({})", keys.join(", ")))
            }
        };

        let plan = CreateTablePlan {
            create_option: *create_option,
            tenant: self.ctx.get_tenant().name().to_string(),
            catalog: catalog.clone(),
            database: database.clone(),
            table,
            schema: schema.clone(),
            engine,
            engine_options,
            storage_params,
            read_only_attach: false,
            part_prefix,
            options,
            field_comments,
            cluster_key,
            as_select: if let Some(query) = as_query {
                let mut bind_context = BindContext::new();
                let stmt = Statement::Query(Box::new(*query.clone()));
                let select_plan = self.bind_statement(&mut bind_context, &stmt).await?;
                // Don't enable distributed optimization for `CREATE TABLE ... AS SELECT ...` for now
                let opt_ctx = OptimizerContext::new(self.ctx.clone(), self.metadata.clone());
                let optimized_plan = optimize(opt_ctx, select_plan).await?;
                Some(Box::new(optimized_plan))
            } else {
                None
            },
        };
        Ok(Plan::CreateTable(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_attach_table(
        &mut self,
        stmt: &AttachTableStmt,
    ) -> Result<Plan> {
        let (catalog, database, table) =
            self.normalize_object_identifier_triple(&stmt.catalog, &stmt.database, &stmt.table);

        let mut path = stmt.uri_location.path.clone();
        // First, to make it easy for users to use, path = "/testbucket/admin/data/1/2" and path = "/testbucket/admin/data/1/2/" are both legal
        // So we need to remove the last "/"
        if path.ends_with('/') {
            path.pop();
        }
        // Then, split the path into two parts, the first part is the root, and the second part is the storage_prefix
        // For example, path = "/testbucket/admin/data/1/2", then root = "/testbucket/admin/data/", storage_prefix = "1/2"
        // root is used by OpenDAL operator, storage_prefix is used to specify the storage location of the table
        // Note that the root must end with "/", and the storage_prefix must not start or end with "/"
        let mut parts = path.split('/').collect::<Vec<_>>();
        if parts.len() < 2 {
            return Err(ErrorCode::BadArguments(format!(
                "Invalid path: {}",
                stmt.uri_location
            )));
        }
        let storage_prefix = parts.split_off(parts.len() - 2).join("/");
        let root = format!("{}/", parts.join("/"));
        let mut options = BTreeMap::new();
        options.insert(OPT_KEY_STORAGE_PREFIX.to_string(), storage_prefix);

        // keep a copy of table data uri_location, will be used in "show create table"
        options.insert(
            OPT_KEY_TABLE_ATTACHED_DATA_URI.to_string(),
            format!("{:#}", stmt.uri_location),
        );

        let mut uri = stmt.uri_location.clone();
        uri.path = root;
        let sp =
            parse_storage_params_from_uri(&mut uri, Some(self.ctx.as_ref()), "when ATTACH TABLE")
                .await?;

        // create a temporary op to check if params is correct
        DataOperator::try_create(&sp).await?;

        // Path ends with "/" means it's a directory.
        let part_prefix = if uri.path.ends_with('/') {
            uri.part_prefix.clone()
        } else {
            "".to_string()
        };

        Ok(Plan::CreateTable(Box::new(CreateTablePlan {
            create_option: CreateOption::Create,
            tenant: self.ctx.get_tenant().name().to_string(),
            catalog,
            database,
            table,
            schema: Arc::new(TableSchema::default()),
            engine: Engine::Fuse,
            engine_options: BTreeMap::new(),
            storage_params: Some(sp),
            read_only_attach: stmt.read_only,
            part_prefix,
            options,
            field_comments: vec![],
            cluster_key: None,
            as_select: None,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_table(
        &mut self,
        stmt: &DropTableStmt,
    ) -> Result<Plan> {
        let DropTableStmt {
            if_exists,
            catalog,
            database,
            table,
            all,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        Ok(Plan::DropTable(Box::new(DropTablePlan {
            if_exists: *if_exists,
            tenant,
            catalog,
            database,
            table,
            all: *all,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_undrop_table(
        &mut self,
        stmt: &UndropTableStmt,
    ) -> Result<Plan> {
        let UndropTableStmt {
            catalog,
            database,
            table,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        Ok(Plan::UndropTable(Box::new(UndropTablePlan {
            tenant: tenant.name().to_string(),
            catalog,
            database,
            table,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_alter_table(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &AlterTableStmt,
    ) -> Result<Plan> {
        let AlterTableStmt {
            if_exists,
            table_reference,
            action,
        } = stmt;

        let tenant = self.ctx.get_tenant();

        let (catalog, database, table) = if let TableReference::Table {
            catalog,
            database,
            table,
            ..
        } = table_reference
        {
            self.normalize_object_identifier_triple(catalog, database, table)
        } else {
            return Err(ErrorCode::Internal(
                "should not happen, parser should have report error already",
            ));
        };

        match action {
            AlterTableAction::RenameTable { new_table } => {
                Ok(Plan::RenameTable(Box::new(RenameTablePlan {
                    tenant: tenant.name().to_string(),
                    if_exists: *if_exists,
                    new_database: database.clone(),
                    new_table: normalize_identifier(new_table, &self.name_resolution_ctx).name,
                    catalog,
                    database,
                    table,
                })))
            }
            AlterTableAction::RenameColumn {
                old_column,
                new_column,
            } => {
                let schema = self
                    .ctx
                    .get_table(&catalog, &database, &table)
                    .await?
                    .schema();
                let (new_schema, old_column, new_column) = self
                    .analyze_rename_column(old_column, new_column, schema)
                    .await?;
                Ok(Plan::RenameTableColumn(Box::new(RenameTableColumnPlan {
                    tenant: self.ctx.get_tenant().name().to_string(),
                    catalog,
                    database,
                    table,
                    schema: new_schema,
                    old_column,
                    new_column,
                })))
            }
            AlterTableAction::AddColumn {
                column,
                option: ast_option,
            } => {
                let schema = self
                    .ctx
                    .get_table(&catalog, &database, &table)
                    .await?
                    .schema();
                let (field, comment) = self.analyze_add_column(column, schema).await?;
                let option = match ast_option {
                    AstAddColumnOption::First => AddColumnOption::First,
                    AstAddColumnOption::After(ident) => AddColumnOption::After(
                        normalize_identifier(ident, &self.name_resolution_ctx).name,
                    ),
                    AstAddColumnOption::End => AddColumnOption::End,
                };
                Ok(Plan::AddTableColumn(Box::new(AddTableColumnPlan {
                    tenant: self.ctx.get_tenant().name().to_string(),
                    catalog,
                    database,
                    table,
                    field,
                    comment,
                    option,
                })))
            }
            AlterTableAction::ModifyColumn { action } => {
                let action_in_plan = match action {
                    ModifyColumnAction::SetMaskingPolicy(column, name) => {
                        ModifyColumnActionInPlan::SetMaskingPolicy(
                            column.to_string(),
                            name.to_string(),
                        )
                    }
                    ModifyColumnAction::UnsetMaskingPolicy(column) => {
                        ModifyColumnActionInPlan::UnsetMaskingPolicy(column.to_string())
                    }
                    ModifyColumnAction::ConvertStoredComputedColumn(column) => {
                        ModifyColumnActionInPlan::ConvertStoredComputedColumn(column.to_string())
                    }
                    ModifyColumnAction::SetDataType(column_def_vec) => {
                        let mut field_and_comment = Vec::with_capacity(column_def_vec.len());
                        let schema = self
                            .ctx
                            .get_table(&catalog, &database, &table)
                            .await?
                            .schema();
                        for column in column_def_vec {
                            let (field, comment) =
                                self.analyze_add_column(column, schema.clone()).await?;
                            field_and_comment.push((field, comment));
                        }
                        ModifyColumnActionInPlan::SetDataType(field_and_comment)
                    }
                };
                Ok(Plan::ModifyTableColumn(Box::new(ModifyTableColumnPlan {
                    catalog,
                    database,
                    table,
                    action: action_in_plan,
                })))
            }
            AlterTableAction::DropColumn { column } => {
                Ok(Plan::DropTableColumn(Box::new(DropTableColumnPlan {
                    catalog,
                    database,
                    table,
                    column: column.to_string(),
                })))
            }
            AlterTableAction::AlterTableClusterKey { cluster_by } => {
                let schema = self
                    .ctx
                    .get_table(&catalog, &database, &table)
                    .await?
                    .schema();
                let cluster_keys = self.analyze_cluster_keys(cluster_by, schema).await?;

                Ok(Plan::AlterTableClusterKey(Box::new(
                    AlterTableClusterKeyPlan {
                        tenant: tenant.name().to_string(),
                        catalog,
                        database,
                        table,
                        cluster_keys,
                    },
                )))
            }
            AlterTableAction::DropTableClusterKey => Ok(Plan::DropTableClusterKey(Box::new(
                DropTableClusterKeyPlan {
                    tenant: tenant.name().to_string(),
                    catalog,
                    database,
                    table,
                },
            ))),
            AlterTableAction::ReclusterTable {
                is_final,
                selection,
                limit,
            } => {
                let (_, mut context) = self
                    .bind_table_reference(bind_context, table_reference)
                    .await?;

                let mut scalar_binder = ScalarBinder::new(
                    &mut context,
                    self.ctx.clone(),
                    &self.name_resolution_ctx,
                    self.metadata.clone(),
                    &[],
                    self.m_cte_bound_ctx.clone(),
                    self.ctes_map.clone(),
                );

                let push_downs = if let Some(expr) = selection {
                    let (scalar, _) = scalar_binder.bind(expr).await?;
                    Some(scalar)
                } else {
                    None
                };

                Ok(Plan::ReclusterTable(Box::new(ReclusterTablePlan {
                    tenant: tenant.name().to_string(),
                    catalog,
                    database,
                    table,
                    is_final: *is_final,
                    metadata: self.metadata.clone(),
                    push_downs,
                    limit: limit.map(|v| v as usize),
                })))
            }
            AlterTableAction::FlashbackTo { point } => {
                let point = self.resolve_data_travel_point(bind_context, point).await?;
                Ok(Plan::RevertTable(Box::new(RevertTablePlan {
                    tenant: tenant.name().to_string(),
                    catalog,
                    database,
                    table,
                    point,
                })))
            }
            AlterTableAction::SetOptions { set_options } => {
                Ok(Plan::SetOptions(Box::new(SetOptionsPlan {
                    set_options: set_options.clone(),
                    catalog,
                    database,
                    table,
                })))
            }
        }
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_rename_table(
        &mut self,
        stmt: &RenameTableStmt,
    ) -> Result<Plan> {
        let RenameTableStmt {
            if_exists,
            catalog,
            database,
            table,
            new_catalog,
            new_database,
            new_table,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let (catalog, db_name, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        let (new_catalog, new_database, new_table) =
            self.normalize_object_identifier_triple(new_catalog, new_database, new_table);

        if new_catalog != catalog || new_database != db_name {
            return Err(ErrorCode::BadArguments(
                "Rename table not allow modify catalog or database",
            )
            .set_span(database.as_ref().and_then(|ident| ident.span)));
        }

        Ok(Plan::RenameTable(Box::new(RenameTablePlan {
            tenant: tenant.name().to_string(),
            if_exists: *if_exists,
            catalog,
            database: db_name,
            table,
            new_database,
            new_table,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_truncate_table(
        &mut self,
        stmt: &TruncateTableStmt,
    ) -> Result<Plan> {
        let TruncateTableStmt {
            catalog,
            database,
            table,
        } = stmt;

        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        Ok(Plan::TruncateTable(Box::new(TruncateTablePlan {
            catalog,
            database,
            table,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_optimize_table(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &OptimizeTableStmt,
    ) -> Result<Plan> {
        let OptimizeTableStmt {
            catalog,
            database,
            table,
            action: ast_action,
            limit,
        } = stmt;

        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);
        let action = match ast_action {
            AstOptimizeTableAction::All => OptimizeTableAction::All,
            AstOptimizeTableAction::Purge { before } => {
                let p = if let Some(point) = before {
                    let point = self.resolve_data_travel_point(bind_context, point).await?;
                    Some(point)
                } else {
                    None
                };
                OptimizeTableAction::Purge(p)
            }
            AstOptimizeTableAction::Compact { target } => match target {
                CompactTarget::Block => OptimizeTableAction::CompactBlocks,
                CompactTarget::Segment => OptimizeTableAction::CompactSegments,
            },
        };

        Ok(Plan::OptimizeTable(Box::new(OptimizeTablePlan {
            catalog,
            database,
            table,
            action,
            limit: limit.map(|v| v as usize),
            need_lock: true,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_vacuum_table(
        &mut self,
        _bind_context: &mut BindContext,
        stmt: &VacuumTableStmt,
    ) -> Result<Plan> {
        let VacuumTableStmt {
            catalog,
            database,
            table,
            option,
        } = stmt;

        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        let option = VacuumTableOption {
            dry_run: option.dry_run,
        };
        Ok(Plan::VacuumTable(Box::new(VacuumTablePlan {
            catalog,
            database,
            table,
            option,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_vacuum_drop_table(
        &mut self,
        _bind_context: &mut BindContext,
        stmt: &VacuumDropTableStmt,
    ) -> Result<Plan> {
        let VacuumDropTableStmt {
            catalog,
            database,
            option,
        } = stmt;

        let catalog = catalog
            .as_ref()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = database
            .as_ref()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| "".to_string());

        let option = {
            VacuumDropTableOption {
                dry_run: option.dry_run,
                limit: option.limit,
            }
        };
        Ok(Plan::VacuumDropTable(Box::new(VacuumDropTablePlan {
            catalog,
            database,
            option,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_vacuum_temporary_files(
        &mut self,
        _bind_context: &mut BindContext,
        stmt: &VacuumTemporaryFiles,
    ) -> Result<Plan> {
        Ok(Plan::VacuumTemporaryFiles(Box::new(
            VacuumTemporaryFilesPlan {
                limit: stmt.limit,
                retain: stmt.retain,
            },
        )))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_analyze_table(
        &mut self,
        stmt: &AnalyzeTableStmt,
    ) -> Result<Plan> {
        let AnalyzeTableStmt {
            catalog,
            database,
            table,
        } = stmt;

        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        Ok(Plan::AnalyzeTable(Box::new(AnalyzeTablePlan {
            catalog,
            database,
            table,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_exists_table(
        &mut self,
        stmt: &ExistsTableStmt,
    ) -> Result<Plan> {
        let ExistsTableStmt {
            catalog,
            database,
            table,
        } = stmt;

        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        Ok(Plan::ExistsTable(Box::new(ExistsTablePlan {
            catalog,
            database,
            table,
        })))
    }

    #[async_backtrace::framed]
    async fn analyze_rename_column(
        &self,
        old_column: &Identifier,
        new_column: &Identifier,
        table_schema: TableSchemaRef,
    ) -> Result<(TableSchema, String, String)> {
        let old_name = normalize_identifier(old_column, &self.name_resolution_ctx).name;
        let new_name = normalize_identifier(new_column, &self.name_resolution_ctx).name;

        if old_name == new_name {
            return Err(ErrorCode::SemanticError(
                "new column name is the same as old column name".to_string(),
            ));
        }
        let mut new_schema = table_schema.as_ref().clone();
        let mut old_column_existed = false;
        for (i, field) in table_schema.fields().iter().enumerate() {
            if field.name() == &new_name {
                return Err(ErrorCode::SemanticError(
                    "new column name existed".to_string(),
                ));
            }
            if field.name() == &old_name {
                new_schema.rename_field(i, &new_name);
                old_column_existed = true;
            }
        }
        if !old_column_existed {
            return Err(ErrorCode::SemanticError(
                "rename column not existed".to_string(),
            ));
        }
        Ok((new_schema, old_name, new_name))
    }

    #[async_backtrace::framed]
    async fn analyze_add_column(
        &self,
        column: &ColumnDefinition,
        table_schema: TableSchemaRef,
    ) -> Result<(TableField, String)> {
        let name = normalize_identifier(&column.name, &self.name_resolution_ctx).name;
        let not_null = self.is_column_not_null();
        let data_type = resolve_type_name(&column.data_type, not_null)?;
        let mut field = TableField::new(&name, data_type);
        if let Some(expr) = &column.expr {
            match expr {
                ColumnExpr::Default(default_expr) => {
                    let expr =
                        parse_default_expr_to_string(self.ctx.clone(), &field, default_expr, true)?;
                    field = field.with_default_expr(Some(expr));
                }
                ColumnExpr::Virtual(virtual_expr) => {
                    let expr = parse_computed_expr_to_string(
                        self.ctx.clone(),
                        table_schema.clone(),
                        &field,
                        virtual_expr,
                    )?;
                    field = field.with_computed_expr(Some(ComputedExpr::Virtual(expr)));
                }
                ColumnExpr::Stored(_) => {
                    // TODO: support add stored computed expression column.
                    return Err(ErrorCode::SemanticError(
                        "can't add a stored computed column".to_string(),
                    ));
                }
            }
        }
        let comment = column.comment.clone().unwrap_or_default();
        Ok((field, comment))
    }

    #[async_backtrace::framed]
    async fn analyze_create_table_schema_by_columns(
        &self,
        columns: &[ColumnDefinition],
    ) -> Result<(TableSchemaRef, Vec<String>)> {
        let mut has_computed = false;
        let mut fields = Vec::with_capacity(columns.len());
        let mut fields_comments = Vec::with_capacity(columns.len());
        let not_null = self.is_column_not_null();
        for column in columns.iter() {
            let name = normalize_identifier(&column.name, &self.name_resolution_ctx).name;
            let schema_data_type = resolve_type_name(&column.data_type, not_null)?;
            fields_comments.push(column.comment.clone().unwrap_or_default());

            let mut field = TableField::new(&name, schema_data_type.clone());
            if let Some(expr) = &column.expr {
                match expr {
                    ColumnExpr::Default(default_expr) => {
                        let expr = parse_default_expr_to_string(
                            self.ctx.clone(),
                            &field,
                            default_expr,
                            false,
                        )?;
                        field = field.with_default_expr(Some(expr));
                    }
                    _ => has_computed = true,
                }
            }
            fields.push(field);
        }

        let fields = if has_computed {
            let mut source_fields = Vec::with_capacity(fields.len());
            for (column, field) in columns.iter().zip(fields.iter()) {
                match &column.expr {
                    Some(ColumnExpr::Virtual(_)) | Some(ColumnExpr::Stored(_)) => {
                        continue;
                    }
                    _ => {}
                }
                source_fields.push(field.clone());
            }
            let source_schema = TableSchemaRefExt::create(source_fields);
            let mut new_fields = Vec::with_capacity(fields.len());
            for (column, field) in columns.iter().zip(fields.into_iter()) {
                match &column.expr {
                    Some(ColumnExpr::Virtual(virtual_expr)) => {
                        let expr = parse_computed_expr_to_string(
                            self.ctx.clone(),
                            source_schema.clone(),
                            &field,
                            virtual_expr,
                        )?;
                        new_fields
                            .push(field.with_computed_expr(Some(ComputedExpr::Virtual(expr))));
                    }
                    Some(ColumnExpr::Stored(stored_expr)) => {
                        let expr = parse_computed_expr_to_string(
                            self.ctx.clone(),
                            source_schema.clone(),
                            &field,
                            stored_expr,
                        )?;
                        new_fields.push(field.with_computed_expr(Some(ComputedExpr::Stored(expr))));
                    }
                    _ => {
                        new_fields.push(field);
                    }
                }
            }
            new_fields
        } else {
            fields
        };

        let schema = TableSchemaRefExt::create(fields);
        Self::validate_create_table_schema(&schema)?;
        Ok((schema, fields_comments))
    }

    #[async_backtrace::framed]
    async fn analyze_create_table_schema(
        &self,
        source: &CreateTableSource,
    ) -> Result<(TableSchemaRef, Vec<String>)> {
        match source {
            CreateTableSource::Columns(columns) => {
                self.analyze_create_table_schema_by_columns(columns).await
            }
            CreateTableSource::Like {
                catalog,
                database,
                table,
            } => {
                let (catalog, database, table) =
                    self.normalize_object_identifier_triple(catalog, database, table);
                let table = self.ctx.get_table(&catalog, &database, &table).await?;

                if table.engine() == VIEW_ENGINE {
                    if let Some(query) = table.get_table_info().options().get(QUERY) {
                        let mut planner = Planner::new(self.ctx.clone());
                        let (plan, _) = planner.plan_sql(query).await?;
                        Ok((infer_table_schema(&plan.schema())?, vec![]))
                    } else {
                        Err(ErrorCode::Internal(
                            "Logical error, View Table must have a SelectQuery inside.",
                        ))
                    }
                } else {
                    Ok((table.schema(), table.field_comments().clone()))
                }
            }
        }
    }

    /// Validate the schema of the table to be created.
    fn validate_create_table_schema(schema: &TableSchemaRef) -> Result<()> {
        // Check if there are duplicated column names
        let mut name_set = HashSet::new();
        for field in schema.fields() {
            if !name_set.insert(field.name().clone()) {
                return Err(ErrorCode::BadArguments(format!(
                    "Duplicated column name: {}",
                    field.name()
                )));
            }
        }

        Ok(())
    }

    fn insert_table_option_with_validation(
        &self,
        options: &mut BTreeMap<String, String>,
        key: String,
        value: String,
    ) -> Result<()> {
        if is_reserved_opt_key(&key) {
            Err(ErrorCode::TableOptionInvalid(format!(
                "table option {key} reserved, please do not specify in the CREATE TABLE statement",
            )))
        } else if options.insert(key.clone(), value).is_some() {
            Err(ErrorCode::TableOptionInvalid(format!(
                "table option {key} duplicated"
            )))
        } else {
            Ok(())
        }
    }

    #[async_backtrace::framed]
    async fn analyze_cluster_keys(
        &mut self,
        cluster_by: &[Expr],
        schema: TableSchemaRef,
    ) -> Result<Vec<String>> {
        // Build a temporary BindContext to resolve the expr
        let mut bind_context = BindContext::new();
        for (index, field) in schema.fields().iter().enumerate() {
            let column = ColumnBindingBuilder::new(
                field.name().clone(),
                index,
                Box::new(DataType::from(field.data_type())),
                Visibility::Visible,
            )
            .build();

            bind_context.add_column_binding(column);
        }
        let mut scalar_binder = ScalarBinder::new(
            &mut bind_context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
            self.m_cte_bound_ctx.clone(),
            self.ctes_map.clone(),
        );
        // cluster keys cannot be a udf expression.
        scalar_binder.forbid_udf();

        let mut cluster_keys = Vec::with_capacity(cluster_by.len());
        for cluster_by in cluster_by.iter() {
            let (cluster_key, _) = scalar_binder.bind(cluster_by).await?;
            if cluster_key.used_columns().len() != 1 || !cluster_key.evaluable() {
                return Err(ErrorCode::InvalidClusterKeys(format!(
                    "Cluster by expression `{:#}` is invalid",
                    cluster_by
                )));
            }

            let expr = cluster_key.as_expr()?;
            if !expr.is_deterministic(&BUILTIN_FUNCTIONS) {
                return Err(ErrorCode::InvalidClusterKeys(format!(
                    "Cluster by expression `{:#}` is not deterministic",
                    cluster_by
                )));
            }

            let data_type = expr.data_type();
            if !Self::valid_cluster_key_type(data_type) {
                return Err(ErrorCode::InvalidClusterKeys(format!(
                    "Unsupported data type '{}' for cluster by expression `{:#}`",
                    data_type, cluster_by
                )));
            }

            let mut cluster_by = cluster_by.clone();
            let mut normalizer = IdentifierNormalizer {
                ctx: &self.name_resolution_ctx,
            };
            cluster_by.drive_mut(&mut normalizer);
            cluster_keys.push(format!("{:#}", &cluster_by));
        }

        Ok(cluster_keys)
    }

    fn valid_cluster_key_type(data_type: &DataType) -> bool {
        let inner_type = data_type.remove_nullable();
        matches!(
            inner_type,
            DataType::Number(_)
                | DataType::String
                | DataType::Timestamp
                | DataType::Date
                | DataType::Boolean
                | DataType::Decimal(_)
        )
    }

    fn is_column_not_null(&self) -> bool {
        !self
            .ctx
            .get_settings()
            .get_ddl_column_type_nullable()
            .unwrap_or(true)
    }
}
