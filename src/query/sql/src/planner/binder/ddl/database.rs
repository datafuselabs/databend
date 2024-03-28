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

use databend_common_ast::ast::AlterDatabaseAction;
use databend_common_ast::ast::AlterDatabaseStmt;
use databend_common_ast::ast::CreateDatabaseStmt;
use databend_common_ast::ast::DatabaseEngine;
use databend_common_ast::ast::DatabaseRef;
use databend_common_ast::ast::DropDatabaseStmt;
use databend_common_ast::ast::SQLProperty;
use databend_common_ast::ast::ShowCreateDatabaseStmt;
use databend_common_ast::ast::ShowDatabasesStmt;
use databend_common_ast::ast::ShowLimit;
use databend_common_ast::ast::UndropDatabaseStmt;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRefExt;
use databend_common_meta_app::schema::DatabaseMeta;
use databend_common_meta_app::share::ShareNameIdent;
use log::debug;

use crate::binder::Binder;
use crate::planner::semantic::normalize_identifier;
use crate::plans::CreateDatabasePlan;
use crate::plans::DropDatabasePlan;
use crate::plans::Plan;
use crate::plans::RenameDatabaseEntity;
use crate::plans::RenameDatabasePlan;
use crate::plans::RewriteKind;
use crate::plans::ShowCreateDatabasePlan;
use crate::plans::UndropDatabasePlan;
use crate::BindContext;
use crate::SelectBuilder;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_databases(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &ShowDatabasesStmt,
    ) -> Result<Plan> {
        let ShowDatabasesStmt {
            catalog,
            full,
            limit,
        } = stmt;

        let mut select_builder = SelectBuilder::from("system.databases");

        let ctl = if let Some(ctl) = catalog {
            normalize_identifier(ctl, &self.name_resolution_ctx).name
        } else {
            self.ctx.get_current_catalog().to_string()
        };

        select_builder.with_filter(format!("catalog = '{ctl}'"));

        if *full {
            select_builder.with_column("catalog AS Catalog");
            select_builder.with_column("owner");
        }
        select_builder.with_column(format!("name AS `databases_in_{ctl}`"));
        select_builder.with_order_by("catalog");
        select_builder.with_order_by("name");

        match limit {
            Some(ShowLimit::Like { pattern }) => {
                select_builder.with_filter(format!("name LIKE '{pattern}'"));
            }
            Some(ShowLimit::Where { selection }) => {
                select_builder.with_filter(format!("({selection})"));
            }
            None => (),
        }

        let query = select_builder.build();
        debug!("show databases rewrite to: {:?}", query);

        self.bind_rewrite_to_query(bind_context, query.as_str(), RewriteKind::ShowDatabases)
            .await
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_create_database(
        &self,
        stmt: &ShowCreateDatabaseStmt,
    ) -> Result<Plan> {
        let ShowCreateDatabaseStmt { catalog, database } = stmt;

        let catalog = catalog
            .as_ref()
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = normalize_identifier(database, &self.name_resolution_ctx).name;
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("Database", DataType::String),
            DataField::new("Create Database", DataType::String),
        ]);

        Ok(Plan::ShowCreateDatabase(Box::new(ShowCreateDatabasePlan {
            catalog,
            database,
            schema,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_alter_database(
        &self,
        stmt: &AlterDatabaseStmt,
    ) -> Result<Plan> {
        let AlterDatabaseStmt {
            if_exists,
            catalog,
            database,
            action,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let catalog = catalog
            .as_ref()
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = normalize_identifier(database, &self.name_resolution_ctx).name;

        match &action {
            AlterDatabaseAction::RenameDatabase { new_db } => {
                let new_database = new_db.name.clone();
                let entry = RenameDatabaseEntity {
                    if_exists: *if_exists,
                    catalog,
                    database,
                    new_database,
                };

                Ok(Plan::RenameDatabase(Box::new(RenameDatabasePlan {
                    tenant,
                    entities: vec![entry],
                })))
            }
        }
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_database(
        &self,
        stmt: &DropDatabaseStmt,
    ) -> Result<Plan> {
        let DropDatabaseStmt {
            if_exists,
            catalog,
            database,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let catalog = catalog
            .as_ref()
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = normalize_identifier(database, &self.name_resolution_ctx).name;

        Ok(Plan::DropDatabase(Box::new(DropDatabasePlan {
            if_exists: *if_exists,
            tenant,
            catalog,
            database,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_undrop_database(
        &self,
        stmt: &UndropDatabaseStmt,
    ) -> Result<Plan> {
        let UndropDatabaseStmt { catalog, database } = stmt;

        let tenant = self.ctx.get_tenant();
        let catalog = catalog
            .as_ref()
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = normalize_identifier(database, &self.name_resolution_ctx).name;

        Ok(Plan::UndropDatabase(Box::new(UndropDatabasePlan {
            tenant,
            catalog,
            database,
        })))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_database(
        &self,
        stmt: &CreateDatabaseStmt,
    ) -> Result<Plan> {
        let CreateDatabaseStmt {
            create_option,
            database: DatabaseRef { catalog, database },
            engine,
            options,
            from_share,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let catalog = catalog
            .as_ref()
            .map(|catalog| normalize_identifier(catalog, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_catalog());
        let database = normalize_identifier(database, &self.name_resolution_ctx).name;

        // change the database engine to share if create from share
        let engine = if from_share.is_some() {
            &Some(DatabaseEngine::Share)
        } else {
            engine
        };
        let meta = self.database_meta(engine, options, from_share)?;

        Ok(Plan::CreateDatabase(Box::new(CreateDatabasePlan {
            create_option: *create_option,
            tenant,
            catalog,
            database,
            meta,
        })))
    }

    fn database_meta(
        &self,
        engine: &Option<DatabaseEngine>,
        options: &[SQLProperty],
        from_share: &Option<ShareNameIdent>,
    ) -> Result<DatabaseMeta> {
        let options = options
            .iter()
            .map(|property| (property.name.clone(), property.value.clone()))
            .collect::<BTreeMap<String, String>>();

        let database_engine = engine.as_ref().unwrap_or(&DatabaseEngine::Default);
        let (engine, engine_options) = match database_engine {
            DatabaseEngine::Default => ("default", BTreeMap::default()),
            DatabaseEngine::Share => ("share", BTreeMap::default()),
        };

        Ok(DatabaseMeta {
            engine: engine.to_string(),
            engine_options,
            options,
            from_share: from_share.clone(),
            ..Default::default()
        })
    }
}
