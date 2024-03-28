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

use databend_common_ast::ast::AlterViewStmt;
use databend_common_ast::ast::CreateViewStmt;
use databend_common_ast::ast::DescribeViewStmt;
use databend_common_ast::ast::DropViewStmt;
use databend_common_ast::ast::ShowLimit;
use databend_common_ast::ast::ShowViewsStmt;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRefExt;
use derive_visitor::DriveMut;
use log::debug;

use crate::binder::Binder;
use crate::planner::semantic::normalize_identifier;
use crate::plans::AlterViewPlan;
use crate::plans::CreateViewPlan;
use crate::plans::DescribeViewPlan;
use crate::plans::DropViewPlan;
use crate::plans::Plan;
use crate::plans::RewriteKind;
use crate::BindContext;
use crate::SelectBuilder;
use crate::ViewRewriter;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_view(
        &mut self,
        stmt: &CreateViewStmt,
    ) -> Result<Plan> {
        let CreateViewStmt {
            create_option,
            catalog,
            database,
            view,
            columns,
            query,
        } = stmt;
        let mut query = *query.clone();
        let tenant = self.ctx.get_tenant();
        let (catalog, database, view_name) =
            self.normalize_object_identifier_triple(catalog, database, view);
        let column_names = columns
            .iter()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .collect::<Vec<_>>();
        let mut visitor = ViewRewriter {
            current_database: database.clone(),
        };
        query.drive_mut(&mut visitor);
        let subquery = format!("{}", query);

        let plan = CreateViewPlan {
            create_option: *create_option,
            tenant: tenant.name().to_string(),
            catalog,
            database,
            view_name,
            column_names,
            subquery,
        };
        Ok(Plan::CreateView(plan.into()))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_alter_view(
        &mut self,
        stmt: &AlterViewStmt,
    ) -> Result<Plan> {
        let AlterViewStmt {
            catalog,
            database,
            view,
            columns,
            query,
        } = stmt;

        let mut query = *query.clone();
        let tenant = self.ctx.get_tenant();
        let (catalog, database, view_name) =
            self.normalize_object_identifier_triple(catalog, database, view);
        let column_names = columns
            .iter()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .collect::<Vec<_>>();
        let mut visitor = ViewRewriter {
            current_database: database.clone(),
        };
        query.drive_mut(&mut visitor);
        let subquery = format!("{}", query);

        let plan = AlterViewPlan {
            tenant: tenant.name().to_string(),
            catalog,
            database,
            view_name,
            column_names,
            subquery,
        };
        Ok(Plan::AlterView(plan.into()))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_view(
        &mut self,
        stmt: &DropViewStmt,
    ) -> Result<Plan> {
        let DropViewStmt {
            if_exists,
            catalog,
            database,
            view,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let (catalog, database, view_name) =
            self.normalize_object_identifier_triple(catalog, database, view);
        let plan = DropViewPlan {
            if_exists: *if_exists,
            tenant: tenant.name().to_string(),
            catalog,
            database,
            view_name,
        };
        Ok(Plan::DropView(plan.into()))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_views(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &ShowViewsStmt,
    ) -> Result<Plan> {
        let ShowViewsStmt {
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
            select_builder.with_column(format!("name AS `Views_in_{database}`"));
            if *with_history {
                select_builder.with_column("dropped_on AS drop_time");
            };
        }

        select_builder.with_column("view_query");

        select_builder
            .with_order_by("catalog")
            .with_order_by("database")
            .with_order_by("name");

        select_builder.with_filter(format!("database = '{database}'"));
        select_builder.with_filter(" engine = 'VIEW'".to_string());

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
        debug!("show views rewrite to: {:?}", query);
        self.bind_rewrite_to_query(
            bind_context,
            query.as_str(),
            RewriteKind::ShowTables(catalog_name, database),
        )
        .await
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_describe_view(
        &mut self,
        stmt: &DescribeViewStmt,
    ) -> Result<Plan> {
        let DescribeViewStmt {
            catalog,
            database,
            view,
        } = stmt;

        let (catalog, database, view_name) =
            self.normalize_object_identifier_triple(catalog, database, view);
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("Field", DataType::String),
            DataField::new("Type", DataType::String),
            DataField::new("Null", DataType::String),
            DataField::new("Default", DataType::String),
            DataField::new("Extra", DataType::String),
        ]);

        Ok(Plan::DescribeView(Box::new(DescribeViewPlan {
            catalog,
            database,
            view_name,
            schema,
        })))
    }
}
