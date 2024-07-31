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

use databend_common_ast::ast::CreateStreamStmt;
use databend_common_ast::ast::DescribeStreamStmt;
use databend_common_ast::ast::DropStreamStmt;
use databend_common_ast::ast::ShowLimit;
use databend_common_ast::ast::ShowStreamsStmt;
use databend_common_exception::Result;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use log::debug;

use crate::binder::Binder;
use crate::normalize_identifier;
use crate::plans::CreateStreamPlan;
use crate::plans::DropStreamPlan;
use crate::plans::Plan;
use crate::plans::RewriteKind;
use crate::BindContext;
use crate::SelectBuilder;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_stream(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &CreateStreamStmt,
    ) -> Result<Plan> {
        let CreateStreamStmt {
            create_option,
            catalog,
            database,
            stream,
            table_database,
            table,
            travel_point,
            append_only,
            comment,
        } = stmt;

        let tenant = self.ctx.get_tenant();
        let (catalog, database, stream_name) =
            self.normalize_object_identifier_triple(catalog, database, stream);

        let table_database = table_database
            .as_ref()
            .map(|ident| normalize_identifier(ident, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| self.ctx.get_current_database());
        let table_name = normalize_identifier(table, &self.name_resolution_ctx).name;

        let navigation = if let Some(point) = travel_point {
            Some(self.resolve_data_travel_point(bind_context, point)?)
        } else {
            None
        };

        let plan = CreateStreamPlan {
            create_option: create_option.clone().into(),
            tenant,
            catalog,
            database,
            stream_name,
            table_database,
            table_name,
            navigation,
            append_only: *append_only,
            comment: comment.clone(),
        };
        Ok(Plan::CreateStream(plan.into()))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_stream(
        &mut self,
        stmt: &DropStreamStmt,
    ) -> Result<Plan> {
        let DropStreamStmt {
            if_exists,
            catalog,
            database,
            stream,
        } = stmt;

        let tenant = self.ctx.get_tenant();

        let (catalog, database, stream_name) =
            self.normalize_object_identifier_triple(catalog, database, stream);

        let plan = DropStreamPlan {
            if_exists: *if_exists,
            tenant,
            catalog,
            database,
            stream_name,
        };
        Ok(Plan::DropStream(plan.into()))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_show_streams(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &ShowStreamsStmt,
    ) -> Result<Plan> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::Stream)?;

        let ShowStreamsStmt {
            catalog,
            database,
            full,
            limit,
        } = stmt;

        let database = self.check_database_exist(catalog, database).await?;

        let mut select_builder = if *full {
            SelectBuilder::from("system.streams")
        } else {
            SelectBuilder::from("system.streams_terse")
        };

        if *full {
            select_builder
                .with_column("created_on")
                .with_column("name")
                .with_column("database")
                .with_column("catalog")
                .with_column("table_name As table_on")
                .with_column("owner")
                .with_column("comment")
                .with_column("mode")
                .with_column("invalid_reason");
        } else {
            select_builder
                .with_column(format!("name AS `Streams_in_{database}`"))
                .with_column("table_name As table_on")
                .with_column("mode");
        }

        select_builder
            .with_order_by("catalog")
            .with_order_by("database")
            .with_order_by("name");

        select_builder.with_filter(format!("database = '{database}'"));
        if let Some(catalog) = catalog {
            let catalog = normalize_identifier(catalog, &self.name_resolution_ctx).name;
            select_builder.with_filter(format!("catalog = '{catalog}'"));
        }
        if let Some(limit) = limit {
            match limit {
                ShowLimit::Like { pattern } => {
                    select_builder.with_filter(format!("name LIKE '{pattern}'"));
                }
                ShowLimit::Where { selection } => {
                    select_builder.with_filter(format!("({selection})"));
                }
            }
        }

        let query = select_builder.build();
        debug!("show streams rewrite to: {:?}", query);

        self.bind_rewrite_to_query(
            bind_context,
            query.as_str(),
            RewriteKind::ShowStreams(database),
        )
        .await
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_describe_stream(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &DescribeStreamStmt,
    ) -> Result<Plan> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::Stream)?;

        let DescribeStreamStmt {
            catalog,
            database,
            stream,
        } = stmt;

        let (catalog, database, stream) =
            self.normalize_object_identifier_triple(catalog, database, stream);

        let mut select_builder = SelectBuilder::from("system.streams");
        select_builder
            .with_column("created_on")
            .with_column("name")
            .with_column("database")
            .with_column("catalog")
            .with_column("table_name As table_on")
            .with_column("owner")
            .with_column("comment")
            .with_column("mode")
            .with_column("invalid_reason");
        select_builder.with_filter(format!("catalog = '{catalog}'"));
        select_builder.with_filter(format!("database = '{database}'"));
        select_builder.with_filter(format!("name = '{stream}'"));
        let query = select_builder.build();
        self.bind_rewrite_to_query(
            bind_context,
            query.as_str(),
            RewriteKind::ShowStreams(database),
        )
        .await
    }
}
