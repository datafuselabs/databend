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

use std::collections::HashSet;

use databend_common_ast::ast::CreateIndexStmt;
use databend_common_ast::ast::CreateInvertedIndexStmt;
use databend_common_ast::ast::DropIndexStmt;
use databend_common_ast::ast::DropInvertedIndexStmt;
use databend_common_ast::ast::ExplainKind;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::RefreshIndexStmt;
use databend_common_ast::ast::RefreshInvertedIndexStmt;
use databend_common_ast::ast::SetExpr;
use databend_common_ast::ast::Statement;
use databend_common_ast::ast::TableReference;
use databend_common_ast::parser::parse_sql;
use databend_common_ast::parser::tokenize_sql;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableDataType;
use databend_common_license::license::Feature::AggregateIndex;
use databend_common_license::license_manager::get_license_manager;
use databend_common_meta_app::schema::GetIndexReq;
use databend_common_meta_app::schema::IndexMeta;
use databend_common_meta_app::schema::IndexNameIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_types::NonEmptyString;
use databend_storages_common_table_meta::meta::Location;
use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::binder::Binder;
use crate::optimizer::optimize;
use crate::optimizer::OptimizerContext;
use crate::plans::CreateIndexPlan;
use crate::plans::CreateTableIndexPlan;
use crate::plans::DropIndexPlan;
use crate::plans::DropTableIndexPlan;
use crate::plans::Plan;
use crate::plans::RefreshIndexPlan;
use crate::plans::RefreshTableIndexPlan;
use crate::AggregatingIndexChecker;
use crate::AggregatingIndexRewriter;
use crate::BindContext;
use crate::MetadataRef;
use crate::RefreshAggregatingIndexRewriter;
use crate::SUPPORTED_AGGREGATING_INDEX_FUNCTIONS;

impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_query_index(
        &mut self,
        bind_context: &mut BindContext,
        plan: &Plan,
    ) -> Result<()> {
        match plan {
            Plan::Query { metadata, .. } => {
                self.do_bind_query_index(bind_context, metadata).await?;
            }
            Plan::Explain { kind, plan, .. }
                if matches!(kind, ExplainKind::Plan) && matches!(**plan, Plan::Query { .. }) =>
            {
                match **plan {
                    Plan::Query { ref metadata, .. } => {
                        self.do_bind_query_index(bind_context, metadata).await?;
                    }
                    _ => unreachable!(),
                }
            }
            _ => {}
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn do_bind_query_index(
        &mut self,
        bind_context: &mut BindContext,
        metadata: &MetadataRef,
    ) -> Result<()> {
        let catalog = self.ctx.get_current_catalog();
        let database = self.ctx.get_current_database();
        let tables = metadata.read().tables().to_vec();

        for table_entry in tables {
            let table = table_entry.table();
            // Avoid death loop
            let mut agg_indexes = vec![];
            if self.ctx.get_can_scan_from_agg_index()
                && self
                    .ctx
                    .get_settings()
                    .get_enable_aggregating_index_scan()?
                && !bind_context.planning_agg_index
                && table.support_index()
                && !matches!(table.engine(), "VIEW" | "STREAM")
            {
                let license_manager = get_license_manager();
                if license_manager
                    .manager
                    .check_enterprise_enabled(self.ctx.get_license_key(), AggregateIndex)
                    .is_ok()
                {
                    let indexes = self
                        .resolve_table_indexes(
                            self.ctx.get_tenant().name(),
                            catalog.as_str(),
                            table.get_id(),
                        )
                        .await?;

                    let mut s_exprs = Vec::with_capacity(indexes.len());
                    for (index_id, _, index_meta) in indexes {
                        let tokens = tokenize_sql(&index_meta.query)?;
                        let (stmt, _) = parse_sql(&tokens, self.dialect)?;
                        let mut new_bind_context =
                            BindContext::with_parent(Box::new(bind_context.clone()));
                        new_bind_context.planning_agg_index = true;
                        if let Statement::Query(query) = &stmt {
                            let (s_expr, _) = self.bind_query(&mut new_bind_context, query).await?;
                            s_exprs.push((index_id, index_meta.query.clone(), s_expr));
                        }
                    }
                    agg_indexes.extend(s_exprs);
                }
            }

            if !agg_indexes.is_empty() {
                // Should use bound table id.
                let table_name = table.name();
                let full_table_name = format!("{catalog}.{database}.{table_name}");
                metadata
                    .write()
                    .add_agg_indexes(full_table_name, agg_indexes);
            }
        }

        Ok(())
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_index(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &CreateIndexStmt,
    ) -> Result<Plan> {
        let CreateIndexStmt {
            index_type,
            create_option,
            index_name,
            query,
            sync_creation,
        } = stmt;

        // check if query support index
        {
            let mut agg_index_checker = AggregatingIndexChecker::default();
            query.drive(&mut agg_index_checker);
            if !agg_index_checker.is_supported() {
                return Err(ErrorCode::UnsupportedIndex(format!(
                    "Currently create aggregating index just support simple query, like: {}, \
                and these aggregate funcs: {}, \
                and non-deterministic functions are not support like: NOW()",
                    "SELECT ... FROM ... WHERE ... GROUP BY ...",
                    SUPPORTED_AGGREGATING_INDEX_FUNCTIONS.join(",")
                )));
            }
        }
        let mut original_query = query.clone();
        // pass checker, rewrite aggregate function
        // we will extract all agg function that select targets have
        // and rewrite some agg functions like `avg`.
        let mut query = query.clone();
        // TODO(ariesdevil): unify the checker and rewriter.
        let mut agg_index_rewritter = AggregatingIndexRewriter::new(self.dialect);
        query.drive_mut(&mut agg_index_rewritter);

        let index_name = self.normalize_object_identifier(index_name);

        bind_context.planning_agg_index = true;
        self.bind_query(bind_context, &query).await?;
        bind_context.planning_agg_index = false;

        let tables = self.metadata.read().tables().to_vec();

        if tables.len() != 1 {
            return Err(ErrorCode::UnsupportedIndex(
                "Create Index currently only support single table",
            ));
        }

        let table_entry = &tables[0];
        let table = table_entry.table();

        if !table.support_index() {
            return Err(ErrorCode::UnsupportedIndex(format!(
                "Table engine {} does not support create index",
                table.engine()
            )));
        }

        let table_id = table.get_id();
        Self::rewrite_query_with_database(&mut original_query, table_entry.database());
        Self::rewrite_query_with_database(&mut query, table_entry.database());

        let plan = CreateIndexPlan {
            create_option: *create_option,
            index_type: *index_type,
            index_name,
            original_query: original_query.to_string(),
            query: query.to_string(),
            table_id,
            sync_creation: *sync_creation,
        };
        Ok(Plan::CreateIndex(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_index(
        &mut self,
        stmt: &DropIndexStmt,
    ) -> Result<Plan> {
        let DropIndexStmt { if_exists, index } = stmt;

        let plan = DropIndexPlan {
            if_exists: *if_exists,
            index: index.to_string(),
        };
        Ok(Plan::DropIndex(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_refresh_index(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &RefreshIndexStmt,
    ) -> Result<Plan> {
        let RefreshIndexStmt { index, limit } = stmt;

        if limit.is_some() && limit.unwrap() < 1 {
            return Err(ErrorCode::RefreshIndexError(format!(
                "Invalid 'limit' value: {}. 'limit' must be greater than or equal to 1.",
                limit.unwrap()
            )));
        }

        let index_name = self.normalize_object_identifier(index);
        let catalog = self
            .ctx
            .get_catalog(&self.ctx.get_current_catalog())
            .await?;

        let tenant_name = self.ctx.get_tenant();

        let non_empty = NonEmptyString::new(tenant_name.name().to_string()).map_err(|_| {
            ErrorCode::TenantIsEmpty(
                "Tenant is empty(when Binder::build_refresh_index()".to_string(),
            )
        })?;

        let tenant = Tenant::new_nonempty(non_empty);

        let get_index_req = GetIndexReq {
            name_ident: IndexNameIdent::new(tenant, &index_name),
        };

        let res = catalog.get_index(get_index_req).await?;

        let index_id = res.index_id;
        let index_meta = res.index_meta;

        let plan = self
            .build_refresh_index_plan(bind_context, index_id, index_name, index_meta, *limit, None)
            .await?;

        Ok(Plan::RefreshIndex(Box::new(plan)))
    }

    pub async fn build_refresh_index_plan(
        &mut self,
        bind_context: &mut BindContext,
        index_id: u64,
        index_name: String,
        index_meta: IndexMeta,
        limit: Option<u64>,
        segment_locs: Option<Vec<Location>>,
    ) -> Result<RefreshIndexPlan> {
        let tokens = tokenize_sql(&index_meta.query)?;
        let (mut stmt, _) = parse_sql(&tokens, self.dialect)?;

        // The file name and block only correspond to each other at the time of table_scan,
        // after multiple transformations, this correspondence does not exist,
        // aggregating index needs to know which file the data comes from at the time of final sink
        // to generate the index file corresponding to the source table data file,
        // so we rewrite the sql here to add `_block_name` to select targets,
        // so that we inline the file name into the data block.

        // NOTE: if user already use the `_block_name` in their sql
        // we no need add it and **MUST NOT** drop this column in sink phase.

        // And we will rewrite the agg function to agg state func in this rewriter.
        let mut index_rewriter = RefreshAggregatingIndexRewriter::default();
        stmt.drive_mut(&mut index_rewriter);

        bind_context.planning_agg_index = true;
        let plan = if let Statement::Query(_) = &stmt {
            let select_plan = self.bind_statement(bind_context, &stmt).await?;
            let opt_ctx = OptimizerContext::new(self.ctx.clone(), self.metadata.clone());
            Ok(optimize(opt_ctx, select_plan).await?)
        } else {
            Err(ErrorCode::UnsupportedIndex("statement is not query"))
        };
        let plan = plan?;
        bind_context.planning_agg_index = false;

        let tables = self.metadata.read().tables().to_vec();

        if tables.len() != 1 {
            return Err(ErrorCode::UnsupportedIndex(
                "Aggregating Index currently only support single table",
            ));
        }

        let table_entry = &tables[0];
        let table = table_entry.table();
        debug_assert_eq!(index_meta.table_id, table.get_id());

        let plan = RefreshIndexPlan {
            index_id,
            index_name,
            index_meta,
            limit,
            table_info: table.get_table_info().clone(),
            query_plan: Box::new(plan),
            segment_locs,
            user_defined_block_name: index_rewriter.user_defined_block_name,
        };

        Ok(plan)
    }

    fn rewrite_query_with_database(query: &mut Query, name: &str) {
        if let SetExpr::Select(stmt) = &mut query.body {
            if let TableReference::Table { database, .. } = &mut stmt.from[0] {
                if database.is_none() {
                    *database = Some(Identifier::from_name(query.span, name));
                }
            }
        }
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_create_inverted_index(
        &mut self,
        _bind_context: &mut BindContext,
        stmt: &CreateInvertedIndexStmt,
    ) -> Result<Plan> {
        let CreateInvertedIndexStmt {
            create_option,
            index_name,
            catalog,
            database,
            table,
            columns,
            sync_creation,
        } = stmt;

        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        let table = self.ctx.get_table(&catalog, &database, &table).await?;
        if !table.support_index() {
            return Err(ErrorCode::UnsupportedIndex(format!(
                "Table engine {} does not support create inverted index",
                table.engine()
            )));
        }
        let table_schema = table.schema();
        let mut column_set = HashSet::with_capacity(columns.len());
        let mut column_ids = Vec::with_capacity(columns.len());
        for column in columns {
            match table_schema.field_with_name(&column.name) {
                Ok(field) => {
                    if field.data_type.remove_nullable() != TableDataType::String {
                        return Err(ErrorCode::UnsupportedIndex(format!(
                            "Inverted index currently only support String type, but the type of column {} is {}",
                            column, field.data_type
                        )));
                    }
                    if column_set.contains(&column.name) {
                        return Err(ErrorCode::UnsupportedIndex(format!(
                            "Inverted index column must be unique, but column {} is duplicate",
                            column.name
                        )));
                    }
                    column_set.insert(column.name.clone());
                    column_ids.push(field.column_id);
                }
                Err(_) => {
                    return Err(ErrorCode::UnsupportedIndex(format!(
                        "Table does not have column {}",
                        column
                    )));
                }
            }
        }
        let table_id = table.get_id();
        let index_name = self.normalize_object_identifier(index_name);

        let plan = CreateTableIndexPlan {
            create_option: *create_option,
            catalog,
            index_name,
            column_ids,
            table_id,
            sync_creation: *sync_creation,
        };
        Ok(Plan::CreateTableIndex(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_drop_inverted_index(
        &mut self,
        _bind_context: &mut BindContext,
        stmt: &DropInvertedIndexStmt,
    ) -> Result<Plan> {
        let DropInvertedIndexStmt {
            if_exists,
            index_name,
            catalog,
            database,
            table,
        } = stmt;

        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);

        let table = self.ctx.get_table(&catalog, &database, &table).await?;
        if !table.support_index() {
            return Err(ErrorCode::UnsupportedIndex(format!(
                "Table engine {} does not support create inverted index",
                table.engine()
            )));
        }
        let table_id = table.get_id();
        let index_name = self.normalize_object_identifier(index_name);

        let plan = DropTableIndexPlan {
            if_exists: *if_exists,
            catalog,
            index_name,
            table_id,
        };
        Ok(Plan::DropTableIndex(Box::new(plan)))
    }

    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_refresh_inverted_index(
        &mut self,
        _bind_context: &mut BindContext,
        stmt: &RefreshInvertedIndexStmt,
    ) -> Result<Plan> {
        let RefreshInvertedIndexStmt {
            index_name,
            catalog,
            database,
            table,
            limit: _,
        } = stmt;

        let (catalog, database, table) =
            self.normalize_object_identifier_triple(catalog, database, table);
        let index_name = self.normalize_object_identifier(index_name);

        let plan = RefreshTableIndexPlan {
            catalog,
            database,
            table,
            index_name,
            segment_locs: None,
            need_lock: true,
        };
        Ok(Plan::RefreshTableIndex(Box::new(plan)))
    }
}
