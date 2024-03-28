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
use std::collections::HashMap;
use std::default::Default;
use std::str::FromStr;
use std::sync::Arc;

use async_recursion::async_recursion;
use chrono::TimeZone;
use chrono::Utc;
use dashmap::DashMap;
use databend_common_ast::ast::Connection;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FileLocation;
use databend_common_ast::ast::FunctionCall as ASTFunctionCall;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Indirection;
use databend_common_ast::ast::Join;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::SelectStageOptions;
use databend_common_ast::ast::SelectStmt;
use databend_common_ast::ast::SelectTarget;
use databend_common_ast::ast::Statement;
use databend_common_ast::ast::TableAlias;
use databend_common_ast::ast::TableReference;
use databend_common_ast::ast::TimeTravelPoint;
use databend_common_ast::ast::UriLocation;
use databend_common_ast::parser::parse_sql;
use databend_common_ast::parser::tokenize_sql;
use databend_common_catalog::catalog_kind::CATALOG_DEFAULT;
use databend_common_catalog::plan::ParquetReadOptions;
use databend_common_catalog::plan::StageTableInfo;
use databend_common_catalog::table::NavigationPoint;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_catalog::table_function::TableFunction;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::Span;
use databend_common_expression::is_stream_column;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::ColumnId;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataField;
use databend_common_expression::FunctionKind;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::StageFileFormatType;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::schema::IndexMeta;
use databend_common_meta_app::schema::ListIndexesReq;
use databend_common_meta_types::MetaId;
use databend_common_storage::DataOperator;
use databend_common_storage::StageFileInfo;
use databend_common_storage::StageFilesInfo;
use databend_common_storages_parquet::ParquetRSTable;
use databend_common_storages_result_cache::ResultCacheMetaManager;
use databend_common_storages_result_cache::ResultCacheReader;
use databend_common_storages_result_cache::ResultScan;
use databend_common_storages_stage::StageTable;
use databend_common_storages_view::view_table::QUERY;
use databend_common_users::UserApiProvider;
use databend_storages_common_table_meta::table::get_change_type;
use databend_storages_common_table_meta::table::ChangeType;
use databend_storages_common_table_meta::table::StreamMode;
use databend_storages_common_table_meta::table::OPT_KEY_TABLE_VER;
use log::info;
use parking_lot::RwLock;

use crate::binder::copy_into_table::resolve_file_location;
use crate::binder::scalar::ScalarBinder;
use crate::binder::table_args::bind_table_args;
use crate::binder::Binder;
use crate::binder::ColumnBindingBuilder;
use crate::binder::CteInfo;
use crate::binder::ExprContext;
use crate::binder::Visibility;
use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::planner::semantic::normalize_identifier;
use crate::planner::semantic::TypeChecker;
use crate::plans::CteScan;
use crate::plans::DummyTableScan;
use crate::plans::EvalScalar;
use crate::plans::FunctionCall;
use crate::plans::RelOperator;
use crate::plans::ScalarItem;
use crate::plans::Scan;
use crate::plans::Statistics;
use crate::BaseTableColumn;
use crate::BindContext;
use crate::ColumnEntry;
use crate::IndexType;
use crate::ScalarExpr;

impl Binder {
    #[async_backtrace::framed]
    pub async fn bind_one_table(
        &mut self,
        bind_context: &BindContext,
        select_list: &Vec<SelectTarget>,
    ) -> Result<(SExpr, BindContext)> {
        for select_target in select_list {
            if let SelectTarget::StarColumns {
                qualified: names, ..
            } = select_target
            {
                for indirect in names {
                    if let Indirection::Star(span) = indirect {
                        return Err(ErrorCode::SemanticError(
                            "'SELECT *' is used without specifying any tables in the FROM clause."
                                .to_string(),
                        )
                        .set_span(*span));
                    }
                }
            }
        }
        let bind_context = BindContext::with_parent(Box::new(bind_context.clone()));
        Ok((
            SExpr::create_leaf(Arc::new(DummyTableScan.into())),
            bind_context,
        ))
    }

    fn check_view_dep(bind_context: &BindContext, database: &str, view_name: &str) -> Result<()> {
        match &bind_context.parent {
            Some(parent) => match &parent.view_info {
                Some((db, v)) => {
                    if db == database && v == view_name {
                        Err(ErrorCode::Internal(format!(
                            "View dependency loop detected (view: {}.{})",
                            database, view_name
                        )))
                    } else {
                        Self::check_view_dep(parent, database, view_name)
                    }
                }
                _ => Ok(()),
            },
            _ => Ok(()),
        }
    }

    /// Bind a base table.
    /// A base table is a table that is not a view or CTE.
    #[allow(clippy::too_many_arguments)]
    #[async_backtrace::framed]
    async fn bind_table(
        &mut self,
        bind_context: &mut BindContext,
        span: &Span,
        catalog: &Option<Identifier>,
        database: &Option<Identifier>,
        table: &Identifier,
        alias: &Option<TableAlias>,
        travel_point: &Option<TimeTravelPoint>,
        since_point: &Option<TimeTravelPoint>,
    ) -> Result<(SExpr, BindContext)> {
        let (catalog, database, table_name) =
            self.normalize_object_identifier_triple(catalog, database, table);
        let table_alias_name = if let Some(table_alias) = alias {
            Some(normalize_identifier(&table_alias.name, &self.name_resolution_ctx).name)
        } else {
            None
        };
        let mut bind_cte = true;
        if let Some(cte_name) = &bind_context.cte_name {
            // If table name equals to cte name, then skip bind cte and find table from catalog
            // Or will dead loop and stack overflow
            if cte_name == &table_name {
                bind_cte = false;
            }
        }
        // Check and bind common table expression
        let ctes_map = self.ctes_map.clone();
        if let Some(cte_info) = ctes_map.get(&table_name) {
            if bind_cte {
                return if !cte_info.materialized {
                    self.bind_cte(*span, bind_context, &table_name, alias, cte_info)
                        .await
                } else {
                    self.bind_m_cte(bind_context, cte_info, &table_name, alias, span)
                        .await
                };
            }
        }

        let tenant = self.ctx.get_tenant();

        let navigation_point = match travel_point {
            Some(tp) => Some(self.resolve_data_travel_point(bind_context, tp).await?),
            None => None,
        };

        let since_point = match since_point {
            Some(tp) => Some(self.resolve_data_travel_point(bind_context, tp).await?),
            None => None,
        };

        // Resolve table with catalog
        let table_meta = match self
            .resolve_data_source(
                tenant.name(),
                catalog.as_str(),
                database.as_str(),
                table_name.as_str(),
                &navigation_point,
                &since_point,
            )
            .await
        {
            Ok(table) => table,
            Err(e) => {
                let mut parent = bind_context.parent.as_mut();
                loop {
                    if parent.is_none() {
                        break;
                    }
                    let bind_context = parent.unwrap().as_mut();
                    let ctes_map = self.ctes_map.clone();
                    if let Some(cte_info) = ctes_map.get(&table_name) {
                        return if !cte_info.materialized {
                            self.bind_cte(*span, bind_context, &table_name, alias, cte_info)
                                .await
                        } else {
                            self.bind_m_cte(bind_context, cte_info, &table_name, alias, span)
                                .await
                        };
                    }
                    parent = bind_context.parent.as_mut();
                }
                if e.code() == ErrorCode::UNKNOWN_DATABASE {
                    return Err(ErrorCode::UnknownDatabase(format!(
                        "Unknown database `{}` in catalog '{catalog}'",
                        database
                    ))
                    .set_span(*span));
                }
                if e.code() == ErrorCode::UNKNOWN_TABLE {
                    return Err(ErrorCode::UnknownTable(format!(
                        "Unknown table `{database}`.`{table_name}` in catalog '{catalog}'"
                    ))
                    .set_span(*span));
                }
                return Err(e);
            }
        };

        match table_meta.engine() {
            "VIEW" => {
                // TODO(leiysky): this check is error-prone,
                // we should find a better way to do this.
                Self::check_view_dep(bind_context, &database, &table_name)?;
                let query = table_meta
                    .options()
                    .get(QUERY)
                    .ok_or_else(|| ErrorCode::Internal("Invalid VIEW object"))?;
                let tokens = tokenize_sql(query.as_str())?;
                let (stmt, _) = parse_sql(&tokens, self.dialect)?;
                // For view, we need use a new context to bind it.
                let mut new_bind_context = BindContext::with_parent(Box::new(bind_context.clone()));
                new_bind_context.view_info = Some((database.clone(), table_name));
                if let Statement::Query(query) = &stmt {
                    self.metadata.write().add_table(
                        catalog,
                        database.clone(),
                        table_meta,
                        table_alias_name,
                        false,
                        false,
                        false,
                    );
                    let (s_expr, mut new_bind_context) =
                        self.bind_query(&mut new_bind_context, query).await?;
                    if let Some(alias) = alias {
                        // view maybe has alias, e.g. select v1.col1 from v as v1;
                        new_bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
                    } else {
                        // e.g. select v0.c0 from v0;
                        for column in new_bind_context.columns.iter_mut() {
                            column.database_name = None;
                            column.table_name =
                                Some(normalize_identifier(table, &self.name_resolution_ctx).name);
                        }
                    }
                    new_bind_context.parent = Some(Box::new(bind_context.clone()));
                    Ok((s_expr, new_bind_context))
                } else {
                    Err(
                        ErrorCode::Internal(format!("Invalid VIEW object: {}", table_meta.name()))
                            .set_span(*span),
                    )
                }
            }
            "STREAM" => {
                let change_type = get_change_type(&table_alias_name);
                if change_type.is_some() {
                    let table_index = self.metadata.write().add_table(
                        catalog,
                        database.clone(),
                        table_meta,
                        table_alias_name,
                        bind_context.view_info.is_some(),
                        bind_context.planning_agg_index,
                        false,
                    );
                    let (s_expr, mut bind_context) = self
                        .bind_base_table(bind_context, database.as_str(), table_index, change_type)
                        .await?;

                    if let Some(alias) = alias {
                        bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
                    }
                    return Ok((s_expr, bind_context));
                }

                let mode = table_meta.get_stream_mode(self.ctx.clone()).await?;
                let table_version = table_meta
                    .options()
                    .get(OPT_KEY_TABLE_VER)
                    .ok_or_else(|| ErrorCode::Internal("table version must be set in stream"))?
                    .parse::<u64>()?;

                let cols = table_meta
                    .schema()
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect::<Vec<_>>();

                let suffix = format!("{:08x}", Utc::now().timestamp());
                let query = match mode {
                    StreamMode::AppendOnly => {
                        let append_alias = format!("_change_append${}", suffix);
                        format!(
                            "select *, \
                                    'INSERT' as change$action, \
                                    false as change$is_update, \
                                    if(is_not_null(_origin_block_id), \
                                       concat(to_uuid(_origin_block_id), lpad(hex(_origin_block_row_num), 6, '0')), \
                                       {append_alias}._base_row_id \
                                    ) as change$row_id \
                             from {database}.{table_name} as {append_alias} \
                             where not(is_not_null(_origin_version) and \
                                       (_origin_version < {table_version} or \
                                        contains({append_alias}._base_block_ids, _origin_block_id)))",
                        )
                    }
                    StreamMode::Standard => {
                        let a_table_alias = format!("_change_insert${}", suffix);
                        let a_cols = cols.join(", ");

                        let d_table_alias = format!("_change_delete${}", suffix);
                        let d_cols = cols
                            .iter()
                            .map(|s| format!("d_{}", s))
                            .collect::<Vec<_>>()
                            .join(", ");

                        format!(
                            "with _change({a_cols}, change$action, change$row_id, \
                                          {d_cols}, d_change$action, d_change$row_id) as materialized \
                            ( \
                                select * \
                                from ( \
                                    select *, \
                                           _row_version, \
                                           'INSERT' as change$action, \
                                           if(is_not_null(_origin_block_id), \
                                              concat(to_uuid(_origin_block_id), lpad(hex(_origin_block_row_num), 6, '0')), \
                                              {a_table_alias}._base_row_id \
                                           ) as change$row_id \
                                    from {database}.{table_name} as {a_table_alias} \
                                ) as A \
                                FULL OUTER JOIN ( \
                                    select *, \
                                           _row_version, \
                                           'DELETE' as change$action, \
                                           if(is_not_null(_origin_block_id), \
                                              concat(to_uuid(_origin_block_id), lpad(hex(_origin_block_row_num), 6, '0')), \
                                              {d_table_alias}._base_row_id \
                                           ) as change$row_id \
                                    from {database}.{table_name} as {d_table_alias} \
                                ) as D \
                                on A.change$row_id = D.change$row_id \
                                where A.change$row_id is null or D.change$row_id is null or A._row_version > D._row_version \
                            ) \
                            select {a_cols}, \
                                   change$action, \
                                   change$row_id, \
                                   d_change$action is not null as change$is_update \
                            from _change \
                            where change$action is not null \
                            union all \
                            select {d_cols}, \
                                   d_change$action, \
                                   d_change$row_id, \
                                   change$action is not null as change$is_update \
                            from _change \
                            where d_change$action is not null",
                        )
                    }
                };
                let mut new_bind_context = BindContext::with_parent(Box::new(bind_context.clone()));
                let tokens = tokenize_sql(query.as_str())?;
                let (stmt, _) = parse_sql(&tokens, self.dialect)?;
                if let Statement::Query(query) = &stmt {
                    let (s_expr, mut new_bind_context) =
                        self.bind_query(&mut new_bind_context, query).await?;

                    for (index, column_name) in cols.iter().enumerate() {
                        new_bind_context.columns[index].column_name = column_name.clone();
                    }
                    if let Some(alias) = alias {
                        new_bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
                    } else {
                        for column in new_bind_context.columns.iter_mut() {
                            column.database_name = None;
                            column.table_name = Some(table_name.clone());
                        }
                    }
                    new_bind_context.parent = Some(Box::new(bind_context.clone()));
                    Ok((s_expr, new_bind_context))
                } else {
                    unreachable!()
                }
            }
            _ => {
                let table_index = self.metadata.write().add_table(
                    catalog,
                    database.clone(),
                    table_meta,
                    table_alias_name,
                    bind_context.view_info.is_some(),
                    bind_context.planning_agg_index,
                    false,
                );

                let (s_expr, mut bind_context) = self
                    .bind_base_table(bind_context, database.as_str(), table_index, None)
                    .await?;
                if let Some(alias) = alias {
                    bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
                }
                Ok((s_expr, bind_context))
            }
        }
    }

    /// Extract the srf inner tuple fields as columns.
    #[async_backtrace::framed]
    async fn extract_srf_table_function_columns(
        &mut self,
        bind_context: &mut BindContext,
        span: &Span,
        func_name: &Identifier,
        srf_expr: SExpr,
        alias: &Option<TableAlias>,
    ) -> Result<(SExpr, BindContext)> {
        let fields = if func_name.name.eq_ignore_ascii_case("flatten") {
            Some(vec![
                "seq".to_string(),
                "key".to_string(),
                "path".to_string(),
                "index".to_string(),
                "value".to_string(),
                "this".to_string(),
            ])
        } else if func_name.name.eq_ignore_ascii_case("json_each") {
            Some(vec!["key".to_string(), "value".to_string()])
        } else {
            None
        };

        if let Some(fields) = fields {
            if let RelOperator::EvalScalar(plan) = (*srf_expr.plan).clone() {
                if plan.items.len() != 1 {
                    return Err(ErrorCode::Internal(format!(
                        "Invalid table function subquery EvalScalar items, expect 1, but got {}",
                        plan.items.len()
                    )));
                }
                // Delete srf result tuple column, extract tuple inner columns instead
                let _ = bind_context.columns.pop();
                let scalar = &plan.items[0].scalar;

                // Add tuple inner columns
                let mut items = Vec::with_capacity(fields.len());
                for (i, field) in fields.into_iter().enumerate() {
                    let field_expr = ScalarExpr::FunctionCall(FunctionCall {
                        span: *span,
                        func_name: "get".to_string(),
                        params: vec![Scalar::Number(NumberScalar::Int64((i + 1) as i64))],
                        arguments: vec![scalar.clone()],
                    });
                    let data_type = field_expr.data_type()?;
                    let index = self
                        .metadata
                        .write()
                        .add_derived_column(field.clone(), data_type.clone());

                    let column_binding = ColumnBindingBuilder::new(
                        field,
                        index,
                        Box::new(data_type),
                        Visibility::Visible,
                    )
                    .build();
                    bind_context.add_column_binding(column_binding);

                    items.push(ScalarItem {
                        scalar: field_expr,
                        index,
                    });
                }
                let eval_scalar = EvalScalar { items };
                let new_expr =
                    SExpr::create_unary(Arc::new(eval_scalar.into()), srf_expr.children[0].clone());

                if let Some(alias) = alias {
                    bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
                }
                return Ok((new_expr, bind_context.clone()));
            } else {
                return Err(ErrorCode::Internal(
                    "Invalid subquery in table function: Table functions do not support this type of subquery.",
                ));
            }
        }
        // Set name for srf result column
        bind_context.columns[0].column_name = "value".to_string();
        if let Some(alias) = alias {
            bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
        }
        Ok((srf_expr, bind_context.clone()))
    }

    /// Bind a lateral table function.
    #[async_backtrace::framed]
    async fn bind_lateral_table_function(
        &mut self,
        parent_context: &mut BindContext,
        child: SExpr,
        table_ref: &TableReference,
    ) -> Result<(SExpr, BindContext)> {
        match table_ref {
            TableReference::TableFunction {
                span,
                name,
                params,
                named_params,
                alias,
                ..
            } => {
                let mut bind_context = BindContext::with_parent(Box::new(parent_context.clone()));
                let func_name = normalize_identifier(name, &self.name_resolution_ctx);

                if BUILTIN_FUNCTIONS
                    .get_property(&func_name.name)
                    .map(|p| p.kind == FunctionKind::SRF)
                    .unwrap_or(false)
                {
                    let args = parse_table_function_args(span, &func_name, params, named_params)?;

                    // convert lateral join table function to srf function
                    let srf = Expr::FunctionCall {
                        span: *span,
                        func: ASTFunctionCall {
                            distinct: false,
                            name: func_name.clone(),
                            args,
                            params: vec![],
                            window: None,
                            lambda: None,
                        },
                    };
                    let srfs = vec![srf.clone()];
                    let srf_expr = self
                        .bind_project_set(&mut bind_context, &srfs, child)
                        .await?;

                    if let Some((_, srf_result)) = bind_context.srfs.remove(&srf.to_string()) {
                        let column_binding =
                            if let ScalarExpr::BoundColumnRef(column_ref) = &srf_result {
                                column_ref.column.clone()
                            } else {
                                // Add result column to metadata
                                let data_type = srf_result.data_type()?;
                                let index = self
                                    .metadata
                                    .write()
                                    .add_derived_column(srf.to_string(), data_type.clone());
                                ColumnBindingBuilder::new(
                                    srf.to_string(),
                                    index,
                                    Box::new(data_type),
                                    Visibility::Visible,
                                )
                                .build()
                            };

                        let eval_scalar = EvalScalar {
                            items: vec![ScalarItem {
                                scalar: srf_result,
                                index: column_binding.index,
                            }],
                        };
                        // Add srf result column
                        bind_context.add_column_binding(column_binding);

                        let flatten_expr =
                            SExpr::create_unary(Arc::new(eval_scalar.into()), Arc::new(srf_expr));

                        let (new_expr, mut bind_context) = self
                            .extract_srf_table_function_columns(
                                &mut bind_context,
                                span,
                                &func_name,
                                flatten_expr,
                                alias,
                            )
                            .await?;

                        // add left table columns.
                        let mut new_columns = parent_context.columns.clone();
                        new_columns.extend_from_slice(&bind_context.columns);
                        bind_context.columns = new_columns;

                        return Ok((new_expr, bind_context));
                    } else {
                        return Err(ErrorCode::Internal("Failed to bind project_set for lateral join. This may indicate an issue with the SRF (Set Returning Function) processing or an internal logic error.")
                            .set_span(*span));
                    }
                } else {
                    return Err(ErrorCode::InvalidArgument(format!(
                        "The function '{}' is not supported for lateral joins. Lateral joins currently support only Set Returning Functions (SRFs).",
                        func_name
                    ))
                    .set_span(*span));
                }
            }
            _ => unreachable!(),
        }
    }

    /// Bind a table function.
    #[async_backtrace::framed]
    async fn bind_table_function(
        &mut self,
        bind_context: &mut BindContext,
        span: &Span,
        name: &Identifier,
        params: &[Expr],
        named_params: &[(Identifier, Expr)],
        alias: &Option<TableAlias>,
    ) -> Result<(SExpr, BindContext)> {
        let func_name = normalize_identifier(name, &self.name_resolution_ctx);

        if BUILTIN_FUNCTIONS
            .get_property(&func_name.name)
            .map(|p| p.kind == FunctionKind::SRF)
            .unwrap_or(false)
        {
            // If it is a set-returning function, we bind it as a subquery.
            let args = parse_table_function_args(span, &func_name, params, named_params)?;

            let select_stmt = SelectStmt {
                span: *span,
                hints: None,
                distinct: false,
                select_list: vec![SelectTarget::AliasedExpr {
                    expr: Box::new(databend_common_ast::ast::Expr::FunctionCall {
                        span: *span,
                        func: ASTFunctionCall {
                            distinct: false,
                            name: databend_common_ast::ast::Identifier::from_name(
                                *span,
                                &func_name.name,
                            ),
                            params: vec![],
                            args,
                            window: None,
                            lambda: None,
                        },
                    }),
                    alias: None,
                }],
                from: vec![],
                selection: None,
                group_by: None,
                having: None,
                window_list: None,
                qualify: None,
            };
            let (srf_expr, mut bind_context) = self
                .bind_select_stmt(bind_context, &select_stmt, &[], 0)
                .await?;

            return self
                .extract_srf_table_function_columns(
                    &mut bind_context,
                    span,
                    &func_name,
                    srf_expr,
                    alias,
                )
                .await;
        }

        let mut scalar_binder = ScalarBinder::new(
            bind_context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
            self.m_cte_bound_ctx.clone(),
            self.ctes_map.clone(),
        );
        let table_args = bind_table_args(&mut scalar_binder, params, named_params).await?;

        if func_name.name.eq_ignore_ascii_case("result_scan") {
            let query_id = parse_result_scan_args(&table_args)?;
            if query_id.is_empty() {
                return Err(ErrorCode::InvalidArgument("The `RESULT_SCAN` function requires a 'query_id' parameter. Please specify a valid query ID.")
                    .set_span(*span));
            }
            let kv_store = UserApiProvider::instance().get_meta_store_client();
            let meta_key = self.ctx.get_result_cache_key(&query_id);
            if meta_key.is_none() {
                return Err(ErrorCode::EmptyData(format!(
                    "`RESULT_SCAN` failed: No cache key found in current session for query ID '{}'.",
                    query_id
                )).set_span(*span));
            }
            let result_cache_mgr = ResultCacheMetaManager::create(kv_store, 0);
            let meta_key = meta_key.unwrap();
            let (table_schema, block_raw_data) = match result_cache_mgr
                .get(meta_key.clone())
                .await?
            {
                Some(value) => {
                    let op = DataOperator::instance().operator();
                    ResultCacheReader::read_table_schema_and_data(op, &value.location).await?
                }
                None => {
                    return Err(ErrorCode::EmptyData(format!(
                        "`RESULT_SCAN` failed: Unable to fetch cached data for query ID '{}'. The data may have exceeded its TTL or been cleaned up. Cache key: '{}'",
                        query_id, meta_key
                    )).set_span(*span));
                }
            };
            let table = ResultScan::try_create(table_schema, query_id, block_raw_data)?;

            let table_alias_name = if let Some(table_alias) = alias {
                Some(normalize_identifier(&table_alias.name, &self.name_resolution_ctx).name)
            } else {
                None
            };

            let table_index = self.metadata.write().add_table(
                CATALOG_DEFAULT.to_string(),
                "system".to_string(),
                table.clone(),
                table_alias_name,
                false,
                false,
                false,
            );

            let (s_expr, mut bind_context) = self
                .bind_base_table(bind_context, "system", table_index, None)
                .await?;
            if let Some(alias) = alias {
                bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
            }
            Ok((s_expr, bind_context))
        } else {
            // Other table functions always reside is default catalog
            let table_meta: Arc<dyn TableFunction> = self
                .catalogs
                .get_default_catalog(self.ctx.txn_mgr())?
                .get_table_function(&func_name.name, table_args)?;
            let table = table_meta.as_table();
            let table_alias_name = if let Some(table_alias) = alias {
                Some(normalize_identifier(&table_alias.name, &self.name_resolution_ctx).name)
            } else {
                None
            };
            let table_index = self.metadata.write().add_table(
                CATALOG_DEFAULT.to_string(),
                "system".to_string(),
                table.clone(),
                table_alias_name,
                false,
                false,
                false,
            );

            let (s_expr, mut bind_context) = self
                .bind_base_table(bind_context, "system", table_index, None)
                .await?;
            if let Some(alias) = alias {
                bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
            }
            Ok((s_expr, bind_context))
        }
    }

    /// Bind a subquery.
    #[async_backtrace::framed]
    async fn bind_subquery(
        &mut self,
        bind_context: &mut BindContext,
        lateral: bool,
        subquery: &Query,
        alias: &Option<TableAlias>,
    ) -> Result<(SExpr, BindContext)> {
        // If the subquery is a lateral subquery, we need to let it see the columns
        // from the previous queries.
        let (result, mut result_bind_context) = if lateral {
            let mut new_bind_context = BindContext::with_parent(Box::new(bind_context.clone()));
            self.bind_query(&mut new_bind_context, subquery).await?
        } else {
            let mut new_bind_context = BindContext::with_parent(
                bind_context
                    .parent
                    .clone()
                    .unwrap_or_else(|| Box::new(BindContext::new())),
            );
            self.bind_query(&mut new_bind_context, subquery).await?
        };

        if let Some(alias) = alias {
            result_bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
            // Reset column name as alias column name
            for i in 0..alias.columns.len() {
                let column = &result_bind_context.columns[i];
                self.metadata
                    .write()
                    .change_derived_column_alias(column.index, column.column_name.clone());
            }
        }
        Ok((result, result_bind_context))
    }

    /// Bind a location.
    #[async_backtrace::framed]
    async fn bind_location(
        &mut self,
        bind_context: &mut BindContext,
        location: &FileLocation,
        options: &SelectStageOptions,
        alias: &Option<TableAlias>,
    ) -> Result<(SExpr, BindContext)> {
        let location = match location {
            FileLocation::Uri(uri) => FileLocation::Uri(UriLocation {
                connection: Connection::new(options.connection.clone()),
                ..uri.clone()
            }),
            _ => location.clone(),
        };
        let (mut stage_info, path) = resolve_file_location(self.ctx.as_ref(), &location).await?;
        if let Some(f) = &options.file_format {
            stage_info.file_format_params = match StageFileFormatType::from_str(f) {
                Ok(t) => FileFormatParams::default_by_type(t)?,
                _ => self.ctx.get_file_format(f).await?,
            }
        }
        let files_info = StageFilesInfo {
            path,
            pattern: options.pattern.clone(),
            files: options.files.clone(),
        };
        let table_ctx = self.ctx.clone();
        self.bind_stage_table(table_ctx, bind_context, stage_info, files_info, alias, None)
            .await
    }

    #[async_recursion]
    #[async_backtrace::framed]
    pub(crate) async fn bind_single_table(
        &mut self,
        bind_context: &mut BindContext,
        table_ref: &TableReference,
    ) -> Result<(SExpr, BindContext)> {
        match table_ref {
            TableReference::Table {
                span,
                catalog,
                database,
                table,
                alias,
                travel_point,
                since_point,
                pivot: _,
                unpivot: _,
            } => {
                self.bind_table(
                    bind_context,
                    span,
                    catalog,
                    database,
                    table,
                    alias,
                    travel_point,
                    since_point,
                )
                .await
            }
            TableReference::TableFunction {
                span,
                name,
                params,
                named_params,
                alias,
                ..
            } => {
                self.bind_table_function(bind_context, span, name, params, named_params, alias)
                    .await
            }
            TableReference::Subquery {
                span: _,
                lateral,
                subquery,
                alias,
            } => {
                self.bind_subquery(bind_context, *lateral, subquery, alias)
                    .await
            }
            TableReference::Location {
                span: _,
                location,
                options,
                alias,
            } => {
                self.bind_location(bind_context, location, options, alias)
                    .await
            }
            TableReference::Join { join, .. } => {
                let (left_expr, left_bind_ctx) =
                    self.bind_table_reference(bind_context, &join.left).await?;
                let (right_expr, right_bind_ctx) =
                    self.bind_table_reference(bind_context, &join.right).await?;
                self.bind_join(
                    bind_context,
                    left_bind_ctx,
                    right_bind_ctx,
                    left_expr,
                    right_expr,
                    join,
                )
                .await
            }
        }
    }

    #[async_backtrace::framed]
    pub(crate) async fn bind_stage_table(
        &mut self,
        table_ctx: Arc<dyn TableContext>,
        bind_context: &BindContext,
        stage_info: StageInfo,
        files_info: StageFilesInfo,
        alias: &Option<TableAlias>,
        files_to_copy: Option<Vec<StageFileInfo>>,
    ) -> Result<(SExpr, BindContext)> {
        let start = std::time::Instant::now();

        let table = match stage_info.file_format_params {
            FileFormatParams::Parquet(..) => {
                let mut read_options = ParquetReadOptions::default();

                if !table_ctx.get_settings().get_enable_parquet_page_index()? {
                    read_options = read_options.with_prune_pages(false);
                }

                if !table_ctx
                    .get_settings()
                    .get_enable_parquet_rowgroup_pruning()?
                {
                    read_options = read_options.with_prune_row_groups(false);
                }

                if !table_ctx.get_settings().get_enable_parquet_prewhere()? {
                    read_options = read_options.with_do_prewhere(false);
                }

                ParquetRSTable::create(
                    table_ctx.clone(),
                    stage_info.clone(),
                    files_info,
                    read_options,
                    files_to_copy,
                )
                .await?
            }
            FileFormatParams::NdJson(..) => {
                let schema = Arc::new(TableSchema::new(vec![TableField::new(
                    "_$1", // TODO: this name should be in visible
                    TableDataType::Variant,
                )]));
                let info = StageTableInfo {
                    schema,
                    stage_info,
                    files_info,
                    files_to_copy: None,
                    duplicated_files_detected: vec![],
                    is_select: true,
                    default_values: None,
                };
                StageTable::try_create(info)?
            }
            FileFormatParams::Csv(..) | FileFormatParams::Tsv(..) => {
                let max_column_position = self.metadata.read().get_max_column_position();
                if max_column_position == 0 {
                    let file_type = match stage_info.file_format_params {
                        FileFormatParams::Csv(..) => "CSV",
                        FileFormatParams::Tsv(..) => "TSV",
                        _ => unreachable!(), // This branch should never be reached
                    };

                    return Err(ErrorCode::SemanticError(format!(
                        "Query from {} file lacks column positions. Specify as $1, $2, etc.",
                        file_type
                    )));
                }

                let mut fields = vec![];
                for i in 1..(max_column_position + 1) {
                    fields.push(TableField::new(
                        &format!("_${}", i),
                        TableDataType::Nullable(Box::new(TableDataType::String)),
                    ));
                }

                let schema = Arc::new(TableSchema::new(fields));
                let info = StageTableInfo {
                    schema,
                    stage_info,
                    files_info,
                    files_to_copy: None,
                    duplicated_files_detected: vec![],
                    is_select: true,
                    default_values: None,
                };
                StageTable::try_create(info)?
            }
            _ => {
                return Err(ErrorCode::Unimplemented(format!(
                    "The file format in the query stage is not supported. Currently supported formats are: Parquet, NDJson, CSV, and TSV. Provided format: '{}'.",
                    stage_info.file_format_params
                )));
            }
        };

        let table_alias_name = if let Some(table_alias) = alias {
            Some(normalize_identifier(&table_alias.name, &self.name_resolution_ctx).name)
        } else {
            None
        };

        let table_index = self.metadata.write().add_table(
            CATALOG_DEFAULT.to_string(),
            "system".to_string(),
            table.clone(),
            table_alias_name,
            false,
            false,
            true,
        );

        let (s_expr, mut bind_context) = self
            .bind_base_table(bind_context, "system", table_index, None)
            .await?;
        if let Some(alias) = alias {
            bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
        }

        info!("bind_stage_table cost: {:?}", start.elapsed());
        Ok((s_expr, bind_context))
    }

    #[async_backtrace::framed]
    pub async fn bind_table_reference(
        &mut self,
        bind_context: &mut BindContext,
        table_ref: &TableReference,
    ) -> Result<(SExpr, BindContext)> {
        let mut current_ref = table_ref;
        let current_ctx = bind_context;

        // Stack to keep track of the joins
        let mut join_stack: Vec<&Join> = Vec::new();

        // Traverse the table reference hierarchy to get to the innermost table
        while let TableReference::Join { join, .. } = current_ref {
            join_stack.push(join);

            // Check whether the right-hand side is a Join or a TableReference
            match &*join.right {
                TableReference::Join { .. } => {
                    // Traverse the right-hand side if the right-hand side is a Join
                    current_ref = &join.right;
                }
                _ => {
                    // Traverse the left-hand side if the right-hand side is a TableReference
                    current_ref = &join.left;
                }
            }
        }

        // Bind the innermost table
        // current_ref must be left table in its join
        let (mut result_expr, mut result_ctx) =
            self.bind_single_table(current_ctx, current_ref).await?;

        for join in join_stack.iter().rev() {
            match &*join.right {
                TableReference::Join { .. } => {
                    let (left_expr, left_ctx) =
                        self.bind_single_table(&mut result_ctx, &join.left).await?;
                    let (join_expr, ctx) = self
                        .bind_join(
                            current_ctx,
                            left_ctx,
                            result_ctx,
                            left_expr,
                            result_expr,
                            join,
                        )
                        .await?;
                    result_expr = join_expr;
                    result_ctx = ctx;
                }
                _ => {
                    if join.right.is_lateral_table_function() {
                        let (expr, ctx) = self
                            .bind_lateral_table_function(
                                &mut result_ctx,
                                result_expr.clone(),
                                &join.right,
                            )
                            .await?;
                        result_expr = expr;
                        result_ctx = ctx;
                    } else {
                        let (right_expr, right_ctx) =
                            self.bind_single_table(&mut result_ctx, &join.right).await?;
                        let (join_expr, ctx) = self
                            .bind_join(
                                current_ctx,
                                result_ctx,
                                right_ctx,
                                result_expr,
                                right_expr,
                                join,
                            )
                            .await?;
                        result_expr = join_expr;
                        result_ctx = ctx;
                    }
                }
            }
        }

        Ok((result_expr, result_ctx))
    }

    fn bind_cte_scan(&mut self, cte_info: &CteInfo) -> Result<SExpr> {
        let blocks = Arc::new(RwLock::new(vec![]));
        self.ctx
            .set_materialized_cte((cte_info.cte_idx, cte_info.used_count), blocks)?;
        // Get the fields in the cte
        let mut fields = vec![];
        let mut offsets = vec![];
        for (idx, column) in cte_info.columns.iter().enumerate() {
            fields.push(DataField::new(
                column.index.to_string().as_str(),
                *column.data_type.clone(),
            ));
            offsets.push(idx);
        }
        let cte_scan = SExpr::create_leaf(Arc::new(
            CteScan {
                cte_idx: (cte_info.cte_idx, cte_info.used_count),
                fields,
                // It is safe to unwrap here because we have checked that the cte is materialized.
                offsets,
                stat: cte_info.stat_info.clone().unwrap(),
            }
            .into(),
        ));
        Ok(cte_scan)
    }

    #[async_backtrace::framed]
    pub(crate) async fn bind_cte(
        &mut self,
        span: Span,
        bind_context: &mut BindContext,
        table_name: &str,
        alias: &Option<TableAlias>,
        cte_info: &CteInfo,
    ) -> Result<(SExpr, BindContext)> {
        let mut new_bind_context = BindContext {
            parent: Some(Box::new(bind_context.clone())),
            bound_internal_columns: BTreeMap::new(),
            columns: vec![],
            aggregate_info: Default::default(),
            windows: Default::default(),
            cte_name: Some(table_name.to_string()),
            cte_map_ref: Box::default(),
            in_grouping: false,
            view_info: None,
            srfs: Default::default(),
            inverted_index_map: Box::default(),
            expr_context: ExprContext::default(),
            planning_agg_index: false,
            allow_internal_columns: true,
            window_definitions: DashMap::new(),
        };

        let (s_expr, mut res_bind_context) = self
            .bind_query(&mut new_bind_context, &cte_info.query)
            .await?;
        let mut cols_alias = cte_info.columns_alias.clone();
        if let Some(alias) = alias {
            for (idx, col_alias) in alias.columns.iter().enumerate() {
                if idx < cte_info.columns_alias.len() {
                    cols_alias[idx] = col_alias.name.clone();
                } else {
                    cols_alias.push(col_alias.name.clone());
                }
            }
        }
        let alias_table_name = alias
            .as_ref()
            .map(|alias| normalize_identifier(&alias.name, &self.name_resolution_ctx).name)
            .unwrap_or_else(|| table_name.to_string());
        for column in res_bind_context.columns.iter_mut() {
            column.database_name = None;
            column.table_name = Some(alias_table_name.clone());
        }

        if cols_alias.len() > res_bind_context.columns.len() {
            return Err(ErrorCode::SemanticError(format!(
                "The CTE '{}' has {} columns, but {} aliases were provided. Ensure the number of aliases matches the number of columns in the CTE.",
                table_name,
                res_bind_context.columns.len(),
                cols_alias.len()
            ))
                .set_span(span));
        }
        for (index, column_name) in cols_alias.iter().enumerate() {
            res_bind_context.columns[index].column_name = column_name.clone();
        }
        Ok((s_expr, res_bind_context))
    }

    // Bind materialized cte
    #[async_backtrace::framed]
    pub(crate) async fn bind_m_cte(
        &mut self,
        bind_context: &mut BindContext,
        cte_info: &CteInfo,
        table_name: &String,
        alias: &Option<TableAlias>,
        span: &Span,
    ) -> Result<(SExpr, BindContext)> {
        let new_bind_context = if cte_info.used_count == 0 {
            let (cte_s_expr, cte_bind_ctx) = self
                .bind_cte(*span, bind_context, table_name, alias, cte_info)
                .await?;
            let stat_info = RelExpr::with_s_expr(&cte_s_expr).derive_cardinality()?;
            self.ctes_map
                .entry(table_name.clone())
                .and_modify(|cte_info| {
                    cte_info.stat_info = Some(stat_info);
                    cte_info.columns = cte_bind_ctx.columns.clone();
                });
            self.set_m_cte_bound_ctx(cte_info.cte_idx, cte_bind_ctx.clone());
            self.set_m_cte_bound_s_expr(cte_info.cte_idx, cte_s_expr);
            cte_bind_ctx
        } else {
            // If the cte has been bound, get the bound context from `Binder`'s `m_cte_bound_ctx`
            let mut bound_ctx = self.m_cte_bound_ctx.get(&cte_info.cte_idx).unwrap().clone();
            // Resolve the alias name for the bound cte.
            let alias_table_name = alias
                .as_ref()
                .map(|alias| normalize_identifier(&alias.name, &self.name_resolution_ctx).name)
                .unwrap_or_else(|| table_name.to_string());
            for column in bound_ctx.columns.iter_mut() {
                column.database_name = None;
                column.table_name = Some(alias_table_name.clone());
            }
            // Pass parent to bound_ctx
            bound_ctx.parent = bind_context.parent.clone();
            bound_ctx
        };
        // `bind_context` is the main BindContext for the whole query
        // Update the `used_count` which will be used in runtime phase
        self.ctes_map
            .entry(table_name.clone())
            .and_modify(|cte_info| {
                cte_info.used_count += 1;
            });
        let cte_info = self.ctes_map.get(table_name).unwrap().clone();
        let s_expr = self.bind_cte_scan(&cte_info)?;
        Ok((s_expr, new_bind_context))
    }

    #[async_backtrace::framed]
    pub(crate) async fn bind_base_table(
        &mut self,
        bind_context: &BindContext,
        database_name: &str,
        table_index: IndexType,
        change_type: Option<ChangeType>,
    ) -> Result<(SExpr, BindContext)> {
        let mut bind_context = BindContext::with_parent(Box::new(bind_context.clone()));

        let table = self.metadata.read().table(table_index).clone();
        let table_name = table.name();
        let table = table.table();
        let statistics_provider = table.column_statistics_provider(self.ctx.clone()).await?;

        let mut col_stats = HashMap::new();
        let columns = self.metadata.read().columns_by_table_index(table_index);
        for column in columns.iter() {
            match column {
                ColumnEntry::BaseTableColumn(BaseTableColumn {
                    column_name,
                    column_index,
                    path_indices,
                    data_type,
                    leaf_index,
                    table_index,
                    column_position,
                    virtual_computed_expr,
                    ..
                }) => {
                    let column_binding = ColumnBindingBuilder::new(
                        column_name.clone(),
                        *column_index,
                        Box::new(DataType::from(data_type)),
                        if path_indices.is_some() || is_stream_column(column_name) {
                            Visibility::InVisible
                        } else {
                            Visibility::Visible
                        },
                    )
                    .table_name(Some(table_name.to_string()))
                    .database_name(Some(database_name.to_string()))
                    .table_index(Some(*table_index))
                    .column_position(*column_position)
                    .virtual_computed_expr(virtual_computed_expr.clone())
                    .build();

                    bind_context.add_column_binding(column_binding);
                    if path_indices.is_none() && virtual_computed_expr.is_none() {
                        if let Some(col_id) = *leaf_index {
                            let col_stat =
                                statistics_provider.column_statistics(col_id as ColumnId);
                            col_stats.insert(*column_index, col_stat.cloned());
                        }
                    }
                }
                other => {
                    return Err(ErrorCode::Internal(format!(
                        "Invalid column entry '{:?}' encountered while binding the base table '{}'. Ensure that the table definition and column references are correct.",
                        other.name(),
                        table_name
                    )));
                }
            }
        }

        let stat = table
            .table_statistics(self.ctx.clone(), change_type.clone())
            .await?;

        Ok((
            SExpr::create_leaf(Arc::new(
                Scan {
                    table_index,
                    columns: columns.into_iter().map(|col| col.index()).collect(),
                    statistics: Statistics {
                        statistics: stat,
                        col_stats,
                    },
                    change_type,
                    ..Default::default()
                }
                .into(),
            )),
            bind_context,
        ))
    }

    #[async_backtrace::framed]
    pub async fn resolve_data_source(
        &self,
        _tenant: &str,
        catalog_name: &str,
        database_name: &str,
        table_name: &str,
        travel_point: &Option<NavigationPoint>,
        since_point: &Option<NavigationPoint>,
    ) -> Result<Arc<dyn Table>> {
        // Resolve table with ctx
        // for example: select * from t1 join (select * from t1 as t2 where a > 1 and a < 13);
        // we will invoke here twice for t1, so in the past, we use catalog every time to get the
        // newest snapshot, we can't get consistent snapshot
        let mut table_meta = self
            .ctx
            .get_table(catalog_name, database_name, table_name)
            .await?;

        if travel_point.is_some() || since_point.is_some() {
            table_meta = table_meta
                .navigate_since_to(since_point, travel_point)
                .await?;
        }
        Ok(table_meta)
    }

    #[async_backtrace::framed]
    pub(crate) async fn resolve_data_travel_point(
        &self,
        bind_context: &mut BindContext,
        travel_point: &TimeTravelPoint,
    ) -> Result<NavigationPoint> {
        match travel_point {
            TimeTravelPoint::Snapshot(s) => Ok(NavigationPoint::SnapshotID(s.to_owned())),
            TimeTravelPoint::Timestamp(expr) => {
                let mut type_checker = TypeChecker::try_create(
                    bind_context,
                    self.ctx.clone(),
                    &self.name_resolution_ctx,
                    self.metadata.clone(),
                    &[],
                    false,
                )?;
                let box (scalar, _) = type_checker.resolve(expr).await?;
                let scalar_expr = scalar.as_expr()?;

                let (new_expr, _) = ConstantFolder::fold(
                    &scalar_expr,
                    &self.ctx.get_function_context()?,
                    &BUILTIN_FUNCTIONS,
                );

                match new_expr {
                    databend_common_expression::Expr::Constant {
                        scalar,
                        data_type: DataType::Timestamp,
                        ..
                    } => {
                        let value = scalar.as_timestamp().unwrap();
                        Ok(NavigationPoint::TimePoint(
                            Utc.timestamp_nanos(*value * 1000),
                        ))
                    }

                    other => Err(ErrorCode::InvalidArgument(format!(
                        "TimeTravelPoint for 'Timestamp' must resolve to a constant timestamp value. Provided expression '{:?}' is not a constant timestamp. Ensure the expression is a constant and of type timestamp.",
                        other
                    ))),
                }
            }
            TimeTravelPoint::Stream {
                catalog,
                database,
                name,
            } => {
                let (catalog, database, name) =
                    self.normalize_object_identifier_triple(catalog, database, name);
                let stream = self.ctx.get_table(&catalog, &database, &name).await?;
                if stream.engine() != "STREAM" {
                    return Err(ErrorCode::TableEngineNotSupported(format!(
                        "{database}.{name} is not STREAM",
                    )));
                }
                let info = stream.get_table_info().clone();
                Ok(NavigationPoint::StreamInfo(info))
            }
        }
    }

    #[async_backtrace::framed]
    pub(crate) async fn resolve_table_indexes(
        &self,
        tenant: &str,
        catalog_name: &str,
        table_id: MetaId,
    ) -> Result<Vec<(u64, String, IndexMeta)>> {
        let catalog = self
            .catalogs
            .get_catalog(tenant, catalog_name, self.ctx.txn_mgr())
            .await?;
        let index_metas = catalog
            .list_indexes(ListIndexesReq::new(tenant, Some(table_id)))
            .await?;

        Ok(index_metas)
    }
}

// copy from common-storages-fuse to avoid cyclic dependency.
fn string_value(value: &Scalar) -> Result<String> {
    match value {
        Scalar::String(val) => Ok(val.clone()),
        other => Err(ErrorCode::BadArguments(format!(
            "Expected a string value, but found a '{}'.",
            other
        ))),
    }
}

#[inline(always)]
pub fn parse_result_scan_args(table_args: &TableArgs) -> Result<String> {
    let args = table_args.expect_all_positioned("RESULT_SCAN", Some(1))?;
    string_value(&args[0])
}

// parse flatten named params to arguments
fn parse_table_function_args(
    span: &Span,
    func_name: &Identifier,
    params: &[Expr],
    named_params: &[(Identifier, Expr)],
) -> Result<Vec<Expr>> {
    if func_name.name.eq_ignore_ascii_case("flatten") {
        // build flatten function arguments.
        let mut named_args: HashMap<String, Expr> = named_params
            .iter()
            .map(|(name, value)| (name.name.to_lowercase(), value.clone()))
            .collect::<HashMap<_, _>>();

        let mut args = Vec::with_capacity(named_args.len() + params.len());
        let names = vec!["input", "path", "outer", "recursive", "mode"];
        for name in names {
            if named_args.is_empty() {
                break;
            }
            match named_args.remove(name) {
                Some(val) => args.push(val),
                None => args.push(Expr::Literal {
                    span: None,
                    value: Literal::Null,
                }),
            }
        }
        if !named_args.is_empty() {
            let invalid_names = named_args.into_keys().collect::<Vec<String>>().join(", ");
            return Err(ErrorCode::InvalidArgument(format!(
                "Invalid named parameters for 'flatten': {}, valid parameters are: [input, path, outer, recursive, mode]",
                invalid_names,
            ))
            .set_span(*span));
        }

        if !params.is_empty() {
            args.extend(params.iter().cloned());
        }
        Ok(args)
    } else {
        if !named_params.is_empty() {
            let invalid_names = named_params
                .iter()
                .map(|(name, _)| name.name.clone())
                .collect::<Vec<String>>()
                .join(", ");
            return Err(ErrorCode::InvalidArgument(format!(
                "Named parameters are not allowed for '{}'. Invalid parameters provided: {}.",
                func_name.name, invalid_names
            ))
            .set_span(*span));
        }

        Ok(params.to_vec())
    }
}
