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

use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Sample;
use databend_common_ast::ast::Statement;
use databend_common_ast::ast::TableAlias;
use databend_common_ast::ast::TemporalClause;
use databend_common_ast::parser::parse_sql;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::Span;
use databend_common_catalog::table::TimeNavigation;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::DatabaseType;
use databend_common_storages_view::view_table::QUERY;
use databend_storages_common_table_meta::table::get_change_type;

use crate::binder::util::TableIdentifier;
use crate::binder::Binder;
use crate::optimizer::SExpr;
use crate::BindContext;

impl Binder {
    /// Bind a base table.
    /// A base table is a table that is not a view or CTE.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn bind_table(
        &mut self,
        bind_context: &mut BindContext,
        span: &Span,
        catalog: &Option<Identifier>,
        database: &Option<Identifier>,
        table: &Identifier,
        alias: &Option<TableAlias>,
        temporal: &Option<TemporalClause>,
        consume: bool,
        sample: &Option<Sample>,
    ) -> Result<(SExpr, BindContext)> {
        let table_identifier = TableIdentifier::new(self, catalog, database, table, alias);
        let (catalog, database, table_name, table_name_alias) = (
            table_identifier.catalog_name(),
            table_identifier.database_name(),
            table_identifier.table_name(),
            table_identifier.table_name_alias(),
        );

        // Check and bind common table expression
        let ctes_map = self.ctes_map.clone();
        if let Some(cte_info) = ctes_map.get(&table_name) {
            if self
                .metadata
                .read()
                .get_table_index(Some(&database), &table_name)
                .is_some()
            {
                return Err(ErrorCode::SyntaxException(format!(
                    "Table name `{}` is misleading, please distinguish it.",
                    table_name
                ))
                .set_span(*span));
            }
            return if cte_info.materialized {
                self.bind_m_cte(bind_context, cte_info, &table_name, alias, span)
            } else if cte_info.recursive {
                if self.bind_recursive_cte {
                    self.bind_r_cte_scan(bind_context, cte_info, &table_name, alias)
                } else {
                    self.bind_r_cte(bind_context, cte_info, &table_name, alias)
                }
            } else {
                self.bind_cte(*span, bind_context, &table_name, alias, cte_info)
            };
        }

        let tenant = self.ctx.get_tenant();

        let navigation = self.resolve_temporal_clause(bind_context, temporal)?;

        // Resolve table with catalog
        let table_meta = if let Some(share_params) = &bind_context.share_paramas {
            self.resolve_share_reference_data_source(
                share_params,
                tenant.tenant_name(),
                catalog.as_str(),
                database.as_str(),
                table_name.as_str(),
            )?
        } else {
            match self.resolve_data_source(
                tenant.tenant_name(),
                catalog.as_str(),
                database.as_str(),
                table_name.as_str(),
                navigation.as_ref(),
                self.ctx.clone().get_abort_checker(),
            ) {
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
                            } else {
                                self.bind_m_cte(bind_context, cte_info, &table_name, alias, span)
                            };
                        }
                        parent = bind_context.parent.as_mut();
                    }
                    return Err(table_identifier.not_found_suggest_error(e));
                }
            }
        };

        if consume && table_meta.engine() != "STREAM" {
            return Err(ErrorCode::StorageUnsupported(
                "WITH CONSUME only support in STREAM",
            ));
        }

        if navigation.is_some_and(|n| matches!(n, TimeNavigation::Changes { .. }))
            || table_meta.engine() == "STREAM"
        {
            let change_type = get_change_type(&table_name_alias);
            if change_type.is_some() {
                let table_index = self.metadata.write().add_table(
                    catalog,
                    database.clone(),
                    table_meta,
                    table_name_alias,
                    bind_context.view_info.is_some(),
                    bind_context.planning_agg_index,
                    false,
                    consume,
                );
                let (s_expr, mut bind_context) = self.bind_base_table(
                    bind_context,
                    database.as_str(),
                    table_index,
                    change_type,
                    sample,
                )?;

                if let Some(alias) = alias {
                    bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
                }
                return Ok((s_expr, bind_context));
            }

            let query =
                databend_common_base::runtime::block_on(table_meta.generate_changes_query(
                    self.ctx.clone(),
                    database.as_str(),
                    table_name.as_str(),
                    consume,
                ))?;

            let mut new_bind_context = BindContext::with_parent(Box::new(bind_context.clone()));
            let tokens = tokenize_sql(query.as_str())?;
            let (stmt, _) = parse_sql(&tokens, self.dialect)?;
            let Statement::Query(query) = &stmt else {
                unreachable!()
            };
            let (s_expr, mut new_bind_context) = self.bind_query(&mut new_bind_context, query)?;

            let cols = table_meta
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect::<Vec<_>>();
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
            return Ok((s_expr, new_bind_context));
        }

        match table_meta.engine() {
            "VIEW" => {
                // if it is a share view, save Share Params to child bind context to resolve reference tables
                let share_paramas =
                    if let DatabaseType::ShareDB(params) = &table_meta.get_table_info().db_type {
                        Some(params.clone())
                    } else {
                        None
                    };

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
                new_bind_context.share_paramas = share_paramas;
                new_bind_context.view_info = Some((database.clone(), table_name));
                if let Statement::Query(query) = &stmt {
                    self.metadata.write().add_table(
                        catalog,
                        database.clone(),
                        table_meta,
                        table_name_alias,
                        false,
                        false,
                        false,
                        false,
                    );
                    let (s_expr, mut new_bind_context) =
                        self.bind_query(&mut new_bind_context, query)?;
                    if let Some(alias) = alias {
                        // view maybe has alias, e.g. select v1.col1 from v as v1;
                        new_bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
                    } else {
                        // e.g. select v0.c0 from v0;
                        for column in new_bind_context.columns.iter_mut() {
                            column.database_name = Some(database.clone());
                            column.table_name = Some(self.normalize_identifier(table).name);
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
            _ => {
                let table_index = self.metadata.write().add_table(
                    catalog,
                    database.clone(),
                    table_meta,
                    table_name_alias,
                    bind_context.view_info.is_some(),
                    bind_context.planning_agg_index,
                    false,
                    false,
                );

                let (s_expr, mut bind_context) = self.bind_base_table(
                    bind_context,
                    database.as_str(),
                    table_index,
                    None,
                    sample,
                )?;
                if let Some(alias) = alias {
                    bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
                }
                Ok((s_expr, bind_context))
            }
        }
    }

    pub(crate) fn check_view_dep(
        bind_context: &BindContext,
        database: &str,
        view_name: &str,
    ) -> Result<()> {
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
}
