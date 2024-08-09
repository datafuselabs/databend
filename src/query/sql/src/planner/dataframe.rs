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

use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FunctionCall;
use databend_common_ast::ast::GroupBy;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Join;
use databend_common_ast::ast::JoinCondition;
use databend_common_ast::ast::JoinOperator;
use databend_common_ast::ast::OrderByExpr;
use databend_common_ast::ast::SelectTarget;
use databend_common_ast::ast::TableReference;
use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::catalog::CATALOG_DEFAULT;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use parking_lot::RwLock;

use crate::planner::optimizer::s_expr::SExpr;
use crate::plans::Limit;
use crate::BindContext;
use crate::Binder;
use crate::Metadata;
use crate::NameResolutionContext;

pub struct Dataframe {
    query_ctx: Arc<dyn TableContext>,
    binder: Binder,
    bind_context: BindContext,
    s_expr: SExpr,
}

impl Dataframe {
    pub async fn scan(
        query_ctx: Arc<dyn TableContext>,
        db: Option<&str>,
        table_name: &str,
    ) -> Result<Self> {
        let table = TableReference::Table {
            database: db.map(|db| Identifier::from_name(None, db)),
            table: Identifier::from_name(None, table_name),
            span: None,
            catalog: None,
            alias: None,
            temporal: None,
            consume: false,
            pivot: None,
            unpivot: None,
            sample: None,
        };

        let settings = query_ctx.get_settings();
        let metadata = Arc::new(RwLock::new(Metadata::default()));
        let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;

        let mut binder = Binder::new(
            query_ctx.clone(),
            CatalogManager::instance(),
            name_resolution_ctx,
            metadata.clone(),
        );

        let mut bind_context = BindContext::new();

        let (s_expr, bind_context) = if db == Some("system") && table_name == "one" {
            let catalog = CATALOG_DEFAULT;
            let database = "system";
            let tenant = query_ctx.get_tenant();
            let table_meta: Arc<dyn Table> = binder.resolve_data_source(
                tenant.tenant_name(),
                catalog,
                database,
                "one",
                None,
                query_ctx.clone().get_abort_checker(),
            )?;

            let table_index = metadata.write().add_table(
                CATALOG_DEFAULT.to_owned(),
                database.to_string(),
                table_meta,
                None,
                false,
                false,
                false,
                false,
            );

            binder.bind_base_table(&bind_context, database, table_index, None, &None)
        } else {
            binder.bind_table_reference(&mut bind_context, &table)
        }?;

        Ok(Dataframe {
            query_ctx,
            binder,
            bind_context,
            s_expr,
        })
    }

    pub async fn scan_one(query_ctx: Arc<dyn TableContext>) -> Result<Self> {
        Self::scan(query_ctx, Some("system"), "one").await
    }

    pub fn select_columns(self, columns: &[&str]) -> Result<Self> {
        let schema = self.bind_context.output_schema();
        let select_list = parse_cols(schema, columns)?;

        self.select_targets(&select_list)
    }

    pub fn select(self, expr_list: Vec<Expr>) -> Result<Self> {
        let select_list: Vec<SelectTarget> = expr_list
            .into_iter()
            .map(|expr| SelectTarget::AliasedExpr {
                expr: Box::new(expr),
                alias: None,
            })
            .collect();

        self.select_targets(&select_list)
    }

    fn select_targets(mut self, select_list: &[SelectTarget]) -> Result<Self> {
        let bind_context = &mut self.bind_context;
        let select_list = self
            .binder
            .normalize_select_list(bind_context, select_list)?;

        let (scalar_items, projections) = self.binder.analyze_projection(
            &bind_context.aggregate_info,
            &bind_context.windows,
            &select_list,
        )?;

        self.s_expr = self.binder.bind_projection(
            &mut self.bind_context,
            &projections,
            &scalar_items,
            self.s_expr,
        )?;
        self.s_expr = self
            .bind_context
            .add_internal_column_into_expr(self.s_expr.clone())?;

        Ok(self)
    }

    pub async fn filter(mut self, expr: Expr) -> Result<Self> {
        let (s_expr, _) =
            self.binder
                .bind_where(&mut self.bind_context, &[], &expr, self.s_expr)?;
        self.s_expr = s_expr;
        Ok(self)
    }

    pub fn count(mut self) -> Result<Self> {
        let select_list = [SelectTarget::AliasedExpr {
            expr: Box::new(Expr::FunctionCall {
                span: None,
                func: FunctionCall {
                    distinct: false,
                    name: Identifier::from_name(None, "count"),
                    args: vec![],
                    params: vec![],
                    window: None,
                    lambda: None,
                },
            }),
            alias: None,
        }];

        let mut select_list = self
            .binder
            .normalize_select_list(&mut self.bind_context, &select_list)?;

        self.binder
            .analyze_aggregate_select(&mut self.bind_context, &mut select_list)?;

        let (scalar_items, projections) = self.binder.analyze_projection(
            &self.bind_context.aggregate_info,
            &self.bind_context.windows,
            &select_list,
        )?;

        self.s_expr = self
            .binder
            .bind_aggregate(&mut self.bind_context, self.s_expr)?;

        self.s_expr = self.binder.bind_projection(
            &mut self.bind_context,
            &projections,
            &scalar_items,
            self.s_expr,
        )?;
        self.s_expr = self
            .bind_context
            .add_internal_column_into_expr(self.s_expr.clone())?;
        Ok(self)
    }

    pub async fn aggregate(
        mut self,
        groupby: GroupBy,
        aggr_expr: Vec<Expr>,
        having: Option<Expr>,
    ) -> Result<Self> {
        let select_list: Vec<SelectTarget> = aggr_expr
            .into_iter()
            .map(|expr| SelectTarget::AliasedExpr {
                expr: Box::new(expr),
                alias: None,
            })
            .collect();

        let select_list = self
            .binder
            .normalize_select_list(&mut self.bind_context, &select_list)?;

        let aliases = select_list
            .items
            .iter()
            .map(|item| (item.alias.clone(), item.scalar.clone()))
            .collect::<Vec<_>>();

        self.binder
            .analyze_group_items(&mut self.bind_context, &select_list, &groupby)?;

        if !self
            .bind_context
            .aggregate_info
            .aggregate_functions
            .is_empty()
            || !self.bind_context.aggregate_info.group_items.is_empty()
        {
            self.s_expr = self
                .binder
                .bind_aggregate(&mut self.bind_context, self.s_expr)?;
        }

        if let Some(having) = &having {
            let having =
                self.binder
                    .analyze_aggregate_having(&mut self.bind_context, &aliases, having)?;
            self.s_expr = self
                .binder
                .bind_having(&mut self.bind_context, having, self.s_expr)?;
        }

        let (scalar_items, projections) = self.binder.analyze_projection(
            &self.bind_context.aggregate_info,
            &self.bind_context.windows,
            &select_list,
        )?;

        self.s_expr = self.binder.bind_projection(
            &mut self.bind_context,
            &projections,
            &scalar_items,
            self.s_expr,
        )?;
        self.s_expr = self
            .bind_context
            .add_internal_column_into_expr(self.s_expr.clone())?;
        Ok(self)
    }

    pub fn distinct_col(self, columns: &[&str]) -> Result<Self> {
        let select_list = parse_cols(self.bind_context.output_schema(), columns)?;
        self.distinct_target(select_list)
    }

    pub fn distinct(self, select_list: Vec<Expr>) -> Result<Self> {
        let select_list: Vec<SelectTarget> = select_list
            .into_iter()
            .map(|expr| SelectTarget::AliasedExpr {
                expr: Box::new(expr),
                alias: None,
            })
            .collect();
        self.distinct_target(select_list)
    }

    pub fn distinct_target(mut self, select_list: Vec<SelectTarget>) -> Result<Self> {
        let mut select_list = self
            .binder
            .normalize_select_list(&mut self.bind_context, select_list.as_slice())?;
        self.binder
            .analyze_aggregate_select(&mut self.bind_context, &mut select_list)?;
        let (mut scalar_items, projections) = self.binder.analyze_projection(
            &self.bind_context.aggregate_info,
            &self.bind_context.windows,
            &select_list,
        )?;
        self.s_expr = self.binder.bind_distinct(
            None,
            &self.bind_context,
            &projections,
            &mut scalar_items,
            self.s_expr.clone(),
        )?;
        self.s_expr = self.binder.bind_projection(
            &mut self.bind_context,
            &projections,
            &scalar_items,
            self.s_expr,
        )?;
        self.s_expr = self
            .bind_context
            .add_internal_column_into_expr(self.s_expr.clone())?;
        Ok(self)
    }

    pub async fn limit(mut self, limit: Option<usize>, offset: usize) -> Result<Self> {
        let limit_plan = Limit {
            before_exchange: false,
            limit,
            offset,
        };
        self.s_expr =
            SExpr::create_unary(Arc::new(limit_plan.into()), Arc::new(self.s_expr.clone()));
        Ok(self)
    }

    pub async fn sort_column(
        self,
        columns: &[&str],
        order_by: Vec<(Expr, Option<bool>, Option<bool>)>,
        distinct: bool,
    ) -> Result<Self> {
        let select_list = parse_cols(self.bind_context.output_schema(), columns)?;
        self.sort_target(select_list, order_by, distinct).await
    }

    pub async fn sort(
        self,
        select_list: Vec<Expr>,
        order_by: Vec<(Expr, Option<bool>, Option<bool>)>,
        distinct: bool,
    ) -> Result<Self> {
        let select_list: Vec<SelectTarget> = select_list
            .into_iter()
            .map(|expr| SelectTarget::AliasedExpr {
                expr: Box::new(expr),
                alias: None,
            })
            .collect();
        self.sort_target(select_list, order_by, distinct).await
    }

    pub async fn sort_target(
        mut self,
        select_list: Vec<SelectTarget>,
        order_by: Vec<(Expr, Option<bool>, Option<bool>)>,
        distinct: bool,
    ) -> Result<Self> {
        let mut order = vec![];
        for (expr, asc, nulls_first) in order_by {
            order.push(OrderByExpr {
                expr,
                asc,
                nulls_first,
            });
        }
        let select_list = self
            .binder
            .normalize_select_list(&mut self.bind_context, select_list.as_slice())?;
        let aliases = select_list
            .items
            .iter()
            .map(|item| (item.alias.clone(), item.scalar.clone()))
            .collect::<Vec<_>>();
        let (mut scalar_items, projections) = self.binder.analyze_projection(
            &self.bind_context.aggregate_info,
            &self.bind_context.windows,
            &select_list,
        )?;
        let order_items = self.binder.analyze_order_items(
            &mut self.bind_context,
            &mut scalar_items,
            &aliases,
            &projections,
            &order,
            distinct,
        )?;
        self.s_expr = self.binder.bind_order_by(
            &self.bind_context,
            order_items,
            &select_list,
            &mut scalar_items,
            self.s_expr,
        )?;

        self.s_expr = self.binder.bind_projection(
            &mut self.bind_context,
            &projections,
            &scalar_items,
            self.s_expr,
        )?;
        self.s_expr = self
            .bind_context
            .add_internal_column_into_expr(self.s_expr.clone())?;

        Ok(self)
    }

    pub async fn except(mut self, dataframe: Dataframe) -> Result<Self> {
        let (s_expr, bind_context) = self.binder.bind_except(
            None,
            None,
            self.bind_context,
            dataframe.bind_context,
            self.s_expr,
            dataframe.s_expr,
        )?;
        self.s_expr = s_expr;
        self.bind_context = bind_context;
        Ok(self)
    }

    pub async fn intersect(mut self, dataframe: Dataframe) -> Result<Self> {
        let (s_expr, bind_context) = self.binder.bind_intersect(
            None,
            None,
            self.bind_context,
            dataframe.bind_context,
            self.s_expr,
            dataframe.s_expr,
        )?;
        self.s_expr = s_expr;
        self.bind_context = bind_context;
        Ok(self)
    }

    pub async fn join(
        mut self,
        from: Vec<(Option<&str>, &str)>,
        op: JoinOperator,
        condition: JoinCondition,
    ) -> Result<Self> {
        let mut table_ref = vec![];
        for (db, table_name) in from {
            let table = TableReference::Table {
                database: db.map(|db| Identifier::from_name(None, db)),
                table: Identifier::from_name(None, table_name),
                span: None,
                catalog: None,
                alias: None,
                temporal: None,
                consume: false,
                pivot: None,
                unpivot: None,
                sample: None,
            };
            table_ref.push(table);
        }
        let cross_joins = table_ref
            .iter()
            .cloned()
            .reduce(|left, right| TableReference::Join {
                span: None,
                join: Join {
                    op: op.clone(),
                    condition: condition.clone(),
                    left: Box::new(left),
                    right: Box::new(right),
                },
            })
            .unwrap();
        let (join_expr, ctx) = self
            .binder
            .bind_table_reference(&mut self.bind_context, &cross_joins)?;

        self.s_expr = join_expr;
        self.bind_context = ctx;
        Ok(self)
    }

    pub async fn union(mut self, dataframe: Dataframe) -> Result<Self> {
        let (s_expr, bind_context) = self.binder.bind_union(
            None,
            None,
            self.bind_context,
            dataframe.bind_context,
            self.s_expr,
            dataframe.s_expr,
            false,
            None,
        )?;
        self.s_expr = s_expr;
        self.bind_context = bind_context;
        Ok(self)
    }

    pub async fn union_distinct(mut self, dataframe: Dataframe) -> Result<Self> {
        let (s_expr, bind_context) = self.binder.bind_union(
            None,
            None,
            self.bind_context,
            dataframe.bind_context,
            self.s_expr,
            dataframe.s_expr,
            true,
            None,
        )?;
        self.s_expr = s_expr;
        self.bind_context = bind_context;
        Ok(self)
    }

    pub fn get_query_ctx(self) -> Arc<dyn TableContext> {
        self.query_ctx.clone()
    }

    pub fn get_expr(&self) -> &SExpr {
        &self.s_expr
    }
}

fn parse_cols(schema: DataSchemaRef, columns: &[&str]) -> Result<Vec<SelectTarget>> {
    for column in columns {
        if schema.field_with_name(column).is_err() {
            return Err(ErrorCode::UnknownColumn(format!(
                "Unknown column: '{}'",
                column
            )));
        }
    }

    Ok(columns
        .iter()
        .map(|c| SelectTarget::AliasedExpr {
            expr: Box::new(Expr::ColumnRef {
                span: None,
                column: ColumnRef {
                    database: None,
                    table: None,
                    column: ColumnID::Name(Identifier::from_name_with_quoted(None, *c, Some('`'))),
                },
            }),
            alias: None,
        })
        .collect())
}
