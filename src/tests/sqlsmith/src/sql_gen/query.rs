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

use std::mem;

use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FunctionCall;
use databend_common_ast::ast::GroupBy;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Join;
use databend_common_ast::ast::JoinCondition;
use databend_common_ast::ast::JoinOperator;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::OrderByExpr;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::SelectStmt;
use databend_common_ast::ast::SelectTarget;
use databend_common_ast::ast::SetExpr;
use databend_common_ast::ast::TableAlias;
use databend_common_ast::ast::TableReference;
use databend_common_ast::ast::WindowDefinition;
use databend_common_ast::ast::With;
use databend_common_ast::ast::CTE;
use databend_common_expression::infer_schema_type;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use rand::Rng;

use crate::sql_gen::Column;
use crate::sql_gen::SqlGenerator;
use crate::sql_gen::Table;

impl<'a, R: Rng> SqlGenerator<'a, R> {
    pub(crate) fn gen_query(&mut self) -> Query {
        self.cte_tables.clear();
        self.bound_tables.clear();
        self.bound_columns.clear();
        self.is_join = false;

        let with = self.gen_with();
        let body = self.gen_set_expr();
        let limit = self.gen_limit();
        let offset = self.gen_offset(limit.len());
        let order_by = self.gen_order_by(self.group_by.clone());

        Query {
            span: None,
            with,
            body,
            order_by,
            limit,
            offset,
            ignore_result: false,
        }
    }

    // Scalar, IN / NOT IN, ANY / SOME / ALL Subquery must return only one column
    // EXISTS / NOT EXISTS Subquery can return any columns
    pub(crate) fn gen_subquery(&mut self, one_column: bool) -> (Query, TableSchemaRef) {
        let current_cte_tables = mem::take(&mut self.cte_tables);
        let current_bound_tables = mem::take(&mut self.bound_tables);
        let current_bound_columns = mem::take(&mut self.bound_columns);
        let current_is_join = self.is_join;

        self.cte_tables = vec![];
        self.bound_tables = vec![];
        self.bound_columns = vec![];
        self.is_join = false;

        // Only generate simple subquery
        // TODO: complex subquery
        let from = self.gen_from();

        let len = if one_column {
            1
        } else {
            self.rng.gen_range(1..=5)
        };

        let name = self.gen_random_name();
        let mut fields = Vec::with_capacity(len);
        let mut select_list = Vec::with_capacity(len);
        for i in 0..len {
            let ty = self.gen_simple_data_type();
            let expr = self.gen_simple_expr(&ty);
            let col_name = format!("c{}{}", name, i);
            let table_type = infer_schema_type(&ty).unwrap();
            let field = TableField::new(&col_name, table_type);
            fields.push(field);
            let alias = Identifier::from_name(None, col_name);
            let target = SelectTarget::AliasedExpr {
                expr: Box::new(expr),
                alias: Some(alias),
            };
            select_list.push(target);
        }
        let schema = TableSchemaRefExt::create(fields);

        let select = SelectStmt {
            span: None,
            hints: None,
            distinct: false,
            top_n: None,
            select_list,
            from,
            selection: None,
            group_by: None,
            having: None,
            window_list: None,
            qualify: None,
        };
        let body = SetExpr::Select(Box::new(select));

        let query = Query {
            span: None,
            with: None,
            body,
            order_by: vec![],
            limit: vec![],
            offset: None,
            ignore_result: false,
        };

        self.cte_tables = current_cte_tables;
        self.bound_tables = current_bound_tables;
        self.bound_columns = current_bound_columns;
        self.is_join = current_is_join;

        (query, schema)
    }

    fn gen_with(&mut self) -> Option<With> {
        if self.rng.gen_bool(0.8) {
            return None;
        }

        let len = self.rng.gen_range(1..=3);
        let mut ctes = Vec::with_capacity(len);
        for _ in 0..len {
            let cte = self.gen_cte();
            ctes.push(cte);
        }

        Some(With {
            span: None,
            recursive: false,
            ctes,
        })
    }

    fn gen_cte(&mut self) -> CTE {
        let (subquery, schema) = self.gen_subquery(false);

        let (table, alias) = self.gen_subquery_table(schema);
        self.cte_tables.push(table);

        let materialized = self.rng.gen_bool(0.5);

        CTE {
            span: None,
            alias,
            materialized,
            query: Box::new(subquery),
        }
    }

    fn gen_set_expr(&mut self) -> SetExpr {
        match self.rng.gen_range(0..=9) {
            0..=9 => {
                let select = self.gen_select();
                SetExpr::Select(Box::new(select))
            }
            // TODO
            _ => unreachable!(),
        }
    }

    pub(crate) fn flip_coin(&mut self) -> bool {
        self.rng.gen_bool(0.5)
    }

    fn gen_order_by(&mut self, group_by: Option<GroupBy>) -> Vec<OrderByExpr> {
        let order_nums = self.rng.gen_range(1..5);
        let mut orders = Vec::with_capacity(order_nums);
        if self.flip_coin() {
            if let Some(group_by) = group_by {
                match group_by {
                    GroupBy::GroupingSets(group_by) => {
                        orders.extend(group_by[0].iter().map(|expr| OrderByExpr {
                            expr: expr.clone(),
                            asc: Some(self.flip_coin()),
                            nulls_first: Some(self.flip_coin()),
                        }))
                    }
                    GroupBy::Rollup(group_by)
                    | GroupBy::Cube(group_by)
                    | GroupBy::Normal(group_by) => {
                        orders.extend(group_by.iter().map(|expr| OrderByExpr {
                            expr: expr.clone(),
                            asc: Some(self.flip_coin()),
                            nulls_first: Some(self.flip_coin()),
                        }))
                    }
                    GroupBy::All => {
                        for _ in 0..order_nums {
                            let ty = self.gen_data_type();
                            let expr = self.gen_expr(&ty);
                            let order_by_expr = if self.rng.gen_bool(0.2) {
                                OrderByExpr {
                                    expr,
                                    asc: None,
                                    nulls_first: None,
                                }
                            } else {
                                OrderByExpr {
                                    expr,
                                    asc: Some(self.flip_coin()),
                                    nulls_first: Some(self.flip_coin()),
                                }
                            };
                            orders.push(order_by_expr);
                        }
                    }
                }
            } else {
                for _ in 0..order_nums {
                    let ty = self.gen_data_type();
                    let expr = self.gen_expr(&ty);
                    let order_by_expr = if self.rng.gen_bool(0.2) {
                        OrderByExpr {
                            expr,
                            asc: None,
                            nulls_first: None,
                        }
                    } else {
                        OrderByExpr {
                            expr,
                            asc: Some(self.flip_coin()),
                            nulls_first: Some(self.flip_coin()),
                        }
                    };
                    orders.push(order_by_expr);
                }
            }
        }
        orders
    }

    fn gen_limit(&mut self) -> Vec<Expr> {
        let mut res = Vec::new();
        if self.flip_coin() {
            let limit = Expr::Literal {
                span: None,
                value: Literal::UInt64(self.rng.gen_range(0..=100)),
            };
            res.push(limit);
            if self.flip_coin() {
                let offset = Expr::Literal {
                    span: None,
                    value: Literal::UInt64(self.rng.gen_range(5..=10)),
                };
                res.push(offset);
            }
        }
        res
    }

    fn gen_offset(&mut self, limit_len: usize) -> Option<Expr> {
        if self.flip_coin() && limit_len != 2 {
            return Some(Expr::Literal {
                span: None,
                value: Literal::UInt64(self.rng.gen_range(0..=10)),
            });
        }
        None
    }

    fn gen_select(&mut self) -> SelectStmt {
        self.windows_name.clear();
        let from = self.gen_from();
        let group_by = self.gen_group_by();
        self.group_by = group_by.clone();
        let window_list = self.gen_window_list();
        if let Some(window_list) = window_list {
            for window in window_list {
                self.windows_name.push(window.name.name)
            }
        }
        let select_list = self.gen_select_list(&group_by);
        let selection = self.gen_selection();
        SelectStmt {
            span: None,
            // TODO
            hints: None,
            distinct: self.rng.gen_bool(0.7),
            top_n: None,
            select_list,
            from,
            selection,
            group_by,
            having: self.gen_selection(),
            window_list: self.gen_window_list(),
            qualify: None, // todo: add qualify.
        }
    }

    fn gen_window_list(&mut self) -> Option<Vec<WindowDefinition>> {
        if self.rng.gen_bool(0.1) {
            let mut res = vec![];
            for _ in 0..self.rng.gen_range(1..3) {
                let name = self.gen_random_name();
                let window_name = format!("w_{}", name);
                let spec = self.gen_window_spec();
                let window_def = WindowDefinition {
                    name: Identifier::from_name(None, window_name),
                    spec,
                };
                res.push(window_def);
            }
            return Some(res);
        }
        None
    }

    fn gen_group_by(&mut self) -> Option<GroupBy> {
        if self.rng.gen_bool(0.8) {
            return None;
        }
        let group_cap = self.rng.gen_range(1..=5);
        let mut groupby_items = Vec::with_capacity(group_cap);

        for _ in 0..group_cap {
            let ty = self.gen_data_type();
            let groupby_item = self.gen_expr(&ty);
            let groupby_item = self.rewrite_position_expr(groupby_item);
            groupby_items.push(groupby_item);
        }

        match self.rng.gen_range(0..=4) {
            0 => Some(GroupBy::Normal(groupby_items)),
            1 => Some(GroupBy::All),
            2 => Some(GroupBy::GroupingSets(vec![groupby_items])),
            3 => Some(GroupBy::Cube(groupby_items)),
            4 => Some(GroupBy::Rollup(groupby_items)),
            _ => unreachable!(),
        }
    }

    fn gen_select_list(&mut self, group_by: &Option<GroupBy>) -> Vec<SelectTarget> {
        let mut targets = Vec::with_capacity(5);

        let generate_target = |expr: Expr| SelectTarget::AliasedExpr {
            expr: Box::new(expr),
            alias: None,
        };

        match group_by {
            Some(GroupBy::Normal(group_by))
            | Some(GroupBy::Cube(group_by))
            | Some(GroupBy::Rollup(group_by)) => {
                let ty = self.gen_data_type();
                let agg_expr = self.gen_agg_func(&ty);
                targets.push(SelectTarget::AliasedExpr {
                    expr: Box::new(agg_expr),
                    alias: None,
                });
                targets.extend(group_by.iter().map(|expr| SelectTarget::AliasedExpr {
                    expr: Box::new(expr.clone()),
                    alias: None,
                }));
            }
            Some(GroupBy::All) => {
                let select_num = self.rng.gen_range(1..=5);
                for _ in 0..select_num {
                    let ty = self.gen_data_type();
                    let expr = if self.rng.gen_bool(0.8) {
                        self.gen_agg_func(&ty)
                    } else {
                        self.gen_expr(&ty)
                    };
                    let target = generate_target(expr);
                    targets.push(target);
                }
            }
            Some(GroupBy::GroupingSets(group_by)) => {
                let ty = self.gen_data_type();
                let agg_expr = self.gen_agg_func(&ty);
                targets.push(SelectTarget::AliasedExpr {
                    expr: Box::new(agg_expr),
                    alias: None,
                });
                targets.extend(group_by[0].iter().map(|expr| SelectTarget::AliasedExpr {
                    expr: Box::new(expr.clone()),
                    alias: None,
                }));
            }
            None => {
                let select_num = self.rng.gen_range(1..=7);
                for _ in 0..select_num {
                    let ty = self.gen_data_type();
                    let expr = self.gen_expr(&ty);
                    let target = generate_target(expr);
                    targets.push(target);
                }
            }
        }

        targets
    }

    fn gen_from(&mut self) -> Vec<TableReference> {
        let mut table_refs = vec![];
        // TODO: generate more table reference
        // let table_ref_num = self.rng.gen_range(1..=3);
        match self.rng.gen_range(0..=10) {
            0..=6 => {
                let (table_ref, _) = self.gen_table_ref();
                table_refs.push(table_ref);
            }
            // join
            7..=8 => {
                self.is_join = true;
                let join = self.gen_join_table_ref();
                table_refs.push(join);
            }
            // subquery
            9 => {
                let subquery = self.gen_subquery_table_ref();
                table_refs.push(subquery);
            }
            10 => {
                let table_func = self.gen_table_func();
                table_refs.push(table_func);
            }
            // TODO
            _ => unreachable!(),
        }
        table_refs
    }

    fn gen_table_ref(&mut self) -> (TableReference, TableSchemaRef) {
        let len = self.tables.len() + self.cte_tables.len();
        let i = self.rng.gen_range(0..len);

        let table = if i < self.tables.len() {
            self.tables[i].clone()
        } else {
            self.cte_tables[len - i - 1].clone()
        };
        let schema = table.schema.clone();
        let table_name = Identifier::from_name(None, table.name.clone());

        self.bound_table(table);

        let table_ref = TableReference::Table {
            span: None,
            // TODO
            catalog: None,
            // TODO
            database: None,
            table: table_name,
            // TODO
            alias: None,
            // TODO
            temporal: None,
            consume: false,
            // TODO
            pivot: None,
            // TODO
            unpivot: None,
            sample: None,
        };
        (table_ref, schema)
    }

    // Only test:
    // [numbers, numbers_mt, numbers_local, generate_series, range]
    // No need to test:
    // [fuse_snapshot,fuse_segment, fuse_block, fuse_column, fuse_statistic, clustering_information,
    // sync_crash_me, async_crash_me ,infer_schema ,list_stage,
    // ai_to_sql, execute_background_job, license_info, suggested_background_tasks ,tenant_quota]
    fn gen_table_func(&mut self) -> TableReference {
        let tbl_func = [
            "numbers",
            "numbers_mt",
            "numbers_local",
            "generate_series",
            "range",
        ];
        let name = tbl_func[self.rng.gen_range(0..=4)];

        match name {
            "numbers" | "numbers_mt" | "numbers_local" => {
                let table = Table {
                    name: name.to_string(),
                    schema: TableSchemaRefExt::create(vec![TableField::new(
                        "number",
                        TableDataType::Number(NumberDataType::UInt64),
                    )]),
                };
                self.bound_table(table);
                TableReference::TableFunction {
                    span: None,
                    lateral: false,
                    name: Identifier::from_name(None, name),
                    params: vec![Expr::Literal {
                        span: None,
                        value: Literal::UInt64(self.rng.gen_range(0..=10)),
                    }],
                    named_params: vec![],
                    alias: None,
                }
            }
            "generate_series" | "range" => {
                let idx = self.rng.gen_range(0..=2);
                let mut gen_expr = |idx: i32| -> (TableDataType, Expr) {
                    match idx {
                        0 => {
                            let arg = Expr::Literal {
                                span: None,
                                value: Literal::UInt64(self.rng.gen_range(0..=10000000000000)),
                            };
                            (TableDataType::Timestamp, Expr::FunctionCall {
                                span: None,
                                func: FunctionCall {
                                    distinct: false,
                                    name: Identifier::from_name(None, "to_timestamp".to_string()),
                                    args: vec![arg],
                                    params: vec![],
                                    window: None,
                                    lambda: None,
                                },
                            })
                        }
                        1 => {
                            let arg = Expr::Literal {
                                span: None,
                                value: Literal::UInt64(self.rng.gen_range(0..=1000000)),
                            };
                            (TableDataType::Date, Expr::FunctionCall {
                                span: None,
                                func: FunctionCall {
                                    distinct: false,
                                    name: Identifier::from_name(None, "to_date".to_string()),
                                    args: vec![arg],
                                    params: vec![],
                                    window: None,
                                    lambda: None,
                                },
                            })
                        }
                        2 => (
                            TableDataType::Number(NumberDataType::Int64),
                            Expr::Literal {
                                span: None,
                                value: Literal::UInt64(self.rng.gen_range(0..=1000)),
                            },
                        ),
                        _ => unreachable!(),
                    }
                };
                let (ty1, param1) = gen_expr(idx);
                let (_, param2) = gen_expr(idx);
                let table = Table {
                    name: name.to_string(),
                    schema: TableSchemaRefExt::create(vec![TableField::new(name, ty1)]),
                };
                let (_, param3) = gen_expr(2);
                self.bound_table(table);

                TableReference::TableFunction {
                    span: None,
                    lateral: false,
                    name: Identifier::from_name(None, name),
                    params: if self.rng.gen_bool(0.5) {
                        vec![param1, param2]
                    } else {
                        vec![param1, param2, param3]
                    },
                    named_params: vec![],
                    alias: None,
                }
            }
            _ => unreachable!(),
        }
    }

    fn gen_join_table_ref(&mut self) -> TableReference {
        let (left_table, left_schema) = self.gen_table_ref();
        let (right_table, right_schema) = self.gen_table_ref();

        let op = match self.rng.gen_range(0..=8) {
            0 => JoinOperator::Inner,
            1 => JoinOperator::LeftOuter,
            2 => JoinOperator::RightOuter,
            3 => JoinOperator::FullOuter,
            4 => JoinOperator::LeftSemi,
            5 => JoinOperator::LeftAnti,
            6 => JoinOperator::RightSemi,
            7 => JoinOperator::RightAnti,
            8 => JoinOperator::CrossJoin,
            _ => unreachable!(),
        };

        let condition = match self.rng.gen_range(0..=2) {
            0 => {
                let ty = self.gen_data_type();
                let expr = self.gen_expr(&ty);
                JoinCondition::On(Box::new(expr))
            }
            1 => {
                let left_fields = left_schema.fields();
                let right_fields = right_schema.fields();

                let mut names = Vec::new();
                for left_field in left_fields {
                    for right_field in right_fields {
                        if left_field.name == right_field.name {
                            names.push(left_field.name.clone());
                        }
                    }
                }
                if names.is_empty() {
                    JoinCondition::Natural
                } else {
                    let mut num = self.rng.gen_range(1..=3);
                    if num > names.len() {
                        num = names.len();
                    }
                    let mut idents = Vec::with_capacity(num);
                    for _ in 0..num {
                        let idx = self.rng.gen_range(0..names.len());
                        idents.push(Identifier::from_name(None, names[idx].clone()));
                    }
                    JoinCondition::Using(idents)
                }
            }
            2 => JoinCondition::Natural,
            _ => unreachable!(),
        };

        let condition = match op {
            // Outer joins can not work with `JoinCondition::None`
            JoinOperator::LeftOuter | JoinOperator::RightOuter | JoinOperator::FullOuter => {
                condition
            }
            // CrossJoin can only work with `JoinCondition::None`
            JoinOperator::CrossJoin => JoinCondition::None,
            _ => {
                if self.rng.gen_bool(0.2) {
                    JoinCondition::None
                } else {
                    condition
                }
            }
        };

        let join = Join {
            op,
            condition,
            left: Box::new(left_table),
            right: Box::new(right_table),
        };
        TableReference::Join { span: None, join }
    }

    fn gen_subquery_table_ref(&mut self) -> TableReference {
        let (subquery, schema) = self.gen_subquery(false);

        let (table, alias) = self.gen_subquery_table(schema);
        self.bound_table(table);

        TableReference::Subquery {
            span: None,
            lateral: false,
            subquery: Box::new(subquery),
            alias: Some(alias),
        }
    }

    fn gen_selection(&mut self) -> Option<Expr> {
        match self.rng.gen_range(0..=9) {
            0..=5 => Some(self.gen_expr(&DataType::Boolean)),
            6 => {
                let ty = self.gen_simple_data_type();
                Some(self.gen_expr(&ty))
            }
            _ => None,
        }
    }

    pub(crate) fn gen_subquery_table(&mut self, schema: TableSchemaRef) -> (Table, TableAlias) {
        let name = self.gen_random_name();
        let table_name = format!("t{}", name);
        let mut columns = Vec::with_capacity(schema.num_fields());
        for field in schema.fields() {
            let column = Identifier::from_name(None, field.name.clone());
            columns.push(column);
        }
        let alias = TableAlias {
            name: Identifier::from_name(None, table_name.clone()),
            columns,
        };
        let table = Table::new(table_name, schema);

        (table, alias)
    }

    pub(crate) fn bound_table(&mut self, table: Table) {
        for (i, field) in table.schema.fields().iter().enumerate() {
            let column = Column {
                table_name: table.name.clone(),
                name: field.name.clone(),
                index: i + 1,
                data_type: DataType::from(&field.data_type),
            };
            self.bound_columns.push(column);
        }
        self.bound_tables.push(table);
    }

    // rewrite position expr in group by and order by,
    // avoiding `GROUP BY position n is not in select list` errors
    fn rewrite_position_expr(&mut self, expr: Expr) -> Expr {
        if let Expr::Literal {
            value: Literal::UInt64(n),
            ..
        } = expr
        {
            let pos = n % 3 + 1;
            Expr::Literal {
                span: None,
                value: Literal::UInt64(pos),
            }
        } else {
            expr
        }
    }
}
