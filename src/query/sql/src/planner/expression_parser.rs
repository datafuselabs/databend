// Copyright 2022 Datafuse Labs.
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

use common_ast::parser::parse_comma_separated_exprs;
use common_ast::parser::tokenize_sql;
use common_ast::Backtrace;
use common_ast::Dialect;
use common_base::base::tokio::task::block_in_place;
use common_base::runtime::Runtime;
use common_catalog::catalog::CATALOG_DEFAULT;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_config::GlobalConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::DataBlock;
use common_expression::Evaluator;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_expression::RemoteExpr;
use common_expression::Scalar;
use common_expression::TableField;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use common_meta_app::schema::TableInfo;
use common_settings::Settings;
use parking_lot::RwLock;

use crate::planner::binder::BindContext;
use crate::planner::semantic::NameResolutionContext;
use crate::planner::semantic::TypeChecker;
use crate::BaseTableColumn;
use crate::ColumnBinding;
use crate::ColumnEntry;
use crate::Metadata;
use crate::Visibility;

pub fn parse_exprs(
    ctx: Arc<dyn TableContext>,
    table_meta: Arc<dyn Table>,
    unwrap_tuple: bool,
    sql: &str,
) -> Result<Vec<Expr>> {
    let settings = Settings::default_settings("", GlobalConfig::instance())?;
    let mut bind_context = BindContext::new();
    let metadata = Arc::new(RwLock::new(Metadata::default()));
    let table_index = metadata.write().add_table(
        CATALOG_DEFAULT.to_owned(),
        "default".to_string(),
        table_meta,
        None,
        false,
    );

    let columns = metadata.read().columns_by_table_index(table_index);
    let table = metadata.read().table(table_index).clone();
    for (index, column) in columns.iter().enumerate() {
        let column_binding = match column {
            ColumnEntry::BaseTableColumn(BaseTableColumn {
                column_name,
                data_type,
                path_indices,
                ..
            }) => ColumnBinding {
                database_name: Some("default".to_string()),
                table_name: Some(table.name().to_string()),
                column_name: column_name.clone(),
                index,
                data_type: Box::new(data_type.into()),
                visibility: if path_indices.is_some() {
                    Visibility::InVisible
                } else {
                    Visibility::Visible
                },
            },

            _ => {
                return Err(ErrorCode::Internal("Invalid column entry"));
            }
        };

        bind_context.add_column_binding(column_binding);
    }

    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;
    let mut type_checker =
        TypeChecker::new(&bind_context, ctx, &name_resolution_ctx, metadata, &[]);

    let sql_dialect = Dialect::MySQL;
    let tokens = tokenize_sql(sql)?;
    let backtrace = Backtrace::new();
    let tokens = if unwrap_tuple {
        &tokens[1..tokens.len() - 1]
    } else {
        &tokens
    };
    let ast_exprs = parse_comma_separated_exprs(tokens, sql_dialect, &backtrace)?;
    let exprs = ast_exprs
        .iter()
        .map(|ast| {
            let (scalar, _) = *block_in_place(|| {
                Runtime::block_on_with_current(type_checker.resolve(ast, None))
            })?;
            let expr = scalar.as_expr_with_col_index()?;
            Ok(expr)
        })
        .collect::<Result<_>>()?;

    Ok(exprs)
}

pub fn parse_to_remote_string_expr(
    ctx: Arc<dyn TableContext>,
    table_meta: Arc<dyn Table>,
    unwrap_tuple: bool,
    sql: &str,
) -> Result<RemoteExpr<String>> {
    let schema = table_meta.schema();
    let exprs = parse_exprs(ctx, table_meta, unwrap_tuple, sql)?;
    let exprs: Vec<RemoteExpr<String>> = exprs
        .iter()
        .map(|expr| {
            expr.project_column_ref(|index| schema.field(*index).name().to_string())
                .as_remote_expr()
        })
        .collect();

    if exprs.len() == 1 {
        Ok(exprs[0].clone())
    } else {
        Err(ErrorCode::BadDataValueType(format!(
            "Expected single expr, but got {}",
            exprs.len()
        )))
    }
}

#[derive(Default)]
struct DummyTable {
    info: TableInfo,
}
impl Table for DummyTable {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn get_table_info(&self) -> &common_meta_app::schema::TableInfo {
        &self.info
    }
}

pub fn field_default_value(ctx: Arc<dyn TableContext>, field: &TableField) -> Result<Scalar> {
    let data_type = field.data_type();
    let data_type = DataType::from(data_type);

    match field.default_expr() {
        Some(default_expr) => {
            let table: Arc<dyn Table> = Arc::new(DummyTable::default());
            let mut expr = parse_exprs(ctx.clone(), table.clone(), false, default_expr)?;
            let mut expr = expr.remove(0);

            if expr.data_type() != &data_type {
                expr = Expr::Cast {
                    span: None,
                    is_try: data_type.is_nullable(),
                    expr: Box::new(expr),
                    dest_type: data_type,
                };
            }

            let dummy_block = DataBlock::new(vec![], 1);
            let evaluator =
                Evaluator::new(&dummy_block, FunctionContext::default(), &BUILTIN_FUNCTIONS);
            let result = evaluator.run(&expr)?;

            match result {
                common_expression::Value::Scalar(s) => Ok(s),
                common_expression::Value::Column(c) if c.len() == 1 => {
                    let value = unsafe { c.index_unchecked(0) };
                    Ok(value.to_owned())
                }
                _ => Err(ErrorCode::BadDataValueType(format!(
                    "Invalid default value for column: {}, must be constant, actual: {}",
                    field.name(),
                    result
                ))),
            }
        }
        None => Ok(Scalar::default_value(&data_type)),
    }
}
