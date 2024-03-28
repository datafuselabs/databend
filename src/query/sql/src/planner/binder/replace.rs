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

use std::str::FromStr;
use std::sync::Arc;

use databend_common_ast::ast::InsertSource;
use databend_common_ast::ast::ReplaceStmt;
use databend_common_ast::ast::Statement;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::OnErrorMode;

use crate::binder::Binder;
use crate::normalize_identifier;
use crate::optimizer::optimize;
use crate::optimizer::OptimizerContext;
use crate::plans::insert::InsertValue;
use crate::plans::CopyIntoTableMode;
use crate::plans::InsertInputSource;
use crate::plans::Plan;
use crate::plans::Replace;
use crate::BindContext;
impl Binder {
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_replace(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &ReplaceStmt,
    ) -> Result<Plan> {
        let ReplaceStmt {
            catalog,
            database,
            table,
            on_conflict_columns,
            columns,
            source,
            delete_when,
            hints: _,
        } = stmt;

        let (catalog_name, database_name, table_name) =
            self.normalize_object_identifier_triple(catalog, database, table);

        let table = self
            .ctx
            .get_table(&catalog_name, &database_name, &table_name)
            .await?;
        let table_id = table.get_id();

        let schema = if columns.is_empty() {
            table.schema()
        } else {
            let schema = table.schema();
            let field_indexes = columns
                .iter()
                .map(|ident| {
                    schema.index_of(&normalize_identifier(ident, &self.name_resolution_ctx).name)
                })
                .collect::<Result<Vec<_>>>()?;
            Arc::new(schema.project(&field_indexes))
        };

        let on_conflict_fields = on_conflict_columns
            .iter()
            .map(|ident| {
                schema
                    .field_with_name(&normalize_identifier(ident, &self.name_resolution_ctx).name)
                    .cloned()
            })
            .collect::<Result<Vec<_>>>()?;

        let input_source: Result<InsertInputSource> = match source.clone() {
            InsertSource::Streaming {
                format,
                rest_str,
                start,
            } => {
                if format.to_uppercase() == "VALUES" {
                    let data = rest_str.trim_end_matches(';').trim_start().to_owned();
                    Ok(InsertInputSource::Values(InsertValue::RawValues {
                        data,
                        start,
                    }))
                } else {
                    Ok(InsertInputSource::StreamingWithFormat(format, start, None))
                }
            }
            InsertSource::StreamingV2 {
                settings,
                on_error_mode,
                start,
            } => {
                let params = settings.to_meta_ast().try_into()?;
                Ok(InsertInputSource::StreamingWithFileFormat {
                    format: params,
                    start,
                    on_error_mode: OnErrorMode::from_str(
                        &on_error_mode.unwrap_or("abort".to_string()),
                    )?,
                    input_context_option: None,
                })
            }
            InsertSource::Values { rows } => {
                let mut new_rows = Vec::with_capacity(rows.len());
                for row in rows {
                    let new_row = bind_context
                        .exprs_to_scalar(
                            &row,
                            &Arc::new(schema.clone().into()),
                            self.ctx.clone(),
                            &self.name_resolution_ctx,
                            self.metadata.clone(),
                        )
                        .await?;
                    new_rows.push(new_row);
                }
                Ok(InsertInputSource::Values(InsertValue::Values {
                    rows: new_rows,
                }))
            }
            InsertSource::RawValues { rest_str, start } => {
                let values_str = rest_str.trim_end_matches(';').trim_start().to_owned();
                match self.ctx.get_stage_attachment() {
                    Some(attachment) => {
                        let plan = self
                            .bind_copy_from_attachment(
                                bind_context,
                                attachment,
                                catalog_name.clone(),
                                database_name.clone(),
                                table_name.clone(),
                                Arc::new(schema.clone().into()),
                                &values_str,
                                CopyIntoTableMode::Replace,
                            )
                            .await?;
                        Ok(InsertInputSource::Stage(Box::new(plan)))
                    }
                    None => Ok(InsertInputSource::Values(InsertValue::RawValues {
                        data: values_str,
                        start,
                    })),
                }
            }
            InsertSource::Select { query } => {
                let statement = Statement::Query(query);
                let select_plan = self.bind_statement(bind_context, &statement).await?;
                if let Plan::Query { s_expr, .. } = &select_plan {
                    if !self.check_sexpr_top(s_expr)? {
                        return Err(ErrorCode::SemanticError(
                            "replace source can't contain udf functions".to_string(),
                        ));
                    }
                }
                let opt_ctx = OptimizerContext::new(self.ctx.clone(), self.metadata.clone())
                    .with_enable_distributed_optimization(false);
                let optimized_plan = optimize(opt_ctx, select_plan).await?;
                Ok(InsertInputSource::SelectPlan(Box::new(optimized_plan)))
            }
        };

        let plan = Replace {
            catalog: catalog_name.to_string(),
            database: database_name.to_string(),
            table: table_name,
            table_id,
            on_conflict_fields,
            schema,
            source: input_source?,
            delete_when: delete_when.clone(),
        };

        Ok(Plan::Replace(Box::new(plan)))
    }
}
