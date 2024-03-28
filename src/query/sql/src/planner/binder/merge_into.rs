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

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Join;
use databend_common_ast::ast::JoinCondition;
use databend_common_ast::ast::JoinOperator;
use databend_common_ast::ast::JoinOperator::Inner;
use databend_common_ast::ast::JoinOperator::RightAnti;
use databend_common_ast::ast::JoinOperator::RightOuter;
use databend_common_ast::ast::MatchOperation;
use databend_common_ast::ast::MatchedClause;
use databend_common_ast::ast::MergeIntoStmt;
use databend_common_ast::ast::TableReference;
use databend_common_ast::ast::UnmatchedClause;
use databend_common_catalog::plan::InternalColumn;
use databend_common_catalog::plan::InternalColumnType;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::ROW_ID_COL_NAME;
use indexmap::IndexMap;

use crate::binder::wrap_cast;
use crate::binder::Binder;
use crate::binder::InternalColumnBinding;
use crate::normalize_identifier;
use crate::optimizer::SExpr;
use crate::plans::BoundColumnRef;
use crate::plans::MatchedEvaluator;
use crate::plans::MaterializedCte;
use crate::plans::MergeInto;
use crate::plans::Plan;
use crate::plans::RelOperator;
use crate::plans::UnmatchedEvaluator;
use crate::BindContext;
use crate::ColumnBinding;
use crate::ColumnBindingBuilder;
use crate::ColumnEntry;
use crate::IndexType;
use crate::ScalarBinder;
use crate::ScalarExpr;
use crate::Visibility;
use crate::DUMMY_COLUMN_INDEX;

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum MergeIntoType {
    MatechedOnly,
    FullOperation,
    InsertOnly,
}

// Optimize Rule:
// for now we think right source table is small table in default.
// 1. insert only:
//      right anti join
// 2. (macthed and unmatched)
//      right outer
// 3. matched only:
//      inner join
impl Binder {
    #[allow(warnings)]
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_merge_into(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &MergeIntoStmt,
    ) -> Result<Plan> {
        if !self
            .ctx
            .get_settings()
            .get_enable_experimental_merge_into()
            .unwrap_or_default()
        {
            return Err(ErrorCode::Unimplemented(
                "merge into is experimental for now, you can use 'set enable_experimental_merge_into = 1' to set it up",
            ));
        };

        let (matched_clauses, unmatched_clauses) = stmt.split_clauses();
        let merge_type = get_merge_type(matched_clauses.len(), unmatched_clauses.len())?;

        let plan = self
            .bind_merge_into_with_join_type(
                bind_context,
                stmt,
                match merge_type {
                    MergeIntoType::MatechedOnly => Inner,
                    MergeIntoType::InsertOnly => RightAnti,
                    _ => RightOuter,
                },
                matched_clauses.clone(),
                unmatched_clauses.clone(),
                merge_type.clone(),
            )
            .await?;

        if merge_type == MergeIntoType::InsertOnly && !insert_only(&plan) {
            return Err(ErrorCode::SemanticError(
                "for unmatched clause, then condition and exprs can only have source fields",
            ));
        }

        Ok(Plan::MergeInto(Box::new(plan)))
    }

    fn can_try_update_column_only(&self, matched_clauses: &[MatchedClause]) -> bool {
        if matched_clauses.len() == 1 {
            let matched_clause = &matched_clauses[0];
            if matched_clause.selection.is_none() {
                if let MatchOperation::Update {
                    update_list,
                    is_star,
                } = &matched_clause.operation
                {
                    let mut is_column_only = true;
                    for update_expr in update_list {
                        is_column_only =
                            is_column_only && matches!(update_expr.expr, Expr::ColumnRef { .. });
                    }
                    return is_column_only || *is_star;
                }
            }
        }
        false
    }

    async fn bind_merge_into_with_join_type(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &MergeIntoStmt,
        join_type: JoinOperator,
        matched_clauses: Vec<MatchedClause>,
        unmatched_clauses: Vec<UnmatchedClause>,
        merge_type: MergeIntoType,
    ) -> Result<MergeInto> {
        let MergeIntoStmt {
            catalog,
            database,
            table_ident,
            source,
            target_alias,
            join_expr,
            merge_options,
            ..
        } = stmt;

        if merge_options.is_empty() {
            return Err(ErrorCode::BadArguments(
                "at least one matched or unmatched clause for merge into",
            ));
        }

        let mut unmatched_evaluators =
            Vec::<UnmatchedEvaluator>::with_capacity(unmatched_clauses.len());
        let mut matched_evaluators = Vec::<MatchedEvaluator>::with_capacity(matched_clauses.len());
        // check clause semantic
        MergeIntoStmt::check_multi_match_clauses_semantic(&matched_clauses)?;
        MergeIntoStmt::check_multi_unmatch_clauses_semantic(&unmatched_clauses)?;

        let (catalog_name, database_name, table_name) =
            self.normalize_object_identifier_triple(catalog, database, table_ident);

        let table = self
            .ctx
            .get_table(&catalog_name, &database_name, &table_name)
            .await?;
        let table_id = table.get_id();
        let table_schema = table.schema();

        // get target_table_reference
        let target_table = TableReference::Table {
            span: None,
            catalog: catalog.clone(),
            database: database.clone(),
            table: table_ident.clone(),
            alias: target_alias.clone(),
            travel_point: None,
            since_point: None,
            pivot: None,
            unpivot: None,
        };

        // get_source_table_reference
        let source_data = source.transform_table_reference();

        // bind source data
        let (mut source_expr, mut source_context) =
            self.bind_single_table(bind_context, &source_data).await?;
        // remove stream column.
        source_context
            .columns
            .retain(|v| v.visibility == Visibility::Visible);

        // Wrap `LogicalMaterializedCte` to `source_expr`
        for (_, cte_info) in self.ctes_map.iter().rev() {
            if !cte_info.materialized || cte_info.used_count == 0 {
                continue;
            }
            let cte_s_expr = self.m_cte_bound_s_expr.get(&cte_info.cte_idx).unwrap();
            let left_output_columns = cte_info.columns.clone();
            source_expr = SExpr::create_binary(
                Arc::new(RelOperator::MaterializedCte(MaterializedCte {
                    left_output_columns,
                    cte_idx: cte_info.cte_idx,
                })),
                Arc::new(cte_s_expr.clone()),
                Arc::new(source_expr),
            );
        }
        if !self.check_sexpr_top(&source_expr)? {
            return Err(ErrorCode::SemanticError(
                "replace source can't contain udf functions".to_string(),
            ));
        }

        // add all left source columns for read
        // todo: (JackTan25) do column prune after finish "split expr for target and source"
        let mut columns_set = HashSet::<IndexType>::new();

        let update_or_insert_columns_star =
            if self.has_star_clause(&matched_clauses, &unmatched_clauses) {
                // when there are "update *"/"insert *", we need to get the index of correlated columns in source.
                let default_target_table_schema = table_schema.remove_computed_fields();
                let mut update_columns =
                    HashMap::with_capacity(default_target_table_schema.num_fields());
                let source_output_columns = &source_context.columns;
                // we use Vec as the value, because there could be duplicate names
                let mut name_map = HashMap::<String, Vec<ColumnBinding>>::new();
                for column in source_output_columns {
                    name_map
                        .entry(column.column_name.clone())
                        .or_default()
                        .push(column.clone());
                }

                for (field_idx, field) in default_target_table_schema.fields.iter().enumerate() {
                    let column = match name_map.get(field.name()) {
                        None => {
                            return Err(ErrorCode::SemanticError(
                                format!("can't find {} in source output", field.name).to_string(),
                            ));
                        }
                        Some(indices) => {
                            if indices.len() != 1 {
                                return Err(ErrorCode::SemanticError(
                                format!(
                                    "there should be only one {} in source output,but we get {}",
                                    field.name,
                                    indices.len()
                                )
                                .to_string(),
                            ));
                            }

                            indices[0].clone()
                        }
                    };
                    let column = ColumnBindingBuilder::new(
                        field.name.to_string(),
                        column.index,
                        column.data_type.clone(),
                        Visibility::Visible,
                    )
                    .build();
                    let col = ScalarExpr::BoundColumnRef(BoundColumnRef { span: None, column });

                    update_columns.insert(field_idx, col);
                }
                Some(update_columns)
            } else {
                None
            };

        // Todo: (JackTan25) Maybe we can remove bind target_table
        // when the target table has been binded in bind_merge_into_source
        // bind table for target table
        let (mut target_expr, mut target_context) = self
            .bind_single_table(&mut source_context, &target_table)
            .await?;

        if table.change_tracking_enabled() && merge_type != MergeIntoType::InsertOnly {
            if let RelOperator::Scan(scan) = target_expr.plan() {
                let new_scan = scan.update_stream_columns(true);
                target_expr = SExpr::create_leaf(Arc::new(new_scan.into()))
            }
        }

        // add internal_column (_row_id)
        let table_index = self
            .metadata
            .read()
            .get_table_index(Some(database_name.as_str()), table_name.as_str())
            .expect("can't get target_table binding");

        let row_id_column_binding = InternalColumnBinding {
            database_name: Some(database_name.clone()),
            table_name: Some(table_name.clone()),
            internal_column: InternalColumn {
                column_name: ROW_ID_COL_NAME.to_string(),
                column_type: InternalColumnType::RowId,
            },
        };

        let column_binding = target_context.add_internal_column_binding(
            &row_id_column_binding,
            self.metadata.clone(),
            true,
        )?;

        target_expr = SExpr::add_internal_column_index(
            &target_expr,
            table_index,
            column_binding.index,
            &None,
        );

        self.metadata
            .write()
            .set_table_row_id_index(table_index, column_binding.index);

        // add row_id_idx
        if merge_type != MergeIntoType::InsertOnly {
            columns_set.insert(column_binding.index);
        }

        // add join, we use _row_id to check_duplicate join row.
        let join = Join {
            op: join_type,
            condition: JoinCondition::On(Box::new(join_expr.clone())),
            left: Box::new(target_table),
            // use source as build table
            right: Box::new(source_data.clone()),
        };

        let (join_sexpr, mut bind_ctx) = self
            .bind_join(
                bind_context,
                target_context.clone(),
                source_context,
                target_expr,
                source_expr,
                &join,
            )
            .await?;
        {
            // add join used column idx
            let join_column_set = bind_ctx.column_set();
            columns_set = columns_set.union(&join_column_set).cloned().collect();
        }

        let has_update = self.has_update(&matched_clauses);
        let update_row_version = if table.change_tracking_enabled() && has_update {
            Some(Self::update_row_version(
                table.schema_with_stream(),
                &bind_ctx.columns,
                Some(&database_name),
                Some(&table_name),
            )?)
        } else {
            None
        };

        let name_resolution_ctx = self.name_resolution_ctx.clone();
        let mut scalar_binder = ScalarBinder::new(
            &mut bind_ctx,
            self.ctx.clone(),
            &name_resolution_ctx,
            self.metadata.clone(),
            &[],
            HashMap::new(),
            Box::new(IndexMap::new()),
        );

        let column_entries = self.metadata.read().columns_by_table_index(table_index);
        let mut field_index_map = HashMap::<usize, String>::new();
        // if true, read all columns of target table
        let has_update = self.has_update(&matched_clauses);
        if has_update {
            for (idx, field) in table.schema_with_stream().fields().iter().enumerate() {
                let used_idx = self.find_column_index(&column_entries, field.name())?;
                columns_set.insert(used_idx);
                field_index_map.insert(idx, used_idx.to_string());
            }
        }

        let target_name = if let Some(target_identify) = target_alias {
            normalize_identifier(&target_identify.name, &self.name_resolution_ctx)
                .name
                .clone()
        } else {
            table_name.clone()
        };

        // bind matched clause columns and add update fields and exprs
        for clause in &matched_clauses {
            matched_evaluators.push(
                self.bind_matched_clause(
                    &mut scalar_binder,
                    clause,
                    &mut columns_set,
                    table_schema.clone(),
                    update_or_insert_columns_star.clone(),
                    update_row_version.clone(),
                    target_name.as_ref(),
                )
                .await?,
            );
        }

        // bind not matched clause columns and add insert exprs
        for clause in &unmatched_clauses {
            unmatched_evaluators.push(
                self.bind_unmatched_clause(
                    &mut scalar_binder,
                    clause,
                    &mut columns_set,
                    table_schema.clone(),
                    update_or_insert_columns_star.clone(),
                )
                .await?,
            );
        }
        let mut split_idx = DUMMY_COLUMN_INDEX;
        // find any target table column index for merge_into_split
        for column in self.metadata.read().columns() {
            if column.table_index().is_some()
                && *column.table_index().as_ref().unwrap() == table_index
                && column.index() != column_binding.index
            {
                split_idx = column.index();
                break;
            }
        }
        assert!(split_idx != DUMMY_COLUMN_INDEX);

        Ok(MergeInto {
            catalog: catalog_name.to_string(),
            database: database_name.to_string(),
            table: table_name,
            target_alias: target_alias.clone(),
            table_id,
            bind_context: Box::new(bind_ctx),
            meta_data: self.metadata.clone(),
            input: Box::new(join_sexpr.clone()),
            columns_set: Box::new(columns_set),
            matched_evaluators,
            unmatched_evaluators,
            target_table_idx: table_index,
            field_index_map,
            merge_type,
            distributed: false,
            change_join_order: false,
            row_id_index: column_binding.index,
            split_idx,
            can_try_update_column_only: self.can_try_update_column_only(&matched_clauses),
            table_schema,
            can_merge_into_source_build_bloom: false,
        })
    }

    async fn bind_matched_clause<'a>(
        &mut self,
        scalar_binder: &mut ScalarBinder<'a>,
        clause: &MatchedClause,
        columns: &mut HashSet<IndexType>,
        schema: TableSchemaRef,
        update_or_insert_columns_star: Option<HashMap<FieldIndex, ScalarExpr>>,
        update_row_version: Option<(FieldIndex, ScalarExpr)>,
        target_name: &str,
    ) -> Result<MatchedEvaluator> {
        let condition = if let Some(expr) = &clause.selection {
            let (scalar_expr, _) = scalar_binder.bind(expr).await?;
            for idx in scalar_expr.used_columns() {
                columns.insert(idx);
            }

            if !self.check_allowed_scalar_expr(&scalar_expr)? {
                return Err(ErrorCode::SemanticError(
                    "matched clause's condition can't contain subquery|window|aggregate|udf functions"
                        .to_string(),
                )
                .set_span(scalar_expr.span()));
            }
            Some(scalar_expr)
        } else {
            None
        };

        let mut update = if let MatchOperation::Update {
            update_list,
            is_star,
        } = &clause.operation
        {
            if *is_star {
                update_or_insert_columns_star
            } else {
                let mut update_columns = HashMap::with_capacity(update_list.len());
                for update_expr in update_list {
                    let (scalar_expr, _) = scalar_binder.bind(&update_expr.expr).await?;
                    if !self.check_allowed_scalar_expr(&scalar_expr)? {
                        return Err(ErrorCode::SemanticError(
                            "update clause's can't contain subquery|window|aggregate|udf functions"
                                .to_string(),
                        )
                        .set_span(scalar_expr.span()));
                    }
                    let col_name =
                        normalize_identifier(&update_expr.name, &self.name_resolution_ctx).name;
                    if let Some(tbl_identify) = &update_expr.table {
                        let update_table_name =
                            normalize_identifier(tbl_identify, &self.name_resolution_ctx).name;
                        if update_table_name != target_name {
                            return Err(ErrorCode::BadArguments(format!(
                                "Update Identify's `{}` should be `{}`",
                                update_table_name, target_name
                            )));
                        }
                    }

                    let index = schema.index_of(&col_name)?;

                    if update_columns.contains_key(&index) {
                        return Err(ErrorCode::BadArguments(format!(
                            "Multiple assignments in the single statement to column `{}`",
                            col_name
                        )));
                    }

                    let field = schema.field(index);
                    if field.computed_expr().is_some() {
                        return Err(ErrorCode::BadArguments(format!(
                            "The value specified for computed column '{}' is not allowed",
                            field.name()
                        )));
                    }

                    update_columns.insert(index, scalar_expr.clone());
                }

                Some(update_columns)
            }
        } else {
            // delete
            None
        };

        // update row_version
        if let Some((index, row_version)) = update_row_version {
            update.as_mut().map(|v| v.insert(index, row_version));
        }
        Ok(MatchedEvaluator { condition, update })
    }

    async fn bind_unmatched_clause<'a>(
        &mut self,
        scalar_binder: &mut ScalarBinder<'a>,
        clause: &UnmatchedClause,
        columns: &mut HashSet<IndexType>,
        table_schema: TableSchemaRef,
        update_or_insert_columns_star: Option<HashMap<FieldIndex, ScalarExpr>>,
    ) -> Result<UnmatchedEvaluator> {
        let condition = if let Some(expr) = &clause.selection {
            let (scalar_expr, _) = scalar_binder.bind(expr).await?;
            if !self.check_allowed_scalar_expr(&scalar_expr)? {
                return Err(ErrorCode::SemanticError(
                    "unmatched clause's condition can't contain subquery|window|aggregate|udf functions"
                        .to_string(),
                )
                .set_span(scalar_expr.span()));
            }

            for idx in scalar_expr.used_columns() {
                columns.insert(idx);
            }
            Some(scalar_expr)
        } else {
            None
        };
        if clause.insert_operation.is_star {
            let default_schema = table_schema.remove_computed_fields();
            let mut values = Vec::with_capacity(default_schema.num_fields());
            let update_or_insert_columns_star = update_or_insert_columns_star.unwrap();
            for idx in 0..default_schema.num_fields() {
                let scalar = update_or_insert_columns_star.get(&idx).unwrap().clone();
                // cast expr
                values.push(wrap_cast(
                    &scalar,
                    &DataType::from(default_schema.field(idx).data_type()),
                ));
            }

            Ok(UnmatchedEvaluator {
                source_schema: Arc::new(Arc::new(default_schema).into()),
                condition,
                values,
            })
        } else {
            if clause.insert_operation.values.is_empty() {
                return Err(ErrorCode::SemanticError(
                    "Values lists must have at least one row".to_string(),
                ));
            }

            let mut values = Vec::with_capacity(clause.insert_operation.values.len());
            // we need to get source schema, and use it for filling columns.
            let source_schema = if let Some(fields) = clause.insert_operation.columns.clone() {
                self.schema_project(&table_schema, &fields)?
            } else {
                table_schema.clone()
            };
            if clause.insert_operation.values.len() != source_schema.num_fields() {
                return Err(ErrorCode::SemanticError(
                    "insert columns and values are not matched".to_string(),
                ));
            }
            for (idx, expr) in clause.insert_operation.values.iter().enumerate() {
                let (mut scalar_expr, _) = scalar_binder.bind(expr).await?;
                if !self.check_allowed_scalar_expr(&scalar_expr)? {
                    return Err(ErrorCode::SemanticError(
                        "insert clause's can't contain subquery|window|aggregate|udf functions"
                            .to_string(),
                    )
                    .set_span(scalar_expr.span()));
                }
                // type cast
                scalar_expr = wrap_cast(
                    &scalar_expr,
                    &DataType::from(source_schema.field(idx).data_type()),
                );

                values.push(scalar_expr.clone());
                for idx in scalar_expr.used_columns() {
                    columns.insert(idx);
                }
            }

            Ok(UnmatchedEvaluator {
                source_schema: Arc::new(source_schema.into()),
                condition,
                values,
            })
        }
    }

    fn find_column_index(
        &self,
        column_entries: &Vec<ColumnEntry>,
        col_name: &str,
    ) -> Result<usize> {
        for column_entry in column_entries {
            if col_name == column_entry.name() {
                return Ok(column_entry.index());
            }
        }
        Err(ErrorCode::BadArguments(format!(
            "not found col name: {}",
            col_name
        )))
    }

    fn has_update(&self, matched_clauses: &Vec<MatchedClause>) -> bool {
        for clause in matched_clauses {
            if let MatchOperation::Update {
                update_list: _,
                is_star: _,
            } = clause.operation
            {
                return true;
            }
        }
        false
    }

    fn has_star_clause(
        &self,
        matched_clauses: &Vec<MatchedClause>,
        unmatched_clauses: &Vec<UnmatchedClause>,
    ) -> bool {
        for item in matched_clauses {
            if let MatchOperation::Update {
                update_list: _,
                is_star,
            } = item.operation
            {
                if is_star {
                    return true;
                }
            }
        }

        for item in unmatched_clauses {
            if item.insert_operation.is_star {
                return true;
            }
        }
        false
    }
}

fn get_merge_type(matched_len: usize, unmatched_len: usize) -> Result<MergeIntoType> {
    if matched_len == 0 && unmatched_len > 0 {
        Ok(MergeIntoType::InsertOnly)
    } else if unmatched_len == 0 && matched_len > 0 {
        Ok(MergeIntoType::MatechedOnly)
    } else if unmatched_len > 0 && matched_len > 0 {
        Ok(MergeIntoType::FullOperation)
    } else {
        Err(ErrorCode::SemanticError(
            "we must have macthed or unmatched clause at least one",
        ))
    }
}

fn insert_only(merge_plan: &MergeInto) -> bool {
    let meta_data = merge_plan.meta_data.read();
    let target_table_columns: HashSet<usize> = meta_data
        .columns_by_table_index(merge_plan.target_table_idx)
        .iter()
        .map(|column| column.index())
        .collect();

    for evaluator in &merge_plan.unmatched_evaluators {
        if evaluator.condition.is_some() {
            let condition = evaluator.condition.as_ref().unwrap();
            for column in condition.used_columns() {
                if target_table_columns.contains(&column) {
                    return false;
                }
            }
        }

        for value in &evaluator.values {
            for column in value.used_columns() {
                if target_table_columns.contains(&column) {
                    return false;
                }
            }
        }
    }
    true
}
