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

use databend_common_catalog::plan::NUM_ROW_ID_PREFIX_BITS;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check::check_cast;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::ComputedExpr;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Expr;
use databend_common_expression::FieldIndex;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchema;
use databend_common_expression::PREDICATE_COLUMN_NAME;
use databend_common_expression::ROW_ID_COL_NAME;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::schema::TableInfo;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::NUM_BLOCK_ID_BITS;
use databend_storages_common_table_meta::readers::snapshot_reader::TableSnapshotAccessor;
use itertools::Itertools;

use super::ColumnMutation;
use crate::binder::wrap_cast;
use crate::binder::MutationStrategy;
use crate::binder::MutationType;
use crate::executor::physical_plan::PhysicalPlan;
use crate::executor::physical_plans::CommitSink;
use crate::executor::physical_plans::Exchange;
use crate::executor::physical_plans::FragmentKind;
use crate::executor::physical_plans::MutationKind;
use crate::executor::physical_plans::MutationManipulate;
use crate::executor::physical_plans::MutationOrganize;
use crate::executor::physical_plans::MutationSplit;
use crate::executor::physical_plans::RowFetch;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::ColumnSet;
use crate::optimizer::SExpr;
use crate::parse_computed_expr;
use crate::plans::BoundColumnRef;
use crate::plans::ConstantExpr;
use crate::plans::FunctionCall;
use crate::BindContext;
use crate::ColumnBindingBuilder;
use crate::ColumnEntry;
use crate::IndexType;
use crate::MetadataRef;
use crate::ScalarExpr;
use crate::TypeCheck;
use crate::Visibility;
use crate::DUMMY_COLUMN_INDEX;

// The predicate_column_index should not be conflict with update expr's column_binding's index.
pub const PREDICATE_COLUMN_INDEX: IndexType = u64::MAX as usize;
pub type MatchExpr = Vec<(Option<RemoteExpr>, Option<Vec<(FieldIndex, RemoteExpr)>>)>;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Mutation {
    pub plan_id: u32,
    pub input: Box<PhysicalPlan>,
    pub table_info: TableInfo,
    // (DataSchemaRef, Option<RemoteExpr>, Vec<RemoteExpr>,Vec<usize>) => (source_schema, condition, value_exprs)
    pub unmatched: Vec<(DataSchemaRef, Option<RemoteExpr>, Vec<RemoteExpr>)>,
    pub segments: Vec<(usize, Location)>,
    pub strategy: MutationStrategy,
    pub target_table_index: usize,
    pub need_match: bool,
    pub distributed: bool,
    pub target_build_optimization: bool,
    pub table_meta_timestamps: TableMetaTimestamps,
}

impl PhysicalPlanBuilder {
    pub async fn build_mutation(
        &mut self,
        s_expr: &SExpr,
        mutation: &crate::plans::Mutation,
        required: ColumnSet,
    ) -> Result<PhysicalPlan> {
        let crate::plans::Mutation {
            bind_context,
            metadata,
            catalog_name,
            database_name,
            table_name,
            table_name_alias,
            matched_evaluators,
            unmatched_evaluators,
            mutation_type,
            target_table_index,
            field_index_map,
            strategy,
            distributed,
            predicate_column_index,
            row_id_index,
            row_id_shuffle,
            can_try_update_column_only,
            ..
        } = mutation;

        let mut plan = self.build(s_expr.child(0)?, required).await?;
        let mutation_input_schema = plan.output_schema()?;

        let table = self
            .ctx
            .get_table(catalog_name, database_name, table_name)
            .await?;
        let table_info = table.get_table_info();
        let table_name = table_name.clone();
        let mutation_build_info = self.mutation_build_info.clone().unwrap();

        if *strategy == MutationStrategy::Direct {
            // MutationStrategy::Direct: If the mutation filter is a simple expression,
            // we use MutationSource to execute the mutation directly.
            let mut field_id_to_schema_index = HashMap::new();
            let (mutation_expr, computed_expr, mutation_kind) =
                if let Some(update_list) = &matched_evaluators[0].update {
                    let (database, table_name) = match table_name_alias {
                        None => (Some(database_name.as_str()), table_name.clone()),
                        Some(table_name_alias) => (None, table_name_alias.to_lowercase()),
                    };
                    let mutation_expr = mutation_update_expr(
                        self.ctx.clone(),
                        bind_context,
                        update_list,
                        table.schema_with_stream().into(),
                        mutation_input_schema.clone(),
                        *predicate_column_index,
                        database,
                        &table_name,
                    )?;

                    build_field_id_to_schema_index(
                        database,
                        &table_name,
                        bind_context,
                        table.schema_with_stream(),
                        mutation_input_schema.clone(),
                        &mut field_id_to_schema_index,
                    );

                    let computed_expr = generate_stored_computed_list(
                        self.ctx.clone(),
                        bind_context,
                        Arc::new(table.schema().into()),
                        mutation_input_schema.clone(),
                        database,
                        &table_name,
                        update_list,
                    )?;

                    (
                        Some(mutation_expr),
                        Some(computed_expr),
                        MutationKind::Update,
                    )
                } else {
                    (None, None, MutationKind::Delete)
                };

            plan = PhysicalPlan::ColumnMutation(ColumnMutation {
                plan_id: 0,
                input: Box::new(plan),
                table_info: mutation_build_info.table_info.clone(),
                mutation_expr,
                computed_expr,
                mutation_type: mutation_type.clone(),
                field_id_to_schema_index,
                input_num_columns: mutation_input_schema.fields().len(),
                has_filter_column: predicate_column_index.is_some(),
                table_meta_timestamps: mutation_build_info.table_meta_timestamps,
            });

            if *distributed {
                plan = PhysicalPlan::Exchange(Exchange {
                    plan_id: 0,
                    input: Box::new(plan),
                    kind: FragmentKind::Merge,
                    keys: vec![],
                    allow_adjust_parallelism: true,
                    ignore_exchange: false,
                });
            }

            plan = PhysicalPlan::CommitSink(Box::new(CommitSink {
                input: Box::new(plan),
                snapshot: mutation_build_info.table_snapshot,
                table_info: table_info.clone(),
                // let's use update first, we will do some optimizations and select exact strategy
                mutation_kind,
                update_stream_meta: vec![],
                merge_meta: false,
                deduplicated_label: unsafe { self.ctx.get_settings().get_deduplicate_label()? },
                plan_id: u32::MAX,
                recluster_info: None,
                table_meta_timestamps: mutation_build_info.table_meta_timestamps,
            }));

            plan.adjust_plan_id(&mut 0);
            return Ok(plan);
        }

        let is_not_matched_only = matches!(strategy, MutationStrategy::NotMatchedOnly);
        let row_id_offset = if !is_not_matched_only {
            mutation_input_schema.index_of(&row_id_index.to_string())?
        } else {
            DUMMY_COLUMN_INDEX
        };

        // For distributed merge, we shuffle data blocks by block_id (drived from row_id) to avoid
        // different nodes update the same physical block simultaneously, data blocks that are needed
        // to insert just keep in local node.
        if *distributed && *row_id_shuffle && !is_not_matched_only {
            plan = PhysicalPlan::Exchange(build_block_id_shuffle_exchange(
                plan,
                bind_context,
                mutation_input_schema.clone(),
                database_name,
                &table_name,
            )?);
        }

        // If the mutation type is FullOperation, we use row_id column to split a block
        // into matched and not matched parts.
        if matches!(strategy, MutationStrategy::MixedMatched) {
            plan = PhysicalPlan::MutationSplit(Box::new(MutationSplit {
                plan_id: 0,
                input: Box::new(plan),
                split_index: row_id_offset,
            }));
        }

        // Construct row fetch plan for lazy columns.
        if let Some(lazy_columns) = self
            .metadata
            .read()
            .get_table_lazy_columns(target_table_index)
            && !lazy_columns.is_empty()
        {
            plan = PhysicalPlan::RowFetch(build_mutation_row_fetch(
                plan,
                metadata.clone(),
                mutation_input_schema.clone(),
                strategy.clone(),
                lazy_columns.clone(),
                *target_table_index,
                row_id_offset,
            ));
        }

        let output_schema = plan.output_schema()?;

        // transform unmatched for insert
        // reference to func `build_eval_scalar`
        // (DataSchemaRef, Option<RemoteExpr>, Vec<RemoteExpr>,Vec<usize>) => (source_schema, condition, value_exprs)
        let mut unmatched =
            Vec::<(DataSchemaRef, Option<RemoteExpr>, Vec<RemoteExpr>)>::with_capacity(
                unmatched_evaluators.len(),
            );

        for item in unmatched_evaluators {
            let filter = if let Some(filter_expr) = &item.condition {
                Some(self.scalar_expr_to_remote_expr(filter_expr, output_schema.clone())?)
            } else {
                None
            };

            let mut values_exprs = Vec::<RemoteExpr>::with_capacity(item.values.len());

            for scalar_expr in &item.values {
                values_exprs
                    .push(self.scalar_expr_to_remote_expr(scalar_expr, output_schema.clone())?)
            }

            unmatched.push((item.source_schema.clone(), filter, values_exprs))
        }

        // the first option is used for condition
        // the second option is used to distinct update and delete
        let mut matched = Vec::with_capacity(matched_evaluators.len());

        // transform matched for delete/update
        for item in matched_evaluators {
            let condition = if let Some(condition) = &item.condition {
                let expr = self
                    .scalar_expr_to_remote_expr(condition, output_schema.clone())?
                    .as_expr(&BUILTIN_FUNCTIONS);
                let (expr, _) = ConstantFolder::fold(
                    &expr,
                    &self.ctx.get_function_context()?,
                    &BUILTIN_FUNCTIONS,
                );
                Some(expr.as_remote_expr())
            } else {
                None
            };

            if *can_try_update_column_only {
                assert!(condition.is_none());
            }

            // update
            let update_list = if let Some(update_list) = &item.update {
                // we don't need real col_indices here, just give a
                // dummy index, that's ok.
                let col_indices = vec![DUMMY_COLUMN_INDEX];
                let (database, table_name) = match table_name_alias {
                    None => (Some(database_name.as_str()), table_name.clone()),
                    Some(table_name_alias) => (None, table_name_alias.to_lowercase()),
                };
                let update_list = generate_update_list(
                    self.ctx.clone(),
                    bind_context,
                    update_list,
                    table.schema_with_stream().into(),
                    col_indices,
                    Some(PREDICATE_COLUMN_INDEX),
                    database,
                    &table_name,
                )?;
                let update_list = update_list
                    .iter()
                    .map(|(idx, remote_expr)| {
                        (
                            *idx,
                            remote_expr
                                .as_expr(&BUILTIN_FUNCTIONS)
                                .project_column_ref(|name| {
                                    // there will add a predicate col when we process matched clauses.
                                    // so it's not in mutation_input_schema for now. But it's must be added
                                    // to the tail, so let do it like below.
                                    if *name == PREDICATE_COLUMN_INDEX.to_string() {
                                        output_schema.num_fields()
                                    } else {
                                        output_schema.index_of(name).unwrap()
                                    }
                                })
                                .as_remote_expr(),
                        )
                    })
                    .collect_vec();
                // update
                Some(update_list)
            } else {
                // delete
                None
            };
            matched.push((condition, update_list))
        }

        let mut field_index_of_input_schema = HashMap::<FieldIndex, usize>::new();
        for (field_index, value) in field_index_map {
            field_index_of_input_schema
                .insert(*field_index, output_schema.index_of(value).unwrap());
        }

        plan = PhysicalPlan::MutationManipulate(Box::new(MutationManipulate {
            plan_id: 0,
            input: Box::new(plan.clone()),
            table_info: table_info.clone(),
            unmatched: unmatched.clone(),
            matched: matched.clone(),
            field_index_of_input_schema: field_index_of_input_schema.clone(),
            strategy: strategy.clone(),
            row_id_idx: row_id_offset,
            can_try_update_column_only: *can_try_update_column_only,
            unmatched_schema: mutation_input_schema.clone(),
        }));

        plan = PhysicalPlan::MutationOrganize(Box::new(MutationOrganize {
            plan_id: 0,
            input: Box::new(plan.clone()),
            strategy: strategy.clone(),
        }));

        let segments: Vec<_> = mutation_build_info
            .table_snapshot
            .segments()
            .iter()
            .cloned()
            .enumerate()
            .collect();

        let mutation = PhysicalPlan::Mutation(Box::new(Mutation {
            input: Box::new(plan.clone()),
            table_info: table_info.clone(),
            unmatched,
            segments: segments.clone(),
            distributed: *distributed,
            strategy: strategy.clone(),
            target_table_index: *target_table_index,
            need_match: !is_not_matched_only,
            target_build_optimization: false,
            plan_id: u32::MAX,
            table_meta_timestamps: mutation_build_info.table_meta_timestamps,
        }));

        let commit_input = if !distributed {
            mutation
        } else {
            PhysicalPlan::Exchange(Exchange {
                plan_id: 0,
                input: Box::new(mutation),
                kind: FragmentKind::Merge,
                keys: vec![],
                allow_adjust_parallelism: true,
                ignore_exchange: false,
            })
        };

        let mutation_kind = match mutation_type {
            MutationType::Update | MutationType::Merge => MutationKind::Update,
            MutationType::Delete => MutationKind::Delete,
        };

        let update_stream_meta = match mutation_type {
            MutationType::Merge => mutation_build_info.update_stream_meta,
            MutationType::Update | MutationType::Delete => vec![],
        };

        // build mutation_aggregate
        let mut physical_plan = PhysicalPlan::CommitSink(Box::new(CommitSink {
            input: Box::new(commit_input),
            snapshot: mutation_build_info.table_snapshot,
            table_info: table_info.clone(),
            // let's use update first, we will do some optimizations and select exact strategy
            mutation_kind,
            update_stream_meta,
            merge_meta: false,
            deduplicated_label: unsafe { self.ctx.get_settings().get_deduplicate_label()? },
            plan_id: u32::MAX,
            recluster_info: None,
            table_meta_timestamps: mutation_build_info.table_meta_timestamps,
        }));
        physical_plan.adjust_plan_id(&mut 0);
        Ok(physical_plan)
    }

    fn scalar_expr_to_remote_expr(
        &self,
        scalar_expr: &ScalarExpr,
        schema: DataSchemaRef,
    ) -> Result<RemoteExpr> {
        let scalar_expr = scalar_expr
            .type_check(schema.as_ref())?
            .project_column_ref(|index| schema.index_of(&index.to_string()).unwrap());
        let (filer, _) = ConstantFolder::fold(
            &scalar_expr,
            &self.ctx.get_function_context().unwrap(),
            &BUILTIN_FUNCTIONS,
        );
        Ok(filer.as_remote_expr())
    }
}

pub fn build_block_id_shuffle_exchange(
    plan: PhysicalPlan,
    bind_context: &BindContext,
    mutation_input_schema: Arc<DataSchema>,
    database_name: &str,
    table_name: &str,
) -> Result<Exchange> {
    let mut row_id_column = None;
    for column_binding in bind_context.columns.iter() {
        if BindContext::match_column_binding(
            Some(database_name),
            Some(table_name),
            ROW_ID_COL_NAME,
            column_binding,
        ) {
            row_id_column = Some(ScalarExpr::BoundColumnRef(BoundColumnRef {
                span: None,
                column: column_binding.clone(),
            }));
            break;
        }
    }
    let row_id_column = row_id_column.ok_or_else(|| ErrorCode::Internal("It's a bug"))?;

    let row_id_expr = row_id_column
        .type_check(mutation_input_schema.as_ref())?
        .project_column_ref(|index| mutation_input_schema.index_of(&index.to_string()).unwrap());

    let block_id_shuffle_key = check_function(
        None,
        "bit_and",
        &[],
        &[
            check_function(
                None,
                "bit_shift_right",
                &[],
                &[row_id_expr, Expr::Constant {
                    span: None,
                    scalar: Scalar::Number(((64 - NUM_ROW_ID_PREFIX_BITS) as u64).into()),
                    data_type: DataType::Number(NumberDataType::UInt64),
                }],
                &BUILTIN_FUNCTIONS,
            )?,
            Expr::Constant {
                span: None,
                scalar: Scalar::Number((((1 << NUM_BLOCK_ID_BITS) - 1) as u64).into()),
                data_type: DataType::Number(NumberDataType::UInt64),
            },
        ],
        &BUILTIN_FUNCTIONS,
    )?;

    Ok(Exchange {
        plan_id: 0,
        input: Box::new(plan),
        kind: FragmentKind::Normal,
        keys: vec![block_id_shuffle_key.as_remote_expr()],
        allow_adjust_parallelism: true,
        ignore_exchange: false,
    })
}

fn build_mutation_row_fetch(
    plan: PhysicalPlan,
    metadata: MetadataRef,
    mutation_input_schema: Arc<DataSchema>,
    strategy: MutationStrategy,
    lazy_columns: HashSet<usize>,
    target_table_index: usize,
    row_id_offset: usize,
) -> RowFetch {
    let metadata = metadata.read();

    let lazy_columns = lazy_columns
        .iter()
        .sorted() // Needs sort because we need to make the order deterministic.
        .filter(|index| !mutation_input_schema.has_field(&index.to_string())) // If the column is already in the input schema, we don't need to fetch it.
        .cloned()
        .collect::<Vec<_>>();
    let mut has_inner_column = false;
    let need_wrap_nullable = matches!(strategy, MutationStrategy::MixedMatched);
    let fetched_fields: Vec<DataField> = lazy_columns
        .iter()
        .map(|index| {
            let col = metadata.column(*index);
            if let ColumnEntry::BaseTableColumn(c) = col {
                if c.path_indices.is_some() {
                    has_inner_column = true;
                }
            }
            let mut data_type = col.data_type();
            if need_wrap_nullable {
                data_type = data_type.wrap_nullable();
            }
            DataField::new(&index.to_string(), data_type)
        })
        .collect();

    let source = metadata
        .get_table_source(&target_table_index)
        .unwrap()
        .clone();
    let table_schema = source.source_info.schema();
    let cols_to_fetch = PhysicalPlanBuilder::build_projection(
        &metadata,
        &table_schema,
        lazy_columns.iter(),
        has_inner_column,
        true,
        true,
        false,
    );

    RowFetch {
        plan_id: 0,
        input: Box::new(plan),
        source: Box::new(source),
        row_id_col_offset: row_id_offset,
        cols_to_fetch,
        fetched_fields,
        need_wrap_nullable,
        stat_info: None,
    }
}

#[allow(clippy::too_many_arguments)]
pub fn generate_update_list(
    ctx: Arc<dyn TableContext>,
    bind_context: &BindContext,
    update_list: &HashMap<FieldIndex, ScalarExpr>,
    schema: DataSchema,
    col_indices: Vec<usize>,
    use_column_name_index: Option<usize>,
    database: Option<&str>,
    table: &str,
) -> Result<Vec<(FieldIndex, RemoteExpr<String>)>> {
    let column = ColumnBindingBuilder::new(
        PREDICATE_COLUMN_NAME.to_string(),
        use_column_name_index.unwrap_or_else(|| schema.num_fields()),
        Box::new(DataType::Boolean),
        Visibility::Visible,
    )
    .build();
    let predicate = ScalarExpr::BoundColumnRef(BoundColumnRef { span: None, column });

    update_list.iter().try_fold(
        Vec::with_capacity(update_list.len()),
        |mut acc, (index, scalar)| {
            let field = schema.field(*index);
            let data_type = scalar.data_type()?;
            let target_type = field.data_type();
            let left = if data_type != *target_type {
                wrap_cast(scalar, target_type)
            } else {
                scalar.clone()
            };

            let scalar = if col_indices.is_empty() {
                // The condition is always true.
                // Replace column to the result of the following expression:
                // CAST(expression, type)
                left
            } else {
                // Replace column to the result of the following expression:
                // if(condition, CAST(expression, type), column)
                let mut right = None;
                for column_binding in bind_context.columns.iter() {
                    if BindContext::match_column_binding(
                        database,
                        Some(table),
                        field.name(),
                        column_binding,
                    ) {
                        right = Some(ScalarExpr::BoundColumnRef(BoundColumnRef {
                            span: None,
                            column: column_binding.clone(),
                        }));
                        break;
                    }
                }

                let right = right.ok_or_else(|| ErrorCode::Internal("It's a bug"))?;

                // corner case: for merge into, if target_table's fields are not null, when after bind_join, it will
                // change into nullable, so we need to cast this. but we will do cast after all matched clauses,please
                // see `cast_data_type_for_merge()`.

                ScalarExpr::FunctionCall(FunctionCall {
                    span: None,
                    func_name: "if".to_string(),
                    params: vec![],
                    arguments: vec![predicate.clone(), left, right],
                })
            };
            let expr = scalar.as_expr()?.project_column_ref(|col| {
                if use_column_name_index.is_none() {
                    col.column_name.clone()
                } else {
                    col.index.to_string()
                }
            });
            let (expr, _) =
                ConstantFolder::fold(&expr, &ctx.get_function_context()?, &BUILTIN_FUNCTIONS);
            acc.push((*index, expr.as_remote_expr()));
            Ok::<_, ErrorCode>(acc)
        },
    )
}

#[allow(clippy::too_many_arguments)]
pub fn mutation_update_expr(
    ctx: Arc<dyn TableContext>,
    bind_context: &BindContext,
    update_list: &HashMap<FieldIndex, ScalarExpr>,
    schema: DataSchema,
    input_schema: Arc<DataSchema>,
    predicate_column_index: Option<usize>,
    database: Option<&str>,
    table: &str,
) -> Result<Vec<(FieldIndex, RemoteExpr)>> {
    let predicate = if let Some(predicate_column_index) = predicate_column_index {
        let column = ColumnBindingBuilder::new(
            PREDICATE_COLUMN_NAME.to_string(),
            predicate_column_index,
            Box::new(DataType::Boolean),
            Visibility::Visible,
        )
        .build();
        ScalarExpr::BoundColumnRef(BoundColumnRef { span: None, column })
    } else {
        ScalarExpr::ConstantExpr(ConstantExpr {
            span: None,
            value: Scalar::Boolean(true),
        })
    };

    update_list.iter().try_fold(
        Vec::with_capacity(update_list.len()),
        |mut acc, (index, scalar)| {
            let field = schema.field(*index);
            let data_type = scalar.data_type()?;
            let target_type = field.data_type();
            let left = if data_type != *target_type {
                wrap_cast(scalar, target_type)
            } else {
                scalar.clone()
            };

            // Replace column to the result of the following expression:
            // if(condition, CAST(expression, type), column)
            let mut right = None;
            for column_binding in bind_context.columns.iter() {
                if BindContext::match_column_binding(
                    database,
                    Some(table),
                    field.name(),
                    column_binding,
                ) {
                    right = Some(ScalarExpr::BoundColumnRef(BoundColumnRef {
                        span: None,
                        column: column_binding.clone(),
                    }));
                    break;
                }
            }
            let right = right.ok_or_else(|| {
                ErrorCode::Internal(
                    format!("Can not find column {} in table {}", field.name(), table).to_string(),
                )
            })?;

            let scalar = ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "if".to_string(),
                params: vec![],
                arguments: vec![predicate.clone(), left, right],
            });
            let expr = scalar
                .type_check(input_schema.as_ref())?
                .project_column_ref(|index| input_schema.index_of(&index.to_string()).unwrap());
            let (expr, _) =
                ConstantFolder::fold(&expr, &ctx.get_function_context()?, &BUILTIN_FUNCTIONS);
            acc.push((*index, expr.as_remote_expr()));
            Ok::<_, ErrorCode>(acc)
        },
    )
}

pub fn generate_stored_computed_list(
    ctx: Arc<dyn TableContext>,
    bind_context: &BindContext,
    schema: DataSchemaRef,
    input_schema: Arc<DataSchema>,
    database: Option<&str>,
    table: &str,
    update_list: &HashMap<FieldIndex, ScalarExpr>,
) -> Result<Vec<(FieldIndex, RemoteExpr)>> {
    let mut remote_exprs = Vec::new();
    for (i, f) in schema.fields().iter().enumerate() {
        if let Some(ComputedExpr::Stored(stored_expr)) = f.computed_expr() {
            let expr = parse_computed_expr(ctx.clone(), schema.clone(), stored_expr)?;
            let expr = check_cast(None, false, expr, f.data_type(), &BUILTIN_FUNCTIONS)?;

            // If related column has updated, the stored computed column need to regenerate.
            let mut need_update = false;
            let field_indices = expr.column_refs();
            for (field_index, _) in field_indices.iter() {
                if update_list.contains_key(field_index) {
                    need_update = true;
                    break;
                }
            }
            if need_update {
                let expr = expr.project_column_ref(|id| {
                    let mut column_index: Option<usize> = None;
                    for column_binding in bind_context.columns.iter() {
                        if BindContext::match_column_binding(
                            database,
                            Some(table),
                            schema.field(*id).name(),
                            column_binding,
                        ) {
                            column_index = Some(column_binding.index);
                            break;
                        }
                    }
                    input_schema
                        .index_of(&column_index.unwrap().to_string())
                        .unwrap()
                });
                let (expr, _) =
                    ConstantFolder::fold(&expr, &ctx.get_function_context()?, &BUILTIN_FUNCTIONS);
                remote_exprs.push((i, expr.as_remote_expr()));
            }
        }
    }
    Ok(remote_exprs)
}

fn build_field_id_to_schema_index(
    database: Option<&str>,
    table_name: &str,
    bind_context: &BindContext,
    table_schema_with_stream: Arc<TableSchema>,
    mutation_input_schema: Arc<DataSchema>,
    field_id_to_schema_index: &mut HashMap<usize, usize>,
) {
    for (field_id, field) in table_schema_with_stream.fields().iter().enumerate() {
        if matches!(field.computed_expr(), Some(ComputedExpr::Virtual(_))) {
            continue;
        }
        for column_binding in bind_context.columns.iter() {
            if BindContext::match_column_binding(
                database,
                Some(table_name),
                field.name(),
                column_binding,
            ) {
                let column_index = column_binding.index;
                let schema_index = mutation_input_schema
                    .index_of(&column_index.to_string())
                    .unwrap();
                field_id_to_schema_index.insert(field_id, schema_index);
                break;
            }
        }
    }
}
