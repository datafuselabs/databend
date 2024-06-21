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

use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::UInt32Type;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FromData;
use databend_common_expression::SendableDataBlockStream;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::executor::MergeIntoBuildInfo;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_sql::executor::PhysicalPlanBuilder;
use databend_common_sql::optimizer::SExpr;
use databend_common_sql::plans;
use databend_common_sql::plans::LockTableOption;
use databend_common_storages_factory::Table;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::TableContext;
use databend_storages_common_table_meta::meta::TableSnapshot;

use crate::interpreters::common::dml_build_update_stream_req;
use crate::interpreters::HookOperator;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::schedulers::build_query_pipeline_without_render_result_set;
use crate::sessions::QueryContext;
use crate::stream::DataBlockStream;

pub struct MergeIntoInterpreter {
    ctx: Arc<QueryContext>,
    s_expr: SExpr,
    schema: DataSchemaRef,
}

impl MergeIntoInterpreter {
    pub fn try_create(
        ctx: Arc<QueryContext>,
        s_expr: SExpr,
        schema: DataSchemaRef,
    ) -> Result<MergeIntoInterpreter> {
        Ok(MergeIntoInterpreter {
            ctx,
            s_expr,
            schema,
        })
    }
}

#[async_trait::async_trait]
impl Interpreter for MergeIntoInterpreter {
    fn name(&self) -> &str {
        "MergeIntoInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let merge_into: databend_common_sql::plans::MergeInto =
            self.s_expr.plan().clone().try_into()?;

        // Acquire table lock before execution.
        let lock_guard = self
            .ctx
            .clone()
            .acquire_table_lock(
                &merge_into.catalog,
                &merge_into.database,
                &merge_into.table,
                &LockTableOption::LockWithRetry,
            )
            .await?;

        // Build physical plan.
        let physical_plan = self.build_physical_plan(&merge_into).await?;

        // Build pipeline.
        let mut build_res =
            build_query_pipeline_without_render_result_set(&self.ctx, &physical_plan).await?;
        build_res.main_pipeline.add_lock_guard(lock_guard);

        // Execute hook.
        {
            let hook_operator = HookOperator::create(
                self.ctx.clone(),
                merge_into.catalog.clone(),
                merge_into.database.clone(),
                merge_into.table.clone(),
                MutationKind::MergeInto,
                LockTableOption::NoLock,
            );
            hook_operator.execute(&mut build_res.main_pipeline).await;
        }

        Ok(build_res)
    }

    fn inject_result(&self) -> Result<SendableDataBlockStream> {
        let blocks = self.get_merge_into_table_result()?;
        Ok(Box::pin(DataBlockStream::create(None, blocks)))
    }
}

impl MergeIntoInterpreter {
    pub async fn build_physical_plan(&self) -> Result<(PhysicalPlan, Option<LockGuard>)> {
        let MergePlan {
            bind_context,
            input,
            meta_data,
            columns_set,
            catalog,
            database,
            table: table_name,
            target_alias,
            matched_evaluators,
            unmatched_evaluators,
            target_table_idx,
            field_index_map,
            merge_type,
            distributed,
            change_join_order,
            split_idx,
            row_id_index,
            can_try_update_column_only,
            enable_right_broadcast,
            ..
        } = &self.plan;
        let enable_right_broadcast = *enable_right_broadcast;
        let mut columns_set = columns_set.clone();

        // Add table lock before execution.
        let lock_guard = self
            .ctx
            .get_table(&merge_into.catalog, &merge_into.database, &merge_into.table)
            .await?;

        // Check if the table supports MERGE INTO.
        table.check_mutable()?;
        let fuse_table = table.as_any().downcast_ref::<FuseTable>().ok_or_else(|| {
            ErrorCode::Unimplemented(format!(
                "table {}, engine type {}, does not support MERGE INTO",
                table.name(),
                table.get_table_info().engine(),
            ))
        })?;

        // Prepare MergeIntoBuildInfo for PhysicalPlanBuilder to build MergeInto physical plan.
        let table_info = fuse_table.get_table_info();

        // attentation!! for now we have some strategies:
        // 1. target_build_optimization, this is enabled in standalone mode and in this case we don't need rowid column anymore.
        // but we just support for `merge into xx using source on xxx when matched then update xxx when not matched then insert xxx`.
        // 2. merge into join strategies:
        // Left,Right,Inner,Left Anti, Right Anti
        // important flag:
        //      I. change join order: if true, target table as build side, if false, source as build side.
        //      II. distributed: this merge into is executed at a distributed stargety.
        // 2.1 Left: there are matched and not matched, and change join order is true.
        // 2.2 Left Anti: change join order is true, but it's insert-only.
        // 2.3 Inner: this is matched only case.
        //      2.3.1 change join order is true, target table as build side,it's matched-only.
        //      2.3.2 change join order is false, source data as build side,it's matched-only.
        // 2.4 Right: change join order is false, there are matched and not matched
        // 2.5 Right Anti: change join order is false, but it's insert-only.
        // distributed execution stargeties:
        // I. change join order is true, we use the `optimize_distributed_query`'s result.
        // II. change join order is false and match_pattern and not enable spill, we use right outer join with rownumber distributed strategies.
        // III otherwise, use `merge_into_join_sexpr` as standalone execution(so if change join order is false,but doesn't match_pattern, we don't support distributed,in fact. case I
        // can take this at most time, if that's a hash shuffle, the I can take it. We think source is always very small).

        // for `target_build_optimization` we don't need to read rowId column. for now, there are two cases we don't read rowid:
        // I. InsertOnly, the MergeIntoType is InsertOnly
        // II. target build optimization for this pr. the MergeIntoType is MergeIntoType
        let mut target_build_optimization =
            matches!(self.plan.merge_type, MergeIntoType::FullOperation)
                && !self.plan.columns_set.contains(&self.plan.row_id_index);
        if target_build_optimization {
            assert!(*change_join_order && !*distributed);
            // so if `target_build_optimization` is true, it means the optimizer enable this rule.
            // but we need to check if it's parquet format or native format. for now,we just support
            // parquet. (we will support native in the next pr).
            if fuse_table.is_native() {
                target_build_optimization = false;
                // and we need to add row_id back and forbidden target_build_optimization
                columns_set.insert(*row_id_index);
                let merge_into_join = self.ctx.get_merge_into_join();
                self.ctx.set_merge_into_join(MergeIntoJoin {
                    target_tbl_idx: DUMMY_TABLE_INDEX,
                    is_distributed: merge_into_join.is_distributed,
                    merge_into_join_type: merge_into_join.merge_into_join_type,
                });
            }
        }

        log::info!("target build optimization is {}", target_build_optimization);

        let update_stream_meta = dml_build_update_stream_req(self.ctx.clone(), meta_data).await?;

        let table_name = table_name.clone();
        let input = input.clone();

        let mut builder = PhysicalPlanBuilder::new(meta_data.clone(), self.ctx.clone(), false);
        let join_input = builder.build(&input, *columns_set.clone()).await?;

        // find row_id column index
        let join_output_schema = join_input.output_schema()?;

        let insert_only = matches!(merge_type, MergeIntoType::InsertOnly);

        let mut row_id_idx = if !insert_only && !target_build_optimization {
            match meta_data
                .read()
                .row_id_index_by_table_index(*target_table_idx)
            {
                None => {
                    return Err(ErrorCode::InvalidRowIdIndex(
                        "can't get internal row_id_idx when running merge into",
                    ));
                }
                Some(row_id_idx) => row_id_idx,
            }
        } else {
            DUMMY_COLUMN_INDEX
        };

        let mut found_row_id = false;
        let mut row_number_idx = None;
        for (idx, data_field) in join_output_schema.fields().iter().enumerate() {
            if *data_field.name() == row_id_idx.to_string() {
                row_id_idx = idx;
                found_row_id = true;
                break;
            }
        }

        // we use `merge_into_split_idx` to specify a column from target table to spilt a block
        // from join into matched part and unmatched part.
        let mut merge_into_split_idx = None;
        if matches!(merge_type, MergeIntoType::FullOperation) {
            for (idx, data_field) in join_output_schema.fields().iter().enumerate() {
                if *data_field.name() == split_idx.to_string() {
                    merge_into_split_idx = Some(idx);
                    break;
                }
            }
        }

        if enable_right_broadcast {
            row_number_idx = Some(join_output_schema.index_of(ROW_NUMBER_COL_NAME)?);
        }

        if !target_build_optimization && !insert_only && !found_row_id {
            // we can't get row_id_idx, throw an exception
            return Err(ErrorCode::InvalidRowIdIndex(
                "can't get internal row_id_idx when running merge into",
            ));
        }

        if enable_right_broadcast && row_number_idx.is_none() {
            return Err(ErrorCode::InvalidRowIdIndex(
                "can't get internal row_number_idx when running merge into",
            ));
        }

        let catalog_ = self.ctx.get_catalog(catalog).await?;

        // transform unmatched for insert
        // reference to func `build_eval_scalar`
        // (DataSchemaRef, Option<RemoteExpr>, Vec<RemoteExpr>,Vec<usize>) => (source_schema, condition, value_exprs)
        let mut unmatched =
            Vec::<(DataSchemaRef, Option<RemoteExpr>, Vec<RemoteExpr>)>::with_capacity(
                unmatched_evaluators.len(),
            );

        for item in unmatched_evaluators {
            let filter = if let Some(filter_expr) = &item.condition {
                Some(self.transform_scalar_expr2expr(filter_expr, join_output_schema.clone())?)
            } else {
                None
            };

            let mut values_exprs = Vec::<RemoteExpr>::with_capacity(item.values.len());

            for scalar_expr in &item.values {
                values_exprs
                    .push(self.transform_scalar_expr2expr(scalar_expr, join_output_schema.clone())?)
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
                    .transform_scalar_expr2expr(condition, join_output_schema.clone())?
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
                let (database, table) = match target_alias {
                    None => (Some(database.as_str()), table_name.clone()),
                    Some(alias) => (None, alias.name.to_string().to_lowercase()),
                };
                let update_list = plans::generate_update_list(
                    self.ctx.clone(),
                    bind_context,
                    update_list,
                    fuse_table.schema_with_stream().into(),
                    col_indices,
                    Some(PREDICATE_COLUMN_INDEX),
                    database,
                    &table,
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
                                    // so it's not in join_output_schema for now. But it's must be added
                                    // to the tail, so let do it like below.
                                    if *name == PREDICATE_COLUMN_INDEX.to_string() {
                                        join_output_schema.num_fields()
                                    } else {
                                        join_output_schema.index_of(name).unwrap()
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

        let base_snapshot = fuse_table.read_table_snapshot().await?.unwrap_or_else(|| {
            Arc::new(TableSnapshot::new_empty_snapshot(
                fuse_table.schema().as_ref().clone(),
                Some(table_info.ident.seq),
            ))
        });

        let mut field_index_of_input_schema = HashMap::<FieldIndex, usize>::new();
        for (field_index, value) in field_index_map {
            field_index_of_input_schema
                .insert(*field_index, join_output_schema.index_of(value).unwrap());
        }

        let segments: Vec<_> = base_snapshot
            .segments
            .clone()
            .into_iter()
            .enumerate()
            .collect();

        let commit_input = if !distributed {
            PhysicalPlan::MergeInto(Box::new(MergeInto {
                input: Box::new(join_input.clone()),
                table_info: table_info.clone(),
                catalog_info: catalog_.info(),
                unmatched,
                matched,
                field_index_of_input_schema,
                row_id_idx,
                segments,
                distributed: false,
                output_schema: DataSchemaRef::default(),
                merge_type: merge_type.clone(),
                change_join_order: *change_join_order,
                target_build_optimization,
                can_try_update_column_only: *can_try_update_column_only,
                plan_id: u32::MAX,
                merge_into_split_idx,
                enable_right_broadcast,
            }))
        } else {
            let merge_append = PhysicalPlan::MergeInto(Box::new(MergeInto {
                input: Box::new(join_input.clone()),
                table_info: table_info.clone(),
                catalog_info: catalog_.info(),
                unmatched: unmatched.clone(),
                matched,
                field_index_of_input_schema,
                row_id_idx,
                segments: segments.clone(),
                distributed: true,
                output_schema: if let Some(row_number_idx) = row_number_idx {
                    DataSchemaRef::new(DataSchema::new(vec![
                        join_output_schema.fields[row_number_idx].clone(),
                    ]))
                } else {
                    DataSchemaRef::new(DataSchema::new(vec![DataField::new(
                        ROW_ID_COL_NAME,
                        databend_common_expression::types::DataType::Number(
                            databend_common_expression::types::NumberDataType::UInt64,
                        ),
                    )]))
                },
                merge_type: merge_type.clone(),
                change_join_order: *change_join_order,
                target_build_optimization: false, // we don't support for distributed mode for now..
                can_try_update_column_only: *can_try_update_column_only,
                plan_id: u32::MAX,
                merge_into_split_idx,
                enable_right_broadcast,
            }));
            // if change_join_order = true, it means the target is build side,
            // in this way, we will do matched operation and not matched operation
            // locally in every node, and the main node just receive rowids to apply.
            let segments = if *change_join_order {
                segments.clone()
            } else {
                vec![]
            };
            PhysicalPlan::MergeIntoAppendNotMatched(Box::new(MergeIntoAppendNotMatched {
                input: Box::new(PhysicalPlan::Exchange(Exchange {
                    plan_id: 0,
                    input: Box::new(merge_append),
                    kind: FragmentKind::Merge,
                    keys: vec![],
                    allow_adjust_parallelism: true,
                    ignore_exchange: false,
                })),
                table_info: table_info.clone(),
                catalog_info: catalog_.info(),
                unmatched: unmatched.clone(),
                input_schema: join_input.output_schema()?,
                merge_type: merge_type.clone(),
                change_join_order: *change_join_order,
                segments,
                plan_id: u32::MAX,
            }))
        };

        // build mutation_aggregate
        let mut physical_plan = PhysicalPlan::CommitSink(Box::new(CommitSink {
            input: Box::new(commit_input),
            snapshot: base_snapshot,
            table_info: table_info.clone(),
            catalog_info: catalog_.info(),
            // let's use update first, we will do some optimizeations and select exact strategy
            mutation_kind: MutationKind::Update,
            update_stream_meta: update_stream_meta.clone(),
            merge_meta: false,
            deduplicated_label: unsafe { self.ctx.get_settings().get_deduplicate_label()? },
            plan_id: u32::MAX,
        }));
        physical_plan.adjust_plan_id(&mut 0);
        Ok((physical_plan, lock_guard))
    }

    fn transform_scalar_expr2expr(
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

    fn get_merge_into_table_result(&self) -> Result<Vec<DataBlock>> {
        let binding = self.ctx.get_merge_status();
        let status = binding.read();
        let mut columns = Vec::new();
        for field in self.schema.as_ref().fields() {
            match field.name().as_str() {
                plans::INSERT_NAME => {
                    columns.push(UInt32Type::from_data(vec![status.insert_rows as u32]))
                }
                plans::UPDATE_NAME => {
                    columns.push(UInt32Type::from_data(vec![status.update_rows as u32]))
                }
                plans::DELETE_NAME => {
                    columns.push(UInt32Type::from_data(vec![status.deleted_rows as u32]))
                }
                _ => unreachable!(),
            }
        }
        Ok(vec![DataBlock::new_from_columns(columns)])
    }
}
