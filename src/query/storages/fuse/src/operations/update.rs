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
use std::sync::Arc;

use databend_common_catalog::plan::Filters;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::FieldIndex;
use databend_common_expression::RemoteExpr;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::PREDICATE_COLUMN_NAME;
use databend_common_expression::ROW_ID_COL_NAME;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::Pipeline;
use databend_common_sql::evaluator::BlockOperator;
use databend_storages_common_table_meta::meta::TableSnapshot;

use crate::operations::mutation::MutationAction;
use crate::operations::mutation::MutationSource;
use crate::FuseTable;

impl FuseTable {
    #[allow(clippy::too_many_arguments)]
    #[async_backtrace::framed]
    pub async fn fast_update(
        &self,
        ctx: Arc<dyn TableContext>,
        filters: &mut Option<Filters>,
        col_indices: Vec<FieldIndex>,
        query_row_id_col: bool,
    ) -> Result<Option<Arc<TableSnapshot>>> {
        let snapshot_opt = self.read_table_snapshot().await?;

        // check if table is empty
        let snapshot = if let Some(val) = snapshot_opt {
            val
        } else {
            // no snapshot, no update
            return Ok(None);
        };

        if snapshot.summary.row_count == 0 {
            // empty snapshot, no update
            return Ok(None);
        }

        if col_indices.is_empty() && filters.is_some() && !query_row_id_col {
            let filter_expr = filters.clone().unwrap();
            if !self.try_eval_const(ctx.clone(), &self.schema(), &filter_expr.filter)? {
                // The condition is always false, do nothing.
                return Ok(None);
            }
            // The condition is always true.
            *filters = None;
        }
        Ok(Some(snapshot))
    }

    #[async_backtrace::framed]
    #[allow(clippy::too_many_arguments)]
    pub fn add_update_source(
        &self,
        ctx: Arc<dyn TableContext>,
        filter: Option<RemoteExpr<String>>,
        col_indices: Vec<FieldIndex>,
        update_list: Vec<(FieldIndex, RemoteExpr<String>)>,
        computed_list: BTreeMap<FieldIndex, RemoteExpr<String>>,
        query_row_id_col: bool,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let all_column_indices = self.all_column_indices();
        let schema = self.schema_with_stream();

        let mut offset_map = BTreeMap::new();
        let mut remain_reader = None;
        let mut pos = 0;
        let (projection, input_schema) = if col_indices.is_empty() {
            all_column_indices.iter().for_each(|&index| {
                offset_map.insert(index, pos);
                pos += 1;
            });

            (
                Projection::Columns(all_column_indices),
                Arc::new(schema.remove_virtual_computed_fields()),
            )
        } else {
            col_indices.iter().for_each(|&index| {
                offset_map.insert(index, pos);
                pos += 1;
            });

            let mut fields: Vec<TableField> = col_indices
                .iter()
                .map(|index| schema.fields()[*index].clone())
                .collect();

            fields.push(TableField::new(
                PREDICATE_COLUMN_NAME,
                TableDataType::Boolean,
            ));
            pos += 1;

            let remain_col_indices: Vec<FieldIndex> = all_column_indices
                .into_iter()
                .filter(|index| !col_indices.contains(index))
                .collect();
            if !remain_col_indices.is_empty() {
                remain_col_indices.iter().for_each(|&index| {
                    offset_map.insert(index, pos);
                    pos += 1;
                });

                let reader = self.create_block_reader(
                    ctx.clone(),
                    Projection::Columns(remain_col_indices),
                    false,
                    self.change_tracking_enabled(),
                    false,
                )?;
                fields.extend_from_slice(reader.schema().fields());
                remain_reader = Some((*reader).clone());
            }

            (
                Projection::Columns(col_indices),
                Arc::new(TableSchema::new(fields)),
            )
        };

        let mut cap = 2;
        if !computed_list.is_empty() {
            cap += 1;
        }
        let mut ops = Vec::with_capacity(cap);

        let mut exprs = Vec::with_capacity(update_list.len());
        for (id, remote_expr) in update_list.into_iter() {
            let expr = remote_expr
                .as_expr(&BUILTIN_FUNCTIONS)
                .project_column_ref(|name| input_schema.index_of(name).unwrap());
            exprs.push(expr);
            offset_map.insert(id, pos);
            pos += 1;
        }
        if !exprs.is_empty() {
            ops.push(BlockOperator::Map {
                exprs,
                projections: None,
            });
        }

        let mut computed_exprs = Vec::with_capacity(computed_list.len());
        for (id, remote_expr) in computed_list.into_iter() {
            let expr = remote_expr
                .as_expr(&BUILTIN_FUNCTIONS)
                .project_column_ref(|name| {
                    let id = schema.index_of(name).unwrap();
                    let pos = offset_map.get(&id).unwrap();
                    *pos as FieldIndex
                });
            computed_exprs.push(expr);
            offset_map.insert(id, pos);
            pos += 1;
        }
        // regenerate related stored computed columns.
        if !computed_exprs.is_empty() {
            ops.push(BlockOperator::Map {
                exprs: computed_exprs,
                projections: None,
            });
        }

        ops.push(BlockOperator::Project {
            projection: offset_map.values().cloned().collect(),
        });

        let block_reader = self.create_block_reader(
            ctx.clone(),
            projection,
            false,
            self.change_tracking_enabled(),
            false,
        )?;
        let mut schema = block_reader.schema().as_ref().clone();
        if query_row_id_col {
            schema.add_internal_field(
                ROW_ID_COL_NAME,
                TableDataType::Number(NumberDataType::UInt64),
                1,
            );
        }
        let remain_reader = Arc::new(remain_reader);
        let filter_expr = Arc::new(filter.map(|v| {
            v.as_expr(&BUILTIN_FUNCTIONS)
                .project_column_ref(|name| schema.index_of(name).unwrap())
        }));

        let _max_threads = (ctx.get_settings().get_max_threads()? as usize)
            .min(ctx.partition_num())
            .max(1);
        // Add source pipe.
        pipeline.add_transform(|input, output| {
            MutationSource::try_create(
                ctx.clone(),
                MutationAction::Update,
                input,
                output,
                filter_expr.clone(),
                block_reader.clone(),
                remain_reader.clone(),
                ops.clone(),
                self.storage_format,
                true,
            )
        })
    }
}
