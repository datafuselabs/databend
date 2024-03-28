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

use databend_common_ast::ast::TableAlias;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableSchema;
use databend_common_meta_types::MetaId;

use crate::binder::MergeIntoType;
use crate::optimizer::SExpr;
use crate::BindContext;
use crate::IndexType;
use crate::MetadataRef;
use crate::ScalarExpr;

#[derive(Clone, Debug)]
pub struct UnmatchedEvaluator {
    pub source_schema: DataSchemaRef,
    pub condition: Option<ScalarExpr>,
    pub values: Vec<ScalarExpr>,
}

#[derive(Clone, Debug)]
pub struct MatchedEvaluator {
    pub condition: Option<ScalarExpr>,
    // table_schema.idx -> update_expression
    // Some => update
    // None => delete
    pub update: Option<HashMap<FieldIndex, ScalarExpr>>,
}

#[derive(Clone)]
pub struct MergeInto {
    pub catalog: String,
    pub database: String,
    pub table: String,
    pub target_alias: Option<TableAlias>,
    pub table_id: MetaId,
    pub input: Box<SExpr>,
    pub bind_context: Box<BindContext>,
    pub columns_set: Box<HashSet<IndexType>>,
    pub meta_data: MetadataRef,
    pub matched_evaluators: Vec<MatchedEvaluator>,
    pub unmatched_evaluators: Vec<UnmatchedEvaluator>,
    pub target_table_idx: usize,
    pub field_index_map: HashMap<FieldIndex, String>,
    pub merge_type: MergeIntoType,
    pub distributed: bool,
    pub change_join_order: bool,
    // when we use target table as build side or insert only, we will remove rowid columns.
    pub row_id_index: IndexType,
    pub split_idx: IndexType,
    // an optimization:
    // if it's full_operation/mactehd only and we have only one update without condition here, we shouldn't run
    // evaluator, we can just do projection to get the right columns.But the limitation is below:
    // `update *`` or `update set t1.a = t2.a ...`, the right expr on the `=` must be only a column,
    // we don't support complex expressions.
    pub can_try_update_column_only: bool,
    pub can_merge_into_source_build_bloom: bool,
    pub table_schema: Arc<TableSchema>,
}

impl std::fmt::Debug for MergeInto {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Merge Into")
            .field("catalog", &self.catalog)
            .field("database", &self.database)
            .field("table", &self.table)
            .field("table_id", &self.table_id)
            .field("join", &self.input)
            .field("matched", &self.matched_evaluators)
            .field("unmatched", &self.unmatched_evaluators)
            .field("distributed", &self.distributed)
            .field(
                "can_try_update_column_only",
                &self.can_try_update_column_only,
            )
            .finish()
    }
}

pub const INSERT_NAME: &str = "number of rows inserted";
pub const UPDATE_NAME: &str = "number of rows updated";
pub const DELETE_NAME: &str = "number of rows deleted";

impl MergeInto {
    // the order of output should be (insert, update, delete),this is
    // consistent with snowflake.
    fn merge_into_mutations(&self) -> (bool, bool, bool) {
        let insert = matches!(self.merge_type, MergeIntoType::FullOperation)
            || matches!(self.merge_type, MergeIntoType::InsertOnly);
        let mut update = false;
        let mut delete = false;
        for evaluator in &self.matched_evaluators {
            if evaluator.update.is_none() {
                delete = true
            } else {
                update = true
            }
        }
        (insert, update, delete)
    }

    fn merge_into_table_schema(&self) -> Result<DataSchemaRef> {
        let (insert, update, delete) = self.merge_into_mutations();

        let fields = [
            (
                DataField::new(INSERT_NAME, DataType::Number(NumberDataType::Int32)),
                insert,
            ),
            (
                DataField::new(UPDATE_NAME, DataType::Number(NumberDataType::Int32)),
                update,
            ),
            (
                DataField::new(DELETE_NAME, DataType::Number(NumberDataType::Int32)),
                delete,
            ),
        ];

        // Filter and collect the fields to include in the schema.
        // Only fields with a corresponding true value in the mutation states are included.
        let schema_fields: Vec<DataField> = fields
            .iter()
            .filter_map(
                |(field, include)| {
                    if *include { Some(field.clone()) } else { None }
                },
            )
            .collect();

        // Check if any fields are included. If none, return an error. Otherwise, return the schema.
        if schema_fields.is_empty() {
            Err(ErrorCode::BadArguments(
                "at least one matched or unmatched clause for merge into",
            ))
        } else {
            Ok(DataSchemaRefExt::create(schema_fields))
        }
    }

    pub fn schema(&self) -> DataSchemaRef {
        self.merge_into_table_schema().unwrap()
    }
}
