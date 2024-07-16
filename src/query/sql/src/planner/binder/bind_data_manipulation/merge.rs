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

use databend_common_ast::ast::MatchOperation;
use databend_common_ast::ast::MatchedClause;
use databend_common_ast::ast::MergeIntoStmt;
use databend_common_ast::ast::MergeOption;
use databend_common_ast::ast::TableReference;
use databend_common_ast::ast::UnmatchedClause;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::binder::bind_data_manipulation::bind::DataManipulation;
use crate::binder::bind_data_manipulation::bind::TargetTableInfo;
use crate::binder::bind_data_manipulation::data_manipulation_input::DataManipulationInput;
use crate::binder::Binder;
use crate::binder::MergeIntoType;
use crate::plans::Plan;
use crate::BindContext;

// Merge into strategies:
// 1. Insert only: RIGHT ANTI join.
// 2. Matched and unmatched: RIGHT OUTER join.
// 3. Matched only: INNER join.
impl Binder {
    #[allow(warnings)]
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_merge_into(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &MergeIntoStmt,
    ) -> Result<Plan> {
        let (catalog_name, database_name, table_name) = self.normalize_object_identifier_triple(
            &stmt.catalog,
            &stmt.database,
            &stmt.table_ident,
        );

        let target_reference = TableReference::Table {
            span: None,
            catalog: stmt.catalog.clone(),
            database: stmt.database.clone(),
            table: stmt.table_ident.clone(),
            alias: stmt.target_alias.clone(),
            temporal: None,
            consume: false,
            pivot: None,
            unpivot: None,
        };
        let source_reference = stmt.source.transform_table_reference();

        let (matched_clauses, unmatched_clauses) =
            Self::split_merge_into_clauses(&stmt.merge_options)?;
        let manipulate_type = get_merge_type(matched_clauses.len(), unmatched_clauses.len())?;

        let data_manipulation = DataManipulation {
            target_table: TargetTableInfo {
                catalog_name,
                database_name,
                table_name,
                table_alias: stmt.target_alias.clone(),
            },
            input: DataManipulationInput::Merge {
                target: target_reference,
                source: source_reference,
                match_expr: stmt.join_expr.clone(),
                has_star_clause: self.has_star_clause(&matched_clauses, &unmatched_clauses),
                merge_type: manipulate_type.clone(),
            },
            manipulate_type: manipulate_type.clone(),
            matched_clauses,
            unmatched_clauses,
        };

        self.bind_data_manipulation(bind_context, data_manipulation)
            .await
    }

    pub fn split_merge_into_clauses(
        merge_options: &[MergeOption],
    ) -> Result<(Vec<MatchedClause>, Vec<UnmatchedClause>)> {
        if merge_options.is_empty() {
            return Err(ErrorCode::BadArguments(
                "at least one matched or unmatched clause for merge into",
            ));
        }
        let mut match_clauses = Vec::with_capacity(merge_options.len());
        let mut unmatch_clauses = Vec::with_capacity(merge_options.len());
        for merge_operation in merge_options.iter() {
            match merge_operation {
                MergeOption::Match(match_clause) => match_clauses.push(match_clause.clone()),
                MergeOption::Unmatch(unmatch_clause) => {
                    unmatch_clauses.push(unmatch_clause.clone())
                }
            }
        }
        Ok((match_clauses, unmatch_clauses))
    }

    fn has_star_clause(
        &self,
        matched_clauses: &Vec<MatchedClause>,
        unmatched_clauses: &Vec<UnmatchedClause>,
    ) -> bool {
        for item in matched_clauses {
            if let MatchOperation::Update { is_star, .. } = item.operation
                && is_star
            {
                return true;
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
        Ok(MergeIntoType::MatchedOnly)
    } else if unmatched_len > 0 && matched_len > 0 {
        Ok(MergeIntoType::FullOperation)
    } else {
        Err(ErrorCode::SemanticError(
            "we must have matched or unmatched clause at least one",
        ))
    }
}
