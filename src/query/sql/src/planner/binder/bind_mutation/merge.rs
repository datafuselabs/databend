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

use crate::binder::bind_mutation::bind::Mutation;
use crate::binder::bind_mutation::mutation_expression::MutationExpression;
use crate::binder::util::TableIdentifier;
use crate::binder::Binder;
use crate::binder::MutationStrategy;
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
        let target_table_identifier = TableIdentifier::new(
            self,
            &stmt.catalog,
            &stmt.database,
            &stmt.table_ident,
            &stmt.target_alias,
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
            sample: None,
        };
        let source_reference = stmt.source.transform_table_reference();

        let (matched_clauses, unmatched_clauses) =
            Self::split_merge_into_clauses(&stmt.merge_options)?;
        let mutation_strategy = get_mutation_type(matched_clauses.len(), unmatched_clauses.len())?;

        let mutation = Mutation {
            target_table_identifier,
            expression: MutationExpression::Merge {
                target: target_reference,
                source: source_reference,
                match_expr: stmt.join_expr.clone(),
                has_star_clause: self.has_star_clause(&matched_clauses, &unmatched_clauses),
                mutation_strategy: mutation_strategy.clone(),
            },
            strategy: mutation_strategy,
            matched_clauses,
            unmatched_clauses,
        };

        self.bind_mutation(bind_context, mutation).await
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

fn get_mutation_type(matched_len: usize, unmatched_len: usize) -> Result<MutationStrategy> {
    if matched_len == 0 && unmatched_len > 0 {
        Ok(MutationStrategy::NotMatchedOnly)
    } else if unmatched_len == 0 && matched_len > 0 {
        Ok(MutationStrategy::MatchedOnly)
    } else if unmatched_len > 0 && matched_len > 0 {
        Ok(MutationStrategy::MixedMatched)
    } else {
        Err(ErrorCode::SemanticError(
            "we must have matched or unmatched clause at least one",
        ))
    }
}
