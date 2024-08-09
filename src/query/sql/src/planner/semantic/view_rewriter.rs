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

use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::TableReference;
use derive_visitor::VisitorMut;

#[derive(Debug, Clone, Default, VisitorMut)]
#[visitor(TableReference(enter))]
pub struct ViewRewriter {
    pub current_database: String,
}

impl ViewRewriter {
    fn enter_table_reference(&mut self, table_ref: &mut TableReference) {
        if let TableReference::Table {
            span,
            catalog,
            database,
            table,
            alias,
            temporal,
            consume,
            pivot,
            unpivot,
            sample,
        } = table_ref
        {
            // Must rewrite view query when table_ref::database is none. If not:
            // e.g.
            // create view default.v_t as select * from t;
            // use db1; -- db1 does not contain table `t`
            // select * from default.v_t; => select * from (select * from t); -- will return err that unknown table db1.t
            if database.is_none() {
                let database = Some(Identifier::from_name(*span, self.current_database.clone()));
                *table_ref = TableReference::Table {
                    span: *span,
                    catalog: catalog.clone(),
                    database,
                    table: table.clone(),
                    alias: alias.clone(),
                    temporal: temporal.clone(),
                    consume: *consume,
                    pivot: pivot.clone(),
                    unpivot: unpivot.clone(),
                    sample: sample.clone(),
                }
            }
        }
    }
}
