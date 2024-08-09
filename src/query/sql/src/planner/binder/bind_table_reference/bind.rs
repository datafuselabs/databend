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

use databend_common_ast::ast::TableReference;
use databend_common_exception::Result;

use crate::binder::Binder;
use crate::optimizer::SExpr;
use crate::BindContext;

impl Binder {
    pub(crate) fn bind_table_reference(
        &mut self,
        bind_context: &mut BindContext,
        table_ref: &TableReference,
    ) -> Result<(SExpr, BindContext)> {
        match table_ref {
            TableReference::Table {
                span,
                catalog,
                database,
                table,
                alias,
                temporal,
                pivot: _,
                unpivot: _,
                consume,
                sample,
            } => self.bind_table(
                bind_context,
                span,
                catalog,
                database,
                table,
                alias,
                temporal,
                *consume,
                sample,
            ),
            TableReference::TableFunction {
                span,
                name,
                params,
                named_params,
                alias,
                ..
            } => self.bind_table_function(bind_context, span, name, params, named_params, alias),
            TableReference::Subquery {
                span: _,
                lateral,
                subquery,
                alias,
            } => self.bind_subquery(bind_context, *lateral, subquery, alias),
            TableReference::Location {
                span: _,
                location,
                options,
                alias,
            } => self.bind_location(bind_context, location, options, alias),
            TableReference::Join { join, .. } => self.bind_join(bind_context, join),
        }
    }
}
