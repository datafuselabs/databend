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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check::check_cast;
use databend_common_expression::ComputedExpr;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Expr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_license::license::Feature::ComputedColumn;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_pipeline_transforms::processors::Transform;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::evaluator::CompoundBlockOperator;
use databend_common_sql::parse_computed_expr;

use crate::sessions::QueryContext;

pub struct TransformAddComputedColumns {
    expression_transform: CompoundBlockOperator,
    input_len: usize,
}

impl TransformAddComputedColumns
where Self: Transform
{
    pub fn try_new(
        ctx: Arc<QueryContext>,
        input_schema: DataSchemaRef,
        output_schema: DataSchemaRef,
    ) -> Result<Self> {
        LicenseManagerSwitch::instance()
            .check_enterprise_enabled(ctx.get_license_key(), ComputedColumn)?;

        let mut exprs = Vec::with_capacity(output_schema.fields().len());
        for f in output_schema.fields().iter() {
            let expr = if !input_schema.has_field(f.name()) {
                if let Some(ComputedExpr::Stored(stored_expr)) = f.computed_expr() {
                    let expr = parse_computed_expr(ctx.clone(), input_schema.clone(), stored_expr)?;
                    check_cast(None, false, expr, f.data_type(), &BUILTIN_FUNCTIONS)?
                } else {
                    return Err(ErrorCode::Internal(
                        "Missed field must be a computed column",
                    ));
                }
            } else {
                let field = input_schema.field_with_name(f.name()).unwrap();
                let id = input_schema.index_of(f.name()).unwrap();
                Expr::ColumnRef {
                    span: None,
                    id,
                    data_type: field.data_type().clone(),
                    display_name: field.name().clone(),
                }
            };
            exprs.push(expr);
        }

        let func_ctx = ctx.get_function_context()?;
        let expression_transform = CompoundBlockOperator {
            ctx: func_ctx,
            operators: vec![BlockOperator::Map {
                exprs,
                projections: None,
            }],
        };

        Ok(Self {
            expression_transform,
            input_len: input_schema.num_fields(),
        })
    }
}

impl Transform for TransformAddComputedColumns {
    const NAME: &'static str = "AddComputedColumnsTransform";

    fn transform(&mut self, mut block: DataBlock) -> Result<DataBlock> {
        block = self.expression_transform.transform(block)?;
        let columns = block.columns()[self.input_len..].to_owned();
        Ok(DataBlock::new(columns, block.num_rows()))
    }
}
