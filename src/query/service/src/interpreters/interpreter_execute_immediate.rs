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

use databend_common_ast::ast::DeclareItem;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FunctionCall;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::ScriptStatement;
use databend_common_ast::ast::TypeName;
use databend_common_ast::parser::run_parser;
use databend_common_ast::parser::script::script_block;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::parser::ParseMode;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::block_debug::box_render;
use databend_common_expression::types::decimal::DecimalScalar;
use databend_common_expression::types::decimal::MAX_DECIMAL256_PRECISION;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::StringType;
use databend_common_expression::with_integer_mapped_type;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_script::compile;
use databend_common_script::ir::ColumnAccess;
use databend_common_script::Client;
use databend_common_script::Executor;
use databend_common_script::ReturnValue;
use databend_common_sql::plans::ExecuteImmediatePlan;
use databend_common_sql::Planner;
use databend_common_storages_fuse::TableContext;
use futures::TryStreamExt;
use itertools::Itertools;
use serde_json::Value as JsonValue;

use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterFactory;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

#[derive(Debug)]
pub struct ExecuteImmediateInterpreter {
    ctx: Arc<QueryContext>,
    plan: ExecuteImmediatePlan,
}

impl ExecuteImmediateInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: ExecuteImmediatePlan) -> Result<Self> {
        Ok(ExecuteImmediateInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for ExecuteImmediateInterpreter {
    fn name(&self) -> &str {
        "ExecuteImmediateInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let res: Result<_> = try {
            let settings = self.ctx.get_settings();
            let sql_dialect = settings.get_sql_dialect()?;
            let tokens = tokenize_sql(&self.plan.script)?;
            let mut ast = run_parser(
                &tokens,
                sql_dialect,
                ParseMode::Template,
                false,
                script_block,
            )?;

            let mut src = vec![];
            for declare in ast.declares {
                match declare {
                    DeclareItem::Var(declare) => src.push(ScriptStatement::LetVar { declare }),
                    DeclareItem::Set(declare) => {
                        src.push(ScriptStatement::LetStatement { declare })
                    }
                }
            }
            src.append(&mut ast.body);
            let compiled = compile(&src)?;

            let client = ScriptClient {
                ctx: self.ctx.clone(),
            };
            let mut executor = Executor::load(ast.span, client, compiled);
            let script_max_steps = settings.get_script_max_steps()?;
            let result = executor.run(script_max_steps as usize).await?;

            match result {
                Some(ReturnValue::Var(scalar)) => {
                    PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
                        StringType::from_data(vec![scalar.to_string()]),
                    ])])?
                }
                Some(ReturnValue::Set(set)) => {
                    let rendered_table = box_render(
                        &set.schema,
                        &[set.block.clone()],
                        usize::MAX,
                        usize::MAX,
                        usize::MAX,
                        true,
                    )?;
                    let lines = rendered_table.lines().map(|x| x.to_string()).collect();
                    PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
                        StringType::from_data(lines),
                    ])])?
                }
                None => PipelineBuildResult::from_blocks(vec![DataBlock::new_from_columns(vec![
                    StringType::from_data(Vec::<String>::new()),
                ])])?,
            }
        };

        res.map_err(|err| err.display_with_sql(&self.plan.script))
    }
}

#[derive(Debug, Clone)]
struct QueryResult {
    schema: DataSchemaRef,
    block: DataBlock,
}

struct ScriptClient {
    ctx: Arc<QueryContext>,
}

impl Client for ScriptClient {
    type Var = Scalar;
    type Set = QueryResult;

    async fn query(&self, query: &str) -> Result<Self::Set> {
        let ctx = self
            .ctx
            .get_current_session()
            .create_query_context()
            .await?;

        let mut planner = Planner::new(ctx.clone());
        let (plan, _) = planner.plan_sql(query).await?;
        let interpreter = InterpreterFactory::get(ctx.clone(), &plan).await?;
        let stream = interpreter.execute(ctx.clone()).await?;
        let blocks = stream.try_collect::<Vec<_>>().await?;
        let schema = plan.schema();

        let block = match blocks.len() {
            0 => DataBlock::empty_with_schema(schema.clone()),
            1 => blocks[0].clone(),
            _ => DataBlock::concat(&blocks)?,
        };

        Ok(QueryResult { schema, block })
    }

    fn var_to_ast(&self, scalar: &Self::Var) -> Result<Expr> {
        let ast = match scalar {
            Scalar::Number(v) => with_integer_mapped_type!(|NUM_TYPE| match v {
                NumberScalar::NUM_TYPE(v) => Expr::Literal {
                    span: None,
                    value: Literal::Decimal256 {
                        value: (*v).into(),
                        precision: MAX_DECIMAL256_PRECISION,
                        scale: 0,
                    },
                },
                NumberScalar::Float32(v) => Expr::Literal {
                    span: None,
                    value: Literal::Float64(v.into_inner() as f64),
                },
                NumberScalar::Float64(v) => Expr::Literal {
                    span: None,
                    value: Literal::Float64(v.into_inner()),
                },
            }),
            Scalar::Boolean(v) => Expr::Literal {
                span: None,
                value: Literal::Boolean(*v),
            },
            Scalar::String(v) => Expr::Literal {
                span: None,
                value: Literal::String(v.clone()),
            },
            Scalar::Tuple(v) => Expr::FunctionCall {
                span: None,
                func: FunctionCall {
                    distinct: false,
                    name: Identifier::from_name(None, "tuple"),
                    args: v
                        .iter()
                        .map(|x| self.var_to_ast(&x.to_owned()))
                        .collect::<Result<Vec<_>>>()?,
                    params: vec![],
                    window: None,
                    lambda: None,
                },
            },
            Scalar::Array(v) => Expr::FunctionCall {
                span: None,
                func: FunctionCall {
                    distinct: false,
                    name: Identifier::from_name(None, "array"),
                    args: v
                        .iter()
                        .map(|x| self.var_to_ast(&x.to_owned()))
                        .collect::<Result<Vec<_>>>()?,
                    params: vec![],
                    window: None,
                    lambda: None,
                },
            },
            Scalar::Decimal(DecimalScalar::Decimal128(v, size)) => Expr::Literal {
                span: None,
                value: Literal::Decimal256 {
                    value: (*v).into(),
                    precision: size.precision,
                    scale: size.scale,
                },
            },
            Scalar::Decimal(DecimalScalar::Decimal256(v, size)) => Expr::Literal {
                span: None,
                value: Literal::Decimal256 {
                    value: *v,
                    precision: size.precision,
                    scale: size.scale,
                },
            },
            Scalar::Map(v) => {
                let col = v.as_tuple().unwrap();
                let keys = col[0]
                    .iter()
                    .map(|x| self.var_to_ast(&x.to_owned()))
                    .collect::<Result<Vec<_>>>()?;
                let vals = col[1]
                    .iter()
                    .map(|x| self.var_to_ast(&x.to_owned()))
                    .collect::<Result<Vec<_>>>()?;
                Expr::FunctionCall {
                    span: None,
                    func: FunctionCall {
                        distinct: false,
                        name: Identifier::from_name(None, "map"),
                        args: vec![
                            Expr::FunctionCall {
                                span: None,
                                func: FunctionCall {
                                    distinct: false,
                                    name: Identifier::from_name(None, "array"),
                                    args: keys,
                                    params: vec![],
                                    window: None,
                                    lambda: None,
                                },
                            },
                            Expr::FunctionCall {
                                span: None,
                                func: FunctionCall {
                                    distinct: false,
                                    name: Identifier::from_name(None, "array"),
                                    args: vals,
                                    params: vec![],
                                    window: None,
                                    lambda: None,
                                },
                            },
                        ],
                        params: vec![],
                        window: None,
                        lambda: None,
                    },
                }
            }
            Scalar::Variant(v) => {
                let value = jsonb::from_slice(v).unwrap();
                let json = JsonValue::from(value).to_string();
                Expr::FunctionCall {
                    span: None,
                    func: FunctionCall {
                        distinct: false,
                        name: Identifier::from_name(None, "parse_json"),
                        args: vec![Expr::Literal {
                            span: None,
                            value: Literal::String(json),
                        }],
                        params: vec![],
                        window: None,
                        lambda: None,
                    },
                }
            }
            Scalar::EmptyArray => Expr::FunctionCall {
                span: None,
                func: FunctionCall {
                    distinct: false,
                    name: Identifier::from_name(None, "array"),
                    args: vec![],
                    params: vec![],
                    window: None,
                    lambda: None,
                },
            },
            Scalar::EmptyMap => Expr::FunctionCall {
                span: None,
                func: FunctionCall {
                    distinct: false,
                    name: Identifier::from_name(None, "map"),
                    args: vec![],
                    params: vec![],
                    window: None,
                    lambda: None,
                },
            },
            Scalar::Date(v) => Expr::Cast {
                span: None,
                expr: Box::new(Expr::Literal {
                    span: None,
                    value: Literal::Decimal256 {
                        value: (*v).into(),
                        precision: MAX_DECIMAL256_PRECISION,
                        scale: 0,
                    },
                }),
                target_type: TypeName::Date,
                pg_style: false,
            },
            Scalar::Timestamp(v) => Expr::Cast {
                span: None,
                expr: Box::new(Expr::Literal {
                    span: None,
                    value: Literal::Decimal256 {
                        value: (*v).into(),
                        precision: MAX_DECIMAL256_PRECISION,
                        scale: 0,
                    },
                }),
                target_type: TypeName::Timestamp,
                pg_style: false,
            },
            Scalar::Null => Expr::Literal {
                span: None,
                value: Literal::Null,
            },
            Scalar::Bitmap(_) | Scalar::Binary(_) | Scalar::Geometry(_) | Scalar::Geography(_) => {
                return Err(ErrorCode::Unimplemented(format!(
                    "variable of type {} is not supported yet",
                    scalar.as_ref().infer_data_type()
                )));
            }
        };

        Ok(ast)
    }

    fn read_from_set(&self, set: &Self::Set, row: usize, col: &ColumnAccess) -> Result<Self::Var> {
        let offset = match col {
            ColumnAccess::Position(offset) => *offset,
            // TODO(andylokandy): name resolution
            ColumnAccess::Name(name) => set
                .schema
                .fields()
                .iter()
                .position(|f| f.name() == name)
                .ok_or_else(|| {
                ErrorCode::ScriptExecutionError(format!(
                    "cannot find column with name {} in block, available columns: {}",
                    name,
                    set.schema
                        .fields()
                        .iter()
                        .map(|f| format!("'{}'", f.name()))
                        .join(", ")
                ))
            })?,
        };
        let col = set.block.columns().get(offset).ok_or_else(|| {
            ErrorCode::ScriptExecutionError(format!(
                "cannot read column at offset {} from block with {} columns",
                offset,
                set.block.num_columns()
            ))
        })?;
        let cell = col
            .value
            .index(row)
            .ok_or_else(|| {
                ErrorCode::ScriptExecutionError(format!(
                    "cannot read value at row {} from column with {} rows",
                    row,
                    set.block.num_rows(),
                ))
            })?
            .to_owned();

        Ok(cell)
    }

    fn set_len(&self, set: &Self::Set) -> usize {
        set.block.num_rows()
    }

    fn is_true(&self, scalar: &Self::Var) -> Result<bool> {
        match scalar {
            Scalar::Boolean(v) => Ok(*v),
            _ => Err(ErrorCode::ScriptExecutionError(format!(
                "`is_true` called on non-boolean value {scalar}",
            ))),
        }
    }
}
