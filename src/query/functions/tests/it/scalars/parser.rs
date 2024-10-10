// Copyright 2022 Datafuse Labs.
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

use databend_common_ast::ast::BinaryOperator;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr as AExpr;
use databend_common_ast::ast::FunctionCall;
use databend_common_ast::ast::IntervalKind;
use databend_common_ast::ast::Literal as ASTLiteral;
use databend_common_ast::ast::MapAccessor;
use databend_common_ast::ast::UnaryOperator;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::parser::Dialect;
use databend_common_base::base::OrderedFloat;
use databend_common_expression::shrink_scalar;
use databend_common_expression::type_check;
use databend_common_expression::types::decimal::DecimalDataType;
use databend_common_expression::types::decimal::DecimalScalar;
use databend_common_expression::types::decimal::DecimalSize;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::ConstantFolder;
use databend_common_expression::FunctionContext;
use databend_common_expression::RawExpr;
use databend_common_expression::Scalar;
use databend_common_functions::BUILTIN_FUNCTIONS;

pub fn parse_raw_expr(text: &str, columns: &[(&str, DataType)]) -> RawExpr {
    let tokens = tokenize_sql(text).unwrap();
    let expr = parse_expr(&tokens, Dialect::PostgreSQL).unwrap();
    transform_expr(expr, columns)
}

macro_rules! with_interval_mapped_name {
    (| $t:tt | $($tail:tt)*) => {
        match_template::match_template! {
            $t = [
              Year => "year", Quarter => "quarter", Month => "month", Week => "week", Day => "day",
              Hour => "hour", Minute => "minute", Second => "second",
            ],
            $($tail)*
        }
    }
}

macro_rules! transform_interval_add_sub {
    ($span: expr, $columns: expr, $op: expr, $unit: expr, $date: expr, $interval: expr) => {
        if $op == BinaryOperator::Plus {
            with_interval_mapped_name!(|INTERVAL| match $unit {
                IntervalKind::INTERVAL => RawExpr::FunctionCall {
                    span: $span,
                    name: concat!("add_", INTERVAL, "s").to_string(),
                    params: vec![],
                    args: vec![
                        transform_expr(*$date, $columns),
                        transform_expr(*$interval, $columns),
                    ],
                },
                kind => {
                    unimplemented!("{kind:?} is not supported for interval")
                }
            })
        } else if $op == BinaryOperator::Minus {
            with_interval_mapped_name!(|INTERVAL| match $unit {
                IntervalKind::INTERVAL => RawExpr::FunctionCall {
                    span: $span,
                    name: concat!("subtract_", INTERVAL, "s").to_string(),
                    params: vec![],
                    args: vec![
                        transform_expr(*$date, $columns),
                        transform_expr(*$interval, $columns),
                    ],
                },
                kind => {
                    unimplemented!("{kind:?} is not supported for interval")
                }
            })
        } else {
            unimplemented!("operator {} is not supported for interval", $op)
        }
    };
}

pub fn transform_expr(ast: AExpr, columns: &[(&str, DataType)]) -> RawExpr {
    match ast {
        AExpr::Literal { span, value } => RawExpr::Constant {
            span,
            scalar: transform_literal(value),
        },
        AExpr::ColumnRef {
            span,
            column:
                ColumnRef {
                    database: None,
                    table: None,
                    column,
                },
        } => {
            let col_id = columns
                .iter()
                .position(|(col_name, _)| *col_name == column.name())
                .unwrap_or_else(|| panic!("expected column {}", column.name()));
            RawExpr::ColumnRef {
                span,
                id: col_id,
                data_type: columns[col_id].1.clone(),
                display_name: columns[col_id].0.to_string(),
            }
        }
        AExpr::Cast {
            span,
            expr,
            target_type,
            ..
        } => RawExpr::Cast {
            span,
            is_try: false,
            expr: Box::new(transform_expr(*expr, columns)),
            dest_type: transform_data_type(target_type),
        },
        AExpr::TryCast {
            span,
            expr,
            target_type,
            ..
        } => RawExpr::Cast {
            span,
            is_try: true,
            expr: Box::new(transform_expr(*expr, columns)),
            dest_type: transform_data_type(target_type),
        },
        AExpr::FunctionCall {
            span,
            func: FunctionCall {
                name, args, params, ..
            },
            ..
        } => RawExpr::FunctionCall {
            span,
            name: name.name,
            args: args
                .into_iter()
                .map(|arg| transform_expr(arg, columns))
                .collect(),
            params: params
                .into_iter()
                .map(|param| {
                    let raw_expr = transform_expr(param, &[]);
                    let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS).unwrap();
                    let (expr, _) = ConstantFolder::fold(
                        &expr,
                        &FunctionContext::default(),
                        &BUILTIN_FUNCTIONS,
                    );
                    expr.into_constant().unwrap().1
                })
                .collect(),
        },
        AExpr::UnaryOp { span, op, expr } => RawExpr::FunctionCall {
            span,
            name: op.to_func_name(),
            params: vec![],
            args: vec![transform_expr(*expr, columns)],
        },
        AExpr::BinaryOp {
            span,
            op,
            left,
            right,
        } => match op {
            BinaryOperator::NotLike => {
                unimplemented!("please use `not (a like b)` instead")
            }
            BinaryOperator::NotRLike | BinaryOperator::NotRegexp => {
                unimplemented!("please use `not (a regexp b)` instead")
            }
            _ => match (*left.clone(), *right.clone()) {
                (AExpr::Interval { expr, unit, .. }, _) => {
                    if op == BinaryOperator::Minus {
                        unimplemented!("interval cannot be minuend")
                    } else {
                        transform_interval_add_sub!(span, columns, op, unit, right, expr)
                    }
                }
                (_, AExpr::Interval { expr, unit, .. }) => {
                    transform_interval_add_sub!(span, columns, op, unit, left, expr)
                }
                (_, _) => RawExpr::FunctionCall {
                    span,
                    name: op.to_func_name(),
                    params: vec![],
                    args: vec![
                        transform_expr(*left, columns),
                        transform_expr(*right, columns),
                    ],
                },
            },
        },
        AExpr::JsonOp {
            span,
            op,
            left,
            right,
        } => RawExpr::FunctionCall {
            span,
            name: op.to_func_name(),
            params: vec![],
            args: vec![
                transform_expr(*left, columns),
                transform_expr(*right, columns),
            ],
        },
        AExpr::Position {
            span,
            substr_expr,
            str_expr,
        } => RawExpr::FunctionCall {
            span,
            name: "position".to_string(),
            params: vec![],
            args: vec![
                transform_expr(*substr_expr, columns),
                transform_expr(*str_expr, columns),
            ],
        },
        AExpr::Trim {
            span,
            expr,
            trim_where,
        } => {
            if let Some(inner) = trim_where {
                match inner.0 {
                    databend_common_ast::ast::TrimWhere::Both => RawExpr::FunctionCall {
                        span,
                        name: "trim_both".to_string(),
                        params: vec![],
                        args: vec![
                            transform_expr(*expr, columns),
                            transform_expr(*inner.1, columns),
                        ],
                    },
                    databend_common_ast::ast::TrimWhere::Leading => RawExpr::FunctionCall {
                        span,
                        name: "trim_leading".to_string(),
                        params: vec![],
                        args: vec![
                            transform_expr(*expr, columns),
                            transform_expr(*inner.1, columns),
                        ],
                    },
                    databend_common_ast::ast::TrimWhere::Trailing => RawExpr::FunctionCall {
                        span,
                        name: "trim_trailing".to_string(),
                        params: vec![],
                        args: vec![
                            transform_expr(*expr, columns),
                            transform_expr(*inner.1, columns),
                        ],
                    },
                }
            } else {
                RawExpr::FunctionCall {
                    span,
                    name: "trim".to_string(),
                    params: vec![],
                    args: vec![transform_expr(*expr, columns)],
                }
            }
        }
        AExpr::Substring {
            span,
            expr,
            substring_from,
            substring_for,
        } => {
            let mut args = vec![
                transform_expr(*expr, columns),
                transform_expr(*substring_from, columns),
            ];
            if let Some(substring_for) = substring_for {
                args.push(transform_expr(*substring_for, columns));
            }
            RawExpr::FunctionCall {
                span,
                name: "substr".to_string(),
                params: vec![],
                args,
            }
        }
        AExpr::Array { span, exprs } => RawExpr::FunctionCall {
            span,
            name: "array".to_string(),
            params: vec![],
            args: exprs
                .into_iter()
                .map(|expr| transform_expr(expr, columns))
                .collect(),
        },
        AExpr::Map { span, kvs } => {
            let mut keys = Vec::with_capacity(kvs.len());
            let mut vals = Vec::with_capacity(kvs.len());
            for (key, val) in kvs {
                keys.push(transform_expr(AExpr::Literal { span, value: key }, columns));
                vals.push(transform_expr(val, columns));
            }
            let keys = RawExpr::FunctionCall {
                span,
                name: "array".to_string(),
                params: vec![],
                args: keys,
            };
            let vals = RawExpr::FunctionCall {
                span,
                name: "array".to_string(),
                params: vec![],
                args: vals,
            };
            let args = vec![keys, vals];
            RawExpr::FunctionCall {
                span,
                name: "map".to_string(),
                params: vec![],
                args,
            }
        }
        AExpr::Tuple { span, exprs } => RawExpr::FunctionCall {
            span,
            name: "tuple".to_string(),
            params: vec![],
            args: exprs
                .into_iter()
                .map(|expr| transform_expr(expr, columns))
                .collect(),
        },
        AExpr::MapAccess {
            span,
            expr,
            accessor,
        } => {
            let (params, args) = match accessor {
                MapAccessor::Bracket { key } => (vec![], vec![
                    transform_expr(*expr, columns),
                    transform_expr(*key, columns),
                ]),
                MapAccessor::Colon { key } => (vec![], vec![
                    transform_expr(*expr, columns),
                    RawExpr::Constant {
                        span,
                        scalar: Scalar::String(key.name),
                    },
                ]),
                MapAccessor::DotNumber { key } => {
                    (vec![key as i64], vec![transform_expr(*expr, columns)])
                }
            };
            let params = params
                .into_iter()
                .map(|x| Scalar::Number(x.into()))
                .collect();
            RawExpr::FunctionCall {
                span,
                name: "get".to_string(),
                params,
                args,
            }
        }
        AExpr::IsNull { span, expr, not } => {
            let expr = transform_expr(*expr, columns);
            let result = RawExpr::FunctionCall {
                span,
                name: "is_not_null".to_string(),
                params: vec![],
                args: vec![expr],
            };

            if not {
                result
            } else {
                RawExpr::FunctionCall {
                    span,
                    name: "not".to_string(),
                    params: vec![],
                    args: vec![result],
                }
            }
        }
        AExpr::DateAdd {
            span,
            unit,
            interval,
            date,
        } => {
            with_interval_mapped_name!(|INTERVAL| match unit {
                IntervalKind::INTERVAL => RawExpr::FunctionCall {
                    span,
                    name: concat!("add_", INTERVAL, "s").to_string(),
                    params: vec![],
                    args: vec![
                        transform_expr(*date, columns),
                        transform_expr(*interval, columns),
                    ],
                },
                kind => {
                    unimplemented!("{kind:?} is not supported")
                }
            })
        }
        AExpr::DateDiff {
            span,
            unit,
            date_start,
            date_end,
        } => {
            with_interval_mapped_name!(|INTERVAL| match unit {
                IntervalKind::INTERVAL => RawExpr::FunctionCall {
                    span,
                    name: concat!("diff_", INTERVAL, "s").to_string(),
                    params: vec![],
                    args: vec![
                        transform_expr(*date_end, columns),
                        transform_expr(*date_start, columns),
                    ],
                },
                kind => {
                    unimplemented!("{kind:?} is not supported")
                }
            })
        }
        AExpr::DateSub {
            span,
            unit,
            interval,
            date,
        } => {
            with_interval_mapped_name!(|INTERVAL| match unit {
                IntervalKind::INTERVAL => RawExpr::FunctionCall {
                    span,
                    name: concat!("subtract_", INTERVAL, "s").to_string(),
                    params: vec![],
                    args: vec![
                        transform_expr(*date, columns),
                        transform_expr(*interval, columns),
                    ],
                },
                kind => {
                    unimplemented!("{kind:?} is not supported")
                }
            })
        }
        AExpr::DateTrunc { span, unit, date } => {
            with_interval_mapped_name!(|INTERVAL| match unit {
                IntervalKind::INTERVAL => RawExpr::FunctionCall {
                    span,
                    name: concat!("to_start_of_", INTERVAL).to_string(),
                    params: vec![],
                    args: vec![transform_expr(*date, columns),],
                },
                kind => {
                    unimplemented!("{kind:?} is not supported")
                }
            })
        }
        AExpr::InList {
            span,
            expr,
            list,
            not,
        } => {
            if not {
                let e = AExpr::UnaryOp {
                    span,
                    op: UnaryOperator::Not,
                    expr: Box::new(AExpr::InList {
                        span,
                        expr,
                        list,
                        not: false,
                    }),
                };
                return transform_expr(e, columns);
            }

            let list: Vec<AExpr> = list
                .into_iter()
                .filter(|e| matches!(e, AExpr::Literal { value, .. } if value != &ASTLiteral::Null))
                .collect();
            if list.is_empty()
                || (list.len() > 3 && list.iter().all(|e| matches!(e, AExpr::Literal { .. })))
            {
                let array_expr = AExpr::Array { span, exprs: list };
                RawExpr::FunctionCall {
                    span,
                    name: "contains".to_string(),
                    params: vec![],
                    args: vec![
                        transform_expr(array_expr, columns),
                        transform_expr(*expr, columns),
                    ],
                }
            } else {
                let result = list
                    .into_iter()
                    .map(|e| AExpr::BinaryOp {
                        span,
                        op: BinaryOperator::Eq,
                        left: expr.clone(),
                        right: Box::new(e),
                    })
                    .fold(None, |mut acc, e| {
                        match acc.as_mut() {
                            None => acc = Some(e),
                            Some(acc) => {
                                *acc = AExpr::BinaryOp {
                                    span,
                                    op: BinaryOperator::Or,
                                    left: Box::new(acc.clone()),
                                    right: Box::new(e),
                                }
                            }
                        }
                        acc
                    })
                    .unwrap();
                transform_expr(result, columns)
            }
        }

        expr => unimplemented!("{expr:?} is unimplemented"),
    }
}

fn transform_data_type(target_type: databend_common_ast::ast::TypeName) -> DataType {
    match target_type {
        databend_common_ast::ast::TypeName::Boolean => DataType::Boolean,
        databend_common_ast::ast::TypeName::UInt8 => DataType::Number(NumberDataType::UInt8),
        databend_common_ast::ast::TypeName::UInt16 => DataType::Number(NumberDataType::UInt16),
        databend_common_ast::ast::TypeName::UInt32 => DataType::Number(NumberDataType::UInt32),
        databend_common_ast::ast::TypeName::UInt64 => DataType::Number(NumberDataType::UInt64),
        databend_common_ast::ast::TypeName::Int8 => DataType::Number(NumberDataType::Int8),
        databend_common_ast::ast::TypeName::Int16 => DataType::Number(NumberDataType::Int16),
        databend_common_ast::ast::TypeName::Int32 => DataType::Number(NumberDataType::Int32),
        databend_common_ast::ast::TypeName::Int64 => DataType::Number(NumberDataType::Int64),
        databend_common_ast::ast::TypeName::Float32 => DataType::Number(NumberDataType::Float32),
        databend_common_ast::ast::TypeName::Float64 => DataType::Number(NumberDataType::Float64),
        databend_common_ast::ast::TypeName::Decimal { precision, scale } => {
            DataType::Decimal(DecimalDataType::from_size(DecimalSize { precision, scale }).unwrap())
        }
        databend_common_ast::ast::TypeName::Binary => DataType::Binary,
        databend_common_ast::ast::TypeName::String => DataType::String,
        databend_common_ast::ast::TypeName::Timestamp => DataType::Timestamp,
        databend_common_ast::ast::TypeName::Date => DataType::Date,
        databend_common_ast::ast::TypeName::Array(item_type) => {
            DataType::Array(Box::new(transform_data_type(*item_type)))
        }
        databend_common_ast::ast::TypeName::Map { key_type, val_type } => {
            let key_type = transform_data_type(*key_type);
            let val_type = transform_data_type(*val_type);
            DataType::Map(Box::new(DataType::Tuple(vec![key_type, val_type])))
        }
        databend_common_ast::ast::TypeName::Bitmap => DataType::Bitmap,
        databend_common_ast::ast::TypeName::Tuple { fields_type, .. } => {
            DataType::Tuple(fields_type.into_iter().map(transform_data_type).collect())
        }
        databend_common_ast::ast::TypeName::Nullable(inner_type) => {
            DataType::Nullable(Box::new(transform_data_type(*inner_type)))
        }
        databend_common_ast::ast::TypeName::Variant => DataType::Variant,
        databend_common_ast::ast::TypeName::Geometry => DataType::Geometry,
        databend_common_ast::ast::TypeName::Geography => DataType::Geography,
        databend_common_ast::ast::TypeName::NotNull(inner_type) => transform_data_type(*inner_type),
    }
}

pub fn transform_literal(lit: ASTLiteral) -> Scalar {
    let scalar = match lit {
        ASTLiteral::UInt64(u) => Scalar::Number(NumberScalar::UInt64(u)),
        ASTLiteral::Decimal256 {
            value,
            precision,
            scale,
        } => Scalar::Decimal(DecimalScalar::Decimal256(value, DecimalSize {
            precision,
            scale,
        })),
        ASTLiteral::String(s) => Scalar::String(s),
        ASTLiteral::Boolean(b) => Scalar::Boolean(b),
        ASTLiteral::Null => Scalar::Null,
        ASTLiteral::Float64(f) => Scalar::Number(NumberScalar::Float64(OrderedFloat(f))),
    };

    shrink_scalar(scalar)
}
