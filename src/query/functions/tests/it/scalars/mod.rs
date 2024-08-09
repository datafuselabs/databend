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

use std::collections::HashMap;
use std::io::Write;

use comfy_table::Table;
use databend_common_exception::Result;
use databend_common_expression::type_check;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::FunctionContext;
use databend_common_expression::Value;
use databend_common_functions::BUILTIN_FUNCTIONS;
use goldenfile::Mint;
use itertools::Itertools;

mod arithmetic;
mod array;
mod binary;
mod bitmap;
mod boolean;
mod cast;
mod comparison;
mod control;
mod datetime;
mod geo;
// NOTE:(everpcpc) result different on macos
// TODO: fix this in running on linux
#[cfg(not(target_os = "macos"))]
mod geo_h3;
mod geography;
mod geometry;
mod hash;
mod map;
mod math;
mod misc;
mod other;
pub(crate) mod parser;
mod regexp;
mod string;
mod tuple;
mod variant;
mod vector;

pub fn run_ast(file: &mut impl Write, text: impl AsRef<str>, columns: &[(&str, Column)]) {
    let text = text.as_ref();
    let result: Result<_> = try {
        let raw_expr = parser::parse_raw_expr(
            text,
            &columns
                .iter()
                .map(|(name, c)| (*name, c.data_type()))
                .collect::<Vec<_>>(),
        );

        let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS)?;

        let input_domains = columns
            .iter()
            .map(|(_, col)| col.domain())
            .enumerate()
            .collect::<HashMap<_, _>>();

        let (optimized_expr, output_domain) = ConstantFolder::fold_with_domain(
            &expr,
            &input_domains,
            &FunctionContext::default(),
            &BUILTIN_FUNCTIONS,
        );

        let remote_expr = optimized_expr.as_remote_expr();
        let optimized_expr = remote_expr.as_expr(&BUILTIN_FUNCTIONS);

        let num_rows = columns.iter().map(|col| col.1.len()).max().unwrap_or(1);
        let block = DataBlock::new(
            columns
                .iter()
                .map(|(_, col)| BlockEntry::new(col.data_type(), Value::Column(col.clone())))
                .collect::<Vec<_>>(),
            num_rows,
        );

        columns.iter().for_each(|(_, col)| {
            test_arrow_conversion(col);
        });

        let func_ctx = FunctionContext::default();
        let evaluator = Evaluator::new(&block, &func_ctx, &BUILTIN_FUNCTIONS);
        let result = evaluator.run(&expr);
        let optimized_result = evaluator.run(&optimized_expr);
        match &result {
            Ok(result) => assert!(
                result
                    .as_ref()
                    .semantically_eq(&optimized_result.clone().unwrap().as_ref()),
                "{} should eq {}, expr: {}, optimized_expr: {}",
                result,
                optimized_result.unwrap(),
                expr.sql_display(),
                optimized_expr.sql_display()
            ),
            Err(e) => {
                let optimized_err = optimized_result.unwrap_err();
                // assert_eq!(e.message(), optimized_err.message());
                assert_eq!(e.span(), optimized_err.span());
            }
        }

        (
            raw_expr,
            expr,
            input_domains,
            optimized_expr,
            output_domain
                .as_ref()
                .map(ToString::to_string)
                .unwrap_or_else(|| "Unknown".to_string()),
            result?,
        )
    };

    match result {
        Ok((raw_expr, expr, input_domains, optimized_expr, output_domain, result)) => {
            writeln!(file, "ast            : {text}").unwrap();
            writeln!(file, "raw expr       : {raw_expr}").unwrap();
            writeln!(file, "checked expr   : {expr}").unwrap();
            if optimized_expr != expr {
                writeln!(file, "optimized expr : {optimized_expr}").unwrap();
            }

            match result {
                Value::Scalar(output_scalar) => {
                    writeln!(file, "output type    : {}", expr.data_type()).unwrap();
                    writeln!(file, "output domain  : {output_domain}").unwrap();
                    writeln!(file, "output         : {}", output_scalar.as_ref()).unwrap();
                }
                Value::Column(output_col) => {
                    test_arrow_conversion(&output_col);

                    // Only display the used input columns
                    let used_columns = raw_expr
                        .column_refs()
                        .keys()
                        .cloned()
                        .sorted()
                        .collect::<Vec<_>>();
                    let input_domains = used_columns
                        .iter()
                        .cloned()
                        .map(|i| input_domains[&i].clone())
                        .collect::<Vec<_>>();
                    let columns = used_columns
                        .into_iter()
                        .map(|i| columns[i].clone())
                        .collect::<Vec<_>>();

                    let mut table = Table::new();
                    table.load_preset("||--+-++|    ++++++");

                    let mut header = vec!["".to_string()];
                    header.extend(columns.iter().map(|(name, _)| name.to_string()));
                    header.push("Output".to_string());
                    table.set_header(header);

                    let mut type_row = vec!["Type".to_string()];
                    type_row.extend(columns.iter().map(|(_, c)| c.data_type().to_string()));
                    type_row.push(expr.data_type().to_string());
                    table.add_row(type_row);

                    let mut domain_row = vec!["Domain".to_string()];
                    domain_row.extend(input_domains.iter().map(|domain| domain.to_string()));
                    domain_row.push(output_domain.to_string());
                    table.add_row(domain_row);

                    for i in 0..output_col.len() {
                        let mut row = vec![format!("Row {i}")];
                        for (_, col) in columns.iter() {
                            let value = col.index(i).unwrap();
                            row.push(format!("{}", value));
                        }
                        row.push(format!("{}", output_col.index(i).unwrap()));
                        table.add_row(row);
                    }

                    writeln!(file, "evaluation:\n{table}").unwrap();

                    let mut table = Table::new();
                    table.load_preset("||--+-++|    ++++++");

                    table.set_header(["Column", "Data"]);

                    for (name, col) in columns.iter() {
                        table.add_row(&[name.to_string(), format!("{col:?}")]);
                    }

                    table.add_row(["Output".to_string(), format!("{output_col:?}")]);

                    writeln!(file, "evaluation (internal):\n{table}").unwrap();
                }
            }
            write!(file, "\n\n").unwrap();
        }
        Err(err) => {
            writeln!(file, "{}\n", err.display_with_sql(text).message()).unwrap();
        }
    }
}

fn test_arrow_conversion(col: &Column) {
    let arrow_col = col.as_arrow();
    let new_col = Column::from_arrow(&*arrow_col, &col.data_type()).unwrap();
    assert_eq!(col, &new_col, "arrow conversion went wrong");
}

#[test]
fn list_all_builtin_functions() {
    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("function_list.txt").unwrap();

    let fn_registry = &BUILTIN_FUNCTIONS;

    writeln!(file, "Function aliases (alias to origin):").unwrap();
    for (alias_name, original_name) in fn_registry
        .aliases
        .iter()
        .sorted_by_key(|(alias_name, _)| alias_name.to_string())
    {
        writeln!(file, "{alias_name} -> {original_name}").unwrap();
    }
    writeln!(file, "\nFunctions overloads:").unwrap();
    let mut funcs = fn_registry
        .funcs
        .values()
        .flatten()
        .map(|(func, id)| {
            (
                (func.signature.name.clone(), *id),
                format!("{}", func.signature),
            )
        })
        .collect::<Vec<_>>();
    fn_registry
        .factories
        .iter()
        .flat_map(|(name, funcs)| {
            funcs
                .iter()
                .map(|(_, id)| ((name.clone(), *id), format!("{} FACTORY", name.clone())))
        })
        .collect_into(&mut funcs);
    funcs.sort_by_key(|(key, _)| key.clone());
    for ((_, id), sig) in funcs {
        writeln!(file, "{id} {sig}").unwrap();
    }
}

#[test]
fn check_ambiguity() {
    BUILTIN_FUNCTIONS.check_ambiguity()
}
