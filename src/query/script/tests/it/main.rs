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

#![feature(try_blocks)]

use std::collections::HashMap;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::io::Write;
use std::sync::Arc;
use std::sync::Mutex;

use databend_common_ast::ast::Literal;
use databend_common_ast::parser::run_parser;
use databend_common_ast::parser::script::script_stmts;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::parser::Dialect;
use databend_common_ast::parser::ParseMode;
use databend_common_exception::Result;
use databend_common_script::compile;
use databend_common_script::ir::ColumnAccess;
use databend_common_script::Client;
use databend_common_script::Executor;
use goldenfile::Mint;

fn run_script(file: &mut dyn Write, src: &str, client: Option<MockClient>) {
    let src = unindent::unindent(src);
    let src = src.trim();

    let res: Result<_> = try {
        let tokens = tokenize_sql(src).unwrap();
        let ast = run_parser(
            &tokens,
            Dialect::PostgreSQL,
            ParseMode::Template,
            false,
            script_stmts,
        )?;
        let ir = compile(&ast)?;
        let client = client.unwrap();
        let query_log = client.query_log.clone();
        let mut executor = Executor::load(client, ir.clone(), 1000);
        let result = executor.run()?;

        (ir, query_log, result)
    };

    match res {
        Ok((ir, query_log, result)) => {
            writeln!(file, "---------- Input ----------").unwrap();
            writeln!(file, "{}", src).unwrap();
            writeln!(file, "---------- IR -------------").unwrap();
            for line in ir {
                writeln!(file, "{}", line).unwrap();
            }
            writeln!(file, "---------- QUERY ---------").unwrap();
            for (query, block) in query_log.lock().unwrap().iter() {
                writeln!(file, "QUERY: {}", query).unwrap();
                writeln!(file, "BLOCK: {}", block).unwrap();
            }
            writeln!(file, "---------- Output ---------").unwrap();
            writeln!(file, "{:?}", result).unwrap();
            writeln!(file, "\n").unwrap();
        }
        Err(err) => {
            let report = err.display_with_sql(src).message().trim().to_string();
            writeln!(file, "---------- Input ----------").unwrap();
            writeln!(file, "{}", src).unwrap();
            writeln!(file, "---------- Output ----------").unwrap();
            writeln!(file, "{}", report).unwrap();
            writeln!(file, "\n").unwrap();
        }
    }
}

#[test]
fn test_script() {
    let mut mint = Mint::new("tests/it/testdata");
    let file = &mut mint.new_goldenfile("script.txt").unwrap();

    run_script(
        file,
        r#"
            CREATE TABLE t1 (a INT, b INT, c INT);
            INSERT INTO t1 VALUES (1, 2, 3);
            DROP TABLE t1;
        "#,
        mock_client(),
    );
    run_script(
        file,
        r#"
            LET x := 1;
            LET y := x + 1;
            LET z RESULTSET := SELECT :y + 1;
        "#,
        mock_client(),
    );
    run_script(
        file,
        r#"
            RETURN;
        "#,
        mock_client(),
    );
    run_script(
        file,
        r#"
            LET x := 1;
            LET sum := 0;
            FOR x IN x TO x + 2 DO
                sum := sum + x;
            END FOR;
            RETURN sum;
        "#,
        mock_client(),
    );
    run_script(
        file,
        r#"
            LET sum := 0;
            FOR x IN REVERSE -1 TO 1 DO
                sum := sum + x;
            END FOR;
            RETURN sum;
        "#,
        mock_client(),
    );
    run_script(
        file,
        r#"
            LET x RESULTSET := SELECT * FROM numbers(3);
            LET sum := 0;
            FOR row IN x DO
                sum := sum + row.number;
            END FOR;
            RETURN sum;
        "#,
        mock_client(),
    );
    run_script(
        file,
        r#"
            LET x := 1;
            WHILE x < 3 DO
                x := x + 1;
            END WHILE;
            RETURN x;
        "#,
        mock_client(),
    );
    run_script(
        file,
        r#"
            LET x := 1;
            REPEAT
                x := x + 1;
            UNTIL x = 3
            END REPEAT;
            RETURN x;
        "#,
        mock_client(),
    );
    run_script(
        file,
        r#"
            LOOP
                LET x := 0;
                LOOP
                    LET y := x;
                    IF y < 2 THEN
                        x := x + 1;
                        CONTINUE;
                    ELSE
                        BREAK loop_label;
                    END IF;
                END LOOP;
            END LOOP loop_label;
        "#,
        mock_client(),
    );
    run_script(
        file,
        r#"
            LET x := 0;
            LOOP
                LET x := 1;
                BREAK;
            END LOOP;
            RETURN x;
        "#,
        mock_client(),
    );
    run_script(
        file,
        r#"
            LET x := 1;
            CASE x
                WHEN 1 THEN RETURN 'ONE';
                WHEN 2 THEN RETURN 'TWO';
                ELSE RETURN 'OTHER';
            END CASE;
        "#,
        mock_client(),
    );
    run_script(
        file,
        r#"
            LET x := 2;
            CASE x
                WHEN 1 THEN RETURN 'ONE';
                WHEN 2 THEN RETURN 'TWO';
                ELSE RETURN 'OTHER';
            END CASE;
        "#,
        mock_client(),
    );
    run_script(
        file,
        r#"
            LET x := 3;
            CASE x
                WHEN 1 THEN RETURN 'ONE';
                WHEN 2 THEN RETURN 'TWO';
                ELSE RETURN 'OTHER';
            END CASE;
        "#,
        mock_client(),
    );
    run_script(
        file,
        r#"
            LET x := 1;
            CASE
                WHEN x = 1 THEN RETURN 'ONE';
                WHEN x = 2 THEN RETURN 'TWO';
                ELSE RETURN 'OTHER';
            END CASE;
        "#,
        mock_client(),
    );
    run_script(
        file,
        r#"
            LET x := 2;
            CASE
                WHEN x = 1 THEN RETURN 'ONE';
                WHEN x = 2 THEN RETURN 'TWO';
                ELSE RETURN 'OTHER';
            END CASE;
        "#,
        mock_client(),
    );
    run_script(
        file,
        r#"
            LET x := 3;
            CASE
                WHEN x = 1 THEN RETURN 'ONE';
                WHEN x = 2 THEN RETURN 'TWO';
                ELSE RETURN 'OTHER';
            END CASE;
        "#,
        mock_client(),
    );
}

#[test]
fn test_script_error() {
    let mut mint = Mint::new("tests/it/testdata");
    let file = &mut mint.new_goldenfile("script-error.txt").unwrap();

    run_script(
        file,
        r#"
            LET x := y + 1;
        "#,
        None,
    );
    run_script(
        file,
        r#"
            LET x := 1;
            LET x RESULTSET := SELECT 1;
        "#,
        None,
    );
    run_script(
        file,
        r#"
            LET x RESULTSET := SELECT 1;
            LET x := 1;
        "#,
        None,
    );
    run_script(
        file,
        r#"
            LET x RESULTSET := SELECT 1;
            LET y := x;
        "#,
        None,
    );
    run_script(
        file,
        r#"
            LET x := 1;
            LET y := x.a;
        "#,
        None,
    );
    run_script(
        file,
        r#"
            LET x := 'min';
            LET y := IDENTIFIER(:x)([1,2]);
        "#,
        None,
    );
    run_script(
        file,
        r#"
            LET x := 1;
            LET y := :x + 1;
        "#,
        None,
    );
    run_script(
        file,
        r#"
            LET x := 1;
            FOR row IN x DO
                BREAK;
            END FOR;
        "#,
        None,
    );
    run_script(
        file,
        r#"
            BREAK;
        "#,
        None,
    );
    run_script(
        file,
        r#"
            CONTINUE;
        "#,
        None,
    );
    run_script(
        file,
        r#"
            LOOP
                BREAK foo;
            END LOOP bar;
        "#,
        None,
    );
    run_script(
        file,
        r#"
            LOOP
                CONTINUE foo;
            END LOOP bar;
        "#,
        None,
    );
    run_script(
        file,
        r#"
            LOOP
                CONTINUE;
            END LOOP;
        "#,
        mock_client(),
    );
}

fn mock_client() -> Option<MockClient> {
    let client = MockClient::new()
        .response_when(
            "SELECT * FROM numbers(3)",
            MockBlock::named(vec!["number"], vec![
                vec![Literal::UInt64(0)],
                vec![Literal::UInt64(1)],
                vec![Literal::UInt64(2)],
            ]),
        )
        .response_when(
            "CREATE TABLE t1 (a Int32, b Int32, c Int32)",
            MockBlock::empty(),
        )
        .response_when("INSERT INTO t1 VALUES (1, 2, 3)", MockBlock::empty())
        .response_when("DROP TABLE t1", MockBlock::empty())
        .response_when(
            "SELECT * FROM generate_series(1, 1 + 2, 1)",
            MockBlock::unnamed(vec![
                vec![Literal::UInt64(1)],
                vec![Literal::UInt64(2)],
                vec![Literal::UInt64(3)],
            ]),
        )
        .response_when(
            "SELECT * FROM generate_series(1, - 1, -1)",
            MockBlock::unnamed(vec![
                vec![Literal::UInt64(1)],
                vec![Literal::UInt64(0)],
                vec![Literal::Decimal256 {
                    value: (-1).into(),
                    precision: 1,
                    scale: 0,
                }],
            ]),
        )
        .response_when(
            "SELECT 0",
            MockBlock::unnamed(vec![vec![Literal::UInt64(0)]]),
        )
        .response_when(
            "SELECT 1",
            MockBlock::unnamed(vec![vec![Literal::UInt64(1)]]),
        )
        .response_when(
            "SELECT 2",
            MockBlock::unnamed(vec![vec![Literal::UInt64(2)]]),
        )
        .response_when(
            "SELECT 3",
            MockBlock::unnamed(vec![vec![Literal::UInt64(3)]]),
        )
        .response_when(
            "SELECT 6",
            MockBlock::unnamed(vec![vec![Literal::UInt64(6)]]),
        )
        .response_when(
            "SELECT 'ONE'",
            MockBlock::unnamed(vec![vec![Literal::String("ONE".to_string())]]),
        )
        .response_when(
            "SELECT 'TWO'",
            MockBlock::unnamed(vec![vec![Literal::String("TWO".to_string())]]),
        )
        .response_when(
            "SELECT 'OTHER'",
            MockBlock::unnamed(vec![vec![Literal::String("OTHER".to_string())]]),
        )
        .response_when(
            "SELECT 0 + 0",
            MockBlock::unnamed(vec![vec![Literal::UInt64(0)]]),
        )
        .response_when(
            "SELECT 0 + 1",
            MockBlock::unnamed(vec![vec![Literal::UInt64(1)]]),
        )
        .response_when(
            "SELECT 1 + 0",
            MockBlock::unnamed(vec![vec![Literal::UInt64(1)]]),
        )
        .response_when(
            "SELECT 1 + -1",
            MockBlock::unnamed(vec![vec![Literal::UInt64(0)]]),
        )
        .response_when(
            "SELECT 1 + 1",
            MockBlock::unnamed(vec![vec![Literal::UInt64(2)]]),
        )
        .response_when(
            "SELECT 1 + 2",
            MockBlock::unnamed(vec![vec![Literal::UInt64(3)]]),
        )
        .response_when(
            "SELECT 2 + 1",
            MockBlock::unnamed(vec![vec![Literal::UInt64(3)]]),
        )
        .response_when(
            "SELECT 3 + 3",
            MockBlock::unnamed(vec![vec![Literal::UInt64(6)]]),
        )
        .response_when(
            "SELECT 3 * 3",
            MockBlock::unnamed(vec![vec![Literal::UInt64(9)]]),
        )
        .response_when(
            "SELECT is_true(0 < 2)",
            MockBlock::unnamed(vec![vec![Literal::Boolean(true)]]),
        )
        .response_when(
            "SELECT is_true(1 < 2)",
            MockBlock::unnamed(vec![vec![Literal::Boolean(true)]]),
        )
        .response_when(
            "SELECT is_true(2 < 2)",
            MockBlock::unnamed(vec![vec![Literal::Boolean(false)]]),
        )
        .response_when(
            "SELECT is_true(1 = 1)",
            MockBlock::unnamed(vec![vec![Literal::Boolean(true)]]),
        )
        .response_when(
            "SELECT is_true(2 = 1)",
            MockBlock::unnamed(vec![vec![Literal::Boolean(false)]]),
        )
        .response_when(
            "SELECT is_true(2 = 2)",
            MockBlock::unnamed(vec![vec![Literal::Boolean(true)]]),
        )
        .response_when(
            "SELECT is_true(2 = 3)",
            MockBlock::unnamed(vec![vec![Literal::Boolean(false)]]),
        )
        .response_when(
            "SELECT is_true(3 = 1)",
            MockBlock::unnamed(vec![vec![Literal::Boolean(false)]]),
        )
        .response_when(
            "SELECT is_true(3 = 2)",
            MockBlock::unnamed(vec![vec![Literal::Boolean(false)]]),
        )
        .response_when(
            "SELECT is_true(3 = 3)",
            MockBlock::unnamed(vec![vec![Literal::Boolean(true)]]),
        )
        .response_when(
            "SELECT NOT is_true(1 < 3)",
            MockBlock::unnamed(vec![vec![Literal::Boolean(false)]]),
        )
        .response_when(
            "SELECT NOT is_true(2 < 3)",
            MockBlock::unnamed(vec![vec![Literal::Boolean(false)]]),
        )
        .response_when(
            "SELECT NOT is_true(3 < 3)",
            MockBlock::unnamed(vec![vec![Literal::Boolean(true)]]),
        );

    Some(client)
}

#[derive(Debug, Clone)]
struct MockClient {
    responses: HashMap<String, MockBlock>,
    query_log: Arc<Mutex<Vec<(String, MockBlock)>>>,
}

impl MockClient {
    pub fn new() -> Self {
        MockClient {
            responses: HashMap::new(),
            query_log: Arc::new(Mutex::new(vec![])),
        }
    }

    pub fn response_when(mut self, query: &str, block: MockBlock) -> Self {
        self.responses.insert(query.to_string(), block);
        self
    }
}

impl Client for MockClient {
    type Scalar = Literal;
    type DataBlock = MockBlock;

    fn query(&self, query: &str) -> Result<Self::DataBlock> {
        match self.responses.get(query) {
            Some(block) => {
                self.query_log
                    .lock()
                    .unwrap()
                    .push((query.to_string(), block.clone()));
                Ok(block.clone())
            }
            None => panic!("response to query is not defined: {query}"),
        }
    }

    fn scalar_to_literal(&self, scalar: &Self::Scalar) -> Literal {
        scalar.clone()
    }

    fn read_from_block(
        &self,
        block: &Self::DataBlock,
        row: usize,
        col: &ColumnAccess,
    ) -> Self::Scalar {
        match col {
            ColumnAccess::Position(col) => block.data[row][*col].clone(),
            ColumnAccess::Name(name) => {
                let col = block.column_names.iter().position(|x| x == name).unwrap();
                block.data[row][col].clone()
            }
        }
    }

    fn block_len(&self, block: &Self::DataBlock) -> usize {
        block.data.len()
    }

    fn is_true(&self, scalar: &Self::Scalar) -> bool {
        *scalar == Literal::Boolean(true)
    }
}

#[derive(Debug, Clone)]
struct MockBlock {
    column_names: Vec<String>,
    data: Vec<Vec<Literal>>,
}

impl MockBlock {
    pub fn empty() -> Self {
        MockBlock {
            column_names: vec![],
            data: vec![],
        }
    }

    pub fn unnamed(data: Vec<Vec<Literal>>) -> Self {
        MockBlock {
            column_names: (0..data[0].len()).map(|x| format!("${x}")).collect(),
            data,
        }
    }

    pub fn named(
        column_names: impl IntoIterator<Item = &'static str>,
        data: Vec<Vec<Literal>>,
    ) -> Self {
        MockBlock {
            column_names: column_names.into_iter().map(|x| x.to_string()).collect(),
            data,
        }
    }
}

impl Display for MockBlock {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "(")?;
        for (i, name) in self.column_names.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", name)?;
        }
        write!(f, "): ")?;
        for (i, row) in self.data.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "(")?;
            for (j, cell) in row.iter().enumerate() {
                if j > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", cell)?;
            }
            write!(f, ")")?;
        }
        Ok(())
    }
}
