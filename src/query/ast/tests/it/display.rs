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

use databend_common_ast::parser::parse_sql;
use databend_common_ast::parser::tokenize_sql;
use databend_common_ast::parser::Dialect;

fn test_stmt_display(sql: &str) {
    let tokens = tokenize_sql(sql).unwrap();
    let (stmt, _) = parse_sql(&tokens, Dialect::PostgreSQL).unwrap();
    let sql1 = stmt.to_string();
    let tokens1 = tokenize_sql(&sql1).unwrap();
    let (stmt1, _) = parse_sql(&tokens1, Dialect::PostgreSQL).unwrap();
    let sql2 = stmt1.to_string();
    assert_eq!(sql1, sql2);
}

#[test]
fn test_multi_table_insert_display() {
    const SQL_FILE_PATH: &str = "tests/it/testsql/multi_table_insert.sql";
    let sqls = std::fs::read_to_string(SQL_FILE_PATH).unwrap();
    for sql in sqls.split(';').filter(|s| !s.is_empty()) {
        test_stmt_display(sql);
    }
}

#[test]
fn test_multi_table_insert_parse_error() {
    const SQL_FILE_PATH: &str = "tests/it/testsql/multi_table_insert_error.sql";
    let sqls = std::fs::read_to_string(SQL_FILE_PATH).unwrap();
    for sql in sqls.split(';').filter(|s| !s.is_empty()) {
        let tokens = tokenize_sql(sql).unwrap();
        assert!(parse_sql(&tokens, Dialect::PostgreSQL).is_err());
    }
}
