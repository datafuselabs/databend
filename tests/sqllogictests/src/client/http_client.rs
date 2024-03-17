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

use std::time::Instant;

use databend_driver::Client;
use databend_driver::Connection;
use databend_driver::DataType;
use futures_util::StreamExt;
use sqllogictest::DBOutput;
use sqllogictest::DefaultColumnType;

use crate::error::Result;

pub struct HttpClient {
    pub conn: Box<dyn Connection>,
    pub debug: bool,
}

// // make error message the same with ErrorCode::display
// fn format_error(value: serde_json::Value) -> String {
//     let value = value.as_object().unwrap();
//     let detail = value["detail"].as_str().unwrap();
//     let code = value["code"].as_u64().unwrap();
//     let message = value["message"].as_str().unwrap();
//     if detail.is_empty() {
//         format!("http query error: code: {}, Text: {}", code, message)
//     } else {
//         format!(
//             "http query error: code: {}, Text: {}\n{}",
//             code, message, detail
//         )
//     }
// }

impl HttpClient {
    pub async fn create() -> Result<Self> {
        let dsn = "databend://root:@localhost:8000/default?sslmode=disable".to_string();
        let client = Client::new(dsn);
        let conn = client.get_conn().await?;
        Ok(Self { conn, debug: false })
    }

    pub async fn query(&mut self, sql: &str) -> Result<DBOutput<DefaultColumnType>> {
        let start = Instant::now();

        let mut rows = self.conn.query_iter(sql).await?;

        let mut parsed_rows = vec![];
        let mut types = vec![];
        while let Some(row) = rows.next().await {
            let row = row?;
            let mut parsed_row = vec![];
            for value in row {
                let mut cell = match value.get_type() {
                    // for backward compatibility
                    DataType::Boolean => match value.to_string().as_str() {
                        "true" => "1".to_string(),
                        "false" => "0".to_string(),
                        _ => unreachable!(),
                    },
                    _ => value.to_string(),
                };
                if cell == "inf" {
                    cell = "Infinity".to_string();
                }
                if cell == "nan" {
                    cell = "NaN".to_string();
                }
                // If the result is empty, we'll use `(empty)` to mark it explicitly to avoid confusion
                if cell.is_empty() {
                    parsed_row.push("(empty)".to_string());
                } else {
                    parsed_row.push(cell);
                }
            }
            parsed_rows.push(parsed_row);
        }
        if !parsed_rows.is_empty() {
            types = vec![DefaultColumnType::Any; rows.schema().fields().len()];
        }

        if self.debug {
            println!(
                "Running sql with http client: [{sql}] ({:?})",
                start.elapsed()
            );
        }

        Ok(DBOutput::Rows {
            types,
            rows: parsed_rows,
        })
    }
}
