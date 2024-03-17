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

use std::fs::ReadDir;
use std::future::Future;
use std::path::Path;
use std::time::Instant;

use clap::Parser;
use futures_util::stream;
use futures_util::StreamExt;
use sqllogictest::default_column_validator;
use sqllogictest::default_validator;
use sqllogictest::parse_file;
use sqllogictest::DBOutput;
use sqllogictest::DefaultColumnType;
use sqllogictest::Record;
use sqllogictest::Runner;
use sqllogictest::TestError;

use crate::arg::SqlLogicTestArgs;
use crate::client::Client;
use crate::client::ClientType;
use crate::client::HttpClient;
use crate::client::MySQLClient;
use crate::error::DSqlLogicTestError;
use crate::error::Result;
use crate::util::get_files;
use crate::util::lazy_prepare_data;

mod arg;
mod client;
mod error;
mod util;

const HANDLER_MYSQL: &str = "mysql";
const HANDLER_HTTP: &str = "http";

pub struct Databend {
    client: Client,
}

impl Databend {
    pub fn create(client: Client) -> Self {
        Databend { client }
    }
}

#[async_trait::async_trait]
impl sqllogictest::AsyncDB for Databend {
    type Error = DSqlLogicTestError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>> {
        self.client.query(sql).await
    }

    fn engine_name(&self) -> &str {
        self.client.engine_name()
    }
}

#[tokio::main]
pub async fn main() -> Result<()> {
    env_logger::init();
    let args = SqlLogicTestArgs::parse();
    let handlers = match &args.handlers {
        Some(hs) => hs.iter().map(|s| s.as_str()).collect(),
        None => vec![HANDLER_MYSQL, HANDLER_HTTP],
    };
    for handler in handlers.iter() {
        match *handler {
            HANDLER_MYSQL => {
                run_mysql_client().await?;
            }
            HANDLER_HTTP => {
                run_http_client().await?;
            }
            _ => {
                return Err(format!("Unknown test handler: {handler}").into());
            }
        }
    }

    Ok(())
}

async fn run_mysql_client() -> Result<()> {
    println!(
        "MySQL client starts to run with: {:?}",
        SqlLogicTestArgs::parse()
    );
    let suits = SqlLogicTestArgs::parse().suites;
    let suits = std::fs::read_dir(suits).unwrap();
    run_suits(suits, ClientType::MySQL).await?;
    Ok(())
}

async fn run_http_client() -> Result<()> {
    println!(
        "Http client starts to run with: {:?}",
        SqlLogicTestArgs::parse()
    );
    let suits = SqlLogicTestArgs::parse().suites;
    let suits = std::fs::read_dir(suits).unwrap();
    run_suits(suits, ClientType::Http).await?;
    Ok(())
}

// Create new databend with client type
async fn create_databend(client_type: &ClientType) -> Result<Databend> {
    let mut client: Client;
    let args = SqlLogicTestArgs::parse();
    match client_type {
        ClientType::MySQL => {
            let mut mysql_client = MySQLClient::create(&args.database).await?;
            if args.bench {
                mysql_client.enable_bench();
            }
            client = Client::MySQL(mysql_client);
        }
        ClientType::Http => {
            client = Client::Http(HttpClient::create().await?);
        }
    }
    if args.enable_sandbox {
        client.create_sandbox().await?;
    }
    if args.debug {
        client.enable_debug();
    }
    Ok(Databend::create(client))
}

async fn run_suits(suits: ReadDir, client_type: ClientType) -> Result<()> {
    // Todo: set validator to process regex
    let args = SqlLogicTestArgs::parse();
    let mut tasks = vec![];
    let mut num_of_tests = 0;
    let start = Instant::now();
    // Walk each suit dir and read all files in it
    // After get a slt file, set the file name to databend
    for suit in suits {
        // Get a suit and find all slt files in the suit
        let suit = suit.unwrap().path();
        // Parse the suit and find all slt files
        let files = get_files(suit)?;
        for file in files.into_iter() {
            let file_name = file
                .as_ref()
                .unwrap()
                .path()
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
                .to_string();
            if let Some(ref specific_file) = args.file {
                if !specific_file.split(',').any(|f| f.eq(&file_name)) {
                    continue;
                }
            }
            if let Some(ref skip_file) = args.skipped_file {
                if skip_file.split(',').any(|f| f.eq(&file_name)) {
                    continue;
                }
            }
            num_of_tests += parse_file::<DefaultColumnType>(file.as_ref().unwrap().path())
                .unwrap()
                .len();

            lazy_prepare_data(file.as_ref().unwrap().path())?;

            if args.complete {
                let col_separator = " ";
                let validator = default_validator;
                let column_validator = default_column_validator;
                let mut runner = Runner::new(|| async { create_databend(&client_type).await });
                runner
                    .update_test_file(
                        file.unwrap().path(),
                        col_separator,
                        validator,
                        column_validator,
                    )
                    .await
                    .unwrap();
            } else {
                tasks.push(async move { run_file_async(&client_type, file.unwrap().path()).await });
            }
        }
    }
    if args.complete {
        return Ok(());
    }
    // Run all tasks parallel
    run_parallel_async(tasks, num_of_tests).await?;
    let duration = start.elapsed();
    println!(
        "Run all tests[{}] using {} ms",
        num_of_tests,
        duration.as_millis()
    );

    Ok(())
}

async fn run_parallel_async(
    tasks: Vec<impl Future<Output = std::result::Result<Vec<TestError>, TestError>>>,
    num_of_tests: usize,
) -> Result<()> {
    let args = SqlLogicTestArgs::parse();
    let jobs = tasks.len().clamp(1, args.parallel);
    let tasks = stream::iter(tasks).buffer_unordered(jobs);
    let no_fail_fast = args.no_fail_fast;
    if !no_fail_fast {
        let errors = tasks
            .filter_map(|result| async { result.err() })
            .collect()
            .await;
        handle_error_records(errors, no_fail_fast, num_of_tests)?;
    } else {
        let errors: Vec<Vec<TestError>> = tasks
            .filter_map(|result| async { result.ok() })
            .collect()
            .await;
        handle_error_records(
            errors.into_iter().flatten().collect(),
            no_fail_fast,
            num_of_tests,
        )?;
    }
    Ok(())
}

async fn run_file_async(
    client_type: &ClientType,
    filename: impl AsRef<Path>,
) -> std::result::Result<Vec<TestError>, TestError> {
    let start = Instant::now();

    println!(
        "Running {} test for file: {} ...",
        client_type,
        filename.as_ref().display()
    );
    let mut error_records = vec![];
    let no_fail_fast = SqlLogicTestArgs::parse().no_fail_fast;
    let records = parse_file(&filename).unwrap();
    let mut runner = Runner::new(|| async { create_databend(client_type).await });
    for record in records.into_iter() {
        if let Record::Halt { .. } = record {
            break;
        }
        // Capture error record and continue to run next records
        if let Err(e) = runner.run_async(record).await {
            if no_fail_fast {
                error_records.push(e);
            } else {
                return Err(e);
            }
        }
    }
    let run_file_status = match error_records.is_empty() {
        true => "✅",
        false => "❌",
    };
    if !SqlLogicTestArgs::parse().bench {
        println!(
            "Completed {} test for file: {} {} ({:?})",
            client_type,
            filename.as_ref().display(),
            run_file_status,
            start.elapsed(),
        );
    }
    Ok(error_records)
}

fn handle_error_records(
    error_records: Vec<TestError>,
    no_fail_fast: bool,
    num_of_tests: usize,
) -> Result<()> {
    if error_records.is_empty() {
        return Ok(());
    }

    println!(
        "Test finished, fail fast {}, {} out of {} records failed to run",
        if no_fail_fast { "disabled" } else { "enabled" },
        error_records.len(),
        num_of_tests
    );
    for (idx, error_record) in error_records.iter().enumerate() {
        println!("{idx}: {}", error_record.display(true));
    }

    Err(DSqlLogicTestError::SelfError(
        "sqllogictest failed".to_string(),
    ))
}
