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

use std::fmt::Display;
use std::io::Error as IOError;

use databend_common_exception::ErrorCode;
use mysql_async::Error as MysqlClientError;
use reqwest::Error as HttpClientError;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Error as SerdeJsonError;
use sqllogictest::TestError;
use thiserror::Error;
use walkdir::Error as WalkDirError;

pub type Result<T> = std::result::Result<T, DSqlLogicTestError>;

#[derive(Debug, Serialize, Deserialize)]
struct Inner {
    code: String,
    message: String,
}
#[derive(Debug, Serialize, Deserialize, Error)]
pub struct TextError {
    error: Inner,
}

impl Display for TextError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            ". Code: {}, Text = {}.",
            self.error.code, self.error.message,
        )
    }
}

#[derive(Debug, Error)]
pub enum DSqlLogicTestError {
    // Error from sqllogictest-rs
    #[error("SqlLogicTest error(from sqllogictest-rs crate): {0}")]
    SqlLogicTest(#[from] TestError),
    // Error from databend
    #[error("Databend error: {0}")]
    Databend(#[from] ErrorCode),
    // Error from mysql client
    #[error("mysql client error: {0}")]
    MysqlClient(#[from] MysqlClientError),
    // Error from http client
    #[error("Http client error(from reqwest crate): {0}")]
    HttpClient(#[from] HttpClientError),
    // Error from WalkDir
    #[error("Walk dir error: {0}")]
    WalkDir(#[from] WalkDirError),
    // Error from IOError
    #[error("io error: {0}")]
    IO(#[from] IOError),
    // Error from serde json
    #[error("Serde json error: {0}")]
    SerdeJson(#[from] SerdeJsonError),
    // Error from databend sqllogictests
    #[error("Databend sqllogictests error: {0}")]
    SelfError(String),
    #[error("Driver error: {0}")]
    DriverError(databend_driver::Error),
}

impl From<String> for DSqlLogicTestError {
    fn from(value: String) -> Self {
        DSqlLogicTestError::SelfError(value)
    }
}

impl From<databend_driver::Error> for DSqlLogicTestError {
    fn from(error: databend_driver::Error) -> Self {
        DSqlLogicTestError::DriverError(error)
    }
}
