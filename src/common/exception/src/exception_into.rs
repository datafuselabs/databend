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

use std::error::Error;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use databend_common_ast::Span;
use geozero::error::GeozeroError;

use crate::exception::ErrorCodeBacktrace;
use crate::exception_backtrace::capture;
use crate::ErrorCode;
use crate::ErrorFrame;

#[derive(thiserror::Error)]
enum OtherErrors {
    AnyHow { error: anyhow::Error },
}

impl Display for OtherErrors {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            OtherErrors::AnyHow { error } => write!(f, "{}", error),
        }
    }
}

impl Debug for OtherErrors {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match self {
            OtherErrors::AnyHow { error } => write!(f, "{:?}", error),
        }
    }
}

impl From<std::net::AddrParseError> for ErrorCode {
    fn from(error: std::net::AddrParseError) -> Self {
        ErrorCode::BadAddressFormat(format!("Bad address format, cause: {}", error))
    }
}

impl From<std::str::Utf8Error> for ErrorCode {
    fn from(error: std::str::Utf8Error) -> Self {
        ErrorCode::Internal(format!("Invalid Utf8, cause: {}", error))
    }
}

impl From<anyhow::Error> for ErrorCode {
    fn from(error: anyhow::Error) -> Self {
        ErrorCode::create(
            1002,
            "anyhow",
            format!("{}, source: {:?}", error, error.source()),
            String::new(),
            Some(Box::new(OtherErrors::AnyHow { error })),
            capture(),
        )
    }
}

impl From<&str> for ErrorCode {
    fn from(error: &str) -> Self {
        ErrorCode::from_string(error.to_string())
    }
}

impl From<std::num::ParseIntError> for ErrorCode {
    fn from(error: std::num::ParseIntError) -> Self {
        ErrorCode::from_std_error(error)
    }
}

impl From<std::str::ParseBoolError> for ErrorCode {
    fn from(error: std::str::ParseBoolError) -> Self {
        ErrorCode::from_std_error(error)
    }
}

impl From<String> for ErrorCode {
    fn from(error: String) -> Self {
        ErrorCode::from_string(error)
    }
}

impl From<std::num::ParseFloatError> for ErrorCode {
    fn from(error: std::num::ParseFloatError) -> Self {
        ErrorCode::from_std_error(error)
    }
}

impl From<std::num::TryFromIntError> for ErrorCode {
    fn from(error: std::num::TryFromIntError) -> Self {
        ErrorCode::from_std_error(error)
    }
}

impl From<databend_common_arrow::arrow::error::Error> for ErrorCode {
    fn from(error: databend_common_arrow::arrow::error::Error) -> Self {
        use databend_common_arrow::arrow::error::Error;
        match error {
            Error::NotYetImplemented(v) => ErrorCode::Unimplemented(format!("arrow: {v}")),
            v => ErrorCode::from_std_error(v),
        }
    }
}

impl From<databend_common_arrow::parquet::error::Error> for ErrorCode {
    fn from(error: databend_common_arrow::parquet::error::Error) -> Self {
        ErrorCode::from_std_error(error)
    }
}

impl From<arrow_schema::ArrowError> for ErrorCode {
    fn from(error: arrow_schema::ArrowError) -> Self {
        match error {
            arrow_schema::ArrowError::NotYetImplemented(v) => {
                ErrorCode::Unimplemented(format!("arrow: {v}"))
            }
            v => ErrorCode::from_std_error(v),
        }
    }
}

impl From<parquet::errors::ParquetError> for ErrorCode {
    fn from(error: parquet::errors::ParquetError) -> Self {
        ErrorCode::from_std_error(error)
    }
}

impl From<bincode::error::EncodeError> for ErrorCode {
    fn from(error: bincode::error::EncodeError) -> Self {
        ErrorCode::from_std_error(error)
    }
}

impl From<bincode::error::DecodeError> for ErrorCode {
    fn from(error: bincode::error::DecodeError) -> Self {
        ErrorCode::from_std_error(error)
    }
}

impl From<bincode::serde::EncodeError> for ErrorCode {
    fn from(error: bincode::serde::EncodeError) -> Self {
        ErrorCode::create(
            1002,
            "EncodeError",
            format!("{error:?}"),
            String::new(),
            None,
            capture(),
        )
    }
}

impl From<bincode::serde::DecodeError> for ErrorCode {
    fn from(error: bincode::serde::DecodeError) -> Self {
        ErrorCode::create(
            1002,
            "DecodeError",
            format!("{error:?}"),
            String::new(),
            None,
            capture(),
        )
    }
}

impl From<serde_json::Error> for ErrorCode {
    fn from(error: serde_json::Error) -> Self {
        ErrorCode::from_std_error(error)
    }
}

impl From<std::convert::Infallible> for ErrorCode {
    fn from(v: std::convert::Infallible) -> Self {
        ErrorCode::from_std_error(v)
    }
}

impl From<opendal::Error> for ErrorCode {
    fn from(error: opendal::Error) -> Self {
        match error.kind() {
            opendal::ErrorKind::NotFound => ErrorCode::StorageNotFound(error.to_string()),
            opendal::ErrorKind::PermissionDenied => {
                ErrorCode::StoragePermissionDenied(error.to_string())
            }
            _ => ErrorCode::StorageOther(error.to_string()),
        }
    }
}

impl From<http::Error> for ErrorCode {
    fn from(error: http::Error) -> Self {
        ErrorCode::from_std_error(error)
    }
}

impl From<std::io::Error> for ErrorCode {
    fn from(error: std::io::Error) -> Self {
        use std::io::ErrorKind;

        let msg = format!("{} ({})", error.kind(), &error);

        match error.kind() {
            ErrorKind::NotFound => ErrorCode::StorageNotFound(msg),
            ErrorKind::PermissionDenied => ErrorCode::StoragePermissionDenied(msg),
            _ => ErrorCode::StorageOther(msg),
        }
    }
}

impl From<std::string::FromUtf8Error> for ErrorCode {
    fn from(error: std::string::FromUtf8Error) -> Self {
        ErrorCode::BadBytes(format!(
            "Bad bytes, cannot parse bytes with UTF8, cause: {}",
            error
        ))
    }
}

impl From<databend_common_ast::ParseError> for ErrorCode {
    fn from(error: databend_common_ast::ParseError) -> Self {
        ErrorCode::SyntaxException(error.1).set_span(error.0)
    }
}

impl From<GeozeroError> for ErrorCode {
    fn from(value: GeozeroError) -> Self {
        ErrorCode::GeometryError(value.to_string())
    }
}

impl From<tantivy::TantivyError> for ErrorCode {
    fn from(error: tantivy::TantivyError) -> Self {
        ErrorCode::TantivyError(error.to_string())
    }
}

impl From<tantivy::directory::error::OpenReadError> for ErrorCode {
    fn from(error: tantivy::directory::error::OpenReadError) -> Self {
        ErrorCode::TantivyOpenReadError(error.to_string())
    }
}

impl From<tantivy::query::QueryParserError> for ErrorCode {
    fn from(error: tantivy::query::QueryParserError) -> Self {
        ErrorCode::TantivyQueryParserError(error.to_string())
    }
}

// ===  prost error ===
impl From<prost::EncodeError> for ErrorCode {
    fn from(error: prost::EncodeError) -> Self {
        ErrorCode::BadBytes(format!(
            "Bad bytes, cannot parse bytes with prost, cause: {}",
            error
        ))
    }
}

// ===  ser/de to/from tonic::Status ===
#[derive(thiserror::Error, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SerializedError {
    pub code: u16,
    pub name: String,
    pub message: String,
    pub span: Span,
    pub backtrace: String,
    pub stacks: Vec<SerializedErrorFrame>,
}

impl Display for SerializedError {
    fn fmt(&self, f: &mut Formatter) -> core::fmt::Result {
        write!(f, "Code: {}, Text = {}.", self.code, self.message)
    }
}

impl From<&ErrorCode> for SerializedError {
    fn from(e: &ErrorCode) -> Self {
        SerializedError {
            code: e.code(),
            name: e.name(),
            message: e.message(),
            span: e.span(),
            backtrace: e.backtrace_str(),
            stacks: e.stacks().iter().map(|f| f.into()).collect(),
        }
    }
}

impl From<&SerializedError> for ErrorCode {
    fn from(se: &SerializedError) -> Self {
        let backtrace = match se.backtrace.len() {
            0 => None,
            _ => Some(ErrorCodeBacktrace::Serialized(Arc::new(
                se.backtrace.clone(),
            ))),
        };
        ErrorCode::create(
            se.code,
            se.name.clone(),
            se.message.clone(),
            String::new(),
            None,
            backtrace,
        )
        .set_span(se.span)
        .set_stacks(se.stacks.iter().map(|f| f.into()).collect())
    }
}

#[derive(thiserror::Error, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct SerializedErrorFrame {
    pub file: String,
    pub line: u32,
    pub col: u32,
    pub message: String,
}

impl Display for SerializedErrorFrame {
    fn fmt(&self, f: &mut Formatter) -> core::fmt::Result {
        write!(
            f,
            "{}:{}:{}: {}",
            self.file, self.line, self.col, self.message
        )
    }
}

impl From<&ErrorFrame> for SerializedErrorFrame {
    fn from(frame: &ErrorFrame) -> Self {
        SerializedErrorFrame {
            file: frame.file.clone(),
            line: frame.line,
            col: frame.col,
            message: frame.message.clone(),
        }
    }
}

impl From<&SerializedErrorFrame> for ErrorFrame {
    fn from(frame: &SerializedErrorFrame) -> Self {
        ErrorFrame {
            file: frame.file.clone(),
            line: frame.line,
            col: frame.col,
            message: frame.message.clone(),
        }
    }
}

impl From<reqwest::Error> for ErrorCode {
    fn from(error: reqwest::Error) -> Self {
        ErrorCode::ReqwestError(format!("Reqwest Error, cause: {}", error))
    }
}

impl From<tonic::Status> for ErrorCode {
    fn from(status: tonic::Status) -> Self {
        match status.code() {
            tonic::Code::Unknown => {
                let details = status.details();
                if details.is_empty() {
                    if status.source().map_or(false, |e| e.is::<hyper::Error>()) {
                        return ErrorCode::CannotConnectNode(format!(
                            "{}, source: {:?}",
                            status.message(),
                            status.source()
                        ));
                    }
                    return ErrorCode::UnknownException(format!(
                        "{}, source: {:?}",
                        status.message(),
                        status.source()
                    ));
                }
                match serde_json::from_slice::<SerializedError>(details) {
                    Err(error) => ErrorCode::from(error),
                    Ok(serialized_error) => match serialized_error.backtrace.len() {
                        0 => ErrorCode::create(
                            serialized_error.code,
                            serialized_error.name,
                            serialized_error.message,
                            String::new(),
                            None,
                            None,
                        )
                        .set_span(serialized_error.span),
                        _ => ErrorCode::create(
                            serialized_error.code,
                            serialized_error.name,
                            serialized_error.message,
                            String::new(),
                            None,
                            Some(ErrorCodeBacktrace::Serialized(Arc::new(
                                serialized_error.backtrace,
                            ))),
                        )
                        .set_span(serialized_error.span),
                    },
                }
            }
            _ => ErrorCode::Unimplemented(status.to_string()),
        }
    }
}

impl From<ErrorCode> for tonic::Status {
    fn from(err: ErrorCode) -> Self {
        let error_json = serde_json::to_vec::<SerializedError>(&SerializedError {
            code: err.code(),
            name: err.name(),
            message: err.message(),
            span: err.span(),
            backtrace: {
                let mut str = err.backtrace_str();
                str.truncate(2 * 1024);
                str
            },
            stacks: err.stacks().iter().map(|f| f.into()).collect(),
        });

        match error_json {
            Ok(serialized_error_json) => {
                // Code::Internal will be used by h2, if something goes wrong internally.
                // To distinguish from that, we use Code::Unknown here
                tonic::Status::with_details(
                    tonic::Code::Unknown,
                    err.message(),
                    serialized_error_json.into(),
                )
            }
            Err(error) => tonic::Status::unknown(error.to_string()),
        }
    }
}

impl From<sqlx::Error> for ErrorCode {
    fn from(error: sqlx::Error) -> Self {
        ErrorCode::DictionarySourceError(format!("Dictionary Sqlx Error, cause: {}", error))
    }
}
