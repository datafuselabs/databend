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

use std::any::type_name;

use databend_common_exception::ErrorCode;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::TenantIsEmpty;
use databend_common_meta_stoerr::MetaStorageError;
use databend_common_meta_types::InvalidReply;
use databend_common_meta_types::MetaAPIError;
use databend_common_meta_types::MetaClientError;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::MetaNetworkError;
use tonic::Status;

/// Errors for a kvapi::KVApi based application, such SchemaApi, ShareApi.
///
/// There are three subset of errors in it:
///
/// - (1) AppError: the errors that relate to the application of meta but not about meta itself.
///
/// - (2) Meta data errors raised by a embedded meta-store(not the remote meta-service): StorageError.
///
/// - (3) Meta data errors returned when accessing the remote meta-store service:
///   - ClientError: errors returned when creating a client.
///   - NetworkError: errors returned when sending/receiving RPC to/from a remote meta-store service.
///   - APIError: errors returned by the remote meta-store service.
///
/// Either a local or remote meta-store will returns (1) AppError.
/// An embedded meta-store only returns (1) and (2), while a remote meta-store service only returns (1) and (3)
#[derive(thiserror::Error, serde::Serialize, serde::Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum KVAppError {
    /// An error that indicates something wrong for the application of kvapi::KVApi, but nothing wrong about meta.
    #[error(transparent)]
    AppError(#[from] AppError),

    #[error("fail to access meta-store: {0}")]
    MetaError(#[from] MetaError),
}

impl From<KVAppError> for ErrorCode {
    fn from(e: KVAppError) -> Self {
        match e {
            KVAppError::AppError(app_err) => app_err.into(),
            KVAppError::MetaError(meta_err) => ErrorCode::MetaServiceError(meta_err.to_string()),
        }
    }
}

impl From<Status> for KVAppError {
    fn from(s: Status) -> Self {
        let meta_err = MetaError::from(s);
        Self::MetaError(meta_err)
    }
}

impl From<MetaStorageError> for KVAppError {
    fn from(e: MetaStorageError) -> Self {
        let meta_err = MetaError::from(e);
        Self::MetaError(meta_err)
    }
}

impl From<MetaClientError> for KVAppError {
    fn from(e: MetaClientError) -> Self {
        let meta_err = MetaError::from(e);
        Self::MetaError(meta_err)
    }
}

impl From<MetaNetworkError> for KVAppError {
    fn from(e: MetaNetworkError) -> Self {
        let meta_err = MetaError::from(e);
        Self::MetaError(meta_err)
    }
}

impl From<MetaAPIError> for KVAppError {
    fn from(e: MetaAPIError) -> Self {
        let meta_err = MetaError::from(e);
        Self::MetaError(meta_err)
    }
}

impl From<InvalidReply> for KVAppError {
    fn from(e: InvalidReply) -> Self {
        let meta_err = MetaError::from(e);
        Self::MetaError(meta_err)
    }
}

impl From<TenantIsEmpty> for KVAppError {
    fn from(value: TenantIsEmpty) -> Self {
        KVAppError::AppError(AppError::from(value))
    }
}

impl TryInto<MetaAPIError> for KVAppError {
    type Error = InvalidReply;

    fn try_into(self) -> Result<MetaAPIError, Self::Error> {
        match self {
            KVAppError::AppError(app_err) => Err(InvalidReply::new(
                format!(
                    "expect: {}, got: {}",
                    type_name::<MetaAPIError>(),
                    typ(&app_err)
                ),
                &app_err,
            )),
            KVAppError::MetaError(meta_err) => match meta_err {
                MetaError::APIError(api_err) => Ok(api_err),
                e => Err(InvalidReply::new(
                    format!("expect: {}, got: {}", type_name::<MetaAPIError>(), typ(&e)),
                    &e,
                )),
            },
        }
    }
}

impl TryInto<MetaError> for KVAppError {
    type Error = InvalidReply;

    fn try_into(self) -> Result<MetaError, Self::Error> {
        match self {
            KVAppError::AppError(app_err) => Err(InvalidReply::new(
                format!(
                    "expect: {}, got: {}",
                    type_name::<MetaError>(),
                    typ(&app_err)
                ),
                &app_err,
            )),
            KVAppError::MetaError(meta_err) => Ok(meta_err),
        }
    }
}

fn typ<T>(_v: &T) -> &'static str {
    type_name::<T>()
}
