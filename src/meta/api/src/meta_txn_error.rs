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

use databend_common_exception::ErrorCode;
use databend_common_meta_app::app_error::AppErrorMessage;
use databend_common_meta_app::app_error::TxnRetryMaxTimes;
use databend_common_meta_types::InvalidArgument;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::MetaNetworkError;

/// A non-business error occurs when executing a meta-service transaction.
#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum MetaTxnError {
    #[error(transparent)]
    TxnRetryMaxTimes(#[from] TxnRetryMaxTimes),

    #[error("fail to access meta-store: {0}")]
    MetaError(#[from] MetaError),
}

impl From<InvalidArgument> for MetaTxnError {
    fn from(value: InvalidArgument) -> Self {
        let network_error = MetaNetworkError::from(value);
        Self::MetaError(MetaError::from(network_error))
    }
}

impl From<MetaNetworkError> for MetaTxnError {
    fn from(value: MetaNetworkError) -> Self {
        Self::MetaError(MetaError::from(value))
    }
}

impl From<MetaTxnError> for ErrorCode {
    fn from(meta_err: MetaTxnError) -> Self {
        match meta_err {
            MetaTxnError::TxnRetryMaxTimes(err) => ErrorCode::TxnRetryMaxTimes(err.message()),
            MetaTxnError::MetaError(err) => ErrorCode::from(err),
        }
    }
}
