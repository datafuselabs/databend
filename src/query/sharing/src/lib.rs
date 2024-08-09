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

//! Sharing crate is used to provide Databend Cloud's sharing support.

#![allow(clippy::uninlined_format_args)]

mod share_endpoint_client;
pub use share_endpoint_client::ShareEndpointClient;

mod layer;
pub use layer::create_share_table_operator;

mod signer;
pub use signer::SharedSigner;

mod share_endpoint;
pub use share_endpoint::ShareEndpointManager;

mod share_presigned_cache_manager;
pub use share_presigned_cache_manager::SharePresignedCacheManager;
