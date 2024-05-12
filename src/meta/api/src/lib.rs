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

#![allow(clippy::uninlined_format_args)]
#![allow(clippy::diverging_sub_expression)]
#![feature(const_fn_floating_point_arithmetic)]

extern crate databend_common_meta_types;

mod background_api;
mod background_api_impl;
mod background_api_test_suite;
mod data_mask_api;
mod data_mask_api_impl;
pub mod kv_app_error;
pub mod kv_pb_api;
pub mod reply;
mod schema_api;
mod schema_api_impl;
mod schema_api_test_suite;
mod sequence_api;
pub(crate) mod testing;
pub mod txn_backoff;
pub(crate) mod util;

pub mod crud;
mod sequence_api_impl;

pub use background_api::BackgroundApi;
pub use background_api_test_suite::BackgroundApiTestSuite;
pub use data_mask_api::DatamaskApi;
pub use schema_api::SchemaApi;
pub use schema_api_test_suite::SchemaApiTestSuite;
pub use sequence_api::SequenceApi;
pub use util::assert_table_exist;
pub use util::db_has_to_exist;
pub use util::deserialize_struct;
pub use util::deserialize_u64;
pub use util::fetch_id;
pub use util::get_pb_value;
pub use util::get_u64_value;
pub use util::list_keys;
pub use util::list_u64_value;
pub use util::send_txn;
pub use util::serialize_struct;
pub use util::serialize_u64;
pub use util::txn_cond_eq_seq;
pub use util::txn_cond_seq;
pub use util::txn_op_del;
pub use util::txn_op_get;
pub use util::txn_op_put;
pub use util::txn_op_put_with_expire;
pub use util::DEFAULT_MGET_SIZE;
