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

// For use of const fn: `Option::<T>::unwrap` at compile time.
#![feature(const_option)]
#![feature(box_into_inner)]
#![allow(clippy::uninlined_format_args)]

//! Provides conversion from and to protobuf defined meta data, which is used for transport.
//!
//! Thus protobuf messages has the maximized compatibility.
//! I.e., a protobuf message is able to contain several different versions of metadata in one format.
//! This mod will convert protobuf message to the current version of meta data used in databend-query.
//!
//! # Versioning and compatibility
//!
//! Alice and Bob can talk with each other if they are compatible.
//! Alice and Bob both have two versioning related field:
//! - `ver`: the version of the subject,
//! - and the minimal version of the target it can talk to.
//!
//! And out algorithm defines that Alice and Bob are compatible iff:
//! - `Alice.min_bob_ver <= Bob.ver`
//! - `Bob.min_alice_ver <= Alice.ver`
//!
//! E.g.:
//! - `A: (ver=3, min_b_ver=1)` is compatible with `B: (ver=3, min_a_ver=2)`.
//! - `A: (ver=4, min_b_ver=4)` is **NOT** compatible with `B: (ver=3, min_a_ver=2)`.
//!   Because although `A.ver(4) >= B.min_a_ver(3)` holds,
//!   but `B.ver(3) >= A.min_b_ver(4)` does not hold.
//!
//! ```text
//! B.ver:    1             3      4
//! B --------+-------------+------+------------>
//!           ^      .------'      ^
//!           |      |             |
//!           '-------------.      |
//!                  |      |      |
//!                  v      |      |
//! A ---------------+------+------+------------>
//! A.ver:           2      3      4
//! ```
//!
//! # Versioning implementation
//!
//! Since a client writes and reads data to meta-service, it is Alice and Bob at the same time(data producer and consumer).
//! Thus it has three version attributes(not 4, because Alice.ver==Bob.ver):
//! - `reader.VER` and `message.VER` are the version of the reader and the writer.
//! - `reader.MIN_MSG_VER` is the minimal message version this program can read.
//! - `message.MIN_READER_VER` is the minimal reader(program) version that can read this message.

mod background_job_from_to_protobuf_impl;
mod background_task_from_to_protobuf_impl;
mod catalog_from_to_protobuf_impl;
mod config_from_to_protobuf_impl;
mod connection_from_to_protobuf_impl;
mod data_mask_from_to_protobuf_impl;
mod database_from_to_protobuf_impl;
mod datetime_from_to_protobuf_impl;
mod dictionary_from_to_protobuf_impl;
mod file_format_from_to_protobuf_impl;
mod from_to_protobuf;
mod index_from_to_protobuf_impl;
mod least_visible_time_from_to_protobuf_impl;
mod lock_from_to_protobuf_impl;
mod owner_from_to_protobuf_impl;
mod ownership_from_to_protobuf_impl;
mod role_from_to_protobuf_impl;
mod schema_from_to_protobuf_impl;
mod sequence_from_to_protobuf_impl;
mod share_from_to_protobuf_impl;
mod share_meta_v1_from_to_protobuf_impl;
mod share_meta_v2_from_to_protobuf_impl;
mod stage_from_to_protobuf_impl;
mod table_from_to_protobuf_impl;
mod tenant_quota_from_to_protobuf_impl;
mod tident_from_to_protobuf_impl;
mod udf_from_to_protobuf_impl;
mod user_from_to_protobuf_impl;
mod util;
mod virtual_column_from_to_protobuf_impl;

pub use from_to_protobuf::FromToProto;
pub use from_to_protobuf::FromToProtoEnum;
pub use from_to_protobuf::Incompatible;
pub use util::missing;
pub use util::reader_check_msg;
pub use util::MIN_MSG_VER;
pub use util::MIN_READER_VER;
pub use util::VER;
