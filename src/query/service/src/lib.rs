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

#![feature(int_roundings)]
#![allow(internal_features)]
#![allow(clippy::useless_asref)]
#![allow(clippy::uninlined_format_args)]
#![feature(ptr_alignment_type)]
#![feature(iter_map_windows)]
#![feature(hash_raw_entry)]
#![feature(core_intrinsics)]
#![feature(arbitrary_self_types)]
#![feature(type_alias_impl_trait)]
#![feature(assert_matches)]
#![feature(trusted_len)]
#![feature(box_patterns)]
#![feature(sync_unsafe_cell)]
#![feature(option_get_or_insert_default)]
#![feature(result_flattening)]
#![feature(iterator_try_reduce)]
#![feature(cursor_remaining)]
#![feature(vec_into_raw_parts)]
#![feature(hash_extract_if)]
#![feature(impl_trait_in_assoc_type)]
#![feature(iterator_try_collect)]
#![feature(let_chains)]
#![feature(try_blocks)]
#![feature(variant_count)]
#![feature(duration_constructors)]
#![allow(clippy::diverging_sub_expression)]
#![allow(clippy::arc_with_non_send_sync)]

extern crate core;

pub mod auth;
pub mod catalogs;
pub mod clusters;
pub mod databases;
pub mod interpreters;
pub mod local;
pub mod locks;
pub mod pipelines;
pub mod schedulers;
pub mod servers;
pub mod sessions;
pub mod spillers;
pub mod stream;
pub mod table_functions;
pub mod test_kits;

mod builtin;
mod global_services;

pub use databend_common_sql as sql;
pub use databend_common_storages_factory as storages;
pub use global_services::GlobalServices;
