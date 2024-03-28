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

#![allow(internal_features)]
#![feature(core_intrinsics)]
#![feature(impl_trait_in_assoc_type)]
#![feature(box_patterns)]
#![allow(clippy::uninlined_format_args)]

mod append;
mod input_context_bridge;
mod one_file_partition;
mod read;
mod stage_table;

pub use stage_table::StageTable;
