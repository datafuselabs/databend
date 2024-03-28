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

#![feature(once_cell_try)]
#![feature(variant_count)]
#![allow(clippy::arc_with_non_send_sync)]
#![allow(clippy::useless_asref)]

pub mod processors;

mod input_error;
mod lock_guard;
mod pipe;
mod pipeline;
mod pipeline_display;
mod unsafe_cell_wrap;

pub use input_error::InputError;
pub use lock_guard::LockGuard;
pub use lock_guard::UnlockApi;
pub use pipe::Pipe;
pub use pipe::PipeItem;
pub use pipe::SinkPipeBuilder;
pub use pipe::SourcePipeBuilder;
pub use pipe::TransformPipeBuilder;
pub use pipeline::query_spill_prefix;
pub use pipeline::DynTransformBuilder;
pub use pipeline::Pipeline;
pub use processors::PlanProfile;
pub use processors::PlanScope;
pub use processors::PlanScopeGuard;
