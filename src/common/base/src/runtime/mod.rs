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

mod backtrace;
mod catch_unwind;
mod defer;
pub mod error_info;
mod global_runtime;
mod memory;
pub mod metrics;
pub mod profile;
#[allow(clippy::module_inception)]
mod runtime;
mod runtime_tracker;
mod thread;

pub use backtrace::dump_backtrace;
pub use backtrace::get_all_tasks;
pub use backtrace::AsyncTaskItem;
pub use catch_unwind::catch_unwind;
pub use catch_unwind::drop_guard;
pub use catch_unwind::CatchUnwindFuture;
pub use defer::defer;
pub use global_runtime::GlobalIORuntime;
pub use global_runtime::GlobalQueryRuntime;
pub use memory::set_alloc_error_hook;
pub use memory::MemStat;
pub use memory::GLOBAL_MEM_STAT;
pub use runtime::block_on;
pub use runtime::execute_futures_in_parallel;
pub use runtime::spawn;
pub use runtime::spawn_blocking;
pub use runtime::spawn_local;
pub use runtime::try_block_on;
pub use runtime::try_spawn_blocking;
pub use runtime::Dropper;
pub use runtime::JoinHandle;
pub use runtime::Runtime;
pub use runtime::TrySpawn;
pub use runtime::GLOBAL_TASK;
pub use runtime_tracker::LimitMemGuard;
pub use runtime_tracker::ThreadTracker;
pub use runtime_tracker::TrackingGuard;
pub use runtime_tracker::TrackingPayload;
pub use runtime_tracker::UnlimitedFuture;
pub use thread::Thread;
pub use thread::ThreadJoinHandle;
