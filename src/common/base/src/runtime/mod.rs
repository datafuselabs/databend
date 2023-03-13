// Copyright 2021 Datafuse Labs.
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

mod block_on;
mod catch_unwind;
mod global_runtime;
#[allow(clippy::module_inception)]
mod runtime;
mod runtime_tracker;
mod thread;
mod thread_pool;

pub use block_on::block_on;
pub use block_on::uncheck_block_on;
pub use catch_unwind::catch_unwind;
pub use catch_unwind::CatchUnwindFuture;
pub use global_runtime::GlobalIORuntime;
pub use runtime::match_join_handle;
pub use runtime::Dropper;
pub use runtime::Runtime;
pub use runtime::TrySpawn;
pub use runtime_tracker::set_alloc_error_hook;
pub use runtime_tracker::LimitMemGuard;
pub use runtime_tracker::MemStat;
pub use runtime_tracker::ThreadTracker;
pub use runtime_tracker::TrackedFuture;
pub use runtime_tracker::UnlimitedFuture;
pub use runtime_tracker::GLOBAL_MEM_STAT;
pub use thread::Thread;
pub use thread::ThreadJoinHandle;
pub use thread_pool::TaskJoinHandler;
pub use thread_pool::ThreadPool;
