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
#![feature(box_patterns)]
#![feature(iterator_try_reduce)]
#![feature(let_chains)]
#![feature(try_blocks)]
#![feature(extend_one)]
#![feature(lazy_cell)]
#![feature(if_let_guard)]
#![feature(iter_next_chunk)]

pub mod evaluator;
pub mod executor;
pub mod planner;

pub use planner::*;
