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
#![allow(dead_code)]
#![recursion_limit = "256"]
#![feature(lazy_cell)]

pub mod count;
mod metrics;

pub type VecLabels = Vec<(&'static str, String)>;

pub use crate::metrics::cache;
pub use crate::metrics::cluster;
/// Metrics.
pub use crate::metrics::http;
pub use crate::metrics::interpreter;
pub use crate::metrics::lock;
pub use crate::metrics::mysql;
pub use crate::metrics::openai;
pub use crate::metrics::session;
pub use crate::metrics::storage;
