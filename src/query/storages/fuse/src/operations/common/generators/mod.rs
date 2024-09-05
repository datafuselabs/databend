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

mod append_generator;
mod conflict_resolve_context;
mod mutation_generator;
mod snapshot_generator;
mod truncate_generator;

pub use append_generator::AppendGenerator;
pub use conflict_resolve_context::ConflictResolveContext;
pub use conflict_resolve_context::SnapshotChanges;
pub use conflict_resolve_context::SnapshotMerged;
pub use mutation_generator::MutationGenerator;
pub use snapshot_generator::decorate_snapshot;
pub use snapshot_generator::SnapshotGenerator;
pub use truncate_generator::TruncateGenerator;
pub use truncate_generator::TruncateMode;
