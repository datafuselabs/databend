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

mod clustering_information;
mod clustering_statistics;
mod function_template;
mod fuse_amend;
mod fuse_block;
mod fuse_column;
mod fuse_encoding;
mod fuse_segment;
mod fuse_snapshot;
mod fuse_statistic;
mod set_cache_capacity;
mod table_args;

pub use clustering_information::ClusteringInformationFunc;
pub use clustering_statistics::ClusteringStatisticsFunc;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_function::TableFunction;
pub use function_template::SimpleTableFunc;
pub use function_template::TableFunctionTemplate;
pub use function_template::*;
pub use fuse_amend::FuseAmendTable;
pub use fuse_block::FuseBlockFunc;
pub use fuse_column::FuseColumnFunc;
pub use fuse_encoding::FuseEncodingFunc;
pub use fuse_segment::FuseSegmentFunc;
pub use fuse_snapshot::FuseSnapshotFunc;
pub use fuse_statistic::FuseStatisticsFunc;
pub use set_cache_capacity::SetCacheCapacityFunc;
pub use table_args::*;
