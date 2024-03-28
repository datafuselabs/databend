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

mod common;
mod physical_multi_table_insert;
pub use common::*;
pub use physical_multi_table_insert::*;
mod physical_aggregate_expand;
pub use physical_aggregate_expand::AggregateExpand;
mod physical_aggregate_final;
pub use physical_aggregate_final::AggregateFinal;
mod physical_aggregate_partial;
pub use physical_aggregate_partial::AggregatePartial;
mod physical_commit_sink;
pub use physical_commit_sink::CommitSink;
mod physical_compact_source;
pub use physical_compact_source::CompactSource;
mod physical_constant_table_scan;
pub use physical_constant_table_scan::ConstantTableScan;
mod physical_copy_into_table;
pub use physical_copy_into_table::*;
mod physical_cte_scan;
pub use physical_cte_scan::CteScan;
mod physical_delete_source;
pub use physical_delete_source::DeleteSource;
mod physical_distributed_insert_select;
pub use physical_distributed_insert_select::DistributedInsertSelect;
mod physical_eval_scalar;
pub use physical_eval_scalar::EvalScalar;
mod physical_exchange;
pub use physical_exchange::Exchange;
mod physical_exchange_sink;
mod physical_update_source;
pub use physical_exchange_sink::ExchangeSink;
pub use physical_update_source::UpdateSource;
mod physical_exchange_source;
pub use physical_exchange_source::ExchangeSource;
mod physical_filter;
pub use physical_filter::Filter;
mod physical_hash_join;
pub use physical_hash_join::HashJoin;
mod physical_join;
pub use physical_join::PhysicalJoinType;
mod physical_limit;
pub use physical_limit::Limit;
mod physical_materialized_cte;
pub use physical_materialized_cte::MaterializedCte;
mod physical_merge_into;
pub use physical_merge_into::*;
mod physical_merge_into_add_row_number;
pub use physical_merge_into_add_row_number::MergeIntoAddRowNumber;
mod physical_project;
pub use physical_project::Project;
mod physical_project_set;
pub use physical_project_set::ProjectSet;
mod physical_range_join;
pub use physical_range_join::*;
mod physical_recluster_sink;
pub use physical_recluster_sink::ReclusterSink;
mod physical_recluster_source;
pub use physical_recluster_source::*;
mod physical_refresh_index;
pub use physical_refresh_index::RefreshIndex;
mod physical_replace_async_source;
pub use physical_replace_async_source::ReplaceAsyncSourcer;
mod physical_replace_deduplicate;
pub use physical_replace_deduplicate::*;
mod physical_replace_into;
pub use physical_replace_into::ReplaceInto;
mod physical_row_fetch;
pub use physical_row_fetch::RowFetch;
mod physical_sort;
pub use physical_sort::Sort;
mod physical_table_scan;
pub use physical_table_scan::TableScan;

mod physical_union_all;
pub use physical_union_all::UnionAll;
mod physical_window;
pub use physical_window::*;
mod physical_copy_into_location;
mod physical_udf;
pub use physical_copy_into_location::CopyIntoLocation;
pub use physical_udf::Udf;
pub use physical_udf::UdfFunctionDesc;
