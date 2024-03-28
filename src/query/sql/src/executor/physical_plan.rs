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

use std::collections::HashMap;

use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_functions::BUILTIN_FUNCTIONS;
use enum_as_inner::EnumAsInner;
use itertools::Itertools;

use crate::executor::physical_plans::AggregateExpand;
use crate::executor::physical_plans::AggregateFinal;
use crate::executor::physical_plans::AggregatePartial;
use crate::executor::physical_plans::ChunkAppendData;
use crate::executor::physical_plans::ChunkCastSchema;
use crate::executor::physical_plans::ChunkCommitInsert;
use crate::executor::physical_plans::ChunkFillAndReorder;
use crate::executor::physical_plans::ChunkFilter;
use crate::executor::physical_plans::ChunkMerge;
use crate::executor::physical_plans::ChunkProject;
use crate::executor::physical_plans::CommitSink;
use crate::executor::physical_plans::CompactSource;
use crate::executor::physical_plans::ConstantTableScan;
use crate::executor::physical_plans::CopyIntoLocation;
use crate::executor::physical_plans::CopyIntoTable;
use crate::executor::physical_plans::CopyIntoTableSource;
use crate::executor::physical_plans::CteScan;
use crate::executor::physical_plans::DeleteSource;
use crate::executor::physical_plans::DistributedInsertSelect;
use crate::executor::physical_plans::Duplicate;
use crate::executor::physical_plans::EvalScalar;
use crate::executor::physical_plans::Exchange;
use crate::executor::physical_plans::ExchangeSink;
use crate::executor::physical_plans::ExchangeSource;
use crate::executor::physical_plans::Filter;
use crate::executor::physical_plans::HashJoin;
use crate::executor::physical_plans::Limit;
use crate::executor::physical_plans::MaterializedCte;
use crate::executor::physical_plans::MergeInto;
use crate::executor::physical_plans::MergeIntoAddRowNumber;
use crate::executor::physical_plans::MergeIntoAppendNotMatched;
use crate::executor::physical_plans::MergeIntoSource;
use crate::executor::physical_plans::Project;
use crate::executor::physical_plans::ProjectSet;
use crate::executor::physical_plans::RangeJoin;
use crate::executor::physical_plans::ReclusterSink;
use crate::executor::physical_plans::ReclusterSource;
use crate::executor::physical_plans::ReplaceAsyncSourcer;
use crate::executor::physical_plans::ReplaceDeduplicate;
use crate::executor::physical_plans::ReplaceInto;
use crate::executor::physical_plans::RowFetch;
use crate::executor::physical_plans::Shuffle;
use crate::executor::physical_plans::Sort;
use crate::executor::physical_plans::TableScan;
use crate::executor::physical_plans::Udf;
use crate::executor::physical_plans::UnionAll;
use crate::executor::physical_plans::UpdateSource;
use crate::executor::physical_plans::Window;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, EnumAsInner)]
pub enum PhysicalPlan {
    /// Query
    TableScan(TableScan),
    Filter(Filter),
    Project(Project),
    EvalScalar(EvalScalar),
    ProjectSet(ProjectSet),
    AggregateExpand(AggregateExpand),
    AggregatePartial(AggregatePartial),
    AggregateFinal(AggregateFinal),
    Window(Window),
    Sort(Sort),
    Limit(Limit),
    RowFetch(RowFetch),
    HashJoin(HashJoin),
    RangeJoin(RangeJoin),
    Exchange(Exchange),
    UnionAll(UnionAll),
    CteScan(CteScan),
    MaterializedCte(MaterializedCte),
    ConstantTableScan(ConstantTableScan),
    Udf(Udf),

    /// For insert into ... select ... in cluster
    DistributedInsertSelect(Box<DistributedInsertSelect>),

    /// Synthesized by fragmented
    ExchangeSource(ExchangeSource),
    ExchangeSink(ExchangeSink),

    /// Delete
    DeleteSource(Box<DeleteSource>),

    /// Copy into table
    CopyIntoTable(Box<CopyIntoTable>),
    CopyIntoLocation(Box<CopyIntoLocation>),

    /// Replace
    ReplaceAsyncSourcer(ReplaceAsyncSourcer),
    ReplaceDeduplicate(Box<ReplaceDeduplicate>),
    ReplaceInto(Box<ReplaceInto>),

    /// MergeInto
    MergeIntoSource(MergeIntoSource),
    MergeInto(Box<MergeInto>),
    MergeIntoAppendNotMatched(Box<MergeIntoAppendNotMatched>),
    MergeIntoAddRowNumber(Box<MergeIntoAddRowNumber>),

    /// Compact
    CompactSource(Box<CompactSource>),

    /// Commit
    CommitSink(Box<CommitSink>),

    /// Recluster
    ReclusterSource(Box<ReclusterSource>),
    ReclusterSink(Box<ReclusterSink>),

    /// Update
    UpdateSource(Box<UpdateSource>),

    /// Multi table insert
    Duplicate(Box<Duplicate>),
    Shuffle(Box<Shuffle>),
    ChunkFilter(Box<ChunkFilter>),
    ChunkProject(Box<ChunkProject>),
    ChunkCastSchema(Box<ChunkCastSchema>),
    ChunkFillAndReorder(Box<ChunkFillAndReorder>),
    ChunkAppendData(Box<ChunkAppendData>),
    ChunkMerge(Box<ChunkMerge>),
    ChunkCommitInsert(Box<ChunkCommitInsert>),
}

impl PhysicalPlan {
    /// Adjust the plan_id of the physical plan.
    /// This function will assign a unique plan_id to each physical plan node in a top-down manner.
    /// Which means the plan_id of a node is always greater than the plan_id of its parent node.
    pub fn adjust_plan_id(&mut self, next_id: &mut u32) {
        match self {
            PhysicalPlan::TableScan(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
            }
            PhysicalPlan::Filter(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::Project(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::EvalScalar(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::ProjectSet(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::AggregateExpand(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::AggregatePartial(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::AggregateFinal(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::Window(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::Sort(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::Limit(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::RowFetch(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::HashJoin(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.probe.adjust_plan_id(next_id);
                plan.build.adjust_plan_id(next_id);
            }
            PhysicalPlan::RangeJoin(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.left.adjust_plan_id(next_id);
                plan.right.adjust_plan_id(next_id);
            }
            PhysicalPlan::Exchange(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::UnionAll(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.left.adjust_plan_id(next_id);
                plan.right.adjust_plan_id(next_id);
            }
            PhysicalPlan::CteScan(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
            }
            PhysicalPlan::MaterializedCte(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
            }
            PhysicalPlan::ConstantTableScan(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
            }
            PhysicalPlan::Udf(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::DistributedInsertSelect(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::ExchangeSource(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
            }
            PhysicalPlan::ExchangeSink(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
            }
            PhysicalPlan::CopyIntoTable(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                match &mut plan.source {
                    CopyIntoTableSource::Query(input) => input.adjust_plan_id(next_id),
                    CopyIntoTableSource::Stage(input) => input.adjust_plan_id(next_id),
                };
            }
            PhysicalPlan::CopyIntoLocation(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::DeleteSource(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
            }
            PhysicalPlan::ReplaceInto(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::MergeInto(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::MergeIntoAddRowNumber(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::MergeIntoSource(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::MergeIntoAppendNotMatched(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::CommitSink(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::ReplaceAsyncSourcer(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
            }
            PhysicalPlan::ReplaceDeduplicate(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::CompactSource(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
            }
            PhysicalPlan::ReclusterSource(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
            }
            PhysicalPlan::ReclusterSink(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::UpdateSource(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
            }
            PhysicalPlan::Duplicate(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::Shuffle(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::ChunkFilter(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::ChunkProject(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::ChunkCastSchema(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::ChunkFillAndReorder(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::ChunkAppendData(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::ChunkMerge(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
            PhysicalPlan::ChunkCommitInsert(plan) => {
                plan.plan_id = *next_id;
                *next_id += 1;
                plan.input.adjust_plan_id(next_id);
            }
        }
    }

    /// Get the id of the plan node
    pub fn get_id(&self) -> u32 {
        match self {
            PhysicalPlan::TableScan(v) => v.plan_id,
            PhysicalPlan::Filter(v) => v.plan_id,
            PhysicalPlan::Project(v) => v.plan_id,
            PhysicalPlan::EvalScalar(v) => v.plan_id,
            PhysicalPlan::ProjectSet(v) => v.plan_id,
            PhysicalPlan::AggregateExpand(v) => v.plan_id,
            PhysicalPlan::AggregatePartial(v) => v.plan_id,
            PhysicalPlan::AggregateFinal(v) => v.plan_id,
            PhysicalPlan::Window(v) => v.plan_id,
            PhysicalPlan::Sort(v) => v.plan_id,
            PhysicalPlan::Limit(v) => v.plan_id,
            PhysicalPlan::RowFetch(v) => v.plan_id,
            PhysicalPlan::HashJoin(v) => v.plan_id,
            PhysicalPlan::RangeJoin(v) => v.plan_id,
            PhysicalPlan::Exchange(v) => v.plan_id,
            PhysicalPlan::UnionAll(v) => v.plan_id,
            PhysicalPlan::DistributedInsertSelect(v) => v.plan_id,
            PhysicalPlan::ExchangeSource(v) => v.plan_id,
            PhysicalPlan::ExchangeSink(v) => v.plan_id,
            PhysicalPlan::CteScan(v) => v.plan_id,
            PhysicalPlan::MaterializedCte(v) => v.plan_id,
            PhysicalPlan::ConstantTableScan(v) => v.plan_id,
            PhysicalPlan::Udf(v) => v.plan_id,
            PhysicalPlan::DeleteSource(v) => v.plan_id,
            PhysicalPlan::MergeInto(v) => v.plan_id,
            PhysicalPlan::MergeIntoAddRowNumber(v) => v.plan_id,
            PhysicalPlan::MergeIntoSource(v) => v.plan_id,
            PhysicalPlan::MergeIntoAppendNotMatched(v) => v.plan_id,
            PhysicalPlan::CommitSink(v) => v.plan_id,
            PhysicalPlan::CopyIntoTable(v) => v.plan_id,
            PhysicalPlan::CopyIntoLocation(v) => v.plan_id,
            PhysicalPlan::ReplaceAsyncSourcer(v) => v.plan_id,
            PhysicalPlan::ReplaceDeduplicate(v) => v.plan_id,
            PhysicalPlan::ReplaceInto(v) => v.plan_id,
            PhysicalPlan::CompactSource(v) => v.plan_id,
            PhysicalPlan::ReclusterSource(v) => v.plan_id,
            PhysicalPlan::ReclusterSink(v) => v.plan_id,
            PhysicalPlan::UpdateSource(v) => v.plan_id,
            PhysicalPlan::Duplicate(v) => v.plan_id,
            PhysicalPlan::Shuffle(v) => v.plan_id,
            PhysicalPlan::ChunkFilter(v) => v.plan_id,
            PhysicalPlan::ChunkProject(v) => v.plan_id,
            PhysicalPlan::ChunkCastSchema(v) => v.plan_id,
            PhysicalPlan::ChunkFillAndReorder(v) => v.plan_id,
            PhysicalPlan::ChunkAppendData(v) => v.plan_id,
            PhysicalPlan::ChunkMerge(v) => v.plan_id,
            PhysicalPlan::ChunkCommitInsert(v) => v.plan_id,
        }
    }

    pub fn output_schema(&self) -> Result<DataSchemaRef> {
        match self {
            PhysicalPlan::TableScan(plan) => plan.output_schema(),
            PhysicalPlan::Filter(plan) => plan.output_schema(),
            PhysicalPlan::Project(plan) => plan.output_schema(),
            PhysicalPlan::EvalScalar(plan) => plan.output_schema(),
            PhysicalPlan::AggregateExpand(plan) => plan.output_schema(),
            PhysicalPlan::AggregatePartial(plan) => plan.output_schema(),
            PhysicalPlan::AggregateFinal(plan) => plan.output_schema(),
            PhysicalPlan::Window(plan) => plan.output_schema(),
            PhysicalPlan::Sort(plan) => plan.output_schema(),
            PhysicalPlan::Limit(plan) => plan.output_schema(),
            PhysicalPlan::RowFetch(plan) => plan.output_schema(),
            PhysicalPlan::HashJoin(plan) => plan.output_schema(),
            PhysicalPlan::Exchange(plan) => plan.output_schema(),
            PhysicalPlan::ExchangeSource(plan) => plan.output_schema(),
            PhysicalPlan::ExchangeSink(plan) => plan.output_schema(),
            PhysicalPlan::UnionAll(plan) => plan.output_schema(),
            PhysicalPlan::ProjectSet(plan) => plan.output_schema(),
            PhysicalPlan::RangeJoin(plan) => plan.output_schema(),
            PhysicalPlan::CopyIntoTable(plan) => plan.output_schema(),
            PhysicalPlan::CopyIntoLocation(plan) => plan.output_schema(),
            PhysicalPlan::CteScan(plan) => plan.output_schema(),
            PhysicalPlan::MaterializedCte(plan) => plan.output_schema(),
            PhysicalPlan::ConstantTableScan(plan) => plan.output_schema(),
            PhysicalPlan::Udf(plan) => plan.output_schema(),
            PhysicalPlan::MergeIntoSource(plan) => plan.input.output_schema(),
            PhysicalPlan::MergeInto(plan) => Ok(plan.output_schema.clone()),
            PhysicalPlan::MergeIntoAddRowNumber(plan) => plan.output_schema(),
            PhysicalPlan::ReplaceAsyncSourcer(_)
            | PhysicalPlan::ReplaceDeduplicate(_)
            | PhysicalPlan::ReplaceInto(_)
            | PhysicalPlan::MergeIntoAppendNotMatched(_)
            | PhysicalPlan::CompactSource(_)
            | PhysicalPlan::CommitSink(_)
            | PhysicalPlan::DistributedInsertSelect(_)
            | PhysicalPlan::DeleteSource(_)
            | PhysicalPlan::ReclusterSource(_)
            | PhysicalPlan::ReclusterSink(_)
            | PhysicalPlan::UpdateSource(_) => Ok(DataSchemaRef::default()),
            PhysicalPlan::Duplicate(plan) => plan.input.output_schema(),
            PhysicalPlan::Shuffle(plan) => plan.input.output_schema(),
            PhysicalPlan::ChunkFilter(plan) => plan.input.output_schema(),
            PhysicalPlan::ChunkProject(_) => todo!(),
            PhysicalPlan::ChunkCastSchema(_) => todo!(),
            PhysicalPlan::ChunkFillAndReorder(_) => todo!(),
            PhysicalPlan::ChunkAppendData(_) => todo!(),
            PhysicalPlan::ChunkMerge(_) => todo!(),
            PhysicalPlan::ChunkCommitInsert(_) => todo!(),
        }
    }

    pub fn name(&self) -> String {
        match self {
            PhysicalPlan::TableScan(v) => match &v.source.source_info {
                DataSourceInfo::TableSource(_) => "TableScan".to_string(),
                DataSourceInfo::StageSource(_) => "StageScan".to_string(),
                DataSourceInfo::ParquetSource(_) => "ParquetScan".to_string(),
                DataSourceInfo::ResultScanSource(_) => "ResultScan".to_string(),
            },
            PhysicalPlan::Filter(_) => "Filter".to_string(),
            PhysicalPlan::Project(_) => "Project".to_string(),
            PhysicalPlan::EvalScalar(_) => "EvalScalar".to_string(),
            PhysicalPlan::AggregateExpand(_) => "AggregateExpand".to_string(),
            PhysicalPlan::AggregatePartial(_) => "AggregatePartial".to_string(),
            PhysicalPlan::AggregateFinal(_) => "AggregateFinal".to_string(),
            PhysicalPlan::Window(_) => "Window".to_string(),
            PhysicalPlan::Sort(_) => "Sort".to_string(),
            PhysicalPlan::Limit(_) => "Limit".to_string(),
            PhysicalPlan::RowFetch(_) => "RowFetch".to_string(),
            PhysicalPlan::HashJoin(_) => "HashJoin".to_string(),
            PhysicalPlan::Exchange(_) => "Exchange".to_string(),
            PhysicalPlan::UnionAll(_) => "UnionAll".to_string(),
            PhysicalPlan::DistributedInsertSelect(_) => "DistributedInsertSelect".to_string(),
            PhysicalPlan::ExchangeSource(_) => "Exchange Source".to_string(),
            PhysicalPlan::ExchangeSink(_) => "Exchange Sink".to_string(),
            PhysicalPlan::ProjectSet(_) => "Unnest".to_string(),
            PhysicalPlan::CompactSource(_) => "CompactBlock".to_string(),
            PhysicalPlan::DeleteSource(_) => "DeleteSource".to_string(),
            PhysicalPlan::CommitSink(_) => "CommitSink".to_string(),
            PhysicalPlan::RangeJoin(_) => "RangeJoin".to_string(),
            PhysicalPlan::CopyIntoTable(_) => "CopyIntoTable".to_string(),
            PhysicalPlan::CopyIntoLocation(_) => "CopyIntoLocation".to_string(),
            PhysicalPlan::ReplaceAsyncSourcer(_) => "ReplaceAsyncSourcer".to_string(),
            PhysicalPlan::ReplaceDeduplicate(_) => "ReplaceDeduplicate".to_string(),
            PhysicalPlan::ReplaceInto(_) => "Replace".to_string(),
            PhysicalPlan::MergeInto(_) => "MergeInto".to_string(),
            PhysicalPlan::MergeIntoSource(_) => "MergeIntoSource".to_string(),
            PhysicalPlan::MergeIntoAppendNotMatched(_) => "MergeIntoAppendNotMatched".to_string(),
            PhysicalPlan::CteScan(_) => "PhysicalCteScan".to_string(),
            PhysicalPlan::MaterializedCte(_) => "PhysicalMaterializedCte".to_string(),
            PhysicalPlan::ConstantTableScan(_) => "PhysicalConstantTableScan".to_string(),
            PhysicalPlan::MergeIntoAddRowNumber(_) => "AddRowNumber".to_string(),
            PhysicalPlan::ReclusterSource(_) => "ReclusterSource".to_string(),
            PhysicalPlan::ReclusterSink(_) => "ReclusterSink".to_string(),
            PhysicalPlan::UpdateSource(_) => "UpdateSource".to_string(),
            PhysicalPlan::Udf(_) => "Udf".to_string(),
            PhysicalPlan::Duplicate(_) => "Duplicate".to_string(),
            PhysicalPlan::Shuffle(_) => "Shuffle".to_string(),
            PhysicalPlan::ChunkFilter(_) => "ChunkFilter".to_string(),
            PhysicalPlan::ChunkProject(_) => "ChunkProject".to_string(),
            PhysicalPlan::ChunkCastSchema(_) => "ChunkCastSchema".to_string(),
            PhysicalPlan::ChunkFillAndReorder(_) => "ChunkFillAndReorder".to_string(),
            PhysicalPlan::ChunkAppendData(_) => "ChunkAppendData".to_string(),
            PhysicalPlan::ChunkMerge(_) => "ChunkMerge".to_string(),
            PhysicalPlan::ChunkCommitInsert(_) => "ChunkCommitInsert".to_string(),
        }
    }

    pub fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        match self {
            PhysicalPlan::TableScan(_)
            | PhysicalPlan::CteScan(_)
            | PhysicalPlan::ConstantTableScan(_)
            | PhysicalPlan::ExchangeSource(_)
            | PhysicalPlan::CompactSource(_)
            | PhysicalPlan::DeleteSource(_)
            | PhysicalPlan::CopyIntoTable(_)
            | PhysicalPlan::ReplaceAsyncSourcer(_)
            | PhysicalPlan::ReclusterSource(_)
            | PhysicalPlan::UpdateSource(_) => Box::new(std::iter::empty()),
            PhysicalPlan::Filter(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Project(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::EvalScalar(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::AggregateExpand(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::AggregatePartial(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::AggregateFinal(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Window(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Sort(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Limit(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::RowFetch(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::HashJoin(plan) => Box::new(
                std::iter::once(plan.probe.as_ref()).chain(std::iter::once(plan.build.as_ref())),
            ),
            PhysicalPlan::Exchange(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::ExchangeSink(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::UnionAll(plan) => Box::new(
                std::iter::once(plan.left.as_ref()).chain(std::iter::once(plan.right.as_ref())),
            ),
            PhysicalPlan::DistributedInsertSelect(plan) => {
                Box::new(std::iter::once(plan.input.as_ref()))
            }
            PhysicalPlan::CommitSink(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::ProjectSet(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::RangeJoin(plan) => Box::new(
                std::iter::once(plan.left.as_ref()).chain(std::iter::once(plan.right.as_ref())),
            ),
            PhysicalPlan::ReplaceDeduplicate(plan) => {
                Box::new(std::iter::once(plan.input.as_ref()))
            }
            PhysicalPlan::ReplaceInto(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::MergeInto(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::MergeIntoAddRowNumber(plan) => {
                Box::new(std::iter::once(plan.input.as_ref()))
            }
            PhysicalPlan::MergeIntoSource(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::MergeIntoAppendNotMatched(plan) => {
                Box::new(std::iter::once(plan.input.as_ref()))
            }
            PhysicalPlan::MaterializedCte(plan) => Box::new(
                std::iter::once(plan.left.as_ref()).chain(std::iter::once(plan.right.as_ref())),
            ),
            PhysicalPlan::ReclusterSink(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Udf(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::CopyIntoLocation(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Duplicate(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::Shuffle(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::ChunkFilter(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::ChunkProject(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::ChunkCastSchema(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::ChunkFillAndReorder(plan) => {
                Box::new(std::iter::once(plan.input.as_ref()))
            }
            PhysicalPlan::ChunkAppendData(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::ChunkMerge(plan) => Box::new(std::iter::once(plan.input.as_ref())),
            PhysicalPlan::ChunkCommitInsert(plan) => Box::new(std::iter::once(plan.input.as_ref())),
        }
    }

    /// Used to find data source info in a non-aggregation and single-table query plan.
    pub fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        match self {
            PhysicalPlan::TableScan(scan) => Some(&scan.source),
            PhysicalPlan::Filter(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::Project(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::EvalScalar(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::Window(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::Sort(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::Limit(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::Exchange(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::ExchangeSink(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::DistributedInsertSelect(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::ProjectSet(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::RowFetch(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::Udf(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::CopyIntoLocation(plan) => plan.input.try_find_single_data_source(),
            PhysicalPlan::UnionAll(_)
            | PhysicalPlan::ExchangeSource(_)
            | PhysicalPlan::HashJoin(_)
            | PhysicalPlan::RangeJoin(_)
            | PhysicalPlan::MaterializedCte(_)
            | PhysicalPlan::AggregateExpand(_)
            | PhysicalPlan::AggregateFinal(_)
            | PhysicalPlan::AggregatePartial(_)
            | PhysicalPlan::CompactSource(_)
            | PhysicalPlan::DeleteSource(_)
            | PhysicalPlan::CommitSink(_)
            | PhysicalPlan::CopyIntoTable(_)
            | PhysicalPlan::ReplaceAsyncSourcer(_)
            | PhysicalPlan::ReplaceDeduplicate(_)
            | PhysicalPlan::ReplaceInto(_)
            | PhysicalPlan::MergeInto(_)
            | PhysicalPlan::MergeIntoAddRowNumber(_)
            | PhysicalPlan::MergeIntoAppendNotMatched(_)
            | PhysicalPlan::MergeIntoSource(_)
            | PhysicalPlan::ConstantTableScan(_)
            | PhysicalPlan::CteScan(_)
            | PhysicalPlan::ReclusterSource(_)
            | PhysicalPlan::ReclusterSink(_)
            | PhysicalPlan::UpdateSource(_)
            | PhysicalPlan::Duplicate(_)
            | PhysicalPlan::Shuffle(_)
            | PhysicalPlan::ChunkFilter(_)
            | PhysicalPlan::ChunkProject(_)
            | PhysicalPlan::ChunkCastSchema(_)
            | PhysicalPlan::ChunkFillAndReorder(_)
            | PhysicalPlan::ChunkAppendData(_)
            | PhysicalPlan::ChunkMerge(_)
            | PhysicalPlan::ChunkCommitInsert(_) => None,
        }
    }

    pub fn is_distributed_plan(&self) -> bool {
        self.children().any(|child| child.is_distributed_plan())
            || matches!(
                self,
                Self::ExchangeSource(_) | Self::ExchangeSink(_) | Self::Exchange(_)
            )
    }

    pub fn get_desc(&self) -> Result<String> {
        Ok(match self {
            PhysicalPlan::TableScan(v) => format!(
                "{}.{}",
                v.source.catalog_info.name_ident.catalog_name,
                v.source.source_info.desc()
            ),
            PhysicalPlan::Filter(v) => match v.predicates.is_empty() {
                true => String::new(),
                false => v.predicates[0].as_expr(&BUILTIN_FUNCTIONS).sql_display(),
            },
            PhysicalPlan::AggregatePartial(v) => {
                v.agg_funcs.iter().map(|x| x.display.clone()).join(", ")
            }
            PhysicalPlan::AggregateFinal(v) => {
                v.agg_funcs.iter().map(|x| x.display.clone()).join(", ")
            }
            PhysicalPlan::Sort(v) => v
                .order_by
                .iter()
                .map(|x| {
                    format!(
                        "{}{}{}",
                        x.display_name,
                        if x.asc { "" } else { " DESC" },
                        if x.nulls_first { " NULLS FIRST" } else { "" },
                    )
                })
                .join(", "),
            PhysicalPlan::Limit(v) => match v.limit {
                Some(limit) => format!("LIMIT {} OFFSET {}", limit, v.offset),
                None => format!("OFFSET {}", v.offset),
            },
            PhysicalPlan::Project(v) => v
                .output_schema()?
                .fields
                .iter()
                .map(|x| x.name())
                .join(", "),
            PhysicalPlan::EvalScalar(v) => v
                .exprs
                .iter()
                .map(|(x, _)| x.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                .join(", "),
            PhysicalPlan::HashJoin(v) => {
                let mut conditions = v
                    .build_keys
                    .iter()
                    .zip(v.probe_keys.iter())
                    .map(|(l, r)| {
                        format!(
                            "({} = {})",
                            l.as_expr(&BUILTIN_FUNCTIONS).sql_display(),
                            r.as_expr(&BUILTIN_FUNCTIONS).sql_display()
                        )
                    })
                    .collect::<Vec<_>>();

                conditions.extend(
                    v.non_equi_conditions
                        .iter()
                        .map(|x| x.as_expr(&BUILTIN_FUNCTIONS).sql_display()),
                );

                conditions.join(" AND ")
            }
            PhysicalPlan::ProjectSet(v) => v
                .srf_exprs
                .iter()
                .map(|(x, _)| x.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                .join(", "),
            PhysicalPlan::AggregateExpand(v) => v
                .grouping_sets
                .sets
                .iter()
                .map(|set| {
                    set.iter()
                        .map(|x| x.to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                })
                .map(|s| format!("({})", s))
                .collect::<Vec<_>>()
                .join(", "),
            PhysicalPlan::Window(v) => {
                let partition_by = v
                    .partition_by
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");

                let order_by = v
                    .order_by
                    .iter()
                    .map(|x| {
                        format!(
                            "{}{}{}",
                            x.display_name,
                            if x.asc { "" } else { " DESC" },
                            if x.nulls_first { " NULLS FIRST" } else { "" },
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(", ");

                format!("partition by {}, order by {}", partition_by, order_by)
            }
            PhysicalPlan::RowFetch(v) => {
                let table_schema = v.source.source_info.schema();
                let projected_schema = v.cols_to_fetch.project_schema(&table_schema);
                projected_schema.fields.iter().map(|f| f.name()).join(", ")
            }
            PhysicalPlan::RangeJoin(v) => {
                let mut condition = v
                    .conditions
                    .iter()
                    .map(|condition| {
                        let left = condition
                            .left_expr
                            .as_expr(&BUILTIN_FUNCTIONS)
                            .sql_display();
                        let right = condition
                            .right_expr
                            .as_expr(&BUILTIN_FUNCTIONS)
                            .sql_display();
                        format!("{left} {:?} {right}", condition.operator)
                    })
                    .collect::<Vec<_>>();

                condition.extend(
                    v.other_conditions
                        .iter()
                        .map(|x| x.as_expr(&BUILTIN_FUNCTIONS).sql_display()),
                );

                condition.join(" AND ")
            }
            PhysicalPlan::Udf(v) => v
                .udf_funcs
                .iter()
                .map(|x| format!("{}({})", x.func_name, x.arg_exprs.join(", ")))
                .join(", "),
            PhysicalPlan::CteScan(v) => {
                format!("CTE index: {}, sub index: {}", v.cte_idx.0, v.cte_idx.1)
            }
            PhysicalPlan::UnionAll(v) => v
                .pairs
                .iter()
                .map(|(l, r)| format!("#{} <- #{}", l, r))
                .join(", "),
            _ => String::new(),
        })
    }

    pub fn get_labels(&self) -> Result<HashMap<String, Vec<String>>> {
        let mut labels = HashMap::with_capacity(16);

        match self {
            PhysicalPlan::TableScan(v) => {
                labels.insert(String::from("Full table name"), vec![format!(
                    "{}.{}",
                    v.source.catalog_info.name_ident.catalog_name,
                    v.source.source_info.desc()
                )]);

                labels.insert(
                    format!(
                        "Columns ({} / {})",
                        v.output_schema()?.num_fields(),
                        std::cmp::max(
                            v.output_schema()?.num_fields(),
                            v.source.source_info.schema().num_fields(),
                        )
                    ),
                    v.name_mapping.keys().cloned().collect(),
                );
                labels.insert(String::from("Total partitions"), vec![
                    v.source.statistics.partitions_total.to_string(),
                ]);
            }
            PhysicalPlan::Filter(v) => {
                labels.insert(
                    String::from("Filter condition"),
                    v.predicates
                        .iter()
                        .map(|x| x.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                        .collect(),
                );
            }
            PhysicalPlan::Limit(v) => {
                labels.insert(String::from("Offset"), vec![v.offset.to_string()]);

                if let Some(limit) = v.limit {
                    labels.insert(String::from("Number of rows"), vec![limit.to_string()]);
                }
            }
            PhysicalPlan::EvalScalar(v) => {
                labels.insert(
                    String::from("List of Expressions"),
                    v.exprs
                        .iter()
                        .map(|(x, _)| x.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                        .collect(),
                );
            }
            PhysicalPlan::Project(v) => {
                labels.insert(
                    String::from("List of Expressions"),
                    v.output_schema()?
                        .fields
                        .iter()
                        .map(|x| x.name())
                        .cloned()
                        .collect(),
                );
            }
            PhysicalPlan::AggregatePartial(v) => {
                if !v.group_by_display.is_empty() {
                    labels.insert(String::from("Grouping keys"), v.group_by_display.clone());
                }

                if !v.agg_funcs.is_empty() {
                    labels.insert(
                        String::from("Aggregate Functions"),
                        v.agg_funcs.iter().map(|x| x.display.clone()).collect(),
                    );
                }
            }
            PhysicalPlan::AggregateFinal(v) => {
                if !v.group_by_display.is_empty() {
                    labels.insert(String::from("Grouping keys"), v.group_by_display.clone());
                }

                if !v.agg_funcs.is_empty() {
                    labels.insert(
                        String::from("Aggregate Functions"),
                        v.agg_funcs.iter().map(|x| x.display.clone()).collect(),
                    );
                }
            }
            PhysicalPlan::HashJoin(v) => {
                labels.insert(String::from("Join Type"), vec![v.join_type.to_string()]);

                if !v.build_keys.is_empty() {
                    labels.insert(
                        String::from("Join Build Side Keys"),
                        v.build_keys
                            .iter()
                            .map(|x| x.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                            .collect(),
                    );
                }

                if !v.probe_keys.is_empty() {
                    labels.insert(
                        String::from("Join Probe Side Keys"),
                        v.probe_keys
                            .iter()
                            .map(|x| x.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                            .collect(),
                    );
                }

                if !v.non_equi_conditions.is_empty() {
                    labels.insert(
                        String::from("Join Conditions"),
                        v.non_equi_conditions
                            .iter()
                            .map(|x| x.as_expr(&BUILTIN_FUNCTIONS).sql_display())
                            .collect(),
                    );
                }
            }
            _ => {}
        };

        Ok(labels)
    }
}
