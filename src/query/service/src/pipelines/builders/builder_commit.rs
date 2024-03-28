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

use databend_common_exception::Result;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_transforms::processors::AccumulatingTransformer;
use databend_common_pipeline_transforms::processors::AsyncAccumulatingTransformer;
use databend_common_sql::executor::physical_plans::CommitSink as PhysicalCommitSink;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_storages_fuse::operations::CommitSink;
use databend_common_storages_fuse::operations::MutationGenerator;
use databend_common_storages_fuse::operations::TableMutationAggregator;
use databend_common_storages_fuse::operations::TransformMergeCommitMeta;
use databend_common_storages_fuse::FuseTable;

use crate::locks::LockManager;
use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_commit_sink(&mut self, plan: &PhysicalCommitSink) -> Result<()> {
        self.build_pipeline(&plan.input)?;
        let table =
            self.ctx
                .build_table_by_table_info(&plan.catalog_info, &plan.table_info, None)?;
        let table = FuseTable::try_from_table(table.as_ref())?;
        let cluster_key_id = table.cluster_key_id();

        self.main_pipeline.try_resize(1)?;
        if plan.merge_meta {
            self.main_pipeline.add_transform(|input, output| {
                let merger = TransformMergeCommitMeta::create(cluster_key_id);
                Ok(ProcessorPtr::create(AccumulatingTransformer::create(
                    input, output, merger,
                )))
            })?;
        } else {
            self.main_pipeline.add_transform(|input, output| {
                let base_segments = if matches!(plan.mutation_kind, MutationKind::Compact) {
                    vec![]
                } else {
                    plan.snapshot.segments.clone()
                };
                let mutation_aggregator = TableMutationAggregator::new(
                    table,
                    self.ctx.clone(),
                    base_segments,
                    plan.mutation_kind,
                );
                Ok(ProcessorPtr::create(AsyncAccumulatingTransformer::create(
                    input,
                    output,
                    mutation_aggregator,
                )))
            })?;
        }

        let snapshot_gen = MutationGenerator::new(plan.snapshot.clone(), false);
        let lock = if plan.need_lock {
            Some(LockManager::create_table_lock(plan.table_info.clone())?)
        } else {
            None
        };
        self.main_pipeline.add_sink(|input| {
            CommitSink::try_create(
                table,
                self.ctx.clone(),
                None,
                plan.update_stream_meta.clone(),
                snapshot_gen.clone(),
                input,
                None,
                lock.clone(),
                None,
                plan.deduplicated_label.clone(),
            )
        })
    }
}
