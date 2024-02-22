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

use async_channel::Receiver;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sinks::UnionReceiveSink;
use databend_common_sql::executor::physical_plans::UnionAll;
use databend_common_sql::executor::PhysicalPlan;

use crate::pipelines::processors::transforms::TransformMergeBlock;
use crate::pipelines::PipelineBuilder;
use crate::sessions::QueryContext;

impl PipelineBuilder {
    pub fn build_union_all(&mut self, union_all: &UnionAll) -> Result<()> {
        self.build_pipeline(&union_all.children[0])?;
        let mut remain_children_receivers = vec![];
        for (idx, remaining_child) in union_all.children.iter().skip(1).enumerate() {
            remain_children_receivers.push((idx + 1, self.expand_union_all(remaining_child)?));
        }
        let schemas: Vec<DataSchemaRef> = union_all
            .children
            .iter()
            .map(|plan| plan.output_schema())
            .collect::<Result<_>>()?;
        self.main_pipeline
            .add_transform(|transform_input_port, transform_output_port| {
                Ok(ProcessorPtr::create(TransformMergeBlock::try_create(
                    transform_input_port,
                    transform_output_port,
                    schemas.clone(),
                    union_all.output_cols.clone(),
                    remain_children_receivers.clone(),
                )?))
            })?;
        Ok(())
    }

    fn expand_union_all(&mut self, input: &PhysicalPlan) -> Result<Receiver<DataBlock>> {
        let union_ctx = QueryContext::create_from(self.ctx.clone());
        let mut pipeline_builder = PipelineBuilder::create(
            self.func_ctx.clone(),
            self.settings.clone(),
            union_ctx,
            self.main_pipeline.get_scopes(),
        );
        pipeline_builder.cte_state = self.cte_state.clone();

        let mut build_res = pipeline_builder.finalize(input)?;

        assert!(build_res.main_pipeline.is_pulling_pipeline()?);

        let (tx, rx) = async_channel::unbounded();

        build_res.main_pipeline.add_sink(|input_port| {
            Ok(ProcessorPtr::create(UnionReceiveSink::create(
                Some(tx.clone()),
                input_port,
                self.ctx.clone(),
            )))
        })?;

        self.pipelines.push(build_res.main_pipeline.finalize());
        self.pipelines.extend(build_res.sources_pipelines);
        Ok(rx)
    }
}
