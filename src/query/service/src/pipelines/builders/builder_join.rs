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

use std::sync::Arc;

use databend_common_base::base::tokio::sync::Barrier;
use databend_common_exception::Result;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sinks::Sinker;
use databend_common_sql::executor::physical_plans::HashJoin;
use databend_common_sql::executor::physical_plans::MaterializedCte;
use databend_common_sql::executor::physical_plans::RangeJoin;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_sql::ColumnBinding;
use databend_common_sql::IndexType;

use crate::pipelines::processors::transforms::range_join::RangeJoinState;
use crate::pipelines::processors::transforms::range_join::TransformRangeJoinLeft;
use crate::pipelines::processors::transforms::range_join::TransformRangeJoinRight;
use crate::pipelines::processors::transforms::HashJoinBuildState;
use crate::pipelines::processors::transforms::HashJoinProbeState;
use crate::pipelines::processors::transforms::MaterializedCteSink;
use crate::pipelines::processors::transforms::MaterializedCteState;
use crate::pipelines::processors::transforms::TransformHashJoinBuild;
use crate::pipelines::processors::transforms::TransformHashJoinProbe;
use crate::pipelines::processors::HashJoinDesc;
use crate::pipelines::processors::HashJoinState;
use crate::pipelines::PipelineBuilder;
use crate::sessions::QueryContext;

impl PipelineBuilder {
    pub(crate) fn build_range_join(&mut self, range_join: &RangeJoin) -> Result<()> {
        let state = Arc::new(RangeJoinState::new(self.ctx.clone(), range_join));
        self.expand_right_side_pipeline(range_join, state.clone())?;
        self.build_left_side(range_join, state)?;
        Ok(())
    }

    fn build_left_side(
        &mut self,
        range_join: &RangeJoin,
        state: Arc<RangeJoinState>,
    ) -> Result<()> {
        self.build_pipeline(&range_join.left)?;
        let max_threads = self.settings.get_max_threads()? as usize;
        self.main_pipeline.try_resize(max_threads)?;
        self.main_pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(TransformRangeJoinLeft::create(
                input,
                output,
                state.clone(),
            )))
        })?;
        Ok(())
    }

    fn expand_right_side_pipeline(
        &mut self,
        range_join: &RangeJoin,
        state: Arc<RangeJoinState>,
    ) -> Result<()> {
        let right_side_context = QueryContext::create_from(self.ctx.clone());
        let mut right_side_builder = PipelineBuilder::create(
            self.func_ctx.clone(),
            self.settings.clone(),
            right_side_context,
            self.main_pipeline.get_scopes(),
        );
        right_side_builder.cte_state = self.cte_state.clone();
        right_side_builder.hash_join_states = self.hash_join_states.clone();

        let mut right_res = right_side_builder.finalize(&range_join.right)?;
        right_res.main_pipeline.add_sink(|input| {
            Ok(ProcessorPtr::create(
                Sinker::<TransformRangeJoinRight>::create(
                    input,
                    TransformRangeJoinRight::create(state.clone()),
                ),
            ))
        })?;
        self.pipelines.push(right_res.main_pipeline.finalize());
        self.pipelines.extend(right_res.sources_pipelines);
        Ok(())
    }

    pub(crate) fn build_join(&mut self, join: &HashJoin) -> Result<()> {
        // for merge into target table as build side.
        let (enable_merge_into_optimization, merge_into_is_distributed) =
            self.merge_into_get_optimization_flag(join);

        let state = self.build_join_state(
            join,
            merge_into_is_distributed,
            enable_merge_into_optimization,
        )?;
        if let Some((build_cache_index, _)) = join.build_side_cache_info {
            self.hash_join_states
                .insert(build_cache_index, state.clone());
        }
        self.expand_build_side_pipeline(&join.build, join, state.clone())?;
        self.build_join_probe(join, state)
    }

    fn build_join_state(
        &mut self,
        join: &HashJoin,
        merge_into_is_distributed: bool,
        enable_merge_into_optimization: bool,
    ) -> Result<Arc<HashJoinState>> {
        HashJoinState::try_create(
            self.ctx.clone(),
            join.build.output_schema()?,
            &join.build_projections,
            HashJoinDesc::create(join)?,
            &join.probe_to_build,
            merge_into_is_distributed,
            enable_merge_into_optimization,
            join.build_side_cache_info.clone(),
        )
    }

    fn expand_build_side_pipeline(
        &mut self,
        build: &PhysicalPlan,
        hash_join_plan: &HashJoin,
        join_state: Arc<HashJoinState>,
    ) -> Result<()> {
        let build_side_context = QueryContext::create_from(self.ctx.clone());
        let mut build_side_builder = PipelineBuilder::create(
            self.func_ctx.clone(),
            self.settings.clone(),
            build_side_context,
            self.main_pipeline.get_scopes(),
        );
        build_side_builder.cte_state = self.cte_state.clone();
        build_side_builder.hash_join_states = self.hash_join_states.clone();
        let mut build_res = build_side_builder.finalize(build)?;

        assert!(build_res.main_pipeline.is_pulling_pipeline()?);
        let output_len = build_res.main_pipeline.output_len();
        let build_state = HashJoinBuildState::try_create(
            self.ctx.clone(),
            self.func_ctx.clone(),
            &hash_join_plan.build_keys,
            &hash_join_plan.build_projections,
            join_state.clone(),
            output_len,
        )?;

        let create_sink_processor = |input| {
            Ok(ProcessorPtr::create(TransformHashJoinBuild::try_create(
                input,
                build_state.clone(),
            )?))
        };
        // for distributed merge into when source as build side.
        if hash_join_plan.need_hold_hash_table {
            self.join_state = Some(build_state.clone())
        }
        build_res.main_pipeline.add_sink(create_sink_processor)?;

        self.pipelines.push(build_res.main_pipeline.finalize());
        self.pipelines.extend(build_res.sources_pipelines);
        Ok(())
    }

    fn build_join_probe(&mut self, join: &HashJoin, state: Arc<HashJoinState>) -> Result<()> {
        self.build_pipeline(&join.probe)?;

        let max_block_size = self.settings.get_max_block_size()? as usize;
        let barrier = Barrier::new(self.main_pipeline.output_len());
        let probe_state = Arc::new(HashJoinProbeState::create(
            self.ctx.clone(),
            self.func_ctx.clone(),
            state.clone(),
            &join.probe_projections,
            &join.probe_keys,
            join.probe.output_schema()?,
            &join.join_type,
            self.main_pipeline.output_len(),
            barrier,
        )?);
        let mut has_string_column = false;
        for filed in join.output_schema()?.fields() {
            has_string_column |= filed.data_type().is_string_column();
        }

        self.main_pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(TransformHashJoinProbe::create(
                input,
                output,
                join.projections.clone(),
                probe_state.clone(),
                max_block_size,
                self.func_ctx.clone(),
                &join.join_type,
                !join.non_equi_conditions.is_empty(),
                has_string_column,
            )?))
        })?;

        if join.need_hold_hash_table {
            let mut projected_probe_fields = vec![];
            for (i, field) in probe_state.probe_schema.fields().iter().enumerate() {
                if probe_state.probe_projections.contains(&i) {
                    projected_probe_fields.push(field.clone());
                }
            }
            self.merge_into_probe_data_fields = Some(projected_probe_fields);
        }

        Ok(())
    }

    pub(crate) fn build_materialized_cte(
        &mut self,
        materialized_cte: &MaterializedCte,
    ) -> Result<()> {
        self.expand_left_side_pipeline(
            &materialized_cte.left,
            materialized_cte.cte_idx,
            &materialized_cte.left_output_columns,
        )?;
        self.build_pipeline(&materialized_cte.right)
    }

    fn expand_left_side_pipeline(
        &mut self,
        left_side: &PhysicalPlan,
        cte_idx: IndexType,
        left_output_columns: &[ColumnBinding],
    ) -> Result<()> {
        let left_side_ctx = QueryContext::create_from(self.ctx.clone());
        let state = Arc::new(MaterializedCteState::new(self.ctx.clone()));
        self.cte_state.insert(cte_idx, state.clone());
        let mut left_side_builder = PipelineBuilder::create(
            self.func_ctx.clone(),
            self.settings.clone(),
            left_side_ctx,
            self.main_pipeline.get_scopes(),
        );
        left_side_builder.cte_state = self.cte_state.clone();
        left_side_builder.hash_join_states = self.hash_join_states.clone();
        let mut left_side_pipeline = left_side_builder.finalize(left_side)?;
        assert!(left_side_pipeline.main_pipeline.is_pulling_pipeline()?);

        PipelineBuilder::build_result_projection(
            &self.func_ctx,
            left_side.output_schema()?,
            left_output_columns,
            &mut left_side_pipeline.main_pipeline,
            false,
        )?;

        left_side_pipeline.main_pipeline.add_sink(|input| {
            let transform = Sinker::<MaterializedCteSink>::create(
                input,
                MaterializedCteSink::create(self.ctx.clone(), cte_idx, state.clone())?,
            );
            Ok(ProcessorPtr::create(transform))
        })?;
        self.pipelines
            .push(left_side_pipeline.main_pipeline.finalize());
        self.pipelines.extend(left_side_pipeline.sources_pipelines);
        Ok(())
    }
}
