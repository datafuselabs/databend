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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::filter::SelectExprBuilder;
use databend_common_expression::type_check::check_function;
use databend_common_expression::types::DataType;
use databend_common_expression::BlockThresholds;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::RemoteExpr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_transforms::processors::AsyncAccumulatingTransformer;
use databend_common_pipeline_transforms::processors::BlockCompactor;
use databend_common_pipeline_transforms::processors::TransformCompact;
use databend_common_pipeline_transforms::processors::TransformDummy;
use databend_common_sql::evaluator::BlockOperator;
use databend_common_sql::evaluator::CompoundBlockOperator;
use databend_common_sql::ColumnSet;
use databend_common_storages_factory::Table;
use databend_common_storages_fuse::operations::TableMutationAggregator;
use databend_common_storages_fuse::operations::TransformSerializeBlock;
use databend_common_storages_fuse::operations::TransformSerializeSegment;
use databend_common_storages_fuse::statistics::ClusterStatsGenerator;
use databend_common_storages_fuse::FuseTable;
use databend_storages_common_table_meta::meta;
use databend_storages_common_table_meta::meta::Statistics;
use meta::TableMetaTimestamps;

use crate::pipelines::processors::transforms::TransformFilter;
use crate::pipelines::processors::InputPort;
use crate::pipelines::processors::OutputPort;
use crate::pipelines::processors::TransformCastSchema;
use crate::pipelines::processors::TransformResortAddOn;
use crate::pipelines::PipelineBuilder;
use crate::sql::executor::physical_plans::MutationKind;
impl PipelineBuilder {
    pub(crate) fn filter_transform_builder(
        &self,
        predicates: &[RemoteExpr],
        projections: HashSet<usize>,
    ) -> Result<impl Fn(Arc<InputPort>, Arc<OutputPort>) -> Result<ProcessorPtr>> {
        let predicate = predicates
            .iter()
            .map(|expr| expr.as_expr(&BUILTIN_FUNCTIONS))
            .try_reduce(|lhs, rhs| {
                check_function(None, "and_filters", &[], &[lhs, rhs], &BUILTIN_FUNCTIONS)
            })
            .transpose()
            .unwrap_or_else(|| {
                Err(ErrorCode::Internal(
                    "Invalid empty predicate list".to_string(),
                ))
            })?;
        assert_eq!(predicate.data_type(), &DataType::Boolean);

        let max_block_size = self.settings.get_max_block_size()? as usize;
        let (select_expr, has_or) = SelectExprBuilder::new().build(&predicate).into();
        let fun_ctx = self.func_ctx.clone();
        Ok(move |input, output| {
            Ok(ProcessorPtr::create(TransformFilter::create(
                input,
                output,
                select_expr.clone(),
                has_or,
                projections.clone(),
                fun_ctx.clone(),
                max_block_size,
            )))
        })
    }

    pub(crate) fn dummy_transform_builder(
        &self,
    ) -> Result<impl Fn(Arc<InputPort>, Arc<OutputPort>) -> Result<ProcessorPtr>> {
        Ok(|input, output| Ok(TransformDummy::create(input, output)))
    }

    pub(crate) fn block_compact_transform_builder(
        &self,
        block_thresholds: BlockThresholds,
    ) -> Result<impl Fn(Arc<InputPort>, Arc<OutputPort>) -> Result<ProcessorPtr>> {
        Ok(move |transform_input_port, transform_output_port| {
            Ok(ProcessorPtr::create(TransformCompact::try_create(
                transform_input_port,
                transform_output_port,
                BlockCompactor::new(block_thresholds),
            )?))
        })
    }

    pub(crate) fn with_tid_serialize_block_transform_builder(
        &self,
        table: Arc<dyn Table>,
        cluster_stats_gen: ClusterStatsGenerator,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<impl Fn(Arc<InputPort>, Arc<OutputPort>) -> Result<ProcessorPtr>> {
        let ctx = self.ctx.clone();
        Ok(move |input, output| {
            let fuse_table = FuseTable::try_from_table(table.as_ref())?;
            let proc = TransformSerializeBlock::try_create_with_tid(
                ctx.clone(),
                input,
                output,
                fuse_table,
                cluster_stats_gen.clone(),
                MutationKind::Insert,
                table_meta_timestamps,
            )?;
            proc.into_processor()
        })
    }

    pub(crate) fn serialize_segment_transform_builder(
        &self,
        table: Arc<dyn Table>,
        block_thresholds: BlockThresholds,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<impl Fn(Arc<InputPort>, Arc<OutputPort>) -> Result<ProcessorPtr>> {
        Ok(move |input, output| {
            let fuse_table = FuseTable::try_from_table(table.as_ref())?;
            let proc = TransformSerializeSegment::new(
                input,
                output,
                fuse_table,
                block_thresholds,
                table_meta_timestamps,
            );
            proc.into_processor()
        })
    }

    pub(crate) fn mutation_aggregator_transform_builder(
        &self,
        table: Arc<dyn Table>,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<impl Fn(Arc<InputPort>, Arc<OutputPort>) -> Result<ProcessorPtr>> {
        let ctx = self.ctx.clone();
        Ok(move |input, output| {
            let fuse_table = FuseTable::try_from_table(table.as_ref())?;
            let aggregator = TableMutationAggregator::create(
                fuse_table,
                ctx.clone(),
                vec![],
                vec![],
                vec![],
                Statistics::default(),
                MutationKind::Insert,
                table_meta_timestamps,
            );
            Ok(ProcessorPtr::create(AsyncAccumulatingTransformer::create(
                input, output, aggregator,
            )))
        })
    }

    pub(crate) fn map_transform_builder(
        &self,
        num_input_columns: usize,
        remote_exprs: Vec<RemoteExpr>,
        projections: Option<ColumnSet>,
    ) -> Result<impl Fn(Arc<InputPort>, Arc<OutputPort>) -> Result<ProcessorPtr>> {
        let func_ctx = self.func_ctx.clone();
        let exprs = remote_exprs
            .iter()
            .map(|scalar| scalar.as_expr(&BUILTIN_FUNCTIONS))
            .collect::<Vec<_>>();
        Ok(move |input, output| {
            Ok(ProcessorPtr::create(CompoundBlockOperator::create(
                input,
                output,
                num_input_columns,
                func_ctx.clone(),
                vec![BlockOperator::Map {
                    exprs: exprs.clone(),
                    projections: projections.clone(),
                }],
            )))
        })
    }

    pub(crate) fn cast_schema_transform_builder(
        &self,
        source_schema: DataSchemaRef,
        target_schema: DataSchemaRef,
    ) -> Result<impl Fn(Arc<InputPort>, Arc<OutputPort>) -> Result<ProcessorPtr>> {
        let func_ctx = self.func_ctx.clone();
        Ok(move |transform_input_port, transform_output_port| {
            TransformCastSchema::try_create(
                transform_input_port,
                transform_output_port,
                source_schema.clone(),
                target_schema.clone(),
                func_ctx.clone(),
            )
        })
    }

    pub(crate) fn fill_and_reorder_transform_builder(
        &self,
        table: Arc<dyn Table>,
        source_schema: DataSchemaRef,
    ) -> Result<impl Fn(Arc<InputPort>, Arc<OutputPort>) -> Result<ProcessorPtr>> {
        let ctx = self.ctx.clone();
        Ok(move |transform_input_port, transform_output_port| {
            TransformResortAddOn::try_create(
                ctx.clone(),
                transform_input_port,
                transform_output_port,
                source_schema.clone(),
                Arc::new(table.schema().into()),
                table.clone(),
            )
        })
    }
}
