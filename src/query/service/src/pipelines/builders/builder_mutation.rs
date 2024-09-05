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

use databend_common_base::base::tokio::sync::Semaphore;
use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipe;
use databend_common_pipeline_transforms::processors::create_dummy_item;
use databend_common_pipeline_transforms::processors::BlockCompactor;
use databend_common_pipeline_transforms::processors::TransformCompact;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_sql::binder::MutationStrategy;
use databend_common_sql::executor::physical_plans::Mutation;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_storages_fuse::operations::TransformSerializeBlock;
use databend_common_storages_fuse::operations::TransformSerializeSegment;
use databend_common_storages_fuse::operations::UnMatchedExprs;
use databend_common_storages_fuse::FuseTable;

use crate::pipelines::processors::transforms::TransformAddComputedColumns;
use crate::pipelines::processors::TransformResortAddOnWithoutSourceSchema;
use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    // build mutation serialize and mutation pipeline
    pub(crate) fn build_mutation(&mut self, merge_into: &Mutation) -> Result<()> {
        self.build_pipeline(&merge_into.input)?;

        let tbl = self
            .ctx
            .build_table_by_table_info(&merge_into.table_info, None)?;

        let table = FuseTable::try_from_table(tbl.as_ref())?;
        let block_thresholds = table.get_block_thresholds();

        let cluster_stats_gen =
            table.get_cluster_stats_gen(self.ctx.clone(), 0, block_thresholds, None)?;

        let io_request_semaphore =
            Arc::new(Semaphore::new(self.settings.get_max_threads()? as usize));

        let serialize_segment_transform = TransformSerializeSegment::new(
            InputPort::create(),
            OutputPort::create(),
            table,
            block_thresholds,
            merge_into.table_meta_timestamps,
        );

        // For row_id port, create rowid_aggregate_mutator
        // For matched data port and unmatched port, do serialize
        let serialize_len = match merge_into.strategy {
            MutationStrategy::NotMatchedOnly => self.main_pipeline.output_len(),
            MutationStrategy::MixedMatched | MutationStrategy::MatchedOnly => {
                // remove row id port
                self.main_pipeline.output_len() - 1
            }
            MutationStrategy::Direct => unreachable!(),
        };

        // 1. Fill default and computed columns
        self.build_fill_columns_in_merge_into(
            tbl.clone(),
            serialize_len,
            merge_into.need_match,
            merge_into.unmatched.clone(),
        )?;

        // 2. Add cluster‘s blocksort if it's a cluster table
        self.build_compact_and_cluster_sort_in_merge_into(
            table,
            merge_into.need_match,
            serialize_len,
            block_thresholds,
        )?;

        let mut pipe_items = Vec::with_capacity(self.main_pipeline.output_len());

        // 3.1 Add rowid_aggregate_mutator for row_id port
        if merge_into.need_match {
            pipe_items.push(table.rowid_aggregate_mutator(
                self.ctx.clone(),
                cluster_stats_gen.clone(),
                io_request_semaphore,
                merge_into.segments.clone(),
                false,
                merge_into.table_meta_timestamps,
            )?);
        }

        // 3.2 Add serialize_block_transform for data port
        for _ in 0..serialize_len {
            let serialize_block_transform = TransformSerializeBlock::try_create(
                self.ctx.clone(),
                InputPort::create(),
                OutputPort::create(),
                table,
                cluster_stats_gen.clone(),
                MutationKind::MergeInto,
                merge_into.table_meta_timestamps,
            )?;
            pipe_items.push(serialize_block_transform.into_pipe_item());
        }

        let output_len = pipe_items.iter().map(|item| item.outputs_port.len()).sum();
        self.main_pipeline.add_pipe(Pipe::create(
            self.main_pipeline.output_len(),
            output_len,
            pipe_items,
        ));

        // The complete pipeline:
        // aggregate_mutator port               aggregate_mutator port
        // serialize_block port0
        // serialize_block port1     ======>    serialize_block port
        // .......
        let mut ranges = Vec::with_capacity(self.main_pipeline.output_len());
        // row id port
        let row_id_offset = if merge_into.need_match {
            ranges.push(vec![0]);
            1
        } else {
            0
        };

        // Resize data ports
        debug_assert!(serialize_len > 0);
        let mut vec = Vec::with_capacity(self.main_pipeline.output_len());
        for idx in 0..serialize_len {
            vec.push(idx + row_id_offset);
        }
        ranges.push(vec);
        self.main_pipeline.resize_partial_one(ranges)?;

        let pipe_items = {
            let mut vec = Vec::with_capacity(2);
            if merge_into.need_match {
                // row_id port
                vec.push(create_dummy_item());
            }
            // data port
            vec.push(serialize_segment_transform.into_pipe_item());
            vec
        };

        // The complete pipeline:
        // output_port0: MutationLogs(row_id)
        // output_port1: MutationLogs(data)
        // 1. FullOperation and MatchedOnly: same as above
        // 2. InsertOnly: no output_port0
        let output_len = pipe_items.iter().map(|item| item.outputs_port.len()).sum();
        self.main_pipeline.add_pipe(Pipe::create(
            self.main_pipeline.output_len(),
            output_len,
            pipe_items,
        ));
        Ok(())
    }

    pub fn build_fill_columns_in_merge_into(
        &mut self,
        tbl: Arc<dyn Table>,
        transform_len: usize,
        need_match: bool,
        unmatched: UnMatchedExprs,
    ) -> Result<()> {
        let table = FuseTable::try_from_table(tbl.as_ref())?;

        // fill default columns
        let table_default_schema = &table.schema_with_stream().remove_computed_fields();
        let mut builder = self
            .main_pipeline
            .try_create_transform_pipeline_builder_with_len(
                || {
                    TransformResortAddOnWithoutSourceSchema::try_new(
                        self.ctx.clone(),
                        Arc::new(DataSchema::from(table_default_schema)),
                        unmatched.clone(),
                        tbl.clone(),
                        Arc::new(DataSchema::from(table.schema_with_stream())),
                    )
                },
                transform_len,
            )?;
        if need_match {
            builder.add_items_prepend(vec![create_dummy_item()]);
        }
        self.main_pipeline.add_pipe(builder.finalize());

        // fill computed columns
        let table_computed_schema = &table.schema_with_stream().remove_virtual_computed_fields();
        let default_schema: DataSchemaRef = Arc::new(table_default_schema.into());
        let computed_schema: DataSchemaRef = Arc::new(table_computed_schema.into());
        if default_schema != computed_schema {
            builder = self
                .main_pipeline
                .try_create_transform_pipeline_builder_with_len(
                    || {
                        TransformAddComputedColumns::try_new(
                            self.ctx.clone(),
                            default_schema.clone(),
                            computed_schema.clone(),
                        )
                    },
                    transform_len,
                )?;
            if need_match {
                builder.add_items_prepend(vec![create_dummy_item()]);
            }
            self.main_pipeline.add_pipe(builder.finalize());
        }
        Ok(())
    }

    pub fn build_compact_and_cluster_sort_in_merge_into(
        &mut self,
        table: &FuseTable,
        need_match: bool,
        transform_len: usize,
        block_thresholds: BlockThresholds,
    ) -> Result<()> {
        // we should avoid too much little block write, because for s3 write, there are too many
        // little blocks, it will cause high latency.
        let mut builder = self.main_pipeline.add_transform_with_specified_len(
            |transform_input_port, transform_output_port| {
                Ok(ProcessorPtr::create(TransformCompact::try_create(
                    transform_input_port,
                    transform_output_port,
                    BlockCompactor::new(block_thresholds),
                )?))
            },
            transform_len,
        )?;
        if need_match {
            builder.add_items_prepend(vec![create_dummy_item()]);
        }
        self.main_pipeline.add_pipe(builder.finalize());

        // cluster sort
        table.cluster_gen_for_append_with_specified_len(
            self.ctx.clone(),
            &mut self.main_pipeline,
            block_thresholds,
            transform_len,
            need_match,
        )?;
        Ok(())
    }
}
