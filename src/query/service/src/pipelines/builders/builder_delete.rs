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

use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_pipeline_sources::EmptySource;
use databend_common_sql::evaluator::CompoundBlockOperator;
use databend_common_sql::executor::physical_plans::DeleteSource;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::gen_mutation_stream_operator;
use databend_common_storages_fuse::operations::TransformSerializeBlock;
use databend_common_storages_fuse::FuseTable;

use crate::pipelines::processors::TransformAddStreamColumns;
use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    /// The flow of Pipeline is as follows:
    ///
    /// +---------------+      +-----------------------+
    /// |MutationSource1| ---> |SerializeDataTransform1|
    /// +---------------+      +-----------------------+
    /// |     ...       | ---> |          ...          |
    /// +---------------+      +-----------------------+
    /// |MutationSourceN| ---> |SerializeDataTransformN|
    /// +---------------+      +-----------------------+
    pub(crate) fn build_delete_source(&mut self, delete: &DeleteSource) -> Result<()> {
        let table =
            self.ctx
                .build_table_by_table_info(&delete.catalog_info, &delete.table_info, None)?;
        let table = FuseTable::try_from_table(table.as_ref())?;

        if delete.parts.is_empty() {
            return self.main_pipeline.add_source(EmptySource::create, 1);
        }
        table.add_deletion_source(
            self.ctx.clone(),
            &delete.filters.filter,
            delete.col_indices.clone(),
            delete.query_row_id_col,
            &mut self.main_pipeline,
        )?;
        if table.change_tracking_enabled() {
            let func_ctx = self.ctx.get_function_context()?;
            let (stream, operators) = gen_mutation_stream_operator(
                table.schema_with_stream(),
                table.get_table_info().ident.seq,
            )?;
            self.main_pipeline
                .add_transform(|transform_input_port, transform_output_port| {
                    TransformAddStreamColumns::try_create(
                        transform_input_port,
                        transform_output_port,
                        CompoundBlockOperator {
                            operators: operators.clone(),
                            ctx: func_ctx.clone(),
                        },
                        stream.clone(),
                    )
                })?;
        }
        let cluster_stats_gen =
            table.get_cluster_stats_gen(self.ctx.clone(), 0, table.get_block_thresholds(), None)?;
        self.main_pipeline.add_transform(|input, output| {
            let proc = TransformSerializeBlock::try_create(
                self.ctx.clone(),
                input,
                output,
                table,
                cluster_stats_gen.clone(),
                MutationKind::Delete,
            )?;
            proc.into_processor()
        })?;
        let ctx: Arc<dyn TableContext> = self.ctx.clone();
        if delete.parts.is_lazy {
            table.chain_mutation_aggregator(
                &ctx,
                &mut self.main_pipeline,
                delete.snapshot.clone(),
                MutationKind::Delete,
            )?;
        }
        Ok(())
    }
}
