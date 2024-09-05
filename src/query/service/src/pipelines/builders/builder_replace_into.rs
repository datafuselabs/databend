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

use databend_common_ast::parser::parse_comma_separated_exprs;
use databend_common_ast::parser::tokenize_sql;
use databend_common_base::base::tokio::sync::Semaphore;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Scalar;
use databend_common_formats::FastFieldDecoderValues;
use databend_common_formats::FastValuesDecodeFallback;
use databend_common_formats::FastValuesDecoder;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::Pipe;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;
use databend_common_pipeline_transforms::processors::create_dummy_item;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::executor::physical_plans::ReplaceAsyncSourcer;
use databend_common_sql::executor::physical_plans::ReplaceDeduplicate;
use databend_common_sql::executor::physical_plans::ReplaceInto;
use databend_common_sql::executor::physical_plans::ReplaceSelectCtx;
use databend_common_sql::plans::InsertValue;
use databend_common_sql::BindContext;
use databend_common_sql::Metadata;
use databend_common_sql::MetadataRef;
use databend_common_sql::NameResolutionContext;
use databend_common_storages_fuse::operations::BroadcastProcessor;
use databend_common_storages_fuse::operations::ReplaceIntoProcessor;
use databend_common_storages_fuse::operations::TransformSerializeBlock;
use databend_common_storages_fuse::operations::TransformSerializeSegment;
use databend_common_storages_fuse::operations::UnbranchedReplaceIntoProcessor;
use databend_common_storages_fuse::FuseTable;
use parking_lot::RwLock;

use crate::pipelines::processors::TransformCastSchema;
use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    // check if cast needed
    fn check_schema_cast(
        select_schema: Arc<DataSchema>,
        output_schema: Arc<DataSchema>,
    ) -> Result<bool> {
        let cast_needed = select_schema != output_schema;
        Ok(cast_needed)
    }

    // build async sourcer pipeline.
    pub(crate) fn build_async_sourcer(
        &mut self,
        async_sourcer: &ReplaceAsyncSourcer,
    ) -> Result<()> {
        self.main_pipeline.add_source(
            |output| {
                let name_resolution_ctx = NameResolutionContext::try_from(self.settings.as_ref())?;
                match &async_sourcer.source {
                    InsertValue::Values { rows } => {
                        let inner = ValueSource::new(rows.clone(), async_sourcer.schema.clone());
                        AsyncSourcer::create(self.ctx.clone(), output, inner)
                    }
                    InsertValue::RawValues { data, start } => {
                        let inner = RawValueSource::new(
                            data.clone(),
                            self.ctx.clone(),
                            name_resolution_ctx,
                            async_sourcer.schema.clone(),
                            *start,
                        );
                        AsyncSourcer::create(self.ctx.clone(), output, inner)
                    }
                }
            },
            1,
        )?;
        Ok(())
    }

    // build replace into pipeline.
    pub(crate) fn build_replace_into(&mut self, replace: &ReplaceInto) -> Result<()> {
        let ReplaceInto {
            input,
            block_thresholds,
            table_info,
            on_conflicts,
            bloom_filter_column_indexes,
            segments,
            block_slots,
            need_insert,
            ..
        } = replace;
        let max_threads = self.settings.get_max_threads()?;
        let segment_partition_num = std::cmp::min(segments.len(), max_threads as usize);
        let table = self.ctx.build_table_by_table_info(table_info, None)?;
        let table = FuseTable::try_from_table(table.as_ref())?;
        let schema = DataSchema::from(table.schema()).into();
        let cluster_stats_gen =
            table.get_cluster_stats_gen(self.ctx.clone(), 0, *block_thresholds, Some(schema))?;
        self.build_pipeline(input)?;
        // connect to broadcast processor and append transform
        let serialize_block_transform = TransformSerializeBlock::try_create(
            self.ctx.clone(),
            InputPort::create(),
            OutputPort::create(),
            table,
            cluster_stats_gen,
            MutationKind::Replace,
            replace.table_meta_timestamps,
        )?;
        let mut block_builder = serialize_block_transform.get_block_builder();
        block_builder.source_schema = table.schema_with_stream();

        let serialize_segment_transform = TransformSerializeSegment::new(
            InputPort::create(),
            OutputPort::create(),
            table,
            *block_thresholds,
            replace.table_meta_timestamps,
        );
        if !*need_insert {
            if segment_partition_num == 0 {
                return Ok(());
            }
            let broadcast_processor = BroadcastProcessor::new(segment_partition_num);
            self.main_pipeline
                .add_pipe(Pipe::create(1, segment_partition_num, vec![
                    broadcast_processor.into_pipe_item(),
                ]));
            let max_threads = self.settings.get_max_threads()?;
            let io_request_semaphore = Arc::new(Semaphore::new(max_threads as usize));

            let merge_into_operation_aggregators = table.merge_into_mutators(
                self.ctx.clone(),
                segment_partition_num,
                block_builder,
                on_conflicts.clone(),
                bloom_filter_column_indexes.clone(),
                segments,
                block_slots.clone(),
                io_request_semaphore,
            )?;
            self.main_pipeline.add_pipe(Pipe::create(
                segment_partition_num,
                segment_partition_num,
                merge_into_operation_aggregators,
            ));
            return Ok(());
        }

        // The Block Size and Rows is promised by DataSource by user.
        if segment_partition_num == 0 {
            let dummy_item = create_dummy_item();
            //                      ┌──────────────────────┐            ┌──────────────────┐
            //                      │                      ├──┬────────►│  SerializeBlock  │
            // ┌─────────────┐      │                      ├──┘         └──────────────────┘
            // │ UpsertSource├─────►│ ReplaceIntoProcessor │
            // └─────────────┘      │                      ├──┐         ┌──────────────────┐
            //                      │                      ├──┴────────►│  DummyTransform  │
            //                      └──────────────────────┘            └──────────────────┘
            // wrap them into pipeline, order matters!
            self.main_pipeline.add_pipe(Pipe::create(2, 2, vec![
                serialize_block_transform.into_pipe_item(),
                dummy_item,
            ]));
        } else {
            //                      ┌──────────────────────┐            ┌──────────────────┐
            //                      │                      ├──┬────────►│ SerializeBlock   │
            // ┌─────────────┐      │                      ├──┘         └──────────────────┘
            // │ UpsertSource├─────►│ ReplaceIntoProcessor │
            // └─────────────┘      │                      ├──┐         ┌──────────────────┐
            //                      │                      ├──┴────────►│BroadcastProcessor│
            //                      └──────────────────────┘            └──────────────────┘
            let broadcast_processor = BroadcastProcessor::new(segment_partition_num);
            // wrap them into pipeline, order matters!
            self.main_pipeline
                .add_pipe(Pipe::create(2, segment_partition_num + 1, vec![
                    serialize_block_transform.into_pipe_item(),
                    broadcast_processor.into_pipe_item(),
                ]));
        };

        // 4. connect with MergeIntoOperationAggregators
        if segment_partition_num == 0 {
            let dummy_item = create_dummy_item();
            self.main_pipeline.add_pipe(Pipe::create(2, 2, vec![
                serialize_segment_transform.into_pipe_item(),
                dummy_item,
            ]));
        } else {
            //      ┌──────────────────┐               ┌────────────────┐
            // ────►│  SerializeBlock  ├──────────────►│SerializeSegment│
            //      └──────────────────┘               └────────────────┘
            //
            //      ┌───────────────────┐              ┌──────────────────────┐
            // ────►│                   ├──┬──────────►│MergeIntoOperationAggr│
            //      │                   ├──┘           └──────────────────────┘
            //      │ BroadcastProcessor│
            //      │                   ├──┐           ┌──────────────────────┐
            //      │                   ├──┴──────────►│MergeIntoOperationAggr│
            //      │                   │              └──────────────────────┘
            //      │                   ├──┐
            //      │                   ├──┴──────────►┌──────────────────────┐
            //      └───────────────────┘              │MergeIntoOperationAggr│
            //                                         └──────────────────────┘

            let item_size = segment_partition_num + 1;
            let mut pipe_items = Vec::with_capacity(item_size);
            // setup the dummy transform
            pipe_items.push(serialize_segment_transform.into_pipe_item());

            let max_threads = self.settings.get_max_threads()?;
            let io_request_semaphore = Arc::new(Semaphore::new(max_threads as usize));

            // setup the merge into operation aggregators
            let mut merge_into_operation_aggregators = table.merge_into_mutators(
                self.ctx.clone(),
                segment_partition_num,
                block_builder,
                on_conflicts.clone(),
                bloom_filter_column_indexes.clone(),
                segments,
                block_slots.clone(),
                io_request_semaphore,
            )?;
            assert_eq!(
                segment_partition_num,
                merge_into_operation_aggregators.len()
            );
            pipe_items.append(&mut merge_into_operation_aggregators);

            // extend the pipeline
            assert_eq!(self.main_pipeline.output_len(), item_size);
            assert_eq!(pipe_items.len(), item_size);
            self.main_pipeline
                .add_pipe(Pipe::create(item_size, item_size, pipe_items));
        }
        Ok(())
    }

    // build deduplicate pipeline.
    pub(crate) fn build_deduplicate(&mut self, deduplicate: &ReplaceDeduplicate) -> Result<()> {
        let ReplaceDeduplicate {
            input,
            on_conflicts,
            bloom_filter_column_indexes,
            table_is_empty,
            table_info,
            select_ctx,
            table_level_range_index,
            target_schema,
            need_insert,
            delete_when,
            ..
        } = deduplicate;

        let tbl = self.ctx.build_table_by_table_info(table_info, None)?;
        let table = FuseTable::try_from_table(tbl.as_ref())?;
        self.build_pipeline(input)?;
        let mut delete_column_idx = 0;
        let mut modified_schema = DataSchema::from(target_schema.clone()).into();
        if let Some(ReplaceSelectCtx {
            select_column_bindings,
            select_schema,
        }) = select_ctx
        {
            PipelineBuilder::build_result_projection(
                &self.func_ctx,
                input.output_schema()?,
                select_column_bindings,
                &mut self.main_pipeline,
                false,
            )?;

            let mut target_schema: DataSchema = target_schema.clone().into();
            if let Some((_, delete_column)) = delete_when {
                delete_column_idx = select_schema.index_of(delete_column.as_str())?;
                let delete_column = select_schema.field(delete_column_idx).clone();
                target_schema
                    .fields
                    .insert(delete_column_idx, delete_column);
                modified_schema = Arc::new(target_schema.clone());
            }
            let target_schema = Arc::new(target_schema.clone());
            if target_schema.fields().len() != select_schema.fields().len() {
                return Err(ErrorCode::BadArguments(
                    "The number of columns in the target table is different from the number of columns in the SELECT clause",
                ));
            }
            if Self::check_schema_cast(select_schema.clone(), target_schema.clone())? {
                self.main_pipeline.try_add_transformer(|| {
                    TransformCastSchema::try_new(
                        select_schema.clone(),
                        target_schema.clone(),
                        self.func_ctx.clone(),
                    )
                })?;
            }
        }

        Self::fill_and_reorder_columns(
            self.ctx.clone(),
            &mut self.main_pipeline,
            tbl.clone(),
            Arc::new(target_schema.clone().into()),
        )?;

        let _ = table.cluster_gen_for_append(
            self.ctx.clone(),
            &mut self.main_pipeline,
            table.get_block_thresholds(),
            Some(modified_schema),
        )?;
        // 1. resize input to 1, since the UpsertTransform need to de-duplicate inputs "globally"
        self.main_pipeline.try_resize(1)?;

        // 2. connect with ReplaceIntoProcessor

        //                      ┌──────────────────────┐
        //                      │                      ├──┐
        // ┌─────────────┐      │                      ├──┘
        // │ UpsertSource├─────►│ ReplaceIntoProcessor │
        // └─────────────┘      │                      ├──┐
        //                      │                      ├──┘
        //                      └──────────────────────┘
        // NOTE: here the pipe items of last pipe are arranged in the following order
        // (0) -> output_port_append_data
        // (1) -> output_port_merge_into_action
        //    the "downstream" is supposed to be connected with a processor which can process MergeIntoOperations
        //    in our case, it is the broadcast processor
        let delete_when = if let Some((remote_expr, delete_column)) = delete_when {
            Some((
                remote_expr.as_expr(&BUILTIN_FUNCTIONS),
                delete_column.clone(),
            ))
        } else {
            None
        };
        let cluster_keys = table.cluster_keys(self.ctx.clone());
        if *need_insert {
            let replace_into_processor = ReplaceIntoProcessor::create(
                self.ctx.clone(),
                on_conflicts.clone(),
                cluster_keys,
                bloom_filter_column_indexes.clone(),
                &table.schema(),
                *table_is_empty,
                table_level_range_index.clone(),
                delete_when.map(|(expr, _)| (expr, delete_column_idx)),
            )?;
            self.main_pipeline
                .add_pipe(replace_into_processor.into_pipe());
        } else {
            let replace_into_processor = UnbranchedReplaceIntoProcessor::create(
                self.ctx.as_ref(),
                on_conflicts.clone(),
                cluster_keys,
                bloom_filter_column_indexes.clone(),
                &table.schema(),
                *table_is_empty,
                table_level_range_index.clone(),
                delete_when.map(|_| delete_column_idx),
            )?;
            self.main_pipeline
                .add_pipe(replace_into_processor.into_pipe());
        }
        Ok(())
    }
}

pub struct ValueSource {
    rows: Arc<Vec<Vec<Scalar>>>,
    schema: DataSchemaRef,
    is_finished: bool,
}

impl ValueSource {
    pub fn new(rows: Vec<Vec<Scalar>>, schema: DataSchemaRef) -> Self {
        Self {
            rows: Arc::new(rows),
            schema,
            is_finished: false,
        }
    }
}

#[async_trait::async_trait]
impl AsyncSource for ValueSource {
    const NAME: &'static str = "ValueSource";
    const SKIP_EMPTY_DATA_BLOCK: bool = true;

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.is_finished {
            return Ok(None);
        }

        let mut columns = self
            .schema
            .fields()
            .iter()
            .map(|f| ColumnBuilder::with_capacity(f.data_type(), self.rows.len()))
            .collect::<Vec<_>>();

        for row in self.rows.as_ref() {
            for (field, column) in row.iter().zip(columns.iter_mut()) {
                column.push(field.as_ref());
            }
        }

        let columns = columns
            .into_iter()
            .map(|col| col.build())
            .collect::<Vec<_>>();
        let block = DataBlock::new_from_columns(columns);
        self.is_finished = true;
        Ok(Some(block))
    }
}

pub struct RawValueSource {
    data: String,
    ctx: Arc<dyn TableContext>,
    name_resolution_ctx: NameResolutionContext,
    bind_context: BindContext,
    schema: DataSchemaRef,
    metadata: MetadataRef,
    start: usize,
    is_finished: bool,
}

impl RawValueSource {
    pub fn new(
        data: String,
        ctx: Arc<dyn TableContext>,
        name_resolution_ctx: NameResolutionContext,
        schema: DataSchemaRef,
        start: usize,
    ) -> Self {
        let bind_context = BindContext::new();
        let metadata = Arc::new(RwLock::new(Metadata::default()));

        Self {
            data,
            ctx,
            name_resolution_ctx,
            schema,
            bind_context,
            metadata,
            start,
            is_finished: false,
        }
    }
}

#[async_trait::async_trait]
impl AsyncSource for RawValueSource {
    const NAME: &'static str = "RawValueSource";
    const SKIP_EMPTY_DATA_BLOCK: bool = true;

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.is_finished {
            return Ok(None);
        }

        let format = self.ctx.get_format_settings()?;
        let numeric_cast_option = self
            .ctx
            .get_settings()
            .get_numeric_cast_option()
            .unwrap_or("rounding".to_string());
        let rounding_mode = numeric_cast_option.as_str() == "rounding";
        let field_decoder = FastFieldDecoderValues::create_for_insert(format, rounding_mode);

        let mut values_decoder = FastValuesDecoder::new(&self.data, &field_decoder);
        let estimated_rows = values_decoder.estimated_rows();

        let mut columns = self
            .schema
            .fields()
            .iter()
            .map(|f| ColumnBuilder::with_capacity(f.data_type(), estimated_rows))
            .collect::<Vec<_>>();

        values_decoder.parse(&mut columns, self).await?;

        let columns = columns
            .into_iter()
            .map(|col| col.build())
            .collect::<Vec<_>>();
        let block = DataBlock::new_from_columns(columns);
        self.is_finished = true;
        Ok(Some(block))
    }
}

#[async_trait::async_trait]
impl FastValuesDecodeFallback for RawValueSource {
    async fn parse_fallback(&self, sql: &str) -> Result<Vec<Scalar>> {
        let res: Result<Vec<Scalar>> = try {
            let settings = self.ctx.get_settings();
            let sql_dialect = settings.get_sql_dialect()?;
            let tokens = tokenize_sql(sql)?;
            let mut bind_context = self.bind_context.clone();
            let metadata = self.metadata.clone();

            let exprs = parse_comma_separated_exprs(&tokens[1..tokens.len()], sql_dialect)?;

            bind_context
                .exprs_to_scalar(
                    &exprs,
                    &self.schema,
                    self.ctx.clone(),
                    &self.name_resolution_ctx,
                    metadata,
                )
                .await?
        };
        res.map_err(|mut err| {
            // The input for ValueSource is a sub-section of the original SQL. This causes
            // the error span to have an offset, so we adjust the span accordingly.
            if let Some(span) = err.span() {
                err = err.set_span(Some(
                    (span.start() + self.start..span.end() + self.start).into(),
                ));
            }
            err
        })
    }
}
