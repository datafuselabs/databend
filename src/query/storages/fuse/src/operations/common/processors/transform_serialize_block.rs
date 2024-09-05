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

use std::any::Any;
use std::sync::Arc;

use databend_common_base::base::ProgressValues;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::ComputedExpr;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchema;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::PipeItem;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use opendal::Operator;

use crate::io::create_inverted_index_builders;
use crate::io::BlockBuilder;
use crate::io::BlockSerialization;
use crate::io::BlockWriter;
use crate::operations::common::BlockMetaIndex;
use crate::operations::common::MutationLogEntry;
use crate::operations::common::MutationLogs;
use crate::operations::mutation::ClusterStatsGenType;
use crate::operations::mutation::SerializeDataMeta;
use crate::statistics::ClusterStatsGenerator;
use crate::FuseTable;

#[allow(clippy::large_enum_variant)]
enum State {
    Consume,
    NeedSerialize {
        block: DataBlock,
        stats_type: ClusterStatsGenType,
        index: Option<BlockMetaIndex>,
    },
    Serialized {
        serialized: BlockSerialization,
        index: Option<BlockMetaIndex>,
    },
}

pub struct TransformSerializeBlock {
    state: State,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,

    block_builder: BlockBuilder,
    dal: Operator,
    table_id: Option<u64>, // Only used in multi table insert
    kind: MutationKind,
}

impl TransformSerializeBlock {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        table: &FuseTable,
        cluster_stats_gen: ClusterStatsGenerator,
        kind: MutationKind,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<Self> {
        Self::do_create(
            ctx,
            input,
            output,
            table,
            cluster_stats_gen,
            kind,
            false,
            table_meta_timestamps,
        )
    }

    pub fn try_create_with_tid(
        ctx: Arc<dyn TableContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        table: &FuseTable,
        cluster_stats_gen: ClusterStatsGenerator,
        kind: MutationKind,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<Self> {
        Self::do_create(
            ctx,
            input,
            output,
            table,
            cluster_stats_gen,
            kind,
            true,
            table_meta_timestamps,
        )
    }

    fn do_create(
        ctx: Arc<dyn TableContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        table: &FuseTable,
        cluster_stats_gen: ClusterStatsGenerator,
        kind: MutationKind,
        with_tid: bool,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<Self> {
        // remove virtual computed fields.
        let mut fields = table
            .schema()
            .fields()
            .iter()
            .filter(|f| !matches!(f.computed_expr(), Some(ComputedExpr::Virtual(_))))
            .cloned()
            .collect::<Vec<_>>();
        if !matches!(kind, MutationKind::Insert | MutationKind::Replace) {
            // add stream fields.
            for stream_column in table.stream_columns().iter() {
                fields.push(stream_column.table_field());
            }
        }
        let source_schema = Arc::new(TableSchema {
            fields,
            ..table.schema().as_ref().clone()
        });

        let bloom_columns_map = table
            .bloom_index_cols
            .bloom_index_fields(source_schema.clone(), BloomIndex::supported_type)?;

        let inverted_index_builders = create_inverted_index_builders(&table.table_info.meta);

        let block_builder = BlockBuilder {
            ctx,
            meta_locations: table.meta_location_generator().clone(),
            source_schema,
            write_settings: table.get_write_settings(),
            cluster_stats_gen,
            bloom_columns_map,
            inverted_index_builders,
            table_meta_timestamps,
        };
        Ok(TransformSerializeBlock {
            state: State::Consume,
            input,
            output,
            output_data: None,
            block_builder,
            dal: table.get_operator(),
            table_id: if with_tid { Some(table.get_id()) } else { None },
            kind,
        })
    }

    pub fn into_processor(self) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(Box::new(self)))
    }

    pub fn into_pipe_item(self) -> PipeItem {
        let input = self.input.clone();
        let output = self.output.clone();
        let processor_ptr = ProcessorPtr::create(Box::new(self));
        PipeItem::create(processor_ptr, vec![input], vec![output])
    }

    pub fn get_block_builder(&self) -> BlockBuilder {
        self.block_builder.clone()
    }

    fn mutation_logs(entry: MutationLogEntry) -> DataBlock {
        let meta = MutationLogs {
            entries: vec![entry],
        };
        DataBlock::empty_with_meta(Box::new(meta))
    }
}

#[async_trait::async_trait]
impl Processor for TransformSerializeBlock {
    fn name(&self) -> String {
        "TransformSerializeBlock".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(self.state, State::NeedSerialize { .. }) {
            return Ok(Event::Sync);
        }

        if matches!(self.state, State::Serialized { .. }) {
            return Ok(Event::Async);
        }

        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        if let Some(data_block) = self.output_data.take() {
            self.output.push_data(Ok(data_block));
            return Ok(Event::NeedConsume);
        }

        if self.input.is_finished() {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if !self.input.has_data() {
            self.input.set_need_data();
            return Ok(Event::NeedData);
        }

        let mut input_data = self.input.pull_data().unwrap()?;
        let meta = input_data.take_meta();
        if let Some(meta) = meta {
            let meta = SerializeDataMeta::downcast_from(meta)
                .ok_or_else(|| ErrorCode::Internal("It's a bug"))?;
            match meta {
                SerializeDataMeta::DeletedSegment(deleted_segment) => {
                    // delete a whole segment, segment level
                    let data_block =
                        Self::mutation_logs(MutationLogEntry::DeletedSegment { deleted_segment });
                    self.output.push_data(Ok(data_block));
                    Ok(Event::NeedConsume)
                }
                SerializeDataMeta::SerializeBlock(serialize_block) => {
                    if input_data.is_empty() {
                        // delete a whole block, block level
                        let data_block = Self::mutation_logs(MutationLogEntry::DeletedBlock {
                            index: serialize_block.index,
                        });
                        self.output.push_data(Ok(data_block));
                        Ok(Event::NeedConsume)
                    } else {
                        // replace the old block
                        self.state = State::NeedSerialize {
                            block: input_data,
                            stats_type: serialize_block.stats_type,
                            index: Some(serialize_block.index),
                        };
                        Ok(Event::Sync)
                    }
                }
                SerializeDataMeta::CompactExtras(compact_extras) => {
                    // compact extras
                    let data_block = Self::mutation_logs(MutationLogEntry::CompactExtras {
                        extras: compact_extras,
                    });
                    self.output.push_data(Ok(data_block));
                    Ok(Event::NeedConsume)
                }
            }
        } else if input_data.is_empty() {
            // do nothing
            let data_block = Self::mutation_logs(MutationLogEntry::DoNothing);
            self.output.push_data(Ok(data_block));
            Ok(Event::NeedConsume)
        } else {
            // append block
            self.state = State::NeedSerialize {
                block: input_data,
                stats_type: ClusterStatsGenType::Generally,
                index: None,
            };
            Ok(Event::Sync)
        }
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Consume) {
            State::NeedSerialize {
                block,
                stats_type,
                index,
            } => {
                // Check if the datablock is valid, this is needed to ensure data is correct
                block.check_valid()?;

                let serialized =
                    self.block_builder
                        .build(block, |block, generator| match &stats_type {
                            ClusterStatsGenType::Generally => generator.gen_stats_for_append(block),
                            ClusterStatsGenType::WithOrigin(origin_stats) => {
                                let cluster_stats = generator
                                    .gen_with_origin_stats(&block, origin_stats.clone())?;
                                Ok((cluster_stats, block))
                            }
                        })?;

                self.state = State::Serialized { serialized, index };
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::Consume) {
            State::Serialized { serialized, index } => {
                let block_meta = BlockWriter::write_down(&self.dal, serialized).await?;

                let mutation_log_data_block = if let Some(index) = index {
                    // we are replacing the block represented by the `index`
                    Self::mutation_logs(MutationLogEntry::ReplacedBlock {
                        index,
                        block_meta: Arc::new(block_meta),
                    })
                } else {
                    // appending new data block
                    let progress_values = ProgressValues {
                        rows: block_meta.row_count as usize,
                        bytes: block_meta.block_size as usize,
                    };
                    self.block_builder
                        .ctx
                        .get_write_progress()
                        .incr(&progress_values);

                    if let Some(tid) = self.table_id {
                        self.block_builder
                            .ctx
                            .update_multi_table_insert_status(tid, block_meta.row_count);
                    }

                    if matches!(self.kind, MutationKind::Recluster) {
                        Self::mutation_logs(MutationLogEntry::ReclusterAppendBlock {
                            block_meta: Arc::new(block_meta),
                        })
                    } else {
                        DataBlock::empty_with_meta(Box::new(block_meta))
                    }
                };
                self.output_data = Some(mutation_log_data_block);
            }
            _ => return Err(ErrorCode::Internal("It's a bug.")),
        }
        Ok(())
    }
}
