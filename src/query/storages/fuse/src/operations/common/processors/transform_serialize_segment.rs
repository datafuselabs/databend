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

use async_trait::async_trait;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::BlockThresholds;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::PipeItem;
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CachedObject;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::Versioned;
use log::info;
use opendal::Operator;

use crate::io::TableMetaLocationGenerator;
use crate::operations::common::MutationLogEntry;
use crate::operations::common::MutationLogs;
use crate::statistics::StatisticsAccumulator;
use crate::FuseTable;
use crate::DEFAULT_BLOCK_PER_SEGMENT;
use crate::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;

enum State {
    None,
    GenerateSegment,
    SerializedSegment {
        data: Vec<u8>,
        location: String,
        segment: Arc<SegmentInfo>,
    },
    PreCommitSegment {
        location: String,
        segment: Arc<SegmentInfo>,
    },
    Finished,
}

pub struct TransformSerializeSegment {
    data_accessor: Operator,
    meta_locations: TableMetaLocationGenerator,
    accumulator: StatisticsAccumulator,
    state: State,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    output_data: Option<DataBlock>,
    block_per_seg: u64,

    thresholds: BlockThresholds,
    default_cluster_key_id: Option<u32>,
    table_meta_timestamps: TableMetaTimestamps,
}

impl TransformSerializeSegment {
    pub fn new(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        table: &FuseTable,
        thresholds: BlockThresholds,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Self {
        let default_cluster_key_id = table.cluster_key_id();
        TransformSerializeSegment {
            input,
            output,
            output_data: None,
            data_accessor: table.get_operator(),
            meta_locations: table.meta_location_generator().clone(),
            state: State::None,
            accumulator: Default::default(),
            block_per_seg: table
                .get_option(FUSE_OPT_KEY_BLOCK_PER_SEGMENT, DEFAULT_BLOCK_PER_SEGMENT)
                as u64,
            thresholds,
            default_cluster_key_id,
            table_meta_timestamps,
        }
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
}

#[async_trait]
impl Processor for TransformSerializeSegment {
    fn name(&self) -> String {
        "TransformSerializeSegment".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if matches!(
            &self.state,
            State::GenerateSegment | State::PreCommitSegment { .. }
        ) {
            return Ok(Event::Sync);
        }

        if matches!(&self.state, State::SerializedSegment { .. }) {
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
            if self.accumulator.summary_row_count != 0 {
                self.state = State::GenerateSegment;
                return Ok(Event::Sync);
            }
            self.output.finish();
            self.state = State::Finished;
            return Ok(Event::Finished);
        }

        if self.input.has_data() {
            let input_meta = self
                .input
                .pull_data()
                .unwrap()?
                .get_meta()
                .cloned()
                .ok_or_else(|| ErrorCode::Internal("No block meta. It's a bug"))?;
            let block_meta = BlockMeta::downcast_ref_from(&input_meta)
                .ok_or_else(|| ErrorCode::Internal("No commit meta. It's a bug"))?
                .clone();

            self.accumulator.add_with_block_meta(block_meta);
            if self.accumulator.summary_block_count >= self.block_per_seg {
                self.state = State::GenerateSegment;
                return Ok(Event::Sync);
            }
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::None) {
            State::GenerateSegment => {
                let acc = std::mem::take(&mut self.accumulator);
                let summary = acc.summary(self.thresholds, self.default_cluster_key_id);

                let segment_info = SegmentInfo::new(acc.blocks_metas, summary);

                self.state = State::SerializedSegment {
                    data: segment_info.to_bytes()?,
                    location: self
                        .meta_locations
                        .gen_segment_info_location(self.table_meta_timestamps),
                    segment: Arc::new(segment_info),
                }
            }
            State::PreCommitSegment { location, segment } => {
                if let Some(segment_cache) = SegmentInfo::cache() {
                    segment_cache.insert(location.clone(), segment.as_ref().try_into()?);
                }

                let format_version = SegmentInfo::VERSION;

                // emit log entry.
                // for newly created segment, always use the latest version
                let meta = MutationLogs {
                    entries: vec![MutationLogEntry::AppendSegment {
                        segment_location: location,
                        format_version,
                        summary: segment.summary.clone(),
                    }],
                };

                self.output_data = Some(DataBlock::empty_with_meta(Box::new(meta)));
            }
            _state => {
                return Err(ErrorCode::Internal("Unknown state for fuse table sink"));
            }
        }

        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        match std::mem::replace(&mut self.state, State::None) {
            State::SerializedSegment {
                data,
                location,
                segment,
            } => {
                self.data_accessor.write(&location, data).await?;
                info!("fuse append wrote down segment {} ", location);

                self.state = State::PreCommitSegment { location, segment };
            }
            _state => {
                return Err(ErrorCode::Internal("Unknown state for fuse table sink."));
            }
        }

        Ok(())
    }
}
