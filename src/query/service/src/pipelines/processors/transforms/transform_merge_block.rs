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

use async_channel::Receiver;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Value;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_sql::IndexType;

pub struct TransformMergeBlock {
    finished: bool,
    input: Arc<InputPort>,
    output: Arc<OutputPort>,

    input_data: Option<DataBlock>,
    output_data: Option<DataBlock>,
    schemas: Vec<DataSchemaRef>,
    output_cols: Vec<Vec<IndexType>>,

    receivers: Vec<(usize, Receiver<DataBlock>)>,
    receiver_results: Vec<(usize, DataBlock)>,
}

impl TransformMergeBlock {
    pub fn try_create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        schemas: Vec<DataSchemaRef>,
        output_cols: Vec<Vec<IndexType>>,
        receivers: Vec<(usize, Receiver<DataBlock>)>,
    ) -> Result<Box<dyn Processor>> {
        Ok(Box::new(TransformMergeBlock {
            finished: false,
            input,
            output,
            input_data: None,
            output_data: None,
            schemas,
            output_cols,
            receivers,
            receiver_results: vec![],
        }))
    }

    fn project_block(&self, block: DataBlock, idx: Option<usize>) -> Result<DataBlock> {
        let num_rows = block.num_rows();
        let columns = if let Some(idx) = idx {
            self.check_type(idx, &block)?
        } else {
            self.output_cols
                .last()
                .unwrap()
                .iter()
                .map(|idx| {
                    Ok(block
                        .get_by_offset(self.schemas.last().unwrap().index_of(&idx.to_string())?)
                        .clone())
                })
                .collect::<Result<Vec<_>>>()?
        };
        let block = DataBlock::new(columns, num_rows);
        Ok(block)
    }

    fn check_type(&self, idx: usize, block: &DataBlock) -> Result<Vec<BlockEntry>> {
        let mut columns = vec![];
        for (left_idx, right_idx) in self
            .output_cols
            .last()
            .unwrap()
            .iter()
            .zip(self.output_cols[idx].iter())
        {
            let left_field = self
                .schemas
                .last()
                .unwrap()
                .field_with_name(&left_idx.to_string())?;
            let left_data_type = left_field.data_type();

            let right_field = self.schemas[idx].field_with_name(&right_idx.to_string())?;
            let right_data_type = right_field.data_type();

            let offset = self.schemas[idx].index_of(&right_idx.to_string())?;
            if left_data_type == right_data_type {
                columns.push(block.get_by_offset(offset).clone());
                continue;
            }

            if left_data_type.remove_nullable() == right_data_type.remove_nullable() {
                let origin_column = block.get_by_offset(offset).clone();
                let mut builder = ColumnBuilder::with_capacity(left_data_type, block.num_rows());
                let value = origin_column.value.as_ref();
                for idx in 0..block.num_rows() {
                    let scalar = value.index(idx).unwrap();
                    builder.push(scalar);
                }
                let col = builder.build();
                columns.push(BlockEntry::new(left_data_type.clone(), Value::Column(col)));
            } else {
                return Err(ErrorCode::IllegalDataType(
                    "The data type on both sides of the union does not match",
                ));
            }
        }
        Ok(columns)
    }
}

#[async_trait::async_trait]
impl Processor for TransformMergeBlock {
    fn name(&self) -> String {
        "TransformMergeBlock".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }

        if let Some(output_data) = self.output_data.take() {
            self.output.push_data(Ok(output_data));
            return Ok(Event::NeedConsume);
        }

        if self.input_data.is_some() || !self.receiver_results.is_empty() {
            return Ok(Event::Sync);
        }

        if self.input.is_finished() {
            if !self.finished {
                return Ok(Event::Async);
            }
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.input.has_data() {
            self.input_data = Some(self.input.pull_data().unwrap()?);
            return Ok(Event::Sync);
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn process(&mut self) -> Result<()> {
        let mut blocks = vec![];
        for (idx, receive_result) in self.receiver_results.iter() {
            blocks.push(self.project_block(receive_result.clone(), Some(*idx))?);
        }
        self.receiver_results.clear();
        if let Some(input_data) = self.input_data.take() {
            blocks.push(self.project_block(input_data, None)?);
        }
        self.output_data = Some(DataBlock::concat(&blocks)?);
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if !self.finished {
            for (idx, receiver) in self.receivers.iter() {
                if let Ok(result) = receiver.recv().await {
                    self.receiver_results.push((*idx, result));
                }
            }
            self.finished = true;
        }
        Ok(())
    }
}
