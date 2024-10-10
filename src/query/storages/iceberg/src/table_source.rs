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

use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_pipeline_core::processors::Event;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_storages_parquet::transform_record_batch;
use futures::stream;
use futures::StreamExt;
use iceberg::scan::ArrowRecordBatchStream;

use crate::partition::IcebergPartInfo;
use crate::IcebergTable;

pub struct IcebergTableSource {
    // Source processor related fields.
    table: IcebergTable,
    output: Arc<OutputPort>,
    scan_progress: Arc<Progress>,

    // Used for event transforming.
    ctx: Arc<dyn TableContext>,
    generated_data: Option<DataBlock>,
    is_finished: bool,

    // Used to read parquet.
    output_schema: DataSchemaRef,
    stream: Option<ArrowRecordBatchStream>,
}

impl IcebergTableSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        output_schema: DataSchemaRef,
        table: IcebergTable,
    ) -> Result<ProcessorPtr> {
        let scan_progress = ctx.get_scan_progress();
        Ok(ProcessorPtr::create(Box::new(IcebergTableSource {
            table,
            output,
            scan_progress,
            ctx,
            output_schema,
            stream: None,
            generated_data: None,
            is_finished: false,
        })))
    }
}

#[async_trait::async_trait]
impl Processor for IcebergTableSource {
    fn name(&self) -> String {
        "IcebergSource".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.is_finished {
            self.output.finish();
            return Ok(Event::Finished);
        }

        if self.output.is_finished() {
            return Ok(Event::Finished);
        }

        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }

        match self.generated_data.take() {
            None => Ok(Event::Async),
            Some(data_block) => {
                let progress_values = ProgressValues {
                    rows: data_block.num_rows(),
                    bytes: data_block.memory_size(),
                };
                self.scan_progress.incr(&progress_values);
                Profile::record_usize_profile(
                    ProfileStatisticsName::ScanBytes,
                    data_block.memory_size(),
                );
                self.output.push_data(Ok(data_block));
                Ok(Event::NeedConsume)
            }
        }
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if let Some(mut stream) = self.stream.take() {
            if let Some(batch) =
                stream.next().await.transpose().map_err(|err| {
                    ErrorCode::Internal(format!("iceberg data stream read: {err:?}"))
                })?
            {
                let block = transform_record_batch(&self.output_schema, &batch, &None)?;
                let block = check_block_schema(&self.output_schema, block)?;

                self.generated_data = Some(block);
                self.stream = Some(stream);
            }
            // else:
            // If `read_block` returns `None`, it means the stream is finished.
            // And we should try to build another stream (in next event loop).
        } else if let Some(part) = self.ctx.get_partition() {
            let part = IcebergPartInfo::from_part(&part)?;
            // TODO: enable row filter?
            let reader = self.table.table().await?.reader_builder().build();
            // TODO: don't use stream here.
            let stream = reader
                .read(Box::pin(stream::iter([Ok(part.to_task())])))
                .map_err(|err| ErrorCode::Internal(format!("iceberg data stream read: {err:?}")))?;
            self.stream = Some(stream);
        } else {
            self.is_finished = true;
        }

        Ok(())
    }
}

fn check_block_schema(schema: &DataSchema, mut block: DataBlock) -> Result<DataBlock> {
    // Check if the schema of the data block is matched with the schema of the table.
    if block.num_columns() != schema.num_fields() {
        return Err(ErrorCode::TableSchemaMismatch(format!(
            "Data schema mismatched. Data columns length: {}, schema fields length: {}",
            block.num_columns(),
            schema.num_fields()
        )));
    }

    for (col, field) in block.columns_mut().iter_mut().zip(schema.fields().iter()) {
        // If the actual data is nullable, the field must be nullbale.
        if col.data_type.is_nullable_or_null() && !field.is_nullable() {
            return Err(ErrorCode::TableSchemaMismatch(format!(
                "Data schema mismatched (col name: {}). Data column is nullable, but schema field is not nullable",
                field.name()
            )));
        }
        // The inner type of the data and field should be the same.
        let data_type = col.data_type.remove_nullable();
        let schema_type = field.data_type().remove_nullable();
        if data_type != schema_type {
            return Err(ErrorCode::TableSchemaMismatch(format!(
                "Data schema mismatched (col name: {}). Data column type is {:?}, but schema field type is {:?}",
                field.name(),
                col.data_type,
                field.data_type()
            )));
        }
        // If the field is nullable but the actual data is not nullable,
        // we should wrap nullable for the data.
        if field.is_nullable() && !col.data_type.is_nullable_or_null() {
            col.data_type = col.data_type.wrap_nullable();
            col.value = col.value.clone().wrap_nullable(None);
        }
    }

    Ok(block)
}
