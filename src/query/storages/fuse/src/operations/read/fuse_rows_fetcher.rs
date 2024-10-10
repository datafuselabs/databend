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

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::nullable::NullableColumn;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::Value;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_transforms::processors::AsyncTransform;
use databend_common_pipeline_transforms::processors::AsyncTransformer;
use databend_storages_common_io::ReadSettings;

use super::native_rows_fetcher::NativeRowsFetcher;
use super::parquet_rows_fetcher::ParquetRowsFetcher;
use crate::FuseStorageFormat;
use crate::FuseTable;

type RowFetcher = Box<dyn Fn(Arc<InputPort>, Arc<OutputPort>) -> Result<ProcessorPtr>>;

pub fn row_fetch_processor(
    ctx: Arc<dyn TableContext>,
    row_id_col_offset: usize,
    source: &DataSourcePlan,
    projection: Projection,
    need_wrap_nullable: bool,
) -> Result<RowFetcher> {
    let table = ctx.build_table_from_source_plan(source)?;
    let fuse_table = table
        .as_any()
        .downcast_ref::<FuseTable>()
        .ok_or_else(|| ErrorCode::Internal("Row fetcher is only supported by Fuse engine"))?
        .to_owned();
    let fuse_table = Arc::new(fuse_table);
    let block_reader =
        fuse_table.create_block_reader(ctx.clone(), projection.clone(), false, false, true)?;
    let max_threads = ctx.get_settings().get_max_threads()? as usize;

    match &fuse_table.storage_format {
        FuseStorageFormat::Native => Ok(Box::new(move |input, output| {
            Ok(if block_reader.support_blocking_api() {
                TransformRowsFetcher::create(
                    input,
                    output,
                    row_id_col_offset,
                    NativeRowsFetcher::<true>::create(
                        fuse_table.clone(),
                        projection.clone(),
                        block_reader.clone(),
                        max_threads,
                    ),
                    need_wrap_nullable,
                )
            } else {
                TransformRowsFetcher::create(
                    input,
                    output,
                    row_id_col_offset,
                    NativeRowsFetcher::<false>::create(
                        fuse_table.clone(),
                        projection.clone(),
                        block_reader.clone(),
                        max_threads,
                    ),
                    need_wrap_nullable,
                )
            })
        })),
        FuseStorageFormat::Parquet => {
            let read_settings = ReadSettings::from_ctx(&ctx)?;
            Ok(Box::new(move |input, output| {
                Ok(if block_reader.support_blocking_api() {
                    TransformRowsFetcher::create(
                        input,
                        output,
                        row_id_col_offset,
                        ParquetRowsFetcher::<true>::create(
                            fuse_table.clone(),
                            projection.clone(),
                            block_reader.clone(),
                            read_settings,
                            max_threads,
                        ),
                        need_wrap_nullable,
                    )
                } else {
                    TransformRowsFetcher::create(
                        input,
                        output,
                        row_id_col_offset,
                        ParquetRowsFetcher::<false>::create(
                            fuse_table.clone(),
                            projection.clone(),
                            block_reader.clone(),
                            read_settings,
                            max_threads,
                        ),
                        need_wrap_nullable,
                    )
                })
            }))
        }
    }
}

#[async_trait::async_trait]
pub trait RowsFetcher {
    async fn on_start(&mut self) -> Result<()>;
    async fn fetch(&mut self, row_ids: &[u64]) -> Result<DataBlock>;
    fn schema(&self) -> DataSchema;
}

pub struct TransformRowsFetcher<F: RowsFetcher> {
    row_id_col_offset: usize,
    fetcher: F,
    need_wrap_nullable: bool,
}

#[async_trait::async_trait]
impl<F> AsyncTransform for TransformRowsFetcher<F>
where F: RowsFetcher + Send + Sync + 'static
{
    const NAME: &'static str = "TransformRowsFetcher";

    #[async_backtrace::framed]
    async fn on_start(&mut self) -> Result<()> {
        self.fetcher.on_start().await
    }

    #[async_backtrace::framed]
    async fn transform(&mut self, mut data: DataBlock) -> Result<DataBlock> {
        let num_rows = data.num_rows();
        if num_rows == 0 {
            // Although the data block is empty, we need to add empty columns to align the schema.
            let fetched_schema = self.fetcher.schema();
            for f in fetched_schema.fields().iter() {
                let builder = ColumnBuilder::with_capacity(f.data_type(), 0);
                let col = builder.build();
                data.add_column(BlockEntry::new(f.data_type().clone(), Value::Column(col)));
            }
            return Ok(data);
        }

        let entry = &data.columns()[self.row_id_col_offset];
        let value = entry
            .value
            .convert_to_full_column(&entry.data_type, num_rows);
        let row_id_column = if matches!(entry.data_type, DataType::Number(NumberDataType::UInt64)) {
            value.into_number().unwrap().into_u_int64().unwrap()
        } else {
            // From merge into matched data, the row id column is nullable but has no null value.
            let value = *value.into_nullable().unwrap();
            debug_assert!(value.validity.unset_bits() == 0);
            value.column.into_number().unwrap().into_u_int64().unwrap()
        };

        let fetched_block = self.fetcher.fetch(&row_id_column).await?;

        for col in fetched_block.columns().iter() {
            if self.need_wrap_nullable {
                data.add_column(wrap_true_validity(col, num_rows));
            } else {
                data.add_column(col.clone());
            }
        }

        Ok(data)
    }
}

impl<F> TransformRowsFetcher<F>
where F: RowsFetcher + Send + Sync + 'static
{
    fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        row_id_col_offset: usize,
        fetcher: F,
        need_wrap_nullable: bool,
    ) -> ProcessorPtr {
        ProcessorPtr::create(AsyncTransformer::create(input, output, Self {
            row_id_col_offset,
            fetcher,
            need_wrap_nullable,
        }))
    }
}

fn wrap_true_validity(column: &BlockEntry, num_rows: usize) -> BlockEntry {
    let (value, data_type) = (&column.value, &column.data_type);
    let col = value.convert_to_full_column(data_type, num_rows);
    if matches!(col, Column::Null { .. }) || col.as_nullable().is_some() {
        column.clone()
    } else {
        let col = NullableColumn::new_column(col, Bitmap::new_trued(num_rows));
        BlockEntry::new(data_type.wrap_nullable(), Value::Column(col))
    }
}
