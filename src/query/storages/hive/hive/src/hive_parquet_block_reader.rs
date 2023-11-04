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

use common_arrow::arrow::datatypes::Field;
use common_arrow::arrow::datatypes::Schema;
use common_arrow::arrow::io::parquet::read::column_iter_to_arrays;
use common_arrow::arrow::io::parquet::read::ArrayIter;
use common_arrow::arrow::io::parquet::read::RowGroupDeserializer;
use common_arrow::parquet::metadata::ColumnChunkMetaData;
use common_arrow::parquet::metadata::FileMetaData;
use common_arrow::parquet::metadata::RowGroupMetaData;
use common_arrow::parquet::read::BasicDecompressor;
use common_arrow::parquet::read::PageReader;
use common_base::base::tokio::sync::Semaphore;
use common_catalog::plan::Projection;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::DataBlock;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_expression::TableField;
use common_expression::TableSchemaRef;
use opendal::Operator;
use common_base::runtime::Runtime;
use common_catalog::table_context::TableContext;
use storages_common_cache::LoadParams;

use crate::hive_partition::HivePartInfo;
use crate::HivePartitionFiller;
use crate::MetaDataReader;

#[derive(Clone)]
pub struct HiveBlockReader {
    operator: Operator,
    query_id: String,
    projection: Vec<usize>,
    arrow_schema: Arc<Schema>,
    projected_schema: DataSchemaRef,
    // have partition columns
    output_schema: DataSchemaRef,
    hive_partition_filler: Option<HivePartitionFiller>,
    chunk_size: usize,
}

pub struct DataBlockDeserializer {
    deserializer: RowGroupDeserializer,
    drained: bool,
}

impl DataBlockDeserializer {
    fn new(deserializer: RowGroupDeserializer) -> Self {
        let num_rows = deserializer.num_rows();
        Self {
            deserializer,
            drained: num_rows == 0,
        }
    }

    fn next_block(
        &mut self,
        schema: &DataSchema,
        filler: &Option<HivePartitionFiller>,
        part_info: &HivePartInfo,
    ) -> Result<Option<DataBlock>> {
        if self.drained {
            return Ok(None);
        };

        let opt = self.deserializer.next().transpose()?;
        if let Some(chunk) = opt {
            // If the `Vec<ArrayIter<'static>>` we have passed into the `RowGroupDeserializer`
            // is empty, the deserializer will returns an empty chunk as well(since now rows are consumed).
            // In this case, mark self as drained.
            if chunk.is_empty() {
                self.drained = true;
            }

            let block: DataBlock = DataBlock::from_arrow_chunk(&chunk, schema)?;

            return if let Some(filler) = filler {
                let num_rows = self.deserializer.num_rows();
                let filled = filler.fill_data(block, part_info, num_rows)?;
                Ok(Some(filled))
            } else {
                Ok(Some(block))
            };
        }

        self.drained = true;
        Ok(None)
    }
}

impl HiveBlockReader {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        operator: Operator,
        schema: TableSchemaRef,
        projection: Projection,
        partition_keys: &Option<Vec<String>>,
        chunk_size: usize,
    ) -> Result<Arc<HiveBlockReader>> {
        let original_projection = match projection {
            Projection::Columns(projection) => projection,
            Projection::InnerColumns(b) => {
                return Err(ErrorCode::Unimplemented(format!(
                    "not support inter columns in hive block reader,{:?}",
                    b
                )));
            }
        };
        let output_schema =
            DataSchemaRef::new(DataSchema::from(&schema.project(&original_projection)));

        let (projection, partition_fields) = filter_hive_partition_from_partition_keys(
            schema.clone(),
            original_projection,
            partition_keys,
        );

        let hive_partition_filler = if !partition_fields.is_empty() {
            Some(HivePartitionFiller::create(
                schema.clone(),
                partition_fields,
            ))
        } else {
            None
        };

        let projected_schema = DataSchemaRef::new(DataSchema::from(&schema.project(&projection)));
        let arrow_schema = schema.to_arrow();
        Ok(Arc::new(HiveBlockReader {
            operator,
            projection,
            projected_schema,
            output_schema,
            query_id: ctx.get_id(),
            arrow_schema: Arc::new(arrow_schema),
            hive_partition_filler,
            chunk_size,
        }))
    }

    fn to_deserialize(
        column_meta: &ColumnChunkMetaData,
        chunk: Vec<u8>,
        rows: usize,
        field: Field,
        chunk_size: usize,
    ) -> Result<ArrayIter<'static>> {
        let primitive_type = column_meta.descriptor().descriptor.primitive_type.clone();
        let pages = PageReader::new(
            std::io::Cursor::new(chunk),
            column_meta,
            Arc::new(|_, _| true),
            vec![],
            usize::MAX,
        );

        let decompressor = BasicDecompressor::new(pages, vec![]);
        Ok(column_iter_to_arrays(
            vec![decompressor],
            vec![&primitive_type],
            field,
            Some(chunk_size),
            rows,
        )?)
    }

    pub fn get_parquet_column_metadata<'a>(
        row_group: &'a RowGroupMetaData,
        field_name: &str,
    ) -> Result<&'a ColumnChunkMetaData> {
        let column_meta: Vec<&ColumnChunkMetaData> = row_group
            .columns()
            .iter()
            .filter(|x| {
                x.descriptor().path_in_schema[0].to_lowercase() == field_name.to_lowercase()
            })
            .collect();
        if column_meta.is_empty() {
            return Err(ErrorCode::ParquetFileInvalid(format!(
                "couldn't find column:{} in parquet file",
                field_name
            )));
        } else if column_meta.len() > 1 {
            return Err(ErrorCode::ParquetFileInvalid(format!(
                "find multi column:{} in parquet file",
                field_name
            )));
        }
        Ok(column_meta[0])
    }

    #[async_backtrace::framed]
    async fn read_column(
        op: Operator,
        query_id: String,
        path: String,
        offset: u64,
        length: u64,
        semaphore: Arc<Semaphore>,
    ) -> Result<Vec<u8>> {
        let handler = Runtime::spawn_current_runtime(query_id, async move {
            let chunk = op.read_with(&path).range(offset..offset + length).await?;

            let _semaphore_permit = semaphore.acquire().await.unwrap();
            Ok(chunk)
        });

        match handler.await {
            Ok(Ok(data)) => Ok(data),
            Ok(Err(cause)) => Err(cause),
            Err(cause) => Err(ErrorCode::TokioError(format!(
                "Cannot join future {:?}",
                cause
            ))),
        }
    }

    #[async_backtrace::framed]
    pub async fn read_meta_data(
        &self,
        dal: Operator,
        filename: &str,
        filesize: u64,
    ) -> Result<Arc<FileMetaData>> {
        let reader = MetaDataReader::meta_data_reader(dal);

        let load_params = LoadParams {
            location: filename.to_owned(),
            len_hint: Some(filesize),
            ver: 0,
            put_cache: true,
        };

        reader.read(&load_params).await
    }

    #[async_backtrace::framed]
    pub async fn read_columns_data(
        &self,
        row_group: &RowGroupMetaData,
        part: &HivePartInfo,
    ) -> Result<Vec<Vec<u8>>> {
        let mut join_handlers = Vec::with_capacity(self.projection.len());

        let semaphore = Arc::new(Semaphore::new(10));
        for index in &self.projection {
            let field = &self.arrow_schema.fields[*index];
            let column_meta = Self::get_parquet_column_metadata(row_group, &field.name)?;
            let (start, len) = column_meta.byte_range();

            join_handlers.push(Self::read_column(
                self.operator.clone(),
                self.query_id.clone(),
                part.filename.to_string(),
                start,
                len,
                semaphore.clone(),
            ));
        }

        futures::future::try_join_all(join_handlers).await
    }

    pub fn create_rowgroup_deserializer(
        &self,
        chunks: Vec<Vec<u8>>,
        row_group: &RowGroupMetaData,
    ) -> Result<DataBlockDeserializer> {
        if self.projection.len() != chunks.len() {
            return Err(ErrorCode::Internal(
                "Columns chunk len must be equals projections len.",
            ));
        }

        let mut columns_array_iter = Vec::with_capacity(self.projection.len());

        for (index, column_chunk) in chunks.into_iter().enumerate() {
            let idx = self.projection[index];
            let field = self.arrow_schema.fields[idx].clone();
            let column_meta = Self::get_parquet_column_metadata(row_group, &field.name)?;

            columns_array_iter.push(Self::to_deserialize(
                column_meta,
                column_chunk,
                row_group.num_rows(),
                field,
                self.chunk_size,
            )?);
        }

        let num_row = row_group.num_rows();
        let deserializer = RowGroupDeserializer::new(columns_array_iter, num_row, None);
        Ok(DataBlockDeserializer::new(deserializer))
    }

    pub fn create_data_block(
        &self,
        row_group_iterator: &mut DataBlockDeserializer,
        part: &HivePartInfo,
    ) -> Result<Option<DataBlock>> {
        row_group_iterator
            .next_block(&self.projected_schema, &self.hive_partition_filler, part)
            .map_err(|e| e.add_message(format!(" filename of hive part {}", part.filename)))
    }

    pub fn get_all_datablocks(
        &self,
        mut rowgroup_deserializer: DataBlockDeserializer,
        part: &HivePartInfo,
    ) -> Result<Vec<DataBlock>> {
        let mut all_blocks = vec![];

        while let Some(datablock) = self.create_data_block(&mut rowgroup_deserializer, part)? {
            all_blocks.push(datablock);
        }

        Ok(all_blocks)
    }

    pub fn get_output_schema(&self) -> DataSchemaRef {
        self.output_schema.clone()
    }
}

pub fn filter_hive_partition_from_partition_keys(
    schema: TableSchemaRef,
    projections: Vec<usize>,
    partition_keys: &Option<Vec<String>>,
) -> (Vec<usize>, Vec<TableField>) {
    match partition_keys {
        Some(partition_keys) => {
            let mut not_partitions = vec![];
            let mut partition_fields = vec![];
            for i in projections.into_iter() {
                let field = schema.field(i);
                if !partition_keys.contains(field.name()) {
                    not_partitions.push(i);
                } else {
                    partition_fields.push(field.clone());
                }
            }
            (not_partitions, partition_fields)
        }
        None => (projections, vec![]),
    }
}
