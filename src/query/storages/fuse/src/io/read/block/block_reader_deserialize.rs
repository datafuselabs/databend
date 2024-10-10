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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_arrow::arrow::array::Array;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataBlock;
use databend_storages_common_cache::SizedColumnArray;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ColumnMeta;
use databend_storages_common_table_meta::meta::Compression;

use super::BlockReader;
use crate::io::read::block::block_reader_merge_io::DataItem;
use crate::BlockReadResult;
use crate::FuseBlockPartInfo;
use crate::FuseStorageFormat;

pub enum DeserializedArray<'a> {
    Cached(&'a Arc<SizedColumnArray>),
    Deserialized((ColumnId, Box<dyn Array>, usize)),
    NoNeedToCache(Box<dyn Array>),
}

pub struct FieldDeserializationContext<'a> {
    pub(crate) column_metas: &'a HashMap<ColumnId, ColumnMeta>,
    pub(crate) column_chunks: &'a HashMap<ColumnId, DataItem<'a>>,
}

impl BlockReader {
    /// Deserialize column chunks data from parquet format to DataBlock.
    pub fn deserialize_chunks_with_part_info(
        &self,
        part: PartInfoPtr,
        chunks: HashMap<ColumnId, DataItem>,
        storage_format: &FuseStorageFormat,
    ) -> Result<DataBlock> {
        let part = FuseBlockPartInfo::from_part(&part)?;

        self.deserialize_chunks(
            &part.location,
            part.nums_rows,
            &part.compression,
            &part.columns_meta,
            chunks,
            storage_format,
        )
    }

    pub fn deserialize_chunks(
        &self,
        block_path: &str,
        num_rows: usize,
        compression: &Compression,
        column_metas: &HashMap<ColumnId, ColumnMeta>,
        column_chunks: HashMap<ColumnId, DataItem>,
        storage_format: &FuseStorageFormat,
    ) -> Result<DataBlock> {
        match storage_format {
            FuseStorageFormat::Parquet => self.deserialize_parquet_chunks(
                num_rows,
                column_metas,
                column_chunks,
                compression,
                block_path,
            ),
            FuseStorageFormat::Native => {
                self.deserialize_native_chunks(block_path, num_rows, column_metas, column_chunks)
            }
        }
    }

    #[fastrace::trace]
    #[async_backtrace::framed]
    pub async fn read_by_meta(
        &self,
        settings: &ReadSettings,
        meta: &BlockMeta,
        storage_format: &FuseStorageFormat,
    ) -> Result<DataBlock> {
        // Get the merged IO read result.
        let merge_io_read_result = self
            .read_columns_data_by_merge_io(settings, &meta.location.0, &meta.col_metas, &None)
            .await?;

        self.deserialize_chunks_with_meta(meta, storage_format, merge_io_read_result)
    }

    pub fn deserialize_chunks_with_meta(
        &self,
        meta: &BlockMeta,
        storage_format: &FuseStorageFormat,
        data: BlockReadResult,
    ) -> Result<DataBlock> {
        // Get the columns chunk.
        let column_chunks = data.columns_chunks()?;

        let num_rows = meta.row_count as usize;

        match storage_format {
            FuseStorageFormat::Parquet => self.deserialize_parquet_chunks(
                num_rows,
                &meta.col_metas,
                column_chunks,
                &meta.compression,
                &meta.location.0,
            ),
            FuseStorageFormat::Native => self.deserialize_native_chunks_with_buffer(
                &meta.location.0,
                num_rows,
                &meta.col_metas,
                column_chunks,
            ),
        }
    }
}
