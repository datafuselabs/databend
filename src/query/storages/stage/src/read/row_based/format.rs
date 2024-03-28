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

use databend_common_exception::Result;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_storage::FileStatus;

use super::batch::BytesBatch;
use super::batch::RowBatchWithPosition;
use super::processors::BlockBuilderState;
use crate::read::load_context::LoadContext;
use crate::read::row_based::formats::CsvInputFormat;
use crate::read::row_based::formats::NdJsonInputFormat;

pub trait SeparatorState: Send + Sync {
    fn append(&mut self, batch: BytesBatch) -> Result<(Vec<RowBatchWithPosition>, FileStatus)>;
}

pub trait RowDecoder: Send + Sync {
    fn add(
        &self,
        block_builder: &mut BlockBuilderState,
        batch: RowBatchWithPosition,
    ) -> Result<Vec<DataBlock>>;

    fn flush(&self, columns: Vec<Column>, _num_rows: usize) -> Vec<Column> {
        columns
    }
}

pub trait RowBasedFileFormat: Sync + Send {
    fn try_create_separator(
        &self,
        load_ctx: Arc<LoadContext>,
        path: &str,
    ) -> Result<Box<dyn SeparatorState>>;
    fn try_create_decoder(&self, load_ctx: Arc<LoadContext>) -> Result<Arc<dyn RowDecoder>>;
}

pub fn create_row_based_file_format(params: &FileFormatParams) -> Arc<dyn RowBasedFileFormat> {
    match params {
        FileFormatParams::Csv(p) => Arc::new(CsvInputFormat { params: p.clone() }),
        FileFormatParams::NdJson(p) => Arc::new(NdJsonInputFormat { params: p.clone() }),
        _ => {
            unreachable!("Unsupported row based file format")
        }
    }
}
