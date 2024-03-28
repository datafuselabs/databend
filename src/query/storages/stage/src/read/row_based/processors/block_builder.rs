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

use std::mem;
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;
use databend_common_storage::FileStatus;
use log::debug;

use crate::read::load_context::LoadContext;
use crate::read::row_based::batch::RowBatchWithPosition;
use crate::read::row_based::format::RowBasedFileFormat;
use crate::read::row_based::format::RowDecoder;

pub struct BlockBuilderState {
    pub mutable_columns: Vec<ColumnBuilder>,
    pub num_rows: usize,
    pub file_status: FileStatus,
    pub file_name: String,
}

impl BlockBuilderState {
    fn create(ctx: Arc<LoadContext>) -> Self {
        let columns = ctx
            .schema
            .fields()
            .iter()
            .map(|f| {
                ColumnBuilder::with_capacity_hint(
                    &f.data_type().into(),
                    // todo(youngsofun): calculate the capacity based on the memory and schema
                    1024,
                    false,
                )
            })
            .collect();

        BlockBuilderState {
            mutable_columns: columns,
            num_rows: 0,
            file_status: Default::default(),
            file_name: "".to_string(),
        }
    }

    fn take_columns(&mut self, on_finish: bool) -> Result<Vec<Column>> {
        // todo(youngsofun): calculate the capacity according to last batch
        let capacity = if on_finish { 0 } else { 1024 };
        self.num_rows = 0;
        self.file_name = "".to_string();
        Ok(self
            .mutable_columns
            .iter_mut()
            .map(|col| {
                let empty_builder =
                    ColumnBuilder::with_capacity_hint(&col.data_type(), capacity, false);
                std::mem::replace(col, empty_builder).build()
            })
            .collect())
    }

    fn flush_status(&mut self, ctx: &Arc<dyn TableContext>) -> Result<()> {
        let file_status = mem::take(&mut self.file_status);
        ctx.add_file_status(&self.file_name, file_status)
    }

    fn memory_size(&self) -> usize {
        self.mutable_columns.iter().map(|x| x.memory_size()).sum()
    }
}

pub struct BlockBuilder {
    pub ctx: Arc<LoadContext>,
    pub state: BlockBuilderState,
    pub decoder: Arc<dyn RowDecoder>,
}

impl BlockBuilder {
    pub fn create(ctx: Arc<LoadContext>, fmt: &Arc<dyn RowBasedFileFormat>) -> Result<Self> {
        let state = BlockBuilderState::create(ctx.clone());
        let decoder = fmt.try_create_decoder(ctx.clone())?;
        Ok(BlockBuilder {
            ctx,
            state,
            decoder,
        })
    }

    pub fn flush_block(&mut self, on_finish: bool) -> Result<Vec<DataBlock>> {
        let num_rows = self.state.num_rows;
        let columns = self.state.take_columns(on_finish)?;
        if columns.is_empty() || num_rows == 0 {
            Ok(vec![])
        } else {
            let columns = self.decoder.flush(columns, num_rows);
            Ok(vec![DataBlock::new_from_columns(columns)])
        }
    }
    pub fn try_flush_block_by_memory(&mut self) -> Result<Vec<DataBlock>> {
        let mem = self.state.memory_size();
        debug!(
            "chunk builder added new batch: row {} size {}",
            self.state.num_rows, mem
        );
        if self.state.num_rows >= self.ctx.block_compact_thresholds.min_rows_per_block
            || mem > self.ctx.block_compact_thresholds.max_bytes_per_block
        {
            self.flush_block(false)
        } else {
            Ok(vec![])
        }
    }
}

impl AccumulatingTransform for BlockBuilder {
    const NAME: &'static str = "BlockBuilder";

    fn transform(&mut self, data: DataBlock) -> Result<Vec<DataBlock>> {
        let batch = data
            .get_owned_meta()
            .and_then(RowBatchWithPosition::downcast_from)
            .unwrap();
        if self.state.file_name != batch.start_pos.path {
            self.state.file_name = batch.start_pos.path.clone();
        }
        let mut blocks = self.decoder.add(&mut self.state, batch)?;
        self.state.flush_status(&self.ctx.table_context)?;
        let more = self.try_flush_block_by_memory()?;
        blocks.extend(more);
        Ok(blocks)
    }

    fn on_finish(&mut self, _output: bool) -> Result<Vec<DataBlock>> {
        self.flush_block(true)
    }
}
