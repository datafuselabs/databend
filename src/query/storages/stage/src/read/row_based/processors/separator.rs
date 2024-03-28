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

use databend_common_base::base::ProgressValues;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline_transforms::processors::AccumulatingTransform;

use crate::read::load_context::LoadContext;
use crate::read::row_based::batch::BytesBatch;
use crate::read::row_based::format::RowBasedFileFormat;
use crate::read::row_based::format::SeparatorState;

pub struct Separator {
    pub ctx: Arc<LoadContext>,
    pub state: Option<Box<dyn SeparatorState>>,
    pub format: Arc<dyn RowBasedFileFormat>,
}

impl Separator {
    pub fn try_create(ctx: Arc<LoadContext>, format: Arc<dyn RowBasedFileFormat>) -> Result<Self> {
        Ok(Separator {
            ctx,
            format,
            state: None,
        })
    }
}

impl AccumulatingTransform for Separator {
    const NAME: &'static str = "Separator";

    fn transform(&mut self, data: DataBlock) -> Result<Vec<DataBlock>> {
        let batch = data
            .get_owned_meta()
            .and_then(BytesBatch::downcast_from)
            .unwrap();

        let state = self.state.get_or_insert_with(|| {
            self.format
                .try_create_separator(self.ctx.clone(), &batch.path)
                .unwrap()
        });
        let mut process_values = ProgressValues { rows: 0, bytes: 0 };

        let batch_meta = batch.meta();
        let (row_batches, file_status) = state.append(batch)?;
        let row_batches = row_batches
            .into_iter()
            .filter(|b| b.data.rows() > 0 || b.data.size() > 0)
            .collect::<Vec<_>>();
        if batch_meta.is_eof {
            if file_status.error.is_some() {
                self.ctx
                    .table_context
                    .get_copy_status()
                    .add_chunk(&batch_meta.path, file_status);
            }
            self.state = None;
        }
        if row_batches.is_empty() {
            Ok(vec![])
        } else {
            for b in row_batches.iter() {
                process_values.rows += b.data.rows();
            }
            self.ctx
                .table_context
                .get_scan_progress()
                .incr(&process_values);

            let blocks = row_batches
                .into_iter()
                .map(|b| DataBlock::empty_with_meta(Box::new(b)))
                .collect::<Vec<_>>();
            Ok(blocks)
        }
    }
}
