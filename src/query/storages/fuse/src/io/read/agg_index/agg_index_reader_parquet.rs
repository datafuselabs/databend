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

use databend_common_catalog::plan::PartInfoPtr;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_storage::parquet_rs::read_metadata_sync;
use databend_common_storage::read_metadata_async;
use databend_storages_common_io::ReadSettings;
use log::debug;

use super::AggIndexReader;
use crate::io::read::utils::build_columns_meta;
use crate::BlockReadResult;
use crate::FuseBlockPartInfo;

impl AggIndexReader {
    pub fn sync_read_parquet_data_by_merge_io(
        &self,
        read_settings: &ReadSettings,
        loc: &str,
    ) -> Option<(PartInfoPtr, BlockReadResult)> {
        let op = self.reader.operator.blocking();
        match op.stat(loc) {
            Ok(_meta) => {
                let metadata = read_metadata_sync(loc, &self.reader.operator, None).ok()?;
                debug_assert_eq!(metadata.num_row_groups(), 1);
                let row_group = &metadata.row_groups()[0];
                let columns_meta = build_columns_meta(row_group);

                let part = FuseBlockPartInfo::create(
                    loc.to_string(),
                    row_group.num_rows() as u64,
                    columns_meta,
                    None,
                    self.compression.into(),
                    None,
                    None,
                    None,
                );
                let res = self
                    .reader
                    .sync_read_columns_data_by_merge_io(read_settings, &part, &None)
                    .inspect_err(|e| debug!("Read aggregating index `{loc}` failed: {e}"))
                    .ok()?;
                Some((part, res))
            }
            Err(e) => {
                if e.kind() == opendal::ErrorKind::NotFound {
                    debug!("Aggregating index `{loc}` not found.")
                } else {
                    debug!("Read aggregating index `{loc}` failed: {e}");
                }
                None
            }
        }
    }

    pub async fn read_parquet_data_by_merge_io(
        &self,
        read_settings: &ReadSettings,
        loc: &str,
    ) -> Option<(PartInfoPtr, BlockReadResult)> {
        match self.reader.operator.stat(loc).await {
            Ok(_meta) => {
                let metadata = read_metadata_async(loc, &self.reader.operator, None)
                    .await
                    .ok()?;
                debug_assert_eq!(metadata.num_row_groups(), 1);
                let row_group = &metadata.row_groups()[0];
                let columns_meta = build_columns_meta(row_group);
                let res = self
                    .reader
                    .read_columns_data_by_merge_io(read_settings, loc, &columns_meta, &None)
                    .await
                    .inspect_err(|e| debug!("Read aggregating index `{loc}` failed: {e}"))
                    .ok()?;
                let part = FuseBlockPartInfo::create(
                    loc.to_string(),
                    row_group.num_rows() as u64,
                    columns_meta,
                    None,
                    self.compression.into(),
                    None,
                    None,
                    None,
                );
                Some((part, res))
            }
            Err(e) => {
                if e.kind() == opendal::ErrorKind::NotFound {
                    debug!("Aggregating index `{loc}` not found.")
                } else {
                    debug!("Read aggregating index `{loc}` failed: {e}");
                }
                None
            }
        }
    }

    pub fn deserialize_parquet_data(
        &self,
        part: PartInfoPtr,
        data: BlockReadResult,
    ) -> Result<DataBlock> {
        let columns_chunks = data.columns_chunks()?;
        let part = FuseBlockPartInfo::from_part(&part)?;
        let block = self.reader.deserialize_parquet_chunks(
            part.nums_rows,
            &part.columns_meta,
            columns_chunks,
            &part.compression,
            &part.location,
        )?;

        self.apply_agg_info(block)
    }
}
