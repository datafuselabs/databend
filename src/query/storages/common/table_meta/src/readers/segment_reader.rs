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

use databend_common_exception::Result;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use futures::AsyncRead;
use futures_util::AsyncReadExt;

use crate::meta::load_json;
use crate::meta::CompactSegmentInfo;
use crate::meta::SegmentInfo;
use crate::meta::SegmentInfoV3;
use crate::meta::SegmentInfoVersion;
use crate::readers::VersionedReader;

#[async_trait::async_trait]
impl VersionedReader<CompactSegmentInfo> for (SegmentInfoVersion, TableSchemaRef) {
    type TargetType = CompactSegmentInfo;
    #[async_backtrace::framed]
    async fn read<R>(&self, mut reader: R) -> Result<CompactSegmentInfo>
    where R: AsyncRead + Unpin + Send {
        let schema = &self.1;
        let mut buffer: Vec<u8> = vec![];
        reader.read_to_end(&mut buffer).await?;
        deserialize_segment_info(&self.0, schema, &buffer).await
    }
}

pub async fn deserialize_segment_info(
    version: &SegmentInfoVersion,
    schema: &TableSchema,
    buffer: &[u8],
) -> Result<CompactSegmentInfo> {
    match version {
        SegmentInfoVersion::V4(_) => CompactSegmentInfo::from_slice(&buffer),
        SegmentInfoVersion::V3(_) => {
            let current: SegmentInfo = SegmentInfoV3::from_slice(&buffer)?.into();
            current.try_into()
        }
        SegmentInfoVersion::V2(v) => {
            let v2 = load_json(&buffer, v).await?;
            let current: SegmentInfo = v2.into();
            current.try_into()
        }
        SegmentInfoVersion::V1(v) => {
            let v1 = load_json(&buffer, v).await?;
            // need leaf fields info to migrate from v1
            let fields = schema.leaf_fields();
            let current: SegmentInfo = (v1, &fields[..]).into();
            current.try_into()
        }
        SegmentInfoVersion::V0(v) => {
            let v0 = load_json(&buffer, v).await?;
            // need leaf fields info to migrate from v0
            let fields = schema.leaf_fields();
            let current: SegmentInfo = (v0, &fields[..]).into();
            current.try_into()
        }
        SegmentInfoVersion::V5(_) => unimplemented!(),
    }
}
