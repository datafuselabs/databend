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

use common_exception::Result;
use futures::AsyncRead;
use futures_util::AsyncReadExt;

use crate::meta::load_json;
use crate::meta::TableSnapshotStatistics;
use crate::meta::TableSnapshotStatisticsVersion;

#[async_trait::async_trait]
pub trait VersionedReader<T> {
    type TargetType;
    async fn read<R>(&self, read: R) -> Result<Self::TargetType>
    where R: AsyncRead + Unpin + Send;
}

#[async_trait::async_trait]
impl VersionedReader<TableSnapshotStatistics> for TableSnapshotStatisticsVersion {
    type TargetType = TableSnapshotStatistics;
    #[async_backtrace::framed]
    async fn read<R>(&self, mut reader: R) -> Result<TableSnapshotStatistics>
    where R: AsyncRead + Unpin + Send {
        let mut buffer: Vec<u8> = vec![];
        reader.read_to_end(&mut buffer).await?;
        let r = match self {
            TableSnapshotStatisticsVersion::V0(v) => load_json(&buffer, v).await?.into(),
            TableSnapshotStatisticsVersion::V1(v) => load_json(&buffer, v).await?,
        };
        Ok(r)
    }
}
