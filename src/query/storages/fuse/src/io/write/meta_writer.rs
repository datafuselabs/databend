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
use databend_storages_common_cache::CacheAccessor;
use databend_storages_common_cache::CachedObject;
use databend_storages_common_table_meta::meta::CompactSegmentInfo;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::TableSnapshotStatistics;
use databend_storages_common_table_meta::meta::Versioned;
use opendal::Operator;

#[async_trait::async_trait]
pub trait MetaWriter<T> {
    /// If meta has a `to_bytes` function, such as `SegmentInfo` and `TableSnapshot`
    /// We should not use `write_meta`. Instead, use `write_meta_data`
    async fn write_meta(&self, data_accessor: &Operator, location: &str) -> Result<()>;
}

#[async_trait::async_trait]
impl<T> MetaWriter<T> for T
where T: Marshal + Sync + Send
{
    #[async_backtrace::framed]
    async fn write_meta(&self, data_accessor: &Operator, location: &str) -> Result<()> {
        data_accessor.write(location, self.marshal()?).await?;
        Ok(())
    }
}

#[async_trait::async_trait]
pub trait CachedMetaWriter<T> {
    /// If meta has a `to_bytes` function, such as `SegmentInfo` and `TableSnapshot`
    /// We should not use `write_meta_through_cache`. Instead, use `write_meta_data_through_cache`
    async fn write_meta_through_cache(self, data_accessor: &Operator, location: &str)
    -> Result<()>;
}

#[async_trait::async_trait]
impl CachedMetaWriter<SegmentInfo> for SegmentInfo {
    #[async_backtrace::framed]
    async fn write_meta_through_cache(
        self,
        data_accessor: &Operator,
        location: &str,
    ) -> Result<()> {
        let bytes = self.marshal()?;
        data_accessor.write(location, bytes.clone()).await?;
        if let Some(cache) = CompactSegmentInfo::cache() {
            cache.insert(location.to_owned(), CompactSegmentInfo::try_from(&self)?);
        }
        Ok(())
    }
}

trait Marshal {
    fn marshal(&self) -> Result<Vec<u8>>;
}

impl Marshal for SegmentInfo {
    fn marshal(&self) -> Result<Vec<u8>> {
        // make sure the table meta we write down to object store always has the current version
        // can we expressed as type constraint?
        assert_eq!(self.format_version, SegmentInfo::VERSION);
        self.to_bytes()
    }
}

impl Marshal for TableSnapshot {
    fn marshal(&self) -> Result<Vec<u8>> {
        // make sure the table meta we write down to object store always has the current version
        // can not by expressed as type constraint yet.
        assert_eq!(self.format_version, TableSnapshot::VERSION);
        self.to_bytes()
    }
}

impl Marshal for TableSnapshotStatistics {
    fn marshal(&self) -> Result<Vec<u8>> {
        // make sure the table meta we write down to object store always has the current version
        // can we expressed as type constraint?
        assert_eq!(self.format_version, TableSnapshotStatistics::VERSION);
        let bytes = serde_json::to_vec(self)?;
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {

    use databend_common_base::runtime::catch_unwind;
    use databend_common_expression::TableSchema;
    use databend_storages_common_table_meta::meta::Statistics;

    use super::*;

    #[test]
    fn test_segment_format_version_validation() {
        // old versions are not allowed (runtime panics)
        for v in 0..SegmentInfo::VERSION {
            let r = catch_unwind(|| {
                let mut segment = SegmentInfo::new(vec![], Statistics::default());
                segment.format_version = v;
                let _ = segment.marshal();
            });
            assert!(r.is_err())
        }

        // current version allowed
        let segment = SegmentInfo::new(vec![], Statistics::default());
        segment.marshal().unwrap();
    }

    #[test]
    fn test_snapshot_format_version_validation() {
        // old versions are not allowed (runtime panics)
        for v in 0..TableSnapshot::VERSION {
            let r = catch_unwind(|| {
                let mut snapshot = TableSnapshot::try_new(
                    None,
                    None,
                    TableSchema::default(),
                    Statistics::default(),
                    vec![],
                    None,
                    None,
                    Default::default(),
                )
                .unwrap();
                snapshot.format_version = v;
                let _ = snapshot.marshal();
            });
            assert!(r.is_err())
        }

        // current version allowed
        let snapshot = TableSnapshot::try_new(
            None,
            None,
            TableSchema::default(),
            Statistics::default(),
            vec![],
            None,
            None,
            Default::default(),
        )
        .unwrap();
        snapshot.marshal().unwrap();
    }
}
