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

use std::ops::Add;
use std::path::Path;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Datelike;
use chrono::TimeZone;
use chrono::Timelike;
use chrono::Utc;
use databend_common_base::base::uuid;
use databend_common_base::base::uuid::NoContext;
use databend_common_base::base::uuid::Uuid;
use databend_common_exception::ErrorCode;

use crate::meta::TableSnapshot;
use crate::readers::snapshot_reader::TableSnapshotAccessor;

pub const V5_OBJECT_KEY_PREFIX: char = 'g';
pub fn trim_timestamp_to_micro_second(ts: DateTime<Utc>) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(
        ts.year(),
        ts.month(),
        ts.day(),
        ts.hour(),
        ts.minute(),
        ts.second(),
    )
    .unwrap()
    .with_nanosecond(ts.timestamp_subsec_micros() * 1_000)
    .unwrap()
}

pub fn monotonically_increased_timestamp(
    timestamp: DateTime<Utc>,
    previous_timestamp: &Option<DateTime<Utc>>,
) -> DateTime<Utc> {
    if let Some(prev_instant) = previous_timestamp {
        // timestamp of the snapshot should always larger than the previous one's
        if prev_instant > &timestamp {
            // if local time is smaller, use the timestamp of previous snapshot, plus 1 ms
            return prev_instant.add(chrono::Duration::milliseconds(1));
        }
    }
    timestamp
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Copy)]
pub struct TableMetaTimestamps {
    pub base_timestamp: chrono::DateTime<chrono::Utc>,
    pub snapshot_lvt: chrono::DateTime<chrono::Utc>,
    pub snapshot_timestamp: chrono::DateTime<chrono::Utc>,
}

impl TableMetaTimestamps {
    pub fn new(
        previous_snapshot: Option<Arc<TableSnapshot>>,
        data_retention_time_in_days: i64,
    ) -> Self {
        let now = chrono::Utc::now();
        let snapshot_timestamp =
            monotonically_increased_timestamp(now, &previous_snapshot.timestamp());
        let snapshot_timestamp = trim_timestamp_to_micro_second(snapshot_timestamp);
        let snapshot_lvt_candidate =
            snapshot_timestamp - chrono::Duration::days(data_retention_time_in_days);
        let snapshot_lvt = monotonically_increased_timestamp(
            snapshot_lvt_candidate,
            &previous_snapshot.least_visible_timestamp(),
        );
        let base_timestamp = now.max(snapshot_lvt);
        Self {
            base_timestamp,
            snapshot_lvt,
            snapshot_timestamp,
        }
    }
}

/// used in ut
impl Default for TableMetaTimestamps {
    fn default() -> Self {
        Self::new(None, 1)
    }
}

pub fn uuid_from_date_time(ts: DateTime<Utc>) -> Uuid {
    // only in this range, the order of timestamps is preserved in UUIDs
    // out of range is unlikely to happen, this is just a safe guard
    let range = 0..=0xFFFF_FFFF_FFFF;
    assert!(range.contains(&ts.timestamp_millis()));
    let seconds = ts.timestamp();
    let nanos = ts.timestamp_subsec_nanos();
    let uuid_ts = uuid::Timestamp::from_unix(NoContext, seconds as u64, nanos);
    Uuid::new_v7(uuid_ts)
}

// Extracts the UUID part from the object key.
// For example, given a path like:
//   bucket/root/115/122/_b/g0191114d30fd78b89fae8e5c88327725_v2.parquet
//   bucket/root/115/122/_b/0191114d30fd78b89fae8e5c88327725_v2.parquet
// The function should return: 0191114d30fd78b89fae8e5c88327725
pub fn try_extract_uuid_str_from_path(path: &str) -> databend_common_exception::Result<&str> {
    if let Some(file_stem) = Path::new(path).file_stem() {
        let file_name = file_stem
            .to_str()
            .unwrap() // path is always valid utf8 string
            .split('_')
            .collect::<Vec<&str>>();
        let uuid = trim_v5_object_prefix(file_name[0]);
        Ok(uuid)
    } else {
        Err(ErrorCode::StorageOther(format!(
            "Illegal object key, no file stem found: {}",
            path
        )))
    }
}

#[inline]
pub fn trim_v5_object_prefix(key: &str) -> &str {
    key.strip_prefix(V5_OBJECT_KEY_PREFIX).unwrap_or(key)
}

#[cfg(test)]
mod tests {
    fn assert_order_preserved(
        ts1: chrono::DateTime<chrono::Utc>,
        ts2: chrono::DateTime<chrono::Utc>,
    ) {
        let uuid1 = super::uuid_from_date_time(ts1);
        let uuid2 = super::uuid_from_date_time(ts2);
        assert_eq!(ts1.cmp(&ts2), uuid1.cmp(&uuid2));
    }

    #[test]
    fn test_uuid_from_date_time() {
        let now = chrono::Utc::now();
        assert_order_preserved(now, now + chrono::Duration::milliseconds(1));
        assert_order_preserved(now, now - chrono::Duration::milliseconds(1));
        assert_order_preserved(now, chrono::DateTime::default());

        let ms = 0xFFFF_FFFF_FFFF;
        let ts = chrono::DateTime::from_timestamp_millis(ms).unwrap();
        assert_order_preserved(now, ts);
        assert_order_preserved(ts - chrono::Duration::milliseconds(1), ts);
    }
}
