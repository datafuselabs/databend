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

use chrono::DateTime;
use chrono::Utc;
use common_base::base::uuid::NoContext;
use common_base::base::uuid::Timestamp;
use common_base::base::uuid::Uuid;
use common_exception::Result;

pub type SnapshotId = Uuid;

pub fn new_snapshot_id_with_timestamp(timestamp: Option<DateTime<Utc>>) -> SnapshotId {
    match timestamp {
        Some(timestamp) => {
            let ts = timestamp.timestamp_micros();
            let sec = ts / 1000000;
            let ns = (ts - sec * 1000000) * 1000;
            let ts = Timestamp::from_unix(NoContext, sec as u64, ns as u32);
            Uuid::new_v7(ts)
        }
        None => Uuid::now_v7(),
    }
}

pub fn new_snapshot_id() -> SnapshotId {
    Uuid::new_v4()
}

pub fn snapshot_id_from_string(snapshot_id_str: &str) -> Result<SnapshotId> {
    Ok(Uuid::parse_str(snapshot_id_str)?)
}

pub fn time_from_snapshot_id(snapshot_id: &SnapshotId) -> Option<DateTime<Utc>> {
    let timestamp_opt = snapshot_id.get_timestamp().map(|timestamp| {
        let (sec, ns) = timestamp.to_unix();
        DateTime::<Utc>::from_timestamp(sec as i64, ns)
    });

    match timestamp_opt {
        Some(timestamp) => timestamp,
        None => None,
    }
}
