//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::collections::HashMap;
use std::ops::Add;
use std::sync::Arc;

use databend_common_expression::TableSchema;
use databend_storages_common_table_meta::meta::testing::StatisticsV0;
use databend_storages_common_table_meta::meta::testing::TableSnapshotV1;
use databend_storages_common_table_meta::meta::testing::TableSnapshotV2;
use databend_storages_common_table_meta::meta::TableSnapshot;
use uuid::Uuid;

fn default_snapshot() -> TableSnapshot {
    let schema = TableSchema::empty();
    let stats = Default::default();
    TableSnapshot::try_new(
        None,
        None,
        schema,
        stats,
        vec![],
        None,
        None,
        Default::default(),
    )
    .unwrap()
}

#[test]
fn snapshot_timestamp_is_some() {
    let s = default_snapshot();
    assert!(s.timestamp.is_some());
}

#[test]
fn snapshot_timestamp_monotonic_increase() {
    let prev = default_snapshot();
    let schema = TableSchema::empty();
    let current = TableSnapshot::try_new(
        None,
        None,
        schema,
        Default::default(),
        vec![],
        None,
        None,
        Default::default(),
    )
    .unwrap();
    let current_ts = current.timestamp.unwrap();
    let prev_ts = prev.timestamp.unwrap();
    assert!(current_ts > prev_ts)
}

#[test]
fn snapshot_timestamp_time_skew_tolerance() {
    let mut prev = default_snapshot();
    let schema = TableSchema::empty();

    // simulating a stalled clock
    prev.timestamp = Some(prev.timestamp.unwrap().add(chrono::Duration::days(1)));

    let current = TableSnapshot::try_new(
        None,
        Some(Arc::new(prev.clone())),
        schema,
        Default::default(),
        vec![],
        None,
        None,
        Default::default(),
    )
    .unwrap();
    let current_ts = current.timestamp.unwrap();
    let prev_ts = prev.timestamp.unwrap();
    assert!(current_ts > prev_ts)
}

#[test]
fn test_snapshot_v1_to_v4() {
    let summary = StatisticsV0 {
        row_count: 0,
        block_count: 0,
        perfect_block_count: 0,
        uncompressed_byte_size: 0,
        compressed_byte_size: 0,
        index_size: 0,
        col_stats: HashMap::new(),
    };
    let v1 = TableSnapshotV1::new(
        Uuid::new_v4(),
        &None,
        None,
        Default::default(),
        summary,
        vec![],
        None,
        None,
    );
    assert!(v1.timestamp.is_some());

    let v4: TableSnapshot = TableSnapshotV2::from(v1.clone()).into();
    assert_eq!(v4.format_version, v1.format_version());
    assert_eq!(v4.snapshot_id, v1.snapshot_id);
    assert_eq!(v4.timestamp, v1.timestamp);
}

#[test]
fn test_snapshot_reader() {
    for data in [
        include_bytes!("legacy_tpch_v4.mpk").as_slice(),
        include_bytes!("c_sn_v4.mpk").as_slice(),
    ] {
        let sn = TableSnapshot::from_slice(data).unwrap();
        assert!(sn.schema.num_fields() > 0);
        assert!(!sn.segments.is_empty());
        assert_eq!(sn.summary.col_stats.len(), sn.schema.num_fields());
    }
}

#[test]
fn test_seg_reader() {
    let meta = include_bytes!("c_seg_v4.mpk");
    let seg = databend_storages_common_table_meta::meta::SegmentInfo::from_slice(meta).unwrap();
    assert!(!seg.summary.col_stats.is_empty());
    assert!(!seg.blocks.is_empty());
}
