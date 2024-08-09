//  Copyright 2021 Datafuse Labs.
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
use std::sync::Arc;
use std::vec;

use databend_common_expression::TableSchema;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_storages_fuse::operations::ConflictResolveContext;
use databend_common_storages_fuse::operations::MutationGenerator;
use databend_common_storages_fuse::operations::SnapshotChanges;
use databend_common_storages_fuse::operations::SnapshotGenerator;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_txn::TxnManager;

/// base snapshot contains segments 1, 2, 3,
///
/// a delete operation wants to remove segment 2,
///
/// but it finds that the latest snapshot does not contain segment 2, which means segment 2 has been modified by other operations
///
/// i.e. in this test, segment 2 and 3 are compacted into segment 4
///
/// so the delete operation cannot be applied
#[test]
fn test_unresolvable_delete_conflict() {
    let mut base_snapshot = TableSnapshot::new_empty_snapshot(TableSchema::default(), None);
    base_snapshot.segments = vec![
        ("1".to_string(), 1),
        ("2".to_string(), 1),
        ("3".to_string(), 1),
    ];

    let mut latest_snapshot = TableSnapshot::new_empty_snapshot(TableSchema::default(), None);
    latest_snapshot.segments = vec![("1".to_string(), 1), ("4".to_string(), 1)];

    let ctx = ConflictResolveContext::ModifiedSegmentExistsInLatest(SnapshotChanges {
        appended_segments: vec![],
        replaced_segments: HashMap::new(),
        removed_segment_indexes: vec![1],
        removed_statistics: Statistics::default(),
        merged_statistics: Statistics::default(),
    });

    let mut generator = MutationGenerator::new(Some(Arc::new(base_snapshot)), MutationKind::Delete);
    generator.set_conflict_resolve_context(ctx);

    let result = generator.generate_new_snapshot(
        TableSchema::default(),
        None,
        Some(Arc::new(latest_snapshot)),
        None,
        TxnManager::init(),
        0,
        Default::default(),
    );
    assert!(result.is_err());
}

#[test]
/// base snapshot contains segments 1, 2, 3,
///
/// a delete operation wants to remove segment 2, and replace segment 3 with segment 8
///
/// the latest snapshot contains segments 2, 3, 4
///
/// the segments 2, 3 are still in the latest snapshot, so the delete operation can be applied
///
/// the delete operation is merged into the latest snapshot, by removing segments 2, 3, and adding segment 8 in the latest snapshot
fn test_resolvable_delete_conflict() {
    let mut base_snapshot = TableSnapshot::new_empty_snapshot(TableSchema::default(), None);
    base_snapshot.segments = vec![
        ("1".to_string(), 1),
        ("2".to_string(), 1),
        ("3".to_string(), 1),
    ];

    base_snapshot.summary = Statistics {
        row_count: 6,
        block_count: 6,
        perfect_block_count: 6,
        uncompressed_byte_size: 6,
        compressed_byte_size: 6,
        index_size: 6,
        col_stats: HashMap::new(),
        cluster_stats: None,
    };

    let mut latest_snapshot = TableSnapshot::new_empty_snapshot(TableSchema::default(), None);
    latest_snapshot.segments = vec![
        ("2".to_string(), 1),
        ("3".to_string(), 1),
        ("4".to_string(), 1),
    ];

    latest_snapshot.summary = Statistics {
        row_count: 9,
        block_count: 9,
        perfect_block_count: 9,
        uncompressed_byte_size: 9,
        compressed_byte_size: 9,
        index_size: 9,
        col_stats: HashMap::new(),
        cluster_stats: None,
    };

    let removed_statistics = Statistics {
        row_count: 5,
        block_count: 5,
        perfect_block_count: 5,
        uncompressed_byte_size: 5,
        compressed_byte_size: 5,
        index_size: 5,
        col_stats: HashMap::new(),
        cluster_stats: None,
    };

    let merged_statistics = Statistics {
        row_count: 8,
        block_count: 8,
        perfect_block_count: 8,
        uncompressed_byte_size: 8,
        compressed_byte_size: 8,
        index_size: 8,
        col_stats: HashMap::new(),
        cluster_stats: None,
    };

    let ctx = ConflictResolveContext::ModifiedSegmentExistsInLatest(SnapshotChanges {
        appended_segments: vec![],
        replaced_segments: HashMap::from([(2, ("8".to_string(), 1))]),
        removed_segment_indexes: vec![1],
        removed_statistics,
        merged_statistics,
    });

    let mut generator = MutationGenerator::new(Some(Arc::new(base_snapshot)), MutationKind::Delete);
    generator.set_conflict_resolve_context(ctx);

    let result = generator.generate_new_snapshot(
        TableSchema::default(),
        None,
        Some(Arc::new(latest_snapshot)),
        None,
        TxnManager::init(),
        0,
        Default::default(),
    );
    let snapshot = result.unwrap();
    let expected = vec![("8".to_string(), 1), ("4".to_string(), 1)];
    assert_eq!(snapshot.segments, expected);

    let actual = snapshot.summary;
    let expected = Statistics {
        row_count: 12,
        block_count: 12,
        perfect_block_count: 12,
        uncompressed_byte_size: 12,
        compressed_byte_size: 12,
        index_size: 12,
        col_stats: HashMap::new(),
        cluster_stats: None,
    };
    assert_eq!(actual, expected);
}

#[test]
/// base snapshot contains segments 1, 2, 3,
///
/// a replace operation wants to remove segment 2, and replace segment 3 with segment 5, and append segment 6
///
/// the latest snapshot contains segments 2, 3, 4
///
/// the segments 2, 3 are still in the latest snapshot, so the replace operation can be applied
///
/// the replace operation is merged into the latest snapshot, by removing segments 2, 3, and adding segment 6,5 in the latest snapshot
fn test_resolvable_replace_conflict() {
    let mut base_snapshot = TableSnapshot::new_empty_snapshot(TableSchema::default(), None);
    base_snapshot.segments = vec![
        ("1".to_string(), 1),
        ("2".to_string(), 1),
        ("3".to_string(), 1),
    ];

    base_snapshot.summary = Statistics {
        row_count: 6,
        block_count: 6,
        perfect_block_count: 6,
        uncompressed_byte_size: 6,
        compressed_byte_size: 6,
        index_size: 6,
        col_stats: HashMap::new(),
        cluster_stats: None,
    };

    let mut latest_snapshot = TableSnapshot::new_empty_snapshot(TableSchema::default(), None);
    latest_snapshot.segments = vec![
        ("2".to_string(), 1),
        ("3".to_string(), 1),
        ("4".to_string(), 1),
    ];

    latest_snapshot.summary = Statistics {
        row_count: 9,
        block_count: 9,
        perfect_block_count: 9,
        uncompressed_byte_size: 9,
        compressed_byte_size: 9,
        index_size: 9,
        col_stats: HashMap::new(),
        cluster_stats: None,
    };

    let removed_statistics = Statistics {
        row_count: 5,
        block_count: 5,
        perfect_block_count: 5,
        uncompressed_byte_size: 5,
        compressed_byte_size: 5,
        index_size: 5,
        col_stats: HashMap::new(),
        cluster_stats: None,
    };

    let merged_statistics = Statistics {
        row_count: 8,
        block_count: 8,
        perfect_block_count: 8,
        uncompressed_byte_size: 8,
        compressed_byte_size: 8,
        index_size: 8,
        col_stats: HashMap::new(),
        cluster_stats: None,
    };

    let ctx = ConflictResolveContext::ModifiedSegmentExistsInLatest(SnapshotChanges {
        appended_segments: vec![("6".to_string(), 1)],
        replaced_segments: HashMap::from([(2, ("5".to_string(), 1))]),
        removed_segment_indexes: vec![1],
        removed_statistics,
        merged_statistics,
    });

    let mut generator =
        MutationGenerator::new(Some(Arc::new(base_snapshot)), MutationKind::Replace);
    generator.set_conflict_resolve_context(ctx);

    let result = generator.generate_new_snapshot(
        TableSchema::default(),
        None,
        Some(Arc::new(latest_snapshot)),
        None,
        TxnManager::init(),
        0,
        Default::default(),
    );
    let snapshot = result.unwrap();
    let expected = vec![
        ("6".to_string(), 1),
        ("5".to_string(), 1),
        ("4".to_string(), 1),
    ];
    assert_eq!(snapshot.segments, expected);

    let actual = snapshot.summary;
    let expected = Statistics {
        row_count: 12,
        block_count: 12,
        perfect_block_count: 12,
        uncompressed_byte_size: 12,
        compressed_byte_size: 12,
        index_size: 12,
        col_stats: HashMap::new(),
        cluster_stats: None,
    };
    assert_eq!(actual, expected);
}
