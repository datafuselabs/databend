// Copyright 2022 Datafuse Labs.
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

use chrono::Duration;
use chrono::Utc;
use databend_common_base::base::tokio;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_storages_fuse::io::MetaWriter;
use databend_common_storages_fuse::FuseTable;
use databend_query::test_kits::*;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::Versioned;
use uuid::Uuid;

use crate::storages::fuse::operations::mutation::compact_segment;

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_purge_normal_case() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    fixture.create_default_table().await?;

    let ctx = fixture.new_query_ctx().await?;

    // ingests some test data
    append_sample_data(1, &fixture).await?;

    // do_gc
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let snapshot_files = fuse_table.list_snapshot_files().await?;
    let keep_last_snapshot = true;
    let table_ctx: Arc<dyn TableContext> = ctx.clone();
    fuse_table
        .do_purge(&table_ctx, snapshot_files, None, keep_last_snapshot, false)
        .await?;

    let expected_num_of_snapshot = 1;
    check_data_dir(
        &fixture,
        "do_gc: there should be 1 snapshot, 0 segment/block",
        expected_num_of_snapshot,
        0, // 0 snapshot statistic
        1, // 1 segments
        1, // 1 blocks
        1, // 1 index
        Some(()),
        None,
    )
    .await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_purge_normal_orphan_snapshot() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    fixture.create_default_table().await?;

    let ctx = fixture.new_query_ctx().await?;

    // ingests some test data
    append_sample_data(1, &fixture).await?;

    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;

    // create orphan snapshot, its timestamp is larger than the current one
    {
        let current_snapshot = fuse_table.read_table_snapshot().await?.unwrap();
        let operator = fuse_table.get_operator();
        let location_gen = fuse_table.meta_location_generator();
        let orphan_snapshot_id = Uuid::new_v4();
        let orphan_snapshot_location = location_gen
            .snapshot_location_from_uuid(&orphan_snapshot_id, TableSnapshot::VERSION)?;
        // orphan_snapshot is created by using `from_previous`, which guarantees
        // that the timestamp of snapshot returned is larger than `current_snapshot`'s.
        let orphan_snapshot =
            TableSnapshot::try_from_previous(current_snapshot.clone(), None, Default::default())?;
        orphan_snapshot
            .write_meta(&operator, &orphan_snapshot_location)
            .await?;
    }

    // do_gc
    let keep_last_snapshot = true;
    let table_ctx: Arc<dyn TableContext> = ctx.clone();
    let snapshot_files = fuse_table.list_snapshot_files().await?;
    fuse_table
        .do_purge(&table_ctx, snapshot_files, None, keep_last_snapshot, false)
        .await?;

    // expects two snapshot there
    // - one snapshot of the latest version
    // - one orphan snapshot which timestamp is larger then the latest snapshot's
    let expected_num_of_snapshot = 2;
    check_data_dir(
        &fixture,
        "do_gc: there should be 1 snapshot, 0 segment/block",
        expected_num_of_snapshot,
        0, // 0 snapshot statistic
        1, // 0 segments
        1, // 0 blocks
        1, // 0 index
        Some(()),
        None,
    )
    .await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_purge_orphan_retention() -> Result<()> {
    // verifies that:
    //
    // - snapshots that beyond retention period shall be collected, but
    // - if segments are referenced by snapshot within retention period,
    //   they shall not be collected during purge.
    //   the blocks referenced by those segments, shall not be collected as well.
    //
    // for example :
    //
    //   ──┬──
    //     │
    //  within retention
    //     │
    //     │             S_2 ────────────────► seg_2 ──────────────► block_2
    //   ──┴──
    //  beyond retention S_current───────────► seg_1 seg_c ──────────────► block_1 block_c
    //     │
    //     │             S_1 ────────────────► seg_1 ──────────────► block_1
    //     │
    //     │             S_0 ────────────────► seg_0 ──────────────► block_0
    //
    // - S_current is the gc root
    // - S_1 is S_current's precedent
    // - S_2, S_0 are orphan snapshots in S_current's point of view
    //   each of them is not a number of  S_current's precedents
    //
    // - S_2 should NOT be purged
    //   since it is within the retention period
    //    - seg_2 shall NOT be purged, since it is referenced by S_2.
    //    - block_2 shall NOT be purged , since it is referenced by seg_2
    //
    // - S_current, seg_1, seg_c, block_1 and block_c shall NOT be purged
    //   since they are referenced by the current table snapshot
    //
    // - S_1 should be purged
    //
    // - S_1 should be purged, since it is beyond the retention period
    //    - seg_1 and block_1 shall be purged

    // - S_0 should be purged, since it is beyond the retention period
    //    - seg_0 and block_0 shall be purged
    //
    //  put them together, after GC, there will be
    //  - 2 snapshots left: s_current, s_2
    //  - 3 segments left: seg_c, seg_2, seg_1
    //  - 3 blocks left: block_c, block_2, block_1

    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    fixture.create_default_table().await?;

    let ctx = fixture.new_query_ctx().await?;

    // 1. prepare `S_1`
    let number_of_block = 1;
    append_sample_data(number_of_block, &fixture).await?;
    // no we have 1 snapshot, 1 segment, 1 blocks

    // 2. prepare `s_current`
    append_sample_data(1, &fixture).await?;
    // no we have 2 snapshot, 2 segment, 2 blocks
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let base_snapshot = fuse_table.read_table_snapshot().await?.unwrap();
    let base_timestamp = base_snapshot.timestamp.unwrap();

    // 2. prepare `seg_2`
    let num_of_segments = 1;
    let blocks_per_segment = 1;
    let segments = generate_segments(
        fuse_table,
        num_of_segments,
        blocks_per_segment,
        false,
        Default::default(),
    )
    .await?;
    let (segment_locations, _segment_info): (Vec<_>, Vec<_>) = segments.into_iter().unzip();

    // 2. prepare S_2
    let new_timestamp = base_timestamp + Duration::minutes(1);
    let _snapshot_location =
        generate_snapshot_with_segments(fuse_table, segment_locations.clone(), Some(new_timestamp))
            .await?;

    // 2. prepare S_0
    {
        let num_of_segments = 1;
        let blocks_per_segment = 1;
        let segments = generate_segments(
            fuse_table,
            num_of_segments,
            blocks_per_segment,
            false,
            Default::default(),
        )
        .await?;
        let segment_locations: Vec<Location> = segments.into_iter().map(|(l, _)| l).collect();
        let new_timestamp = base_timestamp - Duration::days(1);
        let _snapshot_location = generate_snapshot_with_segments(
            fuse_table,
            segment_locations.clone(),
            Some(new_timestamp),
        )
        .await?;
    }

    // do_gc
    let keep_last_snapshot = true;
    let table_ctx: Arc<dyn TableContext> = ctx.clone();
    let snapshot_files = fuse_table.list_snapshot_files().await?;
    fuse_table
        .do_purge(&table_ctx, snapshot_files, None, keep_last_snapshot, false)
        .await?;

    let expected_num_of_snapshot = 2;
    let expected_num_of_segment = 3;
    let expected_num_of_blocks = 3;
    let expected_num_of_index = expected_num_of_blocks;
    check_data_dir(
        &fixture,
        "do_gc: verify retention period",
        expected_num_of_snapshot,
        0,
        expected_num_of_segment,
        expected_num_of_blocks,
        expected_num_of_index,
        Some(()),
        None,
    )
    .await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_purge_older_version() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    fixture.create_normal_table().await?;

    generate_snapshots(&fixture).await?;
    let ctx = fixture.new_query_ctx().await?;
    let table_ctx: Arc<dyn TableContext> = ctx.clone();
    let now = Utc::now();

    // navigate to time point, snapshot 0 is purged.
    {
        let latest_table = fixture.latest_default_table().await?;
        let fuse_table = FuseTable::try_from_table(latest_table.as_ref())?;
        let snapshot_files = fuse_table.list_snapshot_files().await?;
        let time_point = now - Duration::hours(12);
        let snapshot_loc = fuse_table.snapshot_loc().await?.unwrap();
        let table = fuse_table
            .navigate_to_time_point(snapshot_loc, time_point, ctx.clone().get_abort_checker())
            .await?;
        let keep_last_snapshot = true;
        table
            .do_purge(&table_ctx, snapshot_files, None, keep_last_snapshot, false)
            .await?;

        let expected_num_of_snapshot = 2;
        let expected_num_of_segment = 3;
        let expected_num_of_blocks = 6;
        let expected_num_of_index = expected_num_of_blocks;
        check_data_dir(
            &fixture,
            "do_gc: navigate to time point",
            expected_num_of_snapshot,
            0,
            expected_num_of_segment,
            expected_num_of_blocks,
            expected_num_of_index,
            Some(()),
            None,
        )
        .await?;
    }

    // ingests some test data
    append_sample_data(1, &fixture).await?;

    // Do compact segment, generate a new snapshot.
    {
        let table = fixture.latest_default_table().await?;
        compact_segment(ctx.clone(), &table).await?;
        check_data_dir(&fixture, "", 4, 0, 5, 7, 7, Some(()), None).await?;
    }

    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    // base snapshot is root. do purge.
    {
        let snapshot_files = fuse_table.list_snapshot_files().await?;
        fuse_table
            .do_purge(&table_ctx, snapshot_files, None, true, false)
            .await?;

        let expected_num_of_snapshot = 1;
        let expected_num_of_segment = 1;
        let expected_num_of_blocks = 7;
        let expected_num_of_index = expected_num_of_blocks;
        check_data_dir(
            &fixture,
            "do_gc: with older version",
            expected_num_of_snapshot,
            0,
            expected_num_of_segment,
            expected_num_of_blocks,
            expected_num_of_index,
            Some(()),
            None,
        )
        .await?;
    }

    // keep_last_snapshot is false. All of snapshots will be purged.
    {
        let snapshot_files = fuse_table.list_snapshot_files().await?;
        fuse_table
            .do_purge(&table_ctx, snapshot_files, None, false, false)
            .await?;
        let expected_num_of_snapshot = 0;
        let expected_num_of_segment = 0;
        let expected_num_of_blocks = 0;
        let expected_num_of_index = expected_num_of_blocks;
        check_data_dir(
            &fixture,
            "do_gc: purge last snapshot",
            expected_num_of_snapshot,
            0,
            expected_num_of_segment,
            expected_num_of_blocks,
            expected_num_of_index,
            Some(()),
            None,
        )
        .await?;
    }

    Ok(())
}
