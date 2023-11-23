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

use std::io::Error;
use std::sync::Arc;
use std::vec;

use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use common_exception::Result;
use common_expression::types::NumberScalar;
use common_expression::BlockThresholds;
use common_expression::DataBlock;
use common_expression::ScalarRef;
use common_expression::SendableDataBlockStream;
use common_storages_factory::Table;
use common_storages_fuse::io::MetaWriter;
use common_storages_fuse::statistics::gen_columns_statistics;
use common_storages_fuse::statistics::merge_statistics;
use common_storages_fuse::statistics::reducers::reduce_block_metas;
use common_storages_fuse::FuseStorageFormat;
use common_storages_fuse::FuseTable;
use common_storages_fuse::FUSE_TBL_SEGMENT_PREFIX;
use futures_util::TryStreamExt;
use opendal::Operator;
use serde::Serialize;
use storages_common_table_meta::meta::new_snapshot_id;
use storages_common_table_meta::meta::testing::SegmentInfoV2;
use storages_common_table_meta::meta::testing::TableSnapshotV2;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::Location;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::Statistics;
use storages_common_table_meta::meta::TableSnapshot;
use storages_common_table_meta::meta::Versioned;
use uuid::Uuid;

use super::block_writer::BlockWriter;
use super::TestFixture;

pub async fn generate_snapshot_with_segments(
    fuse_table: &FuseTable,
    segment_locations: Vec<Location>,
    time_stamp: Option<DateTime<Utc>>,
    with_table_version: bool,
) -> Result<String> {
    let current_snapshot = fuse_table.read_table_snapshot().await?;
    let operator = fuse_table.get_operator();
    let location_gen = fuse_table.meta_location_generator();
    let table_version_option = if with_table_version {
        Some(fuse_table.current_table_version())
    } else {
        None
    };
    let mut new_snapshot = if let Some(current_snapshot) = current_snapshot {
        TableSnapshot::from_previous(current_snapshot.as_ref(), table_version_option)
    } else {
        TableSnapshot::new(
            &None,
            None,
            Some(fuse_table.current_table_version()),
            fuse_table.schema().as_ref().to_owned(),
            Statistics::default(),
            vec![],
            None,
            None,
        )
    };
    new_snapshot.segments = segment_locations;
    // if not with table version, use snapshot id without timestamp
    if !with_table_version {
        new_snapshot.snapshot_id = new_snapshot_id();
    }
    let new_snapshot_location = location_gen.gen_snapshot_location(
        &new_snapshot.snapshot_id,
        TableSnapshot::VERSION,
        new_snapshot.table_version,
    )?;
    if let Some(ts) = time_stamp {
        new_snapshot.timestamp = Some(ts)
    }

    new_snapshot
        .write_meta(&operator, &new_snapshot_location)
        .await?;

    Ok(new_snapshot_location)
}

pub async fn generate_orphan_files(
    fuse_table: &FuseTable,
    orphan_segment_number: usize,
    orphan_block_number_per_segment: usize,
) -> Result<Vec<(Location, SegmentInfo)>> {
    generate_segments(
        fuse_table,
        orphan_segment_number,
        orphan_block_number_per_segment,
    )
    .await
}

pub async fn generate_segments(
    fuse_table: &FuseTable,
    number_of_segments: usize,
    blocks_per_segment: usize,
) -> Result<Vec<(Location, SegmentInfo)>> {
    let mut segs = vec![];
    for _ in 0..number_of_segments {
        let dal = fuse_table.get_operator_ref();
        let block_metas = generate_blocks(fuse_table, blocks_per_segment).await?;
        let summary = reduce_block_metas(&block_metas, BlockThresholds::default(), None);
        let segment_info = SegmentInfo::new(block_metas, summary);
        let generator = fuse_table.meta_location_generator();
        let location = generator.gen_segment_info_location();
        write_v2_to_storage(dal, &location, &segment_info).await?;
        segs.push(((location, SegmentInfoV2::VERSION), segment_info))
    }
    Ok(segs)
}

pub async fn generate_segments_v2(
    fuse_table: &FuseTable,
    number_of_segments: usize,
    blocks_per_segment: usize,
) -> Result<Vec<(Location, SegmentInfoV2)>> {
    let mut segs = vec![];
    for _ in 0..number_of_segments {
        let dal = fuse_table.get_operator_ref();
        let block_metas = generate_blocks(fuse_table, blocks_per_segment).await?;
        let summary = reduce_block_metas(&block_metas, BlockThresholds::default(), None);
        let segment_info = SegmentInfoV2::new(block_metas, summary);
        let uuid = Uuid::new_v4();
        let location = format!(
            "{}/{}/{}_v{}.json",
            &fuse_table.meta_location_generator().prefix(),
            FUSE_TBL_SEGMENT_PREFIX,
            uuid,
            SegmentInfoV2::VERSION,
        );
        write_v2_to_storage(dal, &location, &segment_info).await?;
        segs.push(((location, SegmentInfoV2::VERSION), segment_info))
    }
    Ok(segs)
}

async fn generate_blocks(fuse_table: &FuseTable, num_blocks: usize) -> Result<Vec<Arc<BlockMeta>>> {
    let dal = fuse_table.get_operator_ref();
    let schema = fuse_table.schema();
    let block_writer = BlockWriter::new(dal, fuse_table.meta_location_generator());
    let mut block_metas = vec![];

    // does not matter in this suite
    let rows_per_block = 1;
    let value_start_from = 1;

    let stream =
        TestFixture::gen_sample_blocks_stream_ex(num_blocks, rows_per_block, value_start_from);

    let blocks: std::vec::Vec<DataBlock> = stream.try_collect().await?;
    for block in blocks {
        let stats = gen_columns_statistics(&block, None, &schema)?;
        let (block_meta, _index_meta) = block_writer
            .write(FuseStorageFormat::Parquet, &schema, block, stats, None)
            .await?;
        block_metas.push(Arc::new(block_meta));
    }
    Ok(block_metas)
}

pub async fn generate_snapshots(fixture: &TestFixture) -> Result<()> {
    let now = Utc::now();
    let schema = TestFixture::default_table_schema();

    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let location_gen = fuse_table.meta_location_generator();
    let operator = fuse_table.get_operator();

    // generate 1 v2 segments, 2 blocks.
    let segments_v2 = generate_segments_v2(fuse_table, 1, 2).await?;

    // create snapshot 0, the format version is 2.
    let locations = vec![segments_v2[0].0.clone()];
    let id = Uuid::new_v4();
    let mut snapshot_0 = TableSnapshotV2::new(
        id,
        &None,
        None,
        schema.as_ref().clone(),
        segments_v2[0].1.summary.clone(),
        locations,
        None,
        None,
    );
    snapshot_0.timestamp = Some(now - Duration::hours(13));

    let new_snapshot_location = location_gen.gen_snapshot_location(
        &snapshot_0.snapshot_id,
        TableSnapshotV2::VERSION,
        None,
    )?;
    write_v2_to_storage(&operator, &new_snapshot_location, &snapshot_0).await?;

    // generate 2 segments, 4 blocks.
    let num_of_segments = 2;
    let blocks_per_segment = 2;
    let segments_v3 = generate_segments(fuse_table, num_of_segments, blocks_per_segment).await?;

    // create snapshot 1, the format version is 3.
    let locations = vec![segments_v3[0].0.clone(), segments_v2[0].0.clone()];
    let mut snapshot_1 = TableSnapshot::new(
        &snapshot_0.timestamp,
        Some((
            snapshot_0.snapshot_id,
            TableSnapshotV2::VERSION,
            Some(fuse_table.current_table_version()),
        )),
        Some(fuse_table.current_table_version() + 1),
        schema.as_ref().clone(),
        Statistics::default(),
        locations,
        None,
        None,
    );
    snapshot_1.timestamp = Some(now - Duration::hours(12));
    snapshot_1.summary = merge_statistics(&snapshot_0.summary, &segments_v3[0].1.summary, None);
    let new_snapshot_location = location_gen.gen_snapshot_location(
        &snapshot_1.snapshot_id,
        TableSnapshot::VERSION,
        snapshot_1.table_version,
    )?;
    snapshot_1
        .write_meta(&operator, &new_snapshot_location)
        .await?;

    // create snapshot 2, the format version is 3.
    let locations = vec![
        segments_v3[1].0.clone(),
        segments_v3[0].0.clone(),
        segments_v2[0].0.clone(),
    ];
    let mut snapshot_2 =
        TableSnapshot::from_previous(&snapshot_1, Some(fuse_table.current_table_version()));
    snapshot_2.segments = locations;
    snapshot_2.timestamp = Some(now);
    snapshot_2.summary = merge_statistics(&snapshot_1.summary, &segments_v3[1].1.summary, None);
    let new_snapshot_location = location_gen.gen_snapshot_location(
        &snapshot_2.snapshot_id,
        TableSnapshot::VERSION,
        snapshot_2.table_version,
    )?;
    snapshot_2
        .write_meta(&operator, &new_snapshot_location)
        .await?;
    fuse_table
        .commit_to_meta_server(
            fixture.new_query_ctx().await?.as_ref(),
            fuse_table.get_table_info(),
            snapshot_2,
            None,
            &None,
        )
        .await
}

async fn write_v2_to_storage<T>(data_accessor: &Operator, location: &str, meta: &T) -> Result<()>
where T: Serialize {
    let bs = serde_json::to_vec(&meta).map_err(Error::other)?;
    data_accessor.write(location, bs).await?;
    Ok(())
}

pub async fn query_count(result_stream: SendableDataBlockStream) -> Result<u64> {
    let blocks: Vec<DataBlock> = result_stream.try_collect().await?;
    let mut count: u64 = 0;
    for block in blocks {
        let value = &block.get_by_offset(0).value;
        let value = value.as_ref();
        let value = unsafe { value.index_unchecked(0) };
        if let ScalarRef::Number(NumberScalar::UInt64(v)) = value {
            count += v;
        }
    }
    Ok(count)
}
