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
use databend_common_exception::Result;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::BlockThresholds;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::ScalarRef;
use databend_common_expression::SendableDataBlockStream;
use databend_common_sql::optimizer::SExpr;
use databend_common_storages_factory::Table;
use databend_common_storages_fuse::io::MetaWriter;
use databend_common_storages_fuse::statistics::gen_columns_statistics;
use databend_common_storages_fuse::statistics::merge_statistics;
use databend_common_storages_fuse::statistics::reducers::reduce_block_metas;
use databend_common_storages_fuse::FuseStorageFormat;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::FUSE_TBL_SEGMENT_PREFIX;
use databend_storages_common_table_meta::meta::testing::SegmentInfoV2;
use databend_storages_common_table_meta::meta::testing::TableSnapshotV2;
use databend_storages_common_table_meta::meta::testing::TableSnapshotV4;
use databend_storages_common_table_meta::meta::testing::TableSnapshotV5;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::Versioned;
use futures_util::TryStreamExt;
use opendal::Operator;
use serde::Serialize;
use uuid::Uuid;

use super::block_writer::BlockWriter;
use super::old_version_generator;
use super::TestFixture;
use crate::interpreters::Interpreter;
use crate::interpreters::MutationInterpreter;
use crate::sessions::QueryContext;

/// This file contains some helper functions for testing fuse table.

pub async fn generate_snapshot_with_segments(
    fuse_table: &FuseTable,
    segment_locations: Vec<Location>,
    time_stamp: Option<DateTime<Utc>>,
) -> Result<String> {
    let current_snapshot = fuse_table.read_table_snapshot().await?.unwrap();
    let operator = fuse_table.get_operator();
    let location_gen = fuse_table.meta_location_generator();
    let mut new_snapshot = TableSnapshot::try_from_previous(
        current_snapshot,
        Some(fuse_table.get_table_info().ident.seq),
        Default::default(),
    )?;
    new_snapshot.segments = segment_locations;
    let new_snapshot_location = location_gen
        .snapshot_location_from_uuid(&new_snapshot.snapshot_id, TableSnapshot::VERSION)?;
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
) -> Result<Vec<(Location, SegmentInfoV2)>> {
    generate_segments_v2(
        fuse_table,
        orphan_segment_number,
        orphan_block_number_per_segment,
    )
    .await
}

pub async fn generate_segments_v2(
    fuse_table: &FuseTable,
    number_of_segments: usize,
    blocks_per_segment: usize,
) -> Result<Vec<(Location, SegmentInfoV2)>> {
    let mut segs = vec![];
    for _ in 0..number_of_segments {
        let dal = fuse_table.get_operator_ref();
        let block_metas =
            generate_blocks(fuse_table, blocks_per_segment, false, Default::default()).await?;
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

pub async fn generate_segments(
    fuse_table: &FuseTable,
    number_of_segments: usize,
    blocks_per_segment: usize,
    is_greater_than_v5: bool,
    table_meta_timestamps: TableMetaTimestamps,
) -> Result<Vec<(Location, SegmentInfo)>> {
    let mut segs = vec![];
    let location_generator = fuse_table.meta_location_generator();
    for _ in 0..number_of_segments {
        let dal = fuse_table.get_operator_ref();
        let block_metas = generate_blocks(
            fuse_table,
            blocks_per_segment,
            is_greater_than_v5,
            table_meta_timestamps,
        )
        .await?;
        let summary = reduce_block_metas(&block_metas, BlockThresholds::default(), None);
        let segment_info = SegmentInfo::new(block_metas, summary);
        let location = if is_greater_than_v5 {
            location_generator.gen_segment_info_location(table_meta_timestamps)
        } else {
            let location_generator = old_version_generator::TableMetaLocationGenerator::with_prefix(
                location_generator.prefix().to_string(),
            );
            location_generator.gen_segment_info_location(table_meta_timestamps)
        };
        segment_info.write_meta(dal, location.as_str()).await?;
        let segment_location = (location, SegmentInfo::VERSION);
        segs.push((segment_location, segment_info))
    }
    Ok(segs)
}

async fn generate_blocks(
    fuse_table: &FuseTable,
    num_blocks: usize,
    is_greater_than_v5: bool,
    table_meta_timestamps: TableMetaTimestamps,
) -> Result<Vec<Arc<BlockMeta>>> {
    let dal = fuse_table.get_operator_ref();
    let schema = fuse_table.schema();
    let location_generator = fuse_table.meta_location_generator();
    let block_writer = BlockWriter::new(
        dal,
        location_generator,
        table_meta_timestamps,
        is_greater_than_v5,
    );
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

    let new_snapshot_location = location_gen
        .snapshot_location_from_uuid(&snapshot_0.snapshot_id, TableSnapshotV2::VERSION)?;
    write_v2_to_storage(&operator, &new_snapshot_location, &snapshot_0).await?;

    // generate 2 segments, 4 blocks.
    let num_of_segments = 2;
    let blocks_per_segment = 2;
    let segments_v3 = generate_segments(
        fuse_table,
        num_of_segments,
        blocks_per_segment,
        false,
        Default::default(),
    )
    .await?;

    // create snapshot 1, the format version is 3.
    let locations = vec![segments_v3[0].0.clone(), segments_v2[0].0.clone()];
    let mut snapshot_1 = TableSnapshot::try_new(
        None,
        None,
        schema.as_ref().clone(),
        Statistics::default(),
        locations,
        None,
        None,
        Default::default(),
    )?;
    snapshot_1.timestamp = Some(now - Duration::hours(12));
    snapshot_1.summary =
        merge_statistics(snapshot_0.summary.clone(), &segments_v3[0].1.summary, None);
    let new_snapshot_location = location_gen
        .snapshot_location_from_uuid(&snapshot_1.snapshot_id, TableSnapshot::VERSION)?;
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
        TableSnapshot::try_from_previous(Arc::new(snapshot_1.clone()), None, Default::default())?;
    snapshot_2.segments = locations;
    snapshot_2.timestamp = Some(now);
    snapshot_2.summary =
        merge_statistics(snapshot_1.summary.clone(), &segments_v3[1].1.summary, None);
    let new_snapshot_location = location_gen
        .snapshot_location_from_uuid(&snapshot_2.snapshot_id, TableSnapshot::VERSION)?;
    snapshot_2
        .write_meta(&operator, &new_snapshot_location)
        .await?;
    FuseTable::commit_to_meta_server(
        fixture.new_query_ctx().await?.as_ref(),
        fuse_table.get_table_info(),
        location_gen,
        snapshot_2,
        None,
        &None,
        &operator,
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

pub async fn append_sample_data(num_blocks: usize, fixture: &TestFixture) -> Result<()> {
    append_sample_data_overwrite(num_blocks, false, fixture).await
}

pub async fn analyze_table(fixture: &TestFixture) -> Result<()> {
    let query = format!(
        "analyze table {}.{}",
        fixture.default_db_name(),
        fixture.default_table_name()
    );
    fixture.execute_command(&query).await
}

pub async fn do_mutation(
    ctx: Arc<QueryContext>,
    s_expr: SExpr,
    schema: DataSchemaRef,
) -> Result<()> {
    let interpreter = MutationInterpreter::try_create(ctx.clone(), s_expr, schema)?;
    let _ = interpreter.execute(ctx).await?;
    Ok(())
}

pub async fn append_sample_data_overwrite(
    num_blocks: usize,
    overwrite: bool,
    fixture: &TestFixture,
) -> Result<()> {
    let stream = TestFixture::gen_sample_blocks_stream(num_blocks, 1);
    let table = fixture.latest_default_table().await?;

    let blocks = stream.try_collect().await?;
    fixture
        .append_commit_blocks(table.clone(), blocks, overwrite, true)
        .await
}

pub async fn append_variant_sample_data(num_blocks: usize, fixture: &TestFixture) -> Result<()> {
    let stream = TestFixture::gen_variant_sample_blocks_stream(num_blocks, 1);
    let table = fixture.latest_default_table().await?;

    let blocks = stream.try_collect().await?;
    fixture
        .append_commit_blocks(table.clone(), blocks, true, true)
        .await
}

pub async fn append_string_sample_data(num_blocks: usize, fixture: &TestFixture) -> Result<()> {
    let stream = TestFixture::gen_string_sample_blocks_stream(num_blocks, 1);
    let table = fixture.latest_default_table().await?;

    let blocks = stream.try_collect().await?;
    fixture
        .append_commit_blocks(table.clone(), blocks, true, true)
        .await
}

pub async fn append_computed_sample_data(num_blocks: usize, fixture: &TestFixture) -> Result<()> {
    let stream = TestFixture::gen_computed_sample_blocks_stream(num_blocks, 1);
    let table = fixture.latest_default_table().await?;

    let blocks = stream.try_collect().await?;
    fixture
        .append_commit_blocks(table.clone(), blocks, true, true)
        .await
}

pub async fn generate_snapshot_v2(
    fixture: &TestFixture,
    number_of_segments: usize,
    blocks_per_segment: usize,
    prev: Option<&TableSnapshot>,
) -> Result<TableSnapshot> {
    let schema = TestFixture::default_table_schema();
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let location_gen = fuse_table.meta_location_generator();
    let operator = fuse_table.get_operator();

    let segments = generate_segments_v2(fuse_table, number_of_segments, blocks_per_segment).await?;

    let id = Uuid::new_v4();
    let snapshot = TableSnapshotV2::new(
        id,
        &prev.as_ref().and_then(|p| p.timestamp),
        prev.map(|p| (p.snapshot_id, p.format_version)),
        schema.as_ref().clone(),
        Statistics::default(),
        segments.iter().map(|s| s.0.clone()).collect(),
        None,
        None,
    );

    let new_snapshot_location = location_gen
        .snapshot_location_from_uuid(&snapshot.snapshot_id, TableSnapshotV2::VERSION)?;
    write_v2_to_storage(&operator, &new_snapshot_location, &snapshot).await?;
    Ok(snapshot.into())
}

pub async fn generate_snapshot_v4(
    fixture: &TestFixture,
    number_of_segments: usize,
    blocks_per_segment: usize,
    prev: Option<&TableSnapshot>,
) -> Result<TableSnapshot> {
    let schema = TestFixture::default_table_schema();
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let location_gen = fuse_table.meta_location_generator();
    let operator = fuse_table.get_operator();

    let segments = generate_segments(
        fuse_table,
        number_of_segments,
        blocks_per_segment,
        false,
        Default::default(),
    )
    .await?;

    let id = Uuid::new_v4();
    let snapshot = TableSnapshotV4::new(
        id,
        None,
        &prev.as_ref().and_then(|p| p.timestamp),
        prev.map(|p| (p.snapshot_id, p.format_version)),
        schema.as_ref().clone(),
        Statistics::default(),
        segments.iter().map(|s| s.0.clone()).collect(),
        None,
        None,
    );
    let new_snapshot_location = location_gen
        .snapshot_location_from_uuid(&snapshot.snapshot_id, TableSnapshotV4::VERSION)?;
    operator
        .write(&new_snapshot_location, snapshot.to_bytes()?)
        .await?;
    let snapshot = TableSnapshot::from(snapshot);
    Ok(snapshot)
}

pub async fn generate_snapshot_v5(
    fixture: &TestFixture,
    number_of_segments: usize,
    blocks_per_segment: usize,
    prev: Option<&TableSnapshot>,
    data_retention_time_in_days: i64,
) -> Result<TableSnapshot> {
    let schema = TestFixture::default_table_schema();
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let location_gen = fuse_table.meta_location_generator();
    let operator = fuse_table.get_operator();

    let table_meta_timestamps = TableMetaTimestamps::new(
        prev.map(|p| Arc::new(p.clone())),
        data_retention_time_in_days,
    );
    let segments = generate_segments(
        fuse_table,
        number_of_segments,
        blocks_per_segment,
        true,
        table_meta_timestamps,
    )
    .await?;

    let prev = prev.map(|p| Arc::new(p.clone()));
    let snapshot = TableSnapshotV5::try_new(
        None,
        prev.clone(),
        schema.as_ref().clone(),
        Statistics::default(),
        segments.iter().map(|s| s.0.clone()).collect(),
        None,
        None,
        TableMetaTimestamps::new(prev, data_retention_time_in_days),
    )?;
    FuseTable::commit_to_meta_server(
        fixture.new_query_ctx().await?.as_ref(),
        fuse_table.get_table_info(),
        location_gen,
        snapshot.clone(),
        None,
        &None,
        &operator,
    )
    .await?;
    Ok(snapshot)
}
