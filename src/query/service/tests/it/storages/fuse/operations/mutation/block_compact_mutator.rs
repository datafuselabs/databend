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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_base::base::tokio;
use databend_common_catalog::plan::PartInfoType;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::table::CompactionLimits;
use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_sql::executor::physical_plans::CommitSink;
use databend_common_sql::executor::physical_plans::CompactSource;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::executor::PhysicalPlan;
use databend_common_storages_fuse::io::SegmentsIO;
use databend_common_storages_fuse::operations::BlockCompactMutator;
use databend_common_storages_fuse::operations::CompactBlockPartInfo;
use databend_common_storages_fuse::operations::CompactOptions;
use databend_common_storages_fuse::statistics::reducers::merge_statistics_mut;
use databend_query::pipelines::executor::ExecutorSettings;
use databend_query::pipelines::executor::PipelineCompleteExecutor;
use databend_query::schedulers::build_query_pipeline_without_render_result_set;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContext;
use databend_query::test_kits::*;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::TableSnapshot;
use opendal::Operator;
use rand::thread_rng;
use rand::Rng;

use crate::storages::fuse::operations::mutation::segments_compact_mutator::CompactSegmentTestFixture;

#[tokio::test(flavor = "multi_thread")]
async fn test_compact() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let tbl_name = fixture.default_table_name();
    let db_name = fixture.default_db_name();

    fixture.create_default_database().await?;
    fixture.create_normal_table().await?;

    // insert
    let qry = format!("insert into {}.{}(id) values(10)", db_name, tbl_name);
    fixture.execute_command(qry.as_str()).await?;

    // compact
    let catalog = ctx
        .get_catalog(fixture.default_catalog_name().as_str())
        .await?;
    let table = catalog
        .get_table(&ctx.get_tenant(), &db_name, &tbl_name)
        .await?;
    let res = do_compact(ctx.clone(), table.clone()).await;
    assert!(res.is_ok());
    assert!(!res.unwrap());

    // insert
    for i in 0..9 {
        let qry = format!("insert into {}.{}(id) values({})", db_name, tbl_name, i);
        fixture.execute_command(qry.as_str()).await?;
    }

    // compact
    let catalog = ctx
        .get_catalog(fixture.default_catalog_name().as_str())
        .await?;
    let table = catalog
        .get_table(&ctx.get_tenant(), &db_name, &tbl_name)
        .await?;
    let res = do_compact(ctx.clone(), table.clone()).await;
    assert!(res.is_ok());
    assert!(res.unwrap());

    // check count
    let expected = vec![
        "+----------+----------+",
        "| Column 0 | Column 1 |",
        "+----------+----------+",
        "| 1        | 1        |",
        "+----------+----------+",
    ];
    let qry = format!(
        "select segment_count, block_count as count from fuse_snapshot('{}', '{}') limit 1",
        db_name, tbl_name
    );
    expects_ok(
        "check segment and block count",
        fixture.execute_query(qry.as_str()).await,
        expected,
    )
    .await?;

    Ok(())
}

async fn do_compact(ctx: Arc<QueryContext>, table: Arc<dyn Table>) -> Result<bool> {
    let settings = ctx.get_settings();
    let mut pipeline = databend_common_pipeline_core::Pipeline::create();
    let res = table
        .compact_blocks(ctx.clone(), CompactionLimits::default())
        .await?;

    let table_info = table.get_table_info().clone();
    if let Some((parts, snapshot)) = res {
        let table_meta_timestamps =
            ctx.get_table_meta_timestamps(table_info.ident.table_id, Some(snapshot.clone()))?;
        let merge_meta = parts.partitions_type() == PartInfoType::LazyLevel;
        let root = PhysicalPlan::CompactSource(Box::new(CompactSource {
            parts,
            table_info: table_info.clone(),
            column_ids: snapshot.schema.to_leaf_column_id_set(),
            plan_id: u32::MAX,
            table_meta_timestamps,
        }));

        let physical_plan = PhysicalPlan::CommitSink(Box::new(CommitSink {
            input: Box::new(root),
            table_info,
            snapshot: Some(snapshot),
            mutation_kind: MutationKind::Compact,
            update_stream_meta: vec![],
            merge_meta,
            deduplicated_label: None,
            plan_id: u32::MAX,
            recluster_info: None,
            table_meta_timestamps,
        }));

        let build_res =
            build_query_pipeline_without_render_result_set(&ctx, &physical_plan).await?;
        pipeline = build_res.main_pipeline;
    };

    if !pipeline.is_empty() {
        pipeline.set_max_threads(settings.get_max_threads()? as usize);
        let executor_settings = ExecutorSettings::try_create(ctx.clone())?;
        let executor = PipelineCompleteExecutor::try_create(pipeline, executor_settings)?;
        ctx.set_executor(executor.get_inner())?;
        executor.execute()?;
        Ok(true)
    } else {
        Ok(false)
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_safety() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let operator = ctx.get_application_level_data_operator()?.operator();
    let settings = ctx.get_settings();
    settings.set_max_threads(2)?;
    settings.set_max_storage_io_requests(4)?;

    let threshold = BlockThresholds {
        max_rows_per_block: 5,
        min_rows_per_block: 4,
        max_bytes_per_block: 1024,
    };

    let schema = TestFixture::default_table_schema();
    let mut rand = thread_rng();

    // for r in 1..100 { // <- use this at home
    for r in 1..10 {
        eprintln!("round {}", r);
        let number_of_segments: usize = rand.gen_range(1..10);

        let mut block_number_of_segments = Vec::with_capacity(number_of_segments);
        let mut rows_per_blocks = Vec::with_capacity(number_of_segments);

        for _ in 0..number_of_segments {
            block_number_of_segments.push(rand.gen_range(1..25));
            rows_per_blocks.push(rand.gen_range(1..8));
        }

        let number_of_blocks: usize = block_number_of_segments.iter().sum();
        if number_of_blocks < 2 {
            eprintln!("number_of_blocks must large than 1");
            continue;
        }
        eprintln!(
            "generating segments number of segments {},  number of blocks {}",
            number_of_segments, number_of_blocks,
        );

        let cluster_key_id = if number_of_segments % 2 == 0 {
            Some(0)
        } else {
            None
        };

        let (locations, _, segment_infos) = CompactSegmentTestFixture::gen_segments(
            ctx.clone(),
            block_number_of_segments,
            rows_per_blocks,
            threshold,
            cluster_key_id,
            5,
            false,
        )
        .await?;

        eprintln!("data ready");

        let mut summary = Statistics::default();
        for seg in &segment_infos {
            merge_statistics_mut(&mut summary, &seg.summary, None);
        }

        let snapshot = TableSnapshot::try_new(
            None,
            None,
            schema.as_ref().clone(),
            summary,
            locations.clone(),
            None,
            None,
            Default::default(),
        )?;

        let limit: usize = rand.gen_range(1..15);
        let compact_params = CompactOptions {
            base_snapshot: Arc::new(snapshot),
            block_per_seg: 10,
            num_segment_limit: Some(limit),
            num_block_limit: None,
        };

        eprintln!("running target select");
        let mut block_compact_mutator = BlockCompactMutator::new(
            ctx.clone(),
            threshold,
            compact_params,
            operator.clone(),
            cluster_key_id,
        );
        let selections = block_compact_mutator.target_select().await?;
        if selections.is_empty() {
            eprintln!("no target select");
            continue;
        }
        verify_compact_tasks(
            ctx.get_application_level_data_operator()?.operator(),
            selections,
            locations,
            HashSet::new(),
        )
        .await?;
    }

    Ok(())
}

pub async fn verify_compact_tasks(
    dal: Operator,
    parts: Partitions,
    locations: Vec<Location>,
    expected_segment_indices: HashSet<usize>,
) -> Result<()> {
    assert!(parts.partitions_type() != PartInfoType::LazyLevel);

    let mut actual_blocks_number = 0;
    let mut compact_segment_indices = HashSet::new();
    let mut actual_block_ids = HashSet::new();
    for part in parts.partitions.into_iter() {
        let part = CompactBlockPartInfo::from_part(&part)?;
        match part {
            CompactBlockPartInfo::CompactExtraInfo(extra) => {
                compact_segment_indices.insert(extra.segment_index);
                compact_segment_indices.extend(extra.removed_segment_indexes.iter());
                actual_blocks_number += extra.unchanged_blocks.len();
                for b in &extra.unchanged_blocks {
                    actual_block_ids.insert(b.1.location.clone());
                }
            }
            CompactBlockPartInfo::CompactTaskInfo(task) => {
                compact_segment_indices.insert(task.index.segment_idx);
                actual_blocks_number += task.blocks.len();
                for b in &task.blocks {
                    actual_block_ids.insert(b.location.clone());
                }
            }
        }
    }

    eprintln!("compact_segment_indices: {:?}", compact_segment_indices);
    if !expected_segment_indices.is_empty() {
        assert_eq!(expected_segment_indices, compact_segment_indices);
    }
    let mut except_blocks_number = 0;
    let mut except_block_ids = HashSet::new();
    for idx in compact_segment_indices.into_iter() {
        let loc = locations.get(idx).unwrap();
        let compact_segment = SegmentsIO::read_compact_segment(
            dal.clone(),
            loc.clone(),
            TestFixture::default_table_schema(),
            false,
        )
        .await?;
        let segment = SegmentInfo::try_from(compact_segment)?;
        except_blocks_number += segment.blocks.len();
        for b in &segment.blocks {
            except_block_ids.insert(b.location.clone());
        }
    }
    assert_eq!(except_blocks_number, actual_blocks_number);
    assert_eq!(except_block_ids, actual_block_ids);
    Ok(())
}
