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
use std::collections::HashSet;
use std::sync::Arc;

use chrono::Utc;
use databend_common_base::base::tokio;
use databend_common_catalog::plan::ReclusterParts;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::BlockThresholds;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_storages_fuse::io::SegmentWriter;
use databend_common_storages_fuse::io::TableMetaLocationGenerator;
use databend_common_storages_fuse::operations::ReclusterMode;
use databend_common_storages_fuse::operations::ReclusterMutator;
use databend_common_storages_fuse::pruning::create_segment_location_vector;
use databend_common_storages_fuse::statistics::reducers::merge_statistics_mut;
use databend_common_storages_fuse::statistics::reducers::reduce_block_metas;
use databend_common_storages_fuse::FuseBlockPartInfo;
use databend_common_storages_fuse::FuseTable;
use databend_query::sessions::TableContext;
use databend_query::test_kits::*;
use databend_storages_common_table_meta::meta;
use databend_storages_common_table_meta::meta::BlockMeta;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::Versioned;
use rand::thread_rng;
use rand::Rng;
use uuid::Uuid;

use crate::storages::fuse::operations::mutation::verify_compact_tasks;
use crate::storages::fuse::operations::mutation::CompactSegmentTestFixture;

#[tokio::test(flavor = "multi_thread")]
async fn test_recluster_mutator_block_select() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let location_generator = TableMetaLocationGenerator::with_prefix("_prefix".to_owned());

    let data_accessor = ctx.get_application_level_data_operator()?.operator();
    let seg_writer = SegmentWriter::new(&data_accessor, &location_generator, Default::default());

    let cluster_key_id = 0;
    let gen_test_seg = |cluster_stats: Option<ClusterStatistics>| async {
        let block_id = Uuid::new_v4().simple().to_string();
        let location = (block_id, DataBlock::VERSION);
        let test_block_meta = Arc::new(BlockMeta::new(
            1,
            1,
            1,
            HashMap::default(),
            HashMap::default(),
            cluster_stats,
            location.clone(),
            None,
            0,
            None,
            meta::Compression::Lz4Raw,
            Some(Utc::now()),
        ));

        let statistics = reduce_block_metas(
            &[test_block_meta.as_ref()],
            BlockThresholds::default(),
            Some(0),
        );

        let segment = SegmentInfo::new(vec![test_block_meta], statistics);
        Ok::<_, ErrorCode>((seg_writer.write_segment(segment).await?, location))
    };

    let mut test_segment_locations = vec![];
    let mut test_block_locations = vec![];
    let (segment_location, block_location) = gen_test_seg(Some(ClusterStatistics::new(
        cluster_key_id,
        vec![Scalar::from(1i64)],
        vec![Scalar::from(3i64)],
        0,
        None,
    )))
    .await?;
    test_segment_locations.push(segment_location);
    test_block_locations.push(block_location);

    let (segment_location, block_location) = gen_test_seg(Some(ClusterStatistics::new(
        cluster_key_id,
        vec![Scalar::from(2i64)],
        vec![Scalar::from(4i64)],
        0,
        None,
    )))
    .await?;
    test_segment_locations.push(segment_location);
    test_block_locations.push(block_location);

    let schema = TableSchemaRef::new(TableSchema::empty());
    let (segment_location, block_location) = gen_test_seg(Some(ClusterStatistics::new(
        cluster_key_id,
        vec![Scalar::from(4i64)],
        vec![Scalar::from(5i64)],
        0,
        None,
    )))
    .await?;
    test_segment_locations.push(segment_location);
    test_block_locations.push(block_location);
    // unused snapshot.
    let snapshot = TableSnapshot::new_empty_snapshot(schema.as_ref().clone(), None);

    let ctx: Arc<dyn TableContext> = ctx.clone();
    let segment_locations = create_segment_location_vector(test_segment_locations, None);
    let compact_segments = FuseTable::segment_pruning(
        &ctx,
        schema.clone(),
        data_accessor.clone(),
        &None,
        segment_locations,
    )
    .await?;

    let column_ids = snapshot.schema.to_leaf_column_id_set();
    let mutator = ReclusterMutator::new(
        ctx,
        schema,
        vec![DataType::Number(NumberDataType::Int64)],
        1.0,
        BlockThresholds::default(),
        cluster_key_id,
        1,
        1000,
        column_ids,
    );
    let (_, parts) = mutator
        .target_select(compact_segments, ReclusterMode::Recluster)
        .await?;
    let need_recluster = !parts.is_empty();
    assert!(need_recluster);
    let ReclusterParts::Recluster { tasks, .. } = parts else {
        return Err(ErrorCode::Internal("Logical error, it's a bug"));
    };
    assert_eq!(tasks.len(), 1);
    let total_block_nums = tasks.iter().map(|t| t.parts.len()).sum::<usize>();
    assert_eq!(total_block_nums, 3);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_safety_for_recluster() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    let operator = ctx.get_application_level_data_operator()?.operator();

    let recluster_block_size = 300;
    ctx.get_settings()
        .set_recluster_block_size(recluster_block_size as u64)?;

    let cluster_key_id = 0;
    let block_per_seg = 5;
    let threshold = BlockThresholds {
        max_rows_per_block: 5,
        min_rows_per_block: 4,
        max_bytes_per_block: 1024,
    };

    let data_accessor = operator.clone();
    let schema = TestFixture::default_table_schema();
    let mut rand = thread_rng();

    // for r in 1..100 { // <- use this at home
    for r in 1..10 {
        eprintln!("round {}", r);
        let number_of_segments: usize = rand.gen_range(1..10);
        let max_tasks: usize = rand.gen_range(1..5);

        let mut block_number_of_segments = Vec::with_capacity(number_of_segments);
        let mut rows_per_blocks = Vec::with_capacity(number_of_segments);

        for _ in 0..number_of_segments {
            block_number_of_segments.push(rand.gen_range(1..8));
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

        let unclustered: bool = rand.gen();
        let mut unclustered_segment_indices = HashSet::new();
        if unclustered {
            unclustered_segment_indices = block_number_of_segments
                .iter()
                .rev()
                .enumerate()
                .filter(|(_, &num)| num % 4 == 0)
                .map(|(index, _)| index)
                .collect();
        }
        let (locations, _, segment_infos) = CompactSegmentTestFixture::gen_segments(
            ctx.clone(),
            block_number_of_segments,
            rows_per_blocks,
            threshold,
            Some(cluster_key_id),
            block_per_seg,
            unclustered,
        )
        .await?;

        eprintln!("data ready");

        let mut summary = Statistics::default();
        for seg in &segment_infos {
            merge_statistics_mut(&mut summary, &seg.summary, Some(cluster_key_id));
        }

        let snapshot = Arc::new(TableSnapshot::try_new(
            None,
            None,
            schema.as_ref().clone(),
            summary,
            locations.clone(),
            None,
            None,
            Default::default(),
        )?);

        let mut block_ids = HashSet::new();
        for seg in &segment_infos {
            for b in &seg.blocks {
                block_ids.insert(b.location.clone());
            }
        }

        let ctx: Arc<dyn TableContext> = ctx.clone();
        let segment_locations = create_segment_location_vector(locations.clone(), None);
        let compact_segments = FuseTable::segment_pruning(
            &ctx,
            schema.clone(),
            data_accessor.clone(),
            &None,
            segment_locations,
        )
        .await?;

        let column_ids = snapshot.schema.to_leaf_column_id_set();
        let mut parts = ReclusterParts::new_recluster_parts();
        let mutator = Arc::new(ReclusterMutator::new(
            ctx.clone(),
            schema.clone(),
            vec![DataType::Number(NumberDataType::Int32)],
            1.0,
            threshold,
            cluster_key_id,
            max_tasks,
            block_per_seg,
            column_ids,
        ));
        let (mode, selected_segs) = mutator.select_segments(&compact_segments, 8)?;
        // select the blocks with the highest depth.
        if selected_segs.is_empty() {
            let result = FuseTable::generate_recluster_parts(mutator, compact_segments).await?;
            if let Some((_, _, recluster_parts)) = result {
                parts = recluster_parts;
            }
        } else {
            let selected_segments = selected_segs
                .into_iter()
                .map(|i| compact_segments[i].clone())
                .collect();
            (_, parts) = mutator.target_select(selected_segments, mode).await?;
        }

        if !parts.is_empty() {
            eprintln!("need_recluster");
            match parts {
                ReclusterParts::Recluster {
                    tasks,
                    remained_blocks,
                    removed_segment_indexes,
                    ..
                } => {
                    assert!(unclustered_segment_indices.is_empty());
                    assert!(tasks.len() <= max_tasks);
                    assert!(!tasks.is_empty() || !remained_blocks.is_empty());
                    eprintln!("tasks_num: {}, max_tasks: {}", tasks.len(), max_tasks);
                    let mut blocks = Vec::new();
                    for task in tasks.into_iter() {
                        let parts = task.parts.partitions;
                        assert!(task.total_bytes <= recluster_block_size);
                        for part in parts.into_iter() {
                            let fuse_part = FuseBlockPartInfo::from_part(&part)?;
                            blocks.push(fuse_part.location.clone());
                        }
                    }

                    eprintln!(
                        "selected segments number {}, selected blocks number {}, remained blocks number {}",
                        removed_segment_indexes.len(),
                        blocks.len(),
                        remained_blocks.len()
                    );
                    for remain in remained_blocks {
                        blocks.push(remain.location.0.clone());
                    }

                    let block_ids_after_target = HashSet::from_iter(blocks.into_iter());

                    let mut origin_blocks_ids = HashSet::new();
                    for idx in &removed_segment_indexes {
                        for b in &segment_infos[*idx].blocks {
                            origin_blocks_ids.insert(b.location.0.clone());
                        }
                    }
                    assert_eq!(block_ids_after_target, origin_blocks_ids);
                }
                ReclusterParts::Compact(parts) => {
                    assert!(unclustered);
                    assert!(!unclustered_segment_indices.is_empty());
                    verify_compact_tasks(
                        ctx.get_application_level_data_operator()?.operator(),
                        parts,
                        locations,
                        unclustered_segment_indices,
                    )
                    .await?;
                }
            };
        }
    }

    Ok(())
}

#[test]
fn test_check_point() {
    // [1,2] [2,3], check point 2
    let start = vec![1];
    let end = vec![0];
    assert!(ReclusterMutator::check_point(&start, &end));

    // [1,2] [2,2], check point 2
    let start = vec![1];
    let end = vec![0, 1];
    assert!(ReclusterMutator::check_point(&start, &end));

    // [1,1] [1,2], check point 1
    let start = vec![0, 1];
    let end = vec![0];
    assert!(ReclusterMutator::check_point(&start, &end));

    // [1,2] [1,3], check point 1
    let start = vec![0, 1];
    let end = vec![];
    assert!(!ReclusterMutator::check_point(&start, &end));

    // [1,3] [2,3], check point 3
    let start = vec![];
    let end = vec![0, 1];
    assert!(!ReclusterMutator::check_point(&start, &end));

    // [1,3] [3,3] [3,4], check point 3
    let start = vec![1, 2];
    let end = vec![0, 1];
    assert!(!ReclusterMutator::check_point(&start, &end));
}
