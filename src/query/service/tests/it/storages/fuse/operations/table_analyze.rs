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

use databend_common_base::base::tokio;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_exception::Result;
use databend_common_expression::types::number::NumberScalar;
use databend_common_expression::ColumnId;
use databend_common_expression::Scalar;
use databend_common_io::prelude::borsh_deserialize_from_slice;
use databend_common_storages_fuse::io::MetaReaders;
use databend_common_storages_fuse::io::MetaWriter;
use databend_common_storages_fuse::statistics::reducers::merge_statistics_mut;
use databend_common_storages_fuse::FuseTable;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContext;
use databend_query::sql::plans::Plan;
use databend_query::sql::Planner;
use databend_query::test_kits::*;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_table_meta::meta::MetaHLL;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::Statistics;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::TableSnapshotStatistics;
use databend_storages_common_table_meta::meta::Versioned;

#[tokio::test(flavor = "multi_thread")]
async fn test_table_modify_column_ndv_statistics() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    // setup
    let create_tbl_command = "create table t(c int)";
    fixture.execute_command(create_tbl_command).await?;

    let catalog = ctx.get_catalog("default").await?;

    let num_inserts = 3;
    append_rows(ctx.clone(), num_inserts).await?;
    ctx.evict_table_from_cache("default", "default", "t")?;
    let statistics_sql = "analyze table default.t";
    fixture.execute_command(statistics_sql).await?;

    let table = catalog.get_table(&ctx.get_tenant(), "default", "t").await?;

    // check count
    ctx.evict_table_from_cache("default", "default", "t")?;
    let count_qry = "select count(*) from t";
    let stream = fixture.execute_query(count_qry).await?;
    assert_eq!(num_inserts, query_count(stream).await? as usize);

    let expected = HashMap::from([(0, num_inserts as u64)]);
    check_column_ndv_statistics(ctx.clone(), table.clone(), expected.clone()).await?;

    // append the same values again, and ndv does changed.
    append_rows(ctx.clone(), num_inserts).await?;
    ctx.evict_table_from_cache("default", "default", "t")?;
    fixture.execute_command(statistics_sql).await?;

    // check count
    ctx.evict_table_from_cache("default", "default", "t")?;
    let count_qry = "select count(*) from t";
    let stream = fixture.execute_query(count_qry).await?;
    assert_eq!(num_inserts * 2, query_count(stream).await? as usize);

    check_column_ndv_statistics(ctx.clone(), table.clone(), expected.clone()).await?;

    // delete
    ctx.evict_table_from_cache("default", "default", "t")?;
    let query = "delete from default.t where c=1";
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(query).await?;
    if let Plan::DataMutation { s_expr, schema, .. } = plan {
        do_mutation(ctx.clone(), *s_expr.clone(), schema.clone()).await?;
    }
    ctx.evict_table_from_cache("default", "default", "t")?;
    fixture.execute_command(statistics_sql).await?;

    // check count: delete not affect counts
    check_column_ndv_statistics(ctx, table.clone(), expected).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_update_analyze_statistics() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    fixture.create_default_database().await?;
    fixture.create_default_table().await?;
    let db_name = fixture.default_db_name();
    let tb_name = fixture.default_table_name();

    // insert
    for i in 0..3 {
        let qry = format!("insert into {}.{}(id) values({})", db_name, tb_name, i);
        fixture.execute_command(&qry).await?;
    }

    // update
    let query = format!("update {}.{} set id = 3 where id = 0", db_name, tb_name);
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(&query).await?;
    if let Plan::DataMutation { s_expr, schema, .. } = plan {
        do_mutation(ctx.clone(), *s_expr.clone(), schema.clone()).await?;
    }

    // check summary after update
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let after_update = fuse_table.read_table_snapshot().await?.unwrap();
    let base_summary = after_update.summary.clone();
    let id_stats = base_summary.col_stats.get(&0).unwrap();
    assert_eq!(id_stats.max(), &Scalar::Number(NumberScalar::Int32(3)));
    assert_eq!(id_stats.min(), &Scalar::Number(NumberScalar::Int32(0)));

    // get segments summary
    let mut segment_summary = Statistics::default();
    let segment_reader = MetaReaders::segment_info_reader(
        ctx.get_application_level_data_operator()?.operator(),
        TestFixture::default_table_schema(),
    );
    for segment in after_update.segments.iter() {
        let param = LoadParams {
            location: segment.0.clone(),
            len_hint: None,
            ver: segment.1,
            put_cache: false,
        };
        let compact_segment = segment_reader.read(&param).await?;
        let segment_info = SegmentInfo::try_from(compact_segment)?;
        merge_statistics_mut(
            &mut segment_summary,
            &segment_info.summary,
            fuse_table.cluster_key_id(),
        );
    }

    // analyze
    analyze_table(&fixture).await?;

    // check summary after analyze
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let after_analyze = fuse_table.read_table_snapshot().await?.unwrap();
    let last_summary = after_analyze.summary.clone();
    let id_stats = last_summary.col_stats.get(&0).unwrap();
    assert_eq!(id_stats.max(), &Scalar::Number(NumberScalar::Int32(3)));
    assert_eq!(id_stats.min(), &Scalar::Number(NumberScalar::Int32(1)));

    assert_eq!(segment_summary, last_summary);

    Ok(())
}

async fn check_column_ndv_statistics(
    ctx: Arc<dyn TableContext>,
    table: Arc<dyn Table>,
    expected: HashMap<u32, u64>,
) -> Result<()> {
    let provider = table.column_statistics_provider(ctx).await?;

    for (i, num) in expected.iter() {
        let stat = provider.column_statistics(*i);
        assert!(stat.is_some());
        // Safe to unwrap: FuseTable's ndv is not None.
        assert_eq!(stat.unwrap().ndv.unwrap(), *num);
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_analyze_without_prev_table_seq() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    // setup
    let create_tbl_command = "create table t(c int)";
    fixture.execute_command(create_tbl_command).await?;

    append_rows(ctx.clone(), 3).await?;
    let catalog = ctx.get_catalog("default").await?;
    let table = catalog.get_table(&ctx.get_tenant(), "default", "t").await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let location_gen = fuse_table.meta_location_generator();
    let operator = fuse_table.get_operator();

    // genenrate snapshot without prev_table_seq
    let snapshot_0 = fuse_table.read_table_snapshot().await?.unwrap();
    let snapshot_1 =
        TableSnapshot::try_from_previous(snapshot_0.clone(), None, Default::default())?;
    let snapshot_loc_1 = location_gen
        .snapshot_location_from_uuid(&snapshot_1.snapshot_id, TableSnapshot::VERSION)?;
    snapshot_1.write_meta(&operator, &snapshot_loc_1).await?;

    // generate table statistics.
    let col: Vec<u8> = vec![1, 3, 0, 0, 0, 118, 5, 1, 21, 6, 3, 229, 13, 3];
    let hll: HashMap<ColumnId, MetaHLL> = HashMap::from([(0, borsh_deserialize_from_slice(&col)?)]);
    let table_statistics =
        TableSnapshotStatistics::new(hll, HashMap::new(), snapshot_1.snapshot_id);
    let table_statistics_location = location_gen.snapshot_statistics_location_from_uuid(
        &table_statistics.snapshot_id,
        table_statistics.format_version(),
    )?;
    // genenrate snapshot without prev_table_seq
    let mut snapshot_2 =
        TableSnapshot::try_from_previous(Arc::new(snapshot_1.clone()), None, Default::default())?;
    snapshot_2.table_statistics_location = Some(table_statistics_location);
    FuseTable::commit_to_meta_server(
        fixture.new_query_ctx().await?.as_ref(),
        fuse_table.get_table_info(),
        location_gen,
        snapshot_2,
        Some(table_statistics),
        &None,
        &operator,
    )
    .await?;

    // check statistics.
    let table = table.refresh(ctx.as_ref()).await?;
    let expected = HashMap::from([(0, 3_u64)]);
    check_column_ndv_statistics(ctx.clone(), table.clone(), expected.clone()).await?;

    let qry = "insert into t values(4)";
    execute_command(ctx.clone(), qry).await?;

    ctx.evict_table_from_cache("default", "default", "t")?;
    let statistics_sql = "analyze table default.t";
    fixture.execute_command(statistics_sql).await?;

    let table = table.refresh(ctx.as_ref()).await?;
    let expected = HashMap::from([(0, 4_u64)]);
    check_column_ndv_statistics(ctx.clone(), table.clone(), expected.clone()).await?;
    Ok(())
}

async fn append_rows(ctx: Arc<QueryContext>, n: usize) -> Result<()> {
    for i in 0..n {
        let qry = format!("insert into t values({})", i);
        execute_command(ctx.clone(), &qry).await?;
    }
    Ok(())
}
