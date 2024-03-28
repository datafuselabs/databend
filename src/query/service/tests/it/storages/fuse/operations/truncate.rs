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

use databend_common_base::base::tokio;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_sql::executor::table_read_plan::ToReadDataSourcePlan;
use databend_query::pipelines::executor::ExecutorSettings;
use databend_query::pipelines::executor::PipelineCompleteExecutor;
use databend_query::sessions::QueryContext;
use databend_query::test_kits::*;
use futures_util::TryStreamExt;

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_table_truncate() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    fixture.create_default_database().await?;
    fixture.create_default_table().await?;

    let table = fixture.latest_default_table().await?;

    // 1. truncate empty table
    let prev_version = table.get_table_info().ident.seq;
    let r = truncate_table(ctx.clone(), table.clone()).await;
    let table = fixture.latest_default_table().await?;
    // no side effects
    assert_eq!(prev_version, table.get_table_info().ident.seq);
    assert!(r.is_ok());

    // 2. truncate table which has data
    let num_blocks = 10;
    let rows_per_block = 3;
    let value_start_from = 1;
    let stream =
        TestFixture::gen_sample_blocks_stream_ex(num_blocks, rows_per_block, value_start_from);

    let blocks = stream.try_collect().await?;
    fixture
        .append_commit_blocks(table.clone(), blocks, false, true)
        .await?;

    let source_plan = table.read_plan(ctx.clone(), None, true).await?;

    // get the latest tbl
    let prev_version = table.get_table_info().ident.seq;
    let table = fixture.latest_default_table().await?;
    assert_ne!(prev_version, table.get_table_info().ident.seq);

    // ensure data ingested
    let (stats, _) = table
        .read_partitions(ctx.clone(), source_plan.push_downs.clone(), true)
        .await?;
    assert_eq!(stats.read_rows, (num_blocks * rows_per_block));

    // truncate
    let r = truncate_table(ctx.clone(), table.clone()).await;
    assert!(r.is_ok());

    // get the latest tbl
    let prev_version = table.get_table_info().ident.seq;
    let table = fixture.latest_default_table().await?;
    assert_ne!(prev_version, table.get_table_info().ident.seq);
    let (stats, parts) = table
        .read_partitions(ctx.clone(), source_plan.push_downs.clone(), true)
        .await?;
    // cleared?
    assert_eq!(parts.len(), 0);
    assert_eq!(stats.read_rows, 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_table_truncate_appending_concurrently() -> Result<()> {
    // the test scenario is as follows:
    // ┌──────┐
    // │  s0  │
    // └───┬──┘
    //     │ append 30 rows
    //     ▼
    // ┌──────┐
    // │  s1  │
    // └───┬──┘
    //     │ append 30 rows
    //     ▼
    // ┌──────┐
    // │  s2  │
    // └───┬──┘
    //     │   ─────────────────► after s2 is committed; `truncate purge table @s1` should fail
    //     ▼ append 30 rows
    // ┌──────┐
    // │  s3  │
    // └──────┘
    //
    //        s3 should be a valid snapshot,full-scan should work as expected

    let fixture = Arc::new(TestFixture::setup().await?);
    let ctx = fixture.new_query_ctx().await?;

    fixture.create_default_database().await?;
    fixture.create_default_table().await?;
    let init_table = fixture.latest_default_table().await?;

    // 1. setup

    let num_blocks = 10;
    let rows_per_block = 3;
    let value_start_from = 1;

    let append_data = |t: Arc<dyn Table>| {
        let fixture = fixture.clone();
        async move {
            let stream = TestFixture::gen_sample_blocks_stream_ex(
                num_blocks,
                rows_per_block,
                value_start_from,
            );

            let blocks = stream.try_collect().await?;
            fixture
                .append_commit_blocks(t.clone(), blocks, false, true)
                .await?;
            Ok::<_, ErrorCode>(())
        }
    };

    // append 10 blocks, generate snapshot s1
    append_data(init_table).await?;

    // 2. pin to snapshot s1, later we use this snapshot to perform the `truncate purge` operation
    let s1_table_to_be_truncated = fixture.latest_default_table().await?;

    // 3. append other 10 blocks, on the base of s1, generate snapshot s2
    append_data(s1_table_to_be_truncated.clone()).await?;
    let s2_table_to_appended = fixture.latest_default_table().await?;

    // 4. perform `truncate` operation on s1
    let r = truncate_table(ctx, s1_table_to_be_truncated).await;
    // version mismatched, and `truncate purge` should result in error (but nothing should have been removed)
    assert!(r.is_err());

    // 5. append 10 blocks, on the base of s2, generate snapshot s3
    append_data(s2_table_to_appended.clone()).await?;

    // 6. check s3
    let qry = format!(
        "select * from  {}.{}",
        fixture.default_db_name(),
        fixture.default_table_name()
    );
    // - full scan should work
    let result = fixture
        .execute_query(qry.as_str())
        .await?
        .try_collect::<Vec<DataBlock>>()
        .await?;

    // - and the number of rows should be as expected
    assert_eq!(
        result.iter().map(|b| b.num_rows()).sum::<usize>(),
        // 3 `append ` operations( s0 -> s1, s1 -> s2, s2 -> s3)
        // each append `num_blocks` blocks, each block has `rows_per_block` rows
        num_blocks * rows_per_block * 3
    );

    Ok(())
}

async fn truncate_table(ctx: Arc<QueryContext>, table: Arc<dyn Table>) -> Result<()> {
    let mut pipeline = databend_common_pipeline_core::Pipeline::create();
    table.truncate(ctx.clone(), &mut pipeline).await?;
    if !pipeline.is_empty() {
        pipeline.set_max_threads(1);
        let executor_settings = ExecutorSettings::try_create(ctx.clone())?;
        let executor = PipelineCompleteExecutor::try_create(pipeline, executor_settings)?;
        ctx.set_executor(executor.get_inner())?;
        executor.execute()?;
    }
    Ok(())
}
