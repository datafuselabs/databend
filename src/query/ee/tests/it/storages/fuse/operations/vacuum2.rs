// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use databend_common_base::base::tokio;
use databend_common_exception::Result;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::TableContext;
use databend_enterprise_query::storages::fuse::do_vacuum2;
use databend_query::test_kits::*;

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_vacuum2_older_version() -> Result<()> {
    env_logger::try_init().unwrap();
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    fixture.create_normal_table().await?;

    let prev = generate_snapshot_v2(&fixture, 2, 2, None).await?;
    let prev = generate_snapshot_v4(&fixture, 2, 2, Some(&prev)).await?;
    let prev = generate_snapshot_v5(&fixture, 2, 2, Some(&prev), 0).await?;
    check_data_dir(&fixture, "vacuum2 5", 3, 0, 6, 12, 12, Some(()), None).await?;

    // v2->v4->v5 no gc root
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    do_vacuum2(fuse_table, fixture.new_query_ctx().await?).await?;
    check_data_dir(&fixture, "vacuum2 6", 3, 0, 6, 12, 12, Some(()), None).await?;

    let _ = generate_snapshot_v5(&fixture, 2, 2, Some(&prev), 0).await?;
    check_data_dir(&fixture, "vacuum2 7", 4, 0, 8, 16, 16, Some(()), None).await?;

    // v2->v4->v5->v5, retention=1, no gc root
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    do_vacuum2(fuse_table, fixture.new_query_ctx().await?).await?;
    check_data_dir(&fixture, "vacuum2 8", 4, 0, 8, 16, 16, Some(()), None).await?;

    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_data_retention_time_in_days(0)?;
    // reserve only one snapshot
    do_vacuum2(fuse_table, ctx).await?;
    check_data_dir(&fixture, "vacuum2 9", 1, 0, 2, 4, 4, Some(()), None).await?;
    Ok(())
}
