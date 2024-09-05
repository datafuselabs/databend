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

use databend_common_config::InnerConfig;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_query::storages::fuse::io::TableMetaLocationGenerator;
use databend_query::test_kits::TestFixture;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::Versioned;
use futures_util::TryStreamExt;
use uuid::Uuid;

#[test]
fn test_meta_locations() -> Result<()> {
    let test_prefix = "test_pref";
    let locs = TableMetaLocationGenerator::with_prefix(test_prefix.to_owned());
    let ((path, _ver), _id) = locs.gen_block_location(Default::default());
    assert!(path.starts_with(test_prefix));
    let seg_loc = locs.gen_segment_info_location(Default::default());
    assert!(seg_loc.starts_with(test_prefix));
    let uuid = Uuid::new_v4();
    let snapshot_loc = locs.snapshot_location_from_uuid(&uuid, TableSnapshot::VERSION)?;
    assert!(snapshot_loc.starts_with(test_prefix));
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_array_cache_of_nested_column_iusse_14502() -> Result<()> {
    // https://github.com/datafuselabs/databend/issues/14502
    // ~~~
    //  create table t1(c tuple(c1 int not null, c2 int null) null);
    //  insert into t1 values((1,null));
    //  select c.1 from t1;
    //  select c from t1;
    // ~~~

    let mut config = InnerConfig::default();
    // memory cache is not enabled by default, let's enable it
    config.cache.table_data_deserialized_data_bytes = 1024 * 1024 * 10;
    let fixture = TestFixture::setup_with_config(&config).await?;

    fixture.create_default_database().await?;
    let db = fixture.default_db_name();

    let sql_create = format!(
        "create table {db}.t1(c tuple(c1 int not null, c2 int null) null) storage_format = Parquet"
    );
    let insert = format!("insert into {db}.t1 values((1,null))");
    let q1 = format!("select c.1 from {db}.t1");
    let q2 = format!("select c from {db}.t1");

    let stmts = vec![sql_create, insert];

    for x in &stmts {
        fixture.execute_command(x).await?;
    }

    let queries = vec![q1, q2];

    for x in &queries {
        let res = fixture
            .execute_query(x)
            .await?
            .try_collect::<Vec<DataBlock>>()
            .await;
        assert!(res.is_ok());
    }

    Ok(())
}
