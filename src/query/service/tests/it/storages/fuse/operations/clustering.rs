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

use databend_common_ast::ast::Engine;
use databend_common_base::base::tokio;
use databend_common_meta_app::schema::CreateOption;
use databend_common_sql::plans::AlterTableClusterKeyPlan;
use databend_common_sql::plans::CreateTablePlan;
use databend_common_sql::plans::DropTableClusterKeyPlan;
use databend_common_storages_fuse::io::MetaReaders;
use databend_common_storages_fuse::FuseTable;
use databend_query::interpreters::AlterTableClusterKeyInterpreter;
use databend_query::interpreters::CreateTableInterpreter;
use databend_query::interpreters::DropTableClusterKeyInterpreter;
use databend_query::interpreters::Interpreter;
use databend_query::test_kits::*;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_alter_table_cluster_key() -> databend_common_exception::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;

    let ctx = fixture.new_query_ctx().await?;

    let create_table_plan = CreateTablePlan {
        create_option: CreateOption::Create,
        tenant: fixture.default_tenant(),
        catalog: fixture.default_catalog_name(),
        database: fixture.default_db_name(),
        table: fixture.default_table_name(),
        schema: TestFixture::default_table_schema(),
        engine: Engine::Fuse,
        engine_options: Default::default(),
        storage_params: None,
        read_only_attach: false,
        part_prefix: "".to_string(),
        options: [
            // database id is required for FUSE
            (OPT_KEY_DATABASE_ID.to_owned(), "1".to_owned()),
        ]
        .into(),
        field_comments: vec![],
        as_select: None,
        cluster_key: None,
    };

    // create test table
    let interpreter = CreateTableInterpreter::try_create(ctx.clone(), create_table_plan)?;
    let _ = interpreter.execute(ctx.clone()).await?;

    // add cluster key
    let alter_table_cluster_key_plan = AlterTableClusterKeyPlan {
        tenant: fixture.default_tenant().name().to_string(),
        catalog: fixture.default_catalog_name(),
        database: fixture.default_db_name(),
        table: fixture.default_table_name(),
        cluster_keys: vec!["id".to_string()],
    };
    let interpreter =
        AlterTableClusterKeyInterpreter::try_create(ctx.clone(), alter_table_cluster_key_plan)?;
    let _ = interpreter.execute(ctx.clone()).await?;

    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let table_info = table.get_table_info();
    assert_eq!(table_info.meta.cluster_keys, vec!["(id)".to_string()]);
    assert_eq!(table_info.meta.default_cluster_key_id, Some(0));

    let snapshot_loc = table
        .get_table_info()
        .options()
        .get(OPT_KEY_SNAPSHOT_LOCATION)
        .unwrap();
    let reader = MetaReaders::table_snapshot_reader(fuse_table.get_operator());

    let load_params = LoadParams {
        location: snapshot_loc.clone(),
        len_hint: None,
        ver: TableSnapshot::VERSION,
        put_cache: false,
    };

    let snapshot = reader.read(&load_params).await?;
    let expected = Some((0, "(id)".to_string()));

    assert_eq!(snapshot.cluster_key_meta, expected);

    // drop cluster key
    let drop_table_cluster_key_plan = DropTableClusterKeyPlan {
        tenant: fixture.default_tenant().name().to_string(),
        catalog: fixture.default_catalog_name(),
        database: fixture.default_db_name(),
        table: fixture.default_table_name(),
    };
    let interpreter =
        DropTableClusterKeyInterpreter::try_create(ctx.clone(), drop_table_cluster_key_plan)?;
    let _ = interpreter.execute(ctx.clone()).await?;

    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let table_info = table.get_table_info();
    assert_eq!(table_info.meta.default_cluster_key, None);
    assert_eq!(table_info.meta.default_cluster_key_id, None);

    let snapshot_loc = table
        .get_table_info()
        .options()
        .get(OPT_KEY_SNAPSHOT_LOCATION)
        .unwrap();
    let reader = MetaReaders::table_snapshot_reader(fuse_table.get_operator());

    let params = LoadParams {
        location: snapshot_loc.clone(),
        len_hint: None,
        ver: TableSnapshot::VERSION,
        put_cache: false,
    };

    let snapshot = reader.read(&params).await?;
    let expected = None;
    assert_eq!(snapshot.cluster_key_meta, expected);

    Ok(())
}
