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

use std::io::Write;
use std::sync::Arc;

use databend_common_base::base::tokio;
use databend_common_catalog::table::Table;
use databend_common_exception::Result;
use databend_common_expression::block_debug::box_render;
use databend_common_expression::block_debug::pretty_format_blocks;
use databend_common_meta_app::principal::AuthInfo;
use databend_common_meta_app::principal::AuthType;
use databend_common_meta_app::principal::RoleInfo;
use databend_common_meta_app::principal::UserGrantSet;
use databend_common_meta_app::principal::UserInfo;
use databend_common_meta_app::principal::UserOption;
use databend_common_meta_app::principal::UserQuota;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::storage::StorageFsConfig;
use databend_common_meta_app::storage::StorageParams;
use databend_common_meta_app::storage::StorageS3Config;
use databend_common_sql::executor::table_read_plan::ToReadDataSourcePlan;
use databend_common_storages_system::BuildOptionsTable;
use databend_common_storages_system::CachesTable;
use databend_common_storages_system::CatalogsTable;
use databend_common_storages_system::ClustersTable;
use databend_common_storages_system::ColumnsTable;
use databend_common_storages_system::ConfigsTable;
use databend_common_storages_system::ContributorsTable;
use databend_common_storages_system::CreditsTable;
use databend_common_storages_system::DatabasesTable;
use databend_common_storages_system::EnginesTable;
use databend_common_storages_system::FunctionsTable;
use databend_common_storages_system::MetricsTable;
use databend_common_storages_system::RolesTable;
use databend_common_storages_system::TracingTable;
use databend_common_storages_system::UsersTable;
use databend_common_users::UserApiProvider;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContext;
use databend_query::stream::ReadDataBlockStream;
use databend_query::test_kits::ClusterDescriptor;
use databend_query::test_kits::ConfigBuilder;
use databend_query::test_kits::TestFixture;
use futures::TryStreamExt;
use goldenfile::Mint;
use wiremock::matchers::method;
use wiremock::matchers::path;
use wiremock::Mock;
use wiremock::MockServer;
use wiremock::ResponseTemplate;

async fn run_table_tests(
    file: &mut impl Write,
    ctx: Arc<QueryContext>,
    table: Arc<dyn Table>,
) -> Result<()> {
    let table_info = table.get_table_info();
    writeln!(file, "---------- TABLE INFO ------------").unwrap();
    writeln!(file, "{table_info}").unwrap();
    let source_plan = table.read_plan(ctx.clone(), None, true).await?;

    let stream = table.read_data_block_stream(ctx, &source_plan).await?;
    let blocks = stream.try_collect::<Vec<_>>().await?;
    let formatted = pretty_format_blocks(&blocks).unwrap();
    let mut actual_lines: Vec<&str> = formatted.trim().lines().collect();

    // sort except for header + footer
    let num_lines = actual_lines.len();
    if num_lines > 3 {
        actual_lines.as_mut_slice()[2..num_lines - 1].sort_unstable()
    }
    writeln!(file, "-------- TABLE CONTENTS ----------").unwrap();
    if table_info.name.to_lowercase() == "settings" {
        actual_lines.retain(|&item| {
            !(item.contains("max_threads")
                || item.contains("max_memory_usage")
                || item.contains("max_storage_io_requests")
                || item.contains("recluster_block_size"))
        });
    }
    for line in actual_lines {
        writeln!(file, "{}", line).unwrap();
    }
    write!(file, "\n\n").unwrap();
    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_build_options_table() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    let table = BuildOptionsTable::create(1);
    let source_plan = table.read_plan(ctx.clone(), None, true).await?;

    let stream = table.read_data_block_stream(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 2);
    assert!(block.num_rows() > 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_columns_table() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    let mut mint = Mint::new("tests/it/storages/testdata");
    let file = &mut mint.new_goldenfile("columns_table.txt").unwrap();
    let table = ColumnsTable::create(1);
    run_table_tests(file, ctx, table).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_clusters_table() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    let table = ClustersTable::create(1);
    let source_plan = table.read_plan(ctx.clone(), None, true).await?;

    let stream = table.read_data_block_stream(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 5);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_configs_table_basic() -> Result<()> {
    let mut config = ConfigBuilder::create().build();
    config.storage.params = StorageParams::Fs(StorageFsConfig::default());
    let fixture = TestFixture::setup_with_config(&config).await?;

    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_max_threads(8)?;

    let mut mint = Mint::new("tests/it/storages/testdata");
    let file = &mut mint.new_goldenfile("configs_table_basic.txt").unwrap();

    let table = ConfigsTable::create(1);
    run_table_tests(file, ctx, table).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_configs_table_redact() -> Result<()> {
    let mut mint = Mint::new("tests/it/storages/testdata");
    let _file = &mut mint.new_goldenfile("configs_table_redact.txt").unwrap();

    let mock_server = MockServer::builder().start().await;
    Mock::given(method("HEAD"))
        .and(path("/test/.opendal"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&mock_server)
        .await;

    let mut conf = databend_query::test_kits::ConfigBuilder::create().build();
    conf.storage.params = StorageParams::S3(StorageS3Config {
        region: "us-east-2".to_string(),
        endpoint_url: mock_server.uri(),
        bucket: "test".to_string(),
        access_key_id: "access_key_id".to_string(),
        secret_access_key: "secret_access_key".to_string(),
        ..Default::default()
    });
    let fixture = TestFixture::setup_with_config(&conf).await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_max_threads(8)?;

    let table = ConfigsTable::create(1);
    let source_plan = table.read_plan(ctx.clone(), None, true).await?;

    let stream = table.read_data_block_stream(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 4);
    // need a method to skip/edit endpoint_url
    // run_table_tests(file, ctx, table).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_contributors_table() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    let table = ContributorsTable::create(1);
    let source_plan = table.read_plan(ctx.clone(), None, true).await?;

    let stream = table.read_data_block_stream(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 1);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_credits_table() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    let table = CreditsTable::create(1);
    let source_plan = table.read_plan(ctx.clone(), None, true).await?;

    let stream = table.read_data_block_stream(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 3);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_catalogs_table() -> Result<()> {
    let mut mint = Mint::new("tests/it/storages/testdata");
    let file = &mut mint.new_goldenfile("catalogs_table.txt").unwrap();

    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    let table = CatalogsTable::create(1);
    run_table_tests(file, ctx, table).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_databases_table() -> Result<()> {
    let mut config = ConfigBuilder::create().build();
    config.storage.params = StorageParams::Fs(StorageFsConfig::default());
    let fixture = TestFixture::setup_with_config(&config).await?;
    let ctx = fixture.new_query_ctx().await?;

    let table = DatabasesTable::create(1);

    let mut mint = Mint::new("tests/it/storages/testdata");
    let file = &mut mint.new_goldenfile("databases_table.txt").unwrap();
    run_table_tests(file, ctx, table).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_engines_table() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    let table = EnginesTable::create(1);
    let mut mint = Mint::new("tests/it/storages/testdata");
    let file = &mut mint.new_goldenfile("engines_table.txt").unwrap();
    run_table_tests(file, ctx, table).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_functions_table() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    let table = FunctionsTable::create(1);
    let source_plan = table.read_plan(ctx.clone(), None, true).await?;

    let stream = table.read_data_block_stream(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 5);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_metrics_table() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    let table = MetricsTable::create(1);
    let source_plan = table.read_plan(ctx.clone(), None, true).await?;
    let counter1 =
        databend_common_base::runtime::metrics::register_counter("test_metrics_table_count");
    let histogram1 = databend_common_base::runtime::metrics::register_histogram_in_milliseconds(
        "test_metrics_table_histogram",
    );

    counter1.inc();
    histogram1.observe(2.0);

    let stream = table.read_data_block_stream(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 5);
    assert!(block.num_rows() >= 1);

    let output = box_render(
        &Arc::new(source_plan.output_schema.into()),
        result.as_slice(),
        1000,
        1024,
        30,
        true,
    )?;
    assert!(output.contains("test_metrics_table_count"));
    assert!(output.contains("test_metrics_table_histogram"));

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_roles_table() -> Result<()> {
    let mut mint = Mint::new("tests/it/storages/testdata");
    let file = &mut mint.new_goldenfile("roles_table.txt").unwrap();

    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_max_threads(2)?;

    let tenant = ctx.get_tenant();

    {
        let role_info = RoleInfo::new("test");
        UserApiProvider::instance()
            .add_role(&tenant, role_info, false)
            .await?;
    }

    {
        let mut role_info = RoleInfo::new("test1");
        role_info.grants.grant_role("test".to_string());
        UserApiProvider::instance()
            .add_role(&tenant, role_info, false)
            .await?;
    }
    let table = RolesTable::create(1);
    run_table_tests(file, ctx, table).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_tracing_table() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;

    let table: Arc<dyn Table> = Arc::new(TracingTable::create(1));
    let source_plan = table.read_plan(ctx.clone(), None, true).await?;

    let stream = table.read_data_block_stream(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 1);
    assert!(block.num_rows() > 0);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_users_table() -> Result<()> {
    let mut mint = Mint::new("tests/it/storages/testdata");
    let file = &mut mint.new_goldenfile("users_table.txt").unwrap();

    let fixture = TestFixture::setup().await?;
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_max_threads(2)?;

    let tenant = ctx.get_tenant();
    let auth_data = AuthInfo::None;
    UserApiProvider::instance()
        .add_user(
            &tenant,
            UserInfo {
                auth_info: auth_data,
                name: "test".to_string(),
                hostname: "%".to_string(),
                grants: UserGrantSet::empty(),
                quota: UserQuota::no_limit(),
                option: UserOption::default(),
                history_auth_infos: vec![],
                password_fails: vec![],
                password_update_on: None,
                lockout_time: None,
            },
            &CreateOption::Create,
        )
        .await?;
    let auth_data = AuthInfo::new(AuthType::Sha256Password, &Some("123456789".to_string()));
    assert!(auth_data.is_ok());
    UserApiProvider::instance()
        .add_user(
            &tenant,
            UserInfo {
                auth_info: auth_data.unwrap(),
                name: "test1".to_string(),
                hostname: "%".to_string(),
                grants: UserGrantSet::empty(),
                quota: UserQuota::no_limit(),
                option: UserOption::default().with_default_role(Some("role1".to_string())),
                history_auth_infos: vec![],
                password_fails: vec![],
                password_update_on: None,
                lockout_time: None,
            },
            &CreateOption::Create,
        )
        .await?;

    let table = UsersTable::create(1);
    run_table_tests(file, ctx, table).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_caches_table() -> Result<()> {
    let mut mint = Mint::new("tests/it/storages/testdata");
    let file = &mut mint.new_goldenfile("caches_table.txt").unwrap();

    let mut config = ConfigBuilder::create().build();
    config.storage.params = StorageParams::Fs(StorageFsConfig::default());
    let fixture = TestFixture::setup_with_config(&config).await?;
    let cluster_desc = ClusterDescriptor::new().with_local_id("test-node");
    let ctx = fixture.new_query_ctx_with_cluster(cluster_desc).await?;

    let table = CachesTable::create(1);
    run_table_tests(file, ctx, table).await?;

    Ok(())
}
