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

use std::collections::VecDeque;
use std::str;
use std::sync::Arc;

use common_ast::ast::Engine;
use common_catalog::catalog_kind::CATALOG_DEFAULT;
use common_catalog::table::AppendMode;
use common_config::GlobalConfig;
use common_config::InnerConfig;
use common_exception::Result;
use common_expression::block_debug::assert_blocks_sorted_eq_with_name;
use common_expression::infer_table_schema;
use common_expression::types::number::Int32Type;
use common_expression::types::number::Int64Type;
use common_expression::types::string::StringColumnBuilder;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::StringType;
use common_expression::Column;
use common_expression::ComputedExpr;
use common_expression::DataBlock;
use common_expression::DataField;
use common_expression::DataSchemaRef;
use common_expression::DataSchemaRefExt;
use common_expression::FromData;
use common_expression::SendableDataBlockStream;
use common_expression::TableDataType;
use common_expression::TableField;
use common_expression::TableSchemaRef;
use common_expression::TableSchemaRefExt;
use common_license::license_manager::LicenseManager;
use common_license::license_manager::OssLicenseManager;
use common_meta_app::principal::AuthInfo;
use common_meta_app::principal::GrantObject;
use common_meta_app::principal::PasswordHashMethod;
use common_meta_app::principal::UserInfo;
use common_meta_app::principal::UserPrivilegeSet;
use common_meta_app::schema::DatabaseMeta;
use common_meta_app::storage::StorageParams;
use common_pipeline_core::processors::ProcessorPtr;
use common_pipeline_sinks::EmptySink;
use common_pipeline_sources::BlocksSource;
use common_sql::plans::CreateDatabasePlan;
use common_sql::plans::CreateTablePlan;
use common_sql::plans::DeletePlan;
use common_sql::plans::UpdatePlan;
use common_storages_fuse::FuseTable;
use common_storages_fuse::FUSE_TBL_BLOCK_PREFIX;
use common_storages_fuse::FUSE_TBL_LAST_SNAPSHOT_HINT;
use common_storages_fuse::FUSE_TBL_SEGMENT_PREFIX;
use common_storages_fuse::FUSE_TBL_SNAPSHOT_PREFIX;
use common_storages_fuse::FUSE_TBL_SNAPSHOT_STATISTICS_PREFIX;
use common_storages_fuse::FUSE_TBL_XOR_BLOOM_INDEX_PREFIX;
use common_tracing::set_panic_hook;
use futures::TryStreamExt;
use jsonb::Number as JsonbNumber;
use jsonb::Object as JsonbObject;
use jsonb::Value as JsonbValue;
use log::info;
use parking_lot::Mutex;
use storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use uuid::Uuid;
use walkdir::WalkDir;

use crate::clusters::ClusterDiscovery;
use crate::interpreters::CreateTableInterpreter;
use crate::interpreters::DeleteInterpreter;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterFactory;
use crate::interpreters::UpdateInterpreter;
use crate::pipelines::executor::ExecutorSettings;
use crate::pipelines::executor::PipelineCompleteExecutor;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::PipelineBuilder;
use crate::sessions::QueryContext;
use crate::sessions::Session;
use crate::sessions::SessionManager;
use crate::sessions::SessionType;
use crate::sessions::TableContext;
use crate::sql::Planner;
use crate::storages::Table;
use crate::test_kits::ConfigBuilder;
use crate::GlobalServices;

pub struct TestFixture {
    default_ctx: Arc<QueryContext>,
    default_session: Arc<Session>,
    conf: InnerConfig,
    prefix: String,
    // Keep in the end.
    // Session will drop first then the guard drop.
    _guard: TestGuard,
}

pub struct TestGuard {
    thread_name: String,
}

impl TestGuard {
    pub fn new(thread_name: String) -> Self {
        Self { thread_name }
    }
}

impl Drop for TestGuard {
    fn drop(&mut self) {
        #[cfg(debug_assertions)]
        common_base::base::GlobalInstance::drop_testing(&self.thread_name);
    }
}

#[async_trait::async_trait]
pub trait Setup {
    async fn setup(&self) -> Result<InnerConfig>;
}

struct OSSSetup {
    config: InnerConfig,
}

#[async_trait::async_trait]
impl Setup for OSSSetup {
    async fn setup(&self) -> Result<InnerConfig> {
        TestFixture::init_global_with_config(&self.config).await?;
        Ok(self.config.clone())
    }
}

impl TestFixture {
    /// Create a new TestFixture with default config.
    pub async fn setup() -> Result<TestFixture> {
        let config = ConfigBuilder::create().config();
        Self::setup_with_custom(OSSSetup { config }).await
    }

    /// Create a new TestFixture with setup impl.
    pub async fn setup_with_custom(setup: impl Setup) -> Result<TestFixture> {
        let conf = setup.setup().await?;

        // This will use a max_active_sessions number.
        let default_session = Self::create_session(SessionType::Dummy).await?;
        let default_ctx = default_session.create_query_context().await?;

        let random_prefix: String = Uuid::new_v4().simple().to_string();

        // prepare a randomly named default database
        {
            let tenant = default_ctx.get_tenant();
            let db_name = gen_db_name(&random_prefix);
            let plan = CreateDatabasePlan {
                catalog: "default".to_owned(),
                tenant,
                if_not_exists: false,
                database: db_name,
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..Default::default()
                },
            };

            default_ctx
                .get_catalog("default")
                .await
                .unwrap()
                .create_database(plan.into())
                .await?;
        }

        let thread_name = std::thread::current().name().unwrap().to_string();
        let guard = TestGuard::new(thread_name.clone());
        Ok(Self {
            default_ctx,
            default_session,
            conf,
            prefix: random_prefix,
            _guard: guard,
        })
    }

    pub async fn setup_with_config(config: &InnerConfig) -> Result<TestFixture> {
        Self::setup_with_custom(OSSSetup {
            config: config.clone(),
        })
        .await
    }

    async fn create_session(session_type: SessionType) -> Result<Arc<Session>> {
        let mut user_info = UserInfo::new("root", "%", AuthInfo::Password {
            hash_method: PasswordHashMethod::Sha256,
            hash_value: Vec::from("pass"),
        });

        user_info.grants.grant_privileges(
            &GrantObject::Global,
            UserPrivilegeSet::available_privileges_on_global(),
        );

        user_info.grants.grant_privileges(
            &GrantObject::Global,
            UserPrivilegeSet::available_privileges_on_stage(),
        );

        let dummy_session = SessionManager::instance()
            .create_session(session_type)
            .await?;

        dummy_session.set_authed_user(user_info, None).await?;
        dummy_session.get_settings().set_max_threads(8)?;

        Ok(dummy_session)
    }

    /// Setup the test environment.
    /// Set the panic hook.
    /// Set the unit test env.
    /// Init the global instance.
    /// Init the global services.
    /// Init the license manager.
    /// Register the cluster to the metastore.
    async fn init_global_with_config(config: &InnerConfig) -> Result<()> {
        set_panic_hook();
        std::env::set_var("UNIT_TEST", "TRUE");

        let thread_name = std::thread::current().name().unwrap().to_string();
        #[cfg(debug_assertions)]
        common_base::base::GlobalInstance::init_testing(&thread_name);

        GlobalServices::init_with(config).await?;
        OssLicenseManager::init(config.query.tenant_id.clone())?;

        // Cluster register.
        {
            ClusterDiscovery::instance()
                .register_to_metastore(config)
                .await?;
            info!(
                "Databend query unit test setup registered:{:?} to metasrv:{:?}.",
                config.query.cluster_id, config.meta.endpoints
            );
        }

        Ok(())
    }

    pub fn default_session(&self) -> Arc<Session> {
        self.default_session.clone()
    }

    /// returns new QueryContext of default session
    pub async fn new_query_ctx(&self) -> Result<Arc<QueryContext>> {
        self.default_session.create_query_context().await
    }

    pub async fn new_session_with_type(&self, session_type: SessionType) -> Result<Arc<Session>> {
        Self::create_session(session_type).await
    }

    pub fn storage_root(&self) -> &str {
        match &self.conf.storage.params {
            StorageParams::Fs(fs) => &fs.root,
            _ => {
                unreachable!()
            }
        }
    }

    pub fn default_tenant(&self) -> String {
        self.conf.query.tenant_id.clone()
    }

    pub fn default_db_name(&self) -> String {
        gen_db_name(&self.prefix)
    }

    pub fn default_catalog_name(&self) -> String {
        "default".to_owned()
    }

    pub fn default_table_name(&self) -> String {
        format!("tbl_{}", self.prefix)
    }

    pub fn default_schema() -> DataSchemaRef {
        let tuple_inner_data_types = vec![
            DataType::Number(NumberDataType::Int32),
            DataType::Number(NumberDataType::Int32),
        ];
        let tuple_data_type = DataType::Tuple(tuple_inner_data_types);
        DataSchemaRefExt::create(vec![
            DataField::new("id", DataType::Number(NumberDataType::Int32)),
            DataField::new("t", tuple_data_type),
        ])
    }

    pub fn default_table_schema() -> TableSchemaRef {
        infer_table_schema(&Self::default_schema()).unwrap()
    }

    pub fn default_create_table_plan(&self) -> CreateTablePlan {
        CreateTablePlan {
            if_not_exists: false,
            tenant: self.default_tenant(),
            catalog: self.default_catalog_name(),
            database: self.default_db_name(),
            table: self.default_table_name(),
            schema: TestFixture::default_table_schema(),
            engine: Engine::Fuse,
            storage_params: None,
            read_only_attach: false,
            part_prefix: "".to_string(),
            options: [
                // database id is required for FUSE
                (OPT_KEY_DATABASE_ID.to_owned(), "1".to_owned()),
            ]
            .into(),
            field_comments: vec!["number".to_string(), "tuple".to_string()],
            as_select: None,
            cluster_key: Some("(id)".to_string()),
        }
    }

    // create a normal table without cluster key.
    pub fn normal_create_table_plan(&self) -> CreateTablePlan {
        CreateTablePlan {
            if_not_exists: false,
            tenant: self.default_tenant(),
            catalog: self.default_catalog_name(),
            database: self.default_db_name(),
            table: self.default_table_name(),
            schema: TestFixture::default_table_schema(),
            engine: Engine::Fuse,
            storage_params: None,
            read_only_attach: false,
            part_prefix: "".to_string(),
            options: [
                // database id is required for FUSE
                (OPT_KEY_DATABASE_ID.to_owned(), "1".to_owned()),
            ]
            .into(),
            field_comments: vec!["number".to_string(), "tuple".to_string()],
            as_select: None,
            cluster_key: None,
        }
    }

    pub fn variant_schema() -> DataSchemaRef {
        DataSchemaRefExt::create(vec![
            DataField::new("id", DataType::Number(NumberDataType::Int32)),
            DataField::new("v", DataType::Variant),
        ])
    }

    pub fn variant_table_schema() -> TableSchemaRef {
        infer_table_schema(&Self::variant_schema()).unwrap()
    }

    // create a variant table
    pub fn variant_create_table_plan(&self) -> CreateTablePlan {
        CreateTablePlan {
            if_not_exists: false,
            tenant: self.default_tenant(),
            catalog: self.default_catalog_name(),
            database: self.default_db_name(),
            table: self.default_table_name(),
            schema: TestFixture::variant_table_schema(),
            engine: Engine::Fuse,
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
        }
    }

    pub fn computed_schema() -> DataSchemaRef {
        DataSchemaRefExt::create(vec![
            DataField::new("id", DataType::Number(NumberDataType::Int32)),
            DataField::new("a1", DataType::String)
                .with_computed_expr(Some(ComputedExpr::Virtual("reverse(c)".to_string()))),
            DataField::new("a2", DataType::String)
                .with_computed_expr(Some(ComputedExpr::Stored("upper(c)".to_string()))),
            DataField::new("b1", DataType::Number(NumberDataType::Int64))
                .with_computed_expr(Some(ComputedExpr::Virtual("(d + 2)".to_string()))),
            DataField::new("b2", DataType::Number(NumberDataType::Int64))
                .with_computed_expr(Some(ComputedExpr::Stored("((d + 1) * 3)".to_string()))),
            DataField::new("c", DataType::String),
            DataField::new("d", DataType::Number(NumberDataType::Int64)),
        ])
    }

    pub fn computed_table_schema() -> TableSchemaRef {
        infer_table_schema(&Self::computed_schema()).unwrap()
    }

    // create a table with computed column
    pub fn computed_create_table_plan(&self) -> CreateTablePlan {
        CreateTablePlan {
            if_not_exists: false,
            tenant: self.default_tenant(),
            catalog: self.default_catalog_name(),
            database: self.default_db_name(),
            table: self.default_table_name(),
            schema: TestFixture::computed_table_schema(),
            engine: Engine::Fuse,
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
        }
    }

    pub async fn create_default_table(&self) -> Result<()> {
        let create_table_plan = self.default_create_table_plan();
        let interpreter =
            CreateTableInterpreter::try_create(self.default_ctx.clone(), create_table_plan)?;
        interpreter.execute(self.default_ctx.clone()).await?;
        Ok(())
    }

    pub async fn create_normal_table(&self) -> Result<()> {
        let create_table_plan = self.normal_create_table_plan();
        let interpreter =
            CreateTableInterpreter::try_create(self.default_ctx.clone(), create_table_plan)?;
        interpreter.execute(self.default_ctx.clone()).await?;
        Ok(())
    }

    pub async fn create_variant_table(&self) -> Result<()> {
        let create_table_plan = self.variant_create_table_plan();
        let interpreter =
            CreateTableInterpreter::try_create(self.default_ctx.clone(), create_table_plan)?;
        interpreter.execute(self.default_ctx.clone()).await?;
        Ok(())
    }

    pub async fn create_computed_table(&self) -> Result<()> {
        let create_table_plan = self.computed_create_table_plan();
        let interpreter =
            CreateTableInterpreter::try_create(self.default_ctx.clone(), create_table_plan)?;
        interpreter.execute(self.default_ctx.clone()).await?;
        Ok(())
    }

    pub fn gen_sample_blocks(
        num_of_blocks: usize,
        start: i32,
    ) -> (TableSchemaRef, Vec<Result<DataBlock>>) {
        Self::gen_sample_blocks_ex(num_of_blocks, 3, start)
    }

    pub fn gen_sample_blocks_ex(
        num_of_block: usize,
        rows_per_block: usize,
        start: i32,
    ) -> (TableSchemaRef, Vec<Result<DataBlock>>) {
        let repeat = rows_per_block % 3 == 0;
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("id", TableDataType::Number(NumberDataType::Int32)),
            TableField::new("t", TableDataType::Tuple {
                fields_name: vec!["a".to_string(), "b".to_string()],
                fields_type: vec![
                    TableDataType::Number(NumberDataType::Int32),
                    TableDataType::Number(NumberDataType::Int32),
                ],
            }),
        ]);
        (
            schema,
            (0..num_of_block)
                .map(|idx| {
                    let mut curr = idx as i32 + start;
                    let column0 = Int32Type::from_data(
                        std::iter::repeat_with(|| {
                            let tmp = curr;
                            if !repeat {
                                curr *= 2;
                            }
                            tmp
                        })
                        .take(rows_per_block)
                        .collect::<Vec<i32>>(),
                    );
                    let column1 = Int32Type::from_data(
                        std::iter::repeat_with(|| (idx as i32 + start) * 2)
                            .take(rows_per_block)
                            .collect::<Vec<i32>>(),
                    );
                    let column2 = Int32Type::from_data(
                        std::iter::repeat_with(|| (idx as i32 + start) * 3)
                            .take(rows_per_block)
                            .collect::<Vec<i32>>(),
                    );
                    let tuple_inner_columns = vec![column1, column2];
                    let tuple_column = Column::Tuple(tuple_inner_columns);

                    let columns = vec![column0, tuple_column];

                    Ok(DataBlock::new_from_columns(columns))
                })
                .collect(),
        )
    }

    pub fn gen_sample_blocks_stream(num: usize, start: i32) -> SendableDataBlockStream {
        let (_, blocks) = Self::gen_sample_blocks(num, start);
        Box::pin(futures::stream::iter(blocks))
    }

    pub fn gen_sample_blocks_stream_ex(
        num_of_block: usize,
        rows_perf_block: usize,
        val_start_from: i32,
    ) -> SendableDataBlockStream {
        let (_, blocks) = Self::gen_sample_blocks_ex(num_of_block, rows_perf_block, val_start_from);
        Box::pin(futures::stream::iter(blocks))
    }

    pub fn gen_variant_sample_blocks(
        num_of_blocks: usize,
        start: i32,
    ) -> (TableSchemaRef, Vec<Result<DataBlock>>) {
        Self::gen_variant_sample_blocks_ex(num_of_blocks, 3, start)
    }

    pub fn gen_variant_sample_blocks_ex(
        num_of_block: usize,
        rows_per_block: usize,
        start: i32,
    ) -> (TableSchemaRef, Vec<Result<DataBlock>>) {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("id", TableDataType::Number(NumberDataType::Int32)),
            TableField::new("v", TableDataType::Variant),
        ]);
        (
            schema,
            (0..num_of_block)
                .map(|idx| {
                    let id_column = Int32Type::from_data(
                        std::iter::repeat_with(|| idx as i32 + start)
                            .take(rows_per_block)
                            .collect::<Vec<i32>>(),
                    );

                    let mut builder =
                        StringColumnBuilder::with_capacity(rows_per_block, rows_per_block * 10);
                    for i in 0..rows_per_block {
                        let mut obj = JsonbObject::new();
                        obj.insert(
                            "a".to_string(),
                            JsonbValue::Number(JsonbNumber::Int64((idx + i) as i64)),
                        );
                        obj.insert(
                            "b".to_string(),
                            JsonbValue::Number(JsonbNumber::Int64(((idx + i) * 2) as i64)),
                        );
                        let val = JsonbValue::Object(obj);
                        val.write_to_vec(&mut builder.data);
                        builder.commit_row();
                    }
                    let variant_column = Column::Variant(builder.build());

                    let columns = vec![id_column, variant_column];

                    Ok(DataBlock::new_from_columns(columns))
                })
                .collect(),
        )
    }

    pub fn gen_variant_sample_blocks_stream(num: usize, start: i32) -> SendableDataBlockStream {
        let (_, blocks) = Self::gen_variant_sample_blocks(num, start);
        Box::pin(futures::stream::iter(blocks))
    }

    pub fn gen_computed_sample_blocks(
        num_of_blocks: usize,
        start: i32,
    ) -> (TableSchemaRef, Vec<Result<DataBlock>>) {
        Self::gen_computed_sample_blocks_ex(num_of_blocks, 3, start)
    }

    pub fn gen_computed_sample_blocks_ex(
        num_of_block: usize,
        rows_per_block: usize,
        start: i32,
    ) -> (TableSchemaRef, Vec<Result<DataBlock>>) {
        let schema = Arc::new(Self::computed_table_schema().remove_computed_fields());
        (
            schema,
            (0..num_of_block)
                .map(|_| {
                    let mut id_values = Vec::with_capacity(rows_per_block);
                    let mut c_values = Vec::with_capacity(rows_per_block);
                    let mut d_values = Vec::with_capacity(rows_per_block);
                    for i in 0..rows_per_block {
                        id_values.push(i as i32 + start * 3);
                        c_values.push(format!("s-{}-{}", start, i).as_bytes().to_vec());
                        d_values.push(i as i64 + (start * 10) as i64);
                    }
                    let column0 = Int32Type::from_data(id_values);
                    let column1 = StringType::from_data(c_values);
                    let column2 = Int64Type::from_data(d_values);
                    let columns = vec![column0, column1, column2];

                    Ok(DataBlock::new_from_columns(columns))
                })
                .collect(),
        )
    }

    pub fn gen_computed_sample_blocks_stream(num: usize, start: i32) -> SendableDataBlockStream {
        let (_, blocks) = Self::gen_computed_sample_blocks(num, start);
        Box::pin(futures::stream::iter(blocks))
    }

    pub async fn latest_default_table(&self) -> Result<Arc<dyn Table>> {
        // table got from catalog is always fresh
        self.default_ctx
            .get_catalog(CATALOG_DEFAULT)
            .await?
            .get_table(
                self.default_tenant().as_str(),
                self.default_db_name().as_str(),
                self.default_table_name().as_str(),
            )
            .await
    }

    /// append_commit_blocks with single thread
    pub async fn append_commit_blocks(
        &self,
        table: Arc<dyn Table>,
        blocks: Vec<DataBlock>,
        overwrite: bool,
        commit: bool,
    ) -> Result<()> {
        let source_schema = &table.schema().remove_computed_fields();
        let mut build_res = PipelineBuildResult::create();

        let ctx = self.new_query_ctx().await?;

        let blocks = Arc::new(Mutex::new(VecDeque::from_iter(blocks)));
        build_res.main_pipeline.add_source(
            |output| BlocksSource::create(ctx.clone(), output, blocks.clone()),
            1,
        )?;

        let data_schema: DataSchemaRef = Arc::new(source_schema.into());
        PipelineBuilder::build_fill_missing_columns_pipeline(
            ctx.clone(),
            &mut build_res.main_pipeline,
            table.clone(),
            data_schema,
        )?;

        table.append_data(
            ctx.clone(),
            &mut build_res.main_pipeline,
            AppendMode::Normal,
        )?;
        if commit {
            table.commit_insertion(
                ctx.clone(),
                &mut build_res.main_pipeline,
                None,
                vec![],
                overwrite,
                None,
            )?;
        } else {
            build_res
                .main_pipeline
                .add_sink(|input| Ok(ProcessorPtr::create(EmptySink::create(input))))?;
        }

        execute_pipeline(ctx, build_res)
    }

    pub async fn execute_command(&self, query: &str) -> Result<()> {
        let res = self.execute_query(query).await?;
        res.try_collect::<Vec<DataBlock>>().await?;
        Ok(())
    }

    pub async fn execute_query(&self, query: &str) -> Result<SendableDataBlockStream> {
        let ctx = self.new_query_ctx().await?;
        let mut planner = Planner::new(ctx.clone());
        let (plan, _) = planner.plan_sql(query).await?;
        let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
        executor.execute(ctx).await
    }
}

fn gen_db_name(prefix: &str) -> String {
    format!("db_{}", prefix)
}

pub fn expects_err<T>(case_name: &str, err_code: u16, res: Result<T>) {
    if let Err(err) = res {
        assert_eq!(
            err.code(),
            err_code,
            "case name {}, unexpected error: {}",
            case_name,
            err
        );
    } else {
        panic!(
            "case name {}, expecting err code {}, but got ok",
            case_name, err_code,
        );
    }
}

pub async fn expects_ok(
    case_name: impl AsRef<str>,
    res: Result<SendableDataBlockStream>,
    expected: Vec<&str>,
) -> Result<()> {
    match res {
        Ok(stream) => {
            let blocks: Vec<DataBlock> = stream.try_collect().await?;
            assert_blocks_sorted_eq_with_name(case_name.as_ref(), expected, &blocks)
        }
        Err(err) => {
            panic!(
                "case name {}, expecting  Ok, but got err {}",
                case_name.as_ref(),
                err,
            )
        }
    };
    Ok(())
}

pub async fn execute_query(ctx: Arc<QueryContext>, query: &str) -> Result<SendableDataBlockStream> {
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(query).await?;
    let executor = InterpreterFactory::get(ctx.clone(), &plan).await?;
    executor.execute(ctx.clone()).await
}

pub fn execute_pipeline(ctx: Arc<QueryContext>, mut res: PipelineBuildResult) -> Result<()> {
    let query_id = ctx.get_id();
    let executor_settings = ExecutorSettings::try_create(&ctx.get_settings(), query_id)?;
    res.set_max_threads(ctx.get_settings().get_max_threads()? as usize);
    let mut pipelines = res.sources_pipelines;
    pipelines.push(res.main_pipeline);
    let executor = PipelineCompleteExecutor::from_pipelines(pipelines, executor_settings)?;
    ctx.set_executor(executor.get_inner())?;
    executor.execute()
}

pub async fn execute_command(ctx: Arc<QueryContext>, query: &str) -> Result<()> {
    let res = execute_query(ctx, query).await?;
    res.try_collect::<Vec<DataBlock>>().await?;
    Ok(())
}

pub async fn append_sample_data(num_blocks: usize, fixture: &TestFixture) -> Result<()> {
    append_sample_data_overwrite(num_blocks, false, fixture).await
}

pub async fn analyze_table(fixture: &TestFixture) -> Result<()> {
    let table = fixture.latest_default_table().await?;
    table.analyze(fixture.default_ctx.clone()).await
}

pub async fn do_deletion(ctx: Arc<QueryContext>, plan: DeletePlan) -> Result<()> {
    let delete_interpreter = DeleteInterpreter::try_create(ctx.clone(), plan.clone())?;
    delete_interpreter.execute(ctx).await?;
    Ok(())
}

pub async fn do_update(ctx: Arc<QueryContext>, plan: UpdatePlan) -> Result<()> {
    let update_interpreter = UpdateInterpreter::try_create(ctx.clone(), plan)?;
    update_interpreter.execute(ctx).await?;
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

pub async fn append_computed_sample_data(num_blocks: usize, fixture: &TestFixture) -> Result<()> {
    let stream = TestFixture::gen_computed_sample_blocks_stream(num_blocks, 1);
    let table = fixture.latest_default_table().await?;

    let blocks = stream.try_collect().await?;
    fixture
        .append_commit_blocks(table.clone(), blocks, true, true)
        .await
}

pub async fn check_data_dir(
    fixture: &TestFixture,
    case_name: &str,
    snapshot_count: u32,
    table_statistic_count: u32,
    segment_count: u32,
    block_count: u32,
    index_count: u32,
    check_last_snapshot: Option<()>,
    check_table_statistic_file: Option<()>,
) -> Result<()> {
    let data_path = match &GlobalConfig::instance().storage.params {
        StorageParams::Fs(v) => v.root.clone(),
        _ => panic!("storage type is not fs"),
    };
    let root = data_path.as_str();
    let mut ss_count = 0;
    let mut ts_count = 0;
    let mut sg_count = 0;
    let mut b_count = 0;
    let mut i_count = 0;
    let mut last_snapshot_loc = "".to_string();
    let mut table_statistic_files = vec![];
    let prefix_snapshot = FUSE_TBL_SNAPSHOT_PREFIX;
    let prefix_snapshot_statistics = FUSE_TBL_SNAPSHOT_STATISTICS_PREFIX;
    let prefix_segment = FUSE_TBL_SEGMENT_PREFIX;
    let prefix_block = FUSE_TBL_BLOCK_PREFIX;
    let prefix_index = FUSE_TBL_XOR_BLOOM_INDEX_PREFIX;
    let prefix_last_snapshot_hint = FUSE_TBL_LAST_SNAPSHOT_HINT;
    for entry in WalkDir::new(root) {
        let entry = entry.unwrap();
        if entry.file_type().is_file() {
            let (_, entry_path) = entry.path().to_str().unwrap().split_at(root.len());
            // trim the leading prefix, e.g. "/db_id/table_id/"
            let path = entry_path.split('/').skip(3).collect::<Vec<_>>();
            let path = path[0];
            if path.starts_with(prefix_snapshot) {
                ss_count += 1;
            } else if path.starts_with(prefix_segment) {
                sg_count += 1;
            } else if path.starts_with(prefix_block) {
                b_count += 1;
            } else if path.starts_with(prefix_index) {
                i_count += 1;
            } else if path.starts_with(prefix_snapshot_statistics) {
                ts_count += 1;
                table_statistic_files.push(entry_path.to_string());
            } else if path.starts_with(prefix_last_snapshot_hint) && check_last_snapshot.is_some() {
                let content = fixture
                    .default_ctx
                    .get_data_operator()?
                    .operator()
                    .read(entry_path)
                    .await?;
                last_snapshot_loc = str::from_utf8(&content)?.to_string();
            }
        }
    }

    assert_eq!(
        ss_count, snapshot_count,
        "case [{}], check snapshot count",
        case_name
    );
    assert_eq!(
        ts_count, table_statistic_count,
        "case [{}], check snapshot statistics count",
        case_name
    );
    assert_eq!(
        sg_count, segment_count,
        "case [{}], check segment count",
        case_name
    );

    assert_eq!(
        b_count, block_count,
        "case [{}], check block count",
        case_name
    );

    assert_eq!(
        i_count, index_count,
        "case [{}], check index count",
        case_name
    );

    if check_last_snapshot.is_some() {
        let table = fixture.latest_default_table().await?;
        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        let snapshot_loc = fuse_table.snapshot_loc().await?;
        let snapshot_loc = snapshot_loc.unwrap();
        assert!(last_snapshot_loc.contains(&snapshot_loc));
        assert_eq!(
            last_snapshot_loc.find(&snapshot_loc),
            Some(last_snapshot_loc.len() - snapshot_loc.len())
        );
    }

    if check_table_statistic_file.is_some() {
        let table = fixture.latest_default_table().await?;
        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        let snapshot_opt = fuse_table.read_table_snapshot().await?;
        assert!(snapshot_opt.is_some());
        let snapshot = snapshot_opt.unwrap();
        let ts_location_opt = snapshot.table_statistics_location.clone();
        assert!(ts_location_opt.is_some());
        let ts_location = ts_location_opt.unwrap();
        println!(
            "ts_location_opt: {:?}, table_statistic_files: {:?}",
            ts_location, table_statistic_files
        );
        assert!(
            table_statistic_files
                .iter()
                .any(|e| e.contains(&ts_location))
        );
    }

    Ok(())
}

pub async fn history_should_have_item(
    fixture: &TestFixture,
    case_name: &str,
    item_cnt: u32,
) -> Result<()> {
    // check history
    let db = fixture.default_db_name();
    let tbl = fixture.default_table_name();
    let count_str = format!("| {}        |", item_cnt);
    let expected = vec![
        "+----------+",
        "| Column 0 |",
        "+----------+",
        count_str.as_str(),
        "+----------+",
    ];

    let qry = format!(
        "select count(*) as count from fuse_snapshot('{}', '{}')",
        db, tbl
    );

    expects_ok(
        format!("{}: count_of_history_item_should_be_1", case_name),
        fixture.execute_query(qry.as_str()).await,
        expected,
    )
    .await
}
