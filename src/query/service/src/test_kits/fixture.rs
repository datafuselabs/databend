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
use common_base::base::tokio;
use common_base::base::tokio::time::sleep;
use common_catalog::catalog_kind::CATALOG_DEFAULT;
use common_catalog::cluster_info::Cluster;
use common_catalog::table::AppendMode;
use common_config::InnerConfig;
use common_exception::Result;
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
use common_tracing::set_panic_hook;
use futures::TryStreamExt;
use jsonb::Number as JsonbNumber;
use jsonb::Object as JsonbObject;
use jsonb::Value as JsonbValue;
use parking_lot::Mutex;
use storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use uuid::Uuid;

use crate::api::RpcService;
use crate::clusters::ClusterDiscovery;
use crate::clusters::ClusterHelper;
use crate::interpreters::CreateTableInterpreter;
use crate::interpreters::Interpreter;
use crate::interpreters::InterpreterFactory;
use crate::pipelines::PipelineBuildResult;
use crate::pipelines::PipelineBuilder;
use crate::sessions::QueryContext;
use crate::sessions::QueryContextShared;
use crate::sessions::Session;
use crate::sessions::SessionManager;
use crate::sessions::SessionType;
use crate::sessions::TableContext;
use crate::sql::Planner;
use crate::storages::Table;
use crate::test_kits::execute_pipeline;
use crate::test_kits::ClusterDescriptor;
use crate::test_kits::ConfigBuilder;
use crate::GlobalServices;

pub struct TestFixture {
    pub(crate) default_ctx: Arc<QueryContext>,
    default_session: Arc<Session>,
    conf: InnerConfig,
    prefix: String,
    // Keep in the end.
    // Session will drop first then the guard drop.
    _guard: TestGuard,
}

pub struct TestGuard {
    _thread_name: String,
}

impl TestGuard {
    pub fn new(thread_name: String) -> Self {
        Self {
            _thread_name: thread_name,
        }
    }
}

impl Drop for TestGuard {
    fn drop(&mut self) {
        #[cfg(debug_assertions)]
        common_base::base::GlobalInstance::drop_testing(&self._thread_name);
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
        TestFixture::start_global_services(&self.config).await?;
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

        #[cfg(debug_assertions)]
        {
            let thread_name = std::thread::current().name().unwrap().to_string();
            common_base::base::GlobalInstance::init_testing(&thread_name);
        }

        GlobalServices::init_with(config).await?;
        OssLicenseManager::init(config.query.tenant_id.clone())?;

        Ok(())
    }

    /// This is a mock as start_services in binaries/query/entry.rs
    pub async fn start_global_services(config: &InnerConfig) -> Result<()> {
        // Flight service.
        let mut srv = RpcService::create(config.clone())?;
        srv.start(config.query.flight_api_address.parse()?).await?;

        // Register to metastore.
        ClusterDiscovery::instance()
            .register_to_metastore(&config)
            .await?;

        sleep(tokio::time::Duration::from_secs(5)).await;

        Ok(())
    }

    pub fn default_session(&self) -> Arc<Session> {
        self.default_session.clone()
    }

    /// returns new QueryContext of default session
    pub async fn new_query_ctx(&self) -> Result<Arc<QueryContext>> {
        self.default_session.create_query_context().await
    }

    /// returns new QueryContext of default session with cluster
    pub async fn new_query_ctx_with_cluster(
        &self,
        desc: ClusterDescriptor,
    ) -> Result<Arc<QueryContext>> {
        let local_id = desc.local_node_id;
        let nodes = desc.cluster_nodes_list;

        let dummy_query_context = QueryContext::create_from_shared(QueryContextShared::try_create(
            self.default_session.clone(),
            Cluster::create(nodes, local_id),
        )?);

        dummy_query_context.get_settings().set_max_threads(8)?;
        Ok(dummy_query_context)
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
            DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int32))),
            DataType::Nullable(Box::new(DataType::Number(NumberDataType::Int32))),
        ];
        let tuple_data_type = DataType::Tuple(tuple_inner_data_types);
        let field_id = DataField::new("id", DataType::Number(NumberDataType::Int32));
        let field_t = DataField::new("t", tuple_data_type);
        DataSchemaRefExt::create(vec![
            field_id.with_default_expr(Some("0".to_string())),
            field_t.with_default_expr(Some("(0,0)".to_string())),
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

    /// Create database with prefix.
    pub async fn create_default_database(&self) -> Result<()> {
        let tenant = self.default_ctx.get_tenant();
        let db_name = gen_db_name(&self.prefix);
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

        self.default_ctx
            .get_catalog("default")
            .await
            .unwrap()
            .create_database(plan.into())
            .await?;

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

    /// Generate a stream of blocks with computed columns.
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
