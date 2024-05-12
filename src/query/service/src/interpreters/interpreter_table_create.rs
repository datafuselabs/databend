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

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::LazyLock;

use chrono::Utc;
use databend_common_base::runtime::GlobalIORuntime;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::is_internal_column;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_io::constants::DEFAULT_BLOCK_MAX_ROWS;
use databend_common_license::license::Feature::ComputedColumn;
use databend_common_license::license_manager::get_license_manager;
use databend_common_management::RoleApi;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_meta_app::schema::TableStatistics;
use databend_common_meta_app::schema::UndropTableByIdReq;
use databend_common_meta_types::MatchSeq;
use databend_common_sql::field_default_value;
use databend_common_sql::plans::CreateTablePlan;
use databend_common_sql::BloomIndexColumns;
use databend_common_storage::DataOperator;
use databend_common_storages_fuse::io::MetaReaders;
use databend_common_storages_fuse::FUSE_OPT_KEY_BLOCK_IN_MEM_SIZE_THRESHOLD;
use databend_common_storages_fuse::FUSE_OPT_KEY_BLOCK_PER_SEGMENT;
use databend_common_storages_fuse::FUSE_OPT_KEY_ROW_AVG_DEPTH_THRESHOLD;
use databend_common_storages_fuse::FUSE_OPT_KEY_ROW_PER_BLOCK;
use databend_common_storages_fuse::FUSE_OPT_KEY_ROW_PER_PAGE;
use databend_common_storages_fuse::FUSE_TBL_LAST_SNAPSHOT_HINT;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;
use databend_storages_common_cache::LoadParams;
use databend_storages_common_index::BloomIndex;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::table::OPT_KEY_BLOOM_INDEX_COLUMNS;
use databend_storages_common_table_meta::table::OPT_KEY_CHANGE_TRACKING;
use databend_storages_common_table_meta::table::OPT_KEY_COMMENT;
use databend_storages_common_table_meta::table::OPT_KEY_CONNECTION_NAME;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use databend_storages_common_table_meta::table::OPT_KEY_ENGINE;
use databend_storages_common_table_meta::table::OPT_KEY_LOCATION;
use databend_storages_common_table_meta::table::OPT_KEY_RANDOM_SEED;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use databend_storages_common_table_meta::table::OPT_KEY_STORAGE_FORMAT;
use databend_storages_common_table_meta::table::OPT_KEY_STORAGE_PREFIX;
use databend_storages_common_table_meta::table::OPT_KEY_TABLE_ATTACHED_READ_ONLY;
use databend_storages_common_table_meta::table::OPT_KEY_TABLE_COMPRESSION;
use log::error;
use log::info;

use crate::interpreters::InsertInterpreter;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContext;
use crate::sql::plans::insert::Insert;
use crate::sql::plans::insert::InsertInputSource;
use crate::sql::plans::Plan;
use crate::storages::StorageDescription;

pub struct CreateTableInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateTablePlan,
}

impl CreateTableInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateTablePlan) -> Result<Self> {
        Ok(CreateTableInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateTableInterpreter {
    fn name(&self) -> &str {
        "CreateTableInterpreterV2"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let tenant = &self.plan.tenant;

        let has_computed_column = self
            .plan
            .schema
            .fields()
            .iter()
            .any(|f| f.computed_expr().is_some());
        if has_computed_column {
            let license_manager = get_license_manager();
            license_manager
                .manager
                .check_enterprise_enabled(self.ctx.get_license_key(), ComputedColumn)?;
        }

        let quota_api = UserApiProvider::instance().tenant_quota_api(tenant);
        let quota = quota_api.get_quota(MatchSeq::GE(0)).await?.data;
        let engine = self.plan.engine;
        let catalog = self.ctx.get_catalog(self.plan.catalog.as_str()).await?;
        if quota.max_tables_per_database > 0 {
            // Note:
            // max_tables_per_database is a config quota. Default is 0.
            // If a database has lot of tables, list_tables will be slow.
            // So We check get it when max_tables_per_database != 0
            let tables = catalog
                .list_tables(&self.plan.tenant, &self.plan.database)
                .await?;
            if tables.len() >= quota.max_tables_per_database as usize {
                return Err(ErrorCode::TenantQuotaExceeded(format!(
                    "Max tables per database quota exceeded: {}",
                    quota.max_tables_per_database
                )));
            }
        }

        let engine_desc: Option<StorageDescription> = catalog
            .get_table_engines()
            .iter()
            .find(|desc| {
                desc.engine_name.to_string().to_lowercase() == engine.to_string().to_lowercase()
            })
            .cloned();

        if let Some(engine) = engine_desc {
            if self.plan.cluster_key.is_some() && !engine.support_cluster_key {
                return Err(ErrorCode::UnsupportedEngineParams(format!(
                    "Unsupported cluster key for engine: {}",
                    engine.engine_name
                )));
            }
        }

        match &self.plan.as_select {
            Some(select_plan_node) => self.create_table_as_select(select_plan_node.clone()).await,
            None => self.create_table().await,
        }
    }
}

impl CreateTableInterpreter {
    #[async_backtrace::framed]
    async fn create_table_as_select(&self, select_plan: Box<Plan>) -> Result<PipelineBuildResult> {
        assert!(
            !self.plan.read_only_attach,
            "There should be no CREATE(not ATTACH) TABLE plan which is READ_ONLY"
        );

        let tenant = self.ctx.get_tenant();

        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;

        let mut req = self.build_request(None)?;

        // create a dropped table first.
        req.as_dropped = true;
        req.table_meta.drop_on = Some(Utc::now());
        let table_meta = req.table_meta.clone();
        let reply = catalog.create_table(req).await?;
        if !reply.new_table && self.plan.create_option != CreateOption::CreateOrReplace {
            return Ok(PipelineBuildResult::create());
        }

        let table_id = reply.table_id;
        let table_id_seq = reply
            .table_id_seq
            .expect("internal error: table_id_seq must have been set. CTAS(replace) of table");
        let db_id = reply.db_id;

        // grant the ownership of the table to the current role.
        let current_role = self.ctx.get_current_role();
        if let Some(current_role) = current_role {
            let role_api = UserApiProvider::instance().role_api(&tenant);
            role_api
                .grant_ownership(
                    &OwnershipObject::Table {
                        catalog_name: self.plan.catalog.clone(),
                        db_id,
                        table_id,
                    },
                    &current_role.name,
                )
                .await?;
            RoleCacheManager::instance().invalidate_cache(&tenant);
        }

        // If the table creation query contains column definitions, like 'CREATE TABLE t1(a int) AS SELECT * from t2',
        // we use the definitions to create the table schema. It may happen that the "AS SELECT" query's schema doesn't
        // match the table's schema. For example,
        //
        //   mysql> create table t2(a int, b int);
        //   mysql> create table t1(x string, y string) as select * from t2;
        //
        // For the situation above, we implicitly cast the data type when inserting data.
        // The casting and schema checking is in interpreter_insert.rs, function check_schema_cast.

        let table_info = TableInfo::new(
            &self.plan.database,
            &self.plan.table,
            TableIdent::new(table_id, table_id_seq),
            table_meta,
        );

        let insert_plan = Insert {
            catalog: self.plan.catalog.clone(),
            database: self.plan.database.clone(),
            table: self.plan.table.clone(),
            schema: self.plan.schema.clone(),
            overwrite: false,
            source: InsertInputSource::SelectPlan(select_plan),
            table_info: Some(table_info),
        };

        let mut pipeline = InsertInterpreter::try_create(self.ctx.clone(), insert_plan)?
            .execute2()
            .await?;

        let db_name = self.plan.database.clone();
        let table_name = self.plan.table.clone();

        // Add a callback to restore table visibility upon successful insert pipeline completion.
        // As there might be previous on_finish callbacks(e.g. refresh/compact/re-cluster hooks) which
        // depend on the table being visible, this callback is added at the beginning of the on_finish
        // callback list.
        //
        // If the un-drop fails, data inserted and the table will be invisible, and available for vacuum.

        pipeline
            .main_pipeline
            .push_front_on_finished_callback(move |(_profiles, err)| {
                if err.is_ok() {
                    let qualified_table_name = format!("{}.{}", db_name, table_name);
                    let undrop_fut = async move {
                        let undrop_by_id = UndropTableByIdReq {
                            name_ident: TableNameIdent {
                                tenant,
                                db_name,
                                table_name,
                            },
                            db_id,
                            table_id,
                            table_id_seq,
                            force_replace: true,
                        };
                        catalog.undrop_table_by_id(undrop_by_id).await
                    };
                    GlobalIORuntime::instance()
                        .block_on(undrop_fut)
                        .map_err(|e| {
                            info!("create {} as select failed. {:?}", qualified_table_name, e);
                            e
                        })?;
                }

                Ok(())
            });

        Ok(pipeline)
    }

    #[async_backtrace::framed]
    async fn create_table(&self) -> Result<PipelineBuildResult> {
        let catalog = self.ctx.get_catalog(self.plan.catalog.as_str()).await?;
        let mut stat = None;
        if !GlobalConfig::instance().query.management_mode {
            if let Some(snapshot_loc) = self.plan.options.get(OPT_KEY_SNAPSHOT_LOCATION) {
                let operator = self.ctx.get_data_operator()?.operator();
                let reader = MetaReaders::table_snapshot_reader(operator);

                let params = LoadParams {
                    location: snapshot_loc.clone(),
                    len_hint: None,
                    ver: TableSnapshot::VERSION,
                    put_cache: true,
                };

                let snapshot = reader.read(&params).await?;
                stat = Some(TableStatistics {
                    number_of_rows: snapshot.summary.row_count,
                    data_bytes: snapshot.summary.uncompressed_byte_size,
                    compressed_data_bytes: snapshot.summary.compressed_byte_size,
                    index_data_bytes: snapshot.summary.index_size,
                    number_of_segments: Some(snapshot.segments.len() as u64),
                    number_of_blocks: Some(snapshot.summary.block_count),
                });
            }
        }
        let req = if let Some(storage_prefix) = self.plan.options.get(OPT_KEY_STORAGE_PREFIX) {
            self.build_attach_request(storage_prefix).await
        } else {
            self.build_request(stat)
        }?;

        let reply = catalog.create_table(req.clone()).await?;

        // grant the ownership of the table to the current role, the above req.table_meta.owner could be removed in future.
        if let Some(current_role) = self.ctx.get_current_role() {
            let tenant = self.ctx.get_tenant();
            let db = catalog.get_database(&tenant, &self.plan.database).await?;
            let db_id = db.get_db_info().ident.db_id;

            let role_api = UserApiProvider::instance().role_api(&tenant);
            role_api
                .grant_ownership(
                    &OwnershipObject::Table {
                        catalog_name: self.plan.catalog.clone(),
                        db_id,
                        table_id: reply.table_id,
                    },
                    &current_role.name,
                )
                .await?;
            RoleCacheManager::instance().invalidate_cache(&tenant);
        }

        Ok(PipelineBuildResult::create())
    }

    /// Build CreateTableReq from CreateTablePlanV2.
    ///
    /// - Rebuild `DataSchema` with default exprs.
    /// - Update cluster key of table meta.
    fn build_request(&self, statistics: Option<TableStatistics>) -> Result<CreateTableReq> {
        let fields = self.plan.schema.fields().clone();
        for field in fields.iter() {
            if field.default_expr().is_some() {
                let _ = field_default_value(self.ctx.clone(), field)?;
            }
            is_valid_column(field.name())?;
        }
        let field_comments = if self.plan.field_comments.is_empty() {
            vec!["".to_string(); fields.len()]
        } else {
            self.plan.field_comments.clone()
        };
        let schema = TableSchemaRefExt::create(fields);
        let mut options = self.plan.options.clone();
        let comment = options.remove(OPT_KEY_COMMENT);

        let mut table_meta = TableMeta {
            schema: schema.clone(),
            engine: self.plan.engine.to_string(),
            storage_params: self.plan.storage_params.clone(),
            part_prefix: self.plan.part_prefix.clone(),
            options,
            engine_options: self.plan.engine_options.clone(),
            default_cluster_key: None,
            field_comments,
            drop_on: None,
            statistics: statistics.unwrap_or_default(),
            comment: comment.unwrap_or_default(),
            ..Default::default()
        };

        is_valid_block_per_segment(&table_meta.options)?;
        is_valid_row_per_block(&table_meta.options)?;
        // check bloom_index_columns.
        is_valid_bloom_index_columns(&table_meta.options, schema)?;
        is_valid_change_tracking(&table_meta.options)?;
        // check random seed
        is_valid_random_seed(&table_meta.options)?;

        for table_option in table_meta.options.iter() {
            let key = table_option.0.to_lowercase();
            if !is_valid_create_opt(&key) {
                error!("invalid opt for fuse table in create table statement");
                return Err(ErrorCode::TableOptionInvalid(format!(
                    "table option {key} is invalid for create table statement",
                )));
            }
        }

        if let Some(cluster_key) = &self.plan.cluster_key {
            table_meta = table_meta.push_cluster_key(cluster_key.clone());
        }

        let req = CreateTableReq {
            create_option: self.plan.create_option,
            name_ident: TableNameIdent {
                tenant: self.plan.tenant.clone(),
                db_name: self.plan.database.to_string(),
                table_name: self.plan.table.to_string(),
            },
            table_meta,
            as_dropped: false,
        };

        Ok(req)
    }

    async fn build_attach_request(&self, storage_prefix: &str) -> Result<CreateTableReq> {
        // Safe to unwrap in this function, as attach table must have storage params.
        let sp = self.plan.storage_params.as_ref().unwrap();
        let operator = DataOperator::try_create(sp).await?;
        let operator = operator.operator();
        let reader = MetaReaders::table_snapshot_reader(operator.clone());
        let hint = format!("{}/{}", storage_prefix, FUSE_TBL_LAST_SNAPSHOT_HINT);
        let snapshot_loc = operator.read(&hint).await?.to_vec();
        let snapshot_loc = String::from_utf8(snapshot_loc)?;
        let info = operator.info();
        let root = info.root();
        let snapshot_loc = snapshot_loc[root.len()..].to_string();
        let mut options = self.plan.options.clone();
        options.insert(OPT_KEY_SNAPSHOT_LOCATION.to_string(), snapshot_loc.clone());

        if self.plan.read_only_attach {
            // mark table as read_only attached
            options.insert(
                OPT_KEY_TABLE_ATTACHED_READ_ONLY.to_string(),
                "T".to_string(),
            );
        }

        let params = LoadParams {
            location: snapshot_loc.clone(),
            len_hint: None,
            ver: TableSnapshot::VERSION,
            put_cache: true,
        };

        let snapshot = reader.read(&params).await?;
        let stat = TableStatistics {
            number_of_rows: snapshot.summary.row_count,
            data_bytes: snapshot.summary.uncompressed_byte_size,
            compressed_data_bytes: snapshot.summary.compressed_byte_size,
            index_data_bytes: snapshot.summary.index_size,
            number_of_segments: Some(snapshot.segments.len() as u64),
            number_of_blocks: Some(snapshot.summary.block_count),
        };

        let field_comments = vec!["".to_string(); snapshot.schema.num_fields()];
        let table_meta = TableMeta {
            schema: Arc::new(snapshot.schema.clone()),
            engine: self.plan.engine.to_string(),
            storage_params: self.plan.storage_params.clone(),
            part_prefix: self.plan.part_prefix.clone(),
            options,
            default_cluster_key: None,
            field_comments,
            drop_on: None,
            statistics: stat,
            ..Default::default()
        };
        let req = CreateTableReq {
            create_option: self.plan.create_option,
            name_ident: TableNameIdent {
                tenant: self.plan.tenant.clone(),
                db_name: self.plan.database.to_string(),
                table_name: self.plan.table.to_string(),
            },
            table_meta,
            as_dropped: false,
        };

        Ok(req)
    }
}

/// Table option keys that can occur in 'create table statement'.
pub static CREATE_TABLE_OPTIONS: LazyLock<HashSet<&'static str>> = LazyLock::new(|| {
    let mut r = HashSet::new();
    r.insert(FUSE_OPT_KEY_ROW_PER_PAGE);
    r.insert(FUSE_OPT_KEY_BLOCK_PER_SEGMENT);
    r.insert(FUSE_OPT_KEY_ROW_PER_BLOCK);
    r.insert(FUSE_OPT_KEY_BLOCK_IN_MEM_SIZE_THRESHOLD);
    r.insert(FUSE_OPT_KEY_ROW_AVG_DEPTH_THRESHOLD);

    r.insert(OPT_KEY_BLOOM_INDEX_COLUMNS);
    r.insert(OPT_KEY_TABLE_COMPRESSION);
    r.insert(OPT_KEY_STORAGE_FORMAT);
    r.insert(OPT_KEY_DATABASE_ID);
    r.insert(OPT_KEY_COMMENT);
    r.insert(OPT_KEY_CHANGE_TRACKING);

    r.insert(OPT_KEY_ENGINE);

    r.insert(OPT_KEY_ENGINE);

    r.insert(OPT_KEY_LOCATION);
    r.insert(OPT_KEY_CONNECTION_NAME);

    r.insert(OPT_KEY_RANDOM_SEED);

    r.insert("transient");
    r
});

pub fn is_valid_create_opt<S: AsRef<str>>(opt_key: S) -> bool {
    CREATE_TABLE_OPTIONS.contains(opt_key.as_ref().to_lowercase().as_str())
}

pub fn is_valid_column(name: &str) -> Result<()> {
    if is_internal_column(name) {
        return Err(ErrorCode::TableWithInternalColumnName(format!(
            "Cannot create table has column with the same name as internal column: {}",
            name
        )));
    }
    Ok(())
}

pub fn is_valid_block_per_segment(options: &BTreeMap<String, String>) -> Result<()> {
    // check block_per_segment is not over 1000.
    if let Some(value) = options.get(FUSE_OPT_KEY_BLOCK_PER_SEGMENT) {
        let blocks_per_segment = value.parse::<u64>()?;
        let error_str = "invalid block_per_segment option, can't be over 1000";
        if blocks_per_segment > 1000 {
            error!("{}", &error_str);
            return Err(ErrorCode::TableOptionInvalid(error_str));
        }
    }

    Ok(())
}

pub fn is_valid_row_per_block(options: &BTreeMap<String, String>) -> Result<()> {
    // check row_per_block can not be over 1000000.
    if let Some(value) = options.get(FUSE_OPT_KEY_ROW_PER_BLOCK) {
        let row_per_block = value.parse::<u64>()?;
        let error_str = "invalid row_per_block option, can't be over 1000000";

        if row_per_block > DEFAULT_BLOCK_MAX_ROWS as u64 {
            error!("{}", error_str);
            return Err(ErrorCode::TableOptionInvalid(error_str));
        }
    }
    Ok(())
}

pub fn is_valid_bloom_index_columns(
    options: &BTreeMap<String, String>,
    schema: TableSchemaRef,
) -> Result<()> {
    if let Some(value) = options.get(OPT_KEY_BLOOM_INDEX_COLUMNS) {
        BloomIndexColumns::verify_definition(value, schema, BloomIndex::supported_type)?;
    }
    Ok(())
}

pub fn is_valid_change_tracking(options: &BTreeMap<String, String>) -> Result<()> {
    if let Some(value) = options.get(OPT_KEY_CHANGE_TRACKING) {
        value.to_lowercase().parse::<bool>()?;
    }
    Ok(())
}

pub fn is_valid_random_seed(options: &BTreeMap<String, String>) -> Result<()> {
    if let Some(value) = options.get(OPT_KEY_RANDOM_SEED) {
        value.parse::<u64>()?;
    }
    Ok(())
}
