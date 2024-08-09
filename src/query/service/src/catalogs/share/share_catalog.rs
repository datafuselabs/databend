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

use std::any::Any;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::sync::Arc;

use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::catalog::CatalogCreator;
use databend_common_catalog::catalog::StorageDescription;
use databend_common_catalog::database::Database;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_function::TableFunction;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_api::ShareApi;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::CatalogOption;
use databend_common_meta_app::schema::CommitTableMetaReply;
use databend_common_meta_app::schema::CommitTableMetaReq;
use databend_common_meta_app::schema::CreateDatabaseReply;
use databend_common_meta_app::schema::CreateDatabaseReq;
use databend_common_meta_app::schema::CreateIndexReply;
use databend_common_meta_app::schema::CreateIndexReq;
use databend_common_meta_app::schema::CreateLockRevReply;
use databend_common_meta_app::schema::CreateLockRevReq;
use databend_common_meta_app::schema::CreateSequenceReply;
use databend_common_meta_app::schema::CreateSequenceReq;
use databend_common_meta_app::schema::CreateTableIndexReply;
use databend_common_meta_app::schema::CreateTableIndexReq;
use databend_common_meta_app::schema::CreateTableReply;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::CreateVirtualColumnReply;
use databend_common_meta_app::schema::CreateVirtualColumnReq;
use databend_common_meta_app::schema::DatabaseIdent;
use databend_common_meta_app::schema::DatabaseInfo;
use databend_common_meta_app::schema::DatabaseMeta;
use databend_common_meta_app::schema::DatabaseType;
use databend_common_meta_app::schema::DeleteLockRevReq;
use databend_common_meta_app::schema::DropDatabaseReply;
use databend_common_meta_app::schema::DropDatabaseReq;
use databend_common_meta_app::schema::DropIndexReply;
use databend_common_meta_app::schema::DropIndexReq;
use databend_common_meta_app::schema::DropSequenceReply;
use databend_common_meta_app::schema::DropSequenceReq;
use databend_common_meta_app::schema::DropTableByIdReq;
use databend_common_meta_app::schema::DropTableIndexReply;
use databend_common_meta_app::schema::DropTableIndexReq;
use databend_common_meta_app::schema::DropTableReply;
use databend_common_meta_app::schema::DropVirtualColumnReply;
use databend_common_meta_app::schema::DropVirtualColumnReq;
use databend_common_meta_app::schema::ExtendLockRevReq;
use databend_common_meta_app::schema::GetIndexReply;
use databend_common_meta_app::schema::GetIndexReq;
use databend_common_meta_app::schema::GetSequenceNextValueReply;
use databend_common_meta_app::schema::GetSequenceNextValueReq;
use databend_common_meta_app::schema::GetSequenceReply;
use databend_common_meta_app::schema::GetSequenceReq;
use databend_common_meta_app::schema::GetTableCopiedFileReply;
use databend_common_meta_app::schema::GetTableCopiedFileReq;
use databend_common_meta_app::schema::IndexMeta;
use databend_common_meta_app::schema::ListIndexesByIdReq;
use databend_common_meta_app::schema::ListIndexesReq;
use databend_common_meta_app::schema::ListLockRevReq;
use databend_common_meta_app::schema::ListLocksReq;
use databend_common_meta_app::schema::ListVirtualColumnsReq;
use databend_common_meta_app::schema::LockInfo;
use databend_common_meta_app::schema::LockMeta;
use databend_common_meta_app::schema::RenameDatabaseReply;
use databend_common_meta_app::schema::RenameDatabaseReq;
use databend_common_meta_app::schema::RenameTableReply;
use databend_common_meta_app::schema::RenameTableReq;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyReply;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyReq;
use databend_common_meta_app::schema::ShareCatalogOption;
use databend_common_meta_app::schema::ShareDbId;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TruncateTableReply;
use databend_common_meta_app::schema::TruncateTableReq;
use databend_common_meta_app::schema::UndropDatabaseReply;
use databend_common_meta_app::schema::UndropDatabaseReq;
use databend_common_meta_app::schema::UndropTableReply;
use databend_common_meta_app::schema::UndropTableReq;
use databend_common_meta_app::schema::UpdateIndexReply;
use databend_common_meta_app::schema::UpdateIndexReq;
use databend_common_meta_app::schema::UpdateVirtualColumnReply;
use databend_common_meta_app::schema::UpdateVirtualColumnReq;
use databend_common_meta_app::schema::UpsertTableOptionReply;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_meta_app::schema::VirtualColumnMeta;
use databend_common_meta_app::share::share_name_ident::ShareNameIdentRaw;
use databend_common_meta_app::share::GetShareEndpointReq;
use databend_common_meta_app::share::ShareDatabaseSpec;
use databend_common_meta_app::share::ShareSpec;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_store::MetaStore;
use databend_common_meta_types::MetaId;
use databend_common_meta_types::SeqV;
use databend_common_sharing::ShareEndpointClient;
use databend_common_storages_factory::StorageFactory;
use databend_common_users::UserApiProvider;

use crate::catalogs::default::CatalogContext;
use crate::databases::DatabaseContext;
use crate::databases::DatabaseFactory;
use crate::databases::ShareDatabase;

#[derive(Debug)]
pub struct ShareCatalogCreator;

impl CatalogCreator for ShareCatalogCreator {
    fn try_create(
        &self,
        info: Arc<CatalogInfo>,
        conf: InnerConfig,
        meta: &MetaStore,
    ) -> Result<Arc<dyn Catalog>> {
        let opt = match &info.meta.catalog_option {
            CatalogOption::Share(opt) => opt,
            _ => unreachable!(
                "trying to create hive catalog from other catalog, must be an internal bug"
            ),
        };

        // Storage factory.
        let storage_factory = StorageFactory::create(conf.clone());

        // Database factory.
        let database_factory = DatabaseFactory::create(conf.clone());

        let ctx = CatalogContext {
            meta: meta.to_owned(),
            storage_factory: Arc::new(storage_factory),
            database_factory: Arc::new(database_factory),
        };

        let catalog: Arc<dyn Catalog> =
            Arc::new(ShareCatalog::try_create(info.clone(), opt.to_owned(), ctx)?);

        Ok(catalog)
    }
}

#[derive(Clone)]
pub struct ShareCatalog {
    // ctx: CatalogContext,
    ctx: DatabaseContext,

    info: Arc<CatalogInfo>,

    option: ShareCatalogOption,

    client: ShareEndpointClient,
}

impl Debug for ShareCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("HiveCatalog")
            .field("info", &self.info)
            .field("option", &self.option)
            .finish_non_exhaustive()
    }
}

impl ShareCatalog {
    pub fn try_create(
        info: Arc<CatalogInfo>,
        option: ShareCatalogOption,
        ctx: CatalogContext,
    ) -> Result<ShareCatalog> {
        let ctx = DatabaseContext {
            meta: ctx.meta.clone(),
            storage_factory: ctx.storage_factory.clone(),
            tenant: Tenant {
                tenant: info.name_ident.tenant.clone(),
            },
            disable_table_info_refresh: false,
        };
        let client = ShareEndpointClient::new();

        Ok(Self {
            info,
            option,
            ctx,
            client,
        })
    }

    async fn get_share_spec(&self) -> Result<ShareSpec> {
        let share_option = &self.option;
        let share_name = &share_option.share_name;
        let share_endpoint = &share_option.share_endpoint;
        let provider = &share_option.provider;
        let tenant = &self.info.name_ident.tenant;

        // 1. get share endpoint
        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let req = GetShareEndpointReq {
            tenant: Tenant {
                tenant: tenant.to_owned(),
            },
            endpoint: Some(share_endpoint.clone()),
        };
        let reply = meta_api.get_share_endpoint(req).await?;
        if reply.share_endpoint_meta_vec.is_empty() {
            return Err(ErrorCode::UnknownShareEndpoint(format!(
                "UnknownShareEndpoint {:?}",
                share_endpoint
            )));
        }

        // 2. check if ShareSpec exists using share endpoint
        let share_endpoint_meta = &reply.share_endpoint_meta_vec[0].1;
        let share_spec = self
            .client
            .get_share_spec_by_name(share_endpoint_meta, tenant, provider, share_name)
            .await?;

        Ok(share_spec)
    }

    fn generate_share_database_info(&self, database: &ShareDatabaseSpec) -> DatabaseInfo {
        let share_option = &self.option;
        let share_name = &share_option.share_name;
        let share_endpoint = &share_option.share_endpoint;
        let provider = &share_option.provider;

        DatabaseInfo {
            ident: DatabaseIdent::default(),
            name_ident: DatabaseNameIdent::new(
                Tenant {
                    tenant: provider.to_owned(),
                },
                &database.name,
            ),
            meta: DatabaseMeta {
                engine: "SHARE".to_string(),
                engine_options: BTreeMap::new(),
                options: BTreeMap::new(),
                created_on: database.created_on,
                updated_on: database.created_on,
                comment: "".to_string(),
                drop_on: None,
                shared_by: BTreeSet::new(),
                from_share: Some(ShareNameIdentRaw::new(provider, share_name)),
                using_share_endpoint: Some(share_endpoint.to_owned()),
                from_share_db_id: Some(ShareDbId::Usage(database.id)),
            },
        }
    }
}

#[async_trait::async_trait]
impl Catalog for ShareCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> String {
        self.info.name_ident.catalog_name.clone()
    }

    fn info(&self) -> Arc<CatalogInfo> {
        self.info.clone()
    }

    #[async_backtrace::framed]
    async fn get_database(&self, _tenant: &Tenant, db_name: &str) -> Result<Arc<dyn Database>> {
        let share_spec = self.get_share_spec().await?;
        if let Some(use_database) = &share_spec.use_database {
            if use_database.name == db_name {
                let db_info = self.generate_share_database_info(use_database);
                let db = ShareDatabase::try_create(self.ctx.clone(), db_info)?;
                return Ok(db.into());
            } else {
                Err(ErrorCode::UnknownDatabase(format!(
                    "cannot find database {} from share {}",
                    db_name, self.option.share_name,
                )))
            }
        } else {
            Err(ErrorCode::ShareHasNoGrantedDatabase(format!(
                "share {}.{} has no granted database",
                self.option.provider, self.option.share_name,
            )))
        }
    }

    // Get all the databases.
    #[async_backtrace::framed]
    async fn list_databases(&self, _tenant: &Tenant) -> Result<Vec<Arc<dyn Database>>> {
        let share_spec = self.get_share_spec().await?;
        if let Some(use_database) = &share_spec.use_database {
            let db_info = self.generate_share_database_info(use_database);
            let db = ShareDatabase::try_create(self.ctx.clone(), db_info)?;
            Ok(vec![db.into()])
        } else {
            Ok(vec![])
        }
    }

    // Operation with database.
    #[async_backtrace::framed]
    async fn create_database(&self, _req: CreateDatabaseReq) -> Result<CreateDatabaseReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot create database in SHARE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn drop_database(&self, _req: DropDatabaseReq) -> Result<DropDatabaseReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot drop database in SHARE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn undrop_database(&self, _req: UndropDatabaseReq) -> Result<UndropDatabaseReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot undrop database in SHARE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn rename_database(&self, _req: RenameDatabaseReq) -> Result<RenameDatabaseReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot rename database in SHARE catalog",
        ))
    }

    fn get_table_by_info(&self, _table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        Err(ErrorCode::Unimplemented(
            "Cannot get_table_by_info in SHARE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn get_table_meta_by_id(&self, _table_id: MetaId) -> Result<Option<SeqV<TableMeta>>> {
        Err(ErrorCode::Unimplemented(
            "Cannot get table by id in SHARE catalog",
        ))
    }

    async fn mget_table_names_by_ids(
        &self,
        _tenant: &Tenant,
        _table_ids: &[MetaId],
    ) -> Result<Vec<Option<String>>> {
        Err(ErrorCode::Unimplemented(
            "Cannot get tables name by ids in SHARE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn get_table_name_by_id(&self, _table_id: MetaId) -> Result<Option<String>> {
        Err(ErrorCode::Unimplemented(
            "Cannot get table name by id in SHARE catalog",
        ))
    }

    async fn get_db_name_by_id(&self, _db_id: MetaId) -> Result<String> {
        Err(ErrorCode::Unimplemented(
            "Cannot get db name by id in SHARE catalog",
        ))
    }

    async fn mget_database_names_by_ids(
        &self,
        _tenant: &Tenant,
        _db_ids: &[MetaId],
    ) -> Result<Vec<Option<String>>> {
        Err(ErrorCode::Unimplemented(
            "Cannot get dbs name by ids in SHARE catalog",
        ))
    }

    // Get one table by db and table name.
    #[async_backtrace::framed]
    async fn get_table(
        &self,
        _tenant: &Tenant,
        db_name: &str,
        table_name: &str,
    ) -> Result<Arc<dyn Table>> {
        let share_option = &self.option;
        let share_name = &share_option.share_name;
        let share_endpoint = &share_option.share_endpoint;
        let provider = &share_option.provider;
        let tenant = &self.info.name_ident.tenant;

        // 1. get share endpoint
        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let req = GetShareEndpointReq {
            tenant: Tenant {
                tenant: tenant.to_owned(),
            },
            endpoint: Some(share_endpoint.clone()),
        };
        let reply = meta_api.get_share_endpoint(req).await?;
        if reply.share_endpoint_meta_vec.is_empty() {
            return Err(ErrorCode::UnknownShareEndpoint(format!(
                "UnknownShareEndpoint {:?}",
                share_endpoint
            )));
        }

        // 2. check if ShareSpec exists using share endpoint
        let share_endpoint_meta = reply.share_endpoint_meta_vec[0].1.clone();
        let mut table_info = self
            .client
            .get_share_table(
                &share_endpoint_meta,
                tenant,
                provider,
                share_name,
                db_name,
                table_name,
            )
            .await?;

        let db_type = table_info.db_type.clone();
        if let DatabaseType::ShareDB(params) = db_type {
            let mut params = params;
            params.share_endpoint_url = share_endpoint_meta.url.clone();
            params.share_endpoint_credential = share_endpoint_meta.credential.clone().unwrap();
            table_info.db_type = DatabaseType::ShareDB(params);

            self.ctx.storage_factory.get_table(&table_info)
        } else {
            unreachable!()
        }
    }

    #[async_backtrace::framed]
    async fn list_tables(&self, _tenant: &Tenant, db_name: &str) -> Result<Vec<Arc<dyn Table>>> {
        let share_option = &self.option;
        let share_name = &share_option.share_name;
        let share_endpoint = &share_option.share_endpoint;
        let provider = &share_option.provider;
        let tenant = &self.info.name_ident.tenant;

        // 1. get share endpoint
        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let req = GetShareEndpointReq {
            tenant: Tenant {
                tenant: tenant.to_owned(),
            },
            endpoint: Some(share_endpoint.clone()),
        };
        let reply = meta_api.get_share_endpoint(req).await?;
        if reply.share_endpoint_meta_vec.is_empty() {
            return Err(ErrorCode::UnknownShareEndpoint(format!(
                "UnknownShareEndpoint {:?}",
                share_endpoint
            )));
        }

        // 2. check if ShareSpec exists using share endpoint
        let share_endpoint_meta = reply.share_endpoint_meta_vec[0].1.clone();
        let table_info_map = self
            .client
            .get_share_tables(&share_endpoint_meta, tenant, provider, share_name, db_name)
            .await?;

        let mut table_info_vec = vec![];
        for info in table_info_map.values() {
            let mut table_info = info.clone();
            if let DatabaseType::ShareDB(params) = &table_info.db_type {
                let mut params = params.clone();
                params.share_endpoint_url = share_endpoint_meta.url.clone();
                params.share_endpoint_credential = share_endpoint_meta.credential.clone().unwrap();
                table_info.db_type = DatabaseType::ShareDB(params);

                table_info_vec.push(self.ctx.storage_factory.get_table(&table_info)?);
            } else {
                unreachable!()
            }
        }
        Ok(table_info_vec)
    }

    #[async_backtrace::framed]
    async fn list_tables_history(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        Err(ErrorCode::PermissionDenied(
            "Permission denied, cannot list table history from a shared database".to_string(),
        ))
    }

    #[async_backtrace::framed]
    async fn create_table(&self, _req: CreateTableReq) -> Result<CreateTableReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot create table in SHARE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn drop_table_by_id(&self, _req: DropTableByIdReq) -> Result<DropTableReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot drop table in SHARE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn undrop_table(&self, _req: UndropTableReq) -> Result<UndropTableReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot undrop table in SHARE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn commit_table_meta(&self, _req: CommitTableMetaReq) -> Result<CommitTableMetaReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot commit_table_meta in SHARE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn rename_table(&self, _req: RenameTableReq) -> Result<RenameTableReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot rename table in SHARE catalog",
        ))
    }

    // Check a db.table is exists or not.
    #[async_backtrace::framed]
    async fn exists_table(&self, tenant: &Tenant, db_name: &str, table_name: &str) -> Result<bool> {
        // TODO refine this
        match self.get_table(tenant, db_name, table_name).await {
            Ok(_) => Ok(true),
            Err(err) => {
                if err.code() == ErrorCode::UNKNOWN_TABLE {
                    Ok(false)
                } else {
                    Err(err)
                }
            }
        }
    }

    #[async_backtrace::framed]
    async fn upsert_table_option(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
        _req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot upsert table option in SHARE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn set_table_column_mask_policy(
        &self,
        _req: SetTableColumnMaskPolicyReq,
    ) -> Result<SetTableColumnMaskPolicyReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot set_table_column_mask_policy in SHARE catalog",
        ))
    }

    #[async_backtrace::framed]
    async fn get_table_copied_file_info(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
        _req: GetTableCopiedFileReq,
    ) -> Result<GetTableCopiedFileReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn truncate_table(
        &self,
        _table_info: &TableInfo,
        _req: TruncateTableReq,
    ) -> Result<TruncateTableReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn list_lock_revisions(&self, _req: ListLockRevReq) -> Result<Vec<(u64, LockMeta)>> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn create_lock_revision(&self, _req: CreateLockRevReq) -> Result<CreateLockRevReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn extend_lock_revision(&self, _req: ExtendLockRevReq) -> Result<()> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn delete_lock_revision(&self, _req: DeleteLockRevReq) -> Result<()> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn list_locks(&self, _req: ListLocksReq) -> Result<Vec<LockInfo>> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn create_table_index(&self, _req: CreateTableIndexReq) -> Result<CreateTableIndexReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn drop_table_index(&self, _req: DropTableIndexReq) -> Result<DropTableIndexReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn create_index(&self, _req: CreateIndexReq) -> Result<CreateIndexReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn drop_index(&self, _req: DropIndexReq) -> Result<DropIndexReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn get_index(&self, _req: GetIndexReq) -> Result<GetIndexReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn update_index(&self, _req: UpdateIndexReq) -> Result<UpdateIndexReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn list_indexes(&self, _req: ListIndexesReq) -> Result<Vec<(u64, String, IndexMeta)>> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn list_index_ids_by_table_id(&self, _req: ListIndexesByIdReq) -> Result<Vec<u64>> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn list_indexes_by_table_id(
        &self,
        _req: ListIndexesByIdReq,
    ) -> Result<Vec<(u64, String, IndexMeta)>> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn create_virtual_column(
        &self,
        _req: CreateVirtualColumnReq,
    ) -> Result<CreateVirtualColumnReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn update_virtual_column(
        &self,
        _req: UpdateVirtualColumnReq,
    ) -> Result<UpdateVirtualColumnReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn drop_virtual_column(
        &self,
        _req: DropVirtualColumnReq,
    ) -> Result<DropVirtualColumnReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn list_virtual_columns(
        &self,
        _req: ListVirtualColumnsReq,
    ) -> Result<Vec<VirtualColumnMeta>> {
        unimplemented!()
    }

    /// Table function

    // Get function by name.
    fn get_table_function(
        &self,
        _func_name: &str,
        _tbl_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        unimplemented!()
    }

    // List all table functions' names.
    fn list_table_functions(&self) -> Vec<String> {
        vec![]
    }

    // Get table engines
    fn get_table_engines(&self) -> Vec<StorageDescription> {
        unimplemented!()
    }

    async fn create_sequence(&self, _req: CreateSequenceReq) -> Result<CreateSequenceReply> {
        unimplemented!()
    }
    async fn get_sequence(&self, _req: GetSequenceReq) -> Result<GetSequenceReply> {
        unimplemented!()
    }

    async fn get_sequence_next_value(
        &self,
        _req: GetSequenceNextValueReq,
    ) -> Result<GetSequenceNextValueReply> {
        unimplemented!()
    }

    async fn drop_sequence(&self, _req: DropSequenceReq) -> Result<DropSequenceReply> {
        unimplemented!()
    }
}
