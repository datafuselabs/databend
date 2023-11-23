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
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use common_catalog::catalog::Catalog;
use common_catalog::catalog::StorageDescription;
use common_catalog::database::Database;
use common_catalog::table_args::TableArgs;
use common_catalog::table_function::TableFunction;
use common_config::InnerConfig;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::CatalogInfo;
use common_meta_app::schema::CountTablesReply;
use common_meta_app::schema::CountTablesReq;
use common_meta_app::schema::CreateDatabaseReply;
use common_meta_app::schema::CreateDatabaseReq;
use common_meta_app::schema::CreateIndexReply;
use common_meta_app::schema::CreateIndexReq;
use common_meta_app::schema::CreateLockRevReply;
use common_meta_app::schema::CreateLockRevReq;
use common_meta_app::schema::CreateTableReply;
use common_meta_app::schema::CreateTableReq;
use common_meta_app::schema::CreateVirtualColumnReply;
use common_meta_app::schema::CreateVirtualColumnReq;
use common_meta_app::schema::DeleteLockRevReq;
use common_meta_app::schema::DropDatabaseReply;
use common_meta_app::schema::DropDatabaseReq;
use common_meta_app::schema::DropIndexReply;
use common_meta_app::schema::DropIndexReq;
use common_meta_app::schema::DropTableByIdReq;
use common_meta_app::schema::DropTableReply;
use common_meta_app::schema::DropVirtualColumnReply;
use common_meta_app::schema::DropVirtualColumnReq;
use common_meta_app::schema::DroppedId;
use common_meta_app::schema::ExtendLockRevReq;
use common_meta_app::schema::GcDroppedTableReq;
use common_meta_app::schema::GcDroppedTableResp;
use common_meta_app::schema::GetIndexReply;
use common_meta_app::schema::GetIndexReq;
use common_meta_app::schema::GetLVTReply;
use common_meta_app::schema::GetTableCopiedFileReply;
use common_meta_app::schema::GetTableCopiedFileReq;
use common_meta_app::schema::IndexMeta;
use common_meta_app::schema::ListDroppedTableReq;
use common_meta_app::schema::ListIndexesByIdReq;
use common_meta_app::schema::ListIndexesReq;
use common_meta_app::schema::ListLockRevReq;
use common_meta_app::schema::ListVirtualColumnsReq;
use common_meta_app::schema::LockMeta;
use common_meta_app::schema::RenameDatabaseReply;
use common_meta_app::schema::RenameDatabaseReq;
use common_meta_app::schema::RenameTableReply;
use common_meta_app::schema::RenameTableReq;
use common_meta_app::schema::SetLVTReply;
use common_meta_app::schema::SetTableColumnMaskPolicyReply;
use common_meta_app::schema::SetTableColumnMaskPolicyReq;
use common_meta_app::schema::TableIdent;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::TableMeta;
use common_meta_app::schema::TruncateTableReply;
use common_meta_app::schema::TruncateTableReq;
use common_meta_app::schema::UndropDatabaseReply;
use common_meta_app::schema::UndropDatabaseReq;
use common_meta_app::schema::UndropTableReply;
use common_meta_app::schema::UndropTableReq;
use common_meta_app::schema::UpdateIndexReply;
use common_meta_app::schema::UpdateIndexReq;
use common_meta_app::schema::UpdateTableMetaReply;
use common_meta_app::schema::UpdateTableMetaReq;
use common_meta_app::schema::UpdateVirtualColumnReply;
use common_meta_app::schema::UpdateVirtualColumnReq;
use common_meta_app::schema::UpsertTableOptionReply;
use common_meta_app::schema::UpsertTableOptionReq;
use common_meta_app::schema::VirtualColumnMeta;
use common_meta_types::MetaId;
use log::info;

use crate::catalogs::default::ImmutableCatalog;
use crate::catalogs::default::MutableCatalog;
use crate::storages::Table;
use crate::table_functions::TableFunctionFactory;

/// Combine two catalogs together
/// - read/search like operations are always performed at
///   upper layer first, and bottom layer later(if necessary)
/// - metadata are written to the bottom layer
#[derive(Clone)]
pub struct DatabaseCatalog {
    /// the upper layer, read only
    immutable_catalog: Arc<dyn Catalog>,
    /// bottom layer, writing goes here
    mutable_catalog: Arc<dyn Catalog>,
    /// table function engine factories
    table_function_factory: Arc<TableFunctionFactory>,
}

impl Debug for DatabaseCatalog {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DefaultCatalog").finish_non_exhaustive()
    }
}

impl DatabaseCatalog {
    pub fn create(
        immutable_catalog: Arc<dyn Catalog>,
        mutable_catalog: Arc<dyn Catalog>,
        table_function_factory: Arc<TableFunctionFactory>,
    ) -> Self {
        Self {
            immutable_catalog,
            mutable_catalog,
            table_function_factory,
        }
    }

    #[async_backtrace::framed]
    pub async fn try_create_with_config(conf: InnerConfig) -> Result<DatabaseCatalog> {
        let immutable_catalog = ImmutableCatalog::try_create_with_config(&conf).await?;
        let mutable_catalog = MutableCatalog::try_create_with_config(conf).await?;
        let table_function_factory = TableFunctionFactory::create();
        let res = DatabaseCatalog::create(
            Arc::new(immutable_catalog),
            Arc::new(mutable_catalog),
            Arc::new(table_function_factory),
        );
        Ok(res)
    }
}

#[async_trait::async_trait]
impl Catalog for DatabaseCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> String {
        "default".to_string()
    }

    fn info(&self) -> CatalogInfo {
        CatalogInfo::new_default()
    }

    #[async_backtrace::framed]
    async fn get_database(&self, tenant: &str, db_name: &str) -> Result<Arc<dyn Database>> {
        if tenant.is_empty() {
            return Err(ErrorCode::TenantIsEmpty(
                "Tenant can not empty(while get database)",
            ));
        }

        let r = self.immutable_catalog.get_database(tenant, db_name).await;
        match r {
            Err(e) => {
                if e.code() == ErrorCode::UNKNOWN_DATABASE {
                    self.mutable_catalog.get_database(tenant, db_name).await
                } else {
                    Err(e)
                }
            }
            Ok(db) => Ok(db),
        }
    }

    #[async_backtrace::framed]
    async fn list_databases(&self, tenant: &str) -> Result<Vec<Arc<dyn Database>>> {
        if tenant.is_empty() {
            return Err(ErrorCode::TenantIsEmpty(
                "Tenant can not empty(while list databases)",
            ));
        }

        let mut dbs = self.immutable_catalog.list_databases(tenant).await?;
        let mut other = self.mutable_catalog.list_databases(tenant).await?;
        dbs.append(&mut other);
        Ok(dbs)
    }

    #[async_backtrace::framed]
    async fn create_database(&self, req: CreateDatabaseReq) -> Result<CreateDatabaseReply> {
        if req.name_ident.tenant.is_empty() {
            return Err(ErrorCode::TenantIsEmpty(
                "Tenant can not empty(while create database)",
            ));
        }
        info!("Create database from req:{:?}", req);

        if self
            .immutable_catalog
            .exists_database(&req.name_ident.tenant, &req.name_ident.db_name)
            .await?
        {
            return Err(ErrorCode::DatabaseAlreadyExists(format!(
                "{} database exists",
                req.name_ident.db_name
            )));
        }
        // create db in BOTTOM layer only
        self.mutable_catalog.create_database(req).await
    }

    #[async_backtrace::framed]
    async fn drop_database(&self, req: DropDatabaseReq) -> Result<DropDatabaseReply> {
        if req.name_ident.tenant.is_empty() {
            return Err(ErrorCode::TenantIsEmpty(
                "Tenant can not empty(while drop database)",
            ));
        }
        info!("Drop database from req:{:?}", req);

        // drop db in BOTTOM layer only
        if self
            .immutable_catalog
            .exists_database(&req.name_ident.tenant, &req.name_ident.db_name)
            .await?
        {
            return self.immutable_catalog.drop_database(req).await;
        }
        self.mutable_catalog.drop_database(req).await
    }

    #[async_backtrace::framed]
    async fn rename_database(&self, req: RenameDatabaseReq) -> Result<RenameDatabaseReply> {
        if req.name_ident.tenant.is_empty() {
            return Err(ErrorCode::TenantIsEmpty(
                "Tenant can not empty(while rename database)",
            ));
        }
        info!("Rename table from req:{:?}", req);

        if self
            .immutable_catalog
            .exists_database(&req.name_ident.tenant, &req.name_ident.db_name)
            .await?
            || self
                .immutable_catalog
                .exists_database(&req.name_ident.tenant, &req.new_db_name)
                .await?
        {
            return self.immutable_catalog.rename_database(req).await;
        }

        self.mutable_catalog.rename_database(req).await
    }

    fn get_table_by_info(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        let res = self.immutable_catalog.get_table_by_info(table_info);
        match res {
            Ok(t) => Ok(t),
            Err(e) => {
                if e.code() == ErrorCode::UNKNOWN_TABLE {
                    self.mutable_catalog.get_table_by_info(table_info)
                } else {
                    Err(e)
                }
            }
        }
    }

    #[async_backtrace::framed]
    async fn get_table_meta_by_id(&self, table_id: MetaId) -> Result<(TableIdent, Arc<TableMeta>)> {
        let res = self.immutable_catalog.get_table_meta_by_id(table_id).await;

        if let Ok(x) = res {
            Ok(x)
        } else {
            self.mutable_catalog.get_table_meta_by_id(table_id).await
        }
    }

    #[async_backtrace::framed]
    async fn get_table(
        &self,
        tenant: &str,
        db_name: &str,
        table_name: &str,
    ) -> Result<Arc<dyn Table>> {
        if tenant.is_empty() {
            return Err(ErrorCode::TenantIsEmpty(
                "Tenant can not empty(while get table)",
            ));
        }

        let res = self
            .immutable_catalog
            .get_table(tenant, db_name, table_name)
            .await;
        match res {
            Ok(v) => Ok(v),
            Err(e) => {
                if e.code() == ErrorCode::UNKNOWN_DATABASE {
                    self.mutable_catalog
                        .get_table(tenant, db_name, table_name)
                        .await
                } else {
                    Err(e)
                }
            }
        }
    }

    #[async_backtrace::framed]
    async fn list_tables(&self, tenant: &str, db_name: &str) -> Result<Vec<Arc<dyn Table>>> {
        if tenant.is_empty() {
            return Err(ErrorCode::TenantIsEmpty(
                "Tenant can not empty(while list tables)",
            ));
        }

        let r = self.immutable_catalog.list_tables(tenant, db_name).await;
        match r {
            Ok(x) => Ok(x),
            Err(e) => {
                if e.code() == ErrorCode::UNKNOWN_DATABASE {
                    self.mutable_catalog.list_tables(tenant, db_name).await
                } else {
                    Err(e)
                }
            }
        }
    }

    #[async_backtrace::framed]
    async fn list_tables_history(
        &self,
        tenant: &str,
        db_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        if tenant.is_empty() {
            return Err(ErrorCode::TenantIsEmpty(
                "Tenant can not empty(while list tables)",
            ));
        }

        let r = self
            .immutable_catalog
            .list_tables_history(tenant, db_name)
            .await;
        match r {
            Ok(x) => Ok(x),
            Err(e) => {
                if e.code() == ErrorCode::UNKNOWN_DATABASE {
                    self.mutable_catalog
                        .list_tables_history(tenant, db_name)
                        .await
                } else {
                    Err(e)
                }
            }
        }
    }

    #[async_backtrace::framed]
    async fn create_table(&self, req: CreateTableReq) -> Result<CreateTableReply> {
        if req.tenant().is_empty() {
            return Err(ErrorCode::TenantIsEmpty(
                "Tenant can not empty(while create table)",
            ));
        }
        info!("Create table from req:{:?}", req);

        if self
            .immutable_catalog
            .exists_database(req.tenant(), req.db_name())
            .await?
        {
            return self.immutable_catalog.create_table(req).await;
        }
        self.mutable_catalog.create_table(req).await
    }

    #[async_backtrace::framed]
    async fn drop_table_by_id(&self, req: DropTableByIdReq) -> Result<DropTableReply> {
        let res = self.mutable_catalog.drop_table_by_id(req).await?;
        Ok(res)
    }

    #[async_backtrace::framed]
    async fn undrop_table(&self, req: UndropTableReq) -> Result<UndropTableReply> {
        if req.tenant().is_empty() {
            return Err(ErrorCode::TenantIsEmpty(
                "Tenant can not empty(while undrop table)",
            ));
        }
        info!("Undrop table from req:{:?}", req);

        if self
            .immutable_catalog
            .exists_database(req.tenant(), req.db_name())
            .await?
        {
            return self.immutable_catalog.undrop_table(req).await;
        }
        self.mutable_catalog.undrop_table(req).await
    }

    #[async_backtrace::framed]
    async fn undrop_database(&self, req: UndropDatabaseReq) -> Result<UndropDatabaseReply> {
        if req.tenant().is_empty() {
            return Err(ErrorCode::TenantIsEmpty(
                "Tenant can not empty(while undrop database)",
            ));
        }
        info!("Undrop database from req:{:?}", req);

        if self
            .immutable_catalog
            .exists_database(req.tenant(), req.db_name())
            .await?
        {
            return self.immutable_catalog.undrop_database(req).await;
        }
        self.mutable_catalog.undrop_database(req).await
    }

    #[async_backtrace::framed]
    async fn rename_table(&self, req: RenameTableReq) -> Result<RenameTableReply> {
        if req.tenant().is_empty() {
            return Err(ErrorCode::TenantIsEmpty(
                "Tenant can not empty(while rename table)",
            ));
        }
        info!("Rename table from req:{:?}", req);

        if self
            .immutable_catalog
            .exists_database(req.tenant(), req.db_name())
            .await?
            || self
                .immutable_catalog
                .exists_database(req.tenant(), &req.new_db_name)
                .await?
        {
            return Err(ErrorCode::Unimplemented(
                "Cannot rename table from(to) system databases",
            ));
        }

        self.mutable_catalog.rename_table(req).await
    }

    #[async_backtrace::framed]
    async fn count_tables(&self, req: CountTablesReq) -> Result<CountTablesReply> {
        if req.tenant.is_empty() {
            return Err(ErrorCode::TenantIsEmpty(
                "Tenant can not empty(while count tables)",
            ));
        }

        let res = self.mutable_catalog.count_tables(req).await?;

        Ok(res)
    }

    #[async_backtrace::framed]
    async fn get_table_copied_file_info(
        &self,
        tenant: &str,
        db_name: &str,
        req: GetTableCopiedFileReq,
    ) -> Result<GetTableCopiedFileReply> {
        self.mutable_catalog
            .get_table_copied_file_info(tenant, db_name, req)
            .await
    }

    #[async_backtrace::framed]
    async fn truncate_table(
        &self,
        table_info: &TableInfo,
        req: TruncateTableReq,
    ) -> Result<TruncateTableReply> {
        self.mutable_catalog.truncate_table(table_info, req).await
    }

    #[async_backtrace::framed]
    async fn upsert_table_option(
        &self,
        tenant: &str,
        db_name: &str,
        req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply> {
        self.mutable_catalog
            .upsert_table_option(tenant, db_name, req)
            .await
    }

    #[async_backtrace::framed]
    async fn update_table_meta(
        &self,
        table_info: &TableInfo,
        req: UpdateTableMetaReq,
    ) -> Result<UpdateTableMetaReply> {
        self.mutable_catalog
            .update_table_meta(table_info, req)
            .await
    }

    #[async_backtrace::framed]
    async fn set_table_column_mask_policy(
        &self,
        req: SetTableColumnMaskPolicyReq,
    ) -> Result<SetTableColumnMaskPolicyReply> {
        self.mutable_catalog.set_table_column_mask_policy(req).await
    }

    // Table index

    #[async_backtrace::framed]
    async fn create_index(&self, req: CreateIndexReq) -> Result<CreateIndexReply> {
        self.mutable_catalog.create_index(req).await
    }

    #[async_backtrace::framed]
    async fn drop_index(&self, req: DropIndexReq) -> Result<DropIndexReply> {
        self.mutable_catalog.drop_index(req).await
    }

    #[async_backtrace::framed]
    async fn get_index(&self, req: GetIndexReq) -> Result<GetIndexReply> {
        self.mutable_catalog.get_index(req).await
    }

    #[async_backtrace::framed]
    async fn update_index(&self, req: UpdateIndexReq) -> Result<UpdateIndexReply> {
        self.mutable_catalog.update_index(req).await
    }

    #[async_backtrace::framed]
    async fn list_indexes(&self, req: ListIndexesReq) -> Result<Vec<(u64, String, IndexMeta)>> {
        self.mutable_catalog.list_indexes(req).await
    }

    #[async_backtrace::framed]
    async fn list_index_ids_by_table_id(&self, req: ListIndexesByIdReq) -> Result<Vec<u64>> {
        self.mutable_catalog.list_index_ids_by_table_id(req).await
    }

    #[async_backtrace::framed]
    async fn list_indexes_by_table_id(
        &self,
        req: ListIndexesByIdReq,
    ) -> Result<Vec<(u64, String, IndexMeta)>> {
        self.mutable_catalog.list_indexes_by_table_id(req).await
    }

    // Virtual column

    #[async_backtrace::framed]
    async fn create_virtual_column(
        &self,
        req: CreateVirtualColumnReq,
    ) -> Result<CreateVirtualColumnReply> {
        self.mutable_catalog.create_virtual_column(req).await
    }

    #[async_backtrace::framed]
    async fn update_virtual_column(
        &self,
        req: UpdateVirtualColumnReq,
    ) -> Result<UpdateVirtualColumnReply> {
        self.mutable_catalog.update_virtual_column(req).await
    }

    #[async_backtrace::framed]
    async fn drop_virtual_column(
        &self,
        req: DropVirtualColumnReq,
    ) -> Result<DropVirtualColumnReply> {
        self.mutable_catalog.drop_virtual_column(req).await
    }

    #[async_backtrace::framed]
    async fn list_virtual_columns(
        &self,
        req: ListVirtualColumnsReq,
    ) -> Result<Vec<VirtualColumnMeta>> {
        self.mutable_catalog.list_virtual_columns(req).await
    }

    fn get_table_function(
        &self,
        func_name: &str,
        tbl_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        self.table_function_factory.get(func_name, tbl_args)
    }

    fn exists_table_function(&self, func_name: &str) -> bool {
        self.table_function_factory.exists(func_name)
    }

    fn list_table_functions(&self) -> Vec<String> {
        self.table_function_factory.list()
    }

    fn get_table_engines(&self) -> Vec<StorageDescription> {
        // only return mutable_catalog storage table engines
        self.mutable_catalog.get_table_engines()
    }

    #[async_backtrace::framed]
    async fn list_lock_revisions(&self, req: ListLockRevReq) -> Result<Vec<(u64, LockMeta)>> {
        self.mutable_catalog.list_lock_revisions(req).await
    }

    #[async_backtrace::framed]
    async fn create_lock_revision(&self, req: CreateLockRevReq) -> Result<CreateLockRevReply> {
        self.mutable_catalog.create_lock_revision(req).await
    }

    #[async_backtrace::framed]
    async fn extend_lock_revision(&self, req: ExtendLockRevReq) -> Result<()> {
        self.mutable_catalog.extend_lock_revision(req).await
    }

    #[async_backtrace::framed]
    async fn delete_lock_revision(&self, req: DeleteLockRevReq) -> Result<()> {
        self.mutable_catalog.delete_lock_revision(req).await
    }

    async fn get_drop_table_infos(
        &self,
        req: ListDroppedTableReq,
    ) -> Result<(Vec<Arc<dyn Table>>, Vec<DroppedId>)> {
        self.mutable_catalog.get_drop_table_infos(req).await
    }

    async fn gc_drop_tables(&self, req: GcDroppedTableReq) -> Result<GcDroppedTableResp> {
        self.mutable_catalog.gc_drop_tables(req).await
    }

    async fn set_table_lvt(&self, table_id: u64, time: DateTime<Utc>) -> Result<SetLVTReply> {
        self.mutable_catalog.set_table_lvt(table_id, time).await
    }

    async fn get_table_lvt(&self, table_id: u64) -> Result<GetLVTReply> {
        self.mutable_catalog.get_table_lvt(table_id).await
    }
}
