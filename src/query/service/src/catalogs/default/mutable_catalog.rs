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
use common_config::InnerConfig;
use common_exception::Result;
use common_meta_api::SchemaApi;
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
use common_meta_app::schema::DatabaseIdent;
use common_meta_app::schema::DatabaseInfo;
use common_meta_app::schema::DatabaseMeta;
use common_meta_app::schema::DatabaseNameIdent;
use common_meta_app::schema::DatabaseType;
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
use common_meta_app::schema::GetDatabaseReq;
use common_meta_app::schema::GetIndexReply;
use common_meta_app::schema::GetIndexReq;
use common_meta_app::schema::GetLVTReply;
use common_meta_app::schema::GetLVTReq;
use common_meta_app::schema::GetTableCopiedFileReply;
use common_meta_app::schema::GetTableCopiedFileReq;
use common_meta_app::schema::IndexMeta;
use common_meta_app::schema::ListDatabaseReq;
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
use common_meta_app::schema::SetLVTReq;
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
use common_meta_store::MetaStoreProvider;
use common_meta_types::MetaId;
use log::info;

use crate::catalogs::default::catalog_context::CatalogContext;
use crate::databases::Database;
use crate::databases::DatabaseContext;
use crate::databases::DatabaseFactory;
use crate::storages::StorageDescription;
use crate::storages::StorageFactory;
use crate::storages::Table;

/// Catalog based on MetaStore
/// - System Database NOT included
/// - Meta data of databases are saved in meta store
/// - Instances of `Database` are created by using database factories according to the engine
/// - Database engines are free to save table meta in metastore or not
#[derive(Clone)]
pub struct MutableCatalog {
    ctx: CatalogContext,
    tenant: String,
}

impl Debug for MutableCatalog {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MutableCatalog").finish_non_exhaustive()
    }
}

impl MutableCatalog {
    /// The component hierarchy is layered as:
    /// ```text
    /// Remote:
    ///
    ///                                        RPC
    /// MetaRemote -------> Meta server      Meta server      Meta Server
    ///                      raft <---------->   raft   <----------> raft
    ///                     MetaEmbedded     MetaEmbedded     MetaEmbedded
    ///
    /// Embedded:
    ///
    /// MetaEmbedded
    /// ```
    #[async_backtrace::framed]
    pub async fn try_create_with_config(conf: InnerConfig) -> Result<Self> {
        let meta = {
            let provider = Arc::new(MetaStoreProvider::new(conf.meta.to_meta_grpc_client_conf()));

            provider.create_meta_store().await?
        };

        let tenant = conf.query.tenant_id.clone();

        // Create default database.
        let req = CreateDatabaseReq {
            if_not_exists: true,
            name_ident: DatabaseNameIdent {
                tenant,
                db_name: "default".to_string(),
            },
            meta: DatabaseMeta {
                engine: "".to_string(),
                ..Default::default()
            },
        };
        meta.create_database(req).await?;

        // Storage factory.
        let storage_factory = StorageFactory::create(conf.clone());

        // Database factory.
        let database_factory = DatabaseFactory::create(conf.clone());

        let ctx = CatalogContext {
            meta,
            storage_factory: Arc::new(storage_factory),
            database_factory: Arc::new(database_factory),
        };
        Ok(MutableCatalog {
            ctx,
            tenant: conf.query.tenant_id.clone(),
        })
    }

    fn build_db_instance(&self, db_info: &Arc<DatabaseInfo>) -> Result<Arc<dyn Database>> {
        let ctx = DatabaseContext {
            meta: self.ctx.meta.clone(),
            storage_factory: self.ctx.storage_factory.clone(),
            tenant: self.tenant.clone(),
        };
        self.ctx.database_factory.get_database(ctx, db_info)
    }
}

#[async_trait::async_trait]
impl Catalog for MutableCatalog {
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
        let db_info = self
            .ctx
            .meta
            .get_database(GetDatabaseReq::new(tenant, db_name))
            .await?;
        self.build_db_instance(&db_info)
    }

    #[async_backtrace::framed]
    async fn list_databases(&self, tenant: &str) -> Result<Vec<Arc<dyn Database>>> {
        let dbs = self
            .ctx
            .meta
            .list_databases(ListDatabaseReq {
                tenant: tenant.to_string(),
                filter: None,
            })
            .await?;

        dbs.iter().try_fold(vec![], |mut acc, item| {
            let db = self.build_db_instance(item)?;
            acc.push(db);
            Ok(acc)
        })
    }

    #[async_backtrace::framed]
    async fn create_database(&self, req: CreateDatabaseReq) -> Result<CreateDatabaseReply> {
        // Create database.
        let res = self.ctx.meta.create_database(req.clone()).await?;
        info!(
            "db name: {}, engine: {}",
            &req.name_ident.db_name, &req.meta.engine
        );

        // Initial the database after creating.
        let db_info = Arc::new(DatabaseInfo {
            ident: DatabaseIdent {
                db_id: res.db_id,
                seq: 0, // TODO
            },
            name_ident: req.name_ident.clone(),
            meta: req.meta.clone(),
        });
        let database = self.build_db_instance(&db_info)?;
        database.init_database(&req.name_ident.tenant).await?;
        Ok(CreateDatabaseReply { db_id: res.db_id })
    }

    #[async_backtrace::framed]
    async fn drop_database(&self, req: DropDatabaseReq) -> Result<DropDatabaseReply> {
        Ok(self.ctx.meta.drop_database(req).await?)
    }

    #[async_backtrace::framed]
    async fn create_index(&self, req: CreateIndexReq) -> Result<CreateIndexReply> {
        Ok(self.ctx.meta.create_index(req).await?)
    }

    #[async_backtrace::framed]
    async fn drop_index(&self, req: DropIndexReq) -> Result<DropIndexReply> {
        Ok(self.ctx.meta.drop_index(req).await?)
    }

    #[async_backtrace::framed]
    async fn get_index(&self, req: GetIndexReq) -> Result<GetIndexReply> {
        Ok(self.ctx.meta.get_index(req).await?)
    }

    #[async_backtrace::framed]
    async fn update_index(&self, req: UpdateIndexReq) -> Result<UpdateIndexReply> {
        Ok(self.ctx.meta.update_index(req).await?)
    }

    #[async_backtrace::framed]
    async fn list_indexes(&self, req: ListIndexesReq) -> Result<Vec<(u64, String, IndexMeta)>> {
        Ok(self.ctx.meta.list_indexes(req).await?)
    }

    #[async_backtrace::framed]
    async fn list_index_ids_by_table_id(&self, req: ListIndexesByIdReq) -> Result<Vec<u64>> {
        Ok(self.ctx.meta.list_index_ids_by_table_id(req).await?)
    }

    #[async_backtrace::framed]
    async fn list_indexes_by_table_id(
        &self,
        req: ListIndexesByIdReq,
    ) -> Result<Vec<(u64, String, IndexMeta)>> {
        Ok(self.ctx.meta.list_indexes_by_table_id(req).await?)
    }

    // Virtual column

    #[async_backtrace::framed]
    async fn create_virtual_column(
        &self,
        req: CreateVirtualColumnReq,
    ) -> Result<CreateVirtualColumnReply> {
        Ok(self.ctx.meta.create_virtual_column(req).await?)
    }

    #[async_backtrace::framed]
    async fn update_virtual_column(
        &self,
        req: UpdateVirtualColumnReq,
    ) -> Result<UpdateVirtualColumnReply> {
        Ok(self.ctx.meta.update_virtual_column(req).await?)
    }

    #[async_backtrace::framed]
    async fn drop_virtual_column(
        &self,
        req: DropVirtualColumnReq,
    ) -> Result<DropVirtualColumnReply> {
        Ok(self.ctx.meta.drop_virtual_column(req).await?)
    }

    #[async_backtrace::framed]
    async fn list_virtual_columns(
        &self,
        req: ListVirtualColumnsReq,
    ) -> Result<Vec<VirtualColumnMeta>> {
        Ok(self.ctx.meta.list_virtual_columns(req).await?)
    }

    #[async_backtrace::framed]
    async fn undrop_database(&self, req: UndropDatabaseReq) -> Result<UndropDatabaseReply> {
        let res = self.ctx.meta.undrop_database(req).await?;
        Ok(res)
    }

    #[async_backtrace::framed]
    async fn rename_database(&self, req: RenameDatabaseReq) -> Result<RenameDatabaseReply> {
        let res = self.ctx.meta.rename_database(req).await?;
        Ok(res)
    }

    fn get_table_by_info(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        let storage = self.ctx.storage_factory.clone();
        storage.get_table(table_info)
    }

    #[async_backtrace::framed]
    async fn get_table_meta_by_id(
        &self,
        table_id: MetaId,
    ) -> common_exception::Result<(TableIdent, Arc<TableMeta>)> {
        let res = self.ctx.meta.get_table_by_id(table_id).await?;
        Ok(res)
    }

    #[async_backtrace::framed]
    async fn get_table(
        &self,
        tenant: &str,
        db_name: &str,
        table_name: &str,
    ) -> Result<Arc<dyn Table>> {
        let db = self.get_database(tenant, db_name).await?;
        db.get_table(table_name).await
    }

    #[async_backtrace::framed]
    async fn list_tables(&self, tenant: &str, db_name: &str) -> Result<Vec<Arc<dyn Table>>> {
        let db = self.get_database(tenant, db_name).await?;
        db.list_tables().await
    }

    #[async_backtrace::framed]
    async fn list_tables_history(
        &self,
        tenant: &str,
        db_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        let db = self.get_database(tenant, db_name).await?;
        db.list_tables_history().await
    }

    async fn get_drop_table_infos(
        &self,
        req: ListDroppedTableReq,
    ) -> Result<(Vec<Arc<dyn Table>>, Vec<DroppedId>)> {
        let ctx = DatabaseContext {
            meta: self.ctx.meta.clone(),
            storage_factory: self.ctx.storage_factory.clone(),
            tenant: self.tenant.clone(),
        };
        let resp = ctx.meta.get_drop_table_infos(req).await?;

        let drop_ids = resp.drop_ids.clone();
        let drop_table_infos = resp.drop_table_infos;

        let storage = ctx.storage_factory.clone();

        let mut tables = vec![];
        for table_info in drop_table_infos {
            tables.push(storage.get_table(table_info.as_ref())?);
        }
        Ok((tables, drop_ids))
    }

    async fn gc_drop_tables(&self, req: GcDroppedTableReq) -> Result<GcDroppedTableResp> {
        let meta = self.ctx.meta.clone();
        let resp = meta.gc_drop_tables(req).await?;
        Ok(resp)
    }

    #[async_backtrace::framed]
    async fn create_table(&self, req: CreateTableReq) -> Result<CreateTableReply> {
        let db = self
            .get_database(&req.name_ident.tenant, &req.name_ident.db_name)
            .await?;
        db.create_table(req).await
    }

    #[async_backtrace::framed]
    async fn drop_table_by_id(&self, req: DropTableByIdReq) -> Result<DropTableReply> {
        let res = self.ctx.meta.drop_table_by_id(req).await?;
        Ok(res)
    }

    #[async_backtrace::framed]
    async fn undrop_table(&self, req: UndropTableReq) -> Result<UndropTableReply> {
        let db = self
            .get_database(&req.name_ident.tenant, &req.name_ident.db_name)
            .await?;
        db.undrop_table(req).await
    }

    #[async_backtrace::framed]
    async fn rename_table(&self, req: RenameTableReq) -> Result<RenameTableReply> {
        let db = self
            .get_database(&req.name_ident.tenant, &req.name_ident.db_name)
            .await?;
        db.rename_table(req).await
    }

    #[async_backtrace::framed]
    async fn upsert_table_option(
        &self,
        tenant: &str,
        db_name: &str,
        req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply> {
        let db = self.get_database(tenant, db_name).await?;
        db.upsert_table_option(req).await
    }

    #[async_backtrace::framed]
    async fn update_table_meta(
        &self,
        table_info: &TableInfo,
        req: UpdateTableMetaReq,
    ) -> Result<UpdateTableMetaReply> {
        match table_info.db_type.clone() {
            DatabaseType::NormalDB => {
                info!(
                    "updating table meta. table desc: [{}], has copied files: [{}]?",
                    table_info.desc,
                    req.copied_files.is_some()
                );
                Ok(self.ctx.meta.update_table_meta(req).await?)
            }
            DatabaseType::ShareDB(share_ident) => {
                let db = self
                    .get_database(&share_ident.tenant, &share_ident.share_name)
                    .await?;
                db.update_table_meta(req).await
            }
        }
    }

    async fn set_table_column_mask_policy(
        &self,
        req: SetTableColumnMaskPolicyReq,
    ) -> Result<SetTableColumnMaskPolicyReply> {
        Ok(self.ctx.meta.set_table_column_mask_policy(req).await?)
    }

    #[async_backtrace::framed]
    async fn get_table_copied_file_info(
        &self,
        tenant: &str,
        db_name: &str,
        req: GetTableCopiedFileReq,
    ) -> Result<GetTableCopiedFileReply> {
        let db = self.get_database(tenant, db_name).await?;
        db.get_table_copied_file_info(req).await
    }

    #[async_backtrace::framed]
    async fn truncate_table(
        &self,
        table_info: &TableInfo,
        req: TruncateTableReq,
    ) -> Result<TruncateTableReply> {
        match table_info.db_type.clone() {
            DatabaseType::NormalDB => Ok(self.ctx.meta.truncate_table(req).await?),
            DatabaseType::ShareDB(share_ident) => {
                let db = self
                    .get_database(&share_ident.tenant, &share_ident.share_name)
                    .await?;
                db.truncate_table(req).await
            }
        }
    }

    #[async_backtrace::framed]
    async fn count_tables(&self, req: CountTablesReq) -> Result<CountTablesReply> {
        let res = self.ctx.meta.count_tables(req).await?;
        Ok(res)
    }

    #[async_backtrace::framed]
    async fn list_lock_revisions(&self, req: ListLockRevReq) -> Result<Vec<(u64, LockMeta)>> {
        Ok(self.ctx.meta.list_lock_revisions(req).await?)
    }

    #[async_backtrace::framed]
    async fn create_lock_revision(&self, req: CreateLockRevReq) -> Result<CreateLockRevReply> {
        Ok(self.ctx.meta.create_lock_revision(req).await?)
    }

    #[async_backtrace::framed]
    async fn extend_lock_revision(&self, req: ExtendLockRevReq) -> Result<()> {
        Ok(self.ctx.meta.extend_lock_revision(req).await?)
    }

    #[async_backtrace::framed]
    async fn delete_lock_revision(&self, req: DeleteLockRevReq) -> Result<()> {
        Ok(self.ctx.meta.delete_lock_revision(req).await?)
    }

    fn get_table_engines(&self) -> Vec<StorageDescription> {
        self.ctx.storage_factory.get_storage_descriptors()
    }

    async fn set_table_lvt(&self, table_id: u64, time: DateTime<Utc>) -> Result<SetLVTReply> {
        let req = SetLVTReq { table_id, time };
        let res = self.ctx.meta.set_table_lvt(req).await?;
        Ok(res)
    }

    async fn get_table_lvt(&self, table_id: u64) -> Result<GetLVTReply> {
        let req = GetLVTReq { table_id };
        let res = self.ctx.meta.get_table_lvt(req).await?;
        Ok(res)
    }
}
