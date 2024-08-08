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

use databend_common_catalog::catalog::Catalog;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::CatalogInfo;
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
use databend_common_meta_app::schema::ExtendLockRevReply;
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
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TruncateTableReply;
use databend_common_meta_app::schema::TruncateTableReq;
use databend_common_meta_app::schema::UndropDatabaseReply;
use databend_common_meta_app::schema::UndropDatabaseReq;
use databend_common_meta_app::schema::UndropTableByIdReq;
use databend_common_meta_app::schema::UndropTableReply;
use databend_common_meta_app::schema::UndropTableReq;
use databend_common_meta_app::schema::UpdateIndexReply;
use databend_common_meta_app::schema::UpdateIndexReq;
use databend_common_meta_app::schema::UpdateVirtualColumnReply;
use databend_common_meta_app::schema::UpdateVirtualColumnReq;
use databend_common_meta_app::schema::UpsertTableOptionReply;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_meta_app::schema::VirtualColumnMeta;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_types::MetaId;
use databend_common_meta_types::SeqV;

use crate::catalogs::InMemoryMetas;
use crate::catalogs::SYS_DB_ID_BEGIN;
use crate::catalogs::SYS_TBL_ID_BEGIN;
use crate::databases::Database;
use crate::databases::InformationSchemaDatabase;
use crate::databases::SystemDatabase;
use crate::storages::Table;

/// System Catalog contains ... all the system databases (no surprise :)
#[derive(Clone)]
pub struct ImmutableCatalog {
    // IT'S CASE SENSITIVE, SO WE WILL NEED TWO SAME DATABASE ONLY WITH THE NAME'S CASE
    info_schema_db: Arc<InformationSchemaDatabase>,
    sys_db: Arc<SystemDatabase>,
    sys_db_meta: Arc<InMemoryMetas>,
}

impl Debug for ImmutableCatalog {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        f.debug_struct("ImmutableCatalog").finish_non_exhaustive()
    }
}

impl ImmutableCatalog {
    #[async_backtrace::framed]
    pub async fn try_create_with_config(conf: &InnerConfig) -> Result<Self> {
        // The global db meta.
        let mut sys_db_meta = InMemoryMetas::create(SYS_DB_ID_BEGIN, SYS_TBL_ID_BEGIN);
        sys_db_meta.init_db("system");
        sys_db_meta.init_db("information_schema");

        let sys_db = SystemDatabase::create(&mut sys_db_meta, conf);
        let info_schema_db = InformationSchemaDatabase::create(&mut sys_db_meta);

        Ok(Self {
            info_schema_db: Arc::new(info_schema_db),
            sys_db: Arc::new(sys_db),
            sys_db_meta: Arc::new(sys_db_meta),
        })
    }
}

#[async_trait::async_trait]
impl Catalog for ImmutableCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> String {
        "default".to_string()
    }

    fn info(&self) -> Arc<CatalogInfo> {
        CatalogInfo::default().into()
    }

    #[async_backtrace::framed]
    async fn get_database(&self, _tenant: &Tenant, db_name: &str) -> Result<Arc<dyn Database>> {
        match db_name {
            "system" => Ok(self.sys_db.clone()),
            "information_schema" => Ok(self.info_schema_db.clone()),
            _ => Err(ErrorCode::UnknownDatabase(format!(
                "Unknown database {}",
                db_name
            ))),
        }
    }

    #[async_backtrace::framed]
    async fn list_databases(&self, _tenant: &Tenant) -> Result<Vec<Arc<dyn Database>>> {
        Ok(vec![self.sys_db.clone(), self.info_schema_db.clone()])
    }

    #[async_backtrace::framed]
    async fn create_database(&self, _req: CreateDatabaseReq) -> Result<CreateDatabaseReply> {
        Err(ErrorCode::Unimplemented("Cannot create system database"))
    }

    #[async_backtrace::framed]
    async fn drop_database(&self, _req: DropDatabaseReq) -> Result<DropDatabaseReply> {
        Err(ErrorCode::Unimplemented("Cannot drop system database"))
    }

    #[async_backtrace::framed]
    async fn rename_database(&self, _req: RenameDatabaseReq) -> Result<RenameDatabaseReply> {
        Err(ErrorCode::Unimplemented("Cannot rename system database"))
    }

    fn get_table_by_info(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        let table_id = table_info.ident.table_id;

        let table = self
            .sys_db_meta
            .get_by_id(&table_id)
            .ok_or_else(|| ErrorCode::UnknownTable(format!("Unknown table id: '{}'", table_id)))?;
        Ok(table.clone())
    }

    #[async_backtrace::framed]
    async fn get_table_meta_by_id(&self, table_id: MetaId) -> Result<Option<SeqV<TableMeta>>> {
        let table = self
            .sys_db_meta
            .get_by_id(&table_id)
            .ok_or_else(|| ErrorCode::UnknownTable(format!("Unknown table id: '{}'", table_id)))?;
        let ti = table.get_table_info();
        let seq_table_meta = SeqV::new(ti.ident.seq, ti.meta.clone());
        Ok(Some(seq_table_meta))
    }

    async fn mget_table_names_by_ids(
        &self,
        _tenant: &Tenant,
        table_ids: &[MetaId],
    ) -> Result<Vec<Option<String>>> {
        let mut table_name = Vec::with_capacity(table_ids.len());
        for id in table_ids {
            if let Some(table) = self.sys_db_meta.get_by_id(id) {
                table_name.push(Some(table.name().to_string()));
            }
        }
        Ok(table_name)
    }

    async fn get_db_name_by_id(&self, db_id: MetaId) -> Result<String> {
        if self.sys_db.get_db_info().ident.db_id == db_id {
            Ok("system".to_string())
        } else if self.info_schema_db.get_db_info().ident.db_id == db_id {
            Ok("information_schema".to_string())
        } else {
            Err(ErrorCode::UnknownDatabaseId(format!(
                "Unknown database id {}",
                db_id
            )))
        }
    }

    async fn mget_database_names_by_ids(
        &self,
        _tenant: &Tenant,
        db_ids: &[MetaId],
    ) -> Result<Vec<Option<String>>> {
        let mut res = Vec::new();
        for id in db_ids {
            if self.sys_db.get_db_info().ident.db_id == *id {
                res.push(Some("system".to_string()));
            } else if self.info_schema_db.get_db_info().ident.db_id == *id {
                res.push(Some("information_schema".to_string()));
            }
        }
        Ok(res)
    }

    async fn get_table_name_by_id(&self, table_id: MetaId) -> Result<Option<String>> {
        let table_name = self
            .sys_db_meta
            .get_by_id(&table_id)
            .map(|v| v.name().to_string());
        Ok(table_name)
    }

    #[async_backtrace::framed]
    async fn get_table(
        &self,
        tenant: &Tenant,
        db_name: &str,
        table_name: &str,
    ) -> Result<Arc<dyn Table>> {
        let _db = self.get_database(tenant, db_name).await?;

        self.sys_db_meta.get_by_name(db_name, table_name)
    }

    #[async_backtrace::framed]
    async fn list_tables(&self, _tenant: &Tenant, db_name: &str) -> Result<Vec<Arc<dyn Table>>> {
        self.sys_db_meta.get_all_tables(db_name)
    }

    #[async_backtrace::framed]
    async fn list_tables_history(
        &self,
        tenant: &Tenant,
        db_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        self.list_tables(tenant, db_name).await
    }

    #[async_backtrace::framed]
    async fn create_table(&self, _req: CreateTableReq) -> Result<CreateTableReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot create table in system database",
        ))
    }

    #[async_backtrace::framed]
    async fn drop_table_by_id(&self, _req: DropTableByIdReq) -> Result<DropTableReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot drop table in system database",
        ))
    }

    #[async_backtrace::framed]
    async fn undrop_table(&self, _req: UndropTableReq) -> Result<UndropTableReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot undrop table in system database",
        ))
    }

    async fn undrop_table_by_id(&self, _req: UndropTableByIdReq) -> Result<UndropTableReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot undrop table by id in system database",
        ))
    }

    #[async_backtrace::framed]
    async fn undrop_database(&self, _req: UndropDatabaseReq) -> Result<UndropDatabaseReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot undrop database in system database",
        ))
    }

    #[async_backtrace::framed]
    async fn rename_table(&self, _req: RenameTableReq) -> Result<RenameTableReply> {
        Err(ErrorCode::Unimplemented(
            "Cannot rename table in system database",
        ))
    }

    async fn commit_table_meta(&self, _req: CommitTableMetaReq) -> Result<CommitTableMetaReply> {
        Err(ErrorCode::Unimplemented(
            "cannot commit_table_meta in system database",
        ))
    }

    async fn create_table_index(&self, _req: CreateTableIndexReq) -> Result<CreateTableIndexReply> {
        unimplemented!()
    }

    async fn drop_table_index(&self, _req: DropTableIndexReq) -> Result<DropTableIndexReply> {
        unimplemented!()
    }

    #[async_backtrace::framed]
    async fn get_table_copied_file_info(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
        req: GetTableCopiedFileReq,
    ) -> Result<GetTableCopiedFileReply> {
        Err(ErrorCode::Unimplemented(format!(
            "get_table_copied_file_info not allowed for system database {:?}",
            req
        )))
    }

    #[async_backtrace::framed]
    async fn truncate_table(
        &self,
        _table_info: &TableInfo,
        req: TruncateTableReq,
    ) -> Result<TruncateTableReply> {
        Err(ErrorCode::Unimplemented(format!(
            "truncate_table not allowed for system database {:?}",
            req
        )))
    }

    #[async_backtrace::framed]
    async fn upsert_table_option(
        &self,
        _tenant: &Tenant,
        _db_name: &str,
        req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply> {
        Err(ErrorCode::Unimplemented(format!(
            "upsert table option not allowed for system database {:?}",
            req
        )))
    }

    #[async_backtrace::framed]
    async fn set_table_column_mask_policy(
        &self,
        req: SetTableColumnMaskPolicyReq,
    ) -> Result<SetTableColumnMaskPolicyReply> {
        Err(ErrorCode::Unimplemented(format!(
            "set_table_column_mask_policy not allowed for system database {:?}",
            req
        )))
    }

    #[async_backtrace::framed]
    async fn list_lock_revisions(&self, _req: ListLockRevReq) -> Result<Vec<(u64, LockMeta)>> {
        Err(ErrorCode::Unimplemented(
            "list_lock_revisions not allowed for system database",
        ))
    }

    #[async_backtrace::framed]
    async fn create_lock_revision(&self, _req: CreateLockRevReq) -> Result<CreateLockRevReply> {
        Err(ErrorCode::Unimplemented(
            "create_lock_revision not allowed for system database",
        ))
    }

    #[async_backtrace::framed]
    async fn extend_lock_revision(&self, _req: ExtendLockRevReq) -> Result<ExtendLockRevReply> {
        Err(ErrorCode::Unimplemented(
            "extend_lock_revision not allowed for system database",
        ))
    }

    #[async_backtrace::framed]
    async fn delete_lock_revision(&self, _req: DeleteLockRevReq) -> Result<()> {
        Err(ErrorCode::Unimplemented(
            "delete_lock_revision not allowed for system database",
        ))
    }

    #[async_backtrace::framed]
    async fn list_locks(&self, _req: ListLocksReq) -> Result<Vec<LockInfo>> {
        Err(ErrorCode::Unimplemented(
            "list_locks not allowed for system database",
        ))
    }

    // Table index

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

    // Virtual column

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
