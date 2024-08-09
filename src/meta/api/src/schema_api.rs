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

use std::sync::Arc;

use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::CommitTableMetaReply;
use databend_common_meta_app::schema::CommitTableMetaReq;
use databend_common_meta_app::schema::CreateCatalogReply;
use databend_common_meta_app::schema::CreateCatalogReq;
use databend_common_meta_app::schema::CreateDatabaseReply;
use databend_common_meta_app::schema::CreateDatabaseReq;
use databend_common_meta_app::schema::CreateDictionaryReply;
use databend_common_meta_app::schema::CreateDictionaryReq;
use databend_common_meta_app::schema::CreateIndexReply;
use databend_common_meta_app::schema::CreateIndexReq;
use databend_common_meta_app::schema::CreateLockRevReply;
use databend_common_meta_app::schema::CreateLockRevReq;
use databend_common_meta_app::schema::CreateTableIndexReply;
use databend_common_meta_app::schema::CreateTableIndexReq;
use databend_common_meta_app::schema::CreateTableReply;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::CreateVirtualColumnReply;
use databend_common_meta_app::schema::CreateVirtualColumnReq;
use databend_common_meta_app::schema::DatabaseInfo;
use databend_common_meta_app::schema::DeleteLockRevReq;
use databend_common_meta_app::schema::DictionaryMeta;
use databend_common_meta_app::schema::DropCatalogReply;
use databend_common_meta_app::schema::DropCatalogReq;
use databend_common_meta_app::schema::DropDatabaseReply;
use databend_common_meta_app::schema::DropDatabaseReq;
use databend_common_meta_app::schema::DropDictionaryReply;
use databend_common_meta_app::schema::DropDictionaryReq;
use databend_common_meta_app::schema::DropIndexReply;
use databend_common_meta_app::schema::DropIndexReq;
use databend_common_meta_app::schema::DropTableByIdReq;
use databend_common_meta_app::schema::DropTableIndexReply;
use databend_common_meta_app::schema::DropTableIndexReq;
use databend_common_meta_app::schema::DropTableReply;
use databend_common_meta_app::schema::DropVirtualColumnReply;
use databend_common_meta_app::schema::DropVirtualColumnReq;
use databend_common_meta_app::schema::ExtendLockRevReq;
use databend_common_meta_app::schema::GcDroppedTableReq;
use databend_common_meta_app::schema::GcDroppedTableResp;
use databend_common_meta_app::schema::GetCatalogReq;
use databend_common_meta_app::schema::GetDatabaseReq;
use databend_common_meta_app::schema::GetDictionaryReply;
use databend_common_meta_app::schema::GetDictionaryReq;
use databend_common_meta_app::schema::GetIndexReply;
use databend_common_meta_app::schema::GetIndexReq;
use databend_common_meta_app::schema::GetLVTReply;
use databend_common_meta_app::schema::GetLVTReq;
use databend_common_meta_app::schema::GetTableCopiedFileReply;
use databend_common_meta_app::schema::GetTableCopiedFileReq;
use databend_common_meta_app::schema::GetTableReq;
use databend_common_meta_app::schema::IndexMeta;
use databend_common_meta_app::schema::ListCatalogReq;
use databend_common_meta_app::schema::ListDatabaseReq;
use databend_common_meta_app::schema::ListDictionaryReq;
use databend_common_meta_app::schema::ListDroppedTableReq;
use databend_common_meta_app::schema::ListDroppedTableResp;
use databend_common_meta_app::schema::ListIndexesByIdReq;
use databend_common_meta_app::schema::ListIndexesReq;
use databend_common_meta_app::schema::ListLockRevReq;
use databend_common_meta_app::schema::ListLocksReq;
use databend_common_meta_app::schema::ListTableReq;
use databend_common_meta_app::schema::ListVirtualColumnsReq;
use databend_common_meta_app::schema::LockInfo;
use databend_common_meta_app::schema::LockMeta;
use databend_common_meta_app::schema::RenameDatabaseReply;
use databend_common_meta_app::schema::RenameDatabaseReq;
use databend_common_meta_app::schema::RenameTableReply;
use databend_common_meta_app::schema::RenameTableReq;
use databend_common_meta_app::schema::SetLVTReply;
use databend_common_meta_app::schema::SetLVTReq;
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
use databend_common_meta_app::schema::UpdateMultiTableMetaReq;
use databend_common_meta_app::schema::UpdateMultiTableMetaResult;
use databend_common_meta_app::schema::UpdateVirtualColumnReply;
use databend_common_meta_app::schema::UpdateVirtualColumnReq;
use databend_common_meta_app::schema::UpsertTableOptionReply;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_meta_app::schema::VirtualColumnMeta;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::MetaId;
use databend_common_meta_types::SeqV;

use crate::kv_app_error::KVAppError;

/// SchemaApi defines APIs that provides schema storage, such as database, table.
#[async_trait::async_trait]
pub trait SchemaApi: Send + Sync {
    // database

    async fn create_database(
        &self,
        req: CreateDatabaseReq,
    ) -> Result<CreateDatabaseReply, KVAppError>;

    async fn drop_database(&self, req: DropDatabaseReq) -> Result<DropDatabaseReply, KVAppError>;

    async fn undrop_database(
        &self,
        req: UndropDatabaseReq,
    ) -> Result<UndropDatabaseReply, KVAppError>;

    async fn get_database(&self, req: GetDatabaseReq) -> Result<Arc<DatabaseInfo>, KVAppError>;

    async fn list_databases(
        &self,
        req: ListDatabaseReq,
    ) -> Result<Vec<Arc<DatabaseInfo>>, KVAppError>;

    async fn rename_database(
        &self,
        req: RenameDatabaseReq,
    ) -> Result<RenameDatabaseReply, KVAppError>;

    async fn get_database_history(
        &self,
        req: ListDatabaseReq,
    ) -> Result<Vec<Arc<DatabaseInfo>>, KVAppError>;

    // index

    async fn create_index(&self, req: CreateIndexReq) -> Result<CreateIndexReply, KVAppError>;

    async fn drop_index(&self, req: DropIndexReq) -> Result<DropIndexReply, KVAppError>;

    async fn get_index(&self, req: GetIndexReq) -> Result<GetIndexReply, KVAppError>;

    async fn update_index(&self, req: UpdateIndexReq) -> Result<UpdateIndexReply, KVAppError>;

    async fn list_indexes(
        &self,
        req: ListIndexesReq,
    ) -> Result<Vec<(u64, String, IndexMeta)>, KVAppError>;

    async fn list_index_ids_by_table_id(
        &self,
        req: ListIndexesByIdReq,
    ) -> Result<Vec<u64>, KVAppError>;

    async fn list_indexes_by_table_id(
        &self,
        req: ListIndexesByIdReq,
    ) -> Result<Vec<(u64, String, IndexMeta)>, KVAppError>;

    // virtual column

    async fn create_virtual_column(
        &self,
        req: CreateVirtualColumnReq,
    ) -> Result<CreateVirtualColumnReply, KVAppError>;

    async fn update_virtual_column(
        &self,
        req: UpdateVirtualColumnReq,
    ) -> Result<UpdateVirtualColumnReply, KVAppError>;

    async fn drop_virtual_column(
        &self,
        req: DropVirtualColumnReq,
    ) -> Result<DropVirtualColumnReply, KVAppError>;

    async fn list_virtual_columns(
        &self,
        req: ListVirtualColumnsReq,
    ) -> Result<Vec<VirtualColumnMeta>, KVAppError>;

    // table

    async fn create_table(&self, req: CreateTableReq) -> Result<CreateTableReply, KVAppError>;

    async fn drop_table_by_id(&self, req: DropTableByIdReq) -> Result<DropTableReply, KVAppError>;

    async fn commit_table_meta(
        &self,
        req: CommitTableMetaReq,
    ) -> Result<CommitTableMetaReply, KVAppError>;

    async fn undrop_table(&self, req: UndropTableReq) -> Result<UndropTableReply, KVAppError>;

    async fn undrop_table_by_id(
        &self,
        req: UndropTableByIdReq,
    ) -> Result<UndropTableReply, KVAppError>;

    async fn rename_table(&self, req: RenameTableReq) -> Result<RenameTableReply, KVAppError>;

    async fn get_table(&self, req: GetTableReq) -> Result<Arc<TableInfo>, KVAppError>;

    async fn get_table_history(&self, req: ListTableReq)
    -> Result<Vec<Arc<TableInfo>>, KVAppError>;

    async fn list_tables(&self, req: ListTableReq) -> Result<Vec<Arc<TableInfo>>, KVAppError>;

    /// Return TableMeta by table_id.
    ///
    /// It returns None instead of KVAppError, if table_id does not exist
    async fn get_table_by_id(&self, table_id: MetaId)
    -> Result<Option<SeqV<TableMeta>>, MetaError>;

    async fn mget_table_names_by_ids(
        &self,
        table_ids: &[MetaId],
    ) -> Result<Vec<Option<String>>, KVAppError>;

    async fn mget_database_names_by_ids(
        &self,
        db_ids: &[MetaId],
    ) -> Result<Vec<Option<String>>, KVAppError>;

    async fn get_db_name_by_id(&self, db_id: MetaId) -> Result<String, KVAppError>;

    async fn get_table_name_by_id(&self, table_id: MetaId) -> Result<Option<String>, MetaError>;

    async fn get_table_copied_file_info(
        &self,
        req: GetTableCopiedFileReq,
    ) -> Result<GetTableCopiedFileReply, KVAppError>;

    async fn truncate_table(&self, req: TruncateTableReq)
    -> Result<TruncateTableReply, KVAppError>;

    async fn upsert_table_option(
        &self,
        req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply, KVAppError>;

    async fn update_multi_table_meta(
        &self,
        req: UpdateMultiTableMetaReq,
    ) -> Result<UpdateMultiTableMetaResult, KVAppError>;

    async fn set_table_column_mask_policy(
        &self,
        req: SetTableColumnMaskPolicyReq,
    ) -> Result<SetTableColumnMaskPolicyReply, KVAppError>;

    async fn create_table_index(
        &self,
        req: CreateTableIndexReq,
    ) -> Result<CreateTableIndexReply, KVAppError>;

    async fn drop_table_index(
        &self,
        req: DropTableIndexReq,
    ) -> Result<DropTableIndexReply, KVAppError>;

    async fn get_drop_table_infos(
        &self,
        req: ListDroppedTableReq,
    ) -> Result<ListDroppedTableResp, KVAppError>;

    async fn gc_drop_tables(
        &self,
        req: GcDroppedTableReq,
    ) -> Result<GcDroppedTableResp, KVAppError>;

    async fn list_lock_revisions(
        &self,
        req: ListLockRevReq,
    ) -> Result<Vec<(u64, LockMeta)>, KVAppError>;

    async fn create_lock_revision(
        &self,
        req: CreateLockRevReq,
    ) -> Result<CreateLockRevReply, KVAppError>;

    async fn extend_lock_revision(&self, req: ExtendLockRevReq) -> Result<(), KVAppError>;

    async fn delete_lock_revision(&self, req: DeleteLockRevReq) -> Result<(), KVAppError>;

    async fn list_locks(&self, req: ListLocksReq) -> Result<Vec<LockInfo>, KVAppError>;

    async fn create_catalog(&self, req: CreateCatalogReq)
    -> Result<CreateCatalogReply, KVAppError>;

    async fn get_catalog(&self, req: GetCatalogReq) -> Result<Arc<CatalogInfo>, KVAppError>;

    async fn drop_catalog(&self, req: DropCatalogReq) -> Result<DropCatalogReply, KVAppError>;

    async fn list_catalogs(&self, req: ListCatalogReq)
    -> Result<Vec<Arc<CatalogInfo>>, KVAppError>;

    // least visible time
    async fn set_table_lvt(&self, req: SetLVTReq) -> Result<SetLVTReply, KVAppError>;
    async fn get_table_lvt(&self, req: GetLVTReq) -> Result<GetLVTReply, KVAppError>;

    fn name(&self) -> String;

    // dictionary
    async fn create_dictionary(
        &self,
        req: CreateDictionaryReq,
    ) -> Result<CreateDictionaryReply, KVAppError>;

    async fn drop_dictionary(
        &self,
        req: DropDictionaryReq,
    ) -> Result<DropDictionaryReply, KVAppError>;

    async fn get_dictionary(
        &self,
        req: GetDictionaryReq,
    ) -> Result<Option<GetDictionaryReply>, KVAppError>;

    async fn list_dictionaries(
        &self,
        req: ListDictionaryReq,
    ) -> Result<Vec<DictionaryMeta>, KVAppError>;
}
