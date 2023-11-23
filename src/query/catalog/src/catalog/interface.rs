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
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
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
use dyn_clone::DynClone;

use crate::database::Database;
use crate::table::Table;
use crate::table_args::TableArgs;
use crate::table_function::TableFunction;

#[derive(Default, Clone)]
pub struct StorageDescription {
    pub engine_name: String,
    pub comment: String,
    pub support_cluster_key: bool,
}

pub trait CatalogCreator: Send + Sync + Debug {
    fn try_create(&self, info: &CatalogInfo) -> Result<Arc<dyn Catalog>>;
}

#[async_trait::async_trait]
pub trait Catalog: DynClone + Send + Sync + Debug {
    /// Catalog itself

    // Get the name of the catalog.
    fn name(&self) -> String;
    // Get the info of the catalog.
    fn info(&self) -> CatalogInfo;

    /// Database.

    // Get the database by name.
    async fn get_database(&self, tenant: &str, db_name: &str) -> Result<Arc<dyn Database>>;

    // Get all the databases.
    async fn list_databases(&self, tenant: &str) -> Result<Vec<Arc<dyn Database>>>;

    // Operation with database.
    async fn create_database(&self, req: CreateDatabaseReq) -> Result<CreateDatabaseReply>;

    async fn drop_database(&self, req: DropDatabaseReq) -> Result<DropDatabaseReply>;

    async fn undrop_database(&self, req: UndropDatabaseReq) -> Result<UndropDatabaseReply>;

    async fn create_index(&self, req: CreateIndexReq) -> Result<CreateIndexReply>;

    async fn drop_index(&self, req: DropIndexReq) -> Result<DropIndexReply>;

    async fn get_index(&self, req: GetIndexReq) -> Result<GetIndexReply>;

    async fn update_index(&self, req: UpdateIndexReq) -> Result<UpdateIndexReply>;

    async fn list_indexes(&self, req: ListIndexesReq) -> Result<Vec<(u64, String, IndexMeta)>>;

    async fn list_index_ids_by_table_id(&self, req: ListIndexesByIdReq) -> Result<Vec<u64>>;

    async fn list_indexes_by_table_id(
        &self,
        req: ListIndexesByIdReq,
    ) -> Result<Vec<(u64, String, IndexMeta)>>;

    async fn create_virtual_column(
        &self,
        req: CreateVirtualColumnReq,
    ) -> Result<CreateVirtualColumnReply>;

    async fn update_virtual_column(
        &self,
        req: UpdateVirtualColumnReq,
    ) -> Result<UpdateVirtualColumnReply>;

    async fn drop_virtual_column(
        &self,
        req: DropVirtualColumnReq,
    ) -> Result<DropVirtualColumnReply>;

    async fn list_virtual_columns(
        &self,
        req: ListVirtualColumnsReq,
    ) -> Result<Vec<VirtualColumnMeta>>;

    #[async_backtrace::framed]
    async fn exists_database(&self, tenant: &str, db_name: &str) -> Result<bool> {
        match self.get_database(tenant, db_name).await {
            Ok(_) => Ok(true),
            Err(err) => {
                if err.code() == ErrorCode::UNKNOWN_DATABASE {
                    Ok(false)
                } else {
                    Err(err)
                }
            }
        }
    }

    async fn rename_database(&self, req: RenameDatabaseReq) -> Result<RenameDatabaseReply>;

    /// Table.

    // Build a `Arc<dyn Table>` from `TableInfo`.
    fn get_table_by_info(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>>;

    // Get the table meta by meta id.
    async fn get_table_meta_by_id(&self, table_id: MetaId) -> Result<(TableIdent, Arc<TableMeta>)>;

    // Get one table by db and table name.
    async fn get_table(
        &self,
        tenant: &str,
        db_name: &str,
        table_name: &str,
    ) -> Result<Arc<dyn Table>>;

    async fn list_tables(&self, tenant: &str, db_name: &str) -> Result<Vec<Arc<dyn Table>>>;
    async fn list_tables_history(&self, tenant: &str, db_name: &str)
    -> Result<Vec<Arc<dyn Table>>>;

    async fn get_drop_table_infos(
        &self,
        _req: ListDroppedTableReq,
    ) -> Result<(Vec<Arc<dyn Table>>, Vec<DroppedId>)> {
        Err(ErrorCode::Unimplemented(
            "'get_drop_table_infos' not implemented",
        ))
    }

    async fn gc_drop_tables(&self, _req: GcDroppedTableReq) -> Result<GcDroppedTableResp> {
        Err(ErrorCode::Unimplemented("'gc_drop_tables' not implemented"))
    }

    async fn create_table(&self, req: CreateTableReq) -> Result<CreateTableReply>;

    async fn drop_table_by_id(&self, req: DropTableByIdReq) -> Result<DropTableReply>;

    async fn undrop_table(&self, req: UndropTableReq) -> Result<UndropTableReply>;

    async fn rename_table(&self, req: RenameTableReq) -> Result<RenameTableReply>;

    // Check a db.table is exists or not.
    #[async_backtrace::framed]
    async fn exists_table(&self, tenant: &str, db_name: &str, table_name: &str) -> Result<bool> {
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

    async fn upsert_table_option(
        &self,
        tenant: &str,
        db_name: &str,
        req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply>;

    async fn update_table_meta(
        &self,
        table_info: &TableInfo,
        req: UpdateTableMetaReq,
    ) -> Result<UpdateTableMetaReply>;

    async fn set_table_column_mask_policy(
        &self,
        req: SetTableColumnMaskPolicyReq,
    ) -> Result<SetTableColumnMaskPolicyReply>;

    async fn count_tables(&self, req: CountTablesReq) -> Result<CountTablesReply>;

    async fn get_table_copied_file_info(
        &self,
        tenant: &str,
        db_name: &str,
        req: GetTableCopiedFileReq,
    ) -> Result<GetTableCopiedFileReply>;

    async fn truncate_table(
        &self,
        table_info: &TableInfo,
        req: TruncateTableReq,
    ) -> Result<TruncateTableReply>;

    async fn list_lock_revisions(&self, req: ListLockRevReq) -> Result<Vec<(u64, LockMeta)>>;

    async fn create_lock_revision(&self, req: CreateLockRevReq) -> Result<CreateLockRevReply>;

    async fn extend_lock_revision(&self, req: ExtendLockRevReq) -> Result<()>;

    async fn delete_lock_revision(&self, req: DeleteLockRevReq) -> Result<()>;

    /// Table function

    // Get function by name.
    fn get_table_function(
        &self,
        _func_name: &str,
        _tbl_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        Err(ErrorCode::Unimplemented(
            "'get_table_function' not implemented",
        ))
    }

    fn exists_table_function(&self, _func_name: &str) -> bool {
        false
    }

    // List all table functions' names.
    fn list_table_functions(&self) -> Vec<String> {
        unimplemented!()
    }

    fn as_any(&self) -> &dyn Any;

    // Get table engines
    fn get_table_engines(&self) -> Vec<StorageDescription> {
        unimplemented!()
    }

    async fn set_table_lvt(&self, _table_id: u64, _time: DateTime<Utc>) -> Result<SetLVTReply> {
        Err(ErrorCode::Unimplemented("'set_table_lvt' not implemented"))
    }

    async fn get_table_lvt(&self, _table_id: u64) -> Result<GetLVTReply> {
        Err(ErrorCode::Unimplemented("'get_table_lvt' not implemented"))
    }
}
