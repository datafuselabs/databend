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

use databend_common_catalog::catalog::StorageDescription;
use databend_common_catalog::database::Database;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_function::TableFunction;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::dictionary_name_ident::DictionaryNameIdent;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::CommitTableMetaReply;
use databend_common_meta_app::schema::CommitTableMetaReq;
use databend_common_meta_app::schema::CreateDatabaseReply;
use databend_common_meta_app::schema::CreateDatabaseReq;
use databend_common_meta_app::schema::CreateDictionaryReply;
use databend_common_meta_app::schema::CreateDictionaryReq;
use databend_common_meta_app::schema::CreateIndexReply;
use databend_common_meta_app::schema::CreateIndexReq;
use databend_common_meta_app::schema::CreateLockRevReply;
use databend_common_meta_app::schema::CreateLockRevReq;
use databend_common_meta_app::schema::CreateSequenceReply;
use databend_common_meta_app::schema::CreateSequenceReq;
use databend_common_meta_app::schema::CreateTableIndexReq;
use databend_common_meta_app::schema::CreateTableReply;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::CreateVirtualColumnReply;
use databend_common_meta_app::schema::CreateVirtualColumnReq;
use databend_common_meta_app::schema::DeleteLockRevReq;
use databend_common_meta_app::schema::DictionaryMeta;
use databend_common_meta_app::schema::DropDatabaseReply;
use databend_common_meta_app::schema::DropDatabaseReq;
use databend_common_meta_app::schema::DropIndexReq;
use databend_common_meta_app::schema::DropSequenceReply;
use databend_common_meta_app::schema::DropSequenceReq;
use databend_common_meta_app::schema::DropTableByIdReq;
use databend_common_meta_app::schema::DropTableIndexReq;
use databend_common_meta_app::schema::DropTableReply;
use databend_common_meta_app::schema::DropVirtualColumnReply;
use databend_common_meta_app::schema::DropVirtualColumnReq;
use databend_common_meta_app::schema::DroppedId;
use databend_common_meta_app::schema::ExtendLockRevReq;
use databend_common_meta_app::schema::GcDroppedTableReq;
use databend_common_meta_app::schema::GetDictionaryReply;
use databend_common_meta_app::schema::GetIndexReply;
use databend_common_meta_app::schema::GetIndexReq;
use databend_common_meta_app::schema::GetSequenceNextValueReply;
use databend_common_meta_app::schema::GetSequenceNextValueReq;
use databend_common_meta_app::schema::GetSequenceReply;
use databend_common_meta_app::schema::GetSequenceReq;
use databend_common_meta_app::schema::GetTableCopiedFileReply;
use databend_common_meta_app::schema::GetTableCopiedFileReq;
use databend_common_meta_app::schema::IndexMeta;
use databend_common_meta_app::schema::ListDictionaryReq;
use databend_common_meta_app::schema::ListDroppedTableReq;
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
use databend_common_meta_app::schema::UpdateDictionaryReply;
use databend_common_meta_app::schema::UpdateDictionaryReq;
use databend_common_meta_app::schema::UpdateIndexReply;
use databend_common_meta_app::schema::UpdateIndexReq;
use databend_common_meta_app::schema::UpdateMultiTableMetaReq;
use databend_common_meta_app::schema::UpdateMultiTableMetaResult;
use databend_common_meta_app::schema::UpdateVirtualColumnReply;
use databend_common_meta_app::schema::UpdateVirtualColumnReq;
use databend_common_meta_app::schema::UpsertTableOptionReply;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_meta_app::schema::VirtualColumnMeta;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_types::MetaId;
use databend_common_meta_types::SeqV;
use databend_storages_common_session::SessionState;
use databend_storages_common_session::TempTblMgrRef;
use databend_storages_common_session::TxnManagerRef;
use databend_storages_common_session::TxnState;
use databend_storages_common_table_meta::table::OPT_KEY_TEMP_PREFIX;
use databend_storages_common_table_meta::table_id_ranges::is_temp_table_id;

use crate::catalogs::default::MutableCatalog;
use crate::catalogs::Catalog;
#[derive(Clone, Debug)]
pub struct SessionCatalog {
    inner: MutableCatalog,
    txn_mgr: TxnManagerRef,
    temp_tbl_mgr: TempTblMgrRef,
}

impl SessionCatalog {
    pub fn create(inner: MutableCatalog, session_state: SessionState) -> Self {
        let SessionState {
            txn_mgr,
            temp_tbl_mgr,
        } = session_state;
        SessionCatalog {
            inner,
            txn_mgr,
            temp_tbl_mgr,
        }
    }
}

#[async_trait::async_trait]
impl Catalog for SessionCatalog {
    /// Catalog itself

    // Get the name of the catalog.
    fn name(&self) -> String {
        self.inner.name()
    }
    // Get the info of the catalog.
    fn info(&self) -> Arc<CatalogInfo> {
        self.inner.info()
    }

    /// Database.

    // Get the database by name.
    async fn get_database(&self, tenant: &Tenant, db_name: &str) -> Result<Arc<dyn Database>> {
        self.inner.get_database(tenant, db_name).await
    }

    // Get all the databases.
    async fn list_databases(&self, tenant: &Tenant) -> Result<Vec<Arc<dyn Database>>> {
        self.inner.list_databases(tenant).await
    }

    // Operation with database.
    async fn create_database(&self, req: CreateDatabaseReq) -> Result<CreateDatabaseReply> {
        self.inner.create_database(req).await
    }

    async fn drop_database(&self, req: DropDatabaseReq) -> Result<DropDatabaseReply> {
        self.inner.drop_database(req).await
    }

    async fn undrop_database(&self, req: UndropDatabaseReq) -> Result<UndropDatabaseReply> {
        self.inner.undrop_database(req).await
    }

    async fn create_index(&self, req: CreateIndexReq) -> Result<CreateIndexReply> {
        if is_temp_table_id(req.meta.table_id) {
            return Err(ErrorCode::StorageUnsupported(format!(
                "CreateIndex: table id {} is a temporary table id",
                req.meta.table_id
            )));
        }
        self.inner.create_index(req).await
    }

    async fn drop_index(&self, req: DropIndexReq) -> Result<()> {
        self.inner.drop_index(req).await
    }

    async fn get_index(&self, req: GetIndexReq) -> Result<GetIndexReply> {
        self.inner.get_index(req).await
    }

    async fn update_index(&self, req: UpdateIndexReq) -> Result<UpdateIndexReply> {
        self.inner.update_index(req).await
    }

    async fn list_indexes(&self, req: ListIndexesReq) -> Result<Vec<(u64, String, IndexMeta)>> {
        if req.table_id.is_some_and(is_temp_table_id) {
            return Ok(vec![]);
        }
        self.inner.list_indexes(req).await
    }

    async fn list_index_ids_by_table_id(&self, req: ListIndexesByIdReq) -> Result<Vec<u64>> {
        if is_temp_table_id(req.table_id) {
            return Ok(vec![]);
        }
        self.inner.list_index_ids_by_table_id(req).await
    }

    async fn list_indexes_by_table_id(
        &self,
        req: ListIndexesByIdReq,
    ) -> Result<Vec<(u64, String, IndexMeta)>> {
        if is_temp_table_id(req.table_id) {
            return Ok(vec![]);
        }
        self.inner.list_indexes_by_table_id(req).await
    }

    async fn create_virtual_column(
        &self,
        req: CreateVirtualColumnReq,
    ) -> Result<CreateVirtualColumnReply> {
        if is_temp_table_id(req.name_ident.table_id()) {
            return Err(ErrorCode::StorageUnsupported(format!(
                "CreateVirtualColumn: table id {} is a temporary table id",
                req.name_ident.table_id()
            )));
        }
        self.inner.create_virtual_column(req).await
    }

    async fn update_virtual_column(
        &self,
        req: UpdateVirtualColumnReq,
    ) -> Result<UpdateVirtualColumnReply> {
        if is_temp_table_id(req.name_ident.table_id()) {
            return Err(ErrorCode::StorageUnsupported(format!(
                "UpdateVirtualColumn: table id {} is a temporary table id",
                req.name_ident.table_id()
            )));
        }
        self.inner.update_virtual_column(req).await
    }

    async fn drop_virtual_column(
        &self,
        req: DropVirtualColumnReq,
    ) -> Result<DropVirtualColumnReply> {
        if is_temp_table_id(req.name_ident.table_id()) {
            return Err(ErrorCode::StorageUnsupported(format!(
                "DropVirtualColumn: table id {} is a temporary table id",
                req.name_ident.table_id()
            )));
        }
        self.inner.drop_virtual_column(req).await
    }

    async fn list_virtual_columns(
        &self,
        req: ListVirtualColumnsReq,
    ) -> Result<Vec<VirtualColumnMeta>> {
        if req.table_id.is_some_and(is_temp_table_id) {
            return Ok(vec![]);
        }
        self.inner.list_virtual_columns(req).await
    }

    async fn rename_database(&self, req: RenameDatabaseReq) -> Result<RenameDatabaseReply> {
        self.inner.rename_database(req).await
    }

    /// Table.

    // Build a `Arc<dyn Table>` from `TableInfo`.
    fn get_table_by_info(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        self.inner.get_table_by_info(table_info)
    }

    async fn get_table_meta_by_id(&self, table_id: MetaId) -> Result<Option<SeqV<TableMeta>>> {
        if let Some(t) = {
            let guard = self.txn_mgr.lock();
            if guard.is_active() {
                guard.get_table_from_buffer_by_id(table_id)
            } else {
                None
            }
        } {
            return Ok(Some(SeqV::new(t.ident.seq, t.meta.clone())));
        }
        if is_temp_table_id(table_id) {
            self.temp_tbl_mgr.lock().get_table_meta_by_id(table_id)
        } else {
            self.inner.get_table_meta_by_id(table_id).await
        }
    }

    async fn mget_table_names_by_ids(
        &self,
        tenant: &Tenant,
        table_ids: &[MetaId],
    ) -> databend_common_exception::Result<Vec<Option<String>>> {
        self.inner.mget_table_names_by_ids(tenant, table_ids).await
    }

    async fn get_table_name_by_id(&self, table_id: MetaId) -> Result<Option<String>> {
        if let Some(name) = self.temp_tbl_mgr.lock().get_table_name_by_id(table_id) {
            return Ok(Some(name));
        }
        self.inner.get_table_name_by_id(table_id).await
    }

    // Get the db name by meta id.
    async fn get_db_name_by_id(&self, db_id: MetaId) -> Result<String> {
        self.inner.get_db_name_by_id(db_id).await
    }

    // Mget the dbs name by meta ids.
    async fn mget_database_names_by_ids(
        &self,
        tenant: &Tenant,
        db_ids: &[MetaId],
    ) -> databend_common_exception::Result<Vec<Option<String>>> {
        self.inner.mget_database_names_by_ids(tenant, db_ids).await
    }

    // Get one table by db and table name.
    async fn get_table(
        &self,
        tenant: &Tenant,
        db_name: &str,
        table_name: &str,
    ) -> Result<Arc<dyn Table>> {
        let (table_in_txn, is_active) = {
            let guard = self.txn_mgr.lock();
            if guard.is_active() {
                (
                    guard
                        .get_table_from_buffer(tenant, db_name, table_name)
                        .map(|table_info| self.get_table_by_info(&table_info)),
                    true,
                )
            } else {
                (None, false)
            }
        };
        if let Some(table) = table_in_txn {
            return table;
        }
        if let Some(table) = self.temp_tbl_mgr.lock().get_table(db_name, table_name)? {
            return self.get_table_by_info(&table);
        }
        let table = self.inner.get_table(tenant, db_name, table_name).await?;
        if table.is_stream() && is_active {
            self.txn_mgr
                .lock()
                .upsert_table_desc_to_id(table.get_table_info().clone());
        }
        Ok(table)
    }

    async fn list_tables(&self, tenant: &Tenant, db_name: &str) -> Result<Vec<Arc<dyn Table>>> {
        self.inner.list_tables(tenant, db_name).await
    }

    fn list_temporary_tables(&self) -> Result<Vec<TableInfo>> {
        self.temp_tbl_mgr.lock().list_tables()
    }

    // Get one table identified as dropped by db and table name.
    async fn get_table_history(
        &self,
        tenant: &Tenant,
        db_name: &str,
        table_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        self.inner
            .get_table_history(tenant, db_name, table_name)
            .await
    }

    async fn list_tables_history(
        &self,
        tenant: &Tenant,
        db_name: &str,
    ) -> Result<Vec<Arc<dyn Table>>> {
        self.inner.list_tables_history(tenant, db_name).await
    }

    async fn get_drop_table_infos(
        &self,
        req: ListDroppedTableReq,
    ) -> Result<(Vec<Arc<dyn Table>>, Vec<DroppedId>)> {
        self.inner.get_drop_table_infos(req).await
    }

    async fn gc_drop_tables(&self, req: GcDroppedTableReq) -> Result<()> {
        self.inner.gc_drop_tables(req).await
    }

    async fn create_table(&self, req: CreateTableReq) -> Result<CreateTableReply> {
        match req.table_meta.options.get(OPT_KEY_TEMP_PREFIX) {
            Some(_) => self.temp_tbl_mgr.lock().create_table(req),
            None => self.inner.create_table(req).await,
        }
    }

    async fn drop_table_by_id(&self, req: DropTableByIdReq) -> Result<DropTableReply> {
        if let Some(reply) = databend_storages_common_session::drop_table_by_id(
            self.temp_tbl_mgr.clone(),
            req.clone(),
        )
        .await?
        {
            return Ok(reply);
        }
        self.inner.drop_table_by_id(req).await
    }

    async fn undrop_table(&self, req: UndropTableReq) -> Result<UndropTableReply> {
        self.inner.undrop_table(req).await
    }

    async fn undrop_table_by_id(&self, req: UndropTableByIdReq) -> Result<UndropTableReply> {
        self.inner.undrop_table_by_id(req).await
    }

    async fn commit_table_meta(&self, req: CommitTableMetaReq) -> Result<CommitTableMetaReply> {
        if is_temp_table_id(req.table_id) {
            self.temp_tbl_mgr.lock().commit_table_meta(&req)
        } else {
            self.inner.commit_table_meta(req).await
        }
    }

    async fn rename_table(&self, req: RenameTableReq) -> Result<RenameTableReply> {
        let reply = self.temp_tbl_mgr.lock().rename_table(&req)?;
        match reply {
            Some(r) => Ok(r),
            None => self.inner.rename_table(req).await,
        }
    }

    async fn upsert_table_option(
        &self,
        tenant: &Tenant,
        db_name: &str,
        req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply> {
        if is_temp_table_id(req.table_id) {
            self.temp_tbl_mgr.lock().upsert_table_option(req)
        } else {
            self.inner.upsert_table_option(tenant, db_name, req).await
        }
    }

    async fn retryable_update_multi_table_meta(
        &self,
        mut req: UpdateMultiTableMetaReq,
    ) -> Result<UpdateMultiTableMetaResult> {
        let state = self.txn_mgr.lock().state();
        match state {
            TxnState::AutoCommit => {
                let update_temp_tables = std::mem::take(&mut req.update_temp_tables);
                let reply = if req.is_empty() {
                    Ok(Default::default())
                } else {
                    self.inner.retryable_update_multi_table_meta(req).await?
                };
                self.temp_tbl_mgr
                    .lock()
                    .update_multi_table_meta(update_temp_tables);
                Ok(reply)
            }
            TxnState::Active => {
                self.txn_mgr.lock().update_multi_table_meta(req);
                Ok(Ok(Default::default()))
            }
            TxnState::Fail => unreachable!(),
        }
    }

    async fn set_table_column_mask_policy(
        &self,
        req: SetTableColumnMaskPolicyReq,
    ) -> Result<SetTableColumnMaskPolicyReply> {
        if is_temp_table_id(req.table_id) {
            return Err(ErrorCode::StorageUnsupported(format!(
                "SetTableColumnMaskPolicy: table id {} is a temporary table id",
                req.table_id
            )));
        }
        self.inner.set_table_column_mask_policy(req).await
    }

    async fn create_table_index(&self, req: CreateTableIndexReq) -> Result<()> {
        if is_temp_table_id(req.table_id) {
            return Err(ErrorCode::StorageUnsupported(format!(
                "CreateTableIndex: table id {} is a temporary table id",
                req.table_id
            )));
        }
        self.inner.create_table_index(req).await
    }

    async fn drop_table_index(&self, req: DropTableIndexReq) -> Result<()> {
        if is_temp_table_id(req.table_id) {
            return Err(ErrorCode::StorageUnsupported(format!(
                "DropTableIndex: table id {} is a temporary table id",
                req.table_id
            )));
        }
        self.inner.drop_table_index(req).await
    }

    async fn get_table_copied_file_info(
        &self,
        tenant: &Tenant,
        db_name: &str,
        req: GetTableCopiedFileReq,
    ) -> Result<GetTableCopiedFileReply> {
        let table_id = req.table_id;
        let mut reply = if is_temp_table_id(table_id) {
            self.temp_tbl_mgr
                .lock()
                .get_table_copied_file_info(req.clone())?
        } else {
            self.inner
                .get_table_copied_file_info(tenant, db_name, req)
                .await?
        };
        reply
            .file_info
            .extend(self.txn_mgr.lock().get_table_copied_file_info(table_id));
        Ok(reply)
    }

    async fn truncate_table(
        &self,
        table_info: &TableInfo,
        req: TruncateTableReq,
    ) -> Result<TruncateTableReply> {
        if is_temp_table_id(req.table_id) {
            self.temp_tbl_mgr.lock().truncate_table(req.table_id)
        } else {
            self.inner.truncate_table(table_info, req).await
        }
    }

    async fn list_lock_revisions(&self, req: ListLockRevReq) -> Result<Vec<(u64, LockMeta)>> {
        if is_temp_table_id(req.lock_key.get_table_id()) {
            return Ok(vec![]);
        }
        self.inner.list_lock_revisions(req).await
    }

    async fn create_lock_revision(&self, req: CreateLockRevReq) -> Result<CreateLockRevReply> {
        if is_temp_table_id(req.lock_key.get_table_id()) {
            return Err(ErrorCode::StorageUnsupported(format!(
                "CreateLockRevision: table id {} is a temporary table id",
                req.lock_key.get_table_id()
            )));
        }
        self.inner.create_lock_revision(req).await
    }

    async fn extend_lock_revision(&self, req: ExtendLockRevReq) -> Result<()> {
        if is_temp_table_id(req.lock_key.get_table_id()) {
            return Err(ErrorCode::StorageUnsupported(format!(
                "ExtendLockRevision: table id {} is a temporary table id",
                req.lock_key.get_table_id()
            )));
        }
        self.inner.extend_lock_revision(req).await
    }

    async fn delete_lock_revision(&self, req: DeleteLockRevReq) -> Result<()> {
        if is_temp_table_id(req.lock_key.get_table_id()) {
            return Err(ErrorCode::StorageUnsupported(format!(
                "DeleteLockRevision: table id {} is a temporary table id",
                req.lock_key.get_table_id()
            )));
        }
        self.inner.delete_lock_revision(req).await
    }

    async fn list_locks(&self, req: ListLocksReq) -> Result<Vec<LockInfo>> {
        self.inner.list_locks(req).await
    }

    /// Table function

    // Get function by name.
    fn get_table_function(
        &self,
        func_name: &str,
        tbl_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        self.inner.get_table_function(func_name, tbl_args)
    }

    fn exists_table_function(&self, func_name: &str) -> bool {
        self.inner.exists_table_function(func_name)
    }

    // List all table functions' names.
    fn list_table_functions(&self) -> Vec<String> {
        self.inner.list_table_functions()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    // Get table engines
    fn get_table_engines(&self) -> Vec<StorageDescription> {
        self.inner.get_table_engines()
    }

    // Get stream source table from buffer by stream desc.
    fn get_stream_source_table(
        &self,
        stream_desc: &str,
        max_batch_size: Option<u64>,
    ) -> Result<Option<Arc<dyn Table>>> {
        let is_active = self.txn_mgr.lock().is_active();
        if is_active {
            self.txn_mgr
                .lock()
                .get_stream_table(stream_desc)
                .map(|stream| {
                    if stream.max_batch_size != max_batch_size {
                        Err(ErrorCode::StorageUnsupported(
                            "Within the same transaction, the batch size for a stream must remain consistent",
                        ))
                    } else {
                        self.get_table_by_info(&stream.source)
                    }
                    })
                .transpose()
        } else {
            Ok(None)
        }
    }

    // Cache stream source table to buffer.
    fn cache_stream_source_table(
        &self,
        stream: TableInfo,
        source: TableInfo,
        max_batch_size: Option<u64>,
    ) {
        let is_active = self.txn_mgr.lock().is_active();
        if is_active {
            self.txn_mgr
                .lock()
                .upsert_stream_table(stream, source, max_batch_size);
        }
    }

    async fn create_sequence(&self, req: CreateSequenceReq) -> Result<CreateSequenceReply> {
        self.inner.create_sequence(req).await
    }
    async fn get_sequence(&self, req: GetSequenceReq) -> Result<GetSequenceReply> {
        self.inner.get_sequence(req).await
    }

    async fn get_sequence_next_value(
        &self,
        req: GetSequenceNextValueReq,
    ) -> Result<GetSequenceNextValueReply> {
        self.inner.get_sequence_next_value(req).await
    }

    async fn drop_sequence(&self, req: DropSequenceReq) -> Result<DropSequenceReply> {
        self.inner.drop_sequence(req).await
    }

    /// Dictionary
    async fn create_dictionary(&self, req: CreateDictionaryReq) -> Result<CreateDictionaryReply> {
        self.inner.create_dictionary(req).await
    }

    async fn update_dictionary(&self, req: UpdateDictionaryReq) -> Result<UpdateDictionaryReply> {
        self.inner.update_dictionary(req).await
    }

    async fn drop_dictionary(
        &self,
        dict_ident: DictionaryNameIdent,
    ) -> Result<Option<SeqV<DictionaryMeta>>> {
        self.inner.drop_dictionary(dict_ident).await
    }

    async fn get_dictionary(&self, req: DictionaryNameIdent) -> Result<Option<GetDictionaryReply>> {
        self.inner.get_dictionary(req).await
    }

    async fn list_dictionaries(
        &self,
        req: ListDictionaryReq,
    ) -> Result<Vec<(String, DictionaryMeta)>> {
        self.inner.list_dictionaries(req).await
    }

    async fn set_table_lvt(&self, req: SetLVTReq) -> Result<SetLVTReply> {
        self.inner.set_table_lvt(req).await
    }
}

impl SessionCatalog {
    pub fn disable_table_info_refresh(&mut self) {
        self.inner.disable_table_info_refresh();
    }

    pub fn inner(&self) -> MutableCatalog {
        self.inner.clone()
    }
}
