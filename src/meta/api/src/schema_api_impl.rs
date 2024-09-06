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

use std::cmp::min;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt::Display;
use std::sync::Arc;
use std::time::Duration;

use chrono::DateTime;
use chrono::Utc;
use databend_common_base::base::uuid::Uuid;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::app_error::CommitTableMetaError;
use databend_common_meta_app::app_error::CreateAsDropTableWithoutDropTime;
use databend_common_meta_app::app_error::CreateDatabaseWithDropTime;
use databend_common_meta_app::app_error::CreateTableWithDropTime;
use databend_common_meta_app::app_error::DatabaseAlreadyExists;
use databend_common_meta_app::app_error::DropDbWithDropTime;
use databend_common_meta_app::app_error::DropTableWithDropTime;
use databend_common_meta_app::app_error::DuplicatedIndexColumnId;
use databend_common_meta_app::app_error::IndexColumnIdNotFound;
use databend_common_meta_app::app_error::MultiStmtTxnCommitFailed;
use databend_common_meta_app::app_error::StreamAlreadyExists;
use databend_common_meta_app::app_error::StreamVersionMismatched;
use databend_common_meta_app::app_error::TableAlreadyExists;
use databend_common_meta_app::app_error::TableLockExpired;
use databend_common_meta_app::app_error::TableVersionMismatched;
use databend_common_meta_app::app_error::UndropDbHasNoHistory;
use databend_common_meta_app::app_error::UndropDbWithNoDropTime;
use databend_common_meta_app::app_error::UndropTableAlreadyExists;
use databend_common_meta_app::app_error::UndropTableHasNoHistory;
use databend_common_meta_app::app_error::UndropTableWithNoDropTime;
use databend_common_meta_app::app_error::UnknownDatabaseId;
use databend_common_meta_app::app_error::UnknownStreamId;
use databend_common_meta_app::app_error::UnknownTable;
use databend_common_meta_app::app_error::UnknownTableId;
use databend_common_meta_app::app_error::ViewAlreadyExists;
use databend_common_meta_app::data_mask::MaskPolicyTableIdListIdent;
use databend_common_meta_app::data_mask::MaskpolicyTableIdList;
use databend_common_meta_app::id_generator::IdGenerator;
use databend_common_meta_app::schema::catalog_id_ident::CatalogId;
use databend_common_meta_app::schema::catalog_name_ident::CatalogNameIdentRaw;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdentRaw;
use databend_common_meta_app::schema::dictionary_id_ident::DictionaryId;
use databend_common_meta_app::schema::dictionary_name_ident::DictionaryNameIdent;
use databend_common_meta_app::schema::index_id_ident::IndexId;
use databend_common_meta_app::schema::index_id_ident::IndexIdIdent;
use databend_common_meta_app::schema::index_id_to_name_ident::IndexIdToNameIdent;
use databend_common_meta_app::schema::index_name_ident::IndexName;
use databend_common_meta_app::schema::CatalogIdToNameIdent;
use databend_common_meta_app::schema::CatalogInfo;
use databend_common_meta_app::schema::CatalogMeta;
use databend_common_meta_app::schema::CatalogNameIdent;
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
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateTableIndexReq;
use databend_common_meta_app::schema::CreateTableReply;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::CreateVirtualColumnReq;
use databend_common_meta_app::schema::DBIdTableName;
use databend_common_meta_app::schema::DatabaseId;
use databend_common_meta_app::schema::DatabaseIdHistoryIdent;
use databend_common_meta_app::schema::DatabaseIdToName;
use databend_common_meta_app::schema::DatabaseInfo;
use databend_common_meta_app::schema::DatabaseInfoFilter;
use databend_common_meta_app::schema::DatabaseMeta;
use databend_common_meta_app::schema::DatabaseType;
use databend_common_meta_app::schema::DbIdList;
use databend_common_meta_app::schema::DeleteLockRevReq;
use databend_common_meta_app::schema::DictionaryIdentity;
use databend_common_meta_app::schema::DictionaryMeta;
use databend_common_meta_app::schema::DropDatabaseReply;
use databend_common_meta_app::schema::DropDatabaseReq;
use databend_common_meta_app::schema::DropTableByIdReq;
use databend_common_meta_app::schema::DropTableIndexReq;
use databend_common_meta_app::schema::DropTableReply;
use databend_common_meta_app::schema::DropVirtualColumnReq;
use databend_common_meta_app::schema::DroppedId;
use databend_common_meta_app::schema::ExtendLockRevReq;
use databend_common_meta_app::schema::GcDroppedTableReq;
use databend_common_meta_app::schema::GetDatabaseReq;
use databend_common_meta_app::schema::GetIndexReply;
use databend_common_meta_app::schema::GetLVTReply;
use databend_common_meta_app::schema::GetLVTReq;
use databend_common_meta_app::schema::GetTableCopiedFileReply;
use databend_common_meta_app::schema::GetTableCopiedFileReq;
use databend_common_meta_app::schema::GetTableReq;
use databend_common_meta_app::schema::IndexMeta;
use databend_common_meta_app::schema::IndexNameIdent;
use databend_common_meta_app::schema::IndexNameIdentRaw;
use databend_common_meta_app::schema::LeastVisibleTime;
use databend_common_meta_app::schema::LeastVisibleTimeKey;
use databend_common_meta_app::schema::ListCatalogReq;
use databend_common_meta_app::schema::ListDatabaseReq;
use databend_common_meta_app::schema::ListDictionaryReq;
use databend_common_meta_app::schema::ListDroppedTableReq;
use databend_common_meta_app::schema::ListDroppedTableResp;
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
use databend_common_meta_app::schema::SetTableColumnMaskPolicyAction;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyReply;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyReq;
use databend_common_meta_app::schema::TableCopiedFileInfo;
use databend_common_meta_app::schema::TableCopiedFileNameIdent;
use databend_common_meta_app::schema::TableId;
use databend_common_meta_app::schema::TableIdHistoryIdent;
use databend_common_meta_app::schema::TableIdList;
use databend_common_meta_app::schema::TableIdToName;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableIndex;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableInfoFilter;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_meta_app::schema::TruncateTableReply;
use databend_common_meta_app::schema::TruncateTableReq;
use databend_common_meta_app::schema::UndropDatabaseReply;
use databend_common_meta_app::schema::UndropDatabaseReq;
use databend_common_meta_app::schema::UndropTableByIdReq;
use databend_common_meta_app::schema::UndropTableReply;
use databend_common_meta_app::schema::UndropTableReq;
use databend_common_meta_app::schema::UpdateDictionaryReply;
use databend_common_meta_app::schema::UpdateDictionaryReq;
use databend_common_meta_app::schema::UpdateMultiTableMetaReq;
use databend_common_meta_app::schema::UpdateMultiTableMetaResult;
use databend_common_meta_app::schema::UpdateStreamMetaReq;
use databend_common_meta_app::schema::UpdateTableMetaReply;
use databend_common_meta_app::schema::UpdateVirtualColumnReq;
use databend_common_meta_app::schema::UpsertTableCopiedFileReq;
use databend_common_meta_app::schema::UpsertTableOptionReply;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_meta_app::schema::VirtualColumnIdent;
use databend_common_meta_app::schema::VirtualColumnMeta;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::tenant_key::errors::UnknownError;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::DirName;
use databend_common_meta_kvapi::kvapi::Key;
use databend_common_meta_types::protobuf as pb;
use databend_common_meta_types::seq_value::SeqV;
use databend_common_meta_types::seq_value::SeqValue;
use databend_common_meta_types::txn_op::Request;
use databend_common_meta_types::txn_op_response::Response;
use databend_common_meta_types::Change;
use databend_common_meta_types::ConditionResult;
use databend_common_meta_types::MatchSeqExt;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::MetaId;
use databend_common_meta_types::TxnCondition;
use databend_common_meta_types::TxnGetRequest;
use databend_common_meta_types::TxnGetResponse;
use databend_common_meta_types::TxnOp;
use databend_common_meta_types::TxnRequest;
use fastrace::func_name;
use futures::TryStreamExt;
use log::debug;
use log::error;
use log::warn;
use ConditionResult::Eq;

use crate::assert_table_exist;
use crate::db_has_to_exist;
use crate::deserialize_struct;
use crate::fetch_id;
use crate::get_pb_value;
use crate::get_u64_value;
use crate::kv_app_error::KVAppError;
use crate::kv_pb_api::KVPbApi;
use crate::list_keys;
use crate::list_u64_value;
use crate::meta_txn_error::MetaTxnError;
use crate::name_id_value_api::NameIdValueApi;
use crate::name_value_api::NameValueApi;
use crate::send_txn;
use crate::serialize_struct;
use crate::serialize_u64;
use crate::txn_backoff::txn_backoff;
use crate::txn_cond_seq;
use crate::txn_op_del;
use crate::txn_op_get;
use crate::txn_op_put;
use crate::util::db_id_has_to_exist;
use crate::util::deserialize_id_get_response;
use crate::util::deserialize_struct_get_response;
use crate::util::get_table_by_id_or_err;
use crate::util::list_tables_from_unshare_db;
use crate::util::mget_pb_values;
use crate::util::unknown_database_error;
use crate::SchemaApi;
use crate::DEFAULT_MGET_SIZE;

const DEFAULT_DATA_RETENTION_SECONDS: i64 = 24 * 60 * 60;
pub const ORPHAN_POSTFIX: &str = "orphan";

/// SchemaApi is implemented upon kvapi::KVApi.
/// Thus every type that impl kvapi::KVApi impls SchemaApi.
#[tonic::async_trait]
impl<KV: kvapi::KVApi<Error = MetaError> + ?Sized> SchemaApi for KV {
    #[logcall::logcall]
    #[fastrace::trace]
    async fn create_database(
        &self,
        req: CreateDatabaseReq,
    ) -> Result<CreateDatabaseReply, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let name_key = &req.name_ident;

        if req.meta.drop_on.is_some() {
            return Err(KVAppError::AppError(AppError::CreateDatabaseWithDropTime(
                CreateDatabaseWithDropTime::new(name_key.database_name()),
            )));
        }

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            // Get db by name to ensure absence
            let curr_seq_db_id = self.get_pb(name_key).await?;
            debug!(curr_seq_db_id :? = curr_seq_db_id, name_key :? =(name_key); "get_database");

            let mut txn = TxnRequest::default();

            if let Some(ref curr_seq_db_id) = curr_seq_db_id {
                match req.create_option {
                    CreateOption::Create => {
                        return Err(KVAppError::AppError(AppError::DatabaseAlreadyExists(
                            DatabaseAlreadyExists::new(
                                name_key.database_name(),
                                format!("create db: tenant: {}", name_key.tenant_name()),
                            ),
                        )));
                    }
                    CreateOption::CreateIfNotExists => {
                        return Ok(CreateDatabaseReply {
                            db_id: curr_seq_db_id.data.into_inner(),
                        });
                    }
                    CreateOption::CreateOrReplace => {
                        let _ = drop_database_meta(self, name_key, false, false, &mut txn).await?;
                    }
                }
            };

            // get db id list from _fd_db_id_list/db_id
            let dbid_idlist =
                DatabaseIdHistoryIdent::new(name_key.tenant(), name_key.database_name());

            let seq_db_id_list = self.get_pb(&dbid_idlist).await?;
            let db_id_list_seq = seq_db_id_list.seq();
            let mut db_id_list = seq_db_id_list.into_value().unwrap_or_else(DbIdList::new);

            // Create db by inserting these record:
            // (tenant, db_name) -> db_id
            // (db_id) -> db_meta
            // append db_id into _fd_db_id_list/<tenant>/<db_name>
            // (db_id) -> (tenant,db_name)

            // if create database from a share then also need to update these record:
            // share_id -> share_meta

            let db_id = fetch_id(self, IdGenerator::database_id()).await?;
            let id_key = DatabaseId { db_id };
            let id_to_name_key = DatabaseIdToName { db_id };

            debug!(db_id = db_id, name_key :? =(name_key); "new database id");

            {
                // append db_id into db_id_list
                db_id_list.append(db_id);

                txn.condition.extend(vec![
                    txn_cond_seq(name_key, Eq, curr_seq_db_id.seq()),
                    txn_cond_seq(&id_to_name_key, Eq, 0),
                    txn_cond_seq(&dbid_idlist, Eq, db_id_list_seq),
                ]);
                txn.if_then.extend(vec![
                    txn_op_put(name_key, serialize_u64(db_id)?), // (tenant, db_name) -> db_id
                    txn_op_put(&id_key, serialize_struct(&req.meta)?), // (db_id) -> db_meta
                    txn_op_put(&dbid_idlist, serialize_struct(&db_id_list)?), /* _fd_db_id_list/<tenant>/<db_name> -> db_id_list */
                    txn_op_put(&id_to_name_key, serialize_struct(&DatabaseNameIdentRaw::from(name_key))?), /* __fd_database_id_to_name/<db_id> -> (tenant,db_name) */
                ]);

                let (succ, _responses) = send_txn(self, txn).await?;

                debug!(
                    name :? =(name_key),
                    id :? =(&id_key),
                    succ = succ;
                    "create_database"
                );

                if succ {
                    return Ok(CreateDatabaseReply { db_id: id_key });
                }
            }
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn drop_database(&self, req: DropDatabaseReq) -> Result<DropDatabaseReply, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let tenant_dbname = &req.name_ident;

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let mut txn = TxnRequest::default();

            let db_id =
                drop_database_meta(self, tenant_dbname, req.if_exists, true, &mut txn).await?;

            let (succ, _responses) = send_txn(self, txn).await?;

            debug!(
                name :? =(tenant_dbname),
                succ = succ;
                "drop_database"
            );

            if succ {
                return Ok(DropDatabaseReply { db_id });
            }
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn undrop_database(
        &self,
        req: UndropDatabaseReq,
    ) -> Result<UndropDatabaseReply, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let name_key = &req.name_ident;

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let res = get_db_or_err(
                self,
                name_key,
                format!("undrop_database: {}", name_key.display()),
            )
            .await;

            if res.is_ok() {
                return Err(KVAppError::AppError(AppError::DatabaseAlreadyExists(
                    DatabaseAlreadyExists::new(
                        name_key.database_name(),
                        format!(
                            "undrop_database: {} has already existed",
                            name_key.database_name()
                        ),
                    ),
                )));
            }

            // get db id list from _fd_db_id_list/<tenant>/<db_name>
            let dbid_idlist =
                DatabaseIdHistoryIdent::new(name_key.tenant(), name_key.database_name());
            let seq_db_id_list = self.get_pb(&dbid_idlist).await?;

            let Some(seq_db_id_list) = seq_db_id_list else {
                return Err(KVAppError::AppError(AppError::UndropDbHasNoHistory(
                    UndropDbHasNoHistory::new(name_key.database_name()),
                )));
            };

            let db_id_list_seq = seq_db_id_list.seq;
            let db_id_list = seq_db_id_list.data;

            // Return error if there is no db id history.
            let db_id = *db_id_list.last().ok_or_else(|| {
                KVAppError::AppError(AppError::UndropDbHasNoHistory(UndropDbHasNoHistory::new(
                    name_key.database_name(),
                )))
            })?;

            // get db_meta of the last db id
            let dbid = DatabaseId { db_id };

            let seq_meta = self.get_pb(&dbid).await?;
            let db_meta_seq = seq_meta.seq();
            let mut db_meta = seq_meta.into_value().unwrap();

            debug!(db_id = db_id, name_key :? =(name_key); "undrop_database");

            {
                // reset drop on time
                // undrop a table with no drop time
                if db_meta.drop_on.is_none() {
                    return Err(KVAppError::AppError(AppError::UndropDbWithNoDropTime(
                        UndropDbWithNoDropTime::new(name_key.database_name()),
                    )));
                }
                db_meta.drop_on = None;

                let txn_req = TxnRequest {
                    condition: vec![
                        txn_cond_seq(name_key, Eq, 0),
                        txn_cond_seq(&dbid_idlist, Eq, db_id_list_seq),
                        txn_cond_seq(&dbid, Eq, db_meta_seq),
                    ],
                    if_then: vec![
                        txn_op_put(name_key, serialize_u64(db_id)?), // (tenant, db_name) -> db_id
                        txn_op_put(&dbid, serialize_struct(&db_meta)?), // (db_id) -> db_meta
                    ],
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                debug!(
                    name_key :? =(name_key),
                    succ = succ;
                    "undrop_database"
                );

                if succ {
                    return Ok(UndropDatabaseReply {});
                }
            }
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn rename_database(
        &self,
        req: RenameDatabaseReq,
    ) -> Result<RenameDatabaseReply, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let tenant_dbname = &req.name_ident;
        let tenant_newdbname = DatabaseNameIdent::new(tenant_dbname.tenant(), &req.new_db_name);

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            // get old db, not exists return err
            let old_seq_db_id = self.get_pb(tenant_dbname).await?;

            let Some(old_seq_db_id) = old_seq_db_id else {
                if req.if_exists {
                    return Ok(RenameDatabaseReply {});
                } else {
                    db_has_to_exist(
                        old_seq_db_id.seq(),
                        tenant_dbname,
                        "rename_database: src (db)",
                    )?;
                    unreachable!("rename_database: src (db) should exist")
                }
            };

            let old_db_id = old_seq_db_id.data.into_inner();

            let old_seq_db_meta = self.get_pb(&old_db_id).await?;

            db_has_to_exist(
                old_seq_db_meta.seq(),
                tenant_dbname,
                "rename_database: src (db)",
            )?;

            debug!(
                old_db_id :? = old_db_id,
                tenant_dbname :? =(tenant_dbname);
                "rename_database"
            );

            // get new db, exists return err
            let new_seq_db_id = self.get_pb(&tenant_newdbname).await?;
            db_has_to_not_exist(new_seq_db_id.seq(), &tenant_newdbname, "rename_database")?;

            // get db id -> name
            let db_id_key = DatabaseIdToName { db_id: *old_db_id };
            let seq_db_name = self.get_pb(&db_id_key).await?;
            let db_name_seq = seq_db_name.seq();

            // get db id list from _fd_db_id_list/<tenant>/<db_name>
            let dbid_idlist =
                DatabaseIdHistoryIdent::new(tenant_dbname.tenant(), tenant_dbname.database_name());

            let seq_db_id_list = self.get_pb(&dbid_idlist).await?;
            let db_id_list_seq = seq_db_id_list.seq();

            // may the database is created before add db_id_list, so we just add the id into the list.
            let mut db_id_list = seq_db_id_list.into_value().unwrap_or_else(|| {
                let mut l = DbIdList::new();
                l.append(*old_db_id);
                l
            });

            if let Some(last_db_id) = db_id_list.last() {
                if *last_db_id != *old_db_id {
                    return Err(KVAppError::AppError(AppError::DatabaseAlreadyExists(
                        DatabaseAlreadyExists::new(
                            tenant_dbname.database_name(),
                            format!(
                                "rename_database: {} with a wrong db id",
                                tenant_dbname.display()
                            ),
                        ),
                    )));
                }
            } else {
                return Err(KVAppError::AppError(AppError::DatabaseAlreadyExists(
                    DatabaseAlreadyExists::new(
                        tenant_dbname.database_name(),
                        format!(
                            "rename_database: {} with none db id history",
                            tenant_dbname.display()
                        ),
                    ),
                )));
            }

            let new_dbid_idlist =
                DatabaseIdHistoryIdent::new(tenant_dbname.tenant(), &req.new_db_name);

            let seq_idlist = self.get_pb(&new_dbid_idlist).await?;

            let new_db_id_list_seq = seq_idlist.seq();
            let mut new_db_id_list = seq_idlist.into_value().unwrap_or_else(DbIdList::new);

            // rename database
            // move db id from old db id list to new db id list
            db_id_list.pop();
            new_db_id_list.append(*old_db_id);

            let condition = vec![
                // Prevent renaming or deleting in other threads.
                txn_cond_seq(tenant_dbname, Eq, old_seq_db_id.seq),
                txn_cond_seq(&db_id_key, Eq, db_name_seq),
                txn_cond_seq(&tenant_newdbname, Eq, 0),
                txn_cond_seq(&dbid_idlist, Eq, db_id_list_seq),
                txn_cond_seq(&new_dbid_idlist, Eq, new_db_id_list_seq),
            ];
            let if_then = vec![
                txn_op_del(tenant_dbname), // del old_db_name
                // Renaming db should not affect the seq of db_meta. Just modify db name.
                txn_op_put(&tenant_newdbname, serialize_u64(*old_db_id)?), /* (tenant, new_db_name) -> old_db_id */
                txn_op_put(&new_dbid_idlist, serialize_struct(&new_db_id_list)?), /* _fd_db_id_list/tenant/new_db_name -> new_db_id_list */
                txn_op_put(&dbid_idlist, serialize_struct(&db_id_list)?), /* _fd_db_id_list/tenant/db_name -> db_id_list */
                txn_op_put(
                    &db_id_key,
                    serialize_struct(&DatabaseNameIdentRaw::from(&tenant_newdbname))?,
                ), /* __fd_database_id_to_name/<db_id> -> (tenant,db_name) */
            ];

            let txn_req = TxnRequest {
                condition,
                if_then,
                else_then: vec![],
            };

            let (succ, _responses) = send_txn(self, txn_req).await?;

            debug!(
                name :? =(tenant_dbname),
                to :? =(&tenant_newdbname),
                database_id :? =(&old_db_id),
                succ = succ;
                "rename_database"
            );

            if succ {
                return Ok(RenameDatabaseReply {});
            }
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_database(&self, req: GetDatabaseReq) -> Result<Arc<DatabaseInfo>, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let name_key = &req.inner;

        let (seq_db_id, db_meta) = get_db_or_err(self, name_key, "get_database").await?;

        let db = DatabaseInfo {
            database_id: seq_db_id.data,
            name_ident: name_key.clone(),
            meta: db_meta,
        };

        Ok(Arc::new(db))
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_database_history(
        &self,
        req: ListDatabaseReq,
    ) -> Result<Vec<Arc<DatabaseInfo>>, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        // List tables by tenant, db_id, table_name.
        let dbid_tbname_idlist = DatabaseIdHistoryIdent::new(&req.tenant, "dummy");
        let dir_name = DirName::new(dbid_tbname_idlist);
        let db_id_list_keys = list_keys(self, &dir_name).await?;

        let mut db_info_list = vec![];
        let now = Utc::now();
        let keys: Vec<String> = db_id_list_keys
            .iter()
            .map(|db_id_list_key| db_id_list_key.to_string_key())
            .collect();
        let mut db_id_list_keys_iter = db_id_list_keys.into_iter();
        let include_drop_db = matches!(&req.filter, Some(DatabaseInfoFilter::IncludeDropped));
        for c in keys.chunks(DEFAULT_MGET_SIZE) {
            let db_id_list_seq_and_list: Vec<(u64, Option<DbIdList>)> =
                mget_pb_values(self, c).await?;

            for (db_id_list_seq, db_id_list_opt) in db_id_list_seq_and_list {
                let db_id_list_key = db_id_list_keys_iter.next().unwrap();
                let db_id_list = if db_id_list_seq == 0 {
                    continue;
                } else {
                    match db_id_list_opt {
                        Some(list) => list,
                        None => {
                            continue;
                        }
                    }
                };

                let inner_keys: Vec<String> = db_id_list
                    .id_list
                    .iter()
                    .map(|db_id| DatabaseId { db_id: *db_id }.to_string_key())
                    .collect();
                let mut db_id_list_iter = db_id_list.id_list.into_iter();
                for c in inner_keys.chunks(DEFAULT_MGET_SIZE) {
                    let db_meta_seq_meta_vec: Vec<(u64, Option<DatabaseMeta>)> =
                        mget_pb_values(self, c).await?;

                    for (db_meta_seq, db_meta) in db_meta_seq_meta_vec {
                        let db_id = db_id_list_iter.next().unwrap();
                        if db_meta_seq == 0 || db_meta.is_none() {
                            error!("get_database_history cannot find {:?} db_meta", db_id);
                            continue;
                        }
                        let db_meta = db_meta.unwrap();
                        // if include drop db, then no need to fill out of retention time db
                        if !include_drop_db
                            && is_drop_time_out_of_retention_time(&db_meta.drop_on, &now)
                        {
                            continue;
                        }

                        let db = DatabaseInfo {
                            database_id: DatabaseId::new(db_id),
                            name_ident: DatabaseNameIdent::new_from(db_id_list_key.clone()),
                            meta: SeqV::new(db_meta_seq, db_meta),
                        };

                        db_info_list.push(Arc::new(db));
                    }
                }
            }
        }

        // `list_database` can list db which has no `DbIdListKey`
        if include_drop_db {
            // if `include_drop_db` is true, return all db info which not exist in db_info_list
            let db_id_set: HashSet<u64> = db_info_list
                .iter()
                .map(|db_info| db_info.database_id.db_id)
                .collect();

            let all_dbs = self.list_databases(req).await?;
            for db_info in all_dbs {
                if !db_id_set.contains(&db_info.database_id.db_id) {
                    warn!(
                        "get db history db:{:?}, db_id:{:?} has no DbIdListKey",
                        db_info.name_ident, db_info.database_id.db_id
                    );
                    db_info_list.push(db_info);
                }
            }
        } else {
            // if `include_drop_db` is false, filter out db which drop_on time out of retention time
            let db_id_set: HashSet<u64> = db_info_list
                .iter()
                .map(|db_info| db_info.database_id.db_id)
                .collect();

            let all_dbs = self.list_databases(req).await?;
            let mut add_dbinfo_map = HashMap::new();
            let mut db_id_list = Vec::new();
            for db_info in all_dbs {
                if !db_id_set.contains(&db_info.database_id.db_id) {
                    warn!(
                        "get db history db:{:?}, db_id:{:?} has no DbIdListKey",
                        db_info.name_ident, db_info.database_id.db_id
                    );
                    db_id_list.push(DatabaseId {
                        db_id: db_info.database_id.db_id,
                    });
                    add_dbinfo_map.insert(db_info.database_id.db_id, db_info);
                }
            }
            let inner_keys: Vec<String> = db_id_list
                .iter()
                .map(|db_id| db_id.to_string_key())
                .collect();
            let mut db_id_list_iter = db_id_list.into_iter();
            for c in inner_keys.chunks(DEFAULT_MGET_SIZE) {
                let db_meta_seq_meta_vec: Vec<(u64, Option<DatabaseMeta>)> =
                    mget_pb_values(self, c).await?;

                for (db_meta_seq, db_meta) in db_meta_seq_meta_vec {
                    let db_id = db_id_list_iter.next().unwrap().db_id;
                    if db_meta_seq == 0 || db_meta.is_none() {
                        error!("get_database_history cannot find {:?} db_meta", db_id);
                        continue;
                    }
                    let db_meta = db_meta.unwrap();
                    // if include drop db, then no need to fill out of retention time db
                    if is_drop_time_out_of_retention_time(&db_meta.drop_on, &now) {
                        continue;
                    }
                    if let Some(db_info) = add_dbinfo_map.get(&db_id) {
                        warn!(
                            "get db history db:{:?}, db_id:{:?} has no DbIdListKey",
                            db_info.name_ident, db_info.database_id.db_id
                        );
                        db_info_list.push(db_info.clone());
                    }
                }
            }
        }

        return Ok(db_info_list);
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_databases(
        &self,
        req: ListDatabaseReq,
    ) -> Result<Vec<Arc<DatabaseInfo>>, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        // Using a empty db to to list all
        let name_key = DatabaseNameIdent::new(req.tenant(), "");

        // Pairs of db-name and db_id with seq
        let (tenant_dbnames, db_ids) = list_u64_value(self, &name_key).await?;

        // Keys for fetching serialized DatabaseMeta from kvapi::KVApi
        let mut kv_keys = Vec::with_capacity(db_ids.len());

        for db_id in db_ids.iter() {
            let k = DatabaseId { db_id: *db_id }.to_string_key();
            kv_keys.push(k);
        }

        // Batch get all db-metas.
        // - A db-meta may be already deleted. It is Ok. Just ignore it.

        let seq_metas = self.mget_kv(&kv_keys).await?;
        let mut db_infos = Vec::with_capacity(kv_keys.len());

        for (i, seq_meta_opt) in seq_metas.iter().enumerate() {
            if let Some(seq_meta) = seq_meta_opt {
                let db_meta: DatabaseMeta = deserialize_struct(&seq_meta.data)?;

                let db_info = DatabaseInfo {
                    database_id: DatabaseId::new(db_ids[i]),
                    name_ident: DatabaseNameIdent::new(
                        name_key.tenant(),
                        tenant_dbnames[i].database_name(),
                    ),
                    meta: SeqV::new(seq_meta.seq, db_meta),
                };
                db_infos.push(Arc::new(db_info));
            } else {
                debug!(
                    k = &kv_keys[i];
                    "db_meta not found, maybe just deleted after listing names and before listing meta"
                );
            }
        }

        Ok(db_infos)
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn create_index(&self, req: CreateIndexReq) -> Result<CreateIndexReply, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let name_ident = &req.name_ident;
        let meta = &req.meta;
        let overriding = req.create_option.is_overriding();
        let name_ident_raw = serialize_struct(&IndexNameIdentRaw::from(name_ident))?;

        let create_res = self
            .create_id_value(name_ident, meta, overriding, |id| {
                vec![(
                    IndexIdToNameIdent::new_generic(name_ident.tenant(), id).to_string_key(),
                    name_ident_raw.clone(),
                )]
            })
            .await?;

        match create_res {
            Ok(id) => Ok(CreateIndexReply { index_id: *id }),
            Err(existent) => match req.create_option {
                CreateOption::Create => {
                    Err(AppError::from(name_ident.exist_error(func_name!())).into())
                }
                CreateOption::CreateIfNotExists => Ok(CreateIndexReply {
                    index_id: *existent.data,
                }),
                CreateOption::CreateOrReplace => {
                    unreachable!(
                        "create_index: CreateOrReplace should never conflict with existent"
                    );
                }
            },
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn drop_index(
        &self,
        name_ident: &IndexNameIdent,
    ) -> Result<Option<(SeqV<IndexId>, SeqV<IndexMeta>)>, MetaTxnError> {
        let dropped = self
            .remove_id_value(name_ident, |id| {
                vec![IndexIdToNameIdent::new_generic(name_ident.tenant(), id).to_string_key()]
            })
            .await?;
        Ok(dropped)
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_index(
        &self,
        name_ident: &IndexNameIdent,
    ) -> Result<Option<GetIndexReply>, MetaError> {
        debug!(req :? =name_ident; "SchemaApi: {}", func_name!());

        let res = self.get_id_value(name_ident).await?;

        let Some((seq_id, seq_meta)) = res else {
            return Ok(None);
        };

        Ok(Some(GetIndexReply {
            index_id: *seq_id.data,
            index_meta: seq_meta.data,
        }))
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn update_index(
        &self,
        id_ident: IndexIdIdent,
        index_meta: IndexMeta,
    ) -> Result<Change<IndexMeta>, MetaError> {
        debug!(id_ident :? =(&id_ident); "SchemaApi: {}", func_name!());
        NameIdValueApi::<IndexNameIdent, _>::update_by_id(self, id_ident, index_meta).await
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_indexes(
        &self,
        req: ListIndexesReq,
    ) -> Result<Vec<(String, IndexId, IndexMeta)>, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        // Get index id list by `prefix_list` "<prefix>/<tenant>"
        let ident = IndexNameIdent::new(&req.tenant, "dummy");
        let dir = DirName::new(ident);

        let name_id_metas = self.list_id_value(&dir).await?;

        let index_metas = name_id_metas
            // table_id is not specified
            // or table_id is specified and equals to the given table_id.
            .filter(|(_k, _id, seq_meta)| {
                req.table_id.is_none() || req.table_id == Some(seq_meta.table_id)
            })
            .map(|(k, id, seq_meta)| (k.index_name().to_string(), id, seq_meta.data))
            .collect::<Vec<_>>();

        Ok(index_metas)
    }

    // virtual column

    #[logcall::logcall]
    #[fastrace::trace]
    async fn create_virtual_column(&self, req: CreateVirtualColumnReq) -> Result<(), KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let virtual_column_meta = VirtualColumnMeta {
            table_id: req.name_ident.table_id(),
            virtual_columns: req.virtual_columns.clone(),
            created_on: Utc::now(),
            updated_on: None,
        };

        self.create_name_value_with_create_option(
            req.name_ident.clone(),
            virtual_column_meta,
            req.create_option,
        )
        .await?
        .map_err(AppError::from)?;

        Ok(())
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn update_virtual_column(&self, req: UpdateVirtualColumnReq) -> Result<(), KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let not_found = || {
            if req.if_exists {
                Ok(())
            } else {
                Err(AppError::from(req.name_ident.unknown_error(func_name!())))
            }
        };

        self.update_existent_name_value(
            &req.name_ident,
            |mut meta| {
                meta.virtual_columns = req.virtual_columns.clone();
                meta.updated_on = Some(Utc::now());
                Some((meta, None))
            },
            not_found,
        )
        .await??;
        Ok(())
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn drop_virtual_column(&self, req: DropVirtualColumnReq) -> Result<(), KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let not_found = || {
            if req.if_exists {
                Ok(())
            } else {
                Err(AppError::from(req.name_ident.unknown_error(func_name!())))
            }
        };

        self.remove_name_value(&req.name_ident, not_found).await??;

        Ok(())
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_virtual_columns(
        &self,
        req: ListVirtualColumnsReq,
    ) -> Result<Vec<VirtualColumnMeta>, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        if let Some(table_id) = req.table_id {
            let name_ident = VirtualColumnIdent::new(&req.tenant, table_id);

            let seq_meta = self.get_pb(&name_ident).await?;
            let x = seq_meta
                .map(|seq_v| seq_v.data)
                .into_iter()
                .collect::<Vec<_>>();

            return Ok(x);
        }

        // Get virtual columns list by `prefix_list` "<prefix>/<tenant>"
        let ident = VirtualColumnIdent::new(&req.tenant, 0u64);
        let dir = DirName::new(ident);

        let strm = self.list_pb_values(&dir).await?;
        let vs = strm.try_collect::<Vec<_>>().await?;

        Ok(vs)
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn create_table(&self, req: CreateTableReq) -> Result<CreateTableReply, KVAppError> {
        // Make an error if table exists.
        fn make_exists_err(req: &CreateTableReq) -> AppError {
            let name = &req.name_ident.table_name;
            let name_ident = &req.name_ident;

            match req.table_meta.engine.as_str() {
                "STREAM" => {
                    let exist_err =
                        StreamAlreadyExists::new(name, format!("create_table: {}", name_ident));
                    AppError::from(exist_err)
                }
                "VIEW" => {
                    let exist_err =
                        ViewAlreadyExists::new(name, format!("create_table: {}", name_ident));
                    AppError::from(exist_err)
                }
                _ => {
                    let exist_err =
                        TableAlreadyExists::new(name, format!("create_table: {}", name_ident));
                    AppError::from(exist_err)
                }
            }
        }

        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let tenant_dbname_tbname = &req.name_ident;
        let tenant_dbname = req.name_ident.db_name_ident();

        let mut maybe_key_table_id: Option<TableId> = None;

        if !req.as_dropped && req.table_meta.drop_on.is_some() {
            return Err(KVAppError::AppError(AppError::CreateTableWithDropTime(
                CreateTableWithDropTime::new(&tenant_dbname_tbname.table_name),
            )));
        }

        if req.as_dropped && req.table_meta.drop_on.is_none() {
            return Err(KVAppError::AppError(
                AppError::CreateAsDropTableWithoutDropTime(CreateAsDropTableWithoutDropTime::new(
                    &tenant_dbname_tbname.table_name,
                )),
            ));
        }

        // fixed: does not change in every loop.
        let seq_db_id = get_db_id_or_err(self, &tenant_dbname, "create_table").await?;

        // fixed
        let key_dbid = seq_db_id.data;

        // fixed
        let key_dbid_tbname = DBIdTableName {
            db_id: *seq_db_id.data,
            table_name: req.name_ident.table_name.clone(),
        };

        // fixed
        let key_table_id_list = TableIdHistoryIdent {
            database_id: *seq_db_id.data,
            table_name: req.name_ident.table_name.clone(),
        };
        // if req.as_dropped, append new table id to orphan table id list
        let (orphan_table_name, save_key_table_id_list) = if req.as_dropped {
            let now = Utc::now().timestamp_micros();
            let orphan_table_name = format!("{}@{}", ORPHAN_POSTFIX, now);
            (Some(orphan_table_name.clone()), TableIdHistoryIdent {
                database_id: *seq_db_id.data,
                table_name: orphan_table_name,
            })
        } else {
            (None, key_table_id_list.clone())
        };

        // The keys of values to re-fetch for every retry in this txn.
        let keys = vec![
            key_dbid.to_string_key(),
            key_dbid_tbname.to_string_key(),
            key_table_id_list.to_string_key(),
        ];

        // Initialize required key-values
        let mut data = {
            let values = self.mget_kv(&keys).await?;
            keys.iter()
                .zip(values.into_iter())
                .map(|(k, v)| TxnGetResponse::new(k, v.map(pb::SeqV::from)))
                .collect::<Vec<_>>()
        };

        if !req.table_meta.indexes.is_empty() {
            // check the index column id exists and not be duplicated.
            let mut index_column_ids = HashSet::new();
            for (_, index) in req.table_meta.indexes.iter() {
                for column_id in &index.column_ids {
                    if req.table_meta.schema.is_column_deleted(*column_id) {
                        return Err(KVAppError::AppError(AppError::IndexColumnIdNotFound(
                            IndexColumnIdNotFound::new(*column_id, &index.name),
                        )));
                    }
                    if index_column_ids.contains(column_id) {
                        return Err(KVAppError::AppError(AppError::DuplicatedIndexColumnId(
                            DuplicatedIndexColumnId::new(*column_id, &index.name),
                        )));
                    }
                    index_column_ids.insert(column_id);
                }
            }
        }

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            // When retrying, data is cleared and re-fetched in one shot.
            if data.is_empty() {
                data = {
                    let values = self.mget_kv(&keys).await?;
                    keys.iter()
                        .zip(values.into_iter())
                        .map(|(k, v)| TxnGetResponse::new(k, v.map(pb::SeqV::from)))
                        .collect::<Vec<_>>()
                };
            }

            let db_meta = {
                let d = data.remove(0);
                let (k, v) = deserialize_struct_get_response::<DatabaseId>(d)?;
                assert_eq!(key_dbid, k);

                v.ok_or_else(|| {
                    AppError::UnknownDatabaseId(UnknownDatabaseId::new(
                        *seq_db_id.data,
                        format!("{}: {}", func_name!(), seq_db_id.data),
                    ))
                })?
            };

            let mut txn = TxnRequest::default();

            let seq_table_id = {
                let d = data.remove(0);
                let (k, v) = deserialize_id_get_response::<DBIdTableName>(d)?;
                assert_eq!(key_dbid_tbname, k);

                if let Some(id) = v {
                    // TODO: move if_not_exists to upper caller. It is not duty of SchemaApi.
                    match req.create_option {
                        CreateOption::Create => {
                            let app_err = make_exists_err(&req);
                            return Err(KVAppError::AppError(app_err));
                        }
                        CreateOption::CreateIfNotExists => {
                            return Ok(CreateTableReply {
                                table_id: *id.data,
                                table_id_seq: None,
                                db_id: *seq_db_id.data,
                                new_table: false,
                                spec_vec: None,
                                prev_table_id: None,
                                orphan_table_name: None,
                            });
                        }
                        CreateOption::CreateOrReplace => {
                            if req.as_dropped {
                                // If the table is being created as a dropped table, we do not
                                // need to combine with drop_table_txn operations, just return
                                // the sequence number associated with the value part of
                                // the key-value pair (key_dbid_tbname, table_id).

                                SeqV::new(id.seq, *id.data)
                            } else {
                                let (seq, id) = construct_drop_table_txn_operations(
                                    self,
                                    req.name_ident.table_name.clone(),
                                    &req.name_ident.tenant,
                                    *id.data,
                                    *seq_db_id.data,
                                    false,
                                    false,
                                    &mut txn,
                                )
                                .await?;
                                SeqV::new(seq, id)
                            }
                        }
                    }
                } else {
                    SeqV::new(0, 0)
                }
            };

            let (mut tb_id_list, prev_table_id, tb_id_list_seq) = {
                let d = data.remove(0);
                let (k, v) = deserialize_struct_get_response::<TableIdHistoryIdent>(d)?;
                assert_eq!(key_table_id_list, k);

                let tb_id_list = v.unwrap_or_default();

                // if `as_dropped` is true, append new table id into a temp new table
                // if create table return success, table id will be moved to table id list,
                // else, it will be vacuum when `vacuum drop table`
                if req.as_dropped {
                    (
                        // a new TableIdList
                        TableIdList::new(),
                        // save last table id and check when commit table meta
                        tb_id_list.data.id_list.last().copied(),
                        // seq MUST be 0
                        0,
                    )
                } else {
                    (tb_id_list.data, None, tb_id_list.seq)
                }
            };

            // Table id is unique and does not need to re-generate in every loop.
            let key_table_id = match &maybe_key_table_id {
                None => {
                    let id = fetch_id(self, IdGenerator::table_id()).await?;
                    maybe_key_table_id = Some(TableId { table_id: id });
                    maybe_key_table_id.as_ref().unwrap()
                }
                Some(id) => id,
            };

            let table_id = key_table_id.table_id;

            let key_table_id_to_name = TableIdToName { table_id };

            debug!(
                key_table_id :? =(key_table_id),
                name :? =(tenant_dbname_tbname);
                "new table id"
            );

            {
                // append new table_id into list
                tb_id_list.append(table_id);
                let dbid_tbname_seq = seq_table_id.seq;

                txn.condition.extend(vec![
                    // db has not to change, i.e., no new table is created.
                    // Renaming db is OK and does not affect the seq of db_meta.
                    txn_cond_seq(&key_dbid, Eq, db_meta.seq),
                    // no other table with the same name is inserted.
                    txn_cond_seq(&key_dbid_tbname, Eq, dbid_tbname_seq),
                    // no other table id with the same name is append.
                    txn_cond_seq(&save_key_table_id_list, Eq, tb_id_list_seq),
                ]);

                txn.if_then.extend( vec![
                        // Changing a table in a db has to update the seq of db_meta,
                        // to block the batch-delete-tables when deleting a db.
                        txn_op_put(&key_dbid, serialize_struct(&db_meta.data)?), /* (db_id) -> db_meta */
                        txn_op_put(
                            key_table_id,
                            serialize_struct(&req.table_meta)?,
                        ), /* (tenant, db_id, tb_id) -> tb_meta */
                        txn_op_put(&save_key_table_id_list, serialize_struct(&tb_id_list)?), /* _fd_table_id_list/db_id/table_name -> tb_id_list */
                        // This record does not need to assert `table_id_to_name_key == 0`,
                        // Because this is a reverse index for db_id/table_name -> table_id, and it is unique.
                        txn_op_put(&key_table_id_to_name, serialize_struct(&key_dbid_tbname)?), /* __fd_table_id_to_name/db_id/table_name -> DBIdTableName */
                    ]);

                if req.as_dropped {
                    // To create the table in a "dropped" state,
                    // - we intentionally omit the tuple (key_dbid_name, table_id).
                    //   This ensures the table remains invisible, and available to be vacuumed.
                    // - also, the `table_id_seq` of newly create table should be obtained.
                    //   The caller need to know the `table_id_seq` to manipulate the table more efficiently
                    //   This TxnOp::Get is(should be) the last operation in the `if_then` list.
                    txn.if_then.push(txn_op_get(key_table_id));
                } else {
                    // Otherwise, make newly created table visible by putting the tuple:
                    // (tenant, db_id, tb_name) -> tb_id
                    txn.if_then
                        .push(txn_op_put(&key_dbid_tbname, serialize_u64(table_id)?))
                }

                let (succ, responses) = send_txn(self, txn).await?;

                debug!(
                    name :? =(tenant_dbname_tbname),
                    key_table_id :? =(key_table_id),
                    succ = succ,
                    responses :? =(responses);
                    "create_table"
                );

                // extract the table_id_seq (if any) from the kv txn responses
                let table_id_seq = if req.as_dropped {
                    responses.last().and_then(|r| match &r.response {
                        Some(Response::Get(resp)) => resp.value.as_ref().map(|v| v.seq),
                        _ => None,
                    })
                } else {
                    None
                };

                if succ {
                    return Ok(CreateTableReply {
                        table_id,
                        table_id_seq,
                        db_id: *seq_db_id.data,
                        new_table: dbid_tbname_seq == 0,
                        spec_vec: None,
                        prev_table_id,
                        orphan_table_name,
                    });
                } else {
                    // re-run txn with re-fetched data
                    data = vec![];
                }
            }
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn undrop_table(&self, req: UndropTableReq) -> Result<UndropTableReply, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());
        handle_undrop_table(self, req).await
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn undrop_table_by_id(
        &self,
        req: UndropTableByIdReq,
    ) -> Result<UndropTableReply, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());
        handle_undrop_table(self, req).await
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn rename_table(&self, req: RenameTableReq) -> Result<RenameTableReply, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let tenant_dbname_tbname = &req.name_ident;
        let tenant_dbname = tenant_dbname_tbname.db_name_ident();
        let tenant_newdbname_newtbname = TableNameIdent {
            tenant: tenant_dbname_tbname.tenant.clone(),
            db_name: req.new_db_name.clone(),
            table_name: req.new_table_name.clone(),
        };

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            // Get db by name to ensure presence

            let (seq_db_id, db_meta) = get_db_or_err(self, &tenant_dbname, "rename_table").await?;

            // Get table by db_id, table_name to assert presence.

            let dbid_tbname = DBIdTableName {
                db_id: *seq_db_id.data,
                table_name: tenant_dbname_tbname.table_name.clone(),
            };

            let (tb_id_seq, table_id) = get_u64_value(self, &dbid_tbname).await?;
            if req.if_exists {
                if tb_id_seq == 0 {
                    // TODO: table does not exist, can not return table id.
                    return Ok(RenameTableReply { table_id: 0 });
                }
            } else {
                assert_table_exist(
                    tb_id_seq,
                    tenant_dbname_tbname,
                    "rename_table: src (db,table)",
                )?;
            }

            // get table id list from _fd_table_id_list/db_id/table_name
            let dbid_tbname_idlist = TableIdHistoryIdent {
                database_id: *seq_db_id.data,
                table_name: req.name_ident.table_name.clone(),
            };
            let (tb_id_list_seq, tb_id_list_opt): (_, Option<TableIdList>) =
                get_pb_value(self, &dbid_tbname_idlist).await?;

            let mut tb_id_list: TableIdList;
            if tb_id_list_seq == 0 {
                // may the table is created before add db_id_list, so we just add the id into the list.
                tb_id_list = TableIdList::new();
                tb_id_list.append(table_id);
            } else {
                match tb_id_list_opt {
                    Some(list) => tb_id_list = list,
                    None => {
                        // may the table is created before add db_id_list, so we just add the id into the list.
                        tb_id_list = TableIdList::new();
                        tb_id_list.append(table_id);
                    }
                }
            };

            if let Some(last_table_id) = tb_id_list.last() {
                if *last_table_id != table_id {
                    error!(
                        "rename_table {:?} but last table id confilct, id list last: {}, current: {}",
                        req.name_ident, last_table_id, table_id
                    );
                    return Err(KVAppError::AppError(AppError::UnknownTable(
                        UnknownTable::new(
                            &req.name_ident.table_name,
                            format!("{}: {}", "rename table", tenant_dbname_tbname),
                        ),
                    )));
                }
            } else {
                return Err(KVAppError::AppError(AppError::UnknownTable(
                    UnknownTable::new(
                        &req.name_ident.table_name,
                        format!("{}: {}", "rename table", tenant_dbname_tbname),
                    ),
                )));
            }

            // Get the renaming target db to ensure presence.

            let tenant_newdbname = DatabaseNameIdent::new(tenant_dbname.tenant(), &req.new_db_name);
            let (new_seq_db_id, new_db_meta) =
                get_db_or_err(self, &tenant_newdbname, "rename_table: new db").await?;

            // Get the renaming target table to ensure absence

            let newdbid_newtbname = DBIdTableName {
                db_id: *new_seq_db_id.data,
                table_name: req.new_table_name.clone(),
            };
            let (new_tb_id_seq, _new_tb_id) = get_u64_value(self, &newdbid_newtbname).await?;
            table_has_to_not_exist(new_tb_id_seq, &tenant_newdbname_newtbname, "rename_table")?;

            let new_dbid_tbname_idlist = TableIdHistoryIdent {
                database_id: *new_seq_db_id.data,
                table_name: req.new_table_name.clone(),
            };

            let seq_list = self.get_pb(&new_dbid_tbname_idlist).await?;

            let new_tb_id_list_seq = seq_list.seq();

            let mut new_tb_id_list = if let Some(seq_list) = seq_list {
                seq_list.data
            } else {
                TableIdList::new()
            };

            // get table id name
            let table_id_to_name_key = TableIdToName { table_id };
            let (table_id_to_name_seq, _): (_, Option<DBIdTableName>) =
                get_pb_value(self, &table_id_to_name_key).await?;
            let db_id_table_name = DBIdTableName {
                db_id: *new_seq_db_id.data,
                table_name: req.new_table_name.clone(),
            };

            {
                // move table id from old table id list to new table id list
                tb_id_list.pop();
                new_tb_id_list.append(table_id);

                let mut txn = TxnRequest {
                    condition: vec![
                        // db has not to change, i.e., no new table is created.
                        // Renaming db is OK and does not affect the seq of db_meta.
                        txn_cond_seq(&seq_db_id.data, Eq, db_meta.seq),
                        txn_cond_seq(&new_seq_db_id.data, Eq, new_db_meta.seq),
                        // table_name->table_id does not change.
                        // Updating the table meta is ok.
                        txn_cond_seq(&dbid_tbname, Eq, tb_id_seq),
                        txn_cond_seq(&newdbid_newtbname, Eq, 0),
                        // no other table id with the same name is append.
                        txn_cond_seq(&dbid_tbname_idlist, Eq, tb_id_list_seq),
                        txn_cond_seq(&new_dbid_tbname_idlist, Eq, new_tb_id_list_seq),
                        txn_cond_seq(&table_id_to_name_key, Eq, table_id_to_name_seq),
                    ],
                    if_then: vec![
                        txn_op_del(&dbid_tbname), // (db_id, tb_name) -> tb_id
                        txn_op_put(&newdbid_newtbname, serialize_u64(table_id)?), /* (db_id, new_tb_name) -> tb_id */
                        // Changing a table in a db has to update the seq of db_meta,
                        // to block the batch-delete-tables when deleting a db.
                        txn_op_put(&seq_db_id.data, serialize_struct(&*db_meta)?), /* (db_id) -> db_meta */
                        txn_op_put(&dbid_tbname_idlist, serialize_struct(&tb_id_list)?), /* _fd_table_id_list/db_id/old_table_name -> tb_id_list */
                        txn_op_put(&new_dbid_tbname_idlist, serialize_struct(&new_tb_id_list)?), /* _fd_table_id_list/db_id/new_table_name -> tb_id_list */
                        txn_op_put(&table_id_to_name_key, serialize_struct(&db_id_table_name)?), /* __fd_table_id_to_name/db_id/table_name -> DBIdTableName */
                    ],
                    else_then: vec![],
                };

                if *seq_db_id.data != *new_seq_db_id.data {
                    txn.if_then.push(
                        txn_op_put(
                            &new_seq_db_id.data,
                            serialize_struct(&*new_db_meta)?,
                        ), // (db_id) -> db_meta
                    );
                }

                let (succ, _responses) = send_txn(self, txn).await?;

                debug!(
                    name :? =(tenant_dbname_tbname),
                    to :? =(&newdbid_newtbname),
                    table_id :? =(&table_id),
                    succ = succ;
                    "rename_table"
                );

                if succ {
                    return Ok(RenameTableReply { table_id });
                }
            }
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_table(&self, req: GetTableReq) -> Result<Arc<TableInfo>, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let tenant_dbname_tbname = &req.inner;
        let tenant_dbname = tenant_dbname_tbname.db_name_ident();

        // Get db by name to ensure presence

        let res = get_db_or_err(
            self,
            &tenant_dbname,
            format!("get_table: {}", tenant_dbname.display()),
        )
        .await;

        let (seq_db_id, _db_meta) = match res {
            Ok(x) => x,
            Err(e) => {
                return Err(e);
            }
        };

        let table_id = {
            // Get table by tenant,db_id, table_name to assert presence.

            let dbid_tbname = DBIdTableName {
                db_id: *seq_db_id.data,
                table_name: tenant_dbname_tbname.table_name.clone(),
            };

            let (tb_id_seq, table_id) = get_u64_value(self, &dbid_tbname).await?;
            assert_table_exist(tb_id_seq, tenant_dbname_tbname, "get_table")?;

            table_id
        };

        let tbid = TableId { table_id };

        let seq_meta = self.get_pb(&tbid).await?;

        let Some(seq_meta) = seq_meta else {
            Err(AppError::from(UnknownTable::new(
                &tenant_dbname_tbname.table_name,
                format!("get_table: {}", tenant_dbname_tbname),
            )))?
        };

        debug!(
            ident :% =(&tbid),
            name :% =(tenant_dbname_tbname),
            table_meta :? =(&seq_meta);
            "get_table"
        );

        let db_type = DatabaseType::NormalDB;

        let tb_info = TableInfo {
            ident: TableIdent {
                table_id: tbid.table_id,
                seq: seq_meta.seq,
            },
            desc: format!(
                "'{}'.'{}'",
                tenant_dbname.database_name(),
                tenant_dbname_tbname.table_name
            ),
            name: tenant_dbname_tbname.table_name.clone(),
            meta: seq_meta.data,
            tenant: req.tenant.tenant_name().to_string(),
            db_type,
            catalog_info: Default::default(),
        };

        return Ok(Arc::new(tb_info));
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_table_meta_history(
        &self,
        database_name: &str,
        table_id_history: &TableIdHistoryIdent,
    ) -> Result<Vec<(TableId, SeqV<TableMeta>)>, KVAppError> {
        let table_name = &table_id_history.table_name;
        let (meta_seq, table_id_list) = get_pb_value(self, table_id_history).await?;
        if meta_seq == 0 || table_id_list.is_none() {
            return Err(KVAppError::AppError(AppError::from(UnknownTable::new(
                table_name,
                format!("get_table_history: {}.{}", database_name, table_name),
            ))));
        }

        let now = Utc::now();
        let table_id_list = table_id_list.unwrap();

        let metas = get_table_meta_history(self, &now, table_id_list).await?;

        debug!(
            name :% =(&table_id_history);
            "get_table_meta_history"
        );
        return Ok(metas);
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_tables_history(
        &self,
        req: ListTableReq,
    ) -> Result<Vec<Arc<TableInfo>>, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let tenant_dbname = &req.inner;

        // Get db by name to ensure presence
        let res = get_db_or_err(
            self,
            tenant_dbname,
            format!("get_tables_history: {}", tenant_dbname.display()),
        )
        .await;

        let (seq_db_id, _db_meta) = match res {
            Ok(x) => x,
            Err(e) => {
                return Err(e);
            }
        };

        // List tables by tenant, db_id, table_name.
        let table_id_history_ident = TableIdHistoryIdent {
            database_id: *seq_db_id.data,
            table_name: "dummy".to_string(),
        };

        let dir_name = DirName::new(table_id_history_ident);

        let table_id_list_keys = list_keys(self, &dir_name).await?;

        let mut tbs_info_list = vec![];
        let now = Utc::now();
        let keys: Vec<String> = table_id_list_keys
            .iter()
            .map(|table_id_list_key| {
                TableIdHistoryIdent {
                    database_id: *seq_db_id.data,
                    table_name: table_id_list_key.table_name.clone(),
                }
                .to_string_key()
            })
            .collect();

        let mut table_id_list_keys_iter = table_id_list_keys.into_iter();
        for c in keys.chunks(DEFAULT_MGET_SIZE) {
            let tb_id_list_seq_vec: Vec<(u64, Option<TableIdList>)> =
                mget_pb_values(self, c).await?;
            for (tb_id_list_seq, tb_id_list_opt) in tb_id_list_seq_vec {
                let table_id_list_key = table_id_list_keys_iter.next().unwrap();
                let tb_id_list = if tb_id_list_seq == 0 {
                    continue;
                } else {
                    match tb_id_list_opt {
                        Some(list) => list,
                        None => {
                            continue;
                        }
                    }
                };

                debug!(
                    name :% =(&table_id_list_key);
                    "get_tables_history"
                );

                let metas = get_table_meta_history(self, &now, tb_id_list).await?;
                let tb_info_list: Vec<Arc<TableInfo>> = metas
                    .into_iter()
                    .map(|(table_id, seqv)| {
                        Arc::new(TableInfo {
                            ident: TableIdent {
                                table_id: table_id.table_id,
                                seq: seqv.seq(),
                            },
                            desc: format!(
                                "'{}'.'{}'",
                                tenant_dbname.database_name(),
                                table_id_list_key.table_name,
                            ),
                            name: table_id_list_key.table_name.to_string(),
                            meta: seqv.data,
                            tenant: tenant_dbname.tenant_name().to_string(),
                            db_type: DatabaseType::NormalDB,
                            catalog_info: Default::default(),
                        })
                    })
                    .collect();
                tbs_info_list.extend(tb_info_list);
            }
        }

        return Ok(tbs_info_list);
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_tables(&self, req: ListTableReq) -> Result<Vec<Arc<TableInfo>>, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let tenant_dbname = &req.inner;

        // Get db by name to ensure presence
        let res = get_db_or_err(
            self,
            tenant_dbname,
            format!("list_tables: {}", tenant_dbname.display()),
        )
        .await;

        let (seq_db_id, _db_meta) = match res {
            Ok(x) => x,
            Err(e) => {
                return Err(e);
            }
        };

        let tb_infos = list_tables_from_unshare_db(self, *seq_db_id.data, tenant_dbname).await?;

        Ok(tb_infos)
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_table_by_id(
        &self,
        table_id: MetaId,
    ) -> Result<Option<SeqV<TableMeta>>, MetaError> {
        debug!(req :? =(&table_id); "SchemaApi: {}", func_name!());

        let id = TableId { table_id };

        let seq_table_meta = self.get_pb(&id).await?;
        Ok(seq_table_meta)
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn mget_table_names_by_ids(
        &self,
        table_ids: &[MetaId],
    ) -> Result<Vec<Option<String>>, KVAppError> {
        debug!(req :? =(&table_ids); "SchemaApi: {}", func_name!());

        let mut id_name_kv_keys = Vec::with_capacity(table_ids.len());
        for id in table_ids {
            let k = TableIdToName { table_id: *id }.to_string_key();
            id_name_kv_keys.push(k);
        }

        // Batch get all table-name by id
        let seq_names = self.mget_kv(&id_name_kv_keys).await?;
        let mut table_names = Vec::with_capacity(table_ids.len());

        for seq_name in seq_names {
            if let Some(seq_name) = seq_name {
                let name_ident: DBIdTableName = deserialize_struct(&seq_name.data)?;
                table_names.push(Some(name_ident.table_name));
            } else {
                table_names.push(None);
            }
        }

        let mut meta_kv_keys = Vec::with_capacity(table_ids.len());
        for id in table_ids {
            let k = TableId { table_id: *id }.to_string_key();
            meta_kv_keys.push(k);
        }

        let seq_metas = self.mget_kv(&meta_kv_keys).await?;
        for (i, seq_meta_opt) in seq_metas.iter().enumerate() {
            if let Some(seq_meta) = seq_meta_opt {
                let table_meta: TableMeta = deserialize_struct(&seq_meta.data)?;
                if table_meta.drop_on.is_some() {
                    table_names[i] = None;
                }
            }
        }

        Ok(table_names)
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_db_name_by_id(&self, db_id: u64) -> Result<String, KVAppError> {
        debug!(req :? =(&db_id); "SchemaApi: {}", func_name!());

        let db_id_to_name_key = DatabaseIdToName { db_id };

        let seq_meta = self.get_pb(&db_id_to_name_key).await?;

        debug!(ident :% =(&db_id_to_name_key); "get_db_name_by_id");

        let Some(seq_meta) = seq_meta else {
            return Err(KVAppError::AppError(AppError::UnknownDatabaseId(
                UnknownDatabaseId::new(db_id, "get_db_name_by_id"),
            )));
        };

        Ok(seq_meta.data.database_name().to_string())
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn mget_database_names_by_ids(
        &self,
        db_ids: &[MetaId],
    ) -> Result<Vec<Option<String>>, KVAppError> {
        debug!(req :? =(&db_ids); "SchemaApi: {}", func_name!());

        let mut kv_keys = Vec::with_capacity(db_ids.len());
        for id in db_ids {
            let k = DatabaseIdToName { db_id: *id }.to_string_key();
            kv_keys.push(k);
        }

        // Batch get all table-name by id
        let seq_names = self.mget_kv(&kv_keys).await?;
        // If multi drop/create db the capacity may not same
        let mut db_names = Vec::with_capacity(db_ids.len());

        for seq_name in seq_names {
            if let Some(seq_name) = seq_name {
                let name_ident: DatabaseNameIdentRaw = deserialize_struct(&seq_name.data)?;
                db_names.push(Some(name_ident.database_name().to_string()));
            } else {
                db_names.push(None);
            }
        }

        let mut meta_kv_keys = Vec::with_capacity(db_ids.len());
        for id in db_ids {
            let k = DatabaseId { db_id: *id }.to_string_key();
            meta_kv_keys.push(k);
        }

        let seq_metas = self.mget_kv(&meta_kv_keys).await?;
        for (i, seq_meta_opt) in seq_metas.iter().enumerate() {
            if let Some(seq_meta) = seq_meta_opt {
                let db_meta: DatabaseMeta = deserialize_struct(&seq_meta.data)?;
                if db_meta.drop_on.is_some() {
                    db_names[i] = None;
                }
            }
        }
        Ok(db_names)
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_table_name_by_id(&self, table_id: MetaId) -> Result<Option<String>, MetaError> {
        debug!(req :? =(&table_id); "SchemaApi: {}", func_name!());

        let table_id_to_name_key = TableIdToName { table_id };

        let seq_table_name = self.get_pb(&table_id_to_name_key).await?;

        debug!(ident :% =(&table_id_to_name_key); "get_table_name_by_id");

        let table_name = seq_table_name.map(|s| s.data.table_name);

        Ok(table_name)
    }

    #[logcall::logcall("debug")]
    #[fastrace::trace]
    async fn drop_table_by_id(&self, req: DropTableByIdReq) -> Result<DropTableReply, KVAppError> {
        let table_id = req.tb_id;
        debug!(req :? =(&table_id); "SchemaApi: {}", func_name!());

        let tenant = &req.tenant;

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let mut txn = TxnRequest::default();

            let opt = construct_drop_table_txn_operations(
                self,
                req.table_name.clone(),
                &req.tenant,
                table_id,
                req.db_id,
                req.if_exists,
                true,
                &mut txn,
            )
            .await?;
            // seq == 0 means that req.if_exists == true and cannot find table meta,
            // in this case just return directly
            if opt.1 == 0 {
                return Ok(DropTableReply {});
            }

            let (succ, _responses) = send_txn(self, txn).await?;

            debug!(
                tenant :% =(tenant.display()),
                id :? =(&table_id),
                succ = succ;
                "drop_table_by_id"
            );
            if succ {
                return Ok(DropTableReply {});
            }
        }
    }

    // make table meta visible by:
    // 1. move table id from orphan table id list to table id list
    // 2. set table meta.drop_on as None
    #[logcall::logcall]
    #[fastrace::trace]
    async fn commit_table_meta(
        &self,
        req: CommitTableMetaReq,
    ) -> Result<CommitTableMetaReply, KVAppError> {
        let table_id = req.table_id;
        debug!(req :? =(&table_id); "SchemaApi: {}", func_name!());

        let tenant_dbname_tbname = &req.name_ident;

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            // Get db by name to ensure presence

            let (db_meta_seq, db_meta) =
                get_db_by_id_or_err(self, req.db_id, "commit_table_meta").await?;
            let db_id = req.db_id;

            // Get table by tenant,db_id, table_name to assert presence.

            let dbid_tbname = DBIdTableName {
                db_id,
                table_name: tenant_dbname_tbname.table_name.clone(),
            };

            let (dbid_tbname_seq, _table_id) = get_u64_value(self, &dbid_tbname).await?;

            // get table id list from _fd_table_id_list/db_id/table_name

            let orphan_dbid_tbname_idlist = TableIdHistoryIdent {
                database_id: db_id,
                table_name: req.orphan_table_name.clone().unwrap(),
            };
            let dbid_tbname_idlist = TableIdHistoryIdent {
                database_id: db_id,
                table_name: tenant_dbname_tbname.table_name.clone(),
            };

            let keys = vec![
                orphan_dbid_tbname_idlist.to_string_key(),
                dbid_tbname_idlist.to_string_key(),
            ];

            let mut data = {
                let values = self.mget_kv(&keys).await?;
                keys.iter()
                    .zip(values.into_iter())
                    .map(|(k, v)| TxnGetResponse::new(k, v.map(pb::SeqV::from)))
                    .collect::<Vec<_>>()
            };

            let orphan_tb_id_list = {
                let d = data.remove(0);
                let (k, v) = deserialize_struct_get_response::<TableIdHistoryIdent>(d)?;
                assert_eq!(orphan_dbid_tbname_idlist, k);

                v.unwrap_or_default()
            };
            if orphan_tb_id_list.data.id_list.len() != 1 {
                error!("table {:?} orphan list is empty", tenant_dbname_tbname);
                let exist_err = CommitTableMetaError::new(
                    tenant_dbname_tbname.table_name.clone(),
                    "orphan list length != 1".to_string(),
                );
                return Err(KVAppError::AppError(AppError::from(exist_err)));
            }

            let mut tb_id_list = {
                let d = data.remove(0);
                let (k, v) = deserialize_struct_get_response::<TableIdHistoryIdent>(d)?;
                assert_eq!(dbid_tbname_idlist, k);

                v.unwrap_or_default()
            };

            if tb_id_list.data.id_list.last() != req.prev_table_id.as_ref() {
                error!(
                    "table {:?} table id list has been changed",
                    tenant_dbname_tbname
                );
                let exist_err = CommitTableMetaError::new(
                    tenant_dbname_tbname.table_name.clone(),
                    "prev_table_id has been changed".to_string(),
                );
                return Err(KVAppError::AppError(AppError::from(exist_err)));
            }

            let table_id = match orphan_tb_id_list.data.id_list.last() {
                Some(table_id) => *table_id,
                None => {
                    return Err(KVAppError::AppError(AppError::UndropTableHasNoHistory(
                        UndropTableHasNoHistory::new(&tenant_dbname_tbname.table_name),
                    )));
                }
            };
            tb_id_list.data.id_list.push(table_id);

            // get tb_meta of the last table id
            let tbid = TableId { table_id };
            let (tb_meta_seq, tb_meta): (_, Option<TableMeta>) = get_pb_value(self, &tbid).await?;

            debug!(
                ident :% =(&tbid),
                name :% =(tenant_dbname_tbname);
                "commit_table_meta"
            );

            {
                // reset drop on time
                let mut tb_meta = tb_meta.unwrap();
                // undrop a table with no drop_on time
                if tb_meta.drop_on.is_none() {
                    return Err(KVAppError::AppError(AppError::UndropTableWithNoDropTime(
                        UndropTableWithNoDropTime::new(&tenant_dbname_tbname.table_name),
                    )));
                }
                tb_meta.drop_on = None;

                let txn_req = TxnRequest {
                    condition: vec![
                        // db has not to change, i.e., no new table is created.
                        // Renaming db is OK and does not affect the seq of db_meta.
                        txn_cond_seq(&DatabaseId { db_id }, Eq, db_meta_seq),
                        // still this table id
                        txn_cond_seq(&dbid_tbname, Eq, dbid_tbname_seq),
                        // table is not changed
                        txn_cond_seq(&tbid, Eq, tb_meta_seq),
                        txn_cond_seq(&orphan_dbid_tbname_idlist, Eq, orphan_tb_id_list.seq),
                        txn_cond_seq(&dbid_tbname_idlist, Eq, tb_id_list.seq),
                    ],
                    if_then: vec![
                        // Changing a table in a db has to update the seq of db_meta,
                        // to block the batch-delete-tables when deleting a db.
                        txn_op_put(&DatabaseId { db_id }, serialize_struct(&db_meta)?), /* (db_id) -> db_meta */
                        txn_op_put(&dbid_tbname, serialize_u64(table_id)?), /* (tenant, db_id, tb_name) -> tb_id */
                        // txn_op_put(&dbid_tbname_idlist, serialize_struct(&tb_id_list)?)?, // _fd_table_id_list/db_id/table_name -> tb_id_list
                        txn_op_put(&tbid, serialize_struct(&tb_meta)?), /* (tenant, db_id, tb_id) -> tb_meta */
                        txn_op_del(&orphan_dbid_tbname_idlist),         // del orphan table idlist
                        txn_op_put(&dbid_tbname_idlist, serialize_struct(&tb_id_list.data)?), /* _fd_table_id_list/db_id/table_name -> tb_id_list */
                    ],
                    else_then: vec![],
                };

                let (succ, _responses) = send_txn(self, txn_req).await?;

                debug!(
                    name :? =(tenant_dbname_tbname),
                    id :? =(&tbid),
                    succ = succ;
                    "commit_table_meta"
                );

                if succ {
                    return Ok(CommitTableMetaReply {});
                }
            }
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_table_copied_file_info(
        &self,
        req: GetTableCopiedFileReq,
    ) -> Result<GetTableCopiedFileReply, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let table_id = req.table_id;
        let tbid = TableId { table_id };

        let seq_meta = self.get_pb(&tbid).await?;

        let Some(seq_meta) = seq_meta else {
            return Err(KVAppError::AppError(AppError::UnknownTableId(
                UnknownTableId::new(table_id, ""),
            )));
        };

        debug!(
            ident :% =(&tbid),
            table_meta :? =(&seq_meta);
            "get_table_copied_file_info"
        );

        let mut file_infos = BTreeMap::new();

        let mut keys = Vec::with_capacity(req.files.len());

        for file in req.files.iter() {
            let ident = TableCopiedFileNameIdent {
                table_id,
                file: file.clone(),
            };
            keys.push(ident.to_string_key());
        }

        let mut file_names = req.files.into_iter();

        for c in keys.chunks(DEFAULT_MGET_SIZE) {
            let seq_infos = mget_pb_values(self, c).await?;

            for (_seq, file_info) in seq_infos {
                let f_name = file_names.next().unwrap();

                if let Some(f_info) = file_info {
                    file_infos.insert(f_name, f_info);
                }
            }
        }

        Ok(GetTableCopiedFileReply {
            file_info: file_infos,
        })
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn truncate_table(
        &self,
        req: TruncateTableReq,
    ) -> Result<TruncateTableReply, KVAppError> {
        // NOTE: this method read and remove in multiple transactions.
        // It is not atomic, but it is safe because it deletes only the files that matches the seq.

        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let ctx = func_name!();

        let table_id = TableId {
            table_id: req.table_id,
        };

        let chunk_size = req.batch_size.unwrap_or(DEFAULT_MGET_SIZE as u64);

        // 1. Grab a snapshot view of the copied files of a table.
        //
        // If table seq is not changed before and after listing, we can be sure the list of copied
        // files is consistent to this version of the table.

        let (mut seq_1, _tb_meta) = get_table_by_id_or_err(self, &table_id, ctx).await?;

        let mut trials = txn_backoff(None, func_name!());
        let copied_files = loop {
            trials.next().unwrap()?.await;

            let copied_files = list_table_copied_files(self, table_id.table_id).await?;

            let (seq_2, _tb_meta) = get_table_by_id_or_err(self, &table_id, ctx).await?;

            if seq_1 == seq_2 {
                debug!(
                    "list all copied file of table {}: {:?}",
                    table_id.table_id, copied_files
                );
                break copied_files;
            } else {
                seq_1 = seq_2;
            }
        };

        // 2. Remove the copied files only when the seq of a copied file has not changed.
        //
        // During running this step with several small transaction, other transactions may be
        // modifying the table.
        //
        // - We assert the table seq is not changed in each transaction.
        // - We do not assert the seq of each copied file in each transaction, since we only delete
        //   non-changed ones.

        for chunk in copied_files.chunks(chunk_size as usize) {
            let str_keys: Vec<_> = chunk.iter().map(|f| f.to_string_key()).collect();

            // Load the `seq` of every copied file
            let seqs = {
                let seq_infos: Vec<(u64, Option<TableCopiedFileInfo>)> =
                    mget_pb_values(self, &str_keys).await?;

                seq_infos.into_iter().map(|(seq, _)| seq)
            };

            let mut if_then = vec![];
            for (copied_seq, copied_str_key) in seqs.zip(str_keys) {
                if copied_seq == 0 {
                    continue;
                }

                if_then.push(TxnOp::delete_exact(copied_str_key, Some(copied_seq)));
            }

            let mut trials = txn_backoff(None, func_name!());
            loop {
                trials.next().unwrap()?.await;

                let (tb_meta_seq, tb_meta) = get_table_by_id_or_err(self, &table_id, ctx).await?;

                let mut if_then = if_then.clone();

                // Update to increase table meta seq, so that to assert no other process modify the table
                if_then.push(txn_op_put(&table_id, serialize_struct(&tb_meta)?));

                let txn_req = TxnRequest {
                    condition: vec![txn_cond_seq(&table_id, Eq, tb_meta_seq)],
                    if_then,
                    else_then: vec![],
                };

                debug!("submit chunk delete copied files: {:?}", txn_req);

                let (succ, _responses) = send_txn(self, txn_req).await?;
                debug!(
                    id :? =(&table_id),
                    succ = succ,
                    ctx = ctx;
                    ""
                );

                if succ {
                    break;
                }
            }
        }

        Ok(TruncateTableReply {})
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn upsert_table_option(
        &self,
        req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let tbid = TableId {
            table_id: req.table_id,
        };
        let req_seq = req.seq;

        loop {
            let (tb_meta_seq, table_meta): (_, Option<TableMeta>) =
                get_pb_value(self, &tbid).await?;

            debug!(ident :% =(&tbid); "upsert_table_option");

            if tb_meta_seq == 0 || table_meta.is_none() {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(req.table_id, "upsert_table_option"),
                )));
            }
            if req_seq.match_seq(tb_meta_seq).is_err() {
                return Err(KVAppError::AppError(AppError::from(
                    TableVersionMismatched::new(
                        req.table_id,
                        req.seq,
                        tb_meta_seq,
                        "upsert_table_option",
                    ),
                )));
            }
            let mut table_meta = table_meta.unwrap();
            // update table options
            let opts = &mut table_meta.options;

            for (k, opt_v) in &req.options {
                match opt_v {
                    None => {
                        opts.remove(k);
                    }
                    Some(v) => {
                        opts.insert(k.to_string(), v.to_string());
                    }
                }
            }
            let txn_req = TxnRequest {
                condition: vec![
                    // table is not changed
                    txn_cond_seq(&tbid, Eq, tb_meta_seq),
                ],
                if_then: vec![
                    txn_op_put(&tbid, serialize_struct(&table_meta)?), // tb_id -> tb_meta
                ],
                else_then: vec![],
            };

            let (succ, _responses) = send_txn(self, txn_req).await?;

            debug!(
                id :? =(&tbid),
                succ = succ;
                "upsert_table_option"
            );

            if succ {
                return Ok(UpsertTableOptionReply {});
            }
        }
    }

    async fn update_multi_table_meta(
        &self,
        req: UpdateMultiTableMetaReq,
    ) -> Result<UpdateMultiTableMetaResult, KVAppError> {
        let UpdateMultiTableMetaReq {
            update_table_metas,
            copied_files,
            update_stream_metas,
            deduplicated_labels,
            update_temp_tables: _,
        } = req;

        let mut tbl_seqs = HashMap::new();
        let mut txn_req = TxnRequest::default();
        let mut mismatched_tbs = vec![];
        let tid_vec = update_table_metas
            .iter()
            .map(|req| {
                TableId {
                    table_id: req.0.table_id,
                }
                .to_string_key()
            })
            .collect::<Vec<_>>();
        let mut tb_meta_vec: Vec<(u64, Option<TableMeta>)> = mget_pb_values(self, &tid_vec).await?;
        for (req, (tb_meta_seq, table_meta)) in
            update_table_metas.iter().zip(tb_meta_vec.iter_mut())
        {
            let req_seq = req.0.seq;

            if *tb_meta_seq == 0 || table_meta.is_none() {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(req.0.table_id, "update_multi_table_meta"),
                )));
            }
            if req_seq.match_seq(*tb_meta_seq).is_err() {
                mismatched_tbs.push((
                    req.0.table_id,
                    *tb_meta_seq,
                    std::mem::take(table_meta).unwrap(),
                ));
            }
        }

        if !mismatched_tbs.is_empty() {
            return Ok(Err(mismatched_tbs));
        }

        let mut new_table_meta_map: BTreeMap<u64, TableMeta> = BTreeMap::new();
        for (req, (tb_meta_seq, table_meta)) in update_table_metas.iter().zip(tb_meta_vec.iter()) {
            let tbid = TableId {
                table_id: req.0.table_id,
            };
            // `update_table_meta` MUST NOT modify `shared_by` field
            let table_meta = table_meta.as_ref().unwrap();
            let mut new_table_meta = req.0.new_table_meta.clone();
            new_table_meta.shared_by = table_meta.shared_by.clone();

            tbl_seqs.insert(req.0.table_id, *tb_meta_seq);
            txn_req
                .condition
                .push(txn_cond_seq(&tbid, Eq, *tb_meta_seq));
            txn_req
                .if_then
                .push(txn_op_put(&tbid, serialize_struct(&new_table_meta)?));
            txn_req.else_then.push(TxnOp {
                request: Some(Request::Get(TxnGetRequest {
                    key: tbid.to_string_key(),
                })),
            });

            new_table_meta_map.insert(req.0.table_id, new_table_meta);
        }
        for (tbid, req) in copied_files {
            let tbid = TableId { table_id: tbid };
            let (conditions, match_operations) = build_upsert_table_copied_file_info_conditions(
                &tbid,
                &req,
                tbl_seqs[&tbid.table_id],
                req.fail_if_duplicated,
            )?;
            txn_req.condition.extend(conditions);
            txn_req.if_then.extend(match_operations)
        }

        let sid_vec = update_stream_metas
            .iter()
            .map(|req| {
                TableId {
                    table_id: req.stream_id,
                }
                .to_string_key()
            })
            .collect::<Vec<_>>();
        let stream_meta_vec: Vec<(u64, Option<TableMeta>)> = mget_pb_values(self, &sid_vec).await?;
        for (req, (stream_meta_seq, stream_meta)) in
            update_stream_metas.iter().zip(stream_meta_vec.into_iter())
        {
            let stream_id = TableId {
                table_id: req.stream_id,
            };

            if stream_meta_seq == 0 || stream_meta.is_none() {
                return Err(KVAppError::AppError(AppError::UnknownStreamId(
                    UnknownStreamId::new(req.stream_id, "update_multi_table_meta"),
                )));
            }

            if req.seq.match_seq(stream_meta_seq).is_err() {
                return Err(KVAppError::AppError(AppError::from(
                    StreamVersionMismatched::new(
                        req.stream_id,
                        req.seq,
                        stream_meta_seq,
                        "update_multi_table_meta",
                    ),
                )));
            }

            let mut new_stream_meta = stream_meta.unwrap();
            new_stream_meta.options = req.options.clone();
            new_stream_meta.updated_on = Utc::now();

            txn_req
                .condition
                .push(txn_cond_seq(&stream_id, Eq, stream_meta_seq));
            txn_req
                .if_then
                .push(txn_op_put(&stream_id, serialize_struct(&new_stream_meta)?));
        }

        for deduplicated_label in deduplicated_labels {
            txn_req
                .if_then
                .push(build_upsert_table_deduplicated_label(deduplicated_label));
        }
        let (succ, responses) = send_txn(self, txn_req).await?;
        if succ {
            return Ok(Ok(UpdateTableMetaReply {}));
        }
        let mut mismatched_tbs = vec![];
        for (resp, req) in responses.iter().zip(update_table_metas.iter()) {
            let Some(Response::Get(get_resp)) = &resp.response else {
                unreachable!(
                    "internal error: expect some TxnGetResponseGet, but got {:?}",
                    resp.response
                )
            };
            // deserialize table version info
            let (tb_meta_seq, table_meta): (_, TableMeta) = if let Some(seq_v) = &get_resp.value {
                (seq_v.seq, deserialize_struct(&seq_v.data)?)
            } else {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(req.0.table_id, "update_multi_table_meta"),
                )));
            };

            // check table version
            if req.0.seq.match_seq(tb_meta_seq).is_err() {
                mismatched_tbs.push((req.0.table_id, tb_meta_seq, table_meta));
            }
        }

        if mismatched_tbs.is_empty() {
            // if all table version does match, but tx failed, we don't know why, just return error
            Err(KVAppError::AppError(AppError::from(
                MultiStmtTxnCommitFailed::new("update_multi_table_meta"),
            )))
        } else {
            // up layer will retry
            Ok(Err(mismatched_tbs))
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn set_table_column_mask_policy(
        &self,
        req: SetTableColumnMaskPolicyReq,
    ) -> Result<SetTableColumnMaskPolicyReply, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());
        let tbid = TableId {
            table_id: req.table_id,
        };
        let req_seq = req.seq;

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let seq_meta = self.get_pb(&tbid).await?;

            debug!(ident :% =(&tbid); "set_table_column_mask_policy");

            let Some(seq_meta) = seq_meta else {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(req.table_id, "set_table_column_mask_policy"),
                )));
            };

            if req_seq.match_seq(seq_meta.seq).is_err() {
                return Err(KVAppError::AppError(AppError::from(
                    TableVersionMismatched::new(
                        req.table_id,
                        req.seq,
                        seq_meta.seq,
                        "set_table_column_mask_policy",
                    ),
                )));
            }

            // upsert column mask policy
            let table_meta = seq_meta.data;

            let mut new_table_meta = table_meta.clone();
            if new_table_meta.column_mask_policy.is_none() {
                let column_mask_policy = BTreeMap::default();
                new_table_meta.column_mask_policy = Some(column_mask_policy);
            }

            match &req.action {
                SetTableColumnMaskPolicyAction::Set(new_mask_name, _old_mask_name) => {
                    new_table_meta
                        .column_mask_policy
                        .as_mut()
                        .unwrap()
                        .insert(req.column.clone(), new_mask_name.clone());
                }
                SetTableColumnMaskPolicyAction::Unset(_) => {
                    new_table_meta
                        .column_mask_policy
                        .as_mut()
                        .unwrap()
                        .remove(&req.column);
                }
            }

            let mut txn_req = TxnRequest {
                condition: vec![
                    // table is not changed
                    txn_cond_seq(&tbid, Eq, seq_meta.seq),
                ],
                if_then: vec![
                    txn_op_put(&tbid, serialize_struct(&new_table_meta)?), // tb_id -> tb_meta
                ],
                else_then: vec![],
            };

            let _ = update_mask_policy(self, &req.action, &mut txn_req, &req.tenant, req.table_id)
                .await;

            let (succ, _responses) = send_txn(self, txn_req).await?;

            debug!(
                id :? =(&tbid),
                succ = succ;
                "set_table_column_mask_policy"
            );

            if succ {
                return Ok(SetTableColumnMaskPolicyReply {});
            }
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn create_table_index(&self, req: CreateTableIndexReq) -> Result<(), KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let tbid = TableId {
            table_id: req.table_id,
        };

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let (tb_meta_seq, table_meta): (_, Option<TableMeta>) =
                get_pb_value(self, &tbid).await?;

            debug!(ident :% =(&tbid); "create_table_index");

            if tb_meta_seq == 0 || table_meta.is_none() {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(req.table_id, "create_table_index"),
                )));
            }

            let mut table_meta = table_meta.unwrap();
            // update table indexes
            let indexes = &mut table_meta.indexes;
            if indexes.contains_key(&req.name) {
                match req.create_option {
                    CreateOption::Create => {
                        return Err(AppError::IndexAlreadyExists(
                            IndexNameIdent::new(&req.tenant, &req.name).exist_error(func_name!()),
                        )
                        .into());
                    }
                    CreateOption::CreateIfNotExists => {
                        return Ok(());
                    }
                    CreateOption::CreateOrReplace => {}
                }
            }
            // check the index column id exists
            for column_id in &req.column_ids {
                if table_meta.schema.is_column_deleted(*column_id) {
                    return Err(KVAppError::AppError(AppError::IndexColumnIdNotFound(
                        IndexColumnIdNotFound::new(*column_id, &req.name),
                    )));
                }
            }

            // column_id can not be duplicated
            for (name, index) in indexes.iter() {
                if *name == req.name {
                    continue;
                }
                for column_id in &req.column_ids {
                    if index.column_ids.contains(column_id) {
                        return Err(KVAppError::AppError(AppError::DuplicatedIndexColumnId(
                            DuplicatedIndexColumnId::new(*column_id, &req.name),
                        )));
                    }
                }
            }

            // If the column ids and options do not change,
            // use the old index version, otherwise create a new index version.
            let mut old_version = None;
            if let Some(old_index) = indexes.get(&req.name) {
                if old_index.column_ids == req.column_ids && old_index.options == req.options {
                    old_version = Some(old_index.version.clone());
                }
            }
            let version = old_version.unwrap_or(Uuid::new_v4().simple().to_string());

            let index = TableIndex {
                name: req.name.clone(),
                column_ids: req.column_ids.clone(),
                sync_creation: req.sync_creation,
                version,
                options: req.options.clone(),
            };
            indexes.insert(req.name.clone(), index);

            let txn_req = TxnRequest {
                condition: vec![
                    // table is not changed
                    txn_cond_seq(&tbid, Eq, tb_meta_seq),
                ],
                if_then: vec![
                    txn_op_put(&tbid, serialize_struct(&table_meta)?), // tb_id -> tb_meta
                ],
                else_then: vec![],
            };

            let (succ, _responses) = send_txn(self, txn_req).await?;

            debug!(
                id :? =(&tbid),
                succ = succ;
                "create_table_index"
            );

            if succ {
                return Ok(());
            }
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn drop_table_index(&self, req: DropTableIndexReq) -> Result<(), KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let tbid = TableId {
            table_id: req.table_id,
        };

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let seq_meta = self.get_pb(&tbid).await?;

            debug!(ident :% =(&tbid); "drop_table_index");

            let Some(seq_meta) = seq_meta else {
                return Err(KVAppError::AppError(AppError::UnknownTableId(
                    UnknownTableId::new(req.table_id, "drop_table_index"),
                )));
            };

            let mut table_meta = seq_meta.data;
            // update table indexes
            let indexes = &mut table_meta.indexes;
            if !indexes.contains_key(&req.name) && !req.if_exists {
                return Err(KVAppError::AppError(AppError::UnknownIndex(
                    UnknownError::<IndexName>::new(req.name.clone(), "drop table index"),
                )));
            }
            indexes.remove(&req.name);

            let txn_req = TxnRequest {
                condition: vec![
                    // table is not changed
                    txn_cond_seq(&tbid, Eq, seq_meta.seq),
                ],
                if_then: vec![
                    txn_op_put(&tbid, serialize_struct(&table_meta)?), // tb_id -> tb_meta
                ],
                else_then: vec![],
            };

            let (succ, _responses) = send_txn(self, txn_req).await?;
            debug!(id :? =(&tbid),succ = succ;"drop_table_index");

            if succ {
                return Ok(());
            }
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_drop_table_infos(
        &self,
        req: ListDroppedTableReq,
    ) -> Result<ListDroppedTableResp, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        if let TableInfoFilter::AllDroppedTables(filter_drop_on) = &req.filter {
            let db_infos = self
                .get_database_history(ListDatabaseReq {
                    tenant: req.inner.tenant().clone(),
                    // need to get all db(include drop db)
                    filter: Some(DatabaseInfoFilter::IncludeDropped),
                })
                .await?;

            let mut drop_table_infos = vec![];
            let mut drop_ids = vec![];
            for db_info in db_infos {
                let mut drop_db = false;
                let filter = match db_info.meta.drop_on {
                    Some(db_drop_on) => {
                        if let Some(filter_drop_on) = filter_drop_on {
                            if db_drop_on.timestamp() <= filter_drop_on.timestamp() {
                                // if db drop on before filter time, then get all the db tables.
                                drop_db = true;
                                TableInfoFilter::All
                            } else {
                                // else get all the db tables drop on before filter time.
                                TableInfoFilter::Dropped(Some(*filter_drop_on))
                            }
                        } else {
                            // while filter_drop_on is None, then get all the drop db tables
                            drop_db = true;
                            TableInfoFilter::All
                        }
                    }
                    None => {
                        // not drop db, only filter drop tables with filter drop on
                        TableInfoFilter::Dropped(*filter_drop_on)
                    }
                };

                let db_filter = (filter, db_info.clone());

                let left_num = if let Some(limit) = req.limit {
                    if drop_table_infos.len() >= limit {
                        return Ok(ListDroppedTableResp {
                            drop_table_infos,
                            drop_ids,
                        });
                    }
                    Some(limit - drop_table_infos.len())
                } else {
                    None
                };

                let table_infos = do_get_table_history(self, db_filter, left_num).await?;

                // check if reach the limit
                if let Some(left_num) = left_num {
                    let num = min(left_num, table_infos.len());
                    for table_info in table_infos.iter().take(num) {
                        let (table_info, db_id) = table_info;
                        drop_ids.push(DroppedId::Table(
                            *db_id,
                            table_info.ident.table_id,
                            table_info.name.clone(),
                        ));
                        drop_table_infos.push(table_info.clone());
                    }

                    // if limit is Some, append DroppedId::Db only when table_infos is empty
                    if drop_db && table_infos.is_empty() {
                        drop_ids.push(DroppedId::Db(
                            db_info.database_id.db_id,
                            db_info.name_ident.database_name().to_string(),
                        ));
                    }
                    if num == left_num {
                        return Ok(ListDroppedTableResp {
                            drop_table_infos,
                            drop_ids,
                        });
                    }
                } else {
                    table_infos.iter().for_each(|(table_info, db_id)| {
                        if !drop_db {
                            drop_ids.push(DroppedId::Table(
                                *db_id,
                                table_info.ident.table_id,
                                table_info.name.clone(),
                            ))
                        }
                    });
                    drop_table_infos.extend(
                        table_infos
                            .into_iter()
                            .map(|(table_info, _)| table_info)
                            .collect::<Vec<_>>(),
                    );
                    if drop_db {
                        drop_ids.push(DroppedId::Db(
                            db_info.database_id.db_id,
                            db_info.name_ident.database_name().to_string(),
                        ));
                    }
                }
            }

            return Ok(ListDroppedTableResp {
                drop_table_infos,
                drop_ids,
            });
        }

        let tenant_dbname = &req.inner;

        // Get db by name to ensure presence
        let res = get_db_or_err(
            self,
            tenant_dbname,
            format!("get_table_history: {}", tenant_dbname.display()),
        )
        .await;

        let (seq_db_id, db_meta) = match res {
            Ok(x) => x,
            Err(e) => {
                return Err(e);
            }
        };

        let db_info = Arc::new(DatabaseInfo {
            database_id: seq_db_id.data,
            name_ident: req.inner.clone(),
            meta: db_meta,
        });
        let db_filter = (req.filter, db_info);
        let table_infos = do_get_table_history(self, db_filter, req.limit).await?;
        let mut drop_ids = vec![];
        let mut drop_table_infos = vec![];
        let num = if let Some(limit) = req.limit {
            min(limit, table_infos.len())
        } else {
            table_infos.len()
        };
        for table_info in table_infos.iter().take(num) {
            let (table_info, db_id) = table_info;
            drop_ids.push(DroppedId::Table(
                *db_id,
                table_info.ident.table_id,
                table_info.name.clone(),
            ));
            drop_table_infos.push(table_info.clone());
        }

        Ok(ListDroppedTableResp {
            drop_table_infos,
            drop_ids,
        })
    }

    #[fastrace::trace]
    async fn gc_drop_tables(&self, req: GcDroppedTableReq) -> Result<(), KVAppError> {
        for drop_id in req.drop_ids {
            match drop_id {
                DroppedId::Db(db_id, db_name) => {
                    gc_dropped_db_by_id(self, db_id, &req.tenant, db_name).await?
                }
                DroppedId::Table(db_id, table_id, table_name) => {
                    gc_dropped_table_by_id(self, &req.tenant, db_id, table_id, table_name).await?
                }
            }
        }
        Ok(())
    }

    #[fastrace::trace]
    async fn list_lock_revisions(
        &self,
        req: ListLockRevReq,
    ) -> Result<Vec<(u64, LockMeta)>, KVAppError> {
        let dir = req.lock_key.gen_prefix();
        let strm = self.list_pb(&dir).await?;

        let list = strm
            .map_ok(|itm| (itm.key.revision(), itm.seqv.data))
            .try_collect::<Vec<_>>()
            .await?;

        Ok(list)
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn create_lock_revision(
        &self,
        req: CreateLockRevReq,
    ) -> Result<CreateLockRevReply, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let lock_key = &req.lock_key;

        let revision = fetch_id(self, IdGenerator::table_lock_id()).await?;
        let key = lock_key.gen_key(revision);

        let lock_meta = LockMeta {
            user: req.user.clone(),
            node: req.node.clone(),
            query_id: req.query_id.clone(),
            created_on: Utc::now(),
            acquired_on: None,
            lock_type: lock_key.lock_type(),
            extra_info: lock_key.get_extra_info(),
        };

        // Revision is unique. if it presents, consider it as success.
        // Thus, we could just ignore create result
        let _create_res = self.create_name_value(key, lock_meta).await?;

        Ok(CreateLockRevReply { revision })
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn extend_lock_revision(&self, req: ExtendLockRevReq) -> Result<(), KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let ctx = func_name!();

        let lock_key = &req.lock_key;
        let table_id = lock_key.get_table_id();
        let key = lock_key.gen_key(req.revision);

        self.update_existent_name_value(
            &key,
            |mut lock_meta| {
                // Set `acquire_lock = true` to initialize `acquired_on` when the
                // first time this lock is acquired. Before the lock is
                // acquired(becoming the first in lock queue), or after being
                // acquired, this argument is always `false`.
                if req.acquire_lock {
                    lock_meta.acquired_on = Some(Utc::now());
                }
                Some((lock_meta, Some(req.ttl)))
            },
            || {
                Err(AppError::TableLockExpired(TableLockExpired::new(
                    table_id, ctx,
                )))
            },
        )
        .await?;
        Ok(())
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn delete_lock_revision(&self, req: DeleteLockRevReq) -> Result<(), KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let lock_key = &req.lock_key;

        let revision = req.revision;
        let key = lock_key.gen_key(revision);

        self.remove_name_value(&key, || Ok::<(), ()>(()))
            .await?
            .unwrap();

        Ok(())
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_locks(&self, req: ListLocksReq) -> Result<Vec<LockInfo>, KVAppError> {
        let mut reply = vec![];
        for dir in &req.prefixes {
            let strm = self.list_pb(dir).await?;
            let locks = strm
                .map_ok(|itm| LockInfo {
                    table_id: itm.key.table_id(),
                    revision: itm.key.revision(),
                    meta: itm.seqv.data,
                })
                .try_collect::<Vec<_>>()
                .await?;

            reply.extend(locks);
        }
        Ok(reply)
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn create_catalog(
        &self,
        name_ident: &CatalogNameIdent,
        meta: &CatalogMeta,
    ) -> Result<Result<CatalogId, SeqV<CatalogId>>, KVAppError> {
        debug!(name_ident :? =(&name_ident), meta :? = meta; "SchemaApi: {}", func_name!());

        let name_ident_raw = serialize_struct(&CatalogNameIdentRaw::from(name_ident))?;

        let res = self
            .create_id_value(name_ident, meta, false, |id| {
                vec![(
                    CatalogIdToNameIdent::new_generic(name_ident.tenant(), id).to_string_key(),
                    name_ident_raw.clone(),
                )]
            })
            .await?;

        Ok(res)
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_catalog(
        &self,
        name_ident: &CatalogNameIdent,
    ) -> Result<Arc<CatalogInfo>, KVAppError> {
        debug!(req :? =name_ident; "SchemaApi: {}", func_name!());

        let (seq_id, seq_meta) = self
            .get_id_and_value(name_ident)
            .await?
            .ok_or_else(|| AppError::unknown(name_ident, func_name!()))?;

        let catalog = CatalogInfo::new(name_ident.clone(), seq_id.data, seq_meta.data);

        Ok(Arc::new(catalog))
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn drop_catalog(
        &self,
        name_ident: &CatalogNameIdent,
    ) -> Result<Option<(SeqV<CatalogId>, SeqV<CatalogMeta>)>, KVAppError> {
        debug!(req :? =(&name_ident); "SchemaApi: {}", func_name!());

        let removed = self
            .remove_id_value(name_ident, |id| {
                vec![CatalogIdToNameIdent::new_generic(name_ident.tenant(), id).to_string_key()]
            })
            .await?;

        Ok(removed)
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_catalogs(
        &self,
        req: ListCatalogReq,
    ) -> Result<Vec<Arc<CatalogInfo>>, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let tenant = req.tenant;
        let name_key = CatalogNameIdent::new(&tenant, "dummy");
        let dir = DirName::new(name_key);

        let name_id_values = self.list_id_value(&dir).await?;

        let catalog_infos = name_id_values
            .map(|(name, id, seq_meta)| Arc::new(CatalogInfo::new(name, id, seq_meta.data)))
            .collect::<Vec<_>>();

        Ok(catalog_infos)
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn set_table_lvt(&self, req: SetLVTReq) -> Result<SetLVTReply, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let table_id = req.table_id;

        let mut trials = txn_backoff(None, func_name!());
        loop {
            trials.next().unwrap()?.await;

            let lvt_key = LeastVisibleTimeKey { table_id };
            let (lvt_seq, lvt_opt): (_, Option<LeastVisibleTime>) =
                get_pb_value(self, &lvt_key).await?;
            let new_time = match lvt_opt {
                Some(lvt) => {
                    if lvt.time >= req.time {
                        return Ok(SetLVTReply { time: lvt.time });
                    } else {
                        req.time
                    }
                }
                None => req.time,
            };

            let new_lvt = LeastVisibleTime { time: new_time };

            let txn_req = TxnRequest {
                condition: vec![txn_cond_seq(&lvt_key, Eq, lvt_seq)],
                if_then: vec![txn_op_put(&lvt_key, serialize_struct(&new_lvt)?)],
                else_then: vec![],
            };

            let (succ, _responses) = send_txn(self, txn_req).await?;

            debug!(
                name :? =(req.table_id),
                succ = succ;
                "set_table_lvt"
            );

            if succ {
                return Ok(SetLVTReply { time: new_time });
            }
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_table_lvt(&self, req: GetLVTReq) -> Result<GetLVTReply, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let table_id = req.table_id;

        let lvt_key = LeastVisibleTimeKey { table_id };

        let seq_lvt = self.get_pb(&lvt_key).await?;
        let lvt_opt = seq_lvt.into_value();

        Ok(GetLVTReply {
            time: lvt_opt.map(|time| time.time),
        })
    }

    fn name(&self) -> String {
        "SchemaApiImpl".to_string()
    }

    // dictionary
    #[logcall::logcall]
    #[fastrace::trace]
    async fn create_dictionary(
        &self,
        req: CreateDictionaryReq,
    ) -> Result<CreateDictionaryReply, KVAppError> {
        debug!(req :? = (&req); "SchemaApi: {}", func_name!());

        let name_ident = &req.dictionary_ident;

        let create_res = self
            .create_id_value(name_ident, &req.dictionary_meta, false, |_| vec![])
            .await?;

        match create_res {
            Ok(id) => Ok(CreateDictionaryReply { dictionary_id: *id }),
            Err(_existent) => Err(AppError::from(name_ident.exist_error(func_name!())).into()),
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn update_dictionary(
        &self,
        req: UpdateDictionaryReq,
    ) -> Result<UpdateDictionaryReply, KVAppError> {
        debug!(req :? = (&req); "SchemaApi: {}", func_name!());

        let res = self
            .update_id_value(&req.dictionary_ident, req.dictionary_meta)
            .await?;

        if let Some((id, _meta)) = res {
            Ok(UpdateDictionaryReply { dictionary_id: *id })
        } else {
            Err(AppError::from(req.dictionary_ident.unknown_error(func_name!())).into())
        }
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn drop_dictionary(
        &self,
        name_ident: DictionaryNameIdent,
    ) -> Result<Option<SeqV<DictionaryMeta>>, MetaTxnError> {
        debug!(dict_ident :? =(&name_ident); "SchemaApi: {}", func_name!());

        let removed = self.remove_id_value(&name_ident, |_| vec![]).await?;
        Ok(removed.map(|(_, meta)| meta))
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn get_dictionary(
        &self,
        name_ident: DictionaryNameIdent,
    ) -> Result<Option<(SeqV<DictionaryId>, SeqV<DictionaryMeta>)>, MetaError> {
        debug!(dict_ident :? =(&name_ident); "SchemaApi: {}", func_name!());

        let got = self.get_id_value(&name_ident).await?;
        Ok(got)
    }

    #[logcall::logcall]
    #[fastrace::trace]
    async fn list_dictionaries(
        &self,
        req: ListDictionaryReq,
    ) -> Result<Vec<(String, DictionaryMeta)>, KVAppError> {
        debug!(req :? =(&req); "SchemaApi: {}", func_name!());

        let dictionary_ident = DictionaryNameIdent::new(
            req.tenant.clone(),
            DictionaryIdentity::new(req.db_id, "dummy".to_string()),
        );
        let dir = DirName::new_with_level(dictionary_ident, 2);

        let name_id_values = self.list_id_value(&dir).await?;
        Ok(name_id_values
            .map(|(name, _seq_id, seq_meta)| (name.dict_name(), seq_meta.data))
            .collect())
    }
}

async fn get_table_meta_history(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    now: &DateTime<Utc>,
    tb_id_list: TableIdList,
) -> Result<Vec<(TableId, SeqV<TableMeta>)>, KVAppError> {
    let mut tb_metas = vec![];
    let inner_keys: Vec<String> = tb_id_list
        .id_list
        .iter()
        .map(|table_id| {
            TableId {
                table_id: *table_id,
            }
            .to_string_key()
        })
        .collect();
    let mut table_id_iter = tb_id_list.id_list.into_iter();
    for c in inner_keys.chunks(DEFAULT_MGET_SIZE) {
        let tb_meta_vec: Vec<(u64, Option<TableMeta>)> = mget_pb_values(kv_api, c).await?;
        for (tb_meta_seq, tb_meta) in tb_meta_vec {
            let table_id = table_id_iter.next().unwrap();
            if tb_meta_seq == 0 || tb_meta.is_none() {
                error!("get_table_history cannot find {:?} table_meta", table_id);
                continue;
            }

            // Safe unwrap() because: tb_meta_seq > 0
            let tb_meta = tb_meta.unwrap();
            if is_drop_time_out_of_retention_time(&tb_meta.drop_on, now) {
                continue;
            }

            tb_metas.push((TableId { table_id }, SeqV::new(tb_meta_seq, tb_meta)));
        }
    }
    Ok(tb_metas)
}

async fn construct_drop_table_txn_operations(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    table_name: String,
    tenant: &Tenant,
    table_id: u64,
    db_id: u64,
    if_exists: bool,
    if_delete: bool,
    txn: &mut TxnRequest,
) -> Result<(u64, u64), KVAppError> {
    let tbid = TableId { table_id };

    // Check if table exists.
    let (tb_meta_seq, tb_meta): (_, Option<TableMeta>) = get_pb_value(kv_api, &tbid).await?;
    if tb_meta_seq == 0 || tb_meta.is_none() {
        return Err(KVAppError::AppError(AppError::UnknownTableId(
            UnknownTableId::new(table_id, "drop_table_by_id failed to find valid tb_meta"),
        )));
    }

    // Get db name, tenant name and related info for tx.
    let table_id_to_name = TableIdToName { table_id };
    let (_, table_name_opt): (_, Option<DBIdTableName>) =
        get_pb_value(kv_api, &table_id_to_name).await?;

    let dbid_tbname = if let Some(db_id_table_name) = table_name_opt {
        db_id_table_name
    } else {
        let dbid_tbname = DBIdTableName {
            db_id,
            table_name: table_name.clone(),
        };
        warn!(
            "drop_table_by_id cannot find {:?}, use {:?} instead",
            table_id_to_name, dbid_tbname
        );

        dbid_tbname
    };

    let db_id = dbid_tbname.db_id;
    let tbname = dbid_tbname.table_name.clone();
    let (tb_id_seq, _) = get_u64_value(kv_api, &dbid_tbname).await?;
    if tb_id_seq == 0 {
        return if if_exists {
            Ok((0, 0))
        } else {
            return Err(KVAppError::AppError(AppError::UnknownTable(
                UnknownTable::new(tbname, "drop_table_by_id"),
            )));
        };
    }

    let (db_meta_seq, db_meta) = get_db_by_id_or_err(kv_api, db_id, "drop_table_by_id").await?;

    debug!(
        ident :% =(&tbid),
        tenant :% =(tenant.display());
        "drop table by id"
    );

    let mut tb_meta = tb_meta.unwrap();
    // drop a table with drop_on time
    if tb_meta.drop_on.is_some() {
        return Err(KVAppError::AppError(AppError::DropTableWithDropTime(
            DropTableWithDropTime::new(&dbid_tbname.table_name),
        )));
    }

    tb_meta.drop_on = Some(Utc::now());

    // There must NOT be concurrent txn(b) that list-then-delete tables:
    // Otherwise, (b) may not delete all of the tables, if this txn(a) is operating on some table.
    // We guarantee there is no `(b)` so we do not have to assert db seq.
    txn.condition.extend(vec![
        // assert db_meta seq so that no other txn can delete this db
        txn_cond_seq(&DatabaseId { db_id }, Eq, db_meta_seq),
        // table is not changed
        txn_cond_seq(&tbid, Eq, tb_meta_seq),
    ]);

    txn.if_then.extend(vec![
        // update db_meta seq so that no other txn can delete this db
        txn_op_put(&DatabaseId { db_id }, serialize_struct(&db_meta)?), // (db_id) -> db_meta
        txn_op_put(&tbid, serialize_struct(&tb_meta)?), // (tenant, db_id, tb_id) -> tb_meta
    ]);
    if if_delete {
        // still this table id
        txn.condition
            .push(txn_cond_seq(&dbid_tbname, Eq, tb_id_seq));
        // (db_id, tb_name) -> tb_id
        txn.if_then.push(txn_op_del(&dbid_tbname));
    }

    // add TableIdListKey if not exist
    if if_delete {
        // get table id list from _fd_table_id_list/db_id/table_name
        let dbid_tbname_idlist = TableIdHistoryIdent {
            database_id: db_id,
            table_name: dbid_tbname.table_name.clone(),
        };
        let (tb_id_list_seq, _tb_id_list_opt): (_, Option<TableIdList>) =
            get_pb_value(kv_api, &dbid_tbname_idlist).await?;
        if tb_id_list_seq == 0 {
            let mut tb_id_list = TableIdList::new();
            tb_id_list.append(table_id);

            warn!(
                "drop table:{:?}, table_id:{:?} has no TableIdList",
                dbid_tbname, table_id
            );

            txn.condition
                .push(txn_cond_seq(&dbid_tbname_idlist, Eq, tb_id_list_seq));
            txn.if_then.push(txn_op_put(
                &dbid_tbname_idlist,
                serialize_struct(&tb_id_list)?,
            ));
        }
    }
    Ok((tb_id_seq, table_id))
}

async fn drop_database_meta(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    tenant_dbname: &DatabaseNameIdent,
    if_exists: bool,
    drop_name_key: bool,
    txn: &mut TxnRequest,
) -> Result<u64, KVAppError> {
    let res = get_db_or_err(
        kv_api,
        tenant_dbname,
        format!("drop_database: {}", tenant_dbname.display()),
    )
    .await;

    let (seq_db_id, mut db_meta) = match res {
        Ok(x) => x,
        Err(e) => {
            if let KVAppError::AppError(AppError::UnknownDatabase(_)) = e {
                if if_exists {
                    return Ok(0);
                }
            }

            return Err(e);
        }
    };

    // remove db_name -> db id
    if drop_name_key {
        txn.condition
            .push(txn_cond_seq(tenant_dbname, Eq, seq_db_id.seq));
        txn.if_then.push(txn_op_del(tenant_dbname)); // (tenant, db_name) -> db_id
    }

    // Delete db by these operations:
    // del (tenant, db_name) -> db_id
    // set db_meta.drop_on = now and update (db_id) -> db_meta

    let db_id_key = seq_db_id.data;

    debug!(
        seq_db_id :? = seq_db_id,
        name_key :? =(tenant_dbname);
        "drop_database"
    );

    {
        // drop a table with drop time
        if db_meta.drop_on.is_some() {
            return Err(KVAppError::AppError(AppError::DropDbWithDropTime(
                DropDbWithDropTime::new(tenant_dbname.database_name()),
            )));
        }
        // update drop on time
        db_meta.drop_on = Some(Utc::now());

        txn.condition
            .push(txn_cond_seq(&db_id_key, Eq, db_meta.seq));

        txn.if_then
            .push(txn_op_put(&db_id_key, serialize_struct(&*db_meta)?)); // (db_id) -> db_meta
    }

    // add DbIdListKey if not exists
    let dbid_idlist =
        DatabaseIdHistoryIdent::new(tenant_dbname.tenant(), tenant_dbname.database_name());
    let (db_id_list_seq, db_id_list_opt): (_, Option<DbIdList>) =
        get_pb_value(kv_api, &dbid_idlist).await?;

    if db_id_list_seq == 0 || db_id_list_opt.is_none() {
        warn!(
            "drop db:{:?}, seq_db_id:{:?} has no DbIdListKey",
            tenant_dbname, seq_db_id
        );

        let mut db_id_list = DbIdList::new();
        db_id_list.append(*seq_db_id.data);

        txn.condition
            .push(txn_cond_seq(&dbid_idlist, Eq, db_id_list_seq));
        // _fd_db_id_list/<tenant>/<db_name> -> db_id_list
        txn.if_then
            .push(txn_op_put(&dbid_idlist, serialize_struct(&db_id_list)?));
    };

    Ok(*seq_db_id.data)
}

/// remove copied files for a table.
///
/// Returns number of files that are going to be removed.
async fn remove_table_copied_files(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    table_id: u64,
    txn: &mut TxnRequest,
) -> Result<usize, KVAppError> {
    let mut n = 0;
    let chunk_size = DEFAULT_MGET_SIZE;

    // `list_keys` list all the `TableCopiedFileNameIdent` of the table.
    // But if a upsert_table_copied_file_info run concurrently, there is chance that
    // `list_keys` may lack of some new inserted TableCopiedFileNameIdent.
    // But since TableCopiedFileNameIdent has expire time, they can be purged by expire time.
    let copied_files = list_table_copied_files(kv_api, table_id).await?;

    for chunk in copied_files.chunks(chunk_size) {
        // Load the `seq` of every copied file
        let seqs = {
            let str_keys: Vec<_> = chunk.iter().map(|f| f.to_string_key()).collect();

            let seq_infos: Vec<(u64, Option<TableCopiedFileInfo>)> =
                mget_pb_values(kv_api, &str_keys).await?;

            seq_infos.into_iter().map(|(seq, _)| seq)
        };

        for (copied_seq, copied_ident) in seqs.zip(chunk) {
            if copied_seq == 0 {
                continue;
            }

            txn.condition
                .push(txn_cond_seq(copied_ident, Eq, copied_seq));
            txn.if_then.push(txn_op_del(copied_ident));
            n += 1;
        }
    }

    Ok(n)
}

/// List the copied file identities belonging to a table.
async fn list_table_copied_files(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    table_id: u64,
) -> Result<Vec<TableCopiedFileNameIdent>, MetaError> {
    let copied_file_ident = TableCopiedFileNameIdent {
        table_id,
        file: "dummy".to_string(),
    };

    let dir_name = DirName::new(copied_file_ident);

    let copied_files = list_keys(kv_api, &dir_name).await?;

    Ok(copied_files)
}

// Return true if drop time is out of `DATA_RETENTION_TIME_IN_DAYS option,
// use DEFAULT_DATA_RETENTION_SECONDS by default.
fn is_drop_time_out_of_retention_time(
    drop_on: &Option<DateTime<Utc>>,
    now: &DateTime<Utc>,
) -> bool {
    if let Some(drop_on) = drop_on {
        return now.timestamp() - drop_on.timestamp() >= DEFAULT_DATA_RETENTION_SECONDS;
    }

    false
}

/// Get db id and its seq by name, returns (db_id_seq, db_id)
///
/// If the db does not exist, returns AppError::UnknownDatabase
pub(crate) async fn get_db_id_or_err(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name_key: &DatabaseNameIdent,
    msg: impl Display,
) -> Result<SeqV<DatabaseId>, KVAppError> {
    let seq_db_id = kv_api.get_pb(name_key).await?;

    let seq_db_id = seq_db_id.ok_or_else(|| unknown_database_error(name_key, msg))?;

    Ok(seq_db_id.map(|x| x.into_inner()))
}

/// Returns (db_id_seq, db_id, db_meta_seq, db_meta)
pub(crate) async fn get_db_or_err(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    name_key: &DatabaseNameIdent,
    msg: impl Display,
) -> Result<(SeqV<DatabaseId>, SeqV<DatabaseMeta>), KVAppError> {
    let seq_db_id = kv_api.get_pb(name_key).await?;
    let seq_db_id = seq_db_id.ok_or_else(|| unknown_database_error(name_key, &msg))?;

    let id_key = seq_db_id.data.into_inner();

    let seq_db_meta = kv_api.get_pb(&id_key).await?;
    let seq_db_meta = seq_db_meta.ok_or_else(|| unknown_database_error(name_key, &msg))?;

    Ok((seq_db_id.map(|x| x.into_inner()), seq_db_meta))
}

/// Returns (db_meta_seq, db_meta)
pub(crate) async fn get_db_by_id_or_err(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    db_id: u64,
    msg: impl Display,
) -> Result<(u64, DatabaseMeta), KVAppError> {
    let id_key = DatabaseId { db_id };

    let (db_meta_seq, db_meta) = get_pb_value(kv_api, &id_key).await?;
    db_id_has_to_exist(db_meta_seq, db_id, msg)?;

    Ok((
        db_meta_seq,
        // Safe unwrap(): db_meta_seq > 0 implies db_meta is not None.
        db_meta.unwrap(),
    ))
}

/// Return OK if a db_id or db_meta does not exist by checking the seq.
///
/// Otherwise returns DatabaseAlreadyExists error
fn db_has_to_not_exist(
    seq: u64,
    name_ident: &DatabaseNameIdent,
    ctx: impl Display,
) -> Result<(), KVAppError> {
    if seq == 0 {
        Ok(())
    } else {
        debug!(seq = seq, name_ident :? =(name_ident); "exist");

        Err(KVAppError::AppError(AppError::DatabaseAlreadyExists(
            DatabaseAlreadyExists::new(
                name_ident.database_name(),
                format!("{}: {}", ctx, name_ident.display()),
            ),
        )))
    }
}

/// Return OK if a table_id or table_meta does not exist by checking the seq.
///
/// Otherwise returns TableAlreadyExists error
fn table_has_to_not_exist(
    seq: u64,
    name_ident: &TableNameIdent,
    ctx: impl Display,
) -> Result<(), KVAppError> {
    if seq == 0 {
        Ok(())
    } else {
        debug!(seq = seq, name_ident :? =(name_ident); "exist");

        Err(KVAppError::AppError(AppError::TableAlreadyExists(
            TableAlreadyExists::new(&name_ident.table_name, format!("{}: {}", ctx, name_ident)),
        )))
    }
}

fn build_upsert_table_copied_file_info_conditions(
    table_id: &TableId,
    req: &UpsertTableCopiedFileReq,
    tb_meta_seq: u64,
    fail_if_duplicated: bool,
) -> Result<(Vec<TxnCondition>, Vec<TxnOp>), KVAppError> {
    let mut condition = vec![txn_cond_seq(table_id, Eq, tb_meta_seq)];
    let mut if_then = vec![];

    // `remove_table_copied_files` and `upsert_table_copied_file_info`
    // all modify `TableCopiedFileInfo`,
    // so there used to has `TableCopiedFileLockKey` in these two functions
    // to protect TableCopiedFileInfo modification.
    // In issue: https://github.com/datafuselabs/databend/issues/8897,
    // there is chance that if copy files concurrently, `upsert_table_copied_file_info`
    // may return `TxnRetryMaxTimes`.
    // So now, in case that `TableCopiedFileInfo` has expire time, remove `TableCopiedFileLockKey`
    // in each function. In this case there is chance that some `TableCopiedFileInfo` may not be
    // removed in `remove_table_copied_files`, but these data can be purged in case of expire time.

    let file_name_infos = req.file_info.clone().into_iter();

    for (file_name, file_info) in file_name_infos {
        let key = TableCopiedFileNameIdent {
            table_id: table_id.table_id,
            file: file_name.to_owned(),
        };
        if fail_if_duplicated {
            // "fail_if_duplicated" mode, assumes files are absent
            condition.push(txn_cond_seq(&key, Eq, 0));
        }
        if_then.push(TxnOp::put_with_ttl(
            key.to_string_key(),
            serialize_struct(&file_info)?,
            req.ttl,
        ))
    }
    Ok((condition, if_then))
}

fn build_upsert_table_deduplicated_label(deduplicated_label: String) -> TxnOp {
    TxnOp::put_with_ttl(
        deduplicated_label,
        1_i8.to_le_bytes().to_vec(),
        Some(Duration::from_secs(86400)),
    )
}

#[logcall::logcall(input = "")]
#[fastrace::trace]
async fn batch_filter_table_info(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    inner_keys: &[String],
    filter_db_info_with_table_name_list: &[(&TableInfoFilter, &Arc<DatabaseInfo>, u64, &String)],
    filter_tb_infos: &mut Vec<(Arc<TableInfo>, u64)>,
) -> Result<(), KVAppError> {
    let tb_meta_vec: Vec<(u64, Option<TableMeta>)> = mget_pb_values(kv_api, inner_keys).await?;
    for (i, (tb_meta_seq, tb_meta)) in tb_meta_vec.iter().enumerate() {
        let (filter, db_info, table_id, table_name) = filter_db_info_with_table_name_list[i];
        if *tb_meta_seq == 0 || tb_meta.is_none() {
            error!("get_table_history cannot find {:?} table_meta", table_id);
            continue;
        }
        // Safe unwrap() because: tb_meta_seq > 0
        let tb_meta = tb_meta.clone().unwrap();

        if let TableInfoFilter::Dropped(drop_on) = filter {
            if let Some(drop_on) = drop_on {
                if let Some(meta_drop_on) = &tb_meta.drop_on {
                    if meta_drop_on.timestamp_millis() >= drop_on.timestamp_millis() {
                        continue;
                    }
                } else {
                    continue;
                }
            } else if tb_meta.drop_on.is_none() {
                continue;
            }
        }

        let db_ident = &db_info.name_ident;

        let tenant_dbname_tbname: TableNameIdent =
            TableNameIdent::new(db_ident.tenant(), db_ident.database_name(), table_name);

        let tb_info = TableInfo {
            ident: TableIdent {
                table_id,
                seq: *tb_meta_seq,
            },
            desc: format!(
                "'{}'.'{}'",
                db_ident.database_name(),
                tenant_dbname_tbname.table_name
            ),
            name: table_name.clone(),
            meta: tb_meta,
            tenant: db_ident.tenant_name().to_string(),
            db_type: DatabaseType::NormalDB,
            catalog_info: Default::default(),
        };

        filter_tb_infos.push((Arc::new(tb_info), db_info.database_id.db_id));
    }

    Ok(())
}

type TableFilterInfoList<'a> = Vec<(&'a TableInfoFilter, &'a Arc<DatabaseInfo>, u64, String)>;

#[logcall::logcall(input = "")]
#[fastrace::trace]
async fn get_gc_table_info(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    limit: Option<usize>,
    table_id_list: &TableFilterInfoList<'_>,
) -> Result<Vec<(Arc<TableInfo>, u64)>, KVAppError> {
    let mut filter_tb_infos = vec![];
    let mut inner_keys: Vec<String> = vec![];
    let mut filter_db_info_with_table_name_list: Vec<(
        &TableInfoFilter,
        &Arc<DatabaseInfo>,
        u64,
        &String,
    )> = vec![];

    for (filter, db_info, table_id, table_name) in table_id_list {
        filter_db_info_with_table_name_list.push((filter, db_info, *table_id, table_name));
        inner_keys.push(
            TableId {
                table_id: *table_id,
            }
            .to_string_key(),
        );
        if inner_keys.len() < DEFAULT_MGET_SIZE {
            continue;
        }

        batch_filter_table_info(
            kv_api,
            &inner_keys,
            &filter_db_info_with_table_name_list,
            &mut filter_tb_infos,
        )
        .await?;

        inner_keys.clear();
        filter_db_info_with_table_name_list.clear();

        // check if reach the limit
        if let Some(limit) = limit {
            if filter_tb_infos.len() >= limit {
                return Ok(filter_tb_infos);
            }
        }
    }

    if !inner_keys.is_empty() {
        batch_filter_table_info(
            kv_api,
            &inner_keys,
            &filter_db_info_with_table_name_list,
            &mut filter_tb_infos,
        )
        .await?;
    }

    Ok(filter_tb_infos)
}

#[logcall::logcall(input = "")]
#[fastrace::trace]
async fn do_get_table_history(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    db_filter: (TableInfoFilter, Arc<DatabaseInfo>),
    limit: Option<usize>,
) -> Result<Vec<(Arc<TableInfo>, u64)>, KVAppError> {
    let mut filter_tb_infos = vec![];

    // step 1: list db table name with db id
    let mut filter_db_info_with_table_id_key_list: Vec<(
        &TableInfoFilter,
        &Arc<DatabaseInfo>,
        TableIdHistoryIdent,
    )> = vec![];
    let (filter, db_info) = db_filter;
    let db_id = db_info.database_id.db_id;

    // List tables by tenant, db_id, table_name.
    let dbid_tbname_idlist = TableIdHistoryIdent {
        database_id: db_id,
        table_name: "dummy".to_string(),
    };

    let dir_name = DirName::new(dbid_tbname_idlist);

    let table_id_list_keys = list_keys(kv_api, &dir_name).await?;

    let keys: Vec<(&TableInfoFilter, &Arc<DatabaseInfo>, TableIdHistoryIdent)> = table_id_list_keys
        .iter()
        .map(|table_id_list_key| (&filter, &db_info, table_id_list_key.clone()))
        .collect();

    filter_db_info_with_table_id_key_list.extend(keys);

    // step 2: list all table id of table by table name
    let keys: Vec<String> = filter_db_info_with_table_id_key_list
        .iter()
        .map(|(_, db_info, table_id_list_key)| {
            TableIdHistoryIdent {
                database_id: db_info.database_id.db_id,
                table_name: table_id_list_key.table_name.clone(),
            }
            .to_string_key()
        })
        .collect();
    let mut filter_db_info_with_table_id_list: TableFilterInfoList<'_> = vec![];
    let mut table_id_list_keys_iter = filter_db_info_with_table_id_key_list.into_iter();
    for c in keys.chunks(DEFAULT_MGET_SIZE) {
        let tb_id_list_seq_vec: Vec<(u64, Option<TableIdList>)> = mget_pb_values(kv_api, c).await?;
        for (tb_id_list_seq, tb_id_list_opt) in tb_id_list_seq_vec {
            let (filter, db_info, table_id_list_key) = table_id_list_keys_iter.next().unwrap();
            let tb_id_list = if tb_id_list_seq == 0 {
                continue;
            } else {
                match tb_id_list_opt {
                    Some(list) => list,
                    None => {
                        continue;
                    }
                }
            };

            let id_list: Vec<(&TableInfoFilter, &Arc<DatabaseInfo>, u64, String)> = tb_id_list
                .id_list
                .iter()
                .map(|id| (filter, db_info, *id, table_id_list_key.table_name.clone()))
                .collect();

            filter_db_info_with_table_id_list.extend(id_list);
            if filter_db_info_with_table_id_list.len() < DEFAULT_MGET_SIZE {
                continue;
            }

            let ret = get_gc_table_info(kv_api, limit, &filter_db_info_with_table_id_list).await?;
            filter_tb_infos.extend(ret);
            filter_db_info_with_table_id_list.clear();

            // check if reach the limit
            if let Some(limit) = limit {
                if filter_tb_infos.len() >= limit {
                    return Ok(filter_tb_infos);
                }
            }
        }

        if !filter_db_info_with_table_id_list.is_empty() {
            let ret = get_gc_table_info(kv_api, limit, &filter_db_info_with_table_id_list).await?;
            filter_tb_infos.extend(ret);
            filter_db_info_with_table_id_list.clear();

            // check if reach the limit
            if let Some(limit) = limit {
                if filter_tb_infos.len() >= limit {
                    return Ok(filter_tb_infos);
                }
            }
        }
    }

    Ok(filter_tb_infos)
}

async fn gc_dropped_db_by_id(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    db_id: u64,
    tenant: &Tenant,
    db_name: String,
) -> Result<(), KVAppError> {
    // List tables by tenant, db_id, table_name.
    let dbid_idlist = DatabaseIdHistoryIdent::new(tenant, db_name);
    let (db_id_list_seq, db_id_list_opt): (_, Option<DbIdList>) =
        get_pb_value(kv_api, &dbid_idlist).await?;

    let mut db_id_list = match db_id_list_opt {
        Some(list) => list,
        None => return Ok(()),
    };
    for (i, dbid) in db_id_list.id_list.iter().enumerate() {
        if *dbid != db_id {
            continue;
        }
        let dbid = DatabaseId { db_id };
        let (db_meta_seq, _db_meta): (_, Option<DatabaseMeta>) =
            get_pb_value(kv_api, &dbid).await?;
        if db_meta_seq == 0 {
            return Ok(());
        }
        let id_to_name = DatabaseIdToName { db_id };
        let (name_ident_seq, _name_ident): (_, Option<DatabaseNameIdentRaw>) =
            get_pb_value(kv_api, &id_to_name).await?;
        if name_ident_seq == 0 {
            return Ok(());
        }

        let dbid_tbname_idlist = TableIdHistoryIdent {
            database_id: db_id,
            table_name: "".to_string(),
        };

        let dir_name = DirName::new(dbid_tbname_idlist);

        let table_id_list_keys = list_keys(kv_api, &dir_name).await?;
        let keys: Vec<String> = table_id_list_keys
            .iter()
            .map(|table_id_list_key| {
                TableIdHistoryIdent {
                    database_id: db_id,
                    table_name: table_id_list_key.table_name.clone(),
                }
                .to_string_key()
            })
            .collect();

        let mut txn = TxnRequest::default();

        for c in keys.chunks(DEFAULT_MGET_SIZE) {
            let tb_id_list_seq_vec: Vec<(u64, Option<TableIdList>)> =
                mget_pb_values(kv_api, c).await?;
            let mut iter = c.iter();
            for (tb_id_list_seq, tb_id_list_opt) in tb_id_list_seq_vec {
                let tb_id_list = match tb_id_list_opt {
                    Some(list) => list,
                    None => {
                        continue;
                    }
                };

                for tb_id in tb_id_list.id_list {
                    gc_dropped_table_data(kv_api, tb_id, &mut txn).await?;
                    gc_dropped_table_index(kv_api, tenant, tb_id, &mut txn).await?;
                }

                let id_key = iter.next().unwrap();
                txn.if_then.push(TxnOp::delete(id_key));
                txn.condition
                    .push(TxnCondition::eq_seq(id_key, tb_id_list_seq));
            }

            // for id_key in c {
            // if_then.push(txn_op_del(id_key));
            // }
        }
        db_id_list.id_list.remove(i);
        txn.condition
            .push(txn_cond_seq(&dbid_idlist, Eq, db_id_list_seq));
        if db_id_list.id_list.is_empty() {
            txn.if_then.push(txn_op_del(&dbid_idlist));
        } else {
            // save new db id list
            txn.if_then
                .push(txn_op_put(&dbid_idlist, serialize_struct(&db_id_list)?));
        }

        txn.condition.push(txn_cond_seq(&dbid, Eq, db_meta_seq));
        txn.if_then.push(txn_op_del(&dbid));
        txn.condition
            .push(txn_cond_seq(&id_to_name, Eq, name_ident_seq));
        txn.if_then.push(txn_op_del(&id_to_name));

        let _resp = kv_api.transaction(txn).await?;
        break;
    }

    Ok(())
}

async fn gc_dropped_table_by_id(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    tenant: &Tenant,
    db_id: u64,
    table_id: u64,
    table_name: String,
) -> Result<(), KVAppError> {
    // first get TableIdList
    let dbid_tbname_idlist = TableIdHistoryIdent {
        database_id: db_id,
        table_name,
    };
    let (tb_id_list_seq, tb_id_list_opt): (_, Option<TableIdList>) =
        get_pb_value(kv_api, &dbid_tbname_idlist).await?;
    let mut tb_id_list = match tb_id_list_opt {
        Some(list) => list,
        None => return Ok(()),
    };

    for (i, tb_id) in tb_id_list.id_list.iter().enumerate() {
        if *tb_id != table_id {
            continue;
        }

        tb_id_list.id_list.remove(i);

        let mut txn = TxnRequest::default();

        // construct the txn request
        txn.condition.push(
            // condition: table id list not changed
            txn_cond_seq(&dbid_tbname_idlist, Eq, tb_id_list_seq),
        );

        if tb_id_list.id_list.is_empty() {
            txn.if_then.push(txn_op_del(&dbid_tbname_idlist));
        } else {
            // save new table id list
            txn.if_then.push(txn_op_put(
                &dbid_tbname_idlist,
                serialize_struct(&tb_id_list)?,
            ));
        }
        gc_dropped_table_data(kv_api, table_id, &mut txn).await?;
        gc_dropped_table_index(kv_api, tenant, table_id, &mut txn).await?;

        let _resp = kv_api.transaction(txn).await?;
        break;
    }

    Ok(())
}

async fn gc_dropped_table_data(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    table_id: u64,
    txn: &mut TxnRequest,
) -> Result<(), KVAppError> {
    let tbid = TableId { table_id };
    let id_to_name = TableIdToName { table_id };

    // Get meta data
    let (tb_meta_seq, tb_meta): (_, Option<TableMeta>) = get_pb_value(kv_api, &tbid).await?;

    if tb_meta_seq == 0 || tb_meta.is_none() {
        error!(
            "gc_dropped_table_by_id cannot find {:?} table_meta",
            table_id
        );
        return Ok(());
    }

    // Get id -> name mapping
    let (name_seq, _name): (_, Option<DBIdTableName>) = get_pb_value(kv_api, &id_to_name).await?;

    // table id not changed
    txn.condition.push(txn_cond_seq(&tbid, Eq, tb_meta_seq));
    // consider only when TableIdToName exist
    if name_seq != 0 {
        // table id to name not changed
        txn.condition.push(txn_cond_seq(&id_to_name, Eq, name_seq));
        // remove table id to name
        txn.if_then.push(txn_op_del(&id_to_name));
    }
    // remove table meta
    txn.if_then.push(txn_op_del(&tbid));

    remove_table_copied_files(kv_api, table_id, txn).await?;

    Ok(())
}

async fn gc_dropped_table_index(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    tenant: &Tenant,
    table_id: u64,
    txn: &mut TxnRequest,
) -> Result<(), KVAppError> {
    let name_id_metas = kv_api
        .list_indexes(ListIndexesReq {
            tenant: tenant.clone(),
            table_id: Some(table_id),
        })
        .await?;

    for (name, index_id, _) in name_id_metas {
        let name_ident = IndexNameIdent::new_generic(tenant, name);
        let id_ident = IndexIdIdent::new_generic(tenant, index_id);
        let id_to_name_ident = IndexIdToNameIdent::new_generic(tenant, index_id);

        txn.if_then.push(txn_op_del(&name_ident)); // (tenant, index_name) -> index_id
        txn.if_then.push(txn_op_del(&id_ident)); // (index_id) -> index_meta
        txn.if_then.push(txn_op_del(&id_to_name_ident)); // __fd_index_id_to_name/<index_id> -> (tenant,index_name)
    }

    Ok(())
}

async fn update_mask_policy(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    action: &SetTableColumnMaskPolicyAction,
    txn_req: &mut TxnRequest,
    tenant: &Tenant,
    table_id: u64,
) -> Result<(), KVAppError> {
    /// Fetch and update the table id list with `f`, and fill in the txn preconditions and operations.
    async fn update_table_ids(
        kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
        txn_req: &mut TxnRequest,
        key: MaskPolicyTableIdListIdent,
        f: impl FnOnce(&mut BTreeSet<u64>),
    ) -> Result<(), KVAppError> {
        let (id_list_seq, id_list_opt): (_, Option<MaskpolicyTableIdList>) =
            get_pb_value(kv_api, &key).await?;

        if let Some(mut id_list) = id_list_opt {
            f(&mut id_list.id_list);

            txn_req.condition.push(txn_cond_seq(&key, Eq, id_list_seq));

            txn_req
                .if_then
                .push(txn_op_put(&key, serialize_struct(&id_list)?));
        }

        Ok(())
    }

    match action {
        SetTableColumnMaskPolicyAction::Set(new_mask_name, old_mask_name_opt) => {
            update_table_ids(
                kv_api,
                txn_req,
                MaskPolicyTableIdListIdent::new(tenant.clone(), new_mask_name),
                |list: &mut BTreeSet<u64>| {
                    list.insert(table_id);
                },
            )
            .await?;

            if let Some(old) = old_mask_name_opt {
                update_table_ids(
                    kv_api,
                    txn_req,
                    MaskPolicyTableIdListIdent::new(tenant.clone(), old),
                    |list: &mut BTreeSet<u64>| {
                        list.remove(&table_id);
                    },
                )
                .await?;
            }
        }
        SetTableColumnMaskPolicyAction::Unset(mask_name) => {
            update_table_ids(
                kv_api,
                txn_req,
                MaskPolicyTableIdListIdent::new(tenant.clone(), mask_name),
                |list: &mut BTreeSet<u64>| {
                    list.remove(&table_id);
                },
            )
            .await?;
        }
    }

    Ok(())
}

#[tonic::async_trait]
pub(crate) trait UndropTableStrategy {
    fn table_name_ident(&self) -> &TableNameIdent;

    // Determines whether replacing an existing table with the same name is allowed.
    fn force_replace(&self) -> bool;

    async fn refresh_target_db_meta<'a>(
        &'a self,
        kv_api: &'a (impl kvapi::KVApi<Error = MetaError> + ?Sized),
    ) -> Result<(u64, SeqV<DatabaseMeta>), KVAppError>;

    fn extract_and_validate_table_id(
        &self,
        tb_id_list: &mut TableIdList,
    ) -> Result<u64, KVAppError>;
}

#[tonic::async_trait]
impl UndropTableStrategy for UndropTableReq {
    fn table_name_ident(&self) -> &TableNameIdent {
        &self.name_ident
    }
    fn force_replace(&self) -> bool {
        false
    }
    async fn refresh_target_db_meta<'a>(
        &'a self,
        kv_api: &'a (impl kvapi::KVApi<Error = MetaError> + ?Sized),
    ) -> Result<(u64, SeqV<DatabaseMeta>), KVAppError> {
        // for plain un-drop table (by name), database meta is refreshed by name
        let (seq_db_id, db_meta) =
            get_db_or_err(kv_api, &self.name_ident.db_name_ident(), "undrop_table").await?;
        Ok((*seq_db_id.data, db_meta))
    }

    fn extract_and_validate_table_id(
        &self,
        tb_id_list: &mut TableIdList,
    ) -> Result<u64, KVAppError> {
        // for plain un-drop table (by name), the last item of
        // tb_id_list should be used.
        let table_id = match tb_id_list.last() {
            Some(table_id) => *table_id,
            None => {
                return Err(KVAppError::AppError(AppError::UndropTableHasNoHistory(
                    UndropTableHasNoHistory::new(&self.name_ident.table_name),
                )));
            }
        };
        Ok(table_id)
    }
}

#[tonic::async_trait]
impl UndropTableStrategy for UndropTableByIdReq {
    fn table_name_ident(&self) -> &TableNameIdent {
        &self.name_ident
    }

    fn force_replace(&self) -> bool {
        self.force_replace
    }
    async fn refresh_target_db_meta<'a>(
        &'a self,
        kv_api: &'a (impl kvapi::KVApi<Error = MetaError> + ?Sized),
    ) -> Result<(u64, SeqV<DatabaseMeta>), KVAppError> {
        // for un-drop table by id, database meta is refreshed by database id
        let (db_meta_seq, db_meta) =
            get_db_by_id_or_err(kv_api, self.db_id, "undrop_table_by_id").await?;
        Ok((self.db_id, SeqV::new(db_meta_seq, db_meta)))
    }

    fn extract_and_validate_table_id(
        &self,
        tb_id_list: &mut TableIdList,
    ) -> Result<u64, KVAppError> {
        // for un-drop table by id, assumes that the last item of tb_id_list should
        // be the table id which is requested to be un-dropped.
        let target_table_id = self.table_id;
        match tb_id_list.last() {
            Some(table_id) if *table_id == target_table_id => Ok(target_table_id),
            _ => Err(KVAppError::AppError(AppError::UndropTableHasNoHistory(
                UndropTableHasNoHistory::new(&self.name_ident.table_name),
            ))),
        }
    }
}

async fn handle_undrop_table(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    req: impl UndropTableStrategy + std::fmt::Debug,
) -> Result<UndropTableReply, KVAppError> {
    let tenant_dbname_tbname = req.table_name_ident();

    let mut trials = txn_backoff(None, func_name!());
    loop {
        trials.next().unwrap()?.await;

        // Get db by name to ensure presence

        let (db_id, db_meta) = req.refresh_target_db_meta(kv_api).await?;

        // Get table by tenant,db_id, table_name to assert presence.

        let dbid_tbname = DBIdTableName {
            db_id,
            table_name: tenant_dbname_tbname.table_name.clone(),
        };

        let (dbid_tbname_seq, table_id) = get_u64_value(kv_api, &dbid_tbname).await?;
        if !req.force_replace() {
            // If table id already exists, return error.
            if dbid_tbname_seq > 0 || table_id > 0 {
                return Err(KVAppError::AppError(AppError::UndropTableAlreadyExists(
                    UndropTableAlreadyExists::new(&tenant_dbname_tbname.table_name),
                )));
            }
        }

        // get table id list from _fd_table_id_list/db_id/table_name
        let dbid_tbname_idlist = TableIdHistoryIdent {
            database_id: db_id,
            table_name: tenant_dbname_tbname.table_name.clone(),
        };
        let (tb_id_list_seq, tb_id_list_opt): (_, Option<TableIdList>) =
            get_pb_value(kv_api, &dbid_tbname_idlist).await?;

        let mut tb_id_list = if tb_id_list_seq == 0 {
            return Err(KVAppError::AppError(AppError::UndropTableHasNoHistory(
                UndropTableHasNoHistory::new(&tenant_dbname_tbname.table_name),
            )));
        } else {
            tb_id_list_opt.ok_or_else(|| {
                KVAppError::AppError(AppError::UndropTableHasNoHistory(
                    UndropTableHasNoHistory::new(&tenant_dbname_tbname.table_name),
                ))
            })?
        };

        let table_id = req.extract_and_validate_table_id(&mut tb_id_list)?;

        // get tb_meta of the last table id
        let tbid = TableId { table_id };
        let (tb_meta_seq, tb_meta): (_, Option<TableMeta>) = get_pb_value(kv_api, &tbid).await?;

        debug!(
            ident :% =(&tbid),
            name :% =(tenant_dbname_tbname);
            "undrop table"
        );

        {
            // reset drop on time
            let mut tb_meta = tb_meta.unwrap();
            // undrop a table with no drop_on time
            if tb_meta.drop_on.is_none() {
                return Err(KVAppError::AppError(AppError::UndropTableWithNoDropTime(
                    UndropTableWithNoDropTime::new(&tenant_dbname_tbname.table_name),
                )));
            }
            tb_meta.drop_on = None;

            let txn_req = TxnRequest {
                condition: vec![
                    // db has not to change, i.e., no new table is created.
                    // Renaming db is OK and does not affect the seq of db_meta.
                    txn_cond_seq(&DatabaseId { db_id }, Eq, db_meta.seq),
                    // still this table id
                    txn_cond_seq(&dbid_tbname, Eq, dbid_tbname_seq),
                    // table is not changed
                    txn_cond_seq(&tbid, Eq, tb_meta_seq),
                ],
                if_then: vec![
                    // Changing a table in a db has to update the seq of db_meta,
                    // to block the batch-delete-tables when deleting a db.
                    txn_op_put(&DatabaseId { db_id }, serialize_struct(&*db_meta)?), /* (db_id) -> db_meta */
                    txn_op_put(&dbid_tbname, serialize_u64(table_id)?), /* (tenant, db_id, tb_name) -> tb_id */
                    // txn_op_put(&dbid_tbname_idlist, serialize_struct(&tb_id_list)?)?, // _fd_table_id_list/db_id/table_name -> tb_id_list
                    txn_op_put(&tbid, serialize_struct(&tb_meta)?), /* (tenant, db_id, tb_id) -> tb_meta */
                ],
                else_then: vec![],
            };

            let (succ, _responses) = send_txn(kv_api, txn_req).await?;

            debug!(
                name :? =(tenant_dbname_tbname),
                id :? =(&tbid),
                succ = succ;
                "undrop_table"
            );

            if succ {
                return Ok(UndropTableReply {});
            }
        }
    }
}

#[fastrace::trace]
async fn append_update_stream_meta_requests(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    txn_req: &mut TxnRequest,
    update_stream_meta: &[UpdateStreamMetaReq],
    context_msg: impl Into<String>,
) -> Result<(), KVAppError> {
    for req in update_stream_meta {
        let stream_id = TableId {
            table_id: req.stream_id,
        };
        let (stream_meta_seq, stream_meta): (_, Option<TableMeta>) =
            get_pb_value(kv_api, &stream_id).await?;

        if stream_meta_seq == 0 || stream_meta.is_none() {
            return Err(KVAppError::AppError(AppError::UnknownStreamId(
                UnknownStreamId::new(req.stream_id, context_msg),
            )));
        }

        if req.seq.match_seq(stream_meta_seq).is_err() {
            return Err(KVAppError::AppError(AppError::from(
                StreamVersionMismatched::new(req.stream_id, req.seq, stream_meta_seq, context_msg),
            )));
        }

        let mut new_stream_meta = stream_meta.unwrap();
        new_stream_meta.options = req.options.clone();
        new_stream_meta.updated_on = Utc::now();

        txn_req
            .condition
            .push(txn_cond_seq(&stream_id, Eq, stream_meta_seq));
        txn_req
            .if_then
            .push(txn_op_put(&stream_id, serialize_struct(&new_stream_meta)?));
    }
    Ok(())
}
