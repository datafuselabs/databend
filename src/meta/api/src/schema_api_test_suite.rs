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

use std::assert_ne;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::vec;

use chrono::DateTime;
use chrono::Duration;
use chrono::Utc;
use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::TrySpawn;
use databend_common_exception::ErrorCode;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_meta_app::app_error::AppError;
use databend_common_meta_app::data_mask::CreateDatamaskReq;
use databend_common_meta_app::data_mask::DataMaskIdIdent;
use databend_common_meta_app::data_mask::DataMaskNameIdent;
use databend_common_meta_app::data_mask::DatamaskMeta;
use databend_common_meta_app::data_mask::DropDatamaskReq;
use databend_common_meta_app::data_mask::MaskPolicyTableIdListIdent;
use databend_common_meta_app::data_mask::MaskpolicyTableIdList;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdent;
use databend_common_meta_app::schema::database_name_ident::DatabaseNameIdentRaw;
use databend_common_meta_app::schema::CatalogMeta;
use databend_common_meta_app::schema::CatalogNameIdent;
use databend_common_meta_app::schema::CatalogOption;
use databend_common_meta_app::schema::CommitTableMetaReq;
use databend_common_meta_app::schema::CreateCatalogReq;
use databend_common_meta_app::schema::CreateDatabaseReply;
use databend_common_meta_app::schema::CreateDatabaseReq;
use databend_common_meta_app::schema::CreateDictionaryReq;
use databend_common_meta_app::schema::CreateIndexReq;
use databend_common_meta_app::schema::CreateLockRevReq;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateSequenceReq;
use databend_common_meta_app::schema::CreateTableIndexReq;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::CreateVirtualColumnReq;
use databend_common_meta_app::schema::DBIdDictionaryName;
use databend_common_meta_app::schema::DBIdTableName;
use databend_common_meta_app::schema::DatabaseId;
use databend_common_meta_app::schema::DatabaseIdHistoryIdent;
use databend_common_meta_app::schema::DatabaseIdToName;
use databend_common_meta_app::schema::DatabaseInfo;
use databend_common_meta_app::schema::DatabaseMeta;
use databend_common_meta_app::schema::DbIdList;
use databend_common_meta_app::schema::DeleteLockRevReq;
use databend_common_meta_app::schema::DictionaryMeta;
use databend_common_meta_app::schema::DictionaryNameIdent;
use databend_common_meta_app::schema::DropCatalogReq;
use databend_common_meta_app::schema::DropDatabaseReq;
use databend_common_meta_app::schema::DropDictionaryReq;
use databend_common_meta_app::schema::DropIndexReq;
use databend_common_meta_app::schema::DropSequenceReq;
use databend_common_meta_app::schema::DropTableByIdReq;
use databend_common_meta_app::schema::DropTableIndexReq;
use databend_common_meta_app::schema::DropVirtualColumnReq;
use databend_common_meta_app::schema::DroppedId;
use databend_common_meta_app::schema::ExtendLockRevReq;
use databend_common_meta_app::schema::GcDroppedTableReq;
use databend_common_meta_app::schema::GetCatalogReq;
use databend_common_meta_app::schema::GetDatabaseReq;
use databend_common_meta_app::schema::GetDictionaryReq;
use databend_common_meta_app::schema::GetIndexReq;
use databend_common_meta_app::schema::GetLVTReq;
use databend_common_meta_app::schema::GetSequenceNextValueReq;
use databend_common_meta_app::schema::GetSequenceReq;
use databend_common_meta_app::schema::GetTableCopiedFileReq;
use databend_common_meta_app::schema::GetTableReq;
use databend_common_meta_app::schema::IcebergCatalogOption;
use databend_common_meta_app::schema::IcebergRestCatalogOption;
use databend_common_meta_app::schema::IndexId;
use databend_common_meta_app::schema::IndexIdToName;
use databend_common_meta_app::schema::IndexMeta;
use databend_common_meta_app::schema::IndexNameIdent;
use databend_common_meta_app::schema::IndexNameIdentRaw;
use databend_common_meta_app::schema::IndexType;
use databend_common_meta_app::schema::ListCatalogReq;
use databend_common_meta_app::schema::ListDatabaseReq;
use databend_common_meta_app::schema::ListDictionaryReq;
use databend_common_meta_app::schema::ListDroppedTableReq;
use databend_common_meta_app::schema::ListIndexesByIdReq;
use databend_common_meta_app::schema::ListIndexesReq;
use databend_common_meta_app::schema::ListLockRevReq;
use databend_common_meta_app::schema::ListTableReq;
use databend_common_meta_app::schema::ListVirtualColumnsReq;
use databend_common_meta_app::schema::LockKey;
use databend_common_meta_app::schema::RenameDatabaseReq;
use databend_common_meta_app::schema::RenameTableReq;
use databend_common_meta_app::schema::SequenceIdent;
use databend_common_meta_app::schema::SetLVTReq;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyAction;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyReq;
use databend_common_meta_app::schema::TableCopiedFileInfo;
use databend_common_meta_app::schema::TableCopiedFileNameIdent;
use databend_common_meta_app::schema::TableId;
use databend_common_meta_app::schema::TableIdHistoryIdent;
use databend_common_meta_app::schema::TableIdList;
use databend_common_meta_app::schema::TableIdToName;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableInfoFilter;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_meta_app::schema::TableStatistics;
use databend_common_meta_app::schema::TruncateTableReq;
use databend_common_meta_app::schema::UndropDatabaseReq;
use databend_common_meta_app::schema::UndropTableReq;
use databend_common_meta_app::schema::UpdateMultiTableMetaReq;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_meta_app::schema::UpdateVirtualColumnReq;
use databend_common_meta_app::schema::UpsertTableCopiedFileReq;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_meta_app::schema::VirtualColumnIdent;
use databend_common_meta_app::share::share_name_ident::ShareNameIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_app::tenant::ToTenant;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_kvapi::kvapi::Key;
use databend_common_meta_kvapi::kvapi::UpsertKVReq;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MetaError;
use databend_common_meta_types::Operation;
use databend_common_meta_types::UpsertKV;
use fastrace::func_name;
use log::debug;
use log::info;

use crate::deserialize_struct;
use crate::is_all_db_data_removed;
use crate::kv_app_error::KVAppError;
use crate::serialize_struct;
use crate::testing::get_kv_data;
use crate::testing::get_kv_u64_data;
use crate::DatamaskApi;
use crate::SchemaApi;
use crate::SequenceApi;
use crate::ShareApi;
use crate::DEFAULT_MGET_SIZE;

/// Test suite of `SchemaApi`.
///
/// It is not used by this crate, but is used by other crate that impl `SchemaApi`,
/// to ensure an impl works as expected,
/// such as `meta/embedded` and `metasrv`.
#[derive(Copy, Clone)]
pub struct SchemaApiTestSuite {}

#[derive(PartialEq, Default, Debug)]
struct DroponInfo {
    pub name: String,
    pub desc: String,
    pub drop_on_cnt: i32,
    pub non_drop_on_cnt: i32,
}

macro_rules! assert_meta_eq_without_updated {
    ($a: expr, $b: expr, $msg: expr) => {
        let mut aa = $a.clone();
        aa.meta.updated_on = $b.meta.updated_on;
        assert_eq!(aa, $b, $msg);
    };
}

fn calc_and_compare_drop_on_db_result(result: Vec<Arc<DatabaseInfo>>, expected: Vec<DroponInfo>) {
    let mut expected_map = BTreeMap::new();
    for expected_item in expected {
        expected_map.insert(expected_item.name.clone(), expected_item);
    }

    let mut get = BTreeMap::new();
    for item in result.iter() {
        let name = item.name_ident.to_string_key();
        let drop_on_info = match get.get_mut(&name) {
            Some(drop_on_info) => drop_on_info,
            None => {
                let info = DroponInfo {
                    name: name.clone(),
                    desc: "".to_string(),
                    drop_on_cnt: 0,
                    non_drop_on_cnt: 0,
                };
                get.insert(name.clone(), info);
                get.get_mut(&name).expect("")
            }
        };
        if item.meta.drop_on.is_some() {
            drop_on_info.drop_on_cnt += 1;
        } else {
            drop_on_info.non_drop_on_cnt += 1;
        }
    }

    assert_eq!(get, expected_map);
}

fn calc_and_compare_drop_on_table_result(result: Vec<Arc<TableInfo>>, expected: Vec<DroponInfo>) {
    let mut expected_map = BTreeMap::new();
    for expected_item in expected {
        expected_map.insert(expected_item.name.clone(), expected_item);
    }

    let mut get = BTreeMap::new();
    for item in result.iter() {
        let name = item.name.clone();
        let drop_on_info = match get.get_mut(&name) {
            Some(drop_on_info) => drop_on_info,
            None => {
                let info = DroponInfo {
                    name: item.name.clone(),
                    desc: item.desc.clone(),
                    drop_on_cnt: 0,
                    non_drop_on_cnt: 0,
                };
                get.insert(name.clone(), info);
                get.get_mut(&name).expect("")
            }
        };
        if item.meta.drop_on.is_some() {
            drop_on_info.drop_on_cnt += 1;
        } else {
            drop_on_info.non_drop_on_cnt += 1;
        }
    }

    assert_eq!(get, expected_map);
}

async fn upsert_test_data(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    key: &impl kvapi::Key,
    value: Vec<u8>,
) -> Result<u64, KVAppError> {
    let res = kv_api
        .upsert_kv(UpsertKVReq {
            key: key.to_string_key(),
            seq: MatchSeq::GE(0),
            value: Operation::Update(value),
            value_meta: None,
        })
        .await?;

    let seq_v = res.result.unwrap();
    Ok(seq_v.seq)
}

async fn delete_test_data(
    kv_api: &(impl kvapi::KVApi<Error = MetaError> + ?Sized),
    key: &impl kvapi::Key,
) -> Result<(), KVAppError> {
    let _res = kv_api
        .upsert_kv(UpsertKVReq {
            key: key.to_string_key(),
            seq: MatchSeq::GE(0),
            value: Operation::Delete,
            value_meta: None,
        })
        .await?;

    Ok(())
}

impl SchemaApiTestSuite {
    /// Test SchemaAPI on a single node
    pub async fn test_single_node<B, MT>(b: B) -> anyhow::Result<()>
    where
        B: kvapi::ApiBuilder<MT>,
        MT: ShareApi
            + kvapi::AsKVApi<Error = MetaError>
            + SchemaApi
            + DatamaskApi
            + SequenceApi
            + 'static,
    {
        let suite = SchemaApiTestSuite {};

        suite.database_and_table_rename(&b.build().await).await?;
        suite.database_create_get_drop(&b.build().await).await?;
        suite
            .database_create_from_share_and_drop(&b.build().await)
            .await?;
        suite
            .database_create_get_drop_in_diff_tenant(&b.build().await)
            .await?;
        suite.database_list(&b.build().await).await?;
        suite.database_list_in_diff_tenant(&b.build().await).await?;
        suite.database_rename(&b.build().await).await?;
        suite
            .database_drop_undrop_list_history(&b.build().await)
            .await?;
        suite.table_commit_table_meta(&b.build().await).await?;
        suite.concurrent_commit_table_meta(b.clone()).await?;
        suite
            .database_drop_out_of_retention_time_history(&b.build().await)
            .await?;

        suite.table_create_get_drop(&b.build().await).await?;
        suite
            .table_drop_without_db_id_to_name(&b.build().await)
            .await?;
        suite.list_db_without_db_id_list(&b.build().await).await?;
        suite
            .drop_table_without_table_id_list(&b.build().await)
            .await?;
        suite.table_rename(&b.build().await).await?;
        suite.table_update_meta(&b.build().await).await?;
        suite.table_update_mask_policy(&b.build().await).await?;
        suite.table_upsert_option(&b.build().await).await?;
        suite.table_list(&b.build().await).await?;
        suite.table_list_many(&b.build().await).await?;
        suite
            .table_drop_undrop_list_history(&b.build().await)
            .await?;
        suite
            .database_gc_out_of_retention_time(&b.build().await)
            .await?;
        suite
            .table_gc_out_of_retention_time(&b.build().await)
            .await?;
        suite
            .db_table_gc_out_of_retention_time(&b.build().await)
            .await?;
        suite
            .table_drop_out_of_retention_time_history(&b.build().await)
            .await?;
        suite.table_history_filter(&b.build().await).await?;
        suite
            .table_history_filter_with_limit(&b.build().await)
            .await?;
        suite.get_table_by_id(&b.build().await).await?;
        suite.get_table_copied_file(&b.build().await).await?;
        suite.truncate_table(&b.build().await).await?;
        suite
            .update_table_with_copied_files(&b.build().await)
            .await?;
        suite.table_index_create_drop(&b.build().await).await?;
        suite.index_create_list_drop(&b.build().await).await?;
        suite.table_lock_revision(&b.build().await).await?;
        suite
            .virtual_column_create_list_drop(&b.build().await)
            .await?;
        suite.catalog_create_get_list_drop(&b.build().await).await?;
        suite.table_least_visible_time(&b.build().await).await?;
        suite
            .drop_table_without_tableid_to_name(&b.build().await)
            .await?;

        suite.get_table_name_by_id(&b.build().await).await?;
        suite.get_db_name_by_id(&b.build().await).await?;
        suite.test_sequence(&b.build().await).await?;

        suite.dictionary_create_list_drop(&b.build().await).await?;

        Ok(())
    }

    /// Test SchemaAPI on cluster
    pub async fn test_cluster<B, MT>(b: B) -> anyhow::Result<()>
    where
        B: kvapi::ApiBuilder<MT>,
        MT: SchemaApi,
    {
        let suite = SchemaApiTestSuite {};

        // leader-follower test
        {
            let cluster = b.build_cluster().await;
            suite
                .database_get_diff_nodes(&cluster[0], &cluster[1])
                .await?;
        }
        {
            let cluster = b.build_cluster().await;
            suite
                .list_database_diff_nodes(&cluster[0], &cluster[1])
                .await?;
        }
        {
            let cluster = b.build_cluster().await;
            suite
                .list_table_diff_nodes(&cluster[0], &cluster[1])
                .await?;
        }
        {
            let cluster = b.build_cluster().await;
            suite.table_get_diff_nodes(&cluster[0], &cluster[1]).await?;
        }

        // follower-follower test
        {
            let cluster = b.build_cluster().await;
            suite
                .database_get_diff_nodes(&cluster[2], &cluster[1])
                .await?;
        }
        {
            let cluster = b.build_cluster().await;
            suite
                .list_database_diff_nodes(&cluster[2], &cluster[1])
                .await?;
        }
        {
            let cluster = b.build_cluster().await;
            suite
                .list_table_diff_nodes(&cluster[2], &cluster[1])
                .await?;
        }
        {
            let cluster = b.build_cluster().await;
            suite.table_get_diff_nodes(&cluster[2], &cluster[1]).await?;
        }

        Ok(())
    }

    #[fastrace::trace]
    async fn database_and_table_rename<MT: SchemaApi + kvapi::AsKVApi<Error = MetaError>>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant = Tenant::new_or_err("tenant1", func_name!())?;

        let db_name = "db1";
        let db2_name = "db2";
        let db3_name = "db3";
        let table_name = "table";
        let table2_name = "table2";
        let table3_name = "table3";
        let db_id;
        let db3_id;
        let table_id;

        let db_name_ident = DatabaseNameIdent::new(&tenant, db_name);
        let db2_name_ident = DatabaseNameIdent::new(&tenant, db2_name);

        let db_table_name_ident = TableNameIdent {
            tenant: tenant.clone(),
            db_name: db_name.to_string(),
            table_name: table_name.to_string(),
        };

        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };

        let options = || maplit::btreemap! {"opt‐1".into() => "val-1".into()};

        let table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: options(),
            updated_on: created_on,
            created_on,
            ..TableMeta::default()
        };

        let created_on = Utc::now();

        let req = CreateTableReq {
            create_option: CreateOption::Create,
            name_ident: db_table_name_ident.clone(),
            table_meta: table_meta(created_on),
            as_dropped: false,
        };

        {
            info!("--- prepare db1,db3 and table");
            // prepare db1
            let res = self.create_database(mt, &tenant, "db1", "eng1").await?;
            assert_eq!(1, res.db_id);
            db_id = res.db_id;

            let res = self.create_database(mt, &tenant, "db3", "eng1").await?;
            db3_id = res.db_id;

            let res = mt.create_table(req).await?;
            table_id = res.table_id;

            let db_id_name_key = DatabaseIdToName { db_id };
            let ret_db_name_ident: DatabaseNameIdentRaw =
                get_kv_data(mt.as_kv_api(), &db_id_name_key).await?;
            assert_eq!(
                ret_db_name_ident,
                DatabaseNameIdentRaw::from(&db_name_ident)
            );

            let table_id_name_key = TableIdToName { table_id };
            let ret_table_name_ident: DBIdTableName =
                get_kv_data(mt.as_kv_api(), &table_id_name_key).await?;
            assert_eq!(ret_table_name_ident, DBIdTableName {
                db_id,
                table_name: table_name.to_string(),
            });
        }

        {
            info!("--- rename exists db db1 to not exists mutable db2");
            let req = RenameDatabaseReq {
                if_exists: false,
                name_ident: db_name_ident.clone(),
                new_db_name: db2_name.to_string(),
            };
            let res = mt.rename_database(req).await;
            info!("rename database res: {:?}", res);
            assert!(res.is_ok());

            let db_id_2_name_key = DatabaseIdToName { db_id };
            let ret_db_name_ident: DatabaseNameIdentRaw =
                get_kv_data(mt.as_kv_api(), &db_id_2_name_key).await?;
            assert_eq!(
                ret_db_name_ident,
                DatabaseNameIdentRaw::from(&db2_name_ident)
            );

            let table_id_name_key = TableIdToName { table_id };
            let ret_table_name_ident: DBIdTableName =
                get_kv_data(mt.as_kv_api(), &table_id_name_key).await?;
            assert_eq!(ret_table_name_ident, DBIdTableName {
                db_id,
                table_name: table_name.to_string(),
            });
        }

        {
            info!("--- rename exists table1 to not exists mutable table2");
            let got = mt
                .rename_table(RenameTableReq {
                    if_exists: true,
                    name_ident: TableNameIdent {
                        tenant: tenant.clone(),
                        db_name: db2_name.to_string(),
                        table_name: table_name.to_string(),
                    },
                    new_db_name: db2_name.to_string(),
                    new_table_name: table2_name.to_string(),
                })
                .await;
            debug!("--- rename table on unknown database got: {:?}", got);

            let table_id_name_key = TableIdToName { table_id };
            let ret_table_name_ident: DBIdTableName =
                get_kv_data(mt.as_kv_api(), &table_id_name_key).await?;
            assert_eq!(ret_table_name_ident, DBIdTableName {
                db_id,
                table_name: table2_name.to_string(),
            });
        }

        {
            info!("--- rename exists table1 to not exists mutable db3.table3");
            let got = mt
                .rename_table(RenameTableReq {
                    if_exists: true,
                    name_ident: TableNameIdent {
                        tenant: tenant.clone(),
                        db_name: db2_name.to_string(),
                        table_name: table2_name.to_string(),
                    },
                    new_db_name: db3_name.to_string(),
                    new_table_name: table3_name.to_string(),
                })
                .await;
            debug!("--- rename table on unknown database got: {:?}", got);

            let table_id_name_key = TableIdToName { table_id };
            let ret_table_name_ident: DBIdTableName =
                get_kv_data(mt.as_kv_api(), &table_id_name_key).await?;
            assert_eq!(ret_table_name_ident, DBIdTableName {
                db_id: db3_id,
                table_name: table3_name.to_string(),
            });
        }

        Ok(())
    }

    #[fastrace::trace]
    async fn database_create_get_drop<MT: SchemaApi + kvapi::AsKVApi<Error = MetaError>>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;
        info!("--- create db1");
        {
            let req = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant, "db1"),
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    ..Default::default()
                },
            };

            let res = mt.create_database(req).await;
            info!("create database res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(1, res.db_id, "first database id is 1");
        }

        info!("--- create db1 again with if_not_exists=false");
        {
            let req = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant, "db1"),
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..Default::default()
                },
            };

            let res = mt.create_database(req).await;
            info!("create database res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::DATABASE_ALREADY_EXISTS,
                ErrorCode::from(err).code()
            );
        }

        info!("--- create db1 again with if_not_exists=true");
        {
            let req = CreateDatabaseReq {
                create_option: CreateOption::CreateIfNotExists,
                name_ident: DatabaseNameIdent::new(&tenant, "db1"),
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let res = mt.create_database(req).await;
            info!("create database res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(1, res.db_id, "db1 id is 1");
        }

        info!("--- get db1");
        {
            let res = mt
                .get_database(GetDatabaseReq::new(
                    Tenant::new_or_err(tenant_name, func_name!())?,
                    "db1",
                ))
                .await;
            debug!("get present database res: {:?}", res);
            let res = res?;
            assert_eq!(1, res.ident.db_id, "db1 id is 1");
            assert_eq!("db1", res.name_ident.database_name(), "db1.db is db1");
        }

        info!("--- create db2");
        {
            let req = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant, "db2"),
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let res = mt.create_database(req).await;
            info!("create database res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(
                6, res.db_id,
                "second database id is 4: seq increment but no used"
            );
        }

        info!("--- get db2");
        {
            let res = mt
                .get_database(GetDatabaseReq::new(tenant.clone(), "db2"))
                .await?;
            assert_eq!("db2", res.name_ident.database_name(), "db1.db is db1");
        }

        info!("--- get absent db");
        {
            let res = mt
                .get_database(GetDatabaseReq::new(tenant.clone(), "absent"))
                .await;
            debug!("=== get absent database res: {:?}", res);
            assert!(res.is_err());
            let err = res.unwrap_err();
            let err_code = ErrorCode::from(err);

            assert_eq!(1003, err_code.code());
            assert!(err_code.message().contains("absent"));
        }

        info!("--- drop db2");
        {
            mt.drop_database(DropDatabaseReq {
                if_exists: false,
                name_ident: DatabaseNameIdent::new(&tenant, "db2"),
            })
            .await?;
        }

        info!("--- get db2 should not found");
        {
            let res = mt
                .get_database(GetDatabaseReq::new(tenant.clone(), "db2"))
                .await;
            let err = res.unwrap_err();
            assert_eq!(ErrorCode::UNKNOWN_DATABASE, ErrorCode::from(err).code());
        }

        info!("--- drop db2 with if_exists=true returns no error");
        {
            mt.drop_database(DropDatabaseReq {
                if_exists: true,
                name_ident: DatabaseNameIdent::new(&tenant, "db2"),
            })
            .await?;
        }

        info!("--- create or replace db1");
        {
            let new_engine = "new github";
            let db_name = DatabaseNameIdent::new(&tenant, "db1");
            let res = mt
                .get_database(GetDatabaseReq::new(tenant.clone(), "db1"))
                .await?;
            debug!("get present database res: {:?}", res);
            assert_ne!(res.meta.engine, new_engine.to_string());

            let orig_db_id: u64 = get_kv_u64_data(mt.as_kv_api(), &db_name).await?;
            assert_eq!(orig_db_id, res.ident.db_id);
            let db_id_name_key = DatabaseIdToName { db_id: orig_db_id };
            let ret_db_name_ident: DatabaseNameIdentRaw =
                get_kv_data(mt.as_kv_api(), &db_id_name_key).await?;
            assert_eq!(ret_db_name_ident, DatabaseNameIdentRaw::from(&db_name));

            let req = CreateDatabaseReq {
                create_option: CreateOption::CreateOrReplace,
                name_ident: DatabaseNameIdent::new(&tenant, "db1"),
                meta: DatabaseMeta {
                    engine: new_engine.to_string(),
                    ..Default::default()
                },
            };

            let _res = mt.create_database(req).await?;

            let res = mt
                .get_database(GetDatabaseReq::new(tenant.clone(), "db1"))
                .await?;
            debug!("get present database res: {:?}", res);
            assert_eq!(res.meta.engine, new_engine.to_string());

            let db_id: u64 = get_kv_u64_data(mt.as_kv_api(), &db_name).await?;
            assert_eq!(db_id, res.ident.db_id);
            let db_id_name_key = DatabaseIdToName { db_id };
            let ret_db_name_ident: DatabaseNameIdentRaw =
                get_kv_data(mt.as_kv_api(), &db_id_name_key).await?;
            assert_eq!(ret_db_name_ident, DatabaseNameIdentRaw::from(&db_name));
            assert_ne!(db_id, orig_db_id);
        }

        Ok(())
    }

    #[fastrace::trace]
    async fn database_create_from_share_and_drop<
        MT: ShareApi + kvapi::AsKVApi<Error = MetaError> + SchemaApi,
    >(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name1 = "tenant1";
        let tenant = Tenant::new_or_err(tenant_name1, func_name!())?;
        let db1 = "db1";
        let share = "share";
        let share_name = ShareNameIdent::new(&tenant, share);
        let db_name1 = DatabaseNameIdent::new(&tenant, db1);

        let db_id;

        info!("--- create a share and tenant1 create db1 from a share");
        {
            let req = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: db_name1.clone(),
                meta: DatabaseMeta {
                    from_share: Some(share_name.clone().into()),
                    ..Default::default()
                },
            };

            let res = mt.create_database(req).await;
            info!("create database res: {:?}", res);

            assert!(res.is_ok());
            // save the db id
            db_id = res.unwrap().db_id;
        };

        // drop database created from share
        {
            mt.drop_database(DropDatabaseReq {
                if_exists: false,
                name_ident: db_name1.clone(),
            })
            .await?;

            // check that DatabaseMeta has been removed
            let res = is_all_db_data_removed(mt.as_kv_api(), db_id).await?;
            assert!(res);

            // db has been removed, so undrop_database MUST return error
            let res = mt
                .undrop_database(UndropDatabaseReq {
                    name_ident: db_name1.clone(),
                })
                .await;
            assert!(res.is_err());
            assert_eq!(
                ErrorCode::UNDROP_DB_HAS_NO_HISTORY,
                ErrorCode::from(res.unwrap_err()).code()
            );
        }

        Ok(())
    }

    #[fastrace::trace]
    async fn database_create_get_drop_in_diff_tenant<MT: SchemaApi>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant1 = Tenant::new_or_err("tenant1", func_name!())?;
        let tenant2 = Tenant::new_or_err("tenant2", func_name!())?;
        info!("--- tenant1 create db1");
        let db_id_1 = {
            let req = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant1, "db1"),
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    ..Default::default()
                },
            };

            let res = mt.create_database(req).await;
            info!("create database res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(1, res.db_id, "first database id is 1");
            res.db_id
        };

        info!("--- tenant1 create db2");
        let db_id_2 = {
            let req = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant1, "db2"),
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    ..Default::default()
                },
            };

            let res = mt.create_database(req).await;
            info!("create database res: {:?}", res);
            let res = res.unwrap();
            assert!(res.db_id > db_id_1, "second database id is > {}", db_id_1);
            res.db_id
        };

        info!("--- tenant2 create db1");
        let _db_id_3 = {
            let req = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant2, "db1"),
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    ..Default::default()
                },
            };

            let res = mt.create_database(req).await;
            info!("create database res: {:?}", res);
            let res = res.unwrap();
            assert!(res.db_id > db_id_2, "third database id > {}", db_id_2);
            res.db_id
        };

        info!("--- tenant1 get db1");
        {
            let res = mt
                .get_database(GetDatabaseReq::new(tenant1.clone(), "db1"))
                .await;
            debug!("get present database res: {:?}", res);
            let res = res?;
            assert_eq!(1, res.ident.db_id, "db1 id is 1");
            assert_eq!("db1", res.name_ident.database_name(), "db1.db is db1");
        }

        info!("--- tenant1 get absent db");
        {
            let res = mt
                .get_database(GetDatabaseReq::new(tenant1.clone(), "absent"))
                .await;
            debug!("=== get absent database res: {:?}", res);
            assert!(res.is_err());
            let err = res.unwrap_err();
            let err = ErrorCode::from(err);

            assert_eq!(ErrorCode::UNKNOWN_DATABASE, err.code());
            assert!(err.message().contains("absent"));
        }

        info!("--- tenant2 get tenant1's db2");
        {
            let res = mt.get_database(GetDatabaseReq::new(tenant2, "db2")).await;
            debug!("=== get other tenant's database res: {:?}", res);
            assert!(res.is_err());
            let res = res.unwrap_err();
            let err = ErrorCode::from(res);

            assert_eq!(ErrorCode::UNKNOWN_DATABASE, err.code());
            assert_eq!("Unknown database 'db2'".to_string(), err.message());
        }

        info!("--- drop db2");
        {
            mt.drop_database(DropDatabaseReq {
                if_exists: false,
                name_ident: DatabaseNameIdent::new(&tenant1, "db2"),
            })
            .await?;
        }

        info!("--- tenant1 get db2 should not found");
        {
            let res = mt
                .get_database(GetDatabaseReq::new(tenant1.clone(), "db2"))
                .await;
            let err = res.unwrap_err();
            assert_eq!(ErrorCode::UNKNOWN_DATABASE, ErrorCode::from(err).code());
        }

        info!("--- tenant1 drop db2 with if_exists=true returns no error");
        {
            mt.drop_database(DropDatabaseReq {
                if_exists: true,
                name_ident: DatabaseNameIdent::new(&tenant1, "db2"),
            })
            .await?;
        }

        Ok(())
    }

    #[fastrace::trace]
    async fn database_list<MT: SchemaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        info!("--- prepare db1 and db2");
        let mut db_ids = vec![];
        let db_names = ["db1", "db2"];
        let engines = ["eng1", "eng2"];
        let tenant = Tenant::new_or_err("tenant1", func_name!())?;
        {
            let res = self.create_database(mt, &tenant, "db1", "eng1").await?;
            assert_eq!(1, res.db_id);
            db_ids.push(res.db_id);

            let res = self.create_database(mt, &tenant, "db2", "eng2").await?;
            assert!(res.db_id > 1);
            db_ids.push(res.db_id);
        }

        info!("--- list_databases");
        {
            let dbs = mt
                .list_databases(ListDatabaseReq {
                    tenant: tenant.clone(),
                    filter: None,
                })
                .await?;

            let got = dbs.iter().map(|x| x.ident.db_id).collect::<Vec<_>>();
            assert_eq!(db_ids, got);

            for (i, db_info) in dbs.iter().enumerate() {
                assert_eq!(tenant.tenant_name(), db_info.name_ident.tenant_name());
                assert_eq!(db_names[i], db_info.name_ident.database_name());
                assert_eq!(db_ids[i], db_info.ident.db_id);
                assert_eq!(engines[i], db_info.meta.engine);
            }
        }

        Ok(())
    }

    #[fastrace::trace]
    async fn database_list_in_diff_tenant<MT: SchemaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        info!("--- prepare db1 and db2");
        let tenant1 = Tenant::new_or_err("tenant1", func_name!())?;
        let tenant2 = Tenant::new_or_err("tenant2", func_name!())?;

        let mut db_ids = vec![];
        {
            let res = self.create_database(mt, &tenant1, "db1", "eng1").await?;
            assert_eq!(1, res.db_id);
            db_ids.push(res.db_id);

            let res = self.create_database(mt, &tenant1, "db2", "eng2").await?;
            assert!(res.db_id > 1);
            db_ids.push(res.db_id);
        }

        let db_id_3 = {
            let res = self.create_database(mt, &tenant2, "db3", "eng1").await?;
            res.db_id
        };

        info!("--- get_databases by tenant1");
        {
            let dbs = mt
                .list_databases(ListDatabaseReq {
                    tenant: tenant1.clone(),
                    filter: None,
                })
                .await?;
            let got = dbs.iter().map(|x| x.ident.db_id).collect::<Vec<_>>();
            assert_eq!(db_ids, got)
        }

        info!("--- get_databases by tenant2");
        {
            let dbs = mt
                .list_databases(ListDatabaseReq {
                    tenant: tenant2.clone(),
                    filter: None,
                })
                .await?;
            let want: Vec<u64> = vec![db_id_3];
            let got = dbs.iter().map(|x| x.ident.db_id).collect::<Vec<_>>();
            assert_eq!(want, got)
        }

        Ok(())
    }

    #[fastrace::trace]
    async fn database_rename<MT: SchemaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        let tenant = Tenant::new_or_err("tenant1", func_name!())?;
        let db_name = "db1";
        let db2_name = "db2";
        let new_db_name = "db3";

        info!("--- rename not exists db1 to not exists db2");
        {
            let req = RenameDatabaseReq {
                if_exists: false,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),
                new_db_name: new_db_name.to_string(),
            };

            let res = mt.rename_database(req).await;
            info!("rename database res: {:?}", res);
            assert!(res.is_err());
            assert_eq!(
                ErrorCode::UNKNOWN_DATABASE,
                ErrorCode::from(res.unwrap_err()).code()
            );
        }

        info!("--- prepare db1 and db2");
        {
            // prepare db2
            let res = self.create_database(mt, &tenant, "db1", "eng1").await?;
            assert_eq!(1, res.db_id);

            info!("--- rename not exists db4 to exists db1");
            {
                let req = RenameDatabaseReq {
                    if_exists: false,
                    name_ident: DatabaseNameIdent::new(&tenant, "db4"),
                    new_db_name: db_name.to_string(),
                };

                let res = mt.rename_database(req).await;
                info!("rename database res: {:?}", res);
                assert!(res.is_err());
                assert_eq!(
                    ErrorCode::UNKNOWN_DATABASE,
                    ErrorCode::from(res.unwrap_err()).code()
                );
            }

            // prepare db2
            let res = self.create_database(mt, &tenant, "db2", "eng1").await?;
            assert!(res.db_id > 1);
        }

        info!("--- rename exists db db1 to exists db db2");
        {
            let req = RenameDatabaseReq {
                if_exists: false,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),
                new_db_name: db2_name.to_string(),
            };

            let res = mt.rename_database(req).await;
            info!("rename database res: {:?}", res);
            assert!(res.is_err());
            assert_eq!(
                ErrorCode::DATABASE_ALREADY_EXISTS,
                ErrorCode::from(res.unwrap_err()).code()
            );
        }

        info!("--- rename exists db db1 to not exists mutable db");
        {
            let req = RenameDatabaseReq {
                if_exists: false,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),

                new_db_name: new_db_name.to_string(),
            };
            let res = mt.rename_database(req).await;
            info!("rename database res: {:?}", res);
            assert!(res.is_ok());

            let res = mt
                .get_database(GetDatabaseReq::new(tenant.clone(), new_db_name))
                .await;
            debug!("get present database res: {:?}", res);
            let res = res?;
            assert_eq!(1, res.ident.db_id, "db3 id is 1");
            assert_eq!("db3", res.name_ident.database_name(), "db3.db is db3");

            info!("--- get old database after rename");
            {
                let res = mt.get_database(GetDatabaseReq::new(tenant, db_name)).await;
                let err = res.err().unwrap();
                assert_eq!(ErrorCode::UNKNOWN_DATABASE, ErrorCode::from(err).code());
            }
        }

        Ok(())
    }

    #[fastrace::trace]
    async fn database_drop_undrop_list_history<MT: SchemaApi>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1_database_drop_undrop_list_history";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;
        let db_name = "db1_database_drop_undrop_list_history";
        let new_db_name = "db2_database_drop_undrop_list_history";

        let db_name_ident = DatabaseNameIdent::new(&tenant, db_name);
        let new_db_name_ident = DatabaseNameIdent::new(&tenant, new_db_name);

        info!("--- create and drop db1");
        {
            // first create database
            let req = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: db_name_ident.clone(),
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    ..Default::default()
                },
            };

            let res = mt.create_database(req).await;
            info!("create database res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(1, res.db_id, "first database id is 1");

            let res = mt
                .get_database_history(ListDatabaseReq {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    filter: None,
                })
                .await?;
            calc_and_compare_drop_on_db_result(res, vec![DroponInfo {
                name: db_name_ident.to_string_key(),
                desc: "".to_string(),
                drop_on_cnt: 0,
                non_drop_on_cnt: 1,
            }]);

            // then drop db1
            mt.drop_database(DropDatabaseReq {
                if_exists: false,
                name_ident: db_name_ident.clone(),
            })
            .await?;
            let res = mt
                .get_database_history(ListDatabaseReq {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    filter: None,
                })
                .await?;
            calc_and_compare_drop_on_db_result(res, vec![DroponInfo {
                name: db_name_ident.to_string_key(),
                desc: "".to_string(),
                drop_on_cnt: 1,
                non_drop_on_cnt: 0,
            }]);

            // undrop db1
            mt.undrop_database(UndropDatabaseReq {
                name_ident: db_name_ident.clone(),
            })
            .await?;
            let res = mt
                .get_database_history(ListDatabaseReq {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    filter: None,
                })
                .await?;
            calc_and_compare_drop_on_db_result(res, vec![DroponInfo {
                name: db_name_ident.to_string_key(),
                desc: "".to_string(),
                drop_on_cnt: 0,
                non_drop_on_cnt: 1,
            }]);
        }

        info!("--- drop and create db1");
        {
            // first drop db1
            mt.drop_database(DropDatabaseReq {
                if_exists: false,
                name_ident: db_name_ident.clone(),
            })
            .await?;
            let res = mt
                .get_database_history(ListDatabaseReq {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    filter: None,
                })
                .await?;
            calc_and_compare_drop_on_db_result(res, vec![DroponInfo {
                name: db_name_ident.to_string_key(),
                desc: "".to_string(),
                drop_on_cnt: 1,
                non_drop_on_cnt: 0,
            }]);

            // then create database
            let req = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: db_name_ident.clone(),
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    ..Default::default()
                },
            };

            let _res = mt.create_database(req).await?;

            let res = mt
                .get_database_history(ListDatabaseReq {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    filter: None,
                })
                .await?;
            calc_and_compare_drop_on_db_result(res, vec![DroponInfo {
                name: db_name_ident.to_string_key(),
                desc: "".to_string(),
                drop_on_cnt: 1,
                non_drop_on_cnt: 1,
            }]);
        }

        info!("--- create and rename db2");
        {
            // first create db2
            let req = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: new_db_name_ident.clone(),
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    ..Default::default()
                },
            };

            let _res = mt.create_database(req).await?;

            let res = mt
                .get_database_history(ListDatabaseReq {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    filter: None,
                })
                .await?;
            calc_and_compare_drop_on_db_result(res, vec![
                DroponInfo {
                    name: db_name_ident.to_string_key(),
                    desc: "".to_string(),
                    drop_on_cnt: 1,
                    non_drop_on_cnt: 1,
                },
                DroponInfo {
                    name: new_db_name_ident.to_string_key(),
                    desc: "".to_string(),
                    drop_on_cnt: 0,
                    non_drop_on_cnt: 1,
                },
            ]);

            // then drop db2
            mt.drop_database(DropDatabaseReq {
                if_exists: false,
                name_ident: new_db_name_ident.clone(),
            })
            .await?;
            let res = mt
                .get_database_history(ListDatabaseReq {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    filter: None,
                })
                .await?;
            calc_and_compare_drop_on_db_result(res, vec![
                DroponInfo {
                    name: db_name_ident.to_string_key(),
                    desc: "".to_string(),
                    drop_on_cnt: 1,
                    non_drop_on_cnt: 1,
                },
                DroponInfo {
                    name: new_db_name_ident.to_string_key(),
                    desc: "".to_string(),
                    drop_on_cnt: 1,
                    non_drop_on_cnt: 0,
                },
            ]);

            // rename db1 to db2
            mt.rename_database(RenameDatabaseReq {
                if_exists: false,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),
                new_db_name: new_db_name.to_string(),
            })
            .await?;
            let res = mt
                .get_database_history(ListDatabaseReq {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    filter: None,
                })
                .await?;
            calc_and_compare_drop_on_db_result(res, vec![
                DroponInfo {
                    name: db_name_ident.to_string_key(),
                    desc: "".to_string(),
                    drop_on_cnt: 1,
                    non_drop_on_cnt: 0,
                },
                DroponInfo {
                    name: new_db_name_ident.to_string_key(),
                    desc: "".to_string(),
                    drop_on_cnt: 1,
                    non_drop_on_cnt: 1,
                },
            ]);
        }

        Ok(())
    }

    #[fastrace::trace]
    async fn catalog_create_get_list_drop<MT: SchemaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        let tenant_name = "tenant1";

        let catalog_name = "catalog1";

        let tenant = Tenant {
            tenant: tenant_name.to_string(),
        };

        let ident = CatalogNameIdent::new(tenant.clone(), catalog_name);

        info!("--- create catalog1");
        let req = CreateCatalogReq {
            if_not_exists: false,
            name_ident: ident.clone(),
            meta: CatalogMeta {
                catalog_option: CatalogOption::Iceberg(IcebergCatalogOption::Rest(
                    IcebergRestCatalogOption {
                        uri: "http://127.0.0.1:8080".to_string(),
                        warehouse: "test".to_string(),
                        props: HashMap::default(),
                    },
                )),
                created_on: Utc::now(),
            },
        };

        let res = mt.create_catalog(req).await?;
        info!("create catalog res: {:?}", res);

        let got = mt.get_catalog(GetCatalogReq::new(ident.clone())).await?;
        assert_eq!(got.id.catalog_id, res.catalog_id);
        assert_eq!(got.name_ident.tenant, "tenant1");
        assert_eq!(got.name_ident.catalog_name, "catalog1");

        let got = mt
            .list_catalogs(ListCatalogReq::new(tenant.clone()))
            .await?;
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].name_ident.tenant, "tenant1");
        assert_eq!(got[0].name_ident.catalog_name, "catalog1");

        let _ = mt
            .drop_catalog(DropCatalogReq {
                if_exists: false,
                name_ident: ident.clone(),
            })
            .await?;

        let got = mt
            .list_catalogs(ListCatalogReq::new(tenant.clone()))
            .await?;
        assert_eq!(got.len(), 0);

        Ok(())
    }

    #[fastrace::trace]
    async fn drop_table_without_tableid_to_name<
        MT: SchemaApi + kvapi::AsKVApi<Error = MetaError>,
    >(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        let db = "db";
        let table_name = "tbl";

        let create_db_req = CreateDatabaseReq {
            create_option: CreateOption::Create,
            name_ident: DatabaseNameIdent::new(&tenant, db),
            meta: DatabaseMeta {
                engine: "".to_string(),
                ..DatabaseMeta::default()
            },
        };

        let res = mt.create_database(create_db_req.clone()).await?;
        let db_id = res.db_id;

        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };

        let table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: BTreeMap::new(),
            updated_on: created_on,
            created_on,
            ..TableMeta::default()
        };
        let created_on = Utc::now();

        let req = CreateTableReq {
            create_option: CreateOption::Create,
            name_ident: TableNameIdent {
                tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                db_name: db.to_string(),
                table_name: table_name.to_string(),
            },

            table_meta: table_meta(created_on),
            as_dropped: false,
        };
        let resp = mt.create_table(req.clone()).await?;
        let table_id = resp.table_id;

        let table_id_to_name = TableIdToName { table_id };
        // delete TableIdToName before drop table
        delete_test_data(mt.as_kv_api(), &table_id_to_name).await?;

        mt.drop_table_by_id(DropTableByIdReq {
            if_exists: false,
            tenant: tenant.clone(),
            db_id,
            table_name: table_name.to_string(),
            tb_id: table_id,
        })
        .await?;

        Ok(())
    }

    #[fastrace::trace]
    async fn table_least_visible_time<MT: SchemaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        let db_name = "db1";
        let tbl_name = "tb1";
        let name_ident = TableNameIdent {
            tenant: Tenant::new_or_err(tenant_name, func_name!())?,
            db_name: db_name.to_string(),
            table_name: tbl_name.to_string(),
        };
        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };
        let options = || maplit::btreemap! {"opt‐1".into() => "val-1".into()};

        info!("--- prepare db");
        {
            let plan = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            mt.create_database(plan).await?;
        }

        let table_id;
        info!("--- create table");
        {
            let table_meta = |created_on| TableMeta {
                schema: schema(),
                engine: "JSON".to_string(),
                options: options(),
                updated_on: created_on,
                created_on,
                ..TableMeta::default()
            };
            let created_on = Utc::now();
            let req = CreateTableReq {
                create_option: CreateOption::Create,
                name_ident: name_ident.clone(),
                table_meta: table_meta(created_on),
                as_dropped: false,
            };
            let res = mt.create_table(req.clone()).await?;
            table_id = res.table_id;
        }

        info!("--- test lvt");
        {
            let time = DateTime::<Utc>::from_timestamp(1024, 0).unwrap();
            let req = SetLVTReq { table_id, time };
            let get_req = GetLVTReq { table_id };

            let res = mt.get_table_lvt(get_req.clone()).await?;
            assert!(res.time.is_none());

            let res = mt.set_table_lvt(req).await?;
            assert_eq!(res.time, DateTime::<Utc>::from_timestamp(1024, 0).unwrap());
            let res = mt.get_table_lvt(get_req.clone()).await?;
            assert_eq!(
                res.time.unwrap(),
                DateTime::<Utc>::from_timestamp(1024, 0).unwrap()
            );

            // test lvt never fall back
            let time = DateTime::<Utc>::from_timestamp(102, 0).unwrap();
            let req = SetLVTReq { table_id, time };

            let res = mt.set_table_lvt(req).await?;
            assert_eq!(res.time, DateTime::<Utc>::from_timestamp(1024, 0).unwrap());
            let res = mt.get_table_lvt(get_req.clone()).await?;
            assert_eq!(
                res.time.unwrap(),
                DateTime::<Utc>::from_timestamp(1024, 0).unwrap()
            );

            let time = DateTime::<Utc>::from_timestamp(1025, 0).unwrap();
            let req = SetLVTReq { table_id, time };

            let res = mt.set_table_lvt(req).await?;
            assert_eq!(res.time, DateTime::<Utc>::from_timestamp(1025, 0).unwrap());
            let res = mt.get_table_lvt(get_req).await?;
            assert_eq!(
                res.time.unwrap(),
                DateTime::<Utc>::from_timestamp(1025, 0).unwrap()
            );
        }

        Ok(())
    }

    #[fastrace::trace]
    async fn table_create_get_drop<MT: SchemaApi + kvapi::AsKVApi<Error = MetaError>>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        let db_name = "db1";
        let tbl_name = "tb2";

        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };

        let options = || maplit::btreemap! {"opt‐1".into() => "val-1".into()};

        let table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: options(),
            updated_on: created_on,
            created_on,
            ..TableMeta::default()
        };

        let unknown_database_code = ErrorCode::UNKNOWN_DATABASE;

        info!("--- create or get table on unknown db");
        {
            let created_on = Utc::now();

            let req = CreateTableReq {
                create_option: CreateOption::Create,
                name_ident: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name.to_string(),
                },

                table_meta: table_meta(created_on),
                as_dropped: false,
            };
            // test create table
            {
                let res = mt.create_table(req).await;
                debug!("create table on unknown db res: {:?}", res);

                assert!(res.is_err());
                let err = res.unwrap_err();
                let err = ErrorCode::from(err);

                assert_eq!(unknown_database_code, err.code());
            };
            // test get table
            {
                let req = GetTableReq::new(&tenant, db_name, tbl_name);
                let got = mt.get_table(req).await;
                debug!("get table on unknown db got: {:?}", got);

                assert!(got.is_err());
                let err = got.unwrap_err();
                let err = ErrorCode::from(err);

                assert_eq!(unknown_database_code, err.code());
            }
        }

        info!("--- prepare db");
        let db_id = {
            let plan = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let res = mt.create_database(plan).await?;
            info!("create database res: {:?}", res);

            assert_eq!(1, res.db_id, "first database id is 1");
            res.db_id
        };

        info!("--- create tb2 and get table");
        let created_on = Utc::now();

        let mut req = CreateTableReq {
            create_option: CreateOption::Create,
            name_ident: TableNameIdent {
                tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                db_name: db_name.to_string(),
                table_name: tbl_name.to_string(),
            },
            table_meta: table_meta(created_on),
            as_dropped: false,
        };
        let tb_ident_2 = {
            {
                let old_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
                let res = mt.create_table(req.clone()).await?;
                let cur_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
                assert!(old_db.ident.seq < cur_db.ident.seq);
                assert!(res.table_id >= 1, "table id >= 1");

                let tb_id = res.table_id;

                let req = GetTableReq::new(&tenant, db_name, tbl_name);
                let got = mt.get_table(req).await?;
                let seq = got.ident.seq;

                let ident = TableIdent::new(tb_id, seq);

                let want = TableInfo {
                    ident,
                    desc: format!("'{}'.'{}'", db_name, tbl_name),
                    name: tbl_name.into(),
                    meta: table_meta(created_on),
                    tenant: tenant_name.to_string(),
                    ..Default::default()
                };
                assert_meta_eq_without_updated!(want, got.as_ref().clone(), "get created table");
                ident
            }
        };
        info!("--- create table again with if_not_exists = true");
        {
            req.create_option = CreateOption::CreateIfNotExists;
            let res = mt.create_table(req.clone()).await?;
            assert_eq!(
                tb_ident_2.table_id, res.table_id,
                "new table id is still the same"
            );

            let req = GetTableReq::new(&tenant, db_name, tbl_name);
            let got = mt.get_table(req).await?;
            let want = TableInfo {
                ident: tb_ident_2,
                desc: format!("'{}'.'{}'", db_name, tbl_name),
                name: tbl_name.into(),
                meta: table_meta(created_on),
                tenant: tenant_name.to_string(),
                ..Default::default()
            };
            assert_meta_eq_without_updated!(want, got.as_ref().clone(), "get created table");
        }

        info!("--- create table again with if_not_exists = false");
        {
            req.create_option = CreateOption::Create;

            let res = mt.create_table(req).await;
            info!("create table res: {:?}", res);

            let status = res.err().unwrap();
            let err_code = ErrorCode::from(status);

            assert_eq!(
                format!(
                    "TableAlreadyExists. Code: 2302, Text = Table '{}' already exists.",
                    tbl_name
                ),
                err_code.to_string()
            );

            // get_table returns the old table

            let req = GetTableReq::new(&tenant, "db1", "tb2");
            let got = mt.get_table(req).await.unwrap();
            let want = TableInfo {
                ident: tb_ident_2,
                desc: format!("'{}'.'{}'", db_name, tbl_name),
                name: tbl_name.into(),
                meta: table_meta(created_on),
                tenant: tenant_name.to_string(),
                ..Default::default()
            };
            assert_meta_eq_without_updated!(want, got.as_ref().clone(), "get old table");
        }

        info!("--- create another table");
        {
            let created_on = Utc::now();

            let req = CreateTableReq {
                create_option: CreateOption::Create,
                name_ident: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: "tb3".to_string(),
                },
                table_meta: table_meta(created_on),
                as_dropped: false,
            };

            let old_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
            let res = mt.create_table(req.clone()).await?;
            let cur_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);
            assert!(
                res.table_id > tb_ident_2.table_id,
                "table id > {}",
                tb_ident_2.table_id
            );
        }

        let req = GetTableReq::new(&tenant, db_name, tbl_name);
        let tb_info = mt.get_table(req).await?;
        let tb_id = tb_info.ident.table_id;
        info!("--- drop table");
        {
            info!("--- drop table with if_exists = false");
            {
                let plan = DropTableByIdReq {
                    if_exists: false,
                    tenant: tenant.clone(),
                    db_id,
                    table_name: tbl_name.to_string(),
                    tb_id,
                };
                mt.drop_table_by_id(plan.clone()).await?;

                info!("--- get table after drop");
                {
                    let req = GetTableReq::new(&tenant, db_name, tbl_name);
                    let res = mt.get_table(req).await;
                    let status = res.err().unwrap();
                    let err_code = ErrorCode::from(status);

                    assert_eq!(
                        format!(
                            "UnknownTable. Code: 1025, Text = Unknown table '{:}'.",
                            tbl_name
                        ),
                        err_code.to_string(),
                        "get dropped table {}",
                        tbl_name
                    );
                }
            }

            info!("--- drop table with if_exists = false again, error");
            {
                let plan = DropTableByIdReq {
                    if_exists: false,
                    tenant: tenant.clone(),
                    db_id,
                    table_name: tbl_name.to_string(),
                    tb_id,
                };
                let res = mt.drop_table_by_id(plan).await;
                let err = res.unwrap_err();
                assert_eq!(
                    ErrorCode::UNKNOWN_TABLE,
                    ErrorCode::from(err).code(),
                    "drop table {} with if_exists=false again",
                    tbl_name
                );
            }

            info!("--- drop table with if_exists = true again, ok");
            {
                let plan = DropTableByIdReq {
                    if_exists: true,
                    tenant: tenant.clone(),
                    db_id,
                    table_name: tbl_name.to_string(),
                    tb_id,
                };
                mt.drop_table_by_id(plan.clone()).await?;
            }
        }

        info!("--- create or replace db1");
        {
            let old_created_on = Utc::now();
            let table = "test_replace";

            let key_dbid_tbname = DBIdTableName {
                db_id,
                table_name: table.to_string(),
            };

            let req = CreateTableReq {
                create_option: CreateOption::Create,
                name_ident: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: table.to_string(),
                },
                table_meta: table_meta(old_created_on),
                as_dropped: false,
            };

            let res = mt.create_table(req.clone()).await?;
            let old_table_id = res.table_id;

            let req = GetTableReq::new(&tenant, db_name, table);
            let res = mt.get_table(req).await?;
            assert_eq!(res.meta.created_on, old_created_on);

            let orig_table_id: u64 = get_kv_u64_data(mt.as_kv_api(), &key_dbid_tbname).await?;
            assert_eq!(orig_table_id, old_table_id);
            let key_table_id_to_name = TableIdToName {
                table_id: res.ident.table_id,
            };
            let ret_table_name_ident: DBIdTableName =
                get_kv_data(mt.as_kv_api(), &key_table_id_to_name).await?;
            assert_eq!(ret_table_name_ident, key_dbid_tbname);

            let created_on = Utc::now();

            let req = CreateTableReq {
                create_option: CreateOption::CreateOrReplace,
                name_ident: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: table.to_string(),
                },
                table_meta: table_meta(created_on),
                as_dropped: false,
            };

            let res = mt.create_table(req.clone()).await?;
            let table_id = res.table_id;

            // table meta has been changed
            let req = GetTableReq::new(&tenant, db_name, table);
            let res = mt.get_table(req).await?;
            assert_eq!(res.meta.created_on, created_on);

            assert_eq!(
                table_id,
                get_kv_u64_data(mt.as_kv_api(), &key_dbid_tbname).await?
            );
            let key_table_id_to_name = TableIdToName { table_id };
            let ret_table_name_ident: DBIdTableName =
                get_kv_data(mt.as_kv_api(), &key_table_id_to_name).await?;
            assert_eq!(ret_table_name_ident, key_dbid_tbname);
        }

        {
            info!("--- create table as dropped, undrop table by id, etc");

            // recall that there is no table named with "tbl_dropped"
            // - create table as dropped
            let created_on = Utc::now();
            let tbl_name = "tbl_dropped";
            let tbl_meta = {
                let mut v = table_meta(created_on);
                v.drop_on = Some(Utc::now());
                v
            };

            let create_table_req = CreateTableReq {
                create_option: CreateOption::Create,
                name_ident: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name.to_string(),
                },
                table_meta: tbl_meta,
                as_dropped: true,
            };
            let old_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
            let create_table_as_dropped_resp = mt.create_table(create_table_req.clone()).await?;

            // - verify that table created is invisible
            let req = GetTableReq::new(&tenant, db_name, tbl_name);
            let got = mt.get_table(req).await;
            use databend_common_meta_app::app_error::AppError;
            assert!(matches!(
                got.unwrap_err(),
                KVAppError::AppError(AppError::UnknownTable(_))
            ));

            // - verify other states are as expected
            let cur_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);
            assert!(create_table_as_dropped_resp.table_id >= 1, "table id >= 1");

            // -- verify this table is committpable

            {
                let commit_table_req = CommitTableMetaReq {
                    name_ident: create_table_req.name_ident.clone(),
                    db_id: create_table_as_dropped_resp.db_id,
                    table_id: create_table_as_dropped_resp.table_id,
                    prev_table_id: create_table_as_dropped_resp.prev_table_id,
                    orphan_table_name: create_table_as_dropped_resp.orphan_table_name.clone(),
                };

                mt.commit_table_meta(commit_table_req).await?;
                let req = GetTableReq::new(&tenant, db_name, tbl_name);
                // after "un-drop", table should be visible
                let tbl = mt.get_table(req).await?;
                // and it should be one which is specified by table id
                assert_eq!(tbl.ident.table_id, create_table_as_dropped_resp.table_id);
            }

            // -- create if not exist (as dropped) should work as expected
            {
                // recall that there is table of same name existing
                // case 1: table exists
                let create_table_if_not_exist_req = {
                    let mut req = create_table_req.clone();
                    req.create_option = CreateOption::CreateIfNotExists;
                    req
                };

                let create_if_not_exist_resp =
                    mt.create_table(create_table_if_not_exist_req).await?;
                // no new table should be created
                assert!(!create_if_not_exist_resp.new_table);
                // the tabled id that returned should be the same
                assert_eq!(
                    create_if_not_exist_resp.table_id,
                    create_table_as_dropped_resp.table_id
                );

                // table should still visible
                let req = GetTableReq::new(&tenant, db_name, tbl_name);
                let _ = mt.get_table(req).await?;

                // case 2: table does not exist
                // let's use a brand-new table name "not_exist"
                let create_table_if_not_exist_req = {
                    let mut req = create_table_req.clone();
                    req.name_ident.table_name = "not_exist".to_owned();
                    req.create_option = CreateOption::CreateIfNotExists;
                    req
                };

                let create_if_not_exist_resp =
                    mt.create_table(create_table_if_not_exist_req).await?;
                // new table should be created
                assert!(create_if_not_exist_resp.new_table);
                // table should not be visible
                let req = GetTableReq::new(&tenant, db_name, "not_exist");
                let got = mt.get_table(req).await;
                assert!(matches!(
                    got.unwrap_err(),
                    KVAppError::AppError(AppError::UnknownTable(_))
                ));
            }

            // -- create or replace (as dropped) should work as expected

            {
                let create_or_replace_req = {
                    let mut req = create_table_req.clone();
                    req.create_option = CreateOption::CreateOrReplace;
                    req
                };

                let create_or_replace_resp = mt.create_table(create_or_replace_req).await?;
                // since table of same name has been created, "new_table" (in the sense of table name) should be false
                assert!(!create_or_replace_resp.new_table);
                // but a table of different id should be created
                assert_ne!(
                    create_or_replace_resp.table_id,
                    create_table_as_dropped_resp.table_id
                );

                // the replaced table should be still visible:
                let req = GetTableReq::new(&tenant, db_name, tbl_name);
                let tbl = mt.get_table(req).await?;
                // the visible one should be the one before the create-or-replace-as-dropped
                assert_eq!(create_table_as_dropped_resp.table_id, tbl.ident.table_id);
                // but not the newly created (as dropped) one
                assert_ne!(create_or_replace_resp.table_id, tbl.ident.table_id);
            }
        }

        Ok(())
    }

    #[fastrace::trace]
    async fn table_drop_without_db_id_to_name<MT>(&self, mt: &MT) -> anyhow::Result<()>
    where MT: SchemaApi + kvapi::AsKVApi<Error = MetaError> {
        let mut util = Util::new(mt, "tenant1", "db1", "tb2", "JSON");

        info!("--- prepare db and table");
        {
            util.create_db().await?;
            let (_tid, _table_meta) = util.create_table().await?;
        }

        info!("--- drop db-id-to-name mapping to ensure dropping table does not rely on it");
        {
            let id_to_name_key = DatabaseIdToName { db_id: util.db_id };
            util.mt
                .as_kv_api()
                .upsert_kv(UpsertKV::delete(id_to_name_key.to_string_key()))
                .await?;
        }

        info!("--- drop table to ensure dropping table does not rely on db-id-to-name");
        util.drop_table_by_id().await?;
        Ok(())
    }

    #[fastrace::trace]
    async fn list_db_without_db_id_list<MT>(&self, mt: &MT) -> anyhow::Result<()>
    where MT: SchemaApi + kvapi::AsKVApi<Error = MetaError> {
        // test drop a db without db_id_list
        {
            let tenant_name = "tenant1";
            let tenant = Tenant::new_literal(tenant_name);

            let db = "db1";
            let mut util = Util::new(mt, tenant_name, db, "tb2", "JSON");
            util.create_db().await?;

            // remove db id list
            let dbid_idlist = DatabaseIdHistoryIdent::new(&tenant, db);
            util.mt
                .as_kv_api()
                .upsert_kv(UpsertKV::delete(dbid_idlist.to_string_key()))
                .await?;

            // drop db
            util.drop_db().await?;

            // after drop db, check if db id list has been added
            let value = util
                .mt
                .as_kv_api()
                .get_kv(&dbid_idlist.to_string_key())
                .await?;

            assert!(value.is_some());
            let seqv = value.unwrap();
            let db_id_list: DbIdList = deserialize_struct(&seqv.data)?;
            assert_eq!(db_id_list.id_list[0], util.db_id);
        }
        // test get_database_history can return db without db_id_list
        {
            let tenant2_name = "tenant2";
            let tenant2 = Tenant::new_literal(tenant2_name);

            let db = "db2";
            let mut util = Util::new(mt, tenant2_name, db, "tb2", "JSON");
            util.create_db().await?;

            // remove db id list
            let dbid_idlist = DatabaseIdHistoryIdent::new(&tenant2, db);

            util.mt
                .as_kv_api()
                .upsert_kv(UpsertKV::delete(dbid_idlist.to_string_key()))
                .await?;

            let res = mt
                .get_database_history(ListDatabaseReq {
                    tenant: Tenant::new_or_err(tenant2_name, func_name!())?,
                    filter: None,
                })
                .await?;

            // check if get_database_history return db_id
            let mut found = false;
            for db_info in res {
                if db_info.ident.db_id == util.db_id {
                    found = true;
                    break;
                }
            }

            assert!(found);
        }
        Ok(())
    }

    #[fastrace::trace]
    async fn drop_table_without_table_id_list<MT>(&self, mt: &MT) -> anyhow::Result<()>
    where MT: SchemaApi + kvapi::AsKVApi<Error = MetaError> {
        // test drop a table without table_id_list
        let tenant = "tenant1";
        let db = "db1";
        let table = "tb1";
        let mut util = Util::new(mt, tenant, db, table, "JSON");
        util.create_db().await?;
        let (tid, _table_meta) = util.create_table().await?;

        // remove db id list
        let table_id_idlist = TableIdHistoryIdent {
            database_id: util.db_id,
            table_name: table.to_string(),
        };
        util.mt
            .as_kv_api()
            .upsert_kv(UpsertKV::delete(table_id_idlist.to_string_key()))
            .await?;

        // drop table
        util.drop_table_by_id().await?;

        // after drop table, check if table id list has been added
        let value = util
            .mt
            .as_kv_api()
            .get_kv(&table_id_idlist.to_string_key())
            .await?;

        assert!(value.is_some());
        let seqv = value.unwrap();
        let id_list: TableIdList = deserialize_struct(&seqv.data)?;
        assert_eq!(id_list.id_list[0], tid);

        Ok(())
    }

    #[fastrace::trace]
    async fn table_rename<MT: SchemaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        let db1_name = "db1";
        let tb2_name = "tb2";
        let db2_name = "db2";
        let tb3_name = "tb3";

        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };

        let options = || maplit::btreemap! {"opt‐1".into() => "val-1".into()};

        let rename_db1tb2_to_db1tb3 = |if_exists| RenameTableReq {
            if_exists,
            name_ident: TableNameIdent {
                tenant: tenant.clone(),
                db_name: db1_name.to_string(),
                table_name: tb2_name.to_string(),
            },
            new_db_name: db1_name.to_string(),
            new_table_name: tb3_name.to_string(),
        };

        let table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: options(),
            created_on,
            ..TableMeta::default()
        };

        info!("--- rename table on unknown db");
        {
            let got = mt.rename_table(rename_db1tb2_to_db1tb3(false)).await;
            debug!("--- rename table on unknown database got: {:?}", got);

            assert!(got.is_err());
            assert_eq!(
                ErrorCode::UNKNOWN_DATABASE,
                ErrorCode::from(got.unwrap_err()).code()
            );
        }

        info!("--- prepare db");
        {
            let plan = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant, db1_name),
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let res = mt.create_database(plan).await?;
            info!("create database res: {:?}", res);
        }

        let created_on = Utc::now();
        let create_tb2_req = CreateTableReq {
            create_option: CreateOption::Create,
            name_ident: TableNameIdent {
                tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                db_name: db1_name.to_string(),
                table_name: tb2_name.to_string(),
            },
            table_meta: table_meta(created_on),
            as_dropped: false,
        };

        info!("--- create table for rename");
        let tb_ident = {
            let old_db = mt.get_database(Self::req_get_db(&tenant, db1_name)).await?;
            mt.create_table(create_tb2_req.clone()).await?;
            let cur_db = mt.get_database(Self::req_get_db(&tenant, db1_name)).await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);

            let req = GetTableReq::new(&tenant, db1_name, tb2_name);
            let got = mt.get_table(req).await?;
            got.ident
        };

        info!("--- rename table, ok");
        {
            let old_db = mt.get_database(Self::req_get_db(&tenant, db1_name)).await?;
            mt.rename_table(rename_db1tb2_to_db1tb3(false)).await?;
            let cur_db = mt.get_database(Self::req_get_db(&tenant, db1_name)).await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);

            let req = GetTableReq::new(&tenant, db1_name, tb3_name);
            let got = mt.get_table(req).await?;

            let want = TableInfo {
                ident: tb_ident,
                desc: format!("'{}'.'{}'", db1_name, tb3_name),
                name: tb3_name.into(),
                meta: table_meta(created_on),
                tenant: tenant_name.to_string(),
                ..Default::default()
            };

            assert_meta_eq_without_updated!(want, got.as_ref().clone(), "get renamed table");

            info!("--- get old table after rename");
            {
                let req = GetTableReq::new(&tenant, db1_name, tb2_name);
                let res = mt.get_table(req).await;
                let err = res.err().unwrap();
                assert_eq!(
                    ErrorCode::UnknownTable("").code(),
                    ErrorCode::from(err).code()
                );
            }
        }

        info!("--- db1,tb2(nil) -> db1,tb3(no_nil), error");
        {
            let res = mt.rename_table(rename_db1tb2_to_db1tb3(false)).await;
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::UNKNOWN_TABLE,
                ErrorCode::from(err).code(),
                "rename table {} again",
                tb2_name
            );
        }

        info!("--- db1,tb2(nil) -> db1,tb3(no_nil), with if_exist=true, OK");
        {
            mt.rename_table(rename_db1tb2_to_db1tb3(true)).await?;
        }

        info!("--- create db1,db2, ok");
        let tb_ident2 = {
            let old_db = mt.get_database(Self::req_get_db(&tenant, db1_name)).await?;
            mt.create_table(create_tb2_req.clone()).await?;
            let cur_db = mt.get_database(Self::req_get_db(&tenant, db1_name)).await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);

            let got = mt
                .get_table((tenant_name, db1_name, tb2_name).into())
                .await?;
            assert_ne!(tb_ident.table_id, got.ident.table_id);
            assert_ne!(tb_ident.seq, got.ident.seq);
            got.ident
        };

        info!("--- db1,tb2(no_nil) -> db1,tb3(no_nil), error");
        {
            let res = mt.rename_table(rename_db1tb2_to_db1tb3(false)).await;
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::TABLE_ALREADY_EXISTS,
                ErrorCode::from(err).code(),
                "rename table {} again after recreate",
                tb2_name
            );
        }

        info!("--- db1,tb2(no_nil) -> db1,tb3(no_nil), if_exists=true, error");
        {
            let res = mt.rename_table(rename_db1tb2_to_db1tb3(true)).await;
            let err = res.unwrap_err();
            assert_eq!(
                ErrorCode::TABLE_ALREADY_EXISTS,
                ErrorCode::from(err).code(),
                "rename table {} again after recreate",
                tb2_name
            );
        }

        info!("--- rename table to unknown db, error");
        {
            let req = RenameTableReq {
                if_exists: false,
                name_ident: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db1_name.to_string(),
                    table_name: tb2_name.to_string(),
                },
                new_db_name: db2_name.to_string(),
                new_table_name: tb3_name.to_string(),
            };
            let res = mt.rename_table(req.clone()).await;
            debug!("--- rename table to other db got: {:?}", res);

            assert!(res.is_err());
            assert_eq!(
                ErrorCode::UNKNOWN_DATABASE,
                ErrorCode::from(res.unwrap_err()).code()
            );
        }

        info!("--- prepare other db");
        {
            let plan = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant, db2_name),
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            mt.create_database(plan).await?;
        }

        info!("--- rename table to other db, ok");
        {
            let req = RenameTableReq {
                if_exists: false,
                name_ident: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db1_name.to_string(),
                    table_name: tb2_name.to_string(),
                },
                new_db_name: db2_name.to_string(),
                new_table_name: tb3_name.to_string(),
            };
            let old_db1 = mt.get_database(Self::req_get_db(&tenant, db1_name)).await?;
            let old_db2 = mt.get_database(Self::req_get_db(&tenant, db2_name)).await?;
            mt.rename_table(req.clone()).await?;
            let cur_db1 = mt.get_database(Self::req_get_db(&tenant, db1_name)).await?;
            let cur_db2 = mt.get_database(Self::req_get_db(&tenant, db2_name)).await?;
            assert!(old_db1.ident.seq < cur_db1.ident.seq);
            assert!(old_db2.ident.seq < cur_db2.ident.seq);

            let got = mt
                .get_table((tenant_name, db2_name, tb3_name).into())
                .await?;
            let want = TableInfo {
                ident: tb_ident2,
                desc: format!("'{}'.'{}'", db2_name, tb3_name),
                name: tb3_name.into(),
                meta: table_meta(created_on),
                tenant: tenant_name.to_string(),
                ..Default::default()
            };
            assert_meta_eq_without_updated!(want, got.as_ref().clone(), "get renamed table");
        }

        Ok(())
    }

    #[fastrace::trace]
    async fn table_update_meta<MT: SchemaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        let db_name = "db1";
        let tbl_name = "tb2";

        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };

        let table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: Default::default(),
            created_on,
            ..TableMeta::default()
        };

        info!("--- prepare db");
        {
            let plan = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let res = mt.create_database(plan).await?;
            info!("create database res: {:?}", res);

            assert_eq!(1, res.db_id, "first database id is 1");
        }

        info!("--- create and get table");
        {
            let created_on = Utc::now();

            let req = CreateTableReq {
                create_option: CreateOption::Create,
                name_ident: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name.to_string(),
                },
                table_meta: table_meta(created_on),
                as_dropped: false,
            };

            let _tb_ident_2 = {
                let old_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
                let res = mt.create_table(req.clone()).await?;
                let cur_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
                assert!(old_db.ident.seq < cur_db.ident.seq);
                assert!(res.table_id >= 1, "table id >= 1");
                let tb_id = res.table_id;

                let got = mt
                    .get_table((tenant_name, db_name, tbl_name).into())
                    .await?;
                let seq = got.ident.seq;

                let ident = TableIdent::new(tb_id, seq);

                let want = TableInfo {
                    ident,
                    desc: format!("'{}'.'{}'", db_name, tbl_name),
                    name: tbl_name.into(),
                    meta: table_meta(created_on),
                    tenant: tenant_name.to_string(),
                    ..Default::default()
                };
                assert_meta_eq_without_updated!(want, got.as_ref().clone(), "get created table");
                ident
            };
        }

        info!("--- update table meta");
        {
            info!("--- update table meta, normal case");
            {
                let table = mt
                    .get_table((tenant_name, "db1", "tb2").into())
                    .await
                    .unwrap();

                let mut new_table_meta = table.meta.clone();
                let table_statistics = TableStatistics {
                    data_bytes: 1,
                    number_of_segments: Some(2),
                    number_of_blocks: Some(200),
                    ..Default::default()
                };
                new_table_meta.statistics = table_statistics;
                let table_id = table.ident.table_id;
                let table_version = table.ident.seq;
                let req = UpdateTableMetaReq {
                    table_id,
                    seq: MatchSeq::Exact(table_version),
                    new_table_meta: new_table_meta.clone(),
                };
                mt.update_multi_table_meta(UpdateMultiTableMetaReq {
                    update_table_metas: vec![(req, table.as_ref().clone())],
                    ..Default::default()
                })
                .await?
                .unwrap();

                let table = mt
                    .get_table((tenant_name, "db1", "tb2").into())
                    .await
                    .unwrap();
                assert_eq!(table.meta, new_table_meta);
            }

            info!("--- update table meta: version mismatch");
            {
                let table = mt
                    .get_table((tenant_name, "db1", "tb2").into())
                    .await
                    .unwrap();

                let new_table_meta = table.meta.clone();
                let table_id = table.ident.table_id;
                let table_version = table.ident.seq;
                let req = UpdateTableMetaReq {
                    table_id,
                    seq: MatchSeq::Exact(table_version + 1),
                    new_table_meta: new_table_meta.clone(),
                };
                let res = mt
                    .update_multi_table_meta(UpdateMultiTableMetaReq {
                        update_table_metas: vec![(req, table.as_ref().clone())],
                        ..Default::default()
                    })
                    .await?;

                let err = res.unwrap_err();

                assert!(!err.is_empty());
            }

            info!("--- update table meta, with upsert file req");
            {
                let table = mt
                    .get_table((tenant_name, "db1", "tb2").into())
                    .await
                    .unwrap();

                let mut new_table_meta = table.meta.clone();
                let table_statistics = TableStatistics {
                    data_bytes: 1,
                    ..Default::default()
                };

                new_table_meta.statistics = table_statistics;
                let table_id = table.ident.table_id;
                let table_version = table.ident.seq;

                let mut file_info = BTreeMap::new();
                file_info.insert("test".to_owned(), TableCopiedFileInfo {
                    etag: Some("tag".to_string()),
                    content_length: 1,
                    last_modified: None,
                });

                let upsert_source_table = UpsertTableCopiedFileReq {
                    file_info,
                    expire_at: None,
                    fail_if_duplicated: true,
                };

                let req = UpdateTableMetaReq {
                    table_id,
                    seq: MatchSeq::Exact(table_version),
                    new_table_meta: new_table_meta.clone(),
                };
                mt.update_multi_table_meta(UpdateMultiTableMetaReq {
                    update_table_metas: vec![(req, table.as_ref().clone())],
                    copied_files: vec![(table_id, upsert_source_table)],
                    ..Default::default()
                })
                .await?
                .unwrap();

                let table = mt
                    .get_table((tenant_name, "db1", "tb2").into())
                    .await
                    .unwrap();
                assert_eq!(table.meta, new_table_meta);
            }

            info!("--- update table meta, with non-duplicated upsert file, no duplication");
            {
                let table = mt
                    .get_table((tenant_name, "db1", "tb2").into())
                    .await
                    .unwrap();

                let mut new_table_meta = table.meta.clone();
                let table_statistics = TableStatistics {
                    data_bytes: 1,
                    ..Default::default()
                };

                new_table_meta.statistics = table_statistics;
                let table_id = table.ident.table_id;
                let table_version = table.ident.seq;

                let mut file_info = BTreeMap::new();
                file_info.insert("not_exist".to_owned(), TableCopiedFileInfo {
                    etag: Some("tag_not_exist".to_string()),
                    content_length: 1,
                    last_modified: None,
                });

                let upsert_source_table = UpsertTableCopiedFileReq {
                    file_info,
                    expire_at: None,
                    fail_if_duplicated: true,
                };
                let req = UpdateTableMetaReq {
                    table_id,
                    seq: MatchSeq::Exact(table_version),
                    new_table_meta: new_table_meta.clone(),
                };
                mt.update_multi_table_meta(UpdateMultiTableMetaReq {
                    update_table_metas: vec![(req, table.as_ref().clone())],
                    copied_files: vec![(table_id, upsert_source_table)],
                    ..Default::default()
                })
                .await?
                .unwrap();

                let table = mt
                    .get_table((tenant_name, "db1", "tb2").into())
                    .await
                    .unwrap();
                assert_eq!(table.meta, new_table_meta);
            }

            info!("--- update table meta, with duplicated upsert files");
            {
                let table = mt
                    .get_table((tenant_name, "db1", "tb2").into())
                    .await
                    .unwrap();

                let mut new_table_meta = table.meta.clone();
                let table_statistics = TableStatistics {
                    data_bytes: 1,
                    ..Default::default()
                };

                new_table_meta.statistics = table_statistics;
                let table_id = table.ident.table_id;
                let table_version = table.ident.seq;

                let mut file_info = BTreeMap::new();
                file_info.insert("test".to_owned(), TableCopiedFileInfo {
                    etag: Some("tag".to_string()),
                    content_length: 1,
                    last_modified: None,
                });

                let upsert_source_table = UpsertTableCopiedFileReq {
                    file_info,
                    expire_at: None,
                    fail_if_duplicated: true,
                };
                let req = UpdateTableMetaReq {
                    table_id,
                    seq: MatchSeq::Exact(table_version),
                    new_table_meta: new_table_meta.clone(),
                };
                let result = mt
                    .update_multi_table_meta(UpdateMultiTableMetaReq {
                        update_table_metas: vec![(req, table.as_ref().clone())],
                        copied_files: vec![(table_id, upsert_source_table)],
                        ..Default::default()
                    })
                    .await;
                let err = result.unwrap_err();
                let err = ErrorCode::from(err);
                assert_eq!(ErrorCode::UNRESOLVABLE_CONFLICT, err.code());
            }
        }
        Ok(())
    }

    #[fastrace::trace]
    async fn table_update_mask_policy<
        MT: SchemaApi + DatamaskApi + kvapi::AsKVApi<Error = MetaError>,
    >(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        let db_name = "db1";
        let tbl_name_1 = "tb1";
        let tbl_name_2 = "tb2";
        let mask_name_1 = "mask1";
        let mask_name_2 = "mask2";

        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };

        let table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: Default::default(),
            created_on,
            ..TableMeta::default()
        };

        info!("--- prepare db");
        {
            let plan = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let res = mt.create_database(plan).await?;
            info!("create database res: {:?}", res);

            assert_eq!(1, res.db_id, "first database id is 1");
        }

        let created_on = Utc::now();
        info!("--- create table");
        {
            let req = CreateTableReq {
                create_option: CreateOption::Create,
                name_ident: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name_1.to_string(),
                },
                table_meta: table_meta(created_on),
                as_dropped: false,
            };
            let _res = mt.create_table(req.clone()).await?;

            let req = CreateTableReq {
                create_option: CreateOption::Create,
                name_ident: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name_2.to_string(),
                },
                table_meta: table_meta(created_on),
                as_dropped: false,
            };
            let _res = mt.create_table(req.clone()).await?;
        }

        info!("--- create mask policy");
        {
            let req = CreateDatamaskReq {
                create_option: CreateOption::CreateIfNotExists,
                name: DataMaskNameIdent::new(tenant.clone(), mask_name_1.to_string()),
                args: vec![],
                return_type: "".to_string(),
                body: "".to_string(),
                comment: None,
                create_on: created_on,
            };
            mt.create_data_mask(req).await?;

            let req = CreateDatamaskReq {
                create_option: CreateOption::CreateIfNotExists,
                name: DataMaskNameIdent::new(tenant.clone(), mask_name_2.to_string()),
                args: vec![],
                return_type: "".to_string(),
                body: "".to_string(),
                comment: None,
                create_on: created_on,
            };
            mt.create_data_mask(req).await?;
        }

        let table_id_1;
        info!("--- apply mask1 policy to table 1 and check");
        {
            let req = GetTableReq {
                inner: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name_1.to_string(),
                },
            };
            let res = mt.get_table(req).await?;
            let table_id = res.ident.table_id;
            table_id_1 = table_id;

            let req = SetTableColumnMaskPolicyReq {
                tenant: tenant.clone(),
                seq: MatchSeq::Exact(res.ident.seq),
                table_id,
                column: "number".to_string(),
                action: SetTableColumnMaskPolicyAction::Set(mask_name_1.to_string(), None),
            };
            let _ = mt.set_table_column_mask_policy(req).await?;
            // check table meta
            let req = GetTableReq {
                inner: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name_1.to_string(),
                },
            };
            let res = mt.get_table(req).await?;
            let mut expect_column_mask_policy = BTreeMap::new();
            expect_column_mask_policy.insert("number".to_string(), mask_name_1.to_string());
            assert_eq!(res.meta.column_mask_policy, Some(expect_column_mask_policy));
            // check mask policy id list
            let id_list_key = MaskPolicyTableIdListIdent::new(tenant.clone(), mask_name_1);
            let id_list: MaskpolicyTableIdList = get_kv_data(mt.as_kv_api(), &id_list_key).await?;
            let mut expect_id_list = BTreeSet::new();
            expect_id_list.insert(table_id);
            assert_eq!(id_list.id_list, expect_id_list);
        }

        let table_id_2;
        info!("--- apply mask1 policy to table 2 and check");
        {
            let req = GetTableReq {
                inner: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name_2.to_string(),
                },
            };
            let res = mt.get_table(req).await?;
            let table_id = res.ident.table_id;
            table_id_2 = table_id;

            let req = SetTableColumnMaskPolicyReq {
                tenant: tenant.clone(),
                seq: MatchSeq::Exact(res.ident.seq),
                table_id,
                column: "number".to_string(),
                action: SetTableColumnMaskPolicyAction::Set(mask_name_1.to_string(), None),
            };
            let _ = mt.set_table_column_mask_policy(req).await?;
            // check table meta
            let req = GetTableReq {
                inner: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name_2.to_string(),
                },
            };
            let res = mt.get_table(req).await?;
            let mut expect_column_mask_policy = BTreeMap::new();
            expect_column_mask_policy.insert("number".to_string(), mask_name_1.to_string());
            assert_eq!(res.meta.column_mask_policy, Some(expect_column_mask_policy));
            // check mask policy id list
            let id_list_key = MaskPolicyTableIdListIdent::new(tenant.clone(), mask_name_1);
            let id_list: MaskpolicyTableIdList = get_kv_data(mt.as_kv_api(), &id_list_key).await?;
            let mut expect_id_list = BTreeSet::new();
            expect_id_list.insert(table_id);
            expect_id_list.insert(table_id_1);
            assert_eq!(id_list.id_list, expect_id_list);
        }

        info!("--- apply mask2 policy to table 1 and check");
        {
            let req = GetTableReq {
                inner: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name_1.to_string(),
                },
            };
            let res = mt.get_table(req).await?;

            let req = SetTableColumnMaskPolicyReq {
                tenant: tenant.clone(),
                seq: MatchSeq::Exact(res.ident.seq),
                table_id: table_id_1,
                column: "number".to_string(),
                action: SetTableColumnMaskPolicyAction::Set(
                    mask_name_2.to_string(),
                    Some(mask_name_1.to_string()),
                ),
            };
            let _ = mt.set_table_column_mask_policy(req).await?;
            // check table meta
            let req = GetTableReq {
                inner: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name_1.to_string(),
                },
            };
            let res = mt.get_table(req).await?;
            let mut expect_column_mask_policy = BTreeMap::new();
            expect_column_mask_policy.insert("number".to_string(), mask_name_2.to_string());
            assert_eq!(res.meta.column_mask_policy, Some(expect_column_mask_policy));
            // check mask policy id list
            let id_list_key = MaskPolicyTableIdListIdent::new(tenant.clone(), mask_name_1);
            let id_list: MaskpolicyTableIdList = get_kv_data(mt.as_kv_api(), &id_list_key).await?;
            let mut expect_id_list = BTreeSet::new();
            expect_id_list.insert(table_id_2);
            assert_eq!(id_list.id_list, expect_id_list);

            let id_list_key = MaskPolicyTableIdListIdent::new(tenant.clone(), mask_name_2);
            let id_list: MaskpolicyTableIdList = get_kv_data(mt.as_kv_api(), &id_list_key).await?;
            let mut expect_id_list = BTreeSet::new();
            expect_id_list.insert(table_id_1);
            assert_eq!(id_list.id_list, expect_id_list);
        }

        info!("--- unset mask policy of table 1 and check");
        {
            let req = GetTableReq {
                inner: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name_1.to_string(),
                },
            };
            let res = mt.get_table(req).await?;

            let req = SetTableColumnMaskPolicyReq {
                tenant: tenant.clone(),
                seq: MatchSeq::Exact(res.ident.seq),
                table_id: table_id_1,
                column: "number".to_string(),
                action: SetTableColumnMaskPolicyAction::Unset(mask_name_2.to_string()),
            };
            let _ = mt.set_table_column_mask_policy(req).await?;

            // check table meta
            let req = GetTableReq {
                inner: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name_1.to_string(),
                },
            };
            let res = mt.get_table(req).await?;
            assert_eq!(res.meta.column_mask_policy, None);

            // check mask policy id list
            let id_list_key = MaskPolicyTableIdListIdent::new(tenant.clone(), mask_name_2);
            let id_list: MaskpolicyTableIdList = get_kv_data(mt.as_kv_api(), &id_list_key).await?;
            let expect_id_list = BTreeSet::new();
            assert_eq!(id_list.id_list, expect_id_list);
        }

        info!("--- drop mask policy check");
        {
            let req = DropDatamaskReq {
                if_exists: true,
                name: DataMaskNameIdent::new(tenant.clone(), mask_name_1),
            };

            mt.drop_data_mask(req).await?;

            // check table meta
            let req = GetTableReq {
                inner: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name_2.to_string(),
                },
            };
            let res = mt.get_table(req).await?;
            assert_eq!(res.meta.column_mask_policy, None);

            // check mask policy id list
            let id_list_key = MaskPolicyTableIdListIdent::new(tenant.clone(), mask_name_1);
            let id_list: Result<MaskpolicyTableIdList, KVAppError> =
                get_kv_data(mt.as_kv_api(), &id_list_key).await;
            assert!(id_list.is_err())
        }

        info!("--- create or replace mask policy");
        {
            let mask_name = "replace_mask";
            let name = DataMaskNameIdent::new(tenant.clone(), mask_name);
            let req = CreateDatamaskReq {
                create_option: CreateOption::CreateIfNotExists,
                name: name.clone(),
                args: vec![],
                return_type: "".to_string(),
                body: "".to_string(),
                comment: Some("before".to_string()),
                create_on: created_on,
            };
            mt.create_data_mask(req).await?;
            let old_id: u64 = get_kv_u64_data(mt.as_kv_api(), &name).await?;

            let id_key = DataMaskIdIdent::new(&tenant, old_id);

            let meta: DatamaskMeta = get_kv_data(mt.as_kv_api(), &id_key).await?;
            assert_eq!(meta.comment, Some("before".to_string()));

            let req = CreateDatamaskReq {
                create_option: CreateOption::CreateOrReplace,
                name: name.clone(),
                args: vec![],
                return_type: "".to_string(),
                body: "".to_string(),
                comment: Some("after".to_string()),
                create_on: created_on,
            };
            mt.create_data_mask(req).await?;

            // assert old id key has been deleted
            let meta: Result<DatamaskMeta, KVAppError> = get_kv_data(mt.as_kv_api(), &id_key).await;
            assert!(meta.is_err());

            let id: u64 = get_kv_u64_data(mt.as_kv_api(), &name).await?;
            assert_ne!(old_id, id);

            let id_key = DataMaskIdIdent::new(&tenant, id);

            let meta: DatamaskMeta = get_kv_data(mt.as_kv_api(), &id_key).await?;
            assert_eq!(meta.comment, Some("after".to_string()));
        }

        Ok(())
    }

    #[fastrace::trace]
    async fn table_upsert_option<MT: SchemaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        let db_name = "db1";
        let tbl_name = "tb2";

        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };

        let options = || maplit::btreemap! {"opt‐1".into() => "val-1".into()};

        let table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: options(),
            created_on,
            ..TableMeta::default()
        };

        info!("--- prepare db");
        {
            let plan = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let res = mt.create_database(plan).await?;
            info!("create database res: {:?}", res);

            assert_eq!(1, res.db_id, "first database id is 1");
        }

        info!("--- create and get table");
        {
            let created_on = Utc::now();

            let req = CreateTableReq {
                create_option: CreateOption::Create,
                name_ident: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name.to_string(),
                },
                table_meta: table_meta(created_on),
                as_dropped: false,
            };

            let _tb_ident_2 = {
                let old_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
                let res = mt.create_table(req.clone()).await?;
                let cur_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
                assert!(old_db.ident.seq < cur_db.ident.seq);
                assert!(res.table_id >= 1, "table id >= 1");
                let tb_id = res.table_id;

                let got = mt
                    .get_table((tenant_name, db_name, tbl_name).into())
                    .await?;
                let seq = got.ident.seq;

                let ident = TableIdent::new(tb_id, seq);

                let want = TableInfo {
                    ident,
                    desc: format!("'{}'.'{}'", db_name, tbl_name),
                    name: tbl_name.into(),
                    meta: table_meta(created_on),
                    tenant: tenant_name.to_string(),
                    ..Default::default()
                };
                assert_meta_eq_without_updated!(want, got.as_ref().clone(), "get created table");
                ident
            };
        }

        info!("--- upsert table options");
        {
            info!("--- upsert table options with key1=val1");
            {
                let table = mt
                    .get_table((tenant_name, "db1", "tb2").into())
                    .await
                    .unwrap();

                mt.upsert_table_option(UpsertTableOptionReq::new(&table.ident, "key1", "val1"))
                    .await?;

                let table = mt
                    .get_table((tenant_name, "db1", "tb2").into())
                    .await
                    .unwrap();
                assert_eq!(table.options().get("key1"), Some(&"val1".into()));
            }

            info!("--- upsert table options with key1=val1");
            {
                let table = mt
                    .get_table((tenant_name, "db1", "tb2").into())
                    .await
                    .unwrap();

                let got = mt
                    .upsert_table_option(UpsertTableOptionReq::new(
                        &TableIdent {
                            table_id: table.ident.table_id,
                            seq: table.ident.seq - 1,
                        },
                        "key1",
                        "val2",
                    ))
                    .await;

                let err = got.unwrap_err();
                let err = ErrorCode::from(err);

                assert_eq!(ErrorCode::TABLE_VERSION_MISMATCHED, err.code());

                // table is not affected.
                let table = mt
                    .get_table((tenant_name, "db1", "tb2").into())
                    .await
                    .unwrap();
                assert_eq!(table.options().get("key1"), Some(&"val1".into()));
            }

            info!("--- upsert table options with not exist table id");
            {
                let table = mt
                    .get_table((tenant_name, "db1", "tb2").into())
                    .await
                    .unwrap();

                let got = mt
                    .upsert_table_option(UpsertTableOptionReq::new(
                        &TableIdent {
                            table_id: 1024,
                            seq: table.ident.seq - 1,
                        },
                        "key1",
                        "val2",
                    ))
                    .await;

                let err = got.unwrap_err();
                let err = ErrorCode::from(err);

                assert_eq!(ErrorCode::UNKNOWN_TABLE_ID, err.code());

                // table is not affected.
                let table = mt
                    .get_table((tenant_name, "db1", "tb2").into())
                    .await
                    .unwrap();
                assert_eq!(table.options().get("key1"), Some(&"val1".into()));
            }
        }
        Ok(())
    }

    #[fastrace::trace]
    async fn database_drop_out_of_retention_time_history<
        MT: SchemaApi + kvapi::AsKVApi<Error = MetaError>,
    >(
        self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1_database_drop_out_of_retention_time_history";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        let db_name = "db1_database_drop_out_of_retention_time_history";
        let db_name_ident = DatabaseNameIdent::new(&tenant, db_name);

        info!("--- create and drop db1");
        {
            let drop_on = Some(Utc::now() - Duration::days(1));

            // first create database
            let req = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: db_name_ident.clone(),
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    // drop_on,
                    ..Default::default()
                },
            };

            let res = mt.create_database(req).await?;
            let db_id = res.db_id;
            info!("create database res: {:?}", res);

            let res = mt
                .get_database_history(ListDatabaseReq {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    filter: None,
                })
                .await?;

            // assert not return out of retention time data
            assert_eq!(res.len(), 1);

            let drop_data = DatabaseMeta {
                engine: "github".to_string(),
                drop_on,
                ..Default::default()
            };
            let id_key = DatabaseId { db_id };
            let data = serialize_struct(&drop_data)?;
            upsert_test_data(mt.as_kv_api(), &id_key, data).await?;

            let res = mt
                .get_database_history(ListDatabaseReq {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    filter: None,
                })
                .await?;

            // assert not return out of retention time data
            assert_eq!(res.len(), 0);
        }

        Ok(())
    }

    #[fastrace::trace]
    async fn create_out_of_retention_time_db<MT: SchemaApi + kvapi::AsKVApi<Error = MetaError>>(
        self,
        mt: &MT,
        db_name: DatabaseNameIdent,
        drop_on: Option<DateTime<Utc>>,
        delete: bool,
    ) -> anyhow::Result<()> {
        let req = CreateDatabaseReq {
            create_option: CreateOption::Create,
            name_ident: db_name.clone(),
            meta: DatabaseMeta {
                engine: "github".to_string(),
                // drop_on,
                ..Default::default()
            },
        };

        let res = mt.create_database(req).await?;
        if drop_on.is_some() {
            let db_id = res.db_id;
            info!("create database res: {:?}", res);

            let drop_data = DatabaseMeta {
                engine: "github".to_string(),
                drop_on,
                ..Default::default()
            };
            let id_key = DatabaseId { db_id };
            let data = serialize_struct(&drop_data)?;
            upsert_test_data(mt.as_kv_api(), &id_key, data).await?;

            if delete {
                delete_test_data(mt.as_kv_api(), &db_name).await?;
            }
        }
        Ok(())
    }

    #[fastrace::trace]
    async fn database_gc_out_of_retention_time<
        MT: SchemaApi + kvapi::AsKVApi<Error = MetaError>,
    >(
        self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1_database_gc_out_of_retention_time";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        let db_name = "db1_database_gc_out_of_retention_time";
        let db_name_ident1 = DatabaseNameIdent::new(&tenant, db_name);

        let dbid_idlist1 = DatabaseIdHistoryIdent::new(&tenant, db_name);

        let db_name2 = "db2_database_gc_out_of_retention_time";
        let db_name_ident2 = DatabaseNameIdent::new(&tenant, db_name2);

        let dbid_idlist2 = DatabaseIdHistoryIdent::new(&tenant, db_name2);

        let drop_on = Some(Utc::now() - Duration::days(1));

        // create db_name_ident1 with two dropped table
        self.create_out_of_retention_time_db(mt, db_name_ident1.clone(), drop_on, true)
            .await?;
        self.create_out_of_retention_time_db(
            mt,
            db_name_ident1.clone(),
            Some(Utc::now() - Duration::days(2)),
            false,
        )
        .await?;
        // create db_name_ident2 with one dropped value and one non-dropped table
        self.create_out_of_retention_time_db(mt, db_name_ident2.clone(), drop_on, true)
            .await?;
        self.create_out_of_retention_time_db(mt, db_name_ident2.clone(), None, false)
            .await?;

        let id_list: DbIdList = get_kv_data(mt.as_kv_api(), &dbid_idlist1).await?;
        assert_eq!(id_list.len(), 2);
        let old_id_list = id_list.id_list().clone();

        let id_list: DbIdList = get_kv_data(mt.as_kv_api(), &dbid_idlist2).await?;
        assert_eq!(id_list.len(), 2);

        {
            let req = ListDroppedTableReq {
                inner: DatabaseNameIdent::new(&tenant, ""),
                filter: TableInfoFilter::AllDroppedTables(None),
                limit: None,
            };
            let resp = mt.get_drop_table_infos(req).await?;

            let req = GcDroppedTableReq {
                tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                drop_ids: resp.drop_ids.clone(),
            };
            let _resp = mt.gc_drop_tables(req).await?;
        }

        // assert db id list key has been removed
        let id_list: Result<DbIdList, KVAppError> =
            get_kv_data(mt.as_kv_api(), &dbid_idlist1).await;
        assert!(id_list.is_err());

        // assert old db meta and id to name mapping has been removed
        for db_id in old_id_list.iter() {
            let id_key = DatabaseId { db_id: *db_id };
            let id_mapping = DatabaseIdToName { db_id: *db_id };

            let meta_res: Result<DatabaseMeta, KVAppError> =
                get_kv_data(mt.as_kv_api(), &id_key).await;
            assert!(meta_res.is_err());

            let mapping_res: Result<DatabaseNameIdentRaw, KVAppError> =
                get_kv_data(mt.as_kv_api(), &id_mapping).await;
            assert!(mapping_res.is_err());
        }

        let id_list: DbIdList = get_kv_data(mt.as_kv_api(), &dbid_idlist2).await?;
        assert_eq!(id_list.len(), 1);

        Ok(())
    }

    /// Return table id and table meta
    #[fastrace::trace]
    async fn create_out_of_retention_time_table<
        MT: SchemaApi + kvapi::AsKVApi<Error = MetaError>,
    >(
        self,
        mt: &MT,
        name_ident: TableNameIdent,
        dbid_tbname: DBIdTableName,
        drop_on: Option<DateTime<Utc>>,
        delete: bool,
    ) -> anyhow::Result<(u64, TableMeta)> {
        let created_on = Utc::now();
        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };

        let create_table_meta = TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            created_on,
            ..TableMeta::default()
        };

        let req = CreateTableReq {
            create_option: CreateOption::Create,
            name_ident,
            table_meta: create_table_meta.clone(),
            as_dropped: false,
        };

        let res = mt.create_table(req).await?;
        let table_id = res.table_id;
        info!("create table res: {:?}", res);

        let drop_data = TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            created_on,
            drop_on,
            ..TableMeta::default()
        };

        let id_key = TableId { table_id };
        let data = serialize_struct(&drop_data)?;
        upsert_test_data(mt.as_kv_api(), &id_key, data).await?;

        if delete {
            delete_test_data(mt.as_kv_api(), &dbid_tbname).await?;
        }
        Ok((table_id, drop_data))
    }

    #[fastrace::trace]
    async fn table_gc_out_of_retention_time<MT: SchemaApi + kvapi::AsKVApi<Error = MetaError>>(
        self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1_table_gc_out_of_retention_time";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        let db1_name = "db1_table_gc_out_of_retention_time";
        let tb1_name = "tb1_table_gc_out_of_retention_time";
        let tbl_name_ident = TableNameIdent {
            tenant: tenant.clone(),
            db_name: db1_name.to_string(),
            table_name: tb1_name.to_string(),
        };

        let plan = CreateDatabaseReq {
            create_option: CreateOption::Create,
            name_ident: DatabaseNameIdent::new(&tenant, db1_name),
            meta: DatabaseMeta {
                engine: "".to_string(),
                ..DatabaseMeta::default()
            },
        };

        let res = mt.create_database(plan).await?;
        info!("create database res: {:?}", res);

        assert_eq!(1, res.db_id, "first database id is 1");
        let one_day_before = Some(Utc::now() - Duration::days(1));
        let two_day_before = Some(Utc::now() - Duration::days(2));

        self.create_out_of_retention_time_table(
            mt,
            tbl_name_ident.clone(),
            DBIdTableName {
                db_id: res.db_id,
                table_name: tb1_name.to_string(),
            },
            one_day_before,
            true,
        )
        .await?;
        let (table_id, table_meta) = self
            .create_out_of_retention_time_table(
                mt,
                tbl_name_ident.clone(),
                DBIdTableName {
                    db_id: res.db_id,
                    table_name: tb1_name.to_string(),
                },
                two_day_before,
                false,
            )
            .await?;

        info!("--- create and get stage file info");
        {
            let stage_info = TableCopiedFileInfo {
                etag: Some("etag".to_owned()),
                content_length: 1024,
                last_modified: Some(Utc::now()),
            };
            let mut file_info = BTreeMap::new();
            file_info.insert("file".to_string(), stage_info.clone());

            let copied_file_req = UpsertTableCopiedFileReq {
                file_info: file_info.clone(),
                expire_at: Some((Utc::now().timestamp() + 86400) as u64),
                fail_if_duplicated: true,
            };

            let req = UpdateTableMetaReq {
                table_id,
                seq: MatchSeq::Any,
                new_table_meta: table_meta.clone(),
            };

            let table = mt
                .get_table(GetTableReq {
                    inner: tbl_name_ident,
                })
                .await?
                .as_ref()
                .clone();

            let req = UpdateMultiTableMetaReq {
                update_table_metas: vec![(req, table)],
                copied_files: vec![(table_id, copied_file_req)],
                ..Default::default()
            };

            let _ = mt.update_multi_table_meta(req).await?;

            let key = TableCopiedFileNameIdent {
                table_id,
                file: "file".to_string(),
            };

            let stage_file: TableCopiedFileInfo = get_kv_data(mt.as_kv_api(), &key).await?;
            assert_eq!(stage_file, stage_info);
        }

        let table_id_idlist = TableIdHistoryIdent {
            database_id: res.db_id,
            table_name: tb1_name.to_string(),
        };

        // save old id list
        let id_list: TableIdList = get_kv_data(mt.as_kv_api(), &table_id_idlist).await?;
        assert_eq!(id_list.len(), 2);
        let old_id_list = id_list.id_list().clone();

        // gc the drop tables
        {
            let req = ListDroppedTableReq {
                inner: DatabaseNameIdent::new(&tenant, ""),
                filter: TableInfoFilter::AllDroppedTables(None),
                limit: None,
            };
            let resp = mt.get_drop_table_infos(req).await?;

            let req = GcDroppedTableReq {
                tenant: tenant.clone(),
                drop_ids: resp.drop_ids.clone(),
            };
            let _resp = mt.gc_drop_tables(req).await?;
        }

        // assert table id list key has been removed
        let id_list: Result<TableIdList, KVAppError> =
            get_kv_data(mt.as_kv_api(), &table_id_idlist).await;
        assert!(id_list.is_err());

        // assert old table meta and id to name mapping has been removed
        for table_id in old_id_list.iter() {
            let id_key = TableId {
                table_id: *table_id,
            };
            let id_mapping = TableIdToName {
                table_id: *table_id,
            };
            let meta_res: Result<DatabaseMeta, KVAppError> =
                get_kv_data(mt.as_kv_api(), &id_key).await;
            let mapping_res: Result<DBIdTableName, KVAppError> =
                get_kv_data(mt.as_kv_api(), &id_mapping).await;
            assert!(meta_res.is_err());
            assert!(mapping_res.is_err());
        }

        info!("--- assert stage file info has been removed");
        {
            let key = TableCopiedFileNameIdent {
                table_id,
                file: "file".to_string(),
            };

            let resp: Result<TableCopiedFileInfo, KVAppError> =
                get_kv_data(mt.as_kv_api(), &key).await;
            assert!(resp.is_err());
        }

        Ok(())
    }

    #[fastrace::trace]
    async fn db_table_gc_out_of_retention_time<
        MT: SchemaApi + kvapi::AsKVApi<Error = MetaError>,
    >(
        self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "db_table_gc_out_of_retention_time";
        let tenant = Tenant::new_literal(tenant_name);
        let db1_name = "db1";
        let tb1_name = "tb1";
        let idx1_name = "idx1";
        let tbl_name_ident = TableNameIdent {
            tenant: tenant.clone(),
            db_name: db1_name.to_string(),
            table_name: tb1_name.to_string(),
        };

        let plan = CreateDatabaseReq {
            create_option: CreateOption::Create,
            name_ident: DatabaseNameIdent::new(&tenant, db1_name),
            meta: DatabaseMeta {
                engine: "".to_string(),
                ..DatabaseMeta::default()
            },
        };

        let res = mt.create_database(plan).await?;
        info!("create database res: {:?}", res);
        let db_id = res.db_id;

        let created_on = Utc::now();
        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };

        let create_table_meta = TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            created_on,
            ..TableMeta::default()
        };

        let req = CreateTableReq {
            create_option: CreateOption::Create,
            name_ident: tbl_name_ident.clone(),
            table_meta: create_table_meta.clone(),
            as_dropped: false,
        };

        let res = mt.create_table(req).await?;
        let table_id = res.table_id;
        info!("--- create and get stage file info");
        {
            let stage_info = TableCopiedFileInfo {
                etag: Some("etag".to_owned()),
                content_length: 1024,
                last_modified: Some(Utc::now()),
            };
            let mut file_info = BTreeMap::new();
            file_info.insert("file".to_string(), stage_info.clone());

            let copied_file_req = UpsertTableCopiedFileReq {
                file_info: file_info.clone(),
                expire_at: Some((Utc::now().timestamp() + 86400) as u64),
                fail_if_duplicated: true,
            };

            let req = UpdateTableMetaReq {
                table_id,
                seq: MatchSeq::Any,
                new_table_meta: create_table_meta.clone(),
            };

            let table = mt
                .get_table(GetTableReq {
                    inner: tbl_name_ident.clone(),
                })
                .await?
                .as_ref()
                .clone();

            let req = UpdateMultiTableMetaReq {
                update_table_metas: vec![(req, table)],
                copied_files: vec![(table_id, copied_file_req)],
                ..Default::default()
            };

            let _ = mt.update_multi_table_meta(req).await?;

            let key = TableCopiedFileNameIdent {
                table_id,
                file: "file".to_string(),
            };

            let stage_file: TableCopiedFileInfo = get_kv_data(mt.as_kv_api(), &key).await?;
            assert_eq!(stage_file, stage_info);
        }

        let agg_index_create_req = CreateIndexReq {
            create_option: CreateOption::CreateIfNotExists,
            name_ident: IndexNameIdent::new(&tenant, idx1_name),
            meta: IndexMeta {
                table_id,
                index_type: IndexType::AGGREGATING,
                created_on: Utc::now(),
                dropped_on: None,
                updated_on: None,
                original_query: "select sum(number) from tb1".to_string(),
                query: "select sum(number) from tb1".to_string(),
                sync_creation: false,
            },
        };

        let res = mt.create_index(agg_index_create_req).await?;
        let index_id = res.index_id;

        // drop the db
        let drop_on = Some(Utc::now() - Duration::days(1));
        let drop_data = DatabaseMeta {
            engine: "github".to_string(),
            drop_on,
            ..Default::default()
        };
        let id_key = DatabaseId { db_id };
        let data = serialize_struct(&drop_data)?;
        upsert_test_data(mt.as_kv_api(), &id_key, data).await?;

        let dbid_idlist1 = DatabaseIdHistoryIdent::new(&tenant, db1_name);
        let old_id_list: DbIdList = get_kv_data(mt.as_kv_api(), &dbid_idlist1).await?;
        assert_eq!(old_id_list.len(), 1);

        let table_id_idlist = TableIdHistoryIdent {
            database_id: db_id,
            table_name: tb1_name.to_string(),
        };

        // save old id list
        let id_list: TableIdList = get_kv_data(mt.as_kv_api(), &table_id_idlist).await?;
        assert_eq!(id_list.len(), 1);
        let old_table_id_list = id_list.id_list().clone();

        // gc the data
        {
            let req = ListDroppedTableReq {
                inner: DatabaseNameIdent::new(&tenant, ""),
                filter: TableInfoFilter::AllDroppedTables(None),
                limit: None,
            };
            let resp = mt.get_drop_table_infos(req).await?;

            let req = GcDroppedTableReq {
                tenant: tenant.clone(),
                drop_ids: resp.drop_ids.clone(),
            };
            let _resp = mt.gc_drop_tables(req).await?;
        }

        // assert db id list has been removed
        let id_list: Result<DbIdList, KVAppError> =
            get_kv_data(mt.as_kv_api(), &dbid_idlist1).await;
        assert!(id_list.is_err());

        // assert old db meta and id to name mapping has been removed
        for db_id in old_id_list.id_list.iter() {
            let id_key = DatabaseId { db_id: *db_id };
            let id_mapping = DatabaseIdToName { db_id: *db_id };

            let meta_res: Result<DatabaseMeta, KVAppError> =
                get_kv_data(mt.as_kv_api(), &id_key).await;
            assert!(meta_res.is_err());

            let mapping_res: Result<DatabaseNameIdentRaw, KVAppError> =
                get_kv_data(mt.as_kv_api(), &id_mapping).await;
            assert!(mapping_res.is_err());
        }

        // check table data has been gc
        let id_list: Result<TableIdList, KVAppError> =
            get_kv_data(mt.as_kv_api(), &table_id_idlist).await;
        assert!(id_list.is_err());

        // assert old table meta and id to name mapping has been removed
        for table_id in old_table_id_list.iter() {
            let id_key = TableId {
                table_id: *table_id,
            };
            let id_mapping = TableIdToName {
                table_id: *table_id,
            };
            let meta_res: Result<DatabaseMeta, KVAppError> =
                get_kv_data(mt.as_kv_api(), &id_key).await;
            let mapping_res: Result<DBIdTableName, KVAppError> =
                get_kv_data(mt.as_kv_api(), &id_mapping).await;
            assert!(meta_res.is_err());
            assert!(mapping_res.is_err());
        }

        // check table's indexes have been cleaned
        {
            let id_key = IndexId { index_id };
            let id_to_name_key = IndexIdToName { index_id };

            let agg_index_meta: Result<IndexMeta, KVAppError> =
                get_kv_data(mt.as_kv_api(), &id_key).await;
            let agg_index_name_ident: Result<IndexNameIdentRaw, KVAppError> =
                get_kv_data(mt.as_kv_api(), &id_to_name_key).await;

            assert!(agg_index_meta.is_err());
            assert!(agg_index_name_ident.is_err());
        }

        info!("--- assert stage file info has been removed");
        {
            let key = TableCopiedFileNameIdent {
                table_id,
                file: "file".to_string(),
            };

            let resp: Result<TableCopiedFileInfo, KVAppError> =
                get_kv_data(mt.as_kv_api(), &key).await;
            assert!(resp.is_err());
        }

        Ok(())
    }

    #[fastrace::trace]
    async fn table_drop_out_of_retention_time_history<
        MT: SchemaApi + kvapi::AsKVApi<Error = MetaError>,
    >(
        self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant_table_drop_history";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        let db_name = "table_table_drop_history_db1";
        let tbl_name = "table_table_drop_history_tb1";
        let tbl_name_ident = TableNameIdent {
            tenant: tenant.clone(),
            db_name: db_name.to_string(),
            table_name: tbl_name.to_string(),
        };

        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };

        info!("--- prepare db");
        {
            let plan = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let res = mt.create_database(plan).await?;
            info!("create database res: {:?}", res);

            assert_eq!(1, res.db_id, "first database id is 1");
        }

        let created_on = Utc::now();
        let create_table_meta = TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            created_on,
            // drop_on: Some(created_on - Duration::days(1)),
            ..TableMeta::default()
        };
        info!("--- create and get table");
        {
            let req = CreateTableReq {
                create_option: CreateOption::Create,
                name_ident: tbl_name_ident.clone(),
                table_meta: create_table_meta.clone(),
                as_dropped: false,
            };

            let old_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
            let res = mt.create_table(req.clone()).await?;
            let cur_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);
            let table_id = res.table_id;
            assert!(table_id >= 1, "table id >= 1");

            let res = mt
                .get_table_history(ListTableReq::new(&tenant, db_name))
                .await?;

            assert_eq!(res.len(), 1);

            let tbid = TableId { table_id };
            let create_drop_table_meta = TableMeta {
                schema: schema(),
                engine: "JSON".to_string(),
                created_on,
                drop_on: Some(created_on - Duration::days(1)),
                ..TableMeta::default()
            };
            let data = serialize_struct(&create_drop_table_meta)?;

            upsert_test_data(mt.as_kv_api(), &tbid, data).await?;
            // assert not return out of retention time data
            let res = mt
                .get_table_history(ListTableReq::new(&tenant, db_name))
                .await?;

            assert_eq!(res.len(), 0);
        }

        Ok(())
    }

    #[fastrace::trace]
    async fn table_history_filter<MT: SchemaApi + kvapi::AsKVApi<Error = MetaError>>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };
        let table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: BTreeMap::new(),
            updated_on: created_on,
            created_on,
            ..TableMeta::default()
        };
        let created_on = Utc::now();

        let mut drop_ids_1 = vec![];
        let mut drop_ids_2 = vec![];

        // first create a database drop within filter time
        info!("--- create db1");
        {
            let db_name = DatabaseNameIdent::new(&tenant, "db1");
            let req = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: db_name.clone(),
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let res = mt.create_database(req).await?;
            drop_ids_1.push(DroppedId::Db(
                res.db_id,
                db_name.database_name().to_string(),
            ));
            drop_ids_2.push(DroppedId::Db(
                res.db_id,
                db_name.database_name().to_string(),
            ));

            let req = CreateTableReq {
                create_option: CreateOption::Create,
                name_ident: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: "db1".to_string(),
                    table_name: "tb1".to_string(),
                },

                table_meta: table_meta(created_on),
                as_dropped: false,
            };
            let _resp = mt.create_table(req.clone()).await?;

            mt.drop_database(DropDatabaseReq {
                if_exists: false,
                name_ident: DatabaseNameIdent::new(&tenant, "db1"),
            })
            .await?;
        }

        // second create a database drop outof filter time, but has a table drop within filter time
        info!("--- create db2");
        {
            let create_db_req = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant, "db2"),
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let res = mt.create_database(create_db_req.clone()).await?;
            let db_id = res.db_id;
            drop_ids_2.push(DroppedId::Db(db_id, "db2".to_string()));

            info!("--- create and drop db2.tb1");
            {
                let table_name = TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: "db2".to_string(),
                    table_name: "tb1".to_string(),
                };
                let req = CreateTableReq {
                    create_option: CreateOption::Create,
                    name_ident: table_name.clone(),
                    table_meta: table_meta(created_on),
                    as_dropped: false,
                };
                let resp = mt.create_table(req.clone()).await?;
                drop_ids_1.push(DroppedId::Table(
                    res.db_id,
                    resp.table_id,
                    table_name.table_name.clone(),
                ));

                mt.drop_table_by_id(DropTableByIdReq {
                    if_exists: false,
                    tenant: req.name_ident.tenant.clone(),
                    db_id,
                    table_name: req.name_ident.table_name.clone(),
                    tb_id: resp.table_id,
                })
                .await?;
            }

            info!("--- create and drop db2.tb2, but make its drop time out of filter time");
            {
                let mut table_meta = table_meta(created_on);
                let req = CreateTableReq {
                    create_option: CreateOption::Create,
                    name_ident: TableNameIdent {
                        tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                        db_name: "db2".to_string(),
                        table_name: "tb2".to_string(),
                    },

                    table_meta: table_meta.clone(),
                    as_dropped: false,
                };
                let resp = mt.create_table(req.clone()).await?;
                mt.drop_table_by_id(DropTableByIdReq {
                    if_exists: false,
                    tenant: req.name_ident.tenant.clone(),
                    db_id,
                    table_name: req.name_ident.table_name.clone(),
                    tb_id: resp.table_id,
                })
                .await?;
                let table_id = resp.table_id;
                let id_key = TableId { table_id };
                table_meta.drop_on = Some(created_on + Duration::seconds(100));
                let data = serialize_struct(&table_meta)?;
                upsert_test_data(mt.as_kv_api(), &id_key, data).await?;
            }

            info!("--- create db2.tb3");
            {
                let req = CreateTableReq {
                    create_option: CreateOption::Create,
                    name_ident: TableNameIdent {
                        tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                        db_name: "db2".to_string(),
                        table_name: "tb3".to_string(),
                    },

                    table_meta: table_meta(created_on),
                    as_dropped: false,
                };
                let _resp = mt.create_table(req.clone()).await?;
            }

            mt.drop_database(DropDatabaseReq {
                if_exists: false,
                name_ident: DatabaseNameIdent::new(&tenant, "db2"),
            })
            .await?;
            // change db meta to make this db drop time outof filter time
            let mut drop_db_meta = create_db_req.meta.clone();
            drop_db_meta.drop_on = Some(created_on + Duration::seconds(100));
            let id_key = DatabaseId { db_id };
            let data = serialize_struct(&drop_db_meta)?;
            upsert_test_data(mt.as_kv_api(), &id_key, data).await?;
        }

        // third create a database not dropped, but has a table drop within filter time
        {
            let create_db_req = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant, "db3"),
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let res = mt.create_database(create_db_req.clone()).await?;
            let db_id = res.db_id;

            info!("--- create and drop db3.tb1");
            {
                let req = CreateTableReq {
                    create_option: CreateOption::Create,
                    name_ident: TableNameIdent {
                        tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                        db_name: "db3".to_string(),
                        table_name: "tb1".to_string(),
                    },

                    table_meta: table_meta(created_on),
                    as_dropped: false,
                };
                let resp = mt.create_table(req.clone()).await?;
                drop_ids_1.push(DroppedId::Table(db_id, resp.table_id, "tb1".to_string()));
                drop_ids_2.push(DroppedId::Table(db_id, resp.table_id, "tb1".to_string()));
                mt.drop_table_by_id(DropTableByIdReq {
                    if_exists: false,
                    tenant: req.name_ident.tenant.clone(),
                    db_id,
                    table_name: req.name_ident.table_name.clone(),
                    tb_id: resp.table_id,
                })
                .await?;
            }

            info!("--- create and drop db3.tb2, but make its drop time out of filter time");
            {
                let mut table_meta = table_meta(created_on);
                let req = CreateTableReq {
                    create_option: CreateOption::Create,
                    name_ident: TableNameIdent {
                        tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                        db_name: "db3".to_string(),
                        table_name: "tb2".to_string(),
                    },

                    table_meta: table_meta.clone(),
                    as_dropped: false,
                };
                let resp = mt.create_table(req.clone()).await?;
                drop_ids_2.push(DroppedId::Table(db_id, resp.table_id, "tb2".to_string()));
                mt.drop_table_by_id(DropTableByIdReq {
                    if_exists: false,
                    tenant: req.name_ident.tenant.clone(),
                    db_id,
                    table_name: req.name_ident.table_name.clone(),
                    tb_id: resp.table_id,
                })
                .await?;
                let table_id = resp.table_id;
                let id_key = TableId { table_id };
                table_meta.drop_on = Some(created_on + Duration::seconds(100));
                let data = serialize_struct(&table_meta)?;
                upsert_test_data(mt.as_kv_api(), &id_key, data).await?;
            }

            info!("--- create db3.tb3");
            {
                let req = CreateTableReq {
                    create_option: CreateOption::Create,
                    name_ident: TableNameIdent {
                        tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                        db_name: "db3".to_string(),
                        table_name: "tb3".to_string(),
                    },

                    table_meta: table_meta(created_on),
                    as_dropped: false,
                };
                let _resp = mt.create_table(req.clone()).await?;
            }
        }

        fn cmp_dropped_id(l: &DroppedId, r: &DroppedId) -> Ordering {
            match (l, r) {
                (
                    DroppedId::Table(left_db_id, left_table_id, _),
                    DroppedId::Table(right_db_id, right_table_id, _),
                ) => {
                    if left_db_id != right_db_id {
                        left_db_id.cmp(right_db_id)
                    } else {
                        left_table_id.cmp(right_table_id)
                    }
                }
                (DroppedId::Db(left_db_id, _), DroppedId::Db(right_db_id, _)) => {
                    left_db_id.cmp(right_db_id)
                }
                (DroppedId::Db(left_db_id, _), DroppedId::Table(right_db_id, _, _)) => {
                    left_db_id.cmp(right_db_id)
                }
                (DroppedId::Table(left_db_id, _, _), DroppedId::Db(right_db_id, _)) => {
                    left_db_id.cmp(right_db_id)
                }
            }
        }
        // case 1: test AllDroppedTables with filter time
        {
            let now = Utc::now();
            let req = ListDroppedTableReq {
                inner: DatabaseNameIdent::new(&tenant, ""),
                filter: TableInfoFilter::AllDroppedTables(Some(now)),
                limit: None,
            };
            let resp = mt.get_drop_table_infos(req).await?;
            // sort drop id by table id
            let mut sort_drop_ids = resp.drop_ids;
            sort_drop_ids.sort_by(cmp_dropped_id);
            assert_eq!(sort_drop_ids, drop_ids_1);

            let expected: BTreeSet<String> = [
                "'db1'.'tb1'".to_string(),
                "'db2'.'tb1'".to_string(),
                "'db3'.'tb1'".to_string(),
            ]
            .iter()
            .cloned()
            .collect();
            let actual: BTreeSet<String> = resp
                .drop_table_infos
                .iter()
                .map(|table_info| table_info.desc.clone())
                .collect();
            assert_eq!(expected, actual);
        }

        // case 2: test AllDroppedTables without filter time
        {
            let req = ListDroppedTableReq {
                inner: DatabaseNameIdent::new(&tenant, ""),
                filter: TableInfoFilter::AllDroppedTables(None),
                limit: None,
            };
            let resp = mt.get_drop_table_infos(req).await?;
            // sort drop id by table id
            let mut sort_drop_ids = resp.drop_ids;
            sort_drop_ids.sort_by(cmp_dropped_id);
            assert_eq!(sort_drop_ids, drop_ids_2);

            let expected: BTreeSet<String> = [
                "'db1'.'tb1'".to_string(),
                "'db2'.'tb1'".to_string(),
                "'db2'.'tb2'".to_string(),
                "'db2'.'tb3'".to_string(),
                "'db3'.'tb1'".to_string(),
                "'db3'.'tb2'".to_string(),
            ]
            .iter()
            .cloned()
            .collect();
            let actual: BTreeSet<String> = resp
                .drop_table_infos
                .iter()
                .map(|table_info| table_info.desc.clone())
                .collect();
            assert_eq!(expected, actual);
        }
        Ok(())
    }

    // construct dropped tables: db1.tb[0..DEFAULT_MGET_SIZE + 1], db2.[0..DEFAULT_MGET_SIZE], db3.{tb1}
    // case 1: with no limit it will return all these tables
    // case 2: with limit 1 it will return db1.tb[0]
    // case 3: with limit DEFAULT_MGET_SIZE it will return db1.tb[0..DEFAULT_MGET_SIZE]
    // case 4: with limit 2 * DEFAULT_MGET_SIZE it will return db1.tb[0..DEFAULT_MGET_SIZE + 1], db2.[0..DEFAULT_MGET_SIZE - 1]
    // case 5: with limit 3 * DEFAULT_MGET_SIZE it will return db1.tb[0..DEFAULT_MGET_SIZE + 1], db2.[0..DEFAULT_MGET_SIZE], db3.{tb1}
    #[fastrace::trace]
    async fn table_history_filter_with_limit<MT: SchemaApi + kvapi::AsKVApi<Error = MetaError>>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        async fn create_dropped_table<MT: SchemaApi + kvapi::AsKVApi<Error = MetaError>>(
            mt: &MT,
            tenant: &str,
            db: &str,
            db_id: u64,
            number: usize,
        ) -> anyhow::Result<Vec<DroppedId>> {
            let schema = || {
                Arc::new(TableSchema::new(vec![TableField::new(
                    "number",
                    TableDataType::Number(NumberDataType::UInt64),
                )]))
            };

            let table_meta = |created_on| TableMeta {
                schema: schema(),
                engine: "JSON".to_string(),
                options: BTreeMap::new(),
                updated_on: created_on,
                created_on,
                ..TableMeta::default()
            };
            let mut drop_ids = vec![];
            let created_on = Utc::now();
            for i in 0..number {
                let table_name = format!("tb{:?}", i);
                let req = CreateTableReq {
                    create_option: CreateOption::Create,
                    name_ident: TableNameIdent {
                        tenant: Tenant::new_or_err(tenant, func_name!())?,
                        db_name: db.to_string(),
                        table_name: table_name.clone(),
                    },

                    table_meta: table_meta(created_on),
                    as_dropped: false,
                };
                let resp = mt.create_table(req.clone()).await?;

                drop_ids.push(DroppedId::Table(db_id, resp.table_id, table_name.clone()));

                mt.drop_table_by_id(DropTableByIdReq {
                    if_exists: false,
                    tenant: req.name_ident.tenant.clone(),
                    db_id,
                    table_name: req.name_ident.table_name.clone(),
                    tb_id: resp.table_id,
                })
                .await?;
            }
            Ok(drop_ids)
        }

        let mut case1_drop_ids = vec![];
        let mut case2_drop_ids = vec![];
        let mut case3_drop_ids = vec![];
        let mut case4_drop_ids = vec![];
        let mut case5_drop_ids = vec![];

        info!("--- create db1 tables");
        {
            let test_db_name = "db1";
            let db_name = DatabaseNameIdent::new(&tenant, test_db_name);
            let req = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: db_name.clone(),
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let res = mt.create_database(req).await?;
            let db_id = res.db_id;

            let drop_ids =
                create_dropped_table(mt, tenant_name, test_db_name, db_id, DEFAULT_MGET_SIZE + 1)
                    .await?;
            let case_drop_ids_vec = vec![
                &mut case1_drop_ids,
                &mut case2_drop_ids,
                &mut case3_drop_ids,
                &mut case4_drop_ids,
                &mut case5_drop_ids,
            ];
            for case_drop_ids in case_drop_ids_vec {
                case_drop_ids.extend(drop_ids.clone());
            }
        }

        info!("--- create db2 tables");
        {
            let test_db_name = "db2";
            let db_name = DatabaseNameIdent::new(&tenant, test_db_name);
            let req = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: db_name.clone(),
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let res = mt.create_database(req).await?;
            let db_id = res.db_id;

            let drop_ids =
                create_dropped_table(mt, tenant_name, test_db_name, db_id, DEFAULT_MGET_SIZE)
                    .await?;
            let case_drop_ids_vec = vec![
                &mut case1_drop_ids,
                &mut case4_drop_ids,
                &mut case5_drop_ids,
            ];
            for case_drop_ids in case_drop_ids_vec {
                case_drop_ids.extend(drop_ids.clone());
            }
        }

        info!("--- create db3 tables");
        {
            let test_db_name = "db3";
            let db_name = DatabaseNameIdent::new(&tenant, test_db_name);
            let req = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: db_name.clone(),
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let res = mt.create_database(req).await?;
            let db_id = res.db_id;

            let drop_ids = create_dropped_table(mt, tenant_name, test_db_name, db_id, 1).await?;
            let case_drop_ids_vec = vec![&mut case1_drop_ids, &mut case5_drop_ids];
            for case_drop_ids in case_drop_ids_vec {
                case_drop_ids.extend(drop_ids.clone());
            }
        }

        let limit_and_drop_ids = vec![
            (None, case1_drop_ids.len(), case1_drop_ids),
            (Some(1), 1, case2_drop_ids),
            (Some(DEFAULT_MGET_SIZE), DEFAULT_MGET_SIZE, case3_drop_ids),
            (
                Some(DEFAULT_MGET_SIZE * 2),
                DEFAULT_MGET_SIZE * 2,
                case4_drop_ids,
            ),
            (
                Some(DEFAULT_MGET_SIZE * 3),
                DEFAULT_MGET_SIZE * 2 + 2,
                case5_drop_ids,
            ),
        ];
        for (limit, number, drop_ids) in limit_and_drop_ids {
            let req = ListDroppedTableReq {
                inner: DatabaseNameIdent::new(&tenant, ""),
                filter: TableInfoFilter::AllDroppedTables(None),
                limit,
            };
            let resp = mt.get_drop_table_infos(req).await?;
            assert_eq!(resp.drop_ids.len(), number);

            let drop_ids_set: HashSet<u64> = drop_ids
                .iter()
                .map(|l| {
                    if let DroppedId::Table(_, table_id, _) = l {
                        *table_id
                    } else {
                        unreachable!()
                    }
                })
                .collect();

            for id in resp.drop_ids {
                if let DroppedId::Table(_, table_id, _) = id {
                    assert!(drop_ids_set.contains(&table_id));
                } else {
                    unreachable!()
                }
            }
        }
        Ok(())
    }

    #[fastrace::trace]
    async fn table_drop_undrop_list_history<MT: SchemaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        let tenant_name = "tenant_drop_undrop_list_history_db1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        let db_name = "table_drop_undrop_list_history_db1";
        let tbl_name = "table_drop_undrop_list_history_tb2";
        let new_tbl_name = "new_table_drop_undrop_list_history_tb2";
        let tbl_name_ident = TableNameIdent {
            tenant: tenant.clone(),
            db_name: db_name.to_string(),
            table_name: tbl_name.to_string(),
        };
        let new_tbl_name_ident = TableNameIdent {
            tenant: tenant.clone(),
            db_name: db_name.to_string(),
            table_name: new_tbl_name.to_string(),
        };

        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };

        let options = || maplit::btreemap! {"opt‐1".into() => "val-1".into()};

        let table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: options(),
            created_on,
            ..TableMeta::default()
        };

        info!("--- prepare db");
        {
            let plan = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let res = mt.create_database(plan).await?;
            info!("create database res: {:?}", res);

            assert_eq!(1, res.db_id, "first database id is 1");
        }

        let created_on = Utc::now();
        let create_table_meta = table_meta(created_on);
        info!("--- create and get table");
        {
            let req = CreateTableReq {
                create_option: CreateOption::Create,
                name_ident: tbl_name_ident.clone(),
                table_meta: create_table_meta.clone(),
                as_dropped: false,
            };

            let old_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
            let res = mt.create_table(req.clone()).await?;
            let cur_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);
            assert!(res.table_id >= 1, "table id >= 1");

            let res = mt
                .get_table_history(ListTableReq::new(&tenant, db_name))
                .await?;

            calc_and_compare_drop_on_table_result(res, vec![DroponInfo {
                name: tbl_name.to_string(),
                desc: format!(
                    "'{}'.'{}'",
                    tbl_name_ident.db_name, tbl_name_ident.table_name
                ),
                drop_on_cnt: 0,
                non_drop_on_cnt: 1,
            }]);
        }
        let tb_info = mt
            .get_table((tenant_name, db_name, tbl_name).into())
            .await?;
        let tb_id = tb_info.ident.table_id;

        info!("--- drop and undrop table");
        {
            // first drop table
            let old_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
            mt.drop_table_by_id(DropTableByIdReq {
                if_exists: false,
                tenant: tbl_name_ident.tenant.clone(),
                db_id: old_db.ident.db_id,
                table_name: tbl_name_ident.table_name.clone(),
                tb_id,
            })
            .await?;
            let cur_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);

            let res = mt
                .get_table_history(ListTableReq::new(&tenant, db_name))
                .await?;
            calc_and_compare_drop_on_table_result(res, vec![DroponInfo {
                name: tbl_name.to_string(),
                desc: format!(
                    "'{}'.'{}'",
                    tbl_name_ident.db_name, tbl_name_ident.table_name
                ),
                drop_on_cnt: 1,
                non_drop_on_cnt: 0,
            }]);

            // then undrop table
            let old_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
            let plan = UndropTableReq {
                name_ident: tbl_name_ident.clone(),
            };
            mt.undrop_table(plan).await?;
            let cur_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);

            let res = mt
                .get_table_history(ListTableReq::new(&tenant, db_name))
                .await?;
            calc_and_compare_drop_on_table_result(res, vec![DroponInfo {
                name: tbl_name.to_string(),
                desc: format!(
                    "'{}'.'{}'",
                    tbl_name_ident.db_name, tbl_name_ident.table_name
                ),
                drop_on_cnt: 0,
                non_drop_on_cnt: 1,
            }]);
        }

        info!("--- drop and create table");
        {
            // first drop table
            let old_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
            mt.drop_table_by_id(DropTableByIdReq {
                if_exists: false,
                tenant: tenant.clone(),
                db_id: old_db.ident.db_id,
                table_name: tbl_name.to_string(),
                tb_id,
            })
            .await?;
            let cur_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);

            let res = mt
                .get_table_history(ListTableReq::new(&tenant, db_name))
                .await?;
            calc_and_compare_drop_on_table_result(res, vec![DroponInfo {
                name: tbl_name.to_string(),
                desc: format!(
                    "'{}'.'{}'",
                    tbl_name_ident.db_name, tbl_name_ident.table_name
                ),
                drop_on_cnt: 1,
                non_drop_on_cnt: 0,
            }]);

            // then create table
            let old_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
            let res = mt
                .create_table(CreateTableReq {
                    create_option: CreateOption::Create,
                    name_ident: tbl_name_ident.clone(),
                    table_meta: create_table_meta.clone(),
                    as_dropped: false,
                })
                .await?;
            let cur_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);
            assert!(res.table_id >= 1, "table id >= 1");

            let res = mt
                .get_table_history(ListTableReq::new(&tenant, db_name))
                .await?;

            calc_and_compare_drop_on_table_result(res, vec![DroponInfo {
                name: tbl_name.to_string(),
                desc: format!(
                    "'{}'.'{}'",
                    tbl_name_ident.db_name, tbl_name_ident.table_name
                ),
                drop_on_cnt: 1,
                non_drop_on_cnt: 1,
            }]);

            // then drop table
            let old_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
            let tb_info = mt
                .get_table((tenant_name, db_name, tbl_name).into())
                .await?;
            mt.drop_table_by_id(DropTableByIdReq {
                if_exists: false,
                tenant: tenant.clone(),
                db_id: old_db.ident.db_id,
                table_name: tbl_name.to_string(),
                tb_id: tb_info.ident.table_id,
            })
            .await?;
            let cur_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);

            let res = mt
                .get_table_history(ListTableReq::new(&tenant, db_name))
                .await?;
            calc_and_compare_drop_on_table_result(res, vec![DroponInfo {
                name: tbl_name.to_string(),
                desc: format!(
                    "'{}'.'{}'",
                    tbl_name_ident.db_name, tbl_name_ident.table_name
                ),
                drop_on_cnt: 2,
                non_drop_on_cnt: 0,
            }]);

            let old_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
            mt.undrop_table(UndropTableReq {
                name_ident: tbl_name_ident.clone(),
            })
            .await?;
            let cur_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);

            let res = mt
                .get_table_history(ListTableReq::new(&tenant, db_name))
                .await?;

            calc_and_compare_drop_on_table_result(res, vec![DroponInfo {
                name: tbl_name.to_string(),
                desc: format!(
                    "'{}'.'{}'",
                    tbl_name_ident.db_name, tbl_name_ident.table_name
                ),
                drop_on_cnt: 1,
                non_drop_on_cnt: 1,
            }]);

            let res = mt
                .undrop_table(UndropTableReq {
                    name_ident: tbl_name_ident.clone(),
                })
                .await;
            assert!(res.is_err());
            let code = ErrorCode::from(res.unwrap_err()).code();
            let undrop_table_already_exists = ErrorCode::UNDROP_TABLE_ALREADY_EXISTS;
            assert_eq!(undrop_table_already_exists, code);
        }

        info!("--- rename table");
        {
            // first create drop table2
            let req = CreateTableReq {
                create_option: CreateOption::Create,
                name_ident: new_tbl_name_ident.clone(),
                table_meta: create_table_meta.clone(),
                as_dropped: false,
            };

            let old_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
            let _res = mt.create_table(req.clone()).await?;
            let res = mt
                .get_table_history(ListTableReq::new(&tenant, db_name))
                .await?;
            let cur_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);

            calc_and_compare_drop_on_table_result(res, vec![
                DroponInfo {
                    name: tbl_name.to_string(),
                    desc: format!(
                        "'{}'.'{}'",
                        tbl_name_ident.db_name, tbl_name_ident.table_name
                    ),
                    drop_on_cnt: 1,
                    non_drop_on_cnt: 1,
                },
                DroponInfo {
                    name: new_tbl_name.to_string(),
                    desc: format!(
                        "'{}'.'{}'",
                        new_tbl_name_ident.db_name, new_tbl_name_ident.table_name
                    ),
                    drop_on_cnt: 0,
                    non_drop_on_cnt: 1,
                },
            ]);

            let new_tb_info = mt
                .get_table((tenant_name, db_name, new_tbl_name).into())
                .await?;

            // then drop table2
            let drop_plan = DropTableByIdReq {
                if_exists: false,
                tenant: tenant.clone(),
                db_id: cur_db.ident.db_id,
                table_name: tbl_name.to_string(),
                tb_id: new_tb_info.ident.table_id,
            };

            let old_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
            mt.drop_table_by_id(drop_plan.clone()).await?;
            let cur_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);

            let res = mt
                .get_table_history(ListTableReq::new(&tenant, db_name))
                .await?;
            calc_and_compare_drop_on_table_result(res, vec![
                DroponInfo {
                    name: tbl_name.to_string(),
                    desc: format!(
                        "'{}'.'{}'",
                        tbl_name_ident.db_name, tbl_name_ident.table_name
                    ),
                    drop_on_cnt: 1,
                    non_drop_on_cnt: 1,
                },
                DroponInfo {
                    name: new_tbl_name.to_string(),
                    desc: format!(
                        "'{}'.'{}'",
                        new_tbl_name_ident.db_name, new_tbl_name_ident.table_name
                    ),
                    drop_on_cnt: 1,
                    non_drop_on_cnt: 0,
                },
            ]);

            // then rename table to table2
            let rename_dbtb_to_dbtb1 = |if_exists| RenameTableReq {
                if_exists,
                name_ident: tbl_name_ident.clone(),
                new_db_name: db_name.to_string(),
                new_table_name: new_tbl_name.to_string(),
            };

            let old_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
            let _got = mt.rename_table(rename_dbtb_to_dbtb1(false)).await;
            let cur_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);

            let res = mt
                .get_table_history(ListTableReq::new(&tenant, db_name))
                .await?;
            calc_and_compare_drop_on_table_result(res, vec![
                DroponInfo {
                    name: tbl_name.to_string(),
                    desc: format!(
                        "'{}'.'{}'",
                        tbl_name_ident.db_name, tbl_name_ident.table_name
                    ),
                    drop_on_cnt: 1,
                    non_drop_on_cnt: 0,
                },
                DroponInfo {
                    name: new_tbl_name.to_string(),
                    desc: format!(
                        "'{}'.'{}'",
                        new_tbl_name_ident.db_name, new_tbl_name_ident.table_name
                    ),
                    drop_on_cnt: 1,
                    non_drop_on_cnt: 1,
                },
            ]);
        }

        Ok(())
    }

    #[fastrace::trace]
    async fn table_commit_table_meta<MT: SchemaApi + kvapi::AsKVApi<Error = MetaError>>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant_name = "table_commit_table_meta_tenant";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        let db_name = "db1";
        let tbl_name = "tb2";

        info!("--- prepare db");
        let plan = CreateDatabaseReq {
            create_option: CreateOption::Create,
            name_ident: DatabaseNameIdent::new(&tenant, db_name),
            meta: DatabaseMeta {
                engine: "".to_string(),
                ..DatabaseMeta::default()
            },
        };

        let res = mt.create_database(plan).await?;
        let db_id = res.db_id;

        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };

        let options = || maplit::btreemap! {"opt‐1".into() => "val-1".into()};

        let table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: options(),
            created_on,
            ..TableMeta::default()
        };
        let drop_table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: options(),
            created_on,
            drop_on: Some(created_on),
            ..TableMeta::default()
        };
        let created_on = Utc::now();

        let create_table_req = CreateTableReq {
            create_option: CreateOption::Create,
            name_ident: TableNameIdent {
                tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                db_name: db_name.to_string(),
                table_name: tbl_name.to_string(),
            },
            table_meta: drop_table_meta(created_on),
            as_dropped: true,
        };

        let create_table_as_dropped_resp = mt.create_table(create_table_req.clone()).await?;

        // commit table meta with a wrong prev_table_id will fail
        {
            let commit_table_req = CommitTableMetaReq {
                name_ident: create_table_req.name_ident.clone(),
                db_id: create_table_as_dropped_resp.db_id,
                table_id: create_table_as_dropped_resp.table_id,
                prev_table_id: Some(1111),
                orphan_table_name: create_table_as_dropped_resp.orphan_table_name.clone(),
            };
            let resp = mt.commit_table_meta(commit_table_req).await;
            use databend_common_meta_app::app_error::AppError;
            assert!(matches!(
                resp.unwrap_err(),
                KVAppError::AppError(AppError::CommitTableMetaError(_))
            ));
        }

        // commit table meta with a wrong orphan table id list will fail
        {
            // first update orphan_table_id_list
            let mut orphan_table_id_list: TableIdList = TableIdList::new();
            orphan_table_id_list.append(1);
            orphan_table_id_list.append(2);
            let key_table_id_list = TableIdHistoryIdent {
                database_id: db_id,
                table_name: create_table_as_dropped_resp
                    .orphan_table_name
                    .clone()
                    .unwrap(),
            };
            upsert_test_data(
                mt.as_kv_api(),
                &key_table_id_list,
                serialize_struct(&orphan_table_id_list)?,
            )
            .await?;

            let commit_table_req = CommitTableMetaReq {
                name_ident: create_table_req.name_ident.clone(),
                db_id: create_table_as_dropped_resp.db_id,
                table_id: create_table_as_dropped_resp.table_id,
                prev_table_id: create_table_as_dropped_resp.prev_table_id,
                orphan_table_name: create_table_as_dropped_resp.orphan_table_name.clone(),
            };
            let resp = mt.commit_table_meta(commit_table_req).await;
            use databend_common_meta_app::app_error::AppError;
            assert!(matches!(
                resp.unwrap_err(),
                KVAppError::AppError(AppError::CommitTableMetaError(_))
            ));
        }

        {
            // first update orphan_table_id_list
            let mut orphan_table_id_list: TableIdList = TableIdList::new();
            orphan_table_id_list.append(1);
            let key_table_id_list = TableIdHistoryIdent {
                database_id: db_id,
                table_name: create_table_as_dropped_resp
                    .orphan_table_name
                    .clone()
                    .unwrap(),
            };
            upsert_test_data(
                mt.as_kv_api(),
                &key_table_id_list,
                serialize_struct(&orphan_table_id_list)?,
            )
            .await?;

            // replace with a new as_dropped = false table
            let create_table_req = CreateTableReq {
                create_option: CreateOption::CreateOrReplace,
                name_ident: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name.to_string(),
                },
                table_meta: table_meta(created_on),
                as_dropped: false,
            };

            let _ = mt.create_table(create_table_req.clone()).await?;

            let commit_table_req = CommitTableMetaReq {
                name_ident: create_table_req.name_ident.clone(),
                db_id: create_table_as_dropped_resp.db_id,
                table_id: create_table_as_dropped_resp.table_id,
                prev_table_id: create_table_as_dropped_resp.prev_table_id,
                orphan_table_name: create_table_as_dropped_resp.orphan_table_name.clone(),
            };
            let resp = mt.commit_table_meta(commit_table_req).await;
            use databend_common_meta_app::app_error::AppError;
            assert!(matches!(
                resp.unwrap_err(),
                KVAppError::AppError(AppError::CommitTableMetaError(_))
            ));
        }

        {
            use databend_common_meta_app::app_error::AppError;

            // replace with a new as_dropped = true and table_meta.as_drop is None table
            let create_table_req = CreateTableReq {
                create_option: CreateOption::CreateOrReplace,
                name_ident: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name.to_string(),
                },
                table_meta: table_meta(created_on),
                as_dropped: true,
            };

            let resp = mt.create_table(create_table_req.clone()).await;
            assert!(matches!(
                resp.unwrap_err(),
                KVAppError::AppError(AppError::CreateAsDropTableWithoutDropTime(_))
            ));

            let create_table_req = CreateTableReq {
                create_option: CreateOption::CreateOrReplace,
                name_ident: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name.to_string(),
                },
                table_meta: drop_table_meta(created_on),
                as_dropped: true,
            };

            let create_table_as_dropped_resp = mt.create_table(create_table_req.clone()).await?;

            let commit_table_req = CommitTableMetaReq {
                name_ident: create_table_req.name_ident.clone(),
                db_id: create_table_as_dropped_resp.db_id,
                table_id: create_table_as_dropped_resp.table_id,
                prev_table_id: create_table_as_dropped_resp.prev_table_id,
                orphan_table_name: create_table_as_dropped_resp.orphan_table_name.clone(),
            };
            mt.commit_table_meta(commit_table_req).await?;
        }

        // verify the orphan table id list will be vacuum
        {
            // use a new tenant and db do test
            let db_name = "orphan_db";
            let tenant_name = "orphan_tenant";
            let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

            let plan = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let res = mt.create_database(plan).await?;
            let db_id = res.db_id;

            let create_table_req = CreateTableReq {
                create_option: CreateOption::CreateOrReplace,
                name_ident: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name.to_string(),
                },
                table_meta: drop_table_meta(created_on),
                as_dropped: true,
            };

            let create_table_as_dropped_resp = mt.create_table(create_table_req.clone()).await?;

            let key_table_id_list = TableIdHistoryIdent {
                database_id: db_id,
                table_name: create_table_as_dropped_resp
                    .orphan_table_name
                    .clone()
                    .unwrap(),
            };

            // assert orphan table id list and table meta exists
            let seqv = mt
                .as_kv_api()
                .get_kv(&key_table_id_list.to_string_key())
                .await?;
            assert!(seqv.is_some() && seqv.unwrap().seq != 0);
            let table_key = TableId {
                table_id: create_table_as_dropped_resp.table_id,
            };
            let seqv = mt.as_kv_api().get_kv(&table_key.to_string_key()).await?;
            assert!(seqv.is_some() && seqv.unwrap().seq != 0);

            // vacuum drop table
            let req = ListDroppedTableReq {
                inner: DatabaseNameIdent::new(&tenant, ""),
                filter: TableInfoFilter::AllDroppedTables(None),
                limit: None,
            };
            let resp = mt.get_drop_table_infos(req).await?;
            assert!(!resp.drop_ids.is_empty());

            let req = GcDroppedTableReq {
                tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                drop_ids: resp.drop_ids.clone(),
            };
            let _resp = mt.gc_drop_tables(req).await?;

            // assert orphan table id list and table meta has been vacuum
            let seqv = mt
                .as_kv_api()
                .get_kv(&key_table_id_list.to_string_key())
                .await?;
            assert!(seqv.is_none());
            let table_key = TableId {
                table_id: create_table_as_dropped_resp.table_id,
            };
            let seqv = mt.as_kv_api().get_kv(&table_key.to_string_key()).await?;
            assert!(seqv.is_none());
        }

        Ok(())
    }

    #[fastrace::trace]
    async fn concurrent_commit_table_meta<
        B: kvapi::ApiBuilder<MT>,
        MT: ShareApi
            + kvapi::AsKVApi<Error = MetaError>
            + SchemaApi
            + DatamaskApi
            + SequenceApi
            + 'static,
    >(
        &self,
        b: B,
    ) -> anyhow::Result<()> {
        let db_name = "db";
        let tenant_name = "concurrent_commit_table_meta";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;
        let tbl_name = "tb";

        let plan = CreateDatabaseReq {
            create_option: CreateOption::Create,
            name_ident: DatabaseNameIdent::new(&tenant, db_name),
            meta: DatabaseMeta {
                engine: "".to_string(),
                ..DatabaseMeta::default()
            },
        };

        let mt = Arc::new(b.build().await);
        let res = mt.create_database(plan).await?;
        let db_id = res.db_id;

        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };

        let options = || maplit::btreemap! {"opt‐1".into() => "val-1".into()};

        let drop_table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: options(),
            created_on,
            drop_on: Some(created_on),
            ..TableMeta::default()
        };
        let created_on = Utc::now();

        let create_table_req = CreateTableReq {
            create_option: CreateOption::CreateOrReplace,
            name_ident: TableNameIdent {
                tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                db_name: db_name.to_string(),
                table_name: tbl_name.to_string(),
            },
            table_meta: drop_table_meta(created_on),
            as_dropped: true,
        };

        let concurrent_count: usize = 5;
        let runtime = Runtime::with_worker_threads(concurrent_count, None)?;
        let mut handles = vec![];

        for _i in 0..concurrent_count {
            let create_table_req = create_table_req.clone();
            let arc_mt = mt.clone();

            let handle = runtime.spawn(async move {
                let resp = arc_mt.create_table(create_table_req.clone()).await;

                // assert that when create table concurrently with corret params return error,
                // the error MUST be TxnRetryMaxTimes
                if resp.is_err() {
                    assert!(matches!(
                        resp.unwrap_err(),
                        KVAppError::AppError(AppError::TxnRetryMaxTimes(_))
                    ));
                    return;
                }

                let resp = resp.unwrap();

                let commit_table_req = CommitTableMetaReq {
                    name_ident: create_table_req.name_ident.clone(),
                    db_id,
                    table_id: resp.table_id,
                    prev_table_id: resp.prev_table_id,
                    orphan_table_name: resp.orphan_table_name.clone(),
                };
                let resp = arc_mt.commit_table_meta(commit_table_req).await;

                // assert that when commit_table_meta concurrently with corret params return error,
                // the error MUST be TxnRetryMaxTimes or CommitTableMetaError(prev table id has been changed)
                if resp.is_err() {
                    assert!(
                        matches!(
                            resp.clone().unwrap_err(),
                            KVAppError::AppError(AppError::TxnRetryMaxTimes(_))
                        ) || matches!(
                            resp.unwrap_err(),
                            KVAppError::AppError(AppError::CommitTableMetaError(_))
                        )
                    );
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await?;
        }

        Ok(())
    }

    #[fastrace::trace]
    async fn get_table_by_id<MT: SchemaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        let db_name = "db1";
        let tbl_name = "tb2";

        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };

        let options = || maplit::btreemap! {"opt‐1".into() => "val-1".into()};

        let table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: options(),
            created_on,
            ..TableMeta::default()
        };

        info!("--- prepare db");
        {
            let plan = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let res = mt.create_database(plan).await?;
            info!("create database res: {:?}", res);

            assert_eq!(1, res.db_id, "first database id is 1");
        }

        info!("--- create and get table");
        {
            let created_on = Utc::now();

            let req = CreateTableReq {
                create_option: CreateOption::Create,
                name_ident: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name.to_string(),
                },
                table_meta: table_meta(created_on),
                as_dropped: false,
            };

            let _tb_ident_2 = {
                let old_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
                let res = mt.create_table(req.clone()).await?;
                let cur_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
                assert!(old_db.ident.seq < cur_db.ident.seq);
                assert!(res.table_id >= 1, "table id >= 1");
                let tb_id = res.table_id;

                let got = mt
                    .get_table((tenant_name, db_name, tbl_name).into())
                    .await?;
                let seq = got.ident.seq;

                let ident = TableIdent::new(tb_id, seq);

                let want = TableInfo {
                    ident,
                    desc: format!("'{}'.'{}'", db_name, tbl_name),
                    name: tbl_name.into(),
                    meta: table_meta(created_on),
                    tenant: tenant_name.to_string(),
                    ..Default::default()
                };
                assert_meta_eq_without_updated!(want, got.as_ref().clone(), "get created table");
                ident
            };
        }

        info!("--- get_table_by_id ");
        {
            info!("--- get_table_by_id ");
            {
                let table = mt
                    .get_table((tenant_name, "db1", "tb2").into())
                    .await
                    .unwrap();

                let seqv = mt.get_table_by_id(table.ident.table_id).await?.unwrap();
                let table_meta = seqv.data;
                assert_eq!(table_meta.options.get("opt‐1"), Some(&"val-1".into()));
            }

            info!("--- get_table_by_id with not exists table_id");
            {
                let got = mt.get_table_by_id(1024).await?;

                assert!(got.is_none());
            }
        }
        Ok(())
    }

    #[fastrace::trace]
    async fn get_table_name_by_id<MT>(&self, mt: &MT) -> anyhow::Result<()>
    where MT: SchemaApi + kvapi::AsKVApi<Error = MetaError> {
        let tenant_name = "tenant1";
        let db_name = "db1";
        let tbl_name = "tb2";

        let mut util = Util::new(mt, tenant_name, db_name, tbl_name, "eng1");
        let table_id;

        info!("--- prepare db and table");
        {
            util.create_db().await?;
            let (tid, _table_meta) = util.create_table().await?;
            table_id = tid;
        }

        info!("--- get_table_name_by_id ");
        {
            info!("--- get_table_name_by_id ");
            {
                let got = mt.get_table_name_by_id(table_id).await?;
                assert!(got.is_some());
                assert_eq!(tbl_name.to_owned(), got.unwrap());
            }

            info!("--- get_table_name_by_id with not exists table_id");
            {
                let got = mt.get_table_name_by_id(1024).await?;
                assert!(got.is_none());
            }
        }
        Ok(())
    }

    #[fastrace::trace]
    async fn get_db_name_by_id<MT: SchemaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        let db_name = "db1";

        info!("--- prepare and get db");
        {
            let plan = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let res = mt.create_database(plan).await?;
            info!("create database res: {:?}", res);

            assert_eq!(1, res.db_id, "first database id is 1");

            let got = mt.get_db_name_by_id(res.db_id).await?;
            assert_eq!(got, db_name.to_string())
        }

        info!("--- get_db_name_by_id ");
        {
            info!("--- get_db_name_by_id ");
            {
                let plan = GetDatabaseReq {
                    inner: DatabaseNameIdent::new(&tenant, db_name),
                };

                let db = mt.get_database(plan).await.unwrap();

                let got = mt.get_db_name_by_id(db.ident.db_id).await?;

                assert_eq!(got, db_name.to_string());
            }

            info!("--- get_db_name_by_id with not exists db_id");
            {
                let got = mt.get_db_name_by_id(1024).await;

                let err = got.unwrap_err();
                let err = ErrorCode::from(err);

                assert_eq!(ErrorCode::UNKNOWN_DATABASE_ID, err.code());
            }
        }
        Ok(())
    }

    #[fastrace::trace]
    async fn test_sequence<MT: SchemaApi + SequenceApi + kvapi::AsKVApi<Error = MetaError>>(
        &self,
        mt: &MT,
    ) -> anyhow::Result<()> {
        let tenant = Tenant {
            tenant: "tenant1".to_string(),
        };
        let sequence_name = "seq";

        let create_on = Utc::now();

        info!("--- create sequence");
        {
            let req = CreateSequenceReq {
                create_option: CreateOption::Create,
                ident: SequenceIdent::new(&tenant, sequence_name),
                create_on,
                comment: Some("seq".to_string()),
            };

            let _resp = mt.create_sequence(req).await?;
        }

        info!("--- get sequence");
        {
            let req = GetSequenceReq {
                ident: SequenceIdent::new(&tenant, sequence_name),
            };
            let resp = mt.get_sequence(req).await?;
            assert_eq!(resp.meta.comment, Some("seq".to_string()));
            assert_eq!(resp.meta.current, 1);
        }

        info!("--- get sequence nextval");
        {
            let req = GetSequenceNextValueReq {
                ident: SequenceIdent::new(&tenant, sequence_name),
                count: 10,
            };
            let resp = mt.get_sequence_next_value(req).await?;
            assert_eq!(resp.start, 1);
            assert_eq!(resp.end, 10);
        }

        info!("--- get sequence after nextval");
        {
            let req = GetSequenceReq {
                ident: SequenceIdent::new(&tenant, sequence_name),
            };

            let resp = mt.get_sequence(req).await?;
            assert_eq!(resp.meta.comment, Some("seq".to_string()));
            assert_eq!(resp.meta.current, 11);
        }

        info!("--- replace sequence");
        {
            let req = CreateSequenceReq {
                create_option: CreateOption::CreateOrReplace,
                ident: SequenceIdent::new(&tenant, sequence_name),
                create_on,
                comment: Some("seq1".to_string()),
            };

            let _resp = mt.create_sequence(req).await?;

            let req = GetSequenceReq {
                ident: SequenceIdent::new(&tenant, sequence_name),
            };

            let resp = mt.get_sequence(req).await?;
            assert_eq!(resp.meta.comment, Some("seq1".to_string()));
            assert_eq!(resp.meta.current, 1);
        }

        {
            let req = DropSequenceReq {
                ident: SequenceIdent::new(&tenant, sequence_name),
                if_exists: true,
            };

            let _resp = mt.drop_sequence(req).await?;

            let req = GetSequenceReq {
                ident: SequenceIdent::new(&tenant, sequence_name),
            };

            let resp = mt.get_sequence(req).await;
            assert!(resp.is_err());
        }

        Ok(())
    }

    #[fastrace::trace]
    async fn get_table_copied_file<MT: SchemaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        let db_name = "db1";
        let tbl_name = "tb2";
        let table_id;

        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };

        let options = || maplit::btreemap! {"opt‐1".into() => "val-1".into()};

        let table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: options(),
            created_on,
            ..TableMeta::default()
        };
        let created_on = Utc::now();
        let tbl_name_ident = TableNameIdent {
            tenant: Tenant::new_or_err(tenant_name, func_name!())?,
            db_name: db_name.to_string(),
            table_name: tbl_name.to_string(),
        };
        info!("--- prepare db and table");
        {
            let plan = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let _ = mt.create_database(plan).await?;

            let req = CreateTableReq {
                create_option: CreateOption::Create,
                name_ident: tbl_name_ident.clone(),
                table_meta: table_meta(created_on),
                as_dropped: false,
            };
            let resp = mt.create_table(req.clone()).await?;
            table_id = resp.table_id;
        }

        info!("--- create and get stage file info");
        {
            let stage_info = TableCopiedFileInfo {
                etag: Some("etag".to_owned()),
                content_length: 1024,
                last_modified: Some(Utc::now()),
            };
            let mut file_info = BTreeMap::new();
            file_info.insert("file".to_string(), stage_info.clone());

            let copied_file_req = UpsertTableCopiedFileReq {
                file_info: file_info.clone(),
                expire_at: Some((Utc::now().timestamp() + 86400) as u64),
                fail_if_duplicated: true,
            };

            let req = UpdateTableMetaReq {
                table_id,
                seq: MatchSeq::Any,
                new_table_meta: table_meta(created_on),
            };

            let table = mt
                .get_table(GetTableReq {
                    inner: tbl_name_ident.clone(),
                })
                .await?
                .as_ref()
                .clone();

            let req = UpdateMultiTableMetaReq {
                update_table_metas: vec![(req, table)],
                copied_files: vec![(table_id, copied_file_req)],
                ..Default::default()
            };

            let _ = mt.update_multi_table_meta(req).await?;

            let req = GetTableCopiedFileReq {
                table_id,
                files: vec!["file".to_string()],
            };

            let resp = mt.get_table_copied_file_info(req).await?;
            assert_eq!(resp.file_info.len(), 1);
            let resp_stage_info = resp.file_info.get("file");
            assert_eq!(resp_stage_info.unwrap(), &stage_info);
        }

        info!("--- test again with expire stage file info");
        {
            let stage_info = TableCopiedFileInfo {
                etag: Some("etag".to_owned()),
                content_length: 1024,
                last_modified: Some(Utc::now()),
            };
            let mut file_info = BTreeMap::new();
            file_info.insert("file2".to_string(), stage_info.clone());

            let copied_file_req = UpsertTableCopiedFileReq {
                file_info: file_info.clone(),
                expire_at: Some((Utc::now().timestamp() - 86400) as u64),
                fail_if_duplicated: true,
            };

            let req = UpdateTableMetaReq {
                table_id,
                seq: MatchSeq::Any,
                new_table_meta: table_meta(created_on),
            };

            let table = mt
                .get_table(GetTableReq {
                    inner: tbl_name_ident.clone(),
                })
                .await?
                .as_ref()
                .clone();

            let req = UpdateMultiTableMetaReq {
                update_table_metas: vec![(req, table)],
                copied_files: vec![(table_id, copied_file_req)],
                ..Default::default()
            };

            let _ = mt.update_multi_table_meta(req).await?;

            let req = GetTableCopiedFileReq {
                table_id,
                files: vec!["file2".to_string()],
            };

            let resp = mt.get_table_copied_file_info(req).await?;
            assert_eq!(resp.file_info.len(), 0);
        }

        Ok(())
    }

    #[fastrace::trace]
    async fn truncate_table<MT>(&self, mt: &MT) -> anyhow::Result<()>
    where MT: SchemaApi + kvapi::AsKVApi<Error = MetaError> {
        let mut util = Util::new(mt, "tenant1", "db1", "tb2", "JSON");
        let table_id;

        info!("--- prepare db and table");
        {
            util.create_db().await?;
            let (tid, _table_meta) = util.create_table().await?;
            table_id = tid;
        }

        let n = 7;
        info!("--- create copied file infos");
        let file_infos = util.update_copied_files(n).await?;

        info!("--- get copied file infos");
        {
            let req = GetTableCopiedFileReq {
                table_id,
                files: file_infos.keys().cloned().collect(),
            };

            let resp = mt.get_table_copied_file_info(req).await?;
            assert_eq!(file_infos, resp.file_info);
        }

        info!("--- truncate table and get stage file info again");
        {
            let req = TruncateTableReq {
                table_id,
                batch_size: Some(2),
            };

            let _ = mt.truncate_table(req).await?;

            let req = GetTableCopiedFileReq {
                table_id,
                files: file_infos.keys().cloned().collect(),
            };

            let resp = mt.get_table_copied_file_info(req).await?;
            assert_eq!(resp.file_info.len(), 0);
        }

        Ok(())
    }

    #[fastrace::trace]
    async fn table_list<MT: SchemaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;
        let db_name = "db1";

        info!("--- list table on unknown db");
        {
            let res = mt.list_tables(ListTableReq::new(&tenant, db_name)).await;
            debug!("list table on unknown db res: {:?}", res);
            assert!(res.is_err());

            let code = ErrorCode::from(res.unwrap_err()).code();
            assert_eq!(ErrorCode::UNKNOWN_DATABASE, code);
        }

        info!("--- prepare db");
        {
            let res = self.create_database(mt, &tenant, db_name, "eng1").await?;
            assert_eq!(1, res.db_id, "first database id is 1");
        }

        info!("--- create 2 tables: tb1 tb2");
        {
            // Table schema with metadata(due to serde issue).
            let schema = Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]));

            let options = maplit::btreemap! {"opt‐1".into() => "val-1".into()};

            let mut req = CreateTableReq {
                create_option: CreateOption::Create,
                name_ident: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: "tb1".to_string(),
                },
                table_meta: TableMeta {
                    schema: schema.clone(),
                    engine: "JSON".to_string(),
                    options: options.clone(),
                    ..Default::default()
                },
                as_dropped: false,
            };

            let tb_ids = {
                let old_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
                let res = mt.create_table(req.clone()).await?;
                let cur_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
                assert!(old_db.ident.seq < cur_db.ident.seq);
                assert!(res.table_id >= 1, "table id >= 1");

                let tb_id1 = res.table_id;

                req.name_ident.table_name = "tb2".to_string();
                let old_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
                let res = mt.create_table(req.clone()).await?;
                let cur_db = mt.get_database(Self::req_get_db(&tenant, db_name)).await?;
                assert!(old_db.ident.seq < cur_db.ident.seq);
                assert!(res.table_id > tb_id1, "table id > tb_id1: {}", tb_id1);
                let tb_id2 = res.table_id;

                vec![tb_id1, tb_id2]
            };

            info!("--- get_tables");
            {
                let res = mt.list_tables(ListTableReq::new(&tenant, db_name)).await?;
                assert_eq!(tb_ids.len(), res.len());
                assert_eq!(tb_ids[0], res[0].ident.table_id);
                assert_eq!(tb_ids[1], res[1].ident.table_id);
            }
        }

        Ok(())
    }

    /// Test listing many tables that exceeds default mget chunk size.
    #[fastrace::trace]
    async fn table_list_many<MT>(&self, mt: &MT) -> anyhow::Result<()>
    where MT: SchemaApi + kvapi::AsKVApi<Error = MetaError> {
        // Create tables that exceeds the default mget chunk size
        let n = DEFAULT_MGET_SIZE + 20;

        let mut util = Util::new(mt, "tenant1", "db1", "tb1", "eng1");

        info!("--- prepare db");
        {
            util.create_db().await?;
        }

        info!("--- create {} tables", n);
        {
            for i in 0..n {
                let table_name = format!("tb_{:0>5}", i);

                let table_meta = util.table_meta();
                let req = CreateTableReq {
                    create_option: CreateOption::Create,
                    name_ident: TableNameIdent {
                        tenant: util.tenant(),
                        db_name: util.db_name(),
                        table_name,
                    },
                    table_meta: table_meta.clone(),
                    as_dropped: false,
                };
                let resp = util.mt.create_table(req).await?;

                if i % 100 == 0 {
                    info!("--- created {} tables: {:?}", i, resp);
                }
            }
        }

        info!("--- get_tables");
        {
            let res = mt
                .list_tables(ListTableReq::new(&util.tenant(), util.db_name()))
                .await?;
            assert_eq!(n, res.len());
        }

        Ok(())
    }

    #[fastrace::trace]
    async fn table_index_create_drop<MT: SchemaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        let db_name = "db1";
        let tbl_name = "tb2";
        let table_id;

        let schema = || {
            Arc::new(TableSchema::new(vec![
                TableField::new("title", TableDataType::String),
                TableField::new("content", TableDataType::String),
                TableField::new("author", TableDataType::String),
            ]))
        };

        let table_meta = |created_on| TableMeta {
            schema: schema(),
            created_on,
            ..TableMeta::default()
        };

        let created_on = Utc::now();

        info!("--- prepare db and table");
        {
            let plan = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let _ = mt.create_database(plan).await?;

            let req = CreateTableReq {
                create_option: CreateOption::Create,
                name_ident: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: tbl_name.to_string(),
                },
                table_meta: table_meta(created_on),
                as_dropped: false,
            };
            let resp = mt.create_table(req.clone()).await?;
            table_id = resp.table_id;
        }

        let index_name_1 = "idx1".to_string();
        let index_column_ids_1 = vec![0, 1];
        let index_name_2 = "idx2".to_string();
        let index_column_ids_2 = vec![2];
        let index_name_3 = "idx2".to_string();
        let index_column_ids_3 = vec![3];

        {
            info!("--- create table index 1");
            let req = CreateTableIndexReq {
                create_option: CreateOption::Create,
                table_id,
                name: index_name_1.clone(),
                column_ids: index_column_ids_1.clone(),
                sync_creation: true,
                options: BTreeMap::new(),
            };
            let res = mt.create_table_index(req).await;
            assert!(res.is_ok());

            info!("--- create table index 2 with duplicate column id");
            let req = CreateTableIndexReq {
                create_option: CreateOption::Create,
                table_id,
                name: index_name_2.clone(),
                column_ids: index_column_ids_1.clone(),
                sync_creation: true,
                options: BTreeMap::new(),
            };
            let res = mt.create_table_index(req).await;
            assert!(res.is_err());

            info!("--- create table index 2");
            let req = CreateTableIndexReq {
                create_option: CreateOption::Create,
                table_id,
                name: index_name_2.clone(),
                column_ids: index_column_ids_2.clone(),
                sync_creation: true,
                options: BTreeMap::new(),
            };
            let res = mt.create_table_index(req).await;
            assert!(res.is_ok());
        }

        {
            info!("--- create table index again with if_not_exists = false");
            let req = CreateTableIndexReq {
                create_option: CreateOption::Create,
                table_id,
                name: index_name_1.clone(),
                column_ids: index_column_ids_1.clone(),
                sync_creation: true,
                options: BTreeMap::new(),
            };

            let res = mt.create_table_index(req).await;
            assert!(res.is_err());
            let status = res.err().unwrap();
            let err_code = ErrorCode::from(status);

            assert_eq!(ErrorCode::INDEX_ALREADY_EXISTS, err_code.code());
        }

        {
            info!("--- create table index again with if_not_exists = true");
            let req = CreateTableIndexReq {
                create_option: CreateOption::CreateIfNotExists,
                table_id,
                name: index_name_1.clone(),
                column_ids: index_column_ids_1.clone(),
                sync_creation: true,
                options: BTreeMap::new(),
            };

            let res = mt.create_table_index(req).await;
            assert!(res.is_ok());
        }

        {
            info!("--- create table index with invalid column id");
            let req = CreateTableIndexReq {
                create_option: CreateOption::Create,
                table_id,
                name: index_name_3.clone(),
                column_ids: index_column_ids_3.clone(),
                sync_creation: true,
                options: BTreeMap::new(),
            };
            let res = mt.create_table_index(req).await;
            assert!(res.is_err());
        }

        {
            info!("--- check table index");
            let seqv = mt.get_table_by_id(table_id).await?.unwrap();
            let table_meta = seqv.data;
            assert_eq!(table_meta.indexes.len(), 2);

            let index1 = table_meta.indexes.get(&index_name_1);
            assert!(index1.is_some());
            let index1 = index1.unwrap();
            assert_eq!(index1.column_ids, index_column_ids_1);

            let index2 = table_meta.indexes.get(&index_name_2);
            assert!(index2.is_some());
            let index2 = index2.unwrap();
            assert_eq!(index2.column_ids, index_column_ids_2);
        }

        {
            info!("--- drop table index");
            let req = DropTableIndexReq {
                if_exists: false,
                table_id,
                name: index_name_1.clone(),
            };
            let res = mt.drop_table_index(req).await;
            assert!(res.is_ok());

            let req = DropTableIndexReq {
                if_exists: false,
                table_id,
                name: index_name_1.clone(),
            };
            let res = mt.drop_table_index(req).await;
            assert!(res.is_err());

            let req = DropTableIndexReq {
                if_exists: true,
                table_id,
                name: index_name_1.clone(),
            };
            let res = mt.drop_table_index(req).await;
            assert!(res.is_ok());
        }

        {
            info!("--- check table index after drop");
            let seqv = mt.get_table_by_id(table_id).await?.unwrap();
            let table_meta = seqv.data;
            assert_eq!(table_meta.indexes.len(), 1);

            let index1 = table_meta.indexes.get(&index_name_1);
            assert!(index1.is_none());

            let index2 = table_meta.indexes.get(&index_name_2);
            assert!(index2.is_some());
            let index2 = index2.unwrap();
            assert_eq!(index2.column_ids, index_column_ids_2);
        }

        Ok(())
    }

    #[fastrace::trace]
    async fn index_create_list_drop<MT>(&self, mt: &MT) -> anyhow::Result<()>
    where MT: SchemaApi + kvapi::AsKVApi<Error = MetaError> {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_literal(tenant_name);

        let mut util = Util::new(mt, tenant_name, "db1", "tb1", "eng1");
        let table_id;
        let index_id;

        info!("--- prepare db and table");
        {
            util.create_db().await?;
            let (tid, _table_meta) = util.create_table().await?;
            table_id = tid;
        }

        let created_on = Utc::now();

        let index_name_1 = "idx1";
        let index_meta_1 = IndexMeta {
            table_id,
            index_type: IndexType::AGGREGATING,
            created_on,
            dropped_on: None,
            updated_on: None,
            original_query: "SELECT a, SUM(b) FROM tb1 WHERE a > 1 GROUP BY b".to_string(),
            query: "SELECT a, SUM(b) FROM tb1 WHERE a > 1 GROUP BY b".to_string(),
            sync_creation: false,
        };

        let index_name_2 = "idx2";
        let index_meta_2 = IndexMeta {
            table_id,
            index_type: IndexType::AGGREGATING,
            created_on,
            dropped_on: None,
            updated_on: None,
            original_query: "SELECT a, SUM(b) FROM tb1 WHERE b > 1 GROUP BY b".to_string(),
            query: "SELECT a, SUM(b) FROM tb1 WHERE b > 1 GROUP BY b".to_string(),
            sync_creation: false,
        };

        let name_ident_1 = IndexNameIdent::new(&tenant, index_name_1);

        let name_ident_2 = IndexNameIdent::new(&tenant, index_name_2);

        {
            info!("--- list index with no create before");
            let req = ListIndexesReq::new(&tenant, Some(table_id));

            let res = mt.list_indexes(req).await?;
            assert!(res.is_empty())
        }

        {
            info!("--- create index");
            let req = CreateIndexReq {
                create_option: CreateOption::Create,
                name_ident: name_ident_1.clone(),
                meta: index_meta_1.clone(),
            };

            let res = mt.create_index(req).await?;
            index_id = res.index_id;

            let req = CreateIndexReq {
                create_option: CreateOption::Create,
                name_ident: name_ident_2.clone(),
                meta: index_meta_2.clone(),
            };

            mt.create_index(req).await?;
        }

        {
            info!("--- create index again with if_not_exists = false");
            let req = CreateIndexReq {
                create_option: CreateOption::Create,
                name_ident: name_ident_1.clone(),
                meta: index_meta_1.clone(),
            };

            let res = mt.create_index(req).await;
            let status = res.err().unwrap();
            let err_code = ErrorCode::from(status);

            assert_eq!(ErrorCode::INDEX_ALREADY_EXISTS, err_code.code());
        }

        {
            info!("--- create index again with if_not_exists = true");
            let req = CreateIndexReq {
                create_option: CreateOption::CreateIfNotExists,
                name_ident: name_ident_1.clone(),
                meta: index_meta_1.clone(),
            };

            let res = mt.create_index(req).await?;
            assert_eq!(index_id, res.index_id);
        }

        {
            info!("--- list index");
            let req = ListIndexesReq::new(&tenant, None);

            let res = mt.list_indexes(req).await?;
            assert_eq!(2, res.len());

            let req = ListIndexesReq::new(&tenant, Some(u64::MAX));

            let res = mt.list_indexes(req).await?;
            assert!(res.is_empty())
        }

        {
            info!("--- list indexes by table id");
            let req = ListIndexesByIdReq::new(&tenant, table_id);

            let res = mt.list_indexes_by_table_id(req).await?;
            assert_eq!(2, res.len());
        }

        {
            info!("--- drop index");
            let req = DropIndexReq {
                if_exists: false,
                name_ident: name_ident_2.clone(),
            };

            let res = mt.drop_index(req).await;
            assert!(res.is_ok())
        }

        {
            info!("--- list index after drop one");
            let req = ListIndexesReq::new(&tenant, Some(table_id));

            let res = mt.list_indexes(req).await?;
            assert_eq!(1, res.len());
        }

        {
            info!("--- check list index content");
            let req = ListIndexesReq::new(&tenant, Some(table_id));

            let res = mt.list_indexes(req).await?;
            assert_eq!(1, res.len());
            assert_eq!(
                vec![(index_id, index_name_1.to_string(), index_meta_1.clone())],
                res
            );
        }

        {
            info!("--- list index after drop all");
            let req = DropIndexReq {
                if_exists: false,
                name_ident: name_ident_1.clone(),
            };

            let res = mt.drop_index(req).await;
            assert!(res.is_ok());

            let req = ListIndexesReq::new(&tenant, Some(table_id));

            let res = mt.list_indexes(req).await?;
            assert!(res.is_empty())
        }

        {
            info!("--- drop index with if exists = false");
            let req = DropIndexReq {
                if_exists: false,
                name_ident: name_ident_1.clone(),
            };

            let res = mt.drop_index(req).await;
            assert!(res.is_err())
        }

        {
            info!("--- drop index with if exists = true");
            let req = DropIndexReq {
                if_exists: true,
                name_ident: name_ident_1.clone(),
            };

            let res = mt.drop_index(req).await;
            assert!(res.is_ok())
        }

        // create or replace index
        {
            info!("--- create or replace index");
            let replace_index_name = "replace_idx";
            let replace_name_ident = IndexNameIdent::new(&tenant, replace_index_name);
            let req = CreateIndexReq {
                create_option: CreateOption::Create,
                name_ident: replace_name_ident.clone(),
                meta: index_meta_1.clone(),
            };

            let get_req = GetIndexReq {
                name_ident: replace_name_ident.clone(),
            };

            let res = mt.create_index(req).await?;
            let old_index_id = res.index_id;
            let old_index_id_key = IndexId {
                index_id: old_index_id,
            };
            let meta: IndexMeta = get_kv_data(mt.as_kv_api(), &old_index_id_key).await?;
            assert_eq!(meta, index_meta_1);

            let resp = mt.get_index(get_req.clone()).await?;

            assert_eq!(resp.index_meta, index_meta_1);

            let req = CreateIndexReq {
                create_option: CreateOption::CreateOrReplace,
                name_ident: replace_name_ident.clone(),
                meta: index_meta_2.clone(),
            };

            let res = mt.create_index(req).await?;

            // assert old index id key has been deleted
            let meta: IndexMeta = get_kv_data(mt.as_kv_api(), &old_index_id_key).await?;
            assert!(meta.dropped_on.is_some());

            // assert new index id key has been created
            let index_id = res.index_id;
            let index_id_key = IndexId { index_id };
            let meta: IndexMeta = get_kv_data(mt.as_kv_api(), &index_id_key).await?;
            assert_eq!(meta, index_meta_2);

            let resp = mt.get_index(get_req).await?;

            assert_eq!(resp.index_meta, index_meta_2);
        }

        Ok(())
    }

    #[fastrace::trace]
    async fn virtual_column_create_list_drop<MT>(&self, mt: &MT) -> anyhow::Result<()>
    where MT: SchemaApi + kvapi::AsKVApi<Error = MetaError> {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_literal(tenant_name);

        let mut util = Util::new(mt, tenant_name, "db1", "tb1", "eng1");
        let table_id;

        info!("--- prepare db and table");
        {
            util.create_db().await?;
            let (tid, _table_meta) = util.create_table().await?;
            table_id = tid;
        }

        let name_ident = VirtualColumnIdent::new(&tenant, table_id);

        {
            info!("--- list virtual columns with no create before");
            let req = ListVirtualColumnsReq::new(&tenant, Some(table_id));

            let res = mt.list_virtual_columns(req).await?;
            assert!(res.is_empty())
        }

        {
            info!("--- create virtual column");
            let req = CreateVirtualColumnReq {
                create_option: CreateOption::Create,
                name_ident: name_ident.clone(),
                virtual_columns: vec!["variant:k1".to_string(), "variant[1]".to_string()],
            };

            let _res = mt.create_virtual_column(req.clone()).await?;

            info!("--- create virtual column again");
            let req = CreateVirtualColumnReq {
                create_option: CreateOption::Create,
                name_ident: name_ident.clone(),
                virtual_columns: vec!["variant:k1".to_string(), "variant[1]".to_string()],
            };

            let res = mt.create_virtual_column(req).await;
            assert!(res.is_err());
        }

        {
            info!("--- list virtual columns");
            let req = ListVirtualColumnsReq::new(&tenant, Some(table_id));

            let res = mt.list_virtual_columns(req).await?;
            assert_eq!(1, res.len());
            assert_eq!(res[0].virtual_columns, vec![
                "variant:k1".to_string(),
                "variant[1]".to_string(),
            ]);

            let req = ListVirtualColumnsReq::new(&tenant, Some(u64::MAX));

            let res = mt.list_virtual_columns(req).await?;
            assert!(res.is_empty())
        }

        {
            info!("--- update virtual column");
            let req = UpdateVirtualColumnReq {
                if_exists: false,
                name_ident: name_ident.clone(),
                virtual_columns: vec!["variant:k2".to_string(), "variant[2]".to_string()],
            };

            let _res = mt.update_virtual_column(req).await?;
        }

        {
            info!("--- list virtual columns after update");
            let req = ListVirtualColumnsReq::new(&tenant, Some(table_id));

            let res = mt.list_virtual_columns(req).await?;
            assert_eq!(1, res.len());
            assert_eq!(res[0].virtual_columns, vec![
                "variant:k2".to_string(),
                "variant[2]".to_string(),
            ]);
        }

        {
            info!("--- drop virtual column");
            let req = DropVirtualColumnReq {
                if_exists: false,
                name_ident: name_ident.clone(),
            };

            let _res = mt.drop_virtual_column(req).await?;
        }

        {
            info!("--- list virtual columns after drop");
            let req = ListVirtualColumnsReq::new(&tenant, Some(table_id));

            let res = mt.list_virtual_columns(req).await?;
            assert_eq!(0, res.len());
        }

        {
            info!("--- update virtual column after drop");
            let req = UpdateVirtualColumnReq {
                if_exists: false,
                name_ident: name_ident.clone(),
                virtual_columns: vec!["variant:k3".to_string(), "variant[3]".to_string()],
            };

            let res = mt.update_virtual_column(req).await;
            assert!(res.is_err());
        }

        {
            info!("--- create or replace virtual column");
            let req = CreateVirtualColumnReq {
                create_option: CreateOption::Create,
                name_ident: name_ident.clone(),
                virtual_columns: vec!["variant:k1".to_string(), "variant[1]".to_string()],
            };

            let _res = mt.create_virtual_column(req.clone()).await?;

            let req = ListVirtualColumnsReq::new(&tenant, Some(table_id));

            let res = mt.list_virtual_columns(req).await?;
            assert_eq!(1, res.len());
            assert_eq!(res[0].virtual_columns, vec![
                "variant:k1".to_string(),
                "variant[1]".to_string(),
            ]);

            let req = CreateVirtualColumnReq {
                create_option: CreateOption::CreateOrReplace,
                name_ident: name_ident.clone(),
                virtual_columns: vec!["variant:k2".to_string()],
            };

            let _res = mt.create_virtual_column(req.clone()).await?;

            let req = ListVirtualColumnsReq::new(&tenant, Some(table_id));

            let res = mt.list_virtual_columns(req).await?;
            assert_eq!(1, res.len());
            assert_eq!(res[0].virtual_columns, vec!["variant:k2".to_string(),]);
        }

        Ok(())
    }

    #[fastrace::trace]
    async fn table_lock_revision<MT>(&self, mt: &MT) -> anyhow::Result<()>
    where MT: SchemaApi + kvapi::AsKVApi<Error = MetaError> {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_literal(tenant_name);

        let mut util = Util::new(mt, tenant_name, "db1", "tb1", "eng1");
        let table_id;

        info!("--- prepare db and table");
        {
            util.create_db().await?;
            let (tid, _table_meta) = util.create_table().await?;
            table_id = tid;
        }

        {
            info!("--- create table lock revision 1");
            let req1 = CreateLockRevReq {
                lock_key: LockKey::Table {
                    tenant: tenant.clone(),
                    table_id,
                },
                expire_secs: 2,
                user: "root".to_string(),
                node: "node1".to_string(),
                query_id: "query1".to_string(),
            };
            let res1 = mt.create_lock_revision(req1).await?;

            info!("--- create table lock revision 2");
            let req2 = CreateLockRevReq {
                lock_key: LockKey::Table {
                    tenant: tenant.clone(),
                    table_id,
                },
                expire_secs: 2,
                user: "root".to_string(),
                node: "node1".to_string(),
                query_id: "query2".to_string(),
            };
            let res2 = mt.create_lock_revision(req2).await?;
            assert!(res2.revision > res1.revision);

            info!("--- list table lock revisiosn");
            let req3 = ListLockRevReq {
                lock_key: LockKey::Table {
                    tenant: tenant.clone(),
                    table_id,
                },
            };
            let res3 = mt.list_lock_revisions(req3).await?;
            assert_eq!(res3.len(), 2);
            assert_eq!(res3[0].0, res1.revision);
            assert_eq!(res3[1].0, res2.revision);

            info!("--- extend table lock revision 2 expire");
            let req4 = ExtendLockRevReq {
                lock_key: LockKey::Table {
                    tenant: tenant.clone(),
                    table_id,
                },
                expire_secs: 4,
                revision: res2.revision,
                acquire_lock: true,
            };
            mt.extend_lock_revision(req4).await?;

            info!("--- table lock revision 1 retired");
            std::thread::sleep(std::time::Duration::from_secs(2));
            let req5 = ListLockRevReq {
                lock_key: LockKey::Table {
                    tenant: tenant.clone(),
                    table_id,
                },
            };
            let res5 = mt.list_lock_revisions(req5).await?;
            assert_eq!(res5.len(), 1);
            assert_eq!(res5[0].0, res2.revision);

            info!("--- delete table lock revision 2");
            let req6 = DeleteLockRevReq {
                lock_key: LockKey::Table {
                    tenant: tenant.clone(),
                    table_id,
                },
                revision: res2.revision,
            };
            mt.delete_lock_revision(req6).await?;

            info!("--- check table locks is empty");
            let req7 = ListLockRevReq {
                lock_key: LockKey::Table { tenant, table_id },
            };
            let res7 = mt.list_lock_revisions(req7).await?;
            assert_eq!(res7.len(), 0);
        }

        Ok(())
    }

    // pub async fn share_create_get_drop<MT: SchemaApi>(&self, mt: &MT) -> anyhow::Result<()> {
    //     let tenant1 = "tenant1";
    //     let share_name1 = "share1";
    //     let share_name2 = "share2";
    //     info!("--- create {}", share_name1);
    //     {
    //         let req = CreateShareReq {
    //             if_not_exists: false,
    //             tenant: tenant1.to_string(),
    //             share_name: share_name1.to_string(),
    //         };
    //
    //         let res = mt.create_share(req).await;
    //         info!("create share res: {:?}", res);
    //         let res = res.unwrap();
    //         assert_eq!(1, res.share_id, "first share id is 1");
    //     }
    //
    //     info!("--- get share1");
    //     {
    //         let res = mt.get_share(GetShareReq::new(tenant1, share_name1)).await;
    //         debug!("get present share res: {:?}", res);
    //         let res = res?;
    //         assert_eq!(1, res.id, "db1 id is 1");
    //         assert_eq!(
    //             share_name1.to_string(),
    //             res.name,
    //             "share1.db is {}",
    //             share_name1
    //         );
    //     }
    //
    //     info!("--- create share1 again with if_not_exists=false");
    //     {
    //         let req = CreateShareReq {
    //             if_not_exists: false,
    //             tenant: tenant1.to_string(),
    //             share_name: share_name1.to_string(),
    //         };
    //
    //         let res = mt.create_share(req).await;
    //         info!("create share res: {:?}", res);
    //         let err = res.unwrap_err();
    //         assert_eq!(
    //             ErrorCode::ShareAlreadyExists("").code(),
    //             ErrorCode::from(err).code()
    //         );
    //     }
    //
    //     info!("--- create share1 again with if_not_exists=true");
    //     {
    //         let req = CreateShareReq {
    //             if_not_exists: true,
    //             tenant: tenant1.to_string(),
    //             share_name: share_name1.to_string(),
    //         };
    //
    //         let res = mt.create_share(req).await;
    //         info!("create database res: {:?}", res);
    //
    //         let res = res.unwrap();
    //         assert_eq!(1, res.share_id, "share1 id is 1");
    //     }
    //
    //     info!("--- create share2");
    //     {
    //         let req = CreateShareReq {
    //             if_not_exists: false,
    //             tenant: tenant1.to_string(),
    //             share_name: share_name2.to_string(),
    //         };
    //
    //         let res = mt.create_share(req).await;
    //         info!("create share res: {:?}", res);
    //         let res = res.unwrap();
    //         assert_eq!(2, res.share_id, "second share id is 2 ");
    //     }
    //
    //     info!("--- get share2");
    //     {
    //         let res = mt.get_share(GetShareReq::new(tenant1, share_name2)).await?;
    //         assert_eq!(2, res.id, "share2 id is 2");
    //         assert_eq!(
    //             share_name2.to_string(),
    //             res.name,
    //             "share2.name is {}",
    //             share_name2
    //         );
    //     }
    //
    //     info!("--- get absent share");
    //     {
    //         let res = mt.get_share(GetShareReq::new(tenant1, "absent")).await;
    //         debug!("=== get absent share res: {:?}", res);
    //         assert!(res.is_err());
    //         let err = res.unwrap_err();
    //         let err_code = ErrorCode::from(err);
    //
    //         assert_eq!(ErrorCode::unknown_share_code(), err_code.code());
    //         assert!(err_code.message().contains("absent"));
    //     }
    //
    //     info!("--- drop share2");
    //     {
    //         mt.drop_share(DropShareReq {
    //             if_exists: false,
    //             tenant: tenant1.to_string(),
    //             share_name: share_name2.to_string(),
    //         })
    //         .await?;
    //     }
    //
    //     info!("--- get share2 should not found");
    //     {
    //         let res = mt.get_share(GetShareReq::new(tenant1, share_name2)).await;
    //         let err = res.unwrap_err();
    //         assert_eq!(
    //             ErrorCode::UnknownShare("").code(),
    //             ErrorCode::from(err).code()
    //         );
    //     }
    //
    //     info!("--- drop share2 with if_exists=true returns no error");
    //     {
    //         mt.drop_share(DropShareReq {
    //             if_exists: true,
    //             tenant: tenant1.to_string(),
    //             share_name: share_name2.to_string(),
    //         })
    //         .await?;
    //     }
    //
    //     Ok(())
    // }
    //
}

/// Supporting utils
impl SchemaApiTestSuite {
    fn req_get_db(tenant: impl ToTenant, db_name: impl ToString) -> GetDatabaseReq {
        GetDatabaseReq {
            inner: DatabaseNameIdent::new(tenant, db_name),
        }
    }

    async fn create_database<MT: SchemaApi>(
        &self,
        mt: &MT,
        tenant: &Tenant,
        db_name: &str,
        engine: &str,
    ) -> anyhow::Result<CreateDatabaseReply> {
        info!("--- create database {}", db_name);

        let req = CreateDatabaseReq {
            create_option: CreateOption::Create,
            name_ident: DatabaseNameIdent::new(tenant, db_name),
            meta: DatabaseMeta {
                engine: engine.to_string(),
                ..Default::default()
            },
        };

        let res = mt.create_database(req).await?;
        info!("create database res: {:?}", res);
        Ok(res)
    }
}

// Test write and read meta on different nodes
// This is meant for testing distributed SchemaApi impl, to ensure a read-after-write consistency.
impl SchemaApiTestSuite {
    /// Create db one node, get db on another
    pub async fn database_get_diff_nodes<MT: SchemaApi>(
        &self,
        node_a: &MT,
        node_b: &MT,
    ) -> anyhow::Result<()> {
        info!("--- create db1 on node_a");
        let tenant_name = "tenant1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        {
            let req = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant, "db1"),
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    ..Default::default()
                },
            };

            let res = node_a.create_database(req).await;
            info!("create database res: {:?}", res);
            let res = res.unwrap();
            assert_eq!(1, res.db_id, "first database id is 1");
        }

        info!("--- get db1 on node_b");
        {
            let res = node_b
                .get_database(GetDatabaseReq::new(tenant.clone(), "db1"))
                .await;
            debug!("get present database res: {:?}", res);
            let res = res?;
            assert_eq!(1, res.ident.db_id, "db1 id is 1");
            assert_eq!("db1", res.name_ident.database_name(), "db1.db is db1");
        }

        info!("--- get nonexistent-db on node_b, expect correct error");
        {
            let res = node_b
                .get_database(GetDatabaseReq::new(tenant.clone(), "nonexistent"))
                .await;
            debug!("get present database res: {:?}", res);
            let err = res.unwrap_err();
            let err = ErrorCode::from(err);
            assert_eq!(ErrorCode::UNKNOWN_DATABASE, err.code());
            assert_eq!("Unknown database 'nonexistent'", err.message());
            assert_eq!(
                "UnknownDatabase. Code: 1003, Text = Unknown database 'nonexistent'.",
                err.to_string()
            );
        }

        Ok(())
    }

    /// Create dbs on node_a, list dbs on node_b
    pub async fn list_database_diff_nodes<MT: SchemaApi>(
        &self,
        node_a: &MT,
        node_b: &MT,
    ) -> anyhow::Result<()> {
        info!("--- create db1 and db3 on node_a");
        let tenant_name = "tenant1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        let mut db_ids = vec![];
        {
            let dbs = vec!["db1", "db3"];
            for db_name in dbs {
                let req = CreateDatabaseReq {
                    create_option: CreateOption::Create,
                    name_ident: DatabaseNameIdent::new(&tenant, db_name),
                    meta: DatabaseMeta {
                        engine: "github".to_string(),
                        ..Default::default()
                    },
                };
                let res = node_a.create_database(req).await?;
                db_ids.push(res.db_id);
            }
        }

        info!("--- list databases from node_b");
        {
            let res = node_b
                .list_databases(ListDatabaseReq {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    filter: None,
                })
                .await;
            debug!("get database list: {:?}", res);
            let res = res?;
            assert_eq!(2, res.len(), "database list len is 2");
            assert_eq!(db_ids[0], res[0].ident.db_id, "db1 id");
            assert_eq!("db1", res[0].name_ident.database_name(), "db1.name is db1");
            assert_eq!(db_ids[1], res[1].ident.db_id, "db3 id");
            assert_eq!("db3", res[1].name_ident.database_name(), "db3.name is db3");
        }

        Ok(())
    }

    /// Create table on node_a, list table on node_b
    pub async fn list_table_diff_nodes<MT: SchemaApi>(
        &self,
        node_a: &MT,
        node_b: &MT,
    ) -> anyhow::Result<()> {
        info!("--- create db1 and tb1, tb2 on node_a");
        let tenant_name = "tenant1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;
        let db_name = "db1";

        let mut tb_ids = vec![];

        {
            let req = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    ..Default::default()
                },
            };
            let res = node_a.create_database(req).await;
            info!("create database res: {:?}", res);
            assert!(res.is_ok());

            let tables = vec!["tb1", "tb2"];
            let schema = Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]));

            let options = maplit::btreemap! {"opt-1".into() => "val-1".into()};
            for tb in tables {
                let req = CreateTableReq {
                    create_option: CreateOption::Create,
                    name_ident: TableNameIdent {
                        tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                        db_name: db_name.to_string(),
                        table_name: tb.to_string(),
                    },
                    table_meta: TableMeta {
                        schema: schema.clone(),
                        engine: "JSON".to_string(),
                        options: options.clone(),
                        ..Default::default()
                    },
                    as_dropped: false,
                };
                let old_db = node_a
                    .get_database(Self::req_get_db(&tenant, db_name))
                    .await?;
                let res = node_a.create_table(req).await?;
                let cur_db = node_a
                    .get_database(Self::req_get_db(&tenant, db_name))
                    .await?;
                assert!(old_db.ident.seq < cur_db.ident.seq);
                tb_ids.push(res.table_id);
            }
        }

        info!("--- list tables from node_b");
        {
            let res = node_b
                .list_tables(ListTableReq::new(&tenant, db_name))
                .await;
            debug!("get table list: {:?}", res);
            let res = res?;
            assert_eq!(2, res.len(), "table list len is 2");
            assert_eq!(tb_ids[0], res[0].ident.table_id, "tb1 id");
            assert_eq!("tb1", res[0].name, "tb1.name is tb1");
            assert_eq!(tb_ids[1], res[1].ident.table_id, "tb2 id");
            assert_eq!("tb2", res[1].name, "tb2.name is tb2");
        }

        Ok(())
    }

    /// Create table on node_a, get table on node_b
    pub async fn table_get_diff_nodes<MT: SchemaApi>(
        &self,
        node_a: &MT,
        node_b: &MT,
    ) -> anyhow::Result<()> {
        info!("--- create table tb1 on node_a");
        let tenant_name = "tenant1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        let db_name = "db1";
        let tb_id = {
            let req = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),
                meta: DatabaseMeta {
                    engine: "github".to_string(),
                    ..Default::default()
                },
            };
            let res = node_a.create_database(req).await;
            info!("create database res: {:?}", res);
            assert!(res.is_ok());

            let schema = Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]));

            let options = maplit::btreemap! {"opt‐1".into() => "val-1".into()};

            let req = CreateTableReq {
                create_option: CreateOption::Create,
                name_ident: TableNameIdent {
                    tenant: Tenant::new_or_err(tenant_name, func_name!())?,
                    db_name: db_name.to_string(),
                    table_name: "tb1".to_string(),
                },
                table_meta: TableMeta {
                    schema: schema.clone(),
                    engine: "JSON".to_string(),
                    options: options.clone(),
                    ..Default::default()
                },
                as_dropped: false,
            };

            let old_db = node_a
                .get_database(Self::req_get_db(&tenant, db_name))
                .await?;
            let res = node_a.create_table(req).await?;
            let cur_db = node_a
                .get_database(Self::req_get_db(&tenant, db_name))
                .await?;
            assert!(old_db.ident.seq < cur_db.ident.seq);
            res.table_id
        };

        info!("--- get tb1 on node_b");
        {
            let res = node_b
                .get_table(GetTableReq::new(&tenant, "db1", "tb1"))
                .await;
            debug!("get present table res: {:?}", res);
            let res = res?;
            assert_eq!(tb_id, res.ident.table_id, "tb1 id is 1");
            assert_eq!("tb1", res.name, "tb1.name is tb1");
        }

        info!("--- get nonexistent-table on node_b, expect correct error");
        {
            let res = node_b
                .get_table(GetTableReq::new(&tenant, "db1", "nonexistent"))
                .await;
            debug!("get present table res: {:?}", res);
            let err = res.unwrap_err();
            assert_eq!(ErrorCode::UNKNOWN_TABLE, ErrorCode::from(err).code());
        }

        Ok(())
    }

    #[fastrace::trace]
    async fn update_table_with_copied_files<MT: SchemaApi>(&self, mt: &MT) -> anyhow::Result<()> {
        let tenant_name = "tenant1";
        let tenant = Tenant::new_or_err(tenant_name, func_name!())?;

        let db_name = "db1";
        let tbl_name = "tb2";
        let table_id;

        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };

        let options = || maplit::btreemap! {"opt‐1".into() => "val-1".into()};

        let table_meta = |created_on| TableMeta {
            schema: schema(),
            engine: "JSON".to_string(),
            options: options(),
            created_on,
            ..TableMeta::default()
        };

        let created_on = Utc::now();

        info!("--- prepare db and table");
        let tbl_name_ident = TableNameIdent {
            tenant: Tenant::new_or_err(tenant_name, func_name!())?,
            db_name: db_name.to_string(),
            table_name: tbl_name.to_string(),
        };
        {
            let plan = CreateDatabaseReq {
                create_option: CreateOption::Create,
                name_ident: DatabaseNameIdent::new(&tenant, db_name),
                meta: DatabaseMeta {
                    engine: "".to_string(),
                    ..DatabaseMeta::default()
                },
            };

            let _ = mt.create_database(plan).await?;

            let req = CreateTableReq {
                create_option: CreateOption::Create,
                name_ident: tbl_name_ident.clone(),
                table_meta: table_meta(created_on),
                as_dropped: false,
            };
            let resp = mt.create_table(req.clone()).await?;
            table_id = resp.table_id;
        };

        info!("--- create and get stage file info");
        {
            let stage_info = TableCopiedFileInfo {
                etag: Some("etag".to_owned()),
                content_length: 1024,
                last_modified: Some(Utc::now()),
            };
            let mut file_info = BTreeMap::new();
            file_info.insert("file".to_string(), stage_info.clone());

            let copied_file_req = UpsertTableCopiedFileReq {
                file_info: file_info.clone(),
                expire_at: Some((Utc::now().timestamp() + 86400) as u64),
                fail_if_duplicated: true,
            };

            let req = UpdateTableMetaReq {
                table_id,
                seq: MatchSeq::Any,
                new_table_meta: table_meta(created_on),
            };

            let table = mt
                .get_table(GetTableReq {
                    inner: tbl_name_ident.clone(),
                })
                .await?
                .as_ref()
                .clone();

            let req = UpdateMultiTableMetaReq {
                update_table_metas: vec![(req, table)],
                copied_files: vec![(table_id, copied_file_req)],
                ..Default::default()
            };

            let _ = mt.update_multi_table_meta(req).await?;

            let req = GetTableCopiedFileReq {
                table_id,
                files: vec!["file".to_string()],
            };

            let resp = mt.get_table_copied_file_info(req).await?;
            assert_eq!(resp.file_info.len(), 1);
            let resp_stage_info = resp.file_info.get("file");
            assert_eq!(resp_stage_info.unwrap(), &stage_info);
        }

        info!("--- upsert table copied files with duplication, fail if duplicated");
        {
            // get previous file info
            let req = GetTableCopiedFileReq {
                table_id,
                files: vec!["file".to_string()],
            };
            let resp = mt.get_table_copied_file_info(req).await?;
            let previous_file_info = resp.file_info.get("file");

            let stage_info = TableCopiedFileInfo {
                etag: Some("etag".to_owned()),
                content_length: 1024,
                last_modified: Some(Utc::now()),
            };
            let mut file_info = BTreeMap::new();
            file_info.insert("file".to_string(), stage_info.clone());
            file_info.insert("file_not_exist".to_string(), stage_info.clone());

            let copied_file_req = UpsertTableCopiedFileReq {
                file_info: file_info.clone(),
                expire_at: Some((Utc::now().timestamp() + 86400) as u64),
                fail_if_duplicated: true,
            };

            let req = UpdateTableMetaReq {
                table_id,
                seq: MatchSeq::Any,
                new_table_meta: table_meta(created_on),
            };

            let table = mt
                .get_table(GetTableReq {
                    inner: tbl_name_ident.clone(),
                })
                .await?
                .as_ref()
                .clone();

            let req = UpdateMultiTableMetaReq {
                update_table_metas: vec![(req, table)],
                copied_files: vec![(table_id, copied_file_req)],
                ..Default::default()
            };

            let result = mt.update_multi_table_meta(req).await;
            let err = result.unwrap_err();
            let err = ErrorCode::from(err);
            assert_eq!(ErrorCode::UNRESOLVABLE_CONFLICT, err.code());

            let req = GetTableCopiedFileReq {
                table_id,
                files: vec!["file".to_string(), "file_not_exist".to_string()],
            };

            let resp = mt.get_table_copied_file_info(req).await?;
            // "file" exist
            assert_eq!(resp.file_info.len(), 1);
            // "file" not modified
            let resp_stage_info = resp.file_info.get("file");
            assert_eq!(resp_stage_info, previous_file_info);
        }

        info!("--- upsert table copied files with duplication, duplicated checking disabled");
        {
            let stage_info = TableCopiedFileInfo {
                etag: Some("etag".to_owned()),
                content_length: 1024,
                last_modified: Some(Utc::now()),
            };
            let mut file_info = BTreeMap::new();
            file_info.insert("file".to_string(), stage_info.clone());
            file_info.insert("file_not_exist".to_string(), stage_info.clone());

            let copied_file_req = UpsertTableCopiedFileReq {
                file_info: file_info.clone(),
                expire_at: Some((Utc::now().timestamp() + 86400) as u64),
                fail_if_duplicated: false,
            };

            let req = UpdateTableMetaReq {
                table_id,
                seq: MatchSeq::Any,
                new_table_meta: table_meta(created_on),
            };

            let table = mt
                .get_table(GetTableReq {
                    inner: tbl_name_ident,
                })
                .await?
                .as_ref()
                .clone();

            let req = UpdateMultiTableMetaReq {
                update_table_metas: vec![(req, table)],
                copied_files: vec![(table_id, copied_file_req)],
                ..Default::default()
            };

            mt.update_multi_table_meta(req).await?.unwrap();

            let req = GetTableCopiedFileReq {
                table_id,
                files: vec!["file".to_string(), "file_not_exist".to_string()],
            };

            let resp = mt.get_table_copied_file_info(req).await?;
            assert_eq!(resp.file_info.len(), 2);
            let resp_stage_info = resp.file_info.get("file");
            assert_eq!(resp_stage_info.unwrap(), &stage_info);
        }

        Ok(())
    }

    #[fastrace::trace]
    async fn dictionary_create_list_drop<MT>(&self, mt: &MT) -> anyhow::Result<()>
    where MT: SchemaApi + kvapi::AsKVApi<Error = MetaError> {
        let tenant_name = "tenant1";
        let db_name = "db1";
        let tbl_name = "tb2";
        let dict_name = "dict3";

        let mut util = Util::new(mt, tenant_name, db_name, tbl_name, "eng1");
        let dict_id;
        let db_id;

        info!("--- prepare db");
        {
            util.create_db().await?;
            db_id = util.db_id;
        }

        let schema = || {
            Arc::new(TableSchema::new(vec![TableField::new(
                "number",
                TableDataType::Number(NumberDataType::UInt64),
            )]))
        };
        let options = || maplit::btreemap! {"opt‐1".into() => "val-1".into()};

        let dictionary_meta = DictionaryMeta {
            source: "mysql".to_string(),
            schema: schema(),
            options: options(),
            created_on: Utc::now(),
            ..DictionaryMeta::default()
        };

        let _unknown_database_code = ErrorCode::UNKNOWN_DATABASE;

        {
            info!("--- list dictionary with no create before");
            let req = ListDictionaryReq::new(db_id, dict_name.to_string());
            let res = mt.list_dictionaries(req).await?;
            assert!(res.is_empty())
        }

        let name_ident_dict = DictionaryNameIdent {
            tenant: Tenant::new_or_err(tenant_name, func_name!())?,
            db_name: db_name.to_string(),
            dictionary_name: dict_name.to_string(),
        };

        {
            info!("--- create dictionary");
            let req = CreateDictionaryReq {
                create_option: CreateOption::Create,
                name_ident: name_ident_dict.clone(),
                dictionary_meta: dictionary_meta.clone(),
            };
            let res = mt.create_dictionary(req).await?;
            dict_id = res.dictionary_id;
        }

        {
            info!("--- create dictionary again with if_not_exists = false");
            let req = CreateDictionaryReq {
                create_option: CreateOption::Create,
                name_ident: name_ident_dict.clone(),
                dictionary_meta: dictionary_meta.clone(),
            };
            let res = mt.create_dictionary(req).await;
            let status = res.err().unwrap();
            let err_code = ErrorCode::from(status);

            assert_eq!(ErrorCode::DICTIONARY_ALREADY_EXISTS, err_code.code());
        }

        {
            info!("--- create dictionary again with if_not_exists = true");
            let req = CreateDictionaryReq {
                create_option: CreateOption::CreateIfNotExists,
                name_ident: name_ident_dict.clone(),
                dictionary_meta: dictionary_meta.clone(),
            };
            let res = mt.create_dictionary(req).await?;
            assert_eq!(dict_id, res.dictionary_id);
        }

        {
            info!("--- get dictionary");
            let db_id_dict_name = DBIdDictionaryName {
                db_id: util.db_id,
                dictionary_name: dict_name.to_string(),
            };
            let req = GetDictionaryReq { db_id_dict_name };
            let res = mt.get_dictionary(req).await?;
            assert!(res.is_some());

            let db_id_dict_name_dummy = DBIdDictionaryName {
                db_id: util.db_id,
                dictionary_name: "dummy_dict".to_string(),
            };
            let req = GetDictionaryReq {
                db_id_dict_name: db_id_dict_name_dummy,
            };
            let res = mt.get_dictionary(req).await?;
            assert!(res.is_none());
        }

        {
            info!("--- list dictionary");
            let req = ListDictionaryReq::new(db_id, dict_name.to_string());
            let res = mt.list_dictionaries(req).await?;
            println!("res={:?}", res);

            assert_eq!(1, res.len());
        }

        {
            info!("--- drop dictionary");
            let req = DropDictionaryReq::new(false, db_id, dict_name.to_string());

            let res = mt.drop_dictionary(req).await;
            assert!(res.is_ok())
        }

        {
            info!("--- list dictionary after drop one");
            let req = ListDictionaryReq::new(db_id, dict_name.to_string());
            let res = mt.list_dictionaries(req).await?;
            assert_eq!(0, res.len());
        }

        {
            info!("--- drop dictionary with if_exists = false");
            let req = DropDictionaryReq::new(false, db_id, dict_name.to_string());
            let res = mt.drop_dictionary(req).await;
            assert!(res.is_err())
        }

        {
            info!("--- drop dictionary with if_exists = true");
            let req = DropDictionaryReq::new(true, db_id, dict_name.to_string());
            let res = mt.drop_dictionary(req).await;
            assert!(res.is_ok())
        }

        Ok(())
    }
}

struct Util<'a, MT>
// where MT: SchemaApi
where MT: kvapi::AsKVApi<Error = MetaError> + SchemaApi
{
    tenant: Tenant,
    db_name: String,
    table_name: String,
    engine: String,
    created_on: DateTime<Utc>,
    db_id: u64,
    table_id: u64,
    mt: &'a MT,
}

impl<'a, MT> Util<'a, MT>
where MT: SchemaApi + kvapi::AsKVApi<Error = MetaError>
{
    fn new(
        mt: &'a MT,
        tenant: impl ToString,
        db_name: impl ToString,
        tbl_name: impl ToString,
        engine: impl ToString,
    ) -> Self {
        Self {
            tenant: Tenant::new_or_err(tenant, func_name!()).unwrap(),
            db_name: db_name.to_string(),
            table_name: tbl_name.to_string(),
            engine: engine.to_string(),
            created_on: Utc::now(),
            db_id: 0,
            table_id: 0,
            mt,
        }
    }

    fn tenant(&self) -> Tenant {
        self.tenant.clone()
    }

    fn db_name(&self) -> String {
        self.db_name.clone()
    }

    fn tbl_name(&self) -> String {
        self.table_name.clone()
    }

    fn engine(&self) -> String {
        self.engine.clone()
    }

    fn schema(&self) -> Arc<TableSchema> {
        Arc::new(TableSchema::new(vec![
            TableField::new("number", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new("variant", TableDataType::Variant),
        ]))
    }

    fn options(&self) -> BTreeMap<String, String> {
        maplit::btreemap! {"opt‐1".into() => "val-1".into()}
    }

    fn table_meta(&self) -> TableMeta {
        TableMeta {
            schema: self.schema(),
            engine: self.engine(),
            options: self.options(),
            created_on: self.created_on,
            ..TableMeta::default()
        }
    }

    async fn create_db(&mut self) -> anyhow::Result<()> {
        let plan = CreateDatabaseReq {
            create_option: CreateOption::Create,
            name_ident: DatabaseNameIdent::new(self.tenant(), self.db_name()),
            meta: DatabaseMeta {
                engine: self.engine(),
                ..DatabaseMeta::default()
            },
        };

        let res = self.mt.create_database(plan).await?;
        self.db_id = res.db_id;

        Ok(())
    }

    #[allow(dead_code)]
    async fn drop_db(&self) -> anyhow::Result<()> {
        let req = DropDatabaseReq {
            if_exists: false,
            name_ident: DatabaseNameIdent::new(self.tenant(), self.db_name()),
        };

        self.mt.drop_database(req).await?;
        Ok(())
    }

    async fn create_table(&mut self) -> anyhow::Result<(u64, TableMeta)> {
        let table_meta = self.table_meta();
        let req = CreateTableReq {
            create_option: CreateOption::Create,
            name_ident: TableNameIdent {
                tenant: self.tenant(),
                db_name: self.db_name(),
                table_name: self.tbl_name(),
            },
            table_meta: table_meta.clone(),
            as_dropped: false,
        };
        let resp = self.mt.create_table(req.clone()).await?;
        let table_id = resp.table_id;

        self.table_id = table_id;

        Ok((table_id, table_meta))
    }

    async fn drop_table_by_id(&mut self) -> anyhow::Result<()> {
        let req = DropTableByIdReq {
            tenant: self.tenant().clone(),
            table_name: self.tbl_name(),
            if_exists: false,
            db_id: self.db_id,
            tb_id: self.table_id,
        };
        self.mt.drop_table_by_id(req.clone()).await?;

        Ok(())
    }

    async fn update_copied_files(
        &self,
        n: usize,
    ) -> anyhow::Result<BTreeMap<String, TableCopiedFileInfo>> {
        let mut file_infos = BTreeMap::new();

        for i in 0..n {
            let stage_info = TableCopiedFileInfo {
                etag: Some(format!("etag{}", i)),
                content_length: 1024,
                last_modified: Some(Utc::now()),
            };
            file_infos.insert(format!("file{}", i), stage_info);
        }

        let copied_file_req = UpsertTableCopiedFileReq {
            file_info: file_infos.clone(),
            expire_at: Some((Utc::now().timestamp() + 86400) as u64),
            fail_if_duplicated: true,
        };

        let req = UpdateTableMetaReq {
            table_id: self.table_id,
            seq: MatchSeq::Any,
            new_table_meta: self.table_meta(),
        };

        let req = UpdateMultiTableMetaReq {
            update_table_metas: vec![(req, Default::default())],
            copied_files: vec![(self.table_id, copied_file_req)],
            ..Default::default()
        };

        self.mt.update_multi_table_meta(req).await?.unwrap();

        Ok(file_infos)
    }
}
