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

use std::str;
use std::sync::Arc;

use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_api::SchemaApi;
use databend_common_meta_api::ShareApi;
use databend_common_meta_app::schema::CommitTableMetaReply;
use databend_common_meta_app::schema::CommitTableMetaReq;
use databend_common_meta_app::schema::CreateTableReply;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::DatabaseInfo;
use databend_common_meta_app::schema::DatabaseType;
use databend_common_meta_app::schema::DropTableByIdReq;
use databend_common_meta_app::schema::DropTableReply;
use databend_common_meta_app::schema::GetTableCopiedFileReply;
use databend_common_meta_app::schema::GetTableCopiedFileReq;
use databend_common_meta_app::schema::RenameTableReply;
use databend_common_meta_app::schema::RenameTableReq;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyReply;
use databend_common_meta_app::schema::SetTableColumnMaskPolicyReq;
use databend_common_meta_app::schema::ShareDbId;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TruncateTableReply;
use databend_common_meta_app::schema::TruncateTableReq;
use databend_common_meta_app::schema::UndropTableReply;
use databend_common_meta_app::schema::UndropTableReq;
use databend_common_meta_app::schema::UpdateMultiTableMetaReq;
use databend_common_meta_app::schema::UpdateMultiTableMetaResult;
use databend_common_meta_app::schema::UpsertTableOptionReply;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_meta_app::share::GetShareEndpointReq;
use databend_common_meta_app::share::ShareEndpointMeta;
use databend_common_meta_app::tenant::Tenant;
use databend_common_sharing::ShareEndpointClient;
use log::error;

use crate::databases::share::dummy_share_database::DummyShareDatabase;
use crate::databases::Database;
use crate::databases::DatabaseContext;

// Share Database implementation for `Database` trait.
#[derive(Clone)]
pub struct ShareDatabase {
    ctx: DatabaseContext,

    db_info: DatabaseInfo,

    from_share_db_id: u64,
}

impl ShareDatabase {
    pub const NAME: &'static str = "SHARE";
    pub fn try_create(ctx: DatabaseContext, db_info: DatabaseInfo) -> Result<Box<dyn Database>> {
        // old share db SQL schema is `create database from <share_name>`,
        // and new share db schema is `create database from <share_name> using <share_endpoint>`
        // for back compatibility, when `using_share_endpoint` is none, return `DummyShareDatabase` instead,
        // which cannot do anything.
        if db_info.meta.using_share_endpoint.is_none() || db_info.meta.from_share_db_id.is_none() {
            return DummyShareDatabase::try_create(ctx, db_info);
        }
        debug_assert!(
            db_info.meta.from_share.is_some()
                && db_info.meta.using_share_endpoint.is_some()
                && db_info.meta.from_share_db_id.is_some()
        );
        let from_share_db_id = db_info.meta.from_share_db_id.as_ref().unwrap();
        let from_share_db_id = match from_share_db_id {
            ShareDbId::Usage(id) => *id,
            ShareDbId::Reference(id) => *id,
        };
        Ok(Box::new(Self {
            ctx,
            db_info,
            from_share_db_id,
        }))
    }

    fn load_share_tables(&self, table_infos: Vec<Arc<TableInfo>>) -> Result<Vec<Arc<dyn Table>>> {
        table_infos.iter().try_fold(vec![], |mut acc, item| {
            let tbl = self.get_table_by_info(item.as_ref());
            match tbl {
                Ok(tbl) => {
                    acc.push(tbl);
                }
                Err(e) => {
                    error!("ShareDatabase get_table_by_info err: {:?}\n", e);
                }
            }

            Ok(acc)
        })
    }

    #[async_backtrace::framed]
    async fn get_share_endpoint_meta(&self) -> Result<ShareEndpointMeta> {
        let endpoint = self.db_info.meta.using_share_endpoint.as_ref().unwrap();
        let req = GetShareEndpointReq {
            tenant: self.get_tenant().clone(),
            endpoint: Some(endpoint.clone()),
        };
        let reply = self.ctx.meta.get_share_endpoint(req).await?;

        if reply.share_endpoint_meta_vec.is_empty() {
            Err(ErrorCode::UnknownShareEndpoint(format!(
                "UnknownShareEndpoint {:?}",
                endpoint
            )))
        } else {
            Ok(reply.share_endpoint_meta_vec[0].1.clone())
        }
    }

    #[async_backtrace::framed]
    async fn add_share_endpoint_into_table_info(&self, table_info: TableInfo) -> Result<TableInfo> {
        let mut table_info = table_info;
        let db_type = table_info.db_type.clone();
        let share_endpoint_meta = self.get_share_endpoint_meta().await?;
        if let DatabaseType::ShareDB(params) = db_type {
            let mut params = params;
            params.share_endpoint_url = share_endpoint_meta.url.clone();
            params.share_endpoint_credential = share_endpoint_meta.credential.clone().unwrap();
            table_info.db_type = DatabaseType::ShareDB(params);
            Ok(table_info)
        } else {
            unreachable!()
        }
    }

    #[async_backtrace::framed]
    async fn get_share_table_info(&self, table_name: &str) -> Result<Arc<TableInfo>> {
        let share_endpoint_meta = self.get_share_endpoint_meta().await?;
        let from_share = self.db_info.meta.from_share.clone().unwrap();

        let client = ShareEndpointClient::new();
        let table_info = client
            .get_share_table_by_name(
                &share_endpoint_meta,
                self.get_tenant().tenant_name(),
                from_share.tenant_name(),
                from_share.share_name(),
                self.from_share_db_id,
                table_name,
            )
            .await?;

        Ok(Arc::new(
            self.add_share_endpoint_into_table_info(table_info.clone())
                .await?,
        ))
    }

    #[async_backtrace::framed]
    async fn list_share_tables(&self) -> Result<Vec<Arc<TableInfo>>> {
        let share_endpoint_meta = self.get_share_endpoint_meta().await?;
        let from_share = self.db_info.meta.from_share.clone().unwrap();

        let client = ShareEndpointClient::new();
        let table_info_map = client
            .get_share_tables_by_db_id(
                &share_endpoint_meta,
                self.get_tenant().tenant_name(),
                from_share.tenant_name(),
                self.from_share_db_id,
                from_share.share_name(),
            )
            .await?;

        let mut table_infos: Vec<Arc<TableInfo>> = Vec::with_capacity(table_info_map.len());
        for table_info in table_info_map.values() {
            table_infos.push(Arc::new(
                self.add_share_endpoint_into_table_info(table_info.to_owned())
                    .await?,
            ));
        }

        Ok(table_infos)
    }
}

#[async_trait::async_trait]
impl Database for ShareDatabase {
    fn name(&self) -> &str {
        self.db_info.name_ident.database_name()
    }

    fn get_db_info(&self) -> &DatabaseInfo {
        &self.db_info
    }

    // use context tenant, instead of db info tenant
    fn get_tenant(&self) -> &Tenant {
        &self.ctx.tenant
    }

    fn get_table_by_info(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        let storage = self.ctx.storage_factory.clone();
        storage.get_table(table_info)
    }

    // Get one table by db and table name.
    #[async_backtrace::framed]
    async fn get_table(&self, table_name: &str) -> Result<Arc<dyn Table>> {
        let table_info = self.get_share_table_info(table_name).await?;
        self.get_table_by_info(table_info.as_ref())
    }

    #[async_backtrace::framed]
    async fn list_tables(&self) -> Result<Vec<Arc<dyn Table>>> {
        let share_table_infos = self.list_share_tables().await?;

        self.load_share_tables(share_table_infos)
    }

    #[async_backtrace::framed]
    async fn list_tables_history(&self) -> Result<Vec<Arc<dyn Table>>> {
        Err(ErrorCode::PermissionDenied(
            "Permission denied, cannot list table history from a shared database".to_string(),
        ))
    }

    #[async_backtrace::framed]
    async fn create_table(&self, _req: CreateTableReq) -> Result<CreateTableReply> {
        Err(ErrorCode::PermissionDenied(
            "Permission denied, cannot create table from a shared database".to_string(),
        ))
    }

    #[async_backtrace::framed]
    async fn drop_table_by_id(&self, _req: DropTableByIdReq) -> Result<DropTableReply> {
        Err(ErrorCode::PermissionDenied(
            "Permission denied, cannot drop table from a shared database".to_string(),
        ))
    }

    #[async_backtrace::framed]
    async fn undrop_table(&self, _req: UndropTableReq) -> Result<UndropTableReply> {
        Err(ErrorCode::PermissionDenied(
            "Permission denied, cannot undrop table from a shared database".to_string(),
        ))
    }

    #[async_backtrace::framed]
    async fn commit_table_meta(&self, _req: CommitTableMetaReq) -> Result<CommitTableMetaReply> {
        Err(ErrorCode::PermissionDenied(
            "Permission denied, cannot commit_table_meta from a shared database".to_string(),
        ))
    }

    #[async_backtrace::framed]
    async fn rename_table(&self, _req: RenameTableReq) -> Result<RenameTableReply> {
        Err(ErrorCode::PermissionDenied(
            "Permission denied, cannot rename table from a shared database".to_string(),
        ))
    }

    #[async_backtrace::framed]
    async fn upsert_table_option(
        &self,
        _req: UpsertTableOptionReq,
    ) -> Result<UpsertTableOptionReply> {
        Err(ErrorCode::PermissionDenied(
            "Permission denied, cannot upsert table option from a shared database".to_string(),
        ))
    }

    #[async_backtrace::framed]
    async fn set_table_column_mask_policy(
        &self,
        _req: SetTableColumnMaskPolicyReq,
    ) -> Result<SetTableColumnMaskPolicyReply> {
        Err(ErrorCode::PermissionDenied(
            "Permission denied, cannot set_table_column_mask_policy from a shared database"
                .to_string(),
        ))
    }

    #[async_backtrace::framed]
    async fn get_table_copied_file_info(
        &self,
        req: GetTableCopiedFileReq,
    ) -> Result<GetTableCopiedFileReply> {
        let res = self.ctx.meta.get_table_copied_file_info(req).await?;
        Ok(res)
    }

    #[async_backtrace::framed]
    async fn truncate_table(&self, _req: TruncateTableReq) -> Result<TruncateTableReply> {
        Err(ErrorCode::PermissionDenied(
            "Permission denied, cannot truncate table from a shared database".to_string(),
        ))
    }

    #[async_backtrace::framed]
    async fn retryable_update_multi_table_meta(
        &self,
        _req: UpdateMultiTableMetaReq,
    ) -> Result<UpdateMultiTableMetaResult> {
        Err(ErrorCode::PermissionDenied(
            "Permission denied, cannot upsert table meta from a shared database".to_string(),
        ))
    }
}
