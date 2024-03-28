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

//! Wrapping of the parent directory containing iceberg tables

use std::sync::Arc;

use async_trait::async_trait;
use databend_common_catalog::database::Database;
use databend_common_catalog::table::Table;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::DatabaseIdent;
use databend_common_meta_app::schema::DatabaseInfo;
use databend_common_meta_app::schema::DatabaseMeta;
use databend_common_meta_app::schema::DatabaseNameIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_common_storage::DataOperator;
use futures::StreamExt;
use opendal::EntryMode;
use opendal::Metakey;

use crate::table::IcebergTable;

#[derive(Clone, Debug)]
pub struct IcebergDatabase {
    /// catalog this database belongs to
    ctl_name: String,
    /// operator pointing to the directory holding iceberg tables
    db_root: DataOperator,
    /// database information
    info: DatabaseInfo,
}

impl IcebergDatabase {
    /// create a new database, but from reading
    pub fn create(ctl_name: &str, db_name: &str, db_root: DataOperator) -> Self {
        let info = DatabaseInfo {
            ident: DatabaseIdent { db_id: 0, seq: 0 },
            name_ident: DatabaseNameIdent {
                tenant: Tenant::new_literal("dummy"),
                db_name: db_name.to_string(),
            },
            meta: DatabaseMeta {
                engine: "iceberg".to_string(),
                created_on: chrono::Utc::now(),
                updated_on: chrono::Utc::now(),
                ..Default::default()
            },
        };
        Self {
            ctl_name: ctl_name.to_string(),
            db_root,
            info,
        }
    }
}

#[async_trait]
impl Database for IcebergDatabase {
    fn name(&self) -> &str {
        &self.info.name_ident.db_name
    }

    fn get_db_info(&self) -> &DatabaseInfo {
        &self.info
    }

    #[async_backtrace::framed]
    async fn get_table(&self, table_name: &str) -> Result<Arc<dyn Table>> {
        let path = format!("{table_name}/");
        let op = self.db_root.operator();
        // check existence first
        if !op.stat(&path).await?.mode().is_dir() {
            return Err(ErrorCode::UnknownTable(format!(
                "table {table_name} does not exist or is not a valid table"
            )));
        }

        let table_sp = self.db_root.params().map_root(|r| format!("{r}{path}"));
        let table_sp = table_sp.auto_detect().await?;
        let tbl_root = DataOperator::try_create(&table_sp).await?;

        let tbl = IcebergTable::try_create_from_iceberg_catalog(
            &self.ctl_name,
            &self.info.name_ident.db_name,
            table_name,
            tbl_root,
        )
        .await?;
        let tbl = Arc::new(tbl) as Arc<dyn Table>;

        Ok(tbl)
    }

    #[async_backtrace::framed]
    async fn list_tables(&self) -> Result<Vec<Arc<dyn Table>>> {
        let mut tables = vec![];
        let op = self.db_root.operator();
        let mut lister = op.lister_with("/").metakey(Metakey::Mode).await?;
        while let Some(entry) = lister.next().await.transpose()? {
            let meta = entry.metadata();
            if meta.mode() != EntryMode::DIR {
                continue;
            }
            let tbl_name = entry.name().trim_end_matches('/');
            let table = self.get_table(tbl_name).await?;
            tables.push(table);
        }
        Ok(tables)
    }
}
