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

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::ops::Deref;

use chrono::DateTime;
use chrono::Utc;

use super::CreateOption;
use crate::schema::database_name_ident::DatabaseNameIdent;
use crate::share::share_name_ident::ShareNameIdentRaw;
use crate::tenant::Tenant;
use crate::tenant::ToTenant;
use crate::KeyWithTenant;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatabaseInfo {
    pub ident: DatabaseIdent,
    pub name_ident: DatabaseNameIdent,
    pub meta: DatabaseMeta,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct DatabaseIdent {
    pub db_id: u64,
    pub seq: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, PartialOrd, Ord)]
pub struct DatabaseId {
    pub db_id: u64,
}

impl DatabaseId {
    pub fn new(db_id: u64) -> Self {
        DatabaseId { db_id }
    }
}

impl From<u64> for DatabaseId {
    fn from(db_id: u64) -> Self {
        DatabaseId { db_id }
    }
}

impl Display for DatabaseId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.db_id)
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DatabaseIdToName {
    pub db_id: u64,
}

impl Display for DatabaseIdToName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.db_id)
    }
}

impl DatabaseIdToName {
    pub fn new(db_id: u64) -> Self {
        DatabaseIdToName { db_id }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DatabaseMeta {
    pub engine: String,
    pub engine_options: BTreeMap<String, String>,
    pub options: BTreeMap<String, String>,
    pub created_on: DateTime<Utc>,
    pub updated_on: DateTime<Utc>,
    pub comment: String,

    // if used in CreateDatabaseReq, this field MUST set to None.
    pub drop_on: Option<DateTime<Utc>>,
    // shared by share_id
    pub shared_by: BTreeSet<u64>,
    pub from_share: Option<ShareNameIdentRaw>,
}

impl Default for DatabaseMeta {
    fn default() -> Self {
        DatabaseMeta {
            engine: "".to_string(),
            engine_options: BTreeMap::new(),
            options: BTreeMap::new(),
            created_on: Utc::now(),
            updated_on: Utc::now(),
            comment: "".to_string(),
            drop_on: None,
            shared_by: BTreeSet::new(),
            from_share: None,
        }
    }
}

impl Display for DatabaseMeta {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Engine: {}={:?}, Options: {:?}, CreatedOn: {:?}",
            self.engine, self.engine_options, self.options, self.created_on
        )
    }
}

impl DatabaseInfo {
    pub fn engine(&self) -> &str {
        &self.meta.engine
    }
}

/// Save db name id list history.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, Default, PartialEq)]
pub struct DbIdList {
    pub id_list: Vec<u64>,
}

impl DbIdList {
    pub fn new() -> DbIdList {
        DbIdList::default()
    }

    pub fn len(&self) -> usize {
        self.id_list.len()
    }

    pub fn id_list(&self) -> &Vec<u64> {
        &self.id_list
    }

    pub fn append(&mut self, table_id: u64) {
        self.id_list.push(table_id);
    }

    pub fn is_empty(&self) -> bool {
        self.id_list.is_empty()
    }

    pub fn pop(&mut self) -> Option<u64> {
        self.id_list.pop()
    }

    pub fn last(&mut self) -> Option<&u64> {
        self.id_list.last()
    }
}

impl Display for DbIdList {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "DB id list: {:?}", self.id_list)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateDatabaseReq {
    pub create_option: CreateOption,
    pub name_ident: DatabaseNameIdent,
    pub meta: DatabaseMeta,
}

impl Display for CreateDatabaseReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.create_option {
            CreateOption::Create => write!(
                f,
                "create_db:{}/{}={:?}",
                self.name_ident.tenant_name(),
                self.name_ident.database_name(),
                self.meta
            ),
            CreateOption::CreateIfNotExists => write!(
                f,
                "create_db_if_not_exists:{}/{}={:?}",
                self.name_ident.tenant_name(),
                self.name_ident.database_name(),
                self.meta
            ),

            CreateOption::CreateOrReplace => write!(
                f,
                "create_or_replace_db:{}/{}={:?}",
                self.name_ident.tenant_name(),
                self.name_ident.database_name(),
                self.meta
            ),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct CreateDatabaseReply {
    pub db_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenameDatabaseReq {
    pub if_exists: bool,
    pub name_ident: DatabaseNameIdent,
    pub new_db_name: String,
}

impl Display for RenameDatabaseReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "rename_database:{}/{}=>{}",
            self.name_ident.tenant_name(),
            self.name_ident.database_name(),
            self.new_db_name
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct RenameDatabaseReply {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropDatabaseReq {
    pub if_exists: bool,
    pub name_ident: DatabaseNameIdent,
}

impl Display for DropDatabaseReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "drop_db(if_exists={}):{}/{}",
            self.if_exists,
            self.name_ident.tenant_name(),
            self.name_ident.database_name(),
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DropDatabaseReply {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UndropDatabaseReq {
    pub name_ident: DatabaseNameIdent,
}

impl Display for UndropDatabaseReq {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "undrop_db:{}/{}",
            self.name_ident.tenant_name(),
            self.name_ident.database_name(),
        )
    }
}

impl UndropDatabaseReq {
    pub fn tenant(&self) -> &Tenant {
        self.name_ident.tenant()
    }
    pub fn db_name(&self) -> &str {
        self.name_ident.database_name()
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct UndropDatabaseReply {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetDatabaseReq {
    pub inner: DatabaseNameIdent,
}

impl Deref for GetDatabaseReq {
    type Target = DatabaseNameIdent;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl GetDatabaseReq {
    pub fn new(tenant: impl ToTenant, db_name: impl ToString) -> GetDatabaseReq {
        GetDatabaseReq {
            inner: DatabaseNameIdent::new(tenant, db_name),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum DatabaseInfoFilter {
    // include all dropped databases
    IncludeDropped,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListDatabaseReq {
    pub tenant: Tenant,
    pub filter: Option<DatabaseInfoFilter>,
}

impl ListDatabaseReq {
    pub fn tenant(&self) -> &Tenant {
        &self.tenant
    }
}

mod kvapi_key_impl {
    use databend_common_meta_kvapi::kvapi;

    use crate::schema::database_name_ident::DatabaseNameIdentRaw;
    use crate::schema::DatabaseId;
    use crate::schema::DatabaseIdToName;
    use crate::schema::DatabaseMeta;

    impl kvapi::KeyCodec for DatabaseId {
        fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
            b.push_u64(self.db_id)
        }

        fn decode_key(parser: &mut kvapi::KeyParser) -> Result<Self, kvapi::KeyError> {
            let db_id = parser.next_u64()?;
            Ok(Self { db_id })
        }
    }

    /// "__fd_database_by_id/<db_id>"
    impl kvapi::Key for DatabaseId {
        const PREFIX: &'static str = "__fd_database_by_id";

        type ValueType = DatabaseMeta;

        fn parent(&self) -> Option<String> {
            None
        }
    }

    impl kvapi::KeyCodec for DatabaseIdToName {
        fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
            b.push_u64(self.db_id)
        }

        fn decode_key(parser: &mut kvapi::KeyParser) -> Result<Self, kvapi::KeyError> {
            let db_id = parser.next_u64()?;
            Ok(Self { db_id })
        }
    }

    /// "__fd_database_id_to_name/<db_id> -> DatabaseNameIdent"
    impl kvapi::Key for DatabaseIdToName {
        const PREFIX: &'static str = "__fd_database_id_to_name";

        type ValueType = DatabaseNameIdentRaw;

        fn parent(&self) -> Option<String> {
            Some(DatabaseId::new(self.db_id).to_string_key())
        }
    }

    impl kvapi::Value for DatabaseMeta {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }

    impl kvapi::Value for DatabaseNameIdentRaw {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }
}
