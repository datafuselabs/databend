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

use core::fmt;
use std::collections::BTreeMap;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use chrono::DateTime;
use chrono::Utc;
use databend_common_expression::TableSchema;

use super::database_name_ident::DatabaseNameIdent;
use super::CreateOption;
use crate::tenant::Tenant;
use crate::tenant::ToTenant;

/// Represents the metadata of a dictionary within the system.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct DictionaryMeta {
    // The source of the dictionary, which specifies where the dictionary data comes from, like `MySQL`.
    pub source: String,
    // Specify the configuration related to the data source in the form of key-value pairs.
    // For example, `host='localhost' user='root' password='1234'`
    pub options: BTreeMap<String, String>,
    // Schema refers to an external table that corresponds to the dictionary.
    // This is typically used to understand the layout and types of data within the dictionary.
    pub schema: Arc<TableSchema>,
    // A list of comments for each field in the dictionary.
    // For example, if we have `id, address` fields, then field_comments could be `[ 'student's number','home address']`
    pub field_comments: Vec<String>,
    // A list of primary column IDs.
    // For example, vec![1, 2] indicating the first and second columns are the primary keys.
    pub primary_column_ids: Vec<u32>,
    // A general comment string that can be used to provide additional notes or information about the dictionary.
    pub comment: String,
    // The timestamp indicating when the dictionary was created, in Coordinated Universal Time (UTC).
    pub created_on: DateTime<Utc>,
    // if used in CreateDictionaryReq,
    // `updated_on` MUST set to None.
    pub updated_on: Option<DateTime<Utc>>,
}

impl Display for DictionaryMeta {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "Source: {}={:?}, Schema: {:?}, Primary_Column_Id: {:?}, CreatedOn: {:?}",
            self.source, self.options, self.schema, self.primary_column_ids, self.created_on
        )
    }
}

impl Default for DictionaryMeta {
    fn default() -> Self {
        DictionaryMeta {
            source: "".to_string(),
            options: BTreeMap::new(),
            schema: Arc::new(TableSchema::empty()),
            primary_column_ids: Vec::new(),
            created_on: Utc::now(),
            updated_on: None,
            comment: "".to_string(),
            field_comments: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateDictionaryReq {
    pub create_option: CreateOption,
    pub name_ident: DictionaryNameIdent,
    pub dictionary_meta: DictionaryMeta,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DictionaryNameIdent {
    pub tenant: Tenant,
    pub db_name: String,
    pub dictionary_name: String,
}

impl DictionaryNameIdent {
    pub fn new(
        tenant: impl ToTenant,
        db_name: impl ToString,
        dictionary_name: impl ToString,
    ) -> DictionaryNameIdent {
        DictionaryNameIdent {
            tenant: tenant.to_tenant(),
            db_name: db_name.to_string(),
            dictionary_name: dictionary_name.to_string(),
        }
    }

    pub fn tenant(&self) -> &Tenant {
        &self.tenant
    }

    pub fn dictionary_name(&self) -> String {
        self.dictionary_name.clone()
    }

    pub fn db_name_ident(&self) -> DatabaseNameIdent {
        DatabaseNameIdent::new(&self.tenant, &self.db_name)
    }

    pub fn tenant_name(&self) -> &str {
        self.tenant.tenant_name()
    }

    pub fn new_generic(
        tenant: impl ToTenant,
        dictionary_name: impl ToString,
        db_name: impl ToString,
    ) -> Self {
        Self {
            tenant: tenant.to_tenant(),
            dictionary_name: dictionary_name.to_string(),
            db_name: db_name.to_string(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq)]
pub struct CreateDictionaryReply {
    pub dictionary_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropDictionaryReq {
    pub if_exists: bool,
    pub db_id_dict_name: DBIdDictionaryName,
}

impl DropDictionaryReq {
    pub fn new(if_exists: bool, db_id: u64, dict_name: String) -> DropDictionaryReq {
        let db_id_dict_name = DBIdDictionaryName {
            db_id,
            dictionary_name: dict_name,
        };
        DropDictionaryReq {
            if_exists,
            db_id_dict_name,
        }
    }
    pub fn dict_name(&self) -> String {
        self.db_id_dict_name.dictionary_name.clone()
    }

    pub fn db_id(&self) -> u64 {
        self.db_id_dict_name.db_id
    }
}

impl Display for DropDictionaryReq {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "drop_dictionary(if_exists={}):{}/{}",
            self.if_exists,
            self.db_id(),
            self.dict_name(),
        )
    }
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct DropDictionaryReply {}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetDictionaryReq {
    pub db_id_dict_name: DBIdDictionaryName,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct GetDictionaryReply {
    pub dictionary_id: u64,
    pub dictionary_meta: DictionaryMeta,
    /// seq AKA version of this table snapshot.
    /// Any change to a dictionary causes the seq to increment
    pub seq: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct DBIdDictionaryName {
    pub db_id: u64,
    pub dictionary_name: String,
}

impl Display for DBIdDictionaryName {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}.'{}'", self.db_id, self.dictionary_name)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Default)]
pub struct DictionaryId {
    pub dictionary_id: u64,
}

impl DictionaryId {
    pub fn new(dictionary_id: u64) -> DictionaryId {
        DictionaryId { dictionary_id }
    }
}

impl Display for DictionaryId {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "DictionaryId{{{}}}", self.dictionary_id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListDictionaryReq {
    pub db_id_dict_name: DBIdDictionaryName,
}

impl ListDictionaryReq {
    pub fn new(db_id: u64, dict_name: String) -> ListDictionaryReq {
        let db_id_dict_name = DBIdDictionaryName {
            db_id,
            dictionary_name: dict_name,
        };
        ListDictionaryReq { db_id_dict_name }
    }
    pub fn db_id(&self) -> u64 {
        self.db_id_dict_name.db_id
    }

    pub fn dict_name(&self) -> String {
        self.db_id_dict_name.dictionary_name.clone()
    }
}

mod kvapi_key_impl {

    use databend_common_meta_kvapi::kvapi;
    use databend_common_meta_kvapi::kvapi::Key;

    use super::DBIdDictionaryName;
    use super::DictionaryId;
    use super::DictionaryMeta;
    use crate::schema::DatabaseId;

    impl kvapi::KeyCodec for DictionaryId {
        fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
            b.push_u64(self.dictionary_id)
        }

        fn decode_key(parser: &mut kvapi::KeyParser) -> Result<Self, kvapi::KeyError>
        where Self: Sized {
            let dict_id = parser.next_u64()?;
            Ok(Self {
                dictionary_id: dict_id,
            })
        }
    }

    /// "<prefix>/<dictionary_id>"
    impl kvapi::Key for DictionaryId {
        const PREFIX: &'static str = "__fd_dictionary_by_id";

        type ValueType = DictionaryMeta;

        fn parent(&self) -> Option<String> {
            None
        }
    }

    impl kvapi::Value for DictionaryMeta {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }

    impl kvapi::KeyCodec for DBIdDictionaryName {
        fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
            b.push_u64(self.db_id).push_str(&self.dictionary_name)
        }

        fn decode_key(parser: &mut kvapi::KeyParser) -> Result<Self, kvapi::KeyError>
        where Self: Sized {
            let db_id = parser.next_u64()?;
            let dictionary_name = parser.next_str()?;
            Ok(Self {
                db_id,
                dictionary_name,
            })
        }
    }

    impl kvapi::Key for DBIdDictionaryName {
        const PREFIX: &'static str = "__fd_dictionary";

        type ValueType = DictionaryId;

        fn parent(&self) -> Option<String> {
            Some(DatabaseId::new(self.db_id).to_string_key())
        }
    }

    impl kvapi::Value for DictionaryId {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            [self.to_string_key()]
        }
    }

    impl kvapi::Value for DBIdDictionaryName {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }
}
