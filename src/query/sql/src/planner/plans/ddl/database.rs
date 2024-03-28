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

use databend_common_expression::DataSchemaRef;
use databend_common_meta_app::schema::CreateDatabaseReq;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::DatabaseMeta;
use databend_common_meta_app::schema::DatabaseNameIdent;
use databend_common_meta_app::schema::DropDatabaseReq;
use databend_common_meta_app::schema::UndropDatabaseReq;
use databend_common_meta_app::tenant::Tenant;

/// Create.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CreateDatabasePlan {
    pub create_option: CreateOption,
    pub tenant: Tenant,
    pub catalog: String,
    pub database: String,
    pub meta: DatabaseMeta,
}

impl From<CreateDatabasePlan> for CreateDatabaseReq {
    fn from(p: CreateDatabasePlan) -> Self {
        CreateDatabaseReq {
            create_option: p.create_option,
            name_ident: DatabaseNameIdent {
                tenant: p.tenant.clone(),
                db_name: p.database,
            },
            meta: p.meta,
        }
    }
}

impl From<&CreateDatabasePlan> for CreateDatabaseReq {
    fn from(p: &CreateDatabasePlan) -> Self {
        CreateDatabaseReq {
            create_option: p.create_option,
            name_ident: DatabaseNameIdent {
                tenant: p.tenant.clone(),
                db_name: p.database.clone(),
            },
            meta: p.meta.clone(),
        }
    }
}

/// Drop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DropDatabasePlan {
    pub if_exists: bool,
    pub tenant: Tenant,
    pub catalog: String,
    pub database: String,
}

impl From<DropDatabasePlan> for DropDatabaseReq {
    fn from(p: DropDatabasePlan) -> Self {
        DropDatabaseReq {
            if_exists: p.if_exists,
            name_ident: DatabaseNameIdent {
                tenant: p.tenant,
                db_name: p.database,
            },
        }
    }
}

impl From<&DropDatabasePlan> for DropDatabaseReq {
    fn from(p: &DropDatabasePlan) -> Self {
        DropDatabaseReq {
            if_exists: p.if_exists,
            name_ident: DatabaseNameIdent {
                tenant: p.tenant.clone(),
                db_name: p.database.clone(),
            },
        }
    }
}

/// Rename.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenameDatabasePlan {
    pub tenant: Tenant,
    pub entities: Vec<RenameDatabaseEntity>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RenameDatabaseEntity {
    pub if_exists: bool,
    pub catalog: String,
    pub database: String,
    pub new_database: String,
}

/// Undrop.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UndropDatabasePlan {
    pub tenant: Tenant,
    pub catalog: String,
    pub database: String,
}

impl From<UndropDatabasePlan> for UndropDatabaseReq {
    fn from(p: UndropDatabasePlan) -> Self {
        UndropDatabaseReq {
            name_ident: DatabaseNameIdent {
                tenant: p.tenant,
                db_name: p.database,
            },
        }
    }
}

/// Use.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UseDatabasePlan {
    pub database: String,
}

/// Show.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShowCreateDatabasePlan {
    pub catalog: String,
    pub database: String,
    pub schema: DataSchemaRef,
}

impl ShowCreateDatabasePlan {
    pub fn schema(&self) -> DataSchemaRef {
        self.schema.clone()
    }
}
