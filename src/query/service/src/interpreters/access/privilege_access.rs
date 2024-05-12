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

use std::collections::HashSet;
use std::sync::Arc;

use databend_common_catalog::catalog::Catalog;
use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::GrantObject;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::PrincipalIdentity;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::principal::StageType;
use databend_common_meta_app::principal::UserGrantSet;
use databend_common_meta_app::principal::UserPrivilegeSet;
use databend_common_meta_app::principal::UserPrivilegeType;
use databend_common_meta_app::tenant::Tenant;
use databend_common_sql::optimizer::get_udf_names;
use databend_common_sql::plans::InsertInputSource;
use databend_common_sql::plans::PresignAction;
use databend_common_sql::plans::RewriteKind;
use databend_common_sql::Planner;
use databend_common_users::RoleCacheManager;
use databend_common_users::UserApiProvider;

use crate::interpreters::access::AccessChecker;
use crate::sessions::QueryContext;
use crate::sessions::Session;
use crate::sql::plans::Plan;

pub struct PrivilegeAccess {
    ctx: Arc<QueryContext>,
}

enum ObjectId {
    Database(u64),
    Table(u64, u64),
}

// some statements like `SELECT 1`, `SHOW USERS`, `SHOW ROLES`, `SHOW TABLES` will be
// rewritten to the queries on the system tables, we need to skip the privilege check on
// these tables.
const SYSTEM_TABLES_ALLOW_LIST: [&str; 18] = [
    "catalogs",
    "columns",
    "databases",
    "tables",
    "views",
    "tables_with_history",
    "views_with_history",
    "password_policies",
    "streams",
    "streams_terse",
    "virtual_columns",
    "users",
    "roles",
    "stages",
    "one",
    "processes",
    "user_functions",
    "functions",
];

impl PrivilegeAccess {
    pub fn create(ctx: Arc<QueryContext>) -> Box<dyn AccessChecker> {
        Box::new(PrivilegeAccess { ctx })
    }

    // PrivilegeAccess checks the privilege by names, we'd need to convert the GrantObject to
    // OwnerObject to check the privilege.
    // Currently we checks db/table ownerships by id, stage/udf ownerships by name.
    async fn convert_to_owner_object(
        &self,
        object: &GrantObject,
    ) -> Result<Option<OwnershipObject>> {
        let tenant = self.ctx.get_tenant();

        let object = match object {
            GrantObject::Database(catalog_name, db_name) => {
                if db_name.to_lowercase() == "system" {
                    return Ok(None);
                }
                let db_id = self
                    .ctx
                    .get_catalog(catalog_name)
                    .await?
                    .get_database(&tenant, db_name)
                    .await?
                    .get_db_info()
                    .ident
                    .db_id;
                OwnershipObject::Database {
                    catalog_name: catalog_name.clone(),
                    db_id,
                }
            }
            GrantObject::Table(catalog_name, db_name, table_name) => {
                if db_name.to_lowercase() == "system" {
                    return Ok(None);
                }
                let catalog = self.ctx.get_catalog(catalog_name).await?;
                let db_id = catalog
                    .get_database(&tenant, db_name)
                    .await?
                    .get_db_info()
                    .ident
                    .db_id;
                let table = catalog.get_table(&tenant, db_name, table_name).await?;
                let table_id = table.get_id();
                OwnershipObject::Table {
                    catalog_name: catalog_name.clone(),
                    db_id,
                    table_id,
                }
            }
            GrantObject::DatabaseById(catalog_name, db_id) => OwnershipObject::Database {
                catalog_name: catalog_name.clone(),
                db_id: *db_id,
            },
            GrantObject::TableById(catalog_name, db_id, table_id) => OwnershipObject::Table {
                catalog_name: catalog_name.clone(),
                db_id: *db_id,
                table_id: *table_id,
            },
            GrantObject::Stage(name) => OwnershipObject::Stage {
                name: name.to_string(),
            },
            GrantObject::UDF(name) => OwnershipObject::UDF {
                name: name.to_string(),
            },
            GrantObject::Global => return Ok(None),
        };

        Ok(Some(object))
    }

    async fn validate_db_access(
        &self,
        catalog_name: &str,
        db_name: &str,
        privileges: UserPrivilegeType,
        if_exists: bool,
    ) -> Result<()> {
        let tenant = self.ctx.get_tenant();
        match self
            .validate_access(
                &GrantObject::Database(catalog_name.to_string(), db_name.to_string()),
                privileges,
            )
            .await
        {
            Ok(_) => {
                return Ok(());
            }
            Err(_err) => {
                let catalog = self.ctx.get_catalog(catalog_name).await?;
                match self.convert_to_id(&tenant, &catalog, db_name, None).await {
                    Ok(obj) => {
                        let (db_id, _) = match obj {
                            ObjectId::Table(db_id, table_id) => (db_id, Some(table_id)),
                            ObjectId::Database(db_id) => (db_id, None),
                        };
                        if let Err(err) = self
                            .validate_access(
                                &GrantObject::DatabaseById(catalog_name.to_string(), db_id),
                                privileges,
                            )
                            .await
                        {
                            if err.code() != ErrorCode::PERMISSION_DENIED {
                                return Err(err);
                            }
                            let current_user = self.ctx.get_current_user()?;
                            let session = self.ctx.get_current_session();
                            let roles_name = session
                                .get_all_effective_roles()
                                .await?
                                .iter()
                                .map(|r| r.name.clone())
                                .collect::<Vec<_>>()
                                .join(",");
                            return Err(ErrorCode::PermissionDenied(format!(
                                "Permission denied: privilege [{:?}] is required on '{}'.'{}'.* for user {} with roles [{}]",
                                privileges,
                                catalog_name,
                                db_name,
                                &current_user.identity().display(),
                                roles_name,
                            )));
                        }
                    }
                    Err(e) => match e.code() {
                        ErrorCode::UNKNOWN_DATABASE
                        | ErrorCode::UNKNOWN_TABLE
                        | ErrorCode::UNKNOWN_CATALOG
                            if if_exists =>
                        {
                            return Ok(());
                        }
                        _ => return Err(e.add_message("error on validating database access")),
                    },
                }
            }
        }
        Ok(())
    }

    async fn validate_table_access(
        &self,
        catalog_name: &str,
        db_name: &str,
        table_name: &str,
        privilege: UserPrivilegeType,
        if_exists: bool,
    ) -> Result<()> {
        // skip checking the privilege on system tables.
        if ((db_name == "system" && SYSTEM_TABLES_ALLOW_LIST.iter().any(|x| x == &table_name))
            || db_name == "information_schema")
            && privilege == UserPrivilegeType::Select
        {
            return Ok(());
        }

        let tenant = self.ctx.get_tenant();

        match self.ctx.get_catalog(catalog_name).await {
            Ok(catalog) => {
                if catalog.exists_table_function(table_name) {
                    return Ok(());
                }
                // to keep compatibility with the legacy privileges which granted by table name,
                // we'd both check the privileges by name and id.
                // we'll completely move to the id side in the future.
                match self
                    .validate_access(
                        &GrantObject::Table(
                            catalog_name.to_string(),
                            db_name.to_string(),
                            table_name.to_string(),
                        ),
                        privilege,
                    )
                    .await
                {
                    Ok(_) => return Ok(()),
                    Err(_err) => {
                        match self
                            .convert_to_id(&tenant, &catalog, db_name, Some(table_name))
                            .await
                        {
                            Ok(obj) => {
                                let (db_id, table_id) = match obj {
                                    ObjectId::Table(db_id, table_id) => (db_id, Some(table_id)),
                                    ObjectId::Database(db_id) => (db_id, None),
                                };
                                if let Err(err) = self
                                    .validate_access(
                                        &GrantObject::TableById(
                                            catalog_name.to_string(),
                                            db_id,
                                            table_id.unwrap(),
                                        ),
                                        privilege,
                                    )
                                    .await
                                {
                                    if err.code() != ErrorCode::PERMISSION_DENIED {
                                        return Err(err);
                                    }
                                    let current_user = self.ctx.get_current_user()?;
                                    let session = self.ctx.get_current_session();
                                    let roles_name = session
                                        .get_all_effective_roles()
                                        .await?
                                        .iter()
                                        .map(|r| r.name.clone())
                                        .collect::<Vec<_>>()
                                        .join(",");
                                    return Err(ErrorCode::PermissionDenied(format!(
                                        "Permission denied: privilege [{:?}] is required on '{}'.'{}'.'{}' for user {} with roles [{}]",
                                        privilege,
                                        catalog_name,
                                        db_name,
                                        table_name,
                                        &current_user.identity().display(),
                                        roles_name,
                                    )));
                                }
                            }
                            Err(e) => match e.code() {
                                ErrorCode::UNKNOWN_DATABASE
                                | ErrorCode::UNKNOWN_TABLE
                                | ErrorCode::UNKNOWN_CATALOG
                                    if if_exists =>
                                {
                                    return Ok(());
                                }

                                _ => return Err(e.add_message("error on validating table access")),
                            },
                        }
                    }
                }
            }
            Err(error) => {
                if error.code() == ErrorCode::UNKNOWN_CATALOG && if_exists {
                    return Ok(());
                } else {
                    return Err(error);
                }
            }
        }

        Ok(())
    }

    async fn has_ownership(
        &self,
        session: &Arc<Session>,
        grant_object: &GrantObject,
    ) -> Result<bool> {
        let owner_object = self
            .convert_to_owner_object(grant_object)
            .await
            .or_else(|e| match e.code() {
                ErrorCode::UNKNOWN_DATABASE
                | ErrorCode::UNKNOWN_TABLE
                | ErrorCode::UNKNOWN_CATALOG => Ok(None),
                _ => Err(e.add_message("error on validating access")),
            })?;
        if let Some(object) = &owner_object {
            if let Ok(ok) = session.has_ownership(object).await {
                if ok {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    async fn validate_access(
        &self,
        grant_object: &GrantObject,
        privilege: UserPrivilegeType,
    ) -> Result<()> {
        let session = self.ctx.get_current_session();

        let verify_ownership = match grant_object {
            GrantObject::Database(_, _)
            | GrantObject::Table(_, _, _)
            | GrantObject::DatabaseById(_, _)
            | GrantObject::UDF(_)
            | GrantObject::Stage(_)
            | GrantObject::TableById(_, _, _) => true,
            GrantObject::Global => false,
        };

        if verify_ownership && self.has_ownership(&session, grant_object).await? {
            return Ok(());
        }

        // wrap an user-facing error message with table/db names on cases like TableByID / DatabaseByID
        match session.validate_privilege(grant_object, privilege).await {
            Ok(_) => Ok(()),
            Err(err) => {
                if err.code() != ErrorCode::PERMISSION_DENIED {
                    return Err(err);
                }
                let current_user = self.ctx.get_current_user()?;

                let roles_name = session
                    .get_all_effective_roles()
                    .await?
                    .iter()
                    .map(|r| r.name.clone())
                    .collect::<Vec<_>>()
                    .join(",");
                match grant_object {
                    GrantObject::TableById(_, _, _) => Err(ErrorCode::PermissionDenied("")),
                    GrantObject::DatabaseById(_, _) => Err(ErrorCode::PermissionDenied("")),
                    GrantObject::Global
                    | GrantObject::UDF(_)
                    | GrantObject::Stage(_)
                    | GrantObject::Database(_, _)
                    | GrantObject::Table(_, _, _) => Err(ErrorCode::PermissionDenied(format!(
                        "Permission denied: privilege [{:?}] is required on {} for user {} with roles [{}]",
                        privilege,
                        grant_object,
                        &current_user.identity().display(),
                        roles_name,
                    ))),
                }
            }
        }
    }

    async fn validate_stage_access(
        &self,
        stage_info: &StageInfo,
        privilege: UserPrivilegeType,
    ) -> Result<()> {
        // this settings might be enabled as default after we got a better confidence on it
        if !self
            .ctx
            .get_settings()
            .get_enable_experimental_rbac_check()?
        {
            return Ok(());
        }

        // skip check the temp stage from uri like `COPY INTO tbl FROM 'http://xxx'`
        if stage_info.is_temporary {
            return Ok(());
        }

        // every user can presign his own user stage like: `PRESIGN @~/tmp.txt`
        if stage_info.stage_type == StageType::User
            && stage_info.stage_name == self.ctx.get_current_user()?.name
        {
            return Ok(());
        }

        self.validate_access(
            &GrantObject::Stage(stage_info.stage_name.to_string()),
            privilege,
        )
        .await
    }

    async fn validate_udf_access(&self, udf_names: HashSet<&String>) -> Result<()> {
        for udf in udf_names {
            self.validate_access(&GrantObject::UDF(udf.clone()), UserPrivilegeType::Usage)
                .await?;
        }
        Ok(())
    }

    async fn convert_to_id(
        &self,
        tenant: &Tenant,
        catalog: &Arc<dyn Catalog>,
        database_name: &str,
        table_name: Option<&str>,
    ) -> Result<ObjectId> {
        let db_id = catalog
            .get_database(tenant, database_name)
            .await?
            .get_db_info()
            .ident
            .db_id;
        if let Some(table_name) = table_name {
            let table_id = catalog
                .get_table(tenant, database_name, table_name)
                .await?
                .get_id();
            return Ok(ObjectId::Table(db_id, table_id));
        }
        Ok(ObjectId::Database(db_id))
    }
}

#[async_trait::async_trait]
impl AccessChecker for PrivilegeAccess {
    #[async_backtrace::framed]
    async fn check(&self, ctx: &Arc<QueryContext>, plan: &Plan) -> Result<()> {
        let user = self.ctx.get_current_user()?;
        let (identity, grant_set) = (user.identity().display().to_string(), user.grants);

        let enable_experimental_rbac_check = self
            .ctx
            .get_settings()
            .get_enable_experimental_rbac_check()?;
        let tenant = self.ctx.get_tenant();
        let catalog_name = self.ctx.get_current_catalog();

        match plan {
            Plan::Query {
                metadata,
                rewrite_kind,
                s_expr,
                ..
            } => {
                match rewrite_kind {
                    Some(RewriteKind::ShowDatabases)
                    | Some(RewriteKind::ShowEngines)
                    | Some(RewriteKind::ShowFunctions)
                    | Some(RewriteKind::ShowUserFunctions) => {
                        return Ok(());
                    }
                    | Some(RewriteKind::ShowTableFunctions) => {
                        return Ok(());
                    }
                    Some(RewriteKind::ShowTables(catalog, database)) => {
                        let session = self.ctx.get_current_session();
                        if self.has_ownership(&session, &GrantObject::Database(catalog.clone(), database.clone())).await? {
                            return Ok(());
                        }
                        let catalog = self.ctx.get_catalog(catalog).await?;
                        let (db_id, table_id) = match self.convert_to_id(&tenant, &catalog, database, None).await? {
                            ObjectId::Table(db_id, table_id) => { (db_id, Some(table_id)) }
                            ObjectId::Database(db_id) => { (db_id, None) }
                        };
                        let has_priv = has_priv(&tenant, database, None, db_id, table_id, grant_set).await?;
                        return if has_priv {
                            Ok(())
                        } else {
                            Err(ErrorCode::PermissionDenied(format!(
                                "Permission denied: User {} does not have the required privileges for database '{}'",
                                identity, database
                            )))
                        };
                    }
                    Some(RewriteKind::ShowStreams(database)) => {
                        let session = self.ctx.get_current_session();
                        if self.has_ownership(&session, &GrantObject::Database(catalog_name.clone(), database.clone())).await? {
                            return Ok(());
                        }
                        let catalog = self.ctx.get_catalog(&catalog_name).await?;
                        let (db_id, table_id) = match self.convert_to_id(&tenant, &catalog, database, None).await? {
                            ObjectId::Table(db_id, table_id) => { (db_id, Some(table_id)) }
                            ObjectId::Database(db_id) => { (db_id, None) }
                        };
                        let has_priv = has_priv(&tenant, database, None, db_id, table_id, grant_set).await?;
                        return if has_priv {
                            Ok(())
                        } else {
                            Err(ErrorCode::PermissionDenied(format!(
                                "Permission denied: User {} does not have the required privileges for database '{}'",
                                identity, database
                            )))
                        };
                    }
                    Some(RewriteKind::ShowColumns(catalog_name, database, table)) => {
                        let session = self.ctx.get_current_session();
                        if self.has_ownership(&session, &GrantObject::Table(catalog_name.clone(), database.clone(), table.clone())).await? {
                            return Ok(());
                        }
                        let catalog = self.ctx.get_catalog(catalog_name).await?;
                        let (db_id, table_id) = match self.convert_to_id(&tenant, &catalog, database, Some(table)).await? {
                            ObjectId::Table(db_id, table_id) => { (db_id, Some(table_id)) }
                            ObjectId::Database(db_id) => { (db_id, None) }
                        };
                        let has_priv = has_priv(&tenant, database, Some(table), db_id, table_id, grant_set).await?;
                        return if has_priv {
                            Ok(())
                        } else {
                            Err(ErrorCode::PermissionDenied(format!(
                                "Permission denied: User {} does not have the required privileges for table '{}.{}'",
                                identity, database, table
                            )))
                        };
                    }
                    _ => {}
                };
                if enable_experimental_rbac_check {
                    match s_expr.get_udfs() {
                        Ok(udfs) => {
                            if !udfs.is_empty() {
                                self.validate_udf_access(udfs).await?;
                            }
                        }
                        Err(err) => {
                            return Err(err.add_message("get udf error on validating access"));
                        }
                    }
                }

                let metadata = metadata.read().clone();

                for table in metadata.tables() {
                    if enable_experimental_rbac_check && table.is_source_of_stage() {
                        match table.table().get_data_source_info() {
                            DataSourceInfo::StageSource(stage_info) => {
                                self.validate_stage_access(&stage_info.stage_info, UserPrivilegeType::Read).await?;
                            }
                            DataSourceInfo::ParquetSource(stage_info) => {
                                self.validate_stage_access(&stage_info.stage_info, UserPrivilegeType::Read).await?;
                            }
                            DataSourceInfo::TableSource(_) | DataSourceInfo::ResultScanSource(_) => {}
                        }
                    }
                    if table.is_source_of_view() {
                        continue;
                    }
                    let catalog_name = table.catalog();
                    // like this sql: copy into t from (select * from @s3); will bind a mock table with name `system.read_parquet(s3)`
                    // this is no means to check table `system.read_parquet(s3)` privilege
                    if !table.is_source_of_stage() {
                        self.validate_table_access(catalog_name, table.database(), table.name(), UserPrivilegeType::Select, false).await?
                    }
                }
            }
            Plan::ExplainAnalyze { plan } | Plan::Explain { plan, .. } => {
                self.check(ctx, plan).await?
            }

            // Database.
            Plan::ShowCreateDatabase(plan) => {
                self.validate_db_access(&plan.catalog, &plan.database, UserPrivilegeType::Select, false).await?
            }
            Plan::CreateDatabase(_) => {
                self.validate_access(&GrantObject::Global, UserPrivilegeType::CreateDatabase)
                    .await?;
            }
            Plan::DropDatabase(plan) => {
                self.validate_db_access(&plan.catalog, &plan.database, UserPrivilegeType::Drop, plan.if_exists).await?;
            }
            Plan::UndropDatabase(_)
            | Plan::DropIndex(_)
            | Plan::DropTableIndex(_) => {
                // undroptable/db need convert name to id. But because of drop, can not find the id. Upgrade Object to Database.
                self.validate_access(&GrantObject::Global, UserPrivilegeType::Drop)
                    .await?;
            }
            Plan::DropUDF(plan) => {
                let udf_name = &plan.udf;
                if !UserApiProvider::instance().exists_udf(&tenant, udf_name).await? && plan.if_exists {
                    return Ok(());
                }
                if enable_experimental_rbac_check {
                    let udf = HashSet::from([udf_name]);
                    self.validate_udf_access(udf).await?;
                } else {
                    self.validate_access(&GrantObject::Global, UserPrivilegeType::Drop)
                                        .await?;
                }
            }
            Plan::DropStage(plan) => {
                match UserApiProvider::instance().get_stage(&tenant, &plan.name).await {
                    Ok(stage) => {
                        if enable_experimental_rbac_check {
                            let privileges = vec![UserPrivilegeType::Read, UserPrivilegeType::Write];
                            for privilege in privileges {
                                self.validate_stage_access(&stage, privilege).await?;
                            }
                        } else {
                            self.validate_access(&GrantObject::Global, UserPrivilegeType::Super)
                                .await?;
                        }
                    }
                    Err(e) => {
                        match e.code() {
                            ErrorCode::UNKNOWN_STAGE if plan.if_exists =>
                                {
                                    return Ok(());
                                }
                            _ => return Err(e.add_message("error on validating stage access")),
                        }
                    }
                }
            }
            Plan::UseDatabase(plan) => {
                let session = self.ctx.get_current_session();
                if self.has_ownership(&session, &GrantObject::Database(catalog_name.clone(), plan.database.clone())).await? {
                    return Ok(());
                }
                let catalog = self.ctx.get_catalog(&catalog_name).await?;
                // Use db is special. Should not check the privilege.
                // Just need to check user grant objects contain the db that be used.
                let (db_id, _) = match self.convert_to_id(&tenant, &catalog, &plan.database, None).await? {
                    ObjectId::Table(db_id, table_id) => { (db_id, Some(table_id)) }
                    ObjectId::Database(db_id) => { (db_id, None) }
                };
                let has_priv = has_priv(&tenant, &plan.database, None, db_id, None, grant_set).await?;

                return if has_priv {
                    Ok(())
                } else {
                    Err(ErrorCode::PermissionDenied(format!(
                        "Permission denied: User {} does not have the required privileges for database '{}'",
                        identity, plan.database.clone()
                    )))
                };
            }

            // Virtual Column.
            Plan::CreateVirtualColumn(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Create, false).await?
            }
            Plan::AlterVirtualColumn(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, plan.if_exists).await?
            }
            Plan::DropVirtualColumn(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Drop, plan.if_exists).await?
            }
            Plan::RefreshVirtualColumn(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Super, false).await?
            }

            // Table.
            Plan::ShowCreateTable(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Select, false).await?
            }
            Plan::DescribeTable(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Select, false).await?
            }
            Plan::CreateTable(plan) => {
                self.validate_db_access(&plan.catalog, &plan.database, UserPrivilegeType::Create, false).await?;
                if let Some(query) = &plan.as_select {
                    self.check(ctx, query).await?;
                }
            }
            Plan::DropTable(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Drop, plan.if_exists).await?;
            }
            Plan::UndropTable(plan) => {
                // undroptable/db need convert name to id. But because of drop, can not find the id. Upgrade Object to Database.
                self.validate_db_access(&plan.catalog, &plan.database, UserPrivilegeType::Drop, false).await?;

            }
            Plan::RenameTable(plan) => {
                // You must have ALTER and DROP privileges for the original table,
                // and CREATE for the new db.
                let privileges = vec![UserPrivilegeType::Alter, UserPrivilegeType::Drop];
                for privilege in privileges {
                    self.validate_table_access(&plan.catalog, &plan.database, &plan.table, privilege, plan.if_exists).await?;
                }
                self.validate_db_access(&plan.catalog, &plan.new_database, UserPrivilegeType::Create, false).await?;
            }
            Plan::SetOptions(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false).await?
            }
            Plan::AddTableColumn(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false).await?
            }
            Plan::RenameTableColumn(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false).await?
            }
            Plan::ModifyTableColumn(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false).await?
            }
            Plan::ModifyTableComment(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false).await?
            }
            Plan::DropTableColumn(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false).await?
            }
            Plan::AlterTableClusterKey(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false).await?
            }
            Plan::DropTableClusterKey(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Drop, false).await?
            }
            Plan::ReclusterTable(plan) => {
                if enable_experimental_rbac_check {
                    if let Some(scalar) = &plan.push_downs {
                        let udf = get_udf_names(scalar)?;
                        self.validate_udf_access(udf).await?;
                    }
                }
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Alter, false).await?
            }
            Plan::TruncateTable(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Delete, false).await?
            }
            Plan::OptimizeTable(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Super, false).await?
            }
            Plan::VacuumTable(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Super, false).await?
            }
            Plan::VacuumDropTable(plan) => {
                self.validate_db_access(&plan.catalog, &plan.database, UserPrivilegeType::Super, false).await?
            }
            Plan::VacuumTemporaryFiles(_) => {
                self.validate_access(&GrantObject::Global, UserPrivilegeType::Super).await?
            }
            Plan::AnalyzeTable(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Super, false).await?
            }
            // Others.
            Plan::Insert(plan) => {
                let target_table_privileges = if plan.overwrite {
                    vec![UserPrivilegeType::Insert, UserPrivilegeType::Delete]
                } else {
                    vec![UserPrivilegeType::Insert]
                };
                for privilege in target_table_privileges {
                    self.validate_table_access(&plan.catalog, &plan.database, &plan.table, privilege, false).await?;
                }
                match &plan.source {
                    InsertInputSource::SelectPlan(plan) => {
                        self.check(ctx, plan).await?;
                    }
                    InsertInputSource::Stage(plan) => {
                        self.check(ctx, plan).await?;
                    }
                    InsertInputSource::StreamingWithFormat(..)
                    | InsertInputSource::StreamingWithFileFormat {..}
                    | InsertInputSource::Values(_) => {}
                }
            }
            Plan::InsertMultiTable(plan) => {
                let target_table_privileges = if plan.overwrite {
                    vec![UserPrivilegeType::Insert, UserPrivilegeType::Delete]
                } else {
                    vec![UserPrivilegeType::Insert]
                };
                for target in plan.whens.iter().flat_map(|when|when.intos.iter()).chain(plan.opt_else.as_ref().into_iter().flat_map(|e|e.intos.iter())){
                    for privilege in target_table_privileges.clone() {
                    self.validate_table_access(&target.catalog, &target.database, &target.table, privilege, false).await?;
                    }
                }
                self.check(ctx, &plan.input_source).await?;
            }
            Plan::Replace(plan) => {
                //plan.delete_when is Expr no need to check privileges.
                let privileges = vec![UserPrivilegeType::Insert, UserPrivilegeType::Delete];
                for privilege in privileges {
                    self.validate_table_access(&plan.catalog, &plan.database, &plan.table, privilege, false).await?;
                }
                match &plan.source {
                    InsertInputSource::SelectPlan(plan) => {
                        self.check(ctx, plan).await?;
                    }
                    InsertInputSource::Stage(plan) => {
                        self.check(ctx, plan).await?;
                    }
                    InsertInputSource::StreamingWithFormat(..)
                    | InsertInputSource::StreamingWithFileFormat {..}
                    | InsertInputSource::Values(_) => {}
                }
            }
            Plan::MergeInto(plan) => {
                if enable_experimental_rbac_check {
                    let s_expr = &plan.input;
                    match s_expr.get_udfs() {
                        Ok(udfs) => {
                            if !udfs.is_empty() {
                                self.validate_udf_access(udfs).await?;
                            }
                        }
                        Err(err) => {
                            return Err(err.add_message("get udf error on validating access"));
                        }
                    }
                    let matched_evaluators = &plan.matched_evaluators;
                    let unmatched_evaluators = &plan.unmatched_evaluators;
                    for matched_evaluator in matched_evaluators {
                        if let Some(condition) = &matched_evaluator.condition {
                            let udf = get_udf_names(condition)?;
                            self.validate_udf_access(udf).await?;
                        }
                        if let Some(updates) = &matched_evaluator.update {
                            for scalar in updates.values() {
                                let udf = get_udf_names(scalar)?;
                                self.validate_udf_access(udf).await?;
                            }
                        }
                    }
                    for unmatched_evaluator in unmatched_evaluators {
                        if let Some(condition) = &unmatched_evaluator.condition {
                            let udf = get_udf_names(condition)?;
                            self.validate_udf_access(udf).await?;
                        }
                        for value in &unmatched_evaluator.values {
                            let udf = get_udf_names(value)?;
                            self.validate_udf_access(udf).await?;
                        }
                    }
                }
                let privileges = vec![UserPrivilegeType::Insert, UserPrivilegeType::Delete];
                for privilege in privileges {
                    self.validate_table_access(&plan.catalog, &plan.database, &plan.table, privilege, false).await?;
                }
            }
            Plan::Delete(plan) => {
                if enable_experimental_rbac_check {
                    if let Some(selection) = &plan.selection {
                        let udf = get_udf_names(selection)?;
                        self.validate_udf_access(udf).await?;
                    }
                    for subquery in &plan.subquery_desc {
                        match subquery.input_expr.get_udfs() {
                            Ok(udfs) => {
                                if !udfs.is_empty() {
                                    self.validate_udf_access(udfs).await?;
                                }
                            }
                            Err(err) => {
                                return Err(err.add_message("Unable to access necessary user-defined functions for executing the DELETE operation"));
                            }
                        }
                    }
                }
                self.validate_table_access(&plan.catalog_name, &plan.database_name, &plan.table_name, UserPrivilegeType::Delete, false).await?
            }
            Plan::Update(plan) => {
                if enable_experimental_rbac_check {
                    for scalar in plan.update_list.values() {
                        let udf = get_udf_names(scalar)?;
                        self.validate_udf_access(udf).await?;
                    }
                    if let Some(selection) = &plan.selection {
                        let udf = get_udf_names(selection)?;
                        self.validate_udf_access(udf).await?;
                    }
                    for subquery in &plan.subquery_desc {
                        match subquery.input_expr.get_udfs() {
                            Ok(udfs) => {
                                if !udfs.is_empty() {
                                    self.validate_udf_access(udfs).await?;
                                }
                            }
                            Err(err) => {
                                return Err(err.add_message("Failed to retrieve necessary user-defined functions for executing the UPDATE operation."));
                            }
                        }
                    }
                }
                self.validate_table_access(&plan.catalog, &plan.database, &plan.table, UserPrivilegeType::Update, false).await?;
            }
            Plan::CreateView(plan) => {
                let mut planner = Planner::new(self.ctx.clone());
                let (plan, _) = planner.plan_sql(&plan.subquery).await?;
                self.check(ctx, &plan).await?
            }
            Plan::AlterView(plan) => {
                self.validate_db_access(&plan.catalog, &plan.database, UserPrivilegeType::Alter, false).await?;
                let mut planner = Planner::new(self.ctx.clone());
                let (plan, _) = planner.plan_sql(&plan.subquery).await?;
                self.check(ctx, &plan).await?
            }
            Plan::DropView(plan) => {
                self.validate_db_access(&plan.catalog, &plan.database, UserPrivilegeType::Drop, plan.if_exists).await?
            }
            Plan::DescribeView(plan) => {
                self.validate_table_access(&plan.catalog, &plan.database, &plan.view_name, UserPrivilegeType::Select, false).await?
            }
            Plan::CreateStream(plan) => {
                self.validate_db_access(&plan.catalog, &plan.database, UserPrivilegeType::Create, false).await?
            }
            Plan::DropStream(plan) => {
                self.validate_db_access(&plan.catalog, &plan.database, UserPrivilegeType::Drop, plan.if_exists).await?
            }
            Plan::CreateDynamicTable(plan) => {
                self.validate_db_access(&plan.catalog, &plan.database, UserPrivilegeType::Create, false).await?;
            }
            Plan::CreateUser(_) => {
                self.validate_access(
                    &GrantObject::Global,
                    UserPrivilegeType::CreateUser,
                )
                    .await?;
            }
            Plan::DropUser(_) => {
                self.validate_access(
                    &GrantObject::Global,
                    UserPrivilegeType::DropUser,
                )
                    .await?;
            }
            Plan::CreateRole(_) => {
                self.validate_access(
                    &GrantObject::Global,
                    UserPrivilegeType::CreateRole,
                )
                    .await?;
            }
            Plan::DropRole(_) => {
                self.validate_access(
                    &GrantObject::Global,
                    UserPrivilegeType::DropRole,
                )
                    .await?;
            }
            | Plan::GrantRole(_)
            | Plan::GrantPriv(_)
            | Plan::RevokePriv(_)
            | Plan::RevokeRole(_) => {
                self.validate_access(&GrantObject::Global, UserPrivilegeType::Grant)
                    .await?;
            }
            Plan::SetVariable(_) | Plan::UnSetVariable(_) | Plan::Kill(_) | Plan::SetPriority(_) => {
                self.validate_access(&GrantObject::Global, UserPrivilegeType::Super)
                    .await?;
            }
            Plan::ShowGrants(plan) => {
                let current_user = self.ctx.get_current_user()?;
                if let Some(principal) = &plan.principal {
                   match principal {
                       PrincipalIdentity::User(user) => {
                           if current_user.identity() == *user {
                               return Ok(());
                           } else {
                               self.validate_access(&GrantObject::Global, UserPrivilegeType::Grant)
                                   .await?;
                           }
                       }
                       PrincipalIdentity::Role(role) => {
                           let roles=current_user.grants.roles();
                           if roles.contains(role) || role.to_lowercase() == "public" {
                               return Ok(());
                           } else {
                               self.validate_access(&GrantObject::Global, UserPrivilegeType::Grant)
                                   .await?;
                           }
                       }
                   }
                } else {
                    return Ok(());
                }
            }
            Plan::AlterUser(_)
            | Plan::RenameDatabase(_)
            | Plan::RevertTable(_)
            | Plan::AlterUDF(_)
            | Plan::RefreshIndex(_)
            | Plan::RefreshTableIndex(_) => {
                self.validate_access(&GrantObject::Global, UserPrivilegeType::Alter)
                    .await?;
            }
            Plan::CopyIntoTable(plan) => {
                self.validate_stage_access(&plan.stage_table_info.stage_info, UserPrivilegeType::Read).await?;
                self.validate_table_access(plan.catalog_info.catalog_name(), &plan.database_name, &plan.table_name, UserPrivilegeType::Insert, false).await?;
                if let Some(query) = &plan.query {
                    self.check(ctx, query).await?;
                }
            }
            Plan::CopyIntoLocation(plan) => {
                self.validate_stage_access(&plan.stage, UserPrivilegeType::Write).await?;
                let from = plan.from.clone();
                return self.check(ctx, &from).await;
            }
            Plan::RemoveStage(plan) => {
                self.validate_stage_access(&plan.stage, UserPrivilegeType::Write).await?;
            }
            | Plan::ShowCreateCatalog(_)
            | Plan::CreateCatalog(_)
            | Plan::DropCatalog(_)
            | Plan::CreateStage(_)
            | Plan::CreateFileFormat(_)
            | Plan::DropFileFormat(_)
            | Plan::ShowFileFormats(_)
            | Plan::CreateNetworkPolicy(_)
            | Plan::AlterNetworkPolicy(_)
            | Plan::DropNetworkPolicy(_)
            | Plan::DescNetworkPolicy(_)
            | Plan::ShowNetworkPolicies(_)
            | Plan::CreatePasswordPolicy(_)
            | Plan::AlterPasswordPolicy(_)
            | Plan::DropPasswordPolicy(_)
            | Plan::DescPasswordPolicy(_)
            | Plan::CreateConnection(_)
            | Plan::ShowConnections(_)
            | Plan::DescConnection(_)
            | Plan::DropConnection(_)
            | Plan::CreateUDF(_)
            | Plan::CreateIndex(_)
            | Plan::CreateTableIndex(_)
            | Plan::CreateNotification(_)
            | Plan::DropNotification(_)
            | Plan::DescNotification(_)
            | Plan::AlterNotification(_)
            | Plan::CreateTask(_)   // TODO: need to build ownership info for task
            | Plan::ShowTasks(_)    // TODO: need to build ownership info for task
            | Plan::DescribeTask(_) // TODO: need to build ownership info for task
            | Plan::ExecuteTask(_)  // TODO: need to build ownership info for task
            | Plan::DropTask(_)     // TODO: need to build ownership info for task
            | Plan::AlterTask(_)
            | Plan::CreateSequence(_)
            | Plan::DropSequence(_) => {
                self.validate_access(&GrantObject::Global, UserPrivilegeType::Super)
                    .await?;
            }
            Plan::CreateDatamaskPolicy(_) | Plan::DropDatamaskPolicy(_) => {
                self.validate_access(
                    &GrantObject::Global,
                    UserPrivilegeType::CreateDataMask,
                )
                    .await?;
            }
            // Note: No need to check privileges
            // SET ROLE & SHOW ROLES is a session-local statement (have same semantic with the SET ROLE in postgres), no need to check privileges
            Plan::SetRole(_) => {}
            Plan::SetSecondaryRoles(_) => {}
            Plan::ShowRoles(_) => {}
            Plan::Presign(plan) => {
                let privilege = match &plan.action {
                    PresignAction::Upload => UserPrivilegeType::Write,
                    PresignAction::Download => UserPrivilegeType::Read,
                };
                self.validate_stage_access(&plan.stage, privilege).await?;
            }
            Plan::ExplainAst { .. } => {}
            Plan::ExplainSyntax { .. } => {}
            // just used in clickhouse-sqlalchemy, no need to check
            Plan::ExistsTable(_) => {}
            Plan::DescDatamaskPolicy(_) => {}
            Plan::Begin => {}
            Plan::Commit => {}
            Plan::Abort => {}
            Plan::ExecuteImmediate(_) => {}
        }

        Ok(())
    }
}

// TODO(liyz): replace it with verify_access
async fn has_priv(
    tenant: &Tenant,
    db_name: &str,
    table_name: Option<&str>,
    db_id: u64,
    table_id: Option<u64>,
    grant_set: UserGrantSet,
) -> Result<bool> {
    if db_name.to_lowercase() == "information_schema" {
        return Ok(true);
    }

    Ok(RoleCacheManager::instance()
        .find_related_roles(tenant, &grant_set.roles())
        .await?
        .into_iter()
        .map(|role| role.grants)
        .fold(grant_set, |a, b| a | b)
        .entries()
        .iter()
        .any(|e| {
            let object = e.object();
            match object {
                GrantObject::Global => {
                    if db_name.to_lowercase() == "system" {
                        return true;
                    }
                    e.privileges().iter().any(|privilege| {
                        UserPrivilegeSet::available_privileges_on_database(false)
                            .has_privilege(privilege)
                    })
                }
                GrantObject::Database(_, ldb) => *ldb == db_name,
                GrantObject::DatabaseById(_, ldb) => *ldb == db_id,
                GrantObject::Table(_, ldb, ltab) => {
                    if let Some(table) = table_name {
                        *ldb == db_name && *ltab == table
                    } else {
                        *ldb == db_name
                    }
                }
                GrantObject::TableById(_, ldb, ltab) => {
                    if let Some(table) = table_id {
                        *ldb == db_id && *ltab == table
                    } else {
                        *ldb == db_id
                    }
                }
                _ => false,
            }
        }))
}
