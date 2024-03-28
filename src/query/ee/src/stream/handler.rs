// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_common_catalog::table::Table;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::CreateTableReply;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::DropTableByIdReq;
use databend_common_meta_app::schema::DropTableReply;
use databend_common_meta_app::schema::TableMeta;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_meta_types::MatchSeq;
use databend_common_sql::plans::CreateStreamPlan;
use databend_common_sql::plans::DropStreamPlan;
use databend_common_sql::plans::StreamNavigation;
use databend_common_storages_fuse::io::SnapshotsIO;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::TableContext;
use databend_common_storages_stream::stream_table::StreamTable;
use databend_common_storages_stream::stream_table::STREAM_ENGINE;
use databend_enterprise_stream_handler::StreamHandler;
use databend_enterprise_stream_handler::StreamHandlerWrapper;
use databend_storages_common_table_meta::table::MODE_APPEND_ONLY;
use databend_storages_common_table_meta::table::MODE_STANDARD;
use databend_storages_common_table_meta::table::OPT_KEY_CHANGE_TRACKING;
use databend_storages_common_table_meta::table::OPT_KEY_CHANGE_TRACKING_BEGIN_VER;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_NAME;
use databend_storages_common_table_meta::table::OPT_KEY_MODE;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use databend_storages_common_table_meta::table::OPT_KEY_TABLE_ID;
use databend_storages_common_table_meta::table::OPT_KEY_TABLE_NAME;
use databend_storages_common_table_meta::table::OPT_KEY_TABLE_VER;

pub struct RealStreamHandler {}

#[async_trait::async_trait]
impl StreamHandler for RealStreamHandler {
    #[async_backtrace::framed]
    async fn do_create_stream(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &CreateStreamPlan,
    ) -> Result<CreateTableReply> {
        let tenant = ctx.get_tenant();
        let catalog = ctx.get_catalog(&plan.catalog).await?;

        let mut table = catalog
            .get_table(tenant.name(), &plan.table_database, &plan.table_name)
            .await?;
        let table_info = table.get_table_info();
        if table_info.options().contains_key("TRANSIENT") {
            return Err(ErrorCode::IllegalStream(format!(
                "The table '{}.{}' is transient, can't create stream",
                plan.table_database, plan.table_name
            )));
        }

        let mut table_version = table_info.ident.seq;
        let table_id = table_info.ident.table_id;
        let schema = table_info.schema().clone();
        if !table.change_tracking_enabled() {
            // enable change tracking.
            let req = UpsertTableOptionReq {
                table_id,
                seq: MatchSeq::Exact(table_version),
                options: HashMap::from([
                    (
                        OPT_KEY_CHANGE_TRACKING.to_string(),
                        Some("true".to_string()),
                    ),
                    (
                        OPT_KEY_CHANGE_TRACKING_BEGIN_VER.to_string(),
                        Some(table_version.to_string()),
                    ),
                ]),
            };

            catalog
                .upsert_table_option(tenant.name(), &plan.table_database, req)
                .await?;
            // refreash table.
            table = table.refresh(ctx.as_ref()).await?;
            table_version = table.get_table_info().ident.seq;
        }

        let (version, snapshot_location) = match &plan.navigation {
            Some(StreamNavigation::AtStream { database, name }) => {
                let stream = catalog.get_table(tenant.name(), database, name).await?;
                let stream = StreamTable::try_from_table(stream.as_ref())?;
                let stream_opts = stream.get_table_info().options();
                let stream_table_name = stream_opts
                    .get(OPT_KEY_TABLE_NAME)
                    .ok_or_else(|| ErrorCode::IllegalStream(format!("Illegal stream '{name}'")))?;
                let stream_database_name = stream_opts
                    .get(OPT_KEY_DATABASE_NAME)
                    .ok_or_else(|| ErrorCode::IllegalStream(format!("Illegal stream '{name}'")))?;
                let stream_table_id = stream_opts
                    .get(OPT_KEY_TABLE_ID)
                    .ok_or_else(|| ErrorCode::IllegalStream(format!("Illegal stream '{name}'")))?
                    .parse::<u64>()?;
                if stream_table_name != &plan.table_name
                    || stream_database_name != &plan.table_database
                    || stream_table_id != table_id
                {
                    return Err(ErrorCode::IllegalStream(format!(
                        "The stream '{name}' is not match the table '{}.{}'",
                        plan.table_database, plan.table_name
                    )));
                }

                let version = stream_opts
                    .get(OPT_KEY_TABLE_VER)
                    .ok_or_else(|| ErrorCode::IllegalStream(format!("Illegal stream '{name}'")))?
                    .parse::<u64>()?;
                (version, stream_opts.get(OPT_KEY_SNAPSHOT_LOCATION).cloned())
            }
            Some(StreamNavigation::AtPoint(point)) => {
                let fuse_table = FuseTable::try_from_table(table.as_ref())?;
                let source = fuse_table.navigate_to(point).await?;
                if let Some(snapshot_loc) = source.snapshot_loc().await? {
                    let (snapshot, _) =
                        SnapshotsIO::read_snapshot(snapshot_loc.clone(), fuse_table.get_operator())
                            .await?;
                    let Some(version) = snapshot.prev_table_seq else {
                        return Err(ErrorCode::IllegalStream(
                            "The stream navigation at point has not table version".to_string(),
                        ));
                    };

                    // The table version is the version of the table when the snapshot was created.
                    // We need make sure the version greater than the table version,
                    // and less equal than the table version after the snapshot commit.
                    (version + 1, Some(snapshot_loc))
                } else {
                    unreachable!()
                }
            }
            None => {
                let fuse_table = FuseTable::try_from_table(table.as_ref())?;
                (table_version, fuse_table.snapshot_loc().await?)
            }
        };

        if let Some(value) = table
            .get_table_info()
            .options()
            .get(OPT_KEY_CHANGE_TRACKING_BEGIN_VER)
        {
            let begin_version = value.parse::<u64>()?;
            if begin_version > version {
                return Err(ErrorCode::IllegalStream(format!(
                    "Change tracking has been missing for the time range requested on table '{}.{}'",
                    plan.table_database, plan.table_name
                )));
            }
        }
        let mut options = BTreeMap::new();
        let stream_mode = if plan.append_only {
            MODE_APPEND_ONLY
        } else {
            MODE_STANDARD
        };
        options.insert(OPT_KEY_MODE.to_string(), stream_mode.to_string());
        options.insert(OPT_KEY_TABLE_NAME.to_string(), plan.table_name.clone());
        options.insert(
            OPT_KEY_DATABASE_NAME.to_string(),
            plan.table_database.clone(),
        );
        options.insert(OPT_KEY_TABLE_ID.to_string(), table_id.to_string());
        options.insert(OPT_KEY_TABLE_VER.to_string(), version.to_string());
        if let Some(snapshot_loc) = snapshot_location {
            options.insert(OPT_KEY_SNAPSHOT_LOCATION.to_string(), snapshot_loc);
        }

        let req = CreateTableReq {
            create_option: plan.create_option,
            name_ident: TableNameIdent {
                tenant: plan.tenant.clone(),
                db_name: plan.database.clone(),
                table_name: plan.stream_name.clone(),
            },
            table_meta: TableMeta {
                engine: STREAM_ENGINE.to_string(),
                options,
                comment: plan.comment.clone().unwrap_or("".to_string()),
                schema,
                ..Default::default()
            },
        };

        catalog.create_table(req).await
    }

    #[async_backtrace::framed]
    async fn do_drop_stream(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DropStreamPlan,
    ) -> Result<DropTableReply> {
        let catalog_name = plan.catalog.clone();
        let db_name = plan.database.clone();
        let stream_name = plan.stream_name.clone();
        let catalog = ctx.get_catalog(&plan.catalog).await?;
        let tenant = ctx.get_tenant();
        let tbl = catalog
            .get_table(tenant.name(), &db_name, &stream_name)
            .await
            .ok();

        if let Some(table) = &tbl {
            let engine = table.get_table_info().engine();
            if engine != STREAM_ENGINE {
                return Err(ErrorCode::TableEngineNotSupported(format!(
                    "{}.{} is not STREAM, please use `DROP {} {}.{}`",
                    &plan.database,
                    &plan.stream_name,
                    if engine == "VIEW" { "VIEW" } else { "TABLE" },
                    &plan.database,
                    &plan.stream_name
                )));
            }

            let db = catalog.get_database(tenant.name(), &db_name).await?;

            catalog
                .drop_table_by_id(DropTableByIdReq {
                    if_exists: plan.if_exists,
                    tenant,
                    table_name: stream_name.clone(),
                    tb_id: table.get_id(),
                    db_id: db.get_db_info().ident.db_id,
                })
                .await
        } else if plan.if_exists {
            Ok(DropTableReply { spec_vec: None })
        } else {
            Err(ErrorCode::UnknownStream(format!(
                "unknown stream `{}`.`{}` in catalog '{}'",
                db_name, stream_name, &catalog_name
            )))
        }
    }
}

impl RealStreamHandler {
    pub fn init() -> Result<()> {
        let rm = RealStreamHandler {};
        let wrapper = StreamHandlerWrapper::new(Box::new(rm));
        GlobalInstance::set(Arc::new(wrapper));
        Ok(())
    }
}
