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

use std::marker::PhantomData;
use std::sync::Arc;

use databend_common_catalog::catalog_kind::CATALOG_DEFAULT;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_storages_common_table_meta::meta::TableSnapshot;
use futures_util::TryStreamExt;

use crate::io::MetaReaders;
use crate::io::SnapshotHistoryReader;
use crate::table_functions::parse_db_tb_opt_args;
use crate::table_functions::string_literal;
use crate::table_functions::SimpleArgFunc;
use crate::table_functions::SimpleArgFuncTemplate;
use crate::FuseTable;

pub struct TableMetaFuncCommonArgs {
    pub database_name: String,
    pub table_name: String,
    pub snapshot_id: Option<String>,
}

impl From<&TableMetaFuncCommonArgs> for TableArgs {
    fn from(args: &TableMetaFuncCommonArgs) -> Self {
        let mut table_args = Vec::new();
        table_args.push(string_literal(args.database_name.as_str()));
        table_args.push(string_literal(args.table_name.as_str()));
        if let Some(arg_snapshot_id) = &args.snapshot_id {
            table_args.push(string_literal(arg_snapshot_id));
        }
        TableArgs::new_positioned(table_args)
    }
}

impl TryFrom<(&str, TableArgs)> for TableMetaFuncCommonArgs {
    type Error = ErrorCode;

    fn try_from(
        (func_name, table_args): (&str, TableArgs),
    ) -> std::result::Result<Self, Self::Error> {
        let (database_name, table_name, snapshot_id) =
            parse_db_tb_opt_args(&table_args, func_name)?;
        Ok(Self {
            database_name,
            table_name,
            snapshot_id,
        })
    }
}

async fn location_snapshot(
    tbl: &FuseTable,
    args: &TableMetaFuncCommonArgs,
) -> Result<Option<Arc<TableSnapshot>>> {
    let snapshot_id = args.snapshot_id.clone();
    let maybe_snapshot = tbl.read_table_snapshot().await?;
    if let Some(snapshot) = maybe_snapshot {
        if let Some(snapshot_id) = snapshot_id {
            // prepare the stream of snapshot
            let snapshot_version = tbl.snapshot_format_version(None).await?;
            let snapshot_location = tbl
                .meta_location_generator
                .snapshot_location_from_uuid(&snapshot.snapshot_id, snapshot_version)?;
            let reader = MetaReaders::table_snapshot_reader(tbl.get_operator());
            let mut snapshot_stream = reader.snapshot_history(
                snapshot_location,
                snapshot_version,
                tbl.meta_location_generator().clone(),
            );

            // find the element by snapshot_id in stream
            while let Some((snapshot, _)) = snapshot_stream.try_next().await? {
                if snapshot.snapshot_id.simple().to_string() == snapshot_id {
                    return Ok(Some(snapshot));
                }
            }
        } else {
            return Ok(Some(snapshot));
        }
    }

    Ok(None)
}

#[async_trait::async_trait]
pub trait TableMetaFunc {
    fn schema() -> TableSchemaRef;
    async fn apply(
        ctx: &Arc<dyn TableContext>,
        tbl: &FuseTable,
        snapshot: Arc<TableSnapshot>,
        limit: Option<usize>,
    ) -> Result<DataBlock>;
}

pub struct SimpleTableMetaFunc<T> {
    _marker: PhantomData<T>,
}

#[async_trait::async_trait]
impl<T> SimpleArgFunc for SimpleTableMetaFunc<T>
where
    T: TableMetaFunc + Send + Sync + 'static + Sized,
    Self: Sized,
{
    type Args = TableMetaFuncCommonArgs;

    fn schema() -> TableSchemaRef {
        T::schema()
    }

    async fn apply(
        ctx: &Arc<dyn TableContext>,
        args: &Self::Args,
        plan: &DataSourcePlan,
    ) -> Result<DataBlock> {
        let tenant_id = ctx.get_tenant();
        let tbl = ctx
            .get_catalog(CATALOG_DEFAULT)
            .await?
            .get_table(
                &tenant_id,
                args.database_name.as_str(),
                args.table_name.as_str(),
            )
            .await?;
        let limit = plan.push_downs.as_ref().and_then(|x| x.limit);
        let tbl = FuseTable::try_from_table(tbl.as_ref())?;
        if let Some(snapshot) = location_snapshot(tbl, args).await? {
            return T::apply(ctx, tbl, snapshot, limit).await;
        } else {
            Ok(DataBlock::empty_with_schema(Arc::new(T::schema().into())))
        }
    }
}

pub type TableMetaFuncTemplate<T> = SimpleArgFuncTemplate<SimpleTableMetaFunc<T>>;
