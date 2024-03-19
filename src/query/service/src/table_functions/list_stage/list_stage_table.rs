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

use std::any::Any;
use std::sync::Arc;

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_context::TableContext;
use databend_common_catalog::table_function::TableFunction;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;
use databend_common_sql::binder::resolve_stage_location;
use databend_common_storage::StageFilesInfo;
use databend_common_storages_stage::StageTable;

use crate::table_functions::list_stage::table_args::ListStageArgsParsed;

const LIST_STAGE: &str = "list_stage";

pub struct ListStageTable {
    args_parsed: ListStageArgsParsed,
    table_args: TableArgs,
    table_info: TableInfo,
}

impl ListStageTable {
    pub fn create(
        database_name: &str,
        table_func_name: &str,
        table_id: u64,
        table_args: TableArgs,
    ) -> Result<Arc<dyn TableFunction>> {
        let args_parsed = ListStageArgsParsed::parse(&table_args)?;
        let table_info = TableInfo {
            ident: TableIdent::new(table_id, 0),
            desc: format!("'{}'.'{}'", database_name, table_func_name),
            name: table_func_name.to_string(),
            meta: TableMeta {
                schema: Self::schema(),
                engine: LIST_STAGE.to_owned(),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(Arc::new(Self {
            table_info,
            args_parsed,
            table_args,
        }))
    }

    fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("name", TableDataType::String),
            TableField::new("size", TableDataType::Number(NumberDataType::UInt64)),
            TableField::new(
                "md5",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new("last_modified", TableDataType::String),
            TableField::new(
                "creator",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
        ])
    }
}

#[async_trait::async_trait]
impl Table for ListStageTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        Ok((PartStatistics::default(), Partitions::default()))
    }

    fn table_args(&self) -> Option<TableArgs> {
        Some(self.table_args.clone())
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        pipeline.add_source(
            |output| ListStagesSource::create(ctx.clone(), output, self.args_parsed.clone()),
            1,
        )?;
        Ok(())
    }
}

impl TableFunction for ListStageTable {
    fn function_name(&self) -> &str {
        self.name()
    }

    fn as_table<'a>(self: Arc<Self>) -> Arc<dyn Table + 'a>
    where Self: 'a {
        self
    }
}

struct ListStagesSource {
    is_finished: bool,
    ctx: Arc<dyn TableContext>,
    args_parsed: ListStageArgsParsed,
}

impl ListStagesSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        args_parsed: ListStageArgsParsed,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.clone(), output, ListStagesSource {
            is_finished: false,
            ctx,
            args_parsed,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for ListStagesSource {
    const NAME: &'static str = LIST_STAGE;

    #[async_trait::unboxed_simple]
    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.is_finished {
            return Ok(None);
        }

        self.is_finished = true;

        let (stage_info, path) =
            resolve_stage_location(self.ctx.as_ref(), &self.args_parsed.location).await?;
        let enable_experimental_rbac_check = self
            .ctx
            .get_settings()
            .get_enable_experimental_rbac_check()?;
        if enable_experimental_rbac_check {
            let visibility_checker = self.ctx.get_visibility_checker().await?;
            if !stage_info.is_temporary
                && !visibility_checker.check_stage_read_visibility(&stage_info.stage_name)
            {
                return Err(ErrorCode::PermissionDenied(format!(
                    "Permission denied: privilege READ is required on stage {} for user {}",
                    stage_info.stage_name.clone(),
                    &self.ctx.get_current_user()?.identity(),
                )));
            }
        }
        let op = StageTable::get_op(&stage_info)?;

        let files_info = StageFilesInfo {
            path,
            files: self.args_parsed.files_info.files.clone(),
            pattern: self.args_parsed.files_info.pattern.clone(),
            start_after: self.args_parsed.files_info.start_after.clone(),
        };

        let files = files_info.list(&op, false, None).await?;

        let names: Vec<String> = files.iter().map(|file| file.path.to_string()).collect();

        let sizes: Vec<u64> = files.iter().map(|file| file.size).collect();
        let etags: Vec<Option<String>> = files
            .iter()
            .map(|file| file.etag.as_ref().map(|f| f.to_string()))
            .collect();
        let last_modifieds: Vec<String> = files
            .iter()
            .map(|file| {
                file.last_modified
                    .format("%Y-%m-%d %H:%M:%S.%3f %z")
                    .to_string()
            })
            .collect();
        let creators: Vec<Option<String>> = files
            .iter()
            .map(|file| file.creator.as_ref().map(|c| c.to_string()))
            .collect();

        let block = DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            UInt64Type::from_data(sizes),
            StringType::from_opt_data(etags),
            StringType::from_data(last_modifieds),
            StringType::from_opt_data(creators),
        ]);

        Ok(Some(block))
    }
}
