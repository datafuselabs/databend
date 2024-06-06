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
use std::sync::Arc;

use databend_common_ast::ast::FileLocation;
use databend_common_ast::ast::UriLocation;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::TableSchema;
use databend_common_meta_app::principal::StageFileFormatType;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;
use databend_common_sql::binder::resolve_file_location;
use databend_common_storage::init_stage_operator;
use databend_common_storage::read_parquet_schema_async_rs;
use databend_common_storage::StageFilesInfo;
use opendal::Scheme;

use crate::table_functions::infer_schema::infer_schema_table::INFER_SCHEMA;
use crate::table_functions::infer_schema::table_args::InferSchemaArgsParsed;

pub(crate) struct ParquetInferSchemaSource {
    is_finished: bool,
    ctx: Arc<dyn TableContext>,
    args_parsed: InferSchemaArgsParsed,
}

impl ParquetInferSchemaSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        args_parsed: InferSchemaArgsParsed,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(ctx.clone(), output, ParquetInferSchemaSource {
            is_finished: false,
            ctx,
            args_parsed,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for ParquetInferSchemaSource {
    const NAME: &'static str = INFER_SCHEMA;

    #[async_trait::unboxed_simple]
    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.is_finished {
            return Ok(None);
        }
        self.is_finished = true;

        let file_location = if let Some(location) =
            self.args_parsed.location.clone().strip_prefix('@')
        {
            FileLocation::Stage(location.to_string())
        } else if let Some(connection_name) = &self.args_parsed.connection_name {
            let conn = self.ctx.get_connection(connection_name).await?;
            let uri = UriLocation::from_uri(
                self.args_parsed.location.clone(),
                "".to_string(),
                conn.storage_params,
            )?;
            let proto = conn.storage_type.parse::<Scheme>()?;
            if proto != uri.protocol.parse::<Scheme>()? {
                return Err(ErrorCode::BadArguments(format!(
                    "protocol from connection_name={connection_name} ({proto}) not match with uri protocol ({0}).",
                    uri.protocol
                )));
            }
            FileLocation::Uri(uri)
        } else {
            let uri = UriLocation::from_uri(
                self.args_parsed.location.clone(),
                "".to_string(),
                BTreeMap::default(),
            )?;
            FileLocation::Uri(uri)
        };
        let (stage_info, path) = resolve_file_location(self.ctx.as_ref(), &file_location).await?;
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
                    &self.ctx.get_current_user()?.identity().identity(),
                )));
            }
        }
        let files_info = StageFilesInfo {
            path: path.clone(),
            ..self.args_parsed.files_info.clone()
        };
        let operator = init_stage_operator(&stage_info)?;

        let first_file = files_info.first_file(&operator).await?;
        let file_format_params = match &self.args_parsed.file_format {
            Some(f) => self.ctx.get_file_format(f).await?,
            None => stage_info.file_format_params.clone(),
        };
        let schema = match file_format_params.get_type() {
            StageFileFormatType::Parquet => {
                let arrow_schema = read_parquet_schema_async_rs(
                    &operator,
                    &first_file.path,
                    Some(first_file.size),
                )
                .await?;
                TableSchema::try_from(&arrow_schema)?
            }
            _ => {
                return Err(ErrorCode::BadArguments(
                    "infer_schema is currently limited to format Parquet",
                ));
            }
        };

        let mut names: Vec<String> = vec![];
        let mut types: Vec<String> = vec![];
        let mut nulls: Vec<bool> = vec![];

        for field in schema.fields().iter() {
            names.push(field.name().to_string());

            let non_null_type = field.data_type().remove_recursive_nullable();
            types.push(non_null_type.sql_name());
            nulls.push(field.is_nullable());
        }

        let order_ids = (0..schema.fields().len() as u64).collect::<Vec<_>>();

        let block = DataBlock::new_from_columns(vec![
            StringType::from_data(names),
            StringType::from_data(types),
            BooleanType::from_data(nulls),
            UInt64Type::from_data(order_ids),
        ]);
        Ok(Some(block))
    }
}
