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

use std::sync::Arc;
use std::vec;

use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::number::UInt64Type;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::utils::FromData;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::principal::OwnershipObject;
use databend_common_meta_app::principal::StageType;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_users::UserApiProvider;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

pub struct StagesTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for StagesTable {
    const NAME: &'static str = "system.stages";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        let tenant = ctx.get_tenant();

        let stages = UserApiProvider::instance().get_stages(&tenant).await?;
        let enable_experimental_rbac_check =
            ctx.get_settings().get_enable_experimental_rbac_check()?;
        let stages = if enable_experimental_rbac_check {
            let visibility_checker = ctx.get_visibility_checker().await?;
            stages
                .into_iter()
                .filter(|stage| {
                    !stage.is_temporary
                        && visibility_checker.check_stage_visibility(&stage.stage_name)
                })
                .collect::<Vec<_>>()
        } else {
            stages
        };

        let user_api = UserApiProvider::instance();
        let mut owners: Vec<Option<String>> = vec![];
        let mut name: Vec<String> = Vec::with_capacity(stages.len());
        let mut stage_type: Vec<String> = Vec::with_capacity(stages.len());
        let mut stage_params: Vec<String> = Vec::with_capacity(stages.len());
        let mut copy_options: Vec<String> = Vec::with_capacity(stages.len());
        let mut file_format_options: Vec<String> = Vec::with_capacity(stages.len());
        let mut comment: Vec<String> = Vec::with_capacity(stages.len());
        let mut number_of_files: Vec<Option<u64>> = Vec::with_capacity(stages.len());
        let mut creator: Vec<Option<String>> = Vec::with_capacity(stages.len());
        let mut created_on = Vec::with_capacity(stages.len());
        for stage in stages.into_iter() {
            let stage_name = stage.stage_name;
            name.push(stage_name.clone());
            owners.push(
                user_api
                    .get_ownership(&tenant, &OwnershipObject::Stage { name: stage_name })
                    .await
                    .ok()
                    .and_then(|ownership| ownership.map(|o| o.role.clone())),
            );
            stage_type.push(stage.stage_type.clone().to_string());
            stage_params.push(format!("{:?}", stage.stage_params));
            copy_options.push(format!("{:?}", stage.copy_options));
            file_format_options.push(format!("{:?}", stage.file_format_params));
            // TODO(xuanwo): we will remove this line.
            match stage.stage_type {
                StageType::LegacyInternal | StageType::Internal | StageType::User => {
                    number_of_files.push(Some(stage.number_of_files));
                }
                StageType::External => {
                    number_of_files.push(None);
                }
            };
            creator.push(stage.creator.map(|c| c.identity().to_string()));
            created_on.push(stage.created_on.timestamp_micros());
            comment.push(stage.comment.clone());
        }

        Ok(DataBlock::new_from_columns(vec![
            StringType::from_data(name),
            StringType::from_data(stage_type),
            StringType::from_data(stage_params),
            StringType::from_data(copy_options),
            StringType::from_data(file_format_options),
            UInt64Type::from_opt_data(number_of_files),
            StringType::from_opt_data(creator),
            TimestampType::from_data(created_on),
            StringType::from_data(comment),
            StringType::from_opt_data(owners),
        ]))
    }
}

impl StagesTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("name", TableDataType::String),
            TableField::new("stage_type", TableDataType::String),
            TableField::new("stage_params", TableDataType::String),
            TableField::new("copy_options", TableDataType::String),
            TableField::new("file_format_options", TableDataType::String),
            // NULL for external stage
            TableField::new(
                "number_of_files",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt64))),
            ),
            TableField::new(
                "creator",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
            TableField::new("created_on", TableDataType::Timestamp),
            TableField::new("comment", TableDataType::String),
            TableField::new(
                "owner",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
        ]);
        let table_info = TableInfo {
            desc: "'system'.'stages'".to_string(),
            name: "stages".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemStages".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(StagesTable { table_info })
    }
}
