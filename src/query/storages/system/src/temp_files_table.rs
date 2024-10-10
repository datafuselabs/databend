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

use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::Value;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline_core::query_spill_prefix;
use databend_common_storage::DataOperator;
use futures::StreamExt;
use futures::TryStreamExt;
use opendal::Metakey;

use crate::table::AsyncOneBlockSystemTable;
use crate::table::AsyncSystemTable;

pub struct TempFilesTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl AsyncSystemTable for TempFilesTable {
    const NAME: &'static str = "system.temp_files";

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    #[async_backtrace::framed]
    async fn get_full_data(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<DataBlock> {
        let tenant = ctx.get_tenant();
        let operator = DataOperator::instance().operator();

        let mut temp_files_name: Vec<String> = vec![];
        let mut temp_files_content_length = vec![];
        let mut temp_files_last_modified = vec![];

        let location_prefix = format!("{}/", query_spill_prefix(tenant.tenant_name(), ""));
        if let Ok(lister) = operator
            .lister_with(&location_prefix)
            .recursive(true)
            .metakey(Metakey::LastModified | Metakey::ContentLength)
            .await
        {
            let limit = push_downs.and_then(|x| x.limit).unwrap_or(usize::MAX);
            let mut lister = lister.take(limit);

            while let Some(entry) = lister.try_next().await? {
                let metadata = entry.metadata();

                if metadata.is_file() {
                    temp_files_name.push(
                        entry
                            .path()
                            .trim_start_matches(&location_prefix)
                            .to_string(),
                    );

                    temp_files_last_modified
                        .push(metadata.last_modified().map(|x| x.timestamp_micros()));
                    temp_files_content_length.push(metadata.content_length());
                }
            }
        }

        let num_rows = temp_files_name.len();
        let data_block = DataBlock::new(
            vec![
                BlockEntry::new(
                    DataType::String,
                    Value::Scalar(Scalar::String("Spill".to_string())),
                ),
                BlockEntry::new(
                    DataType::String,
                    Value::Column(StringType::from_data(temp_files_name)),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Column(NumberType::from_data(temp_files_content_length)),
                ),
                BlockEntry::new(
                    DataType::Timestamp.wrap_nullable(),
                    Value::Column(TimestampType::from_opt_data(temp_files_last_modified)),
                ),
            ],
            num_rows,
        );

        Ok(data_block.consume_convert_to_full())
    }
}

impl TempFilesTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("file_type", TableDataType::String),
            TableField::new("file_name", TableDataType::String),
            TableField::new(
                "file_content_length",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "file_last_modified_time",
                TableDataType::Timestamp.wrap_nullable(),
            ),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'temp_files'".to_string(),
            name: "temp_files".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemTempFilesTable".to_string(),

                ..Default::default()
            },
            ..Default::default()
        };

        AsyncOneBlockSystemTable::create(Self { table_info })
    }
}
