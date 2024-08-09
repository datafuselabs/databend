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

use databend_common_arrow::arrow::datatypes::Field;
use databend_common_arrow::native::read::reader::NativeReader;
use databend_common_arrow::native::stat::stat_simple;
use databend_common_arrow::native::stat::ColumnInfo;
use databend_common_arrow::native::stat::PageBody;
use databend_common_catalog::catalog_kind::CATALOG_DEFAULT;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::Filters;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_args::TableArgs;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::nullable::NullableColumnBuilder;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::UInt32Type;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::Evaluator;
use databend_common_expression::Expr;
use databend_common_expression::FromData;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::RemoteExpr;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::Value;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_table_meta::meta::SegmentInfo;

use crate::io::BlockReader;
use crate::io::ReadSettings;
use crate::io::SegmentsIO;
use crate::sessions::TableContext;
use crate::table_functions::parse_db_tb_col_args;
use crate::table_functions::string_literal;
use crate::table_functions::SimpleArgFunc;
use crate::table_functions::SimpleArgFuncTemplate;
use crate::FuseStorageFormat;
use crate::FuseTable;

pub struct FuseEncodingArgs {
    database_name: String,
}

impl TryFrom<(&str, TableArgs)> for FuseEncodingArgs {
    type Error = ErrorCode;
    fn try_from(
        (func_name, table_args): (&str, TableArgs),
    ) -> std::result::Result<Self, Self::Error> {
        let database_name = parse_db_tb_col_args(&table_args, func_name)?;
        Ok(Self { database_name })
    }
}

impl From<&FuseEncodingArgs> for TableArgs {
    fn from(args: &FuseEncodingArgs) -> Self {
        TableArgs::new_positioned(vec![string_literal(args.database_name.as_str())])
    }
}

pub type FuseEncodingFunc = SimpleArgFuncTemplate<FuseEncoding>;
pub struct FuseEncoding;

#[async_trait::async_trait]
impl SimpleArgFunc for FuseEncoding {
    type Args = FuseEncodingArgs;

    fn schema() -> TableSchemaRef {
        FuseEncodingImpl::schema()
    }

    async fn apply(
        ctx: &Arc<dyn TableContext>,
        args: &Self::Args,
        plan: &DataSourcePlan,
    ) -> Result<DataBlock> {
        let tenant_id = ctx.get_tenant();
        let tbls = ctx
            .get_catalog(CATALOG_DEFAULT)
            .await?
            .get_database(&tenant_id, args.database_name.as_str())
            .await?
            .list_tables()
            .await?;

        let fuse_tables = tbls
            .iter()
            .map(|tbl| {
                let tbl = FuseTable::try_from_table(tbl.as_ref()).unwrap();
                tbl
            })
            .collect::<Vec<_>>();

        let filters = plan.push_downs.as_ref().and_then(|x| x.filters.clone());
        FuseEncodingImpl::new(ctx.clone(), fuse_tables, filters)
            .get_blocks()
            .await
    }
}

pub struct FuseEncodingImpl<'a> {
    pub ctx: Arc<dyn TableContext>,
    pub tables: Vec<&'a FuseTable>,
    pub filters: Option<Filters>,
}

impl<'a> FuseEncodingImpl<'a> {
    pub fn new(
        ctx: Arc<dyn TableContext>,
        tables: Vec<&'a FuseTable>,
        filters: Option<Filters>,
    ) -> Self {
        Self {
            ctx,
            tables,
            filters,
        }
    }

    #[async_backtrace::framed]
    pub async fn get_blocks(&self) -> Result<DataBlock> {
        let mut info = Vec::new();
        for table in self.tables.clone() {
            if matches!(table.storage_format, FuseStorageFormat::Parquet) {
                continue;
            }
            let mut columns_info = vec![];
            let snapshot = table.read_table_snapshot().await?;
            if snapshot.is_none() {
                continue;
            }
            let snapshot = snapshot.unwrap();

            let segments_io =
                SegmentsIO::create(self.ctx.clone(), table.operator.clone(), table.schema());

            let chunk_size = self.ctx.get_settings().get_max_threads()? as usize * 4;

            let schema = table.schema();
            let fields = schema.fields();
            for chunk in snapshot.segments.chunks(chunk_size) {
                let segments = segments_io
                    .read_segments::<SegmentInfo>(chunk, false)
                    .await?;
                for segment in segments {
                    let segment = segment?;
                    for block in segment.blocks.iter() {
                        for field in fields {
                            if field.is_nested() {
                                continue;
                            }
                            let column_id = field.column_id;
                            let arrow_field: Field = field.into();
                            let column_meta = block.col_metas.get(&column_id).unwrap();
                            let (offset, len) = column_meta.offset_length();
                            let ranges = vec![(column_id, offset..(offset + len))];
                            let read_settings = ReadSettings::from_ctx(&self.ctx)?;
                            let merge_io_read_res = BlockReader::merge_io_read(
                                &read_settings,
                                table.operator.clone(),
                                &block.location.0,
                                &ranges,
                                true,
                            )
                            .await?;
                            let column_chunks = merge_io_read_res.columns_chunks()?;
                            let pages = column_chunks
                                .get(&column_id)
                                .unwrap()
                                .as_raw_data()
                                .unwrap();
                            let pages = std::io::Cursor::new(pages);
                            let page_metas = column_meta.as_native().unwrap().pages.clone();
                            let reader = NativeReader::new(pages, page_metas, vec![]);
                            let this_column_info = stat_simple(reader, arrow_field.clone())?;
                            columns_info.push((field.data_type.sql_name(), this_column_info));
                        }
                    }
                }
            }
            info.push((table.name(), columns_info));
        }
        let data_block = self.to_block(&info).await?;
        let result = if let Some(filter) = self.filters.as_ref().map(|f| &f.filter) {
            let func_ctx = FunctionContext::default();
            let evaluator = Evaluator::new(&data_block, &func_ctx, &BUILTIN_FUNCTIONS);
            let filter = evaluator
                .run(&as_expr(
                    filter,
                    &BUILTIN_FUNCTIONS,
                    &FuseEncodingImpl::schema(),
                ))?
                .try_downcast::<BooleanType>()
                .unwrap();
            data_block.filter_boolean_value(&filter)?
        } else {
            data_block
        };

        Ok(result)
    }

    #[async_backtrace::framed]
    async fn to_block(&self, info: &Vec<(&str, Vec<(String, ColumnInfo)>)>) -> Result<DataBlock> {
        let mut validity_size = Vec::new();
        let mut compressed_size = Vec::new();
        let mut uncompressed_size = Vec::new();
        let mut l1 = StringColumnBuilder::with_capacity(0, 0);
        let mut l2 = NullableColumnBuilder::<StringType>::with_capacity(0, &[]);
        let mut table_name = StringColumnBuilder::with_capacity(0, 0);
        let mut column_name = StringColumnBuilder::with_capacity(0, 0);
        let mut column_type = StringColumnBuilder::with_capacity(0, 0);
        let mut all_num_rows = 0;
        for (table, columns_info) in info {
            for (type_str, column_info) in columns_info {
                let pages_info = &column_info.pages;
                let num_row = pages_info.len();
                all_num_rows += num_row;
                validity_size.reserve(num_row);
                compressed_size.reserve(num_row);
                uncompressed_size.reserve(num_row);
                let tmp_table_name = StringColumnBuilder::repeat(table, num_row);
                let tmp_column_name = StringColumnBuilder::repeat(&column_info.field.name, num_row);
                let tmp_column_type = StringColumnBuilder::repeat(type_str, num_row);
                for p in pages_info {
                    validity_size.push(p.validity_size);
                    compressed_size.push(p.compressed_size);
                    uncompressed_size.push(p.uncompressed_size);
                    l1.put_str(&encoding_to_string(&p.body));
                    l1.commit_row();
                    let l2_encoding = match &p.body {
                        PageBody::Dict(dict) => Some(encoding_to_string(&dict.indices.body)),
                        PageBody::Freq(freq) => freq
                            .exceptions
                            .as_ref()
                            .map(|e| encoding_to_string(&e.body)),
                        _ => None,
                    };
                    if let Some(l2_encoding) = l2_encoding {
                        l2.push(&l2_encoding);
                    } else {
                        l2.push_null();
                    }
                }

                table_name.append_column(&tmp_table_name.build());
                column_name.append_column(&tmp_column_name.build());
                column_type.append_column(&tmp_column_type.build());
            }
        }

        Ok(DataBlock::new(
            vec![
                BlockEntry::new(
                    DataType::String,
                    Value::Column(Column::String(table_name.build())),
                ),
                BlockEntry::new(
                    DataType::String,
                    Value::Column(Column::String(column_name.build())),
                ),
                BlockEntry::new(
                    DataType::String,
                    Value::Column(Column::String(column_type.build())),
                ),
                BlockEntry::new(
                    DataType::Nullable(Box::new(DataType::Number(NumberDataType::UInt32))),
                    Value::Column(UInt32Type::from_opt_data(validity_size)),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt32),
                    Value::Column(UInt32Type::from_data(compressed_size)),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt32),
                    Value::Column(UInt32Type::from_data(uncompressed_size)),
                ),
                BlockEntry::new(DataType::String, Value::Column(Column::String(l1.build()))),
                BlockEntry::new(
                    DataType::Nullable(Box::new(DataType::String)),
                    Value::Column(Column::Nullable(Box::new(l2.build().upcast()))),
                ),
            ],
            all_num_rows,
        ))
    }

    pub fn schema() -> Arc<TableSchema> {
        TableSchemaRefExt::create(vec![
            TableField::new("table_name", TableDataType::String),
            TableField::new("column_name", TableDataType::String),
            TableField::new("column_type", TableDataType::String),
            TableField::new(
                "validity_size",
                TableDataType::Nullable(Box::new(TableDataType::Number(NumberDataType::UInt32))),
            ),
            TableField::new(
                "compressed_size",
                TableDataType::Number(NumberDataType::UInt32),
            ),
            TableField::new(
                "uncompressed_size",
                TableDataType::Number(NumberDataType::UInt32),
            ),
            TableField::new("level_one", TableDataType::String),
            TableField::new(
                "level_two",
                TableDataType::Nullable(Box::new(TableDataType::String)),
            ),
        ])
    }
}

fn encoding_to_string(page_body: &PageBody) -> String {
    match page_body {
        PageBody::Dict(_) => "Dict".to_string(),
        PageBody::Freq(_) => "Freq".to_string(),
        PageBody::OneValue => "OneValue".to_string(),
        PageBody::Rle => "Rle".to_string(),
        PageBody::Patas => "Patas".to_string(),
        PageBody::Bitpack => "Bitpack".to_string(),
        PageBody::DeltaBitpack => "DeltaBitpack".to_string(),
        PageBody::Common(c) => format!("Common({:?})", c),
    }
}

pub fn as_expr(
    remote_expr: &RemoteExpr<String>,
    fn_registry: &FunctionRegistry,
    schema: &Arc<TableSchema>,
) -> Expr {
    match remote_expr {
        RemoteExpr::Constant {
            span,
            scalar,
            data_type,
        } => Expr::Constant {
            span: *span,
            scalar: scalar.clone(),
            data_type: data_type.clone(),
        },
        RemoteExpr::ColumnRef {
            span,
            id,
            data_type,
            display_name,
        } => {
            let id = schema.index_of(id).unwrap();
            Expr::ColumnRef {
                span: *span,
                id,
                data_type: data_type.clone(),
                display_name: display_name.clone(),
            }
        }
        RemoteExpr::Cast {
            span,
            is_try,
            expr,
            dest_type,
        } => Expr::Cast {
            span: *span,
            is_try: *is_try,
            expr: Box::new(as_expr(expr, fn_registry, schema)),
            dest_type: dest_type.clone(),
        },
        RemoteExpr::FunctionCall {
            span,
            id,
            generics,
            args,
            return_type,
        } => {
            let function = fn_registry.get(id).expect("function id not found");
            Expr::FunctionCall {
                span: *span,
                id: id.clone(),
                function,
                generics: generics.clone(),
                args: args
                    .iter()
                    .map(|arg| as_expr(arg, fn_registry, schema))
                    .collect(),
                return_type: return_type.clone(),
            }
        }
        RemoteExpr::LambdaFunctionCall {
            span,
            name,
            args,
            lambda_expr,
            lambda_display,
            return_type,
        } => {
            let args = args
                .iter()
                .map(|arg| as_expr(arg, fn_registry, schema))
                .collect();
            Expr::LambdaFunctionCall {
                span: *span,
                name: name.clone(),
                args,
                lambda_expr: *lambda_expr.clone(),
                lambda_display: lambda_display.clone(),
                return_type: return_type.clone(),
            }
        }
    }
}
