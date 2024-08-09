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

use databend_common_base::base::tokio::io::AsyncWrite;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::number::NumberScalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::Column as ExprColumn;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::ScalarRef;
use databend_common_expression::SendableDataBlockStream;
use databend_common_formats::field_encoder::FieldEncoderValues;
use databend_common_io::prelude::FormatSettings;
use futures_util::StreamExt;
use log::error;
use opensrv_mysql::*;

use crate::sessions::Session;
/// Reports progress information as string, intend to be put into the mysql Ok packet.
/// Mainly for decoupling with concrete type like `QueryContext`
///
/// Something like
/// "Read x rows, y MiB in z sec., A million rows/sec., B MiB/sec."
pub trait ProgressReporter {
    fn progress_info(&self) -> String;
    fn affected_rows(&self) -> u64;
}

pub struct QueryResult {
    blocks: SendableDataBlockStream,
    extra_info: Option<Box<dyn ProgressReporter + Send>>,
    has_result_set: bool,
    schema: DataSchemaRef,
    sql: String,
}

impl QueryResult {
    pub fn create(
        blocks: SendableDataBlockStream,
        extra_info: Option<Box<dyn ProgressReporter + Send>>,
        has_result_set: bool,
        schema: DataSchemaRef,
        sql: String,
    ) -> QueryResult {
        QueryResult {
            blocks,
            extra_info,
            has_result_set,
            schema,
            sql,
        }
    }
}

pub struct DFQueryResultWriter<'a, W: AsyncWrite + Send + Unpin> {
    inner: Option<QueryResultWriter<'a, W>>,
    session: Arc<Session>,
}

fn write_field<W: AsyncWrite + Unpin>(
    row_writer: &mut RowWriter<W>,
    column: &ExprColumn,
    encoder: &FieldEncoderValues,
    buf: &mut Vec<u8>,
    row_index: usize,
) -> Result<()> {
    buf.clear();
    encoder.write_field(column, row_index, buf, false);
    row_writer.write_col(&buf[..])?;
    Ok(())
}

impl<'a, W: AsyncWrite + Send + Unpin> DFQueryResultWriter<'a, W> {
    pub fn create(
        inner: QueryResultWriter<'a, W>,
        session: Arc<Session>,
    ) -> DFQueryResultWriter<'a, W> {
        DFQueryResultWriter::<'a, W> {
            inner: Some(inner),
            session,
        }
    }

    #[async_backtrace::framed]
    pub async fn write(
        &mut self,
        query_result: Result<(QueryResult, Option<FormatSettings>)>,
        format: &FormatSettings,
    ) -> Result<()> {
        if let Some(writer) = self.inner.take() {
            match query_result {
                Ok((query_result, query_format)) => {
                    if let Some(format) = query_format {
                        self.ok(query_result, writer, &format).await?
                    } else {
                        self.ok(query_result, writer, format).await?
                    }
                }
                Err(error) => self.err(&error, writer).await?,
            }
        }
        Ok(())
    }

    #[async_backtrace::framed]
    async fn ok(
        &self,
        mut query_result: QueryResult,
        dataset_writer: QueryResultWriter<'a, W>,
        format: &FormatSettings,
    ) -> Result<()> {
        // XXX: num_columns == 0 may is error?
        if !query_result.has_result_set {
            // For statements without result sets, we still need to pull the stream because errors may occur in the stream.
            let blocks = &mut query_result.blocks;
            while let Some(block) = blocks.next().await {
                if let Err(e) = block {
                    error!("dataset write failed: {:?}", e);
                    self.session.txn_mgr().lock().set_fail();
                    dataset_writer
                        .error(
                            ErrorKind::ER_UNKNOWN_ERROR,
                            format!(
                                "dataset write failed: {}",
                                e.display_with_sql(&query_result.sql)
                            )
                            .as_bytes(),
                        )
                        .await?;

                    return Ok(());
                }
            }

            let affected_rows = query_result
                .extra_info
                .map(|r| r.affected_rows())
                .unwrap_or_default();
            dataset_writer
                .completed(OkResponse {
                    affected_rows,
                    ..Default::default()
                })
                .await?;
            return Ok(());
        }

        fn convert_field_type(field: &DataField) -> Result<ColumnType> {
            match field.data_type().remove_nullable() {
                DataType::Null => Ok(ColumnType::MYSQL_TYPE_NULL),
                DataType::EmptyArray => Ok(ColumnType::MYSQL_TYPE_VARCHAR),
                DataType::EmptyMap => Ok(ColumnType::MYSQL_TYPE_VARCHAR),
                DataType::Boolean => Ok(ColumnType::MYSQL_TYPE_SHORT),
                DataType::Binary => Ok(ColumnType::MYSQL_TYPE_BLOB),
                DataType::String => Ok(ColumnType::MYSQL_TYPE_VARCHAR),
                DataType::Number(num_ty) => match num_ty {
                    NumberDataType::Int8 => Ok(ColumnType::MYSQL_TYPE_TINY),
                    NumberDataType::Int16 => Ok(ColumnType::MYSQL_TYPE_SHORT),
                    NumberDataType::Int32 => Ok(ColumnType::MYSQL_TYPE_LONG),
                    NumberDataType::Int64 => Ok(ColumnType::MYSQL_TYPE_LONGLONG),
                    NumberDataType::UInt8 => Ok(ColumnType::MYSQL_TYPE_TINY),
                    NumberDataType::UInt16 => Ok(ColumnType::MYSQL_TYPE_SHORT),
                    NumberDataType::UInt32 => Ok(ColumnType::MYSQL_TYPE_LONG),
                    NumberDataType::UInt64 => Ok(ColumnType::MYSQL_TYPE_LONGLONG),
                    NumberDataType::Float32 => Ok(ColumnType::MYSQL_TYPE_FLOAT),
                    NumberDataType::Float64 => Ok(ColumnType::MYSQL_TYPE_DOUBLE),
                },
                DataType::Date => Ok(ColumnType::MYSQL_TYPE_DATE),
                DataType::Timestamp => Ok(ColumnType::MYSQL_TYPE_DATETIME),
                DataType::Array(_) => Ok(ColumnType::MYSQL_TYPE_VARCHAR),
                DataType::Map(_) => Ok(ColumnType::MYSQL_TYPE_VARCHAR),
                DataType::Bitmap => Ok(ColumnType::MYSQL_TYPE_VARCHAR),
                DataType::Tuple(_) => Ok(ColumnType::MYSQL_TYPE_VARCHAR),
                DataType::Variant => Ok(ColumnType::MYSQL_TYPE_VARCHAR),
                DataType::Geometry => Ok(ColumnType::MYSQL_TYPE_GEOMETRY),
                DataType::Geography => Ok(ColumnType::MYSQL_TYPE_GEOMETRY),
                DataType::Decimal(_) => Ok(ColumnType::MYSQL_TYPE_DECIMAL),
                _ => Err(ErrorCode::Unimplemented(format!(
                    "Unsupported column type:{:?}",
                    field.data_type()
                ))),
            }
        }

        fn make_column_from_field(field: &DataField) -> Result<Column> {
            convert_field_type(field).map(|column_type| Column {
                table: "".to_string(),
                column: field.name().to_string(),
                coltype: column_type,
                colflags: ColumnFlags::empty(),
            })
        }

        fn convert_schema(schema: &DataSchemaRef) -> Result<Vec<Column>> {
            schema.fields().iter().map(make_column_from_field).collect()
        }

        let _tz = format.timezone;
        match convert_schema(&query_result.schema) {
            Err(error) => self.err(&error, dataset_writer).await,
            Ok(columns) => {
                let mut row_writer = dataset_writer.start(&columns).await?;
                let blocks = &mut query_result.blocks;

                while let Some(block) = blocks.next().await {
                    let block = match block {
                        Err(e) => {
                            error!("result row write failed: {:?}", e);
                            self.session.txn_mgr().lock().set_fail();
                            row_writer
                                .finish_error(
                                    ErrorKind::ER_UNKNOWN_ERROR,
                                    &e.display_with_sql(&query_result.sql).to_string().as_bytes(),
                                )
                                .await?;
                            return Ok(());
                        }
                        Ok(block) => block,
                    };

                    let num_rows = block.num_rows();
                    let encoder = FieldEncoderValues::create_for_mysql_handler(
                        format.timezone,
                        format.geometry_format,
                    );
                    let mut buf = Vec::<u8>::new();

                    let columns = block
                        .convert_to_full()
                        .columns()
                        .iter()
                        .map(|column| column.value.clone().into_column().unwrap())
                        .collect::<Vec<_>>();

                    for row_index in 0..num_rows {
                        for column in columns.iter() {
                            let value = unsafe { column.index_unchecked(row_index) };
                            match value {
                                ScalarRef::Null => {
                                    row_writer.write_col(None::<u8>)?;
                                }
                                ScalarRef::Boolean(v) => {
                                    row_writer.write_col(v as u8)?;
                                }
                                ScalarRef::Number(number) => match number {
                                    NumberScalar::UInt8(v) => {
                                        row_writer.write_col(v)?;
                                    }
                                    NumberScalar::UInt16(v) => {
                                        row_writer.write_col(v)?;
                                    }
                                    NumberScalar::UInt32(v) => {
                                        row_writer.write_col(v)?;
                                    }
                                    NumberScalar::UInt64(v) => {
                                        row_writer.write_col(v)?;
                                    }
                                    NumberScalar::Int8(v) => {
                                        row_writer.write_col(v)?;
                                    }
                                    NumberScalar::Int16(v) => {
                                        row_writer.write_col(v)?;
                                    }
                                    NumberScalar::Int32(v) => {
                                        row_writer.write_col(v)?;
                                    }
                                    NumberScalar::Int64(v) => {
                                        row_writer.write_col(v)?;
                                    }
                                    _ => {
                                        write_field(
                                            &mut row_writer,
                                            column,
                                            &encoder,
                                            &mut buf,
                                            row_index,
                                        )?;
                                    }
                                },
                                ScalarRef::Bitmap(_) => {
                                    let bitmap_result = "<bitmap binary>".as_bytes();
                                    row_writer.write_col(bitmap_result)?;
                                }
                                _ => write_field(
                                    &mut row_writer,
                                    column,
                                    &encoder,
                                    &mut buf,
                                    row_index,
                                )?,
                            }
                        }
                        row_writer.end_row().await?;
                    }
                }

                let info = query_result
                    .extra_info
                    .map(|r| r.progress_info())
                    .unwrap_or_default();
                row_writer.finish_with_info(&info).await?;

                Ok(())
            }
        }
    }

    #[async_backtrace::framed]
    async fn err(&self, error: &ErrorCode, writer: QueryResultWriter<'a, W>) -> Result<()> {
        self.session.txn_mgr().lock().set_fail();
        if error.code() != ErrorCode::ABORTED_QUERY && error.code() != ErrorCode::ABORTED_SESSION {
            error!("OnQuery Error: {:?}", error);
            writer
                .error(ErrorKind::ER_UNKNOWN_ERROR, error.to_string().as_bytes())
                .await?;
        } else {
            writer
                .error(
                    ErrorKind::ER_ABORTING_CONNECTION,
                    error.to_string().as_bytes(),
                )
                .await?;
        }

        Ok(())
    }
}
