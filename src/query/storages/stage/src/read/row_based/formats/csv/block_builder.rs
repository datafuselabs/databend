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

use databend_common_exception::Result;
use databend_common_expression::types::nullable::NullableColumnBuilder;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::TableDataType;
use databend_common_formats::SeparatedTextDecoder;
use databend_common_meta_app::principal::EmptyFieldAs;
use databend_common_pipeline_sources::input_formats::error_utils::get_decode_error_by_pos;
use databend_common_storage::FileParseError;

use crate::read::load_context::LoadContext;
use crate::read::row_based::batch::RowBatchWithPosition;
use crate::read::row_based::format::RowDecoder;
use crate::read::row_based::formats::csv::CsvInputFormat;
use crate::read::row_based::processors::BlockBuilderState;

pub struct CsvDecoder {
    pub load_context: Arc<LoadContext>,
    pub fmt: CsvInputFormat,
    pub field_decoder: SeparatedTextDecoder,
}

impl CsvDecoder {
    pub fn create(fmt: CsvInputFormat, load_context: Arc<LoadContext>) -> Self {
        let field_decoder =
            SeparatedTextDecoder::create_csv(&fmt.params, &load_context.file_format_options_ext);
        Self {
            load_context,
            fmt,
            field_decoder,
        }
    }

    fn read_column(
        &self,
        builder: &mut ColumnBuilder,
        col_data: &[u8],
        column_index: usize,
    ) -> std::result::Result<(), FileParseError> {
        let empty_filed_as = &self.fmt.params.empty_field_as;
        if col_data.is_empty() {
            match &self.load_context.default_values {
                None => {
                    // query
                    builder.push_default();
                }
                Some(values) => {
                    let field = &self.load_context.schema.fields()[column_index];
                    // copy
                    match empty_filed_as {
                        EmptyFieldAs::FieldDefault => {
                            builder.push(values[column_index].as_ref());
                        }
                        EmptyFieldAs::Null => {
                            if !matches!(field.data_type, TableDataType::Nullable(_)) {
                                return Err(FileParseError::ColumnEmptyError {
                                    column_index,
                                    column_name: field.name().to_owned(),
                                    column_type: field.data_type.to_string(),
                                    empty_field_as: empty_filed_as.to_string(),
                                    remedy: format!(
                                        "one of the following options: 1. Modify the `{}` column to allow NULL values. 2. Set EMPTY_FIELD_AS to FIELD_DEFAULT.",
                                        field.name()
                                    ),
                                });
                            }
                            builder.push_default();
                        }
                        EmptyFieldAs::String => match builder {
                            ColumnBuilder::String(b) => {
                                b.put_str("");
                                b.commit_row();
                            }
                            ColumnBuilder::Nullable(box NullableColumnBuilder {
                                builder: ColumnBuilder::String(b),
                                validity,
                            }) => {
                                b.put_str("");
                                b.commit_row();
                                validity.push(true);
                            }
                            _ => {
                                let field = &self.load_context.schema.fields()[column_index];
                                return Err(FileParseError::ColumnEmptyError {
                                    column_index,
                                    column_name: field.name().to_owned(),
                                    column_type: field.data_type.to_string(),
                                    empty_field_as: empty_filed_as.to_string(),
                                    remedy: "Set EMPTY_FIELD_AS to FIELD_DEFAULT or NULL."
                                        .to_string(),
                                });
                            }
                        },
                    }
                }
            }
            return Ok(());
        }
        self.field_decoder
            .read_field(builder, col_data)
            .map_err(|e| {
                get_decode_error_by_pos(
                    column_index,
                    &self.load_context.schema,
                    &e.message(),
                    col_data,
                )
            })
    }

    fn read_row(
        &self,
        buf: &[u8],
        columns: &mut [ColumnBuilder],
        field_ends: &[usize],
    ) -> std::result::Result<(), FileParseError> {
        if let Some(columns_to_read) = &self.load_context.pos_projection {
            for c in columns_to_read {
                if *c >= field_ends.len() {
                    columns[*c].push_default();
                } else {
                    let field_start = if *c == 0 { 0 } else { field_ends[c - 1] };
                    let field_end = field_ends[*c];
                    let col_data = &buf[field_start..field_end];
                    self.read_column(&mut columns[*c], col_data, *c)?;
                }
            }
        } else {
            let mut field_start = 0;
            for (c, column) in columns.iter_mut().enumerate() {
                let field_end = field_ends[c];
                let col_data = &buf[field_start..field_end];
                self.read_column(column, col_data, c)?;
                field_start = field_end;
            }
        }
        Ok(())
    }
}

impl RowDecoder for CsvDecoder {
    fn add(
        &self,
        state: &mut BlockBuilderState,
        batch: RowBatchWithPosition,
    ) -> Result<Vec<DataBlock>> {
        let data = batch.data.into_csv().unwrap();
        let columns = &mut state.mutable_columns;
        let mut start = 0usize;
        let mut field_end_idx = 0;
        for (i, end) in data.row_ends.iter().enumerate() {
            let num_fields = data.num_fields[i];
            let buf = &data.data[start..*end];
            if let Err(e) = self.read_row(
                buf,
                columns,
                &data.field_ends[field_end_idx..field_end_idx + num_fields],
            ) {
                self.load_context.error_handler.on_error(
                    e,
                    Some((columns, state.num_rows)),
                    &mut state.file_status,
                    &batch.start_pos.path,
                    i + batch.start_pos.rows,
                )?
            } else {
                state.num_rows += 1;
                state.file_status.num_rows_loaded += 1;
            }
            start = *end;
            field_end_idx += num_fields;
        }
        Ok(vec![])
    }

    fn flush(&self, columns: Vec<Column>, num_rows: usize) -> Vec<Column> {
        if let Some(projection) = &self.load_context.pos_projection {
            let empty_strings = Column::String(
                StringColumnBuilder {
                    need_estimated: false,
                    data: vec![],
                    offsets: vec![0; num_rows + 1],
                }
                .build(),
            );
            columns
                .into_iter()
                .enumerate()
                .map(|(i, c)| {
                    if projection.contains(&i) {
                        c
                    } else {
                        empty_strings.clone()
                    }
                })
                .collect::<Vec<_>>()
        } else {
            columns
        }
    }
}
