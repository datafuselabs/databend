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

use bstr::ByteSlice;
use databend_common_exception::Result;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_formats::FieldJsonAstDecoder;
use databend_common_meta_app::principal::NullAs;
use databend_common_pipeline_sources::input_formats::error_utils::truncate_column_data;
use databend_common_storage::FileParseError;

use crate::read::load_context::LoadContext;
use crate::read::row_based::batch::RowBatchWithPosition;
use crate::read::row_based::format::RowDecoder;
use crate::read::row_based::formats::ndjson::format::NdJsonInputFormat;
use crate::read::row_based::processors::BlockBuilderState;

pub struct NdJsonDecoder {
    pub load_context: Arc<LoadContext>,
    pub fmt: NdJsonInputFormat,
    pub field_decoder: FieldJsonAstDecoder,
}

impl NdJsonDecoder {
    pub fn create(fmt: NdJsonInputFormat, load_context: Arc<LoadContext>) -> Self {
        let field_decoder = FieldJsonAstDecoder::create(&load_context.file_format_options_ext);
        Self {
            load_context,
            fmt,
            field_decoder,
        }
    }
    fn read_row(
        &self,
        buf: &[u8],
        columns: &mut [ColumnBuilder],
        null_if: &[&str],
    ) -> std::result::Result<(), FileParseError> {
        let mut json: serde_json::Value =
            serde_json::from_reader(buf).map_err(|e| FileParseError::InvalidNDJsonRow {
                message: e.to_string(),
            })?;
        // todo: this is temporary
        if self.field_decoder.is_select {
            self.field_decoder
                .read_field(&mut columns[0], &json)
                .map_err(|e| FileParseError::InvalidNDJsonRow {
                    message: e.to_string(),
                })?;
        } else {
            // if it's not case_sensitive, we convert to lowercase
            if !self.field_decoder.ident_case_sensitive {
                if let serde_json::Value::Object(x) = json {
                    let y = x.into_iter().map(|(k, v)| (k.to_lowercase(), v)).collect();
                    json = serde_json::Value::Object(y);
                }
            }

            for ((column_index, field), column) in self
                .load_context
                .schema
                .fields()
                .iter()
                .enumerate()
                .zip(columns.iter_mut())
            {
                let field_name = if self.field_decoder.ident_case_sensitive {
                    field.name().to_owned()
                } else {
                    field.name().to_lowercase()
                };
                let value = json.get(field_name);
                match value {
                    None => match self.fmt.params.missing_field_as {
                        NullAs::Error => {
                            return Err(FileParseError::ColumnMissingError {
                                column_index,
                                column_name: field.name().to_owned(),
                                column_type: field.data_type.to_string(),
                            });
                        }
                        NullAs::Null => {
                            if field.is_nullable_or_null() {
                                column.push_default();
                            } else {
                                return Err(FileParseError::ColumnMissingError {
                                    column_index,
                                    column_name: field.name().to_owned(),
                                    column_type: field.data_type.to_string(),
                                });
                            }
                        }
                        NullAs::FieldDefault => {
                            if let Some(values) = &self.load_context.default_values {
                                column.push(values[column_index].as_ref());
                            } else {
                                column.push_default();
                            }
                        }
                    },
                    Some(serde_json::Value::Null) => match self.fmt.params.null_field_as {
                        NullAs::Error => unreachable!("null_field_as should be error"),
                        NullAs::Null => {
                            if field.is_nullable_or_null() {
                                column.push_default();
                            } else {
                                return Err(FileParseError::ColumnDecodeError {
                                    column_index,
                                    column_name: field.name().to_owned(),
                                    column_type: field.data_type.to_string(),
                                    decode_error: "null value is not allowed for non-nullable field, when NULL_FIELDS_AS=NULL".to_owned(),
                                    column_data: "null".to_owned(),
                                });
                            }
                        }
                        NullAs::FieldDefault => {
                            if let Some(values) = &self.load_context.default_values {
                                column.push(values[column_index].as_ref());
                            } else {
                                column.push_default();
                            }
                        }
                    },
                    Some(value) => {
                        if !null_if.is_empty()
                            && matches!(column, ColumnBuilder::Nullable(_))
                            && value.is_string()
                            && null_if.contains(&value.as_str().unwrap())
                        {
                            column.push_default();
                        } else {
                            self.field_decoder.read_field(column, value).map_err(|e| {
                                FileParseError::ColumnDecodeError {
                                    column_index,
                                    column_name: field.name().to_owned(),
                                    column_type: field.data_type.to_string(),
                                    decode_error: e.to_string(),
                                    column_data: truncate_column_data(value.to_string()),
                                }
                            })?;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

impl RowDecoder for NdJsonDecoder {
    fn add(
        &self,
        state: &mut BlockBuilderState,
        batch: RowBatchWithPosition,
    ) -> Result<Vec<DataBlock>> {
        let columns = &mut state.mutable_columns;
        let data = batch.data.into_nd_json().unwrap();
        let null_if = self
            .fmt
            .params
            .null_if
            .iter()
            .map(|x| x.as_str())
            .collect::<Vec<_>>();

        for (row_id, row) in data.iter().enumerate() {
            let row = row.trim();
            if !row.is_empty() {
                if let Err(e) = self.read_row(row, columns, &null_if) {
                    self.load_context.error_handler.on_error(
                        e,
                        Some((columns, state.num_rows)),
                        &mut state.file_status,
                        &batch.start_pos.path,
                        batch.start_pos.rows + row_id,
                    )?
                } else {
                    state.num_rows += 1;
                    state.file_status.num_rows_loaded += 1;
                }
            }
        }
        Ok(vec![])
    }
}
