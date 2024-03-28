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

use std::mem;
use std::sync::Arc;

use csv_core::ReadRecordResult;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_formats::RecordDelimiter;
use databend_common_storage::FileParseError;
use databend_common_storage::FileStatus;
use log::debug;

use crate::read::load_context::LoadContext;
use crate::read::row_based::batch::BytesBatch;
use crate::read::row_based::batch::CSVRowBatch;
use crate::read::row_based::batch::Position;
use crate::read::row_based::batch::RowBatch;
use crate::read::row_based::batch::RowBatchWithPosition;
use crate::read::row_based::format::SeparatorState;
use crate::read::row_based::formats::csv::CsvInputFormat;

pub const MAX_CSV_COLUMNS: usize = 1000;

pub struct CsvReader {
    load_ctx: Arc<LoadContext>,

    // select $1, $2, $3 ..  from csv
    projection: Option<Vec<usize>>,
    error_on_column_count_mismatch: bool,
    num_fields: usize,

    reader: csv_core::Reader,
    // remain from last read batch
    last_partial_row: Vec<u8>,

    // field_end[..n_end] store the output of each call to reader.read_record()
    // it may belong to part of a row.
    // flush to RowBatch when a complete row is read
    field_ends: Vec<usize>,
    n_end: usize,

    pos: Position,
    rows_to_skip: usize,
}

impl SeparatorState for CsvReader {
    fn append(&mut self, batch: BytesBatch) -> Result<(Vec<RowBatchWithPosition>, FileStatus)> {
        self.separate(batch)
    }
}

enum ReadRecordOutput {
    Record { num_fields: usize, bytes: usize },
    RecordSkipped,
    PartialRecord { bytes: usize },
}

impl CsvReader {
    pub fn try_create(
        load_ctx: Arc<LoadContext>,
        path: &str,
        format: &CsvInputFormat,
    ) -> Result<Self> {
        let escape = if format.params.escape.is_empty() {
            None
        } else {
            Some(format.params.escape.as_bytes()[0])
        };
        let reader = csv_core::ReaderBuilder::new()
            .delimiter(format.params.field_delimiter.as_bytes()[0])
            .quote(format.params.quote.as_bytes()[0])
            .escape(escape)
            .terminator(match format.params.record_delimiter.as_str().try_into()? {
                RecordDelimiter::Crlf => csv_core::Terminator::CRLF,
                RecordDelimiter::Any(v) => csv_core::Terminator::Any(v),
            })
            .build();
        let projection = load_ctx.pos_projection.clone();
        let max_fields = match &projection {
            Some(p) => p.iter().copied().max().unwrap_or(1),
            None => load_ctx.schema.num_fields(),
        } + MAX_CSV_COLUMNS;

        let num_fields = load_ctx.schema.fields().len();
        Ok(Self {
            load_ctx,
            projection,
            error_on_column_count_mismatch: format.params.error_on_column_count_mismatch,
            num_fields,
            reader,
            pos: Position::new(path.to_string()),
            rows_to_skip: format.params.headers as usize,
            field_ends: vec![0; max_fields],
            last_partial_row: vec![],
            n_end: 0,
        })
    }

    fn read_record(
        &mut self,
        input: &[u8],
        output: &mut [u8],
        file_status: &mut FileStatus,
    ) -> Result<(ReadRecordOutput, usize)> {
        let (result, n_in, n_out, n_end) =
            self.reader
                .read_record(input, output, &mut self.field_ends[self.n_end..]);
        self.n_end += n_end;
        // shadow the n_end return from reader to avoid misuse
        let n_end = self.n_end;

        match result {
            ReadRecordResult::InputEmpty => {
                if input.is_empty() {
                    Err(ErrorCode::BadBytes("unexpected eof"))
                } else {
                    self.pos.offset += n_in;
                    Ok((ReadRecordOutput::PartialRecord { bytes: n_out }, n_in))
                }
            }
            ReadRecordResult::OutputFull => Err(self.error_output_full()),
            ReadRecordResult::OutputEndsFull => Err(self.error_output_ends_full()),
            ReadRecordResult::Record => {
                let output = {
                    if self.projection.is_some() {
                        // select $1, $2, $3 ..  from csv, not check num of fields here
                        ReadRecordOutput::Record {
                            num_fields: n_end,
                            bytes: n_out,
                        }
                    } else {
                        // copy
                        if !self.error_on_column_count_mismatch {
                            // cut or patch to num_fields

                            if self.n_end < self.num_fields {
                                // support we expect 4 fields but got row with only 2 columns : "1,2\n"
                                // here we pretend we read "1,2,,\n"
                                debug_assert!(self.n_end > 0);
                                let end = self.field_ends[n_end - 1];
                                for i in n_end..self.num_fields {
                                    self.field_ends[i] = end;
                                }
                            }
                            ReadRecordOutput::Record {
                                num_fields: self.num_fields,
                                bytes: n_out,
                            }
                        } else {
                            // check num of fields strictly

                            if let Err(e) = self.check_num_field() {
                                self.load_ctx.error_handler.on_error(
                                    e,
                                    None,
                                    file_status,
                                    &self.pos.path,
                                    self.pos.rows,
                                )?;
                                ReadRecordOutput::RecordSkipped
                            } else {
                                ReadRecordOutput::Record {
                                    num_fields: self.num_fields,
                                    bytes: n_out,
                                }
                            }
                        }
                    }
                };
                self.pos.offset += n_in;
                self.pos.rows += 1;
                self.n_end = 0;
                Ok((output, n_in))
            }
            ReadRecordResult::End => {
                if !input.is_empty() {
                    Err(ErrorCode::BadBytes("unexpected eof"))
                } else {
                    Ok((ReadRecordOutput::PartialRecord { bytes: n_out }, n_in))
                }
            }
        }
    }

    fn separate(&mut self, batch: BytesBatch) -> Result<(Vec<RowBatchWithPosition>, FileStatus)> {
        // prepare for reading header and data

        let need_flush = batch.is_eof;
        let mut buf_in = &batch.data[..];
        let size_in = buf_in.len();
        let mut file_status = FileStatus::default();
        // the output of reader is always shorter than input
        let mut buf_out = vec![0u8; buf_in.len()];

        // skip headers
        // be careful not passing empty input to reader, which indicates eof
        while self.rows_to_skip > 0 && !buf_in.is_empty() {
            let (res, n_in) = self.read_record(buf_in, &mut buf_out, &mut file_status)?;
            buf_in = &buf_in[n_in..];
            if matches!(res, ReadRecordOutput::Record { .. }) {
                self.rows_to_skip -= 1;
            }
        }

        if self.rows_to_skip > 0 {
            return Ok((vec![], file_status));
        }

        // prepare for reading data

        let mut buf_out_pos = 0usize;
        let mut buf_out_row_end: usize = 0;
        let last_batch_remain_len = self.last_partial_row.len();
        let pos = Position::from_bytes_batch(&batch, self.pos.rows);
        let mut row_batch = CSVRowBatch::default();

        // read data

        // The state of last partial row from last BytesBatch is recorded in
        //    reader: state of the CSV decoder StateMachine
        //    out: the decoded data
        //    field_ends, n_end: the end of the last partial row, relative to `out`
        // We continue to feed the new BytesBatch to the reader until input is empty.
        // One additional call with empty input to flush the state of reader when meet eof.
        while !buf_in.is_empty() || need_flush {
            let (res, n_in) =
                self.read_record(buf_in, &mut buf_out[buf_out_pos..], &mut file_status)?;
            match res {
                ReadRecordOutput::Record { num_fields, bytes } => {
                    buf_out_pos += bytes;
                    row_batch.num_fields.push(num_fields);
                    row_batch
                        .field_ends
                        .extend_from_slice(&self.field_ends[..num_fields]);
                    row_batch.row_ends.push(last_batch_remain_len + buf_out_pos);
                    buf_out_row_end = buf_out_pos;
                }
                ReadRecordOutput::PartialRecord { bytes } => {
                    buf_out_pos += bytes;
                }
                _ => {}
            }
            if buf_in.is_empty() {
                // state already flushed
                break;
            } else {
                buf_in = &buf_in[n_in..];
            }
        }

        // try move data from (self.last_partial_row, buf_out) to (row_batch.data, self.last_partial_row)

        buf_out.truncate(buf_out_pos);
        if row_batch.row_ends.is_empty() {
            debug!(
                "csv aligner: {} + {} bytes => 0 rows",
                self.last_partial_row.len(),
                size_in,
            );
            self.last_partial_row.extend_from_slice(&buf_out);
            Ok((vec![], file_status))
        } else {
            let last_remain = mem::take(&mut self.last_partial_row);

            self.last_partial_row
                .extend_from_slice(&buf_out[buf_out_row_end..]);
            debug!(
                "csv aligner: {} + {} bytes => {} rows + {} bytes remain",
                last_remain.len(),
                size_in,
                row_batch.row_ends.len(),
                self.last_partial_row.len()
            );

            buf_out.truncate(buf_out_row_end);
            row_batch.data = if last_remain.is_empty() {
                buf_out
            } else {
                [last_remain, buf_out].concat()
            };

            Ok((
                vec![RowBatchWithPosition::new(RowBatch::Csv(row_batch), pos)],
                file_status,
            ))
        }
    }

    fn check_num_field(&self) -> std::result::Result<(), FileParseError> {
        let expected = self.num_fields;
        let found = self.n_end;
        if found < expected
            || found > expected + 1
            || (found == expected + 1 && self.field_ends[expected] != self.field_ends[expected - 1])
        {
            Err(FileParseError::NumberOfColumnsMismatch {
                table: expected,
                file: found,
            })
        } else {
            Ok(())
        }
    }

    fn error_output_full(&self) -> ErrorCode {
        ErrorCode::BadBytes("Bug: CSV Reader return output longer then input.")
    }

    fn error_output_ends_full(&self) -> ErrorCode {
        if self.projection.is_some() {
            ErrorCode::BadBytes(format!(
                "too many fields, expect {}, got more than {}",
                self.num_fields,
                self.field_ends.len()
            ))
        } else {
            ErrorCode::BadBytes(format!(
                "select from CSV allow at most {} fields",
                MAX_CSV_COLUMNS
            ))
        }
    }
}
