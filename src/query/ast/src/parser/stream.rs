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

use nom::combinator::map;

use crate::ast::CreateStreamStmt;
use crate::ast::DescribeStreamStmt;
use crate::ast::DropStreamStmt;
use crate::ast::ShowStreamsStmt;
use crate::ast::Statement;
use crate::parser::common::dot_separated_idents_1_to_2;
use crate::parser::common::dot_separated_idents_1_to_3;
use crate::parser::common::map_res;
use crate::parser::common::IResult;
use crate::parser::expr::literal_bool;
use crate::parser::expr::literal_string;
use crate::parser::query::travel_point;
use crate::parser::statement::parse_create_option;
use crate::parser::statement::show_limit;
use crate::parser::token::TokenKind::*;
use crate::parser::Input;
use crate::rule;

pub fn stream_table(i: Input) -> IResult<Statement> {
    rule!(
         #create_stream: "`CREATE [OR REPLACE] STREAM [IF NOT EXISTS] [<database>.]<stream> ON TABLE [<database>.]<table> [<travel_point>] [COMMENT = '<string_literal>']`"
         | #drop_stream: "`DROP STREAM [IF EXISTS] [<database>.]<stream>`"
         | #show_streams: "`SHOW [FULL] STREAMS [FROM <database>] [<show_limit>]`"
         | #describe_stream: "`DESCRIBE STREAM [<database>.]<stream>`"
    )(i)
}

fn create_stream(i: Input) -> IResult<Statement> {
    map_res(
        rule! {
            CREATE ~ ( OR ~ ^REPLACE )? ~ STREAM ~ ( IF ~ ^NOT ~ ^EXISTS )?
            ~ #dot_separated_idents_1_to_3
            ~ ON ~ TABLE ~ #dot_separated_idents_1_to_2
            ~ ( AT ~ ^#travel_point )?
            ~ ( APPEND_ONLY ~ "=" ~ #literal_bool )?
            ~ ( COMMENT ~ "=" ~ #literal_string )?
        },
        |(
            _,
            opt_or_replace,
            _,
            opt_if_not_exists,
            (catalog, database, stream),
            _,
            _,
            (table_database, table),
            opt_travel_point,
            opt_append_only,
            opt_comment,
        )| {
            let create_option =
                parse_create_option(opt_or_replace.is_some(), opt_if_not_exists.is_some())?;
            Ok(Statement::CreateStream(CreateStreamStmt {
                create_option,
                catalog,
                database,
                stream,
                table_database,
                table,
                travel_point: opt_travel_point.map(|p| p.1),
                append_only: opt_append_only
                    .map(|(_, _, append_only)| append_only)
                    .unwrap_or(true),
                comment: opt_comment.map(|(_, _, comment)| comment),
            }))
        },
    )(i)
}

fn drop_stream(i: Input) -> IResult<Statement> {
    map(
        rule! {
            DROP ~ STREAM ~ ( IF ~ ^EXISTS )? ~ #dot_separated_idents_1_to_3
        },
        |(_, _, opt_if_exists, (catalog, database, stream))| {
            Statement::DropStream(DropStreamStmt {
                if_exists: opt_if_exists.is_some(),
                catalog,
                database,
                stream,
            })
        },
    )(i)
}

fn show_streams(i: Input) -> IResult<Statement> {
    map(
        rule! {
            SHOW ~ FULL? ~ STREAMS ~ ( ( FROM | IN ) ~ #dot_separated_idents_1_to_2 )? ~ #show_limit?
        },
        |(_, opt_full, _, ctl_db, limit)| {
            let (catalog, database) = match ctl_db {
                Some((_, (Some(c), d))) => (Some(c), Some(d)),
                Some((_, (None, d))) => (None, Some(d)),
                _ => (None, None),
            };
            Statement::ShowStreams(ShowStreamsStmt {
                catalog,
                database,
                full: opt_full.is_some(),
                limit,
            })
        },
    )(i)
}

fn describe_stream(i: Input) -> IResult<Statement> {
    map(
        rule! {
            ( DESC | DESCRIBE ) ~ STREAM ~ #dot_separated_idents_1_to_3
        },
        |(_, _, (catalog, database, stream))| {
            Statement::DescribeStream(DescribeStreamStmt {
                catalog,
                database,
                stream,
            })
        },
    )(i)
}
