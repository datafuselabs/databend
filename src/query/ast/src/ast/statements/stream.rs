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

use std::fmt::Display;
use std::fmt::Formatter;

use databend_common_meta_app::schema::CreateOption;
use derive_visitor::Drive;
use derive_visitor::DriveMut;

use crate::ast::write_dot_separated_list;
use crate::ast::Identifier;
use crate::ast::ShowLimit;
use crate::ast::TimeTravelPoint;

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct CreateStreamStmt {
    #[drive(skip)]
    pub create_option: CreateOption,
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub stream: Identifier,
    pub table_database: Option<Identifier>,
    pub table: Identifier,
    pub travel_point: Option<TimeTravelPoint>,
    #[drive(skip)]
    pub append_only: bool,
    #[drive(skip)]
    pub comment: Option<String>,
}

impl Display for CreateStreamStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CREATE ")?;
        if let CreateOption::CreateOrReplace = self.create_option {
            write!(f, "OR REPLACE ")?;
        }
        write!(f, "STREAM ")?;
        if let CreateOption::CreateIfNotExists = self.create_option {
            write!(f, "IF NOT EXISTS ")?;
        }
        write_dot_separated_list(
            f,
            self.catalog
                .iter()
                .chain(self.database.iter())
                .chain(Some(&self.stream)),
        )?;
        write!(f, " ON TABLE ")?;
        write_dot_separated_list(f, self.table_database.iter().chain(Some(&self.table)))?;
        if let Some(travel_point) = &self.travel_point {
            write!(f, " AT {}", travel_point)?;
        }
        if !self.append_only {
            write!(f, " APPEND_ONLY = false")?;
        }
        if let Some(comment) = &self.comment {
            write!(f, " COMMENT = '{}'", comment)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct DropStreamStmt {
    #[drive(skip)]
    pub if_exists: bool,
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub stream: Identifier,
}

impl Display for DropStreamStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DROP STREAM ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write_dot_separated_list(
            f,
            self.catalog
                .iter()
                .chain(self.database.iter())
                .chain(Some(&self.stream)),
        )
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct ShowStreamsStmt {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    #[drive(skip)]
    pub full: bool,
    pub limit: Option<ShowLimit>,
}

impl Display for ShowStreamsStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "SHOW ")?;
        if self.full {
            write!(f, "FULL ")?;
        }
        write!(f, "STREAMS")?;
        if let Some(database) = &self.database {
            write!(f, " FROM ")?;
            if let Some(catalog) = &self.catalog {
                write!(f, "{catalog}.",)?;
            }
            write!(f, "{database}")?;
        }
        if let Some(limit) = &self.limit {
            write!(f, " {limit}")?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Drive, DriveMut)]
pub struct DescribeStreamStmt {
    pub catalog: Option<Identifier>,
    pub database: Option<Identifier>,
    pub stream: Identifier,
}

impl Display for DescribeStreamStmt {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DESCRIBE STREAM ")?;
        write_dot_separated_list(
            f,
            self.catalog
                .iter()
                .chain(self.database.iter().chain(Some(&self.stream))),
        )
    }
}
