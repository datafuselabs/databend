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

use std::collections::HashMap;

use common_expression::ColumnId;
use serde::Deserialize;
use serde::Serialize;
use snapshot_id::new_snapshot_id;

use super::super::v1;
use crate::meta::snapshot_id;
use crate::meta::FormatVersion;
use crate::meta::SnapshotId;
use crate::meta::Versioned;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TableSnapshotStatistics {
    /// format version of snapshot
    pub format_version: FormatVersion,

    /// id of snapshot
    pub snapshot_id: SnapshotId,

    pub column_distinct_values: HashMap<ColumnId, u64>,
}

impl<T> From<T> for TableSnapshotStatistics
where T: Into<v1::TableSnapshotStatistics>
{
    fn from(value: T) -> Self {
        Self::from_v1(value.into())
    }
}

impl TableSnapshotStatistics {
    pub fn from_v1(s: v1::TableSnapshotStatistics) -> Self {
        Self {
            format_version: s.format_version,
            snapshot_id: s.snapshot_id,
            column_distinct_values: s.column_distinct_values,
        }
    }

    pub fn new(column_distinct_values: HashMap<ColumnId, u64>) -> Self {
        Self {
            format_version: TableSnapshotStatistics::VERSION,
            snapshot_id: new_snapshot_id(),
            column_distinct_values,
        }
    }

    pub fn format_version(&self) -> u64 {
        self.format_version
    }

    pub fn get_column_distinct_values(&self) -> &HashMap<ColumnId, u64> {
        &self.column_distinct_values
    }
}
