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

use std::collections::BTreeMap;
use std::ops::Add;
use std::path::Path;

use chrono::DateTime;
use chrono::Datelike;
use chrono::TimeZone;
use chrono::Timelike;
use chrono::Utc;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use crate::table::table_storage_prefix;
use crate::table::OPT_KEY_DATABASE_ID;
use crate::table::OPT_KEY_STORAGE_PREFIX;
use crate::table::OPT_KEY_TEMP_PREFIX;

pub const TEMP_TABLE_STORAGE_PREFIX: &str = "_tmp_tbl";
pub const VACUUM2_OBJECT_KEY_PREFIX: &str = "g";

pub fn trim_timestamp_to_micro_second(ts: DateTime<Utc>) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(
        ts.year(),
        ts.month(),
        ts.day(),
        ts.hour(),
        ts.minute(),
        ts.second(),
    )
    .unwrap()
    .with_nanosecond(ts.timestamp_subsec_micros() * 1_000)
    .unwrap()
}

pub fn monotonically_increased_timestamp(
    timestamp: DateTime<Utc>,
    previous_timestamp: &Option<DateTime<Utc>>,
) -> DateTime<Utc> {
    if let Some(prev_instant) = previous_timestamp {
        // timestamp of the snapshot should always larger than the previous one's
        if prev_instant > &timestamp {
            // if local time is smaller, use the timestamp of previous snapshot, plus 1 ms
            return prev_instant.add(chrono::Duration::milliseconds(1));
        }
    }
    timestamp
}

pub fn parse_storage_prefix(options: &BTreeMap<String, String>, table_id: u64) -> Result<String> {
    // if OPT_KE_STORAGE_PREFIX is specified, use it as storage prefix
    if let Some(prefix) = options.get(OPT_KEY_STORAGE_PREFIX) {
        return Ok(prefix.clone());
    }

    // otherwise, use database id and table id as storage prefix

    let db_id = options.get(OPT_KEY_DATABASE_ID).ok_or_else(|| {
        ErrorCode::Internal(format!(
            "Invalid fuse table, table option {} not found",
            OPT_KEY_DATABASE_ID
        ))
    })?;
    let mut prefix = table_storage_prefix(db_id, table_id);
    if let Some(temp_prefix) = options.get(OPT_KEY_TEMP_PREFIX) {
        prefix = format!("{}/{}/{}", TEMP_TABLE_STORAGE_PREFIX, temp_prefix, prefix);
    }
    Ok(prefix)
}

#[inline]
pub fn trim_vacuum2_object_prefix(key: &str) -> &str {
    // if object key (the file_name/stem part only) starts with a char which is larger or equals to
    // VACUUM2_OBJECT_KEY_PREFIX( i.e. char 'g'), strip it off
    if key >= VACUUM2_OBJECT_KEY_PREFIX {
        &key[1..]
    } else {
        key
    }
}

pub fn is_possible_non_standard_decimal_block(block_full_path: &str) -> Result<bool> {
    let file_name = Path::new(block_full_path)
        .file_name()
        .ok_or_else(|| {
            ErrorCode::StorageOther(format!(
                "Illegal block path, no file name found: {}",
                block_full_path
            ))
        })?
        .to_str()
        .expect("File stem of a block full path should always be valid UTF-8");
    Ok(file_name < VACUUM2_OBJECT_KEY_PREFIX)
}

// Extracts the UUID part from the object key.
// For example, given a path like:
//   bucket/root/115/122/_b/g0191114d30fd78b89fae8e5c88327725_v2.parquet
//   bucket/root/115/122/_b/0191114d30fd78b89fae8e5c88327725_v2.parquet
// The function should return: 0191114d30fd78b89fae8e5c88327725
pub fn try_extract_uuid_str_from_path(path: &str) -> databend_common_exception::Result<&str> {
    if let Some(file_stem) = Path::new(path).file_stem() {
        let file_name = file_stem
            .to_str()
            .unwrap() // path is always valid utf8 string
            .split('_')
            .collect::<Vec<&str>>();
        let uuid = trim_vacuum2_object_prefix(file_name[0]);
        Ok(uuid)
    } else {
        Err(ErrorCode::StorageOther(format!(
            "Illegal object key, no file stem found: {}",
            path
        )))
    }
}

#[cfg(test)]
mod tests {
    use databend_common_base::base::uuid::Uuid;

    use super::*;

    #[test]
    fn test_trim_vacuum2_object_prefix() {
        let uuid = Uuid::now_v7();
        assert_eq!(
            trim_vacuum2_object_prefix(&format!("g{}", uuid)),
            uuid.to_string()
        );
        assert_eq!(
            trim_vacuum2_object_prefix(&uuid.to_string()),
            uuid.to_string()
        );
    }

    #[test]
    fn test_try_extract_uuid_str_from_path() {
        let test_cases = vec![
            (
                "bucket/root/115/122/_b/g0191114d30fd78b89fae8e5c88327725_v2.parquet",
                "0191114d30fd78b89fae8e5c88327725",
            ),
            (
                "bucket/root/115/122/_b/0191114d30fd78b89fae8e5c88327725_v2.parquet",
                "0191114d30fd78b89fae8e5c88327725",
            ),
        ];

        for (input, expected) in test_cases {
            assert_eq!(try_extract_uuid_str_from_path(input).unwrap(), expected);
        }
    }

    #[test]
    fn test_is_possible_non_standard_decimal_block() {
        let test_cases = vec![
            (
                // stem of block path starts with 'g', should not contain non-standard decimals
                "bucket/root/115/122/_b/g0191114d30fd78b89fae8e5c88327725_v2.parquet",
                false,
            ),
            (
                "bucket/root/115/122/_b/0191114d30fd78b89fae8e5c88327725_v2.parquet",
                true,
            ),
        ];

        for (input, expected) in test_cases {
            assert_eq!(
                is_possible_non_standard_decimal_block(input).unwrap(),
                expected
            );
        }
    }
}
