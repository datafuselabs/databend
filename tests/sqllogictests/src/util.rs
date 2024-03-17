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

use std::path::Path;
use std::path::PathBuf;

use clap::Parser;
use walkdir::DirEntry;
use walkdir::WalkDir;

use crate::arg::SqlLogicTestArgs;
use crate::error::DSqlLogicTestError;
use crate::error::Result;
#[derive(Deserialize, Serialize, Debug, Clone, PartialEq)]
pub struct ServerInfo {
    pub id: String,
    pub start_time: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct HttpSessionConf {
    pub database: Option<String>,
    pub role: Option<String>,
    pub secondary_roles: Option<Vec<String>>,
    pub settings: Option<BTreeMap<String, String>>,
    pub txn_state: Option<String>,
    pub last_server_info: Option<ServerInfo>,
    #[serde(default)]
    pub last_query_ids: Vec<String>,
}

pub fn parser_rows(rows: &Value) -> Result<Vec<Vec<String>>> {
    let mut parsed_rows = Vec::new();
    for row in rows.as_array().unwrap() {
        let mut parsed_row = Vec::new();
        for col in row.as_array().unwrap() {
            let mut cell = col.as_str().unwrap();
            if cell == "inf" {
                cell = "Infinity";
            }
            if cell == "nan" {
                cell = "NaN";
            }
            // If the result is empty, we'll use `(empty)` to mark it explicitly to avoid confusion
            if cell.is_empty() {
                parsed_row.push("(empty)".to_string());
            } else {
                parsed_row.push(cell.to_string());
            }
        }
        parsed_rows.push(parsed_row);
    }
    Ok(parsed_rows)
}

fn find_specific_dir(dir: &str, suit: PathBuf) -> Result<DirEntry> {
    for entry in WalkDir::new(suit)
        .min_depth(0)
        .max_depth(100)
        .sort_by(|a, b| a.file_name().cmp(b.file_name()))
        .into_iter()
    {
        let e = entry.as_ref().unwrap();
        if e.file_type().is_dir() && e.file_name().to_str().unwrap() == dir {
            return Ok(entry?);
        }
    }
    Err(DSqlLogicTestError::SelfError(
        "Didn't find specific dir".to_string(),
    ))
}

pub fn get_files(suit: PathBuf) -> Result<Vec<walkdir::Result<DirEntry>>> {
    let args = SqlLogicTestArgs::parse();
    let mut files = vec![];

    let dirs = match args.dir {
        Some(ref dir) => {
            // Find specific dir
            let dir_entry = find_specific_dir(dir, suit);
            match dir_entry {
                Ok(dir_entry) => Some(dir_entry.into_path()),
                // If didn't find specific dir, return empty vec
                Err(_) => None,
            }
        }
        None => Some(suit),
    };
    let target = match dirs {
        Some(dir) => dir,
        None => return Ok(vec![]),
    };
    for entry in WalkDir::new(target)
        .min_depth(0)
        .max_depth(100)
        .sort_by(|a, b| a.file_name().cmp(b.file_name()))
        .into_iter()
        .filter_entry(|e| {
            if let Some(skipped_dir) = &args.skipped_dir {
                let dirs = skipped_dir.split(',').collect::<Vec<&str>>();
                if dirs.contains(&e.file_name().to_str().unwrap()) {
                    return false;
                }
            }
            true
        })
        .filter(|e| !e.as_ref().unwrap().file_type().is_dir())
    {
        files.push(entry);
    }
    Ok(files)
}

static PREPARE_TPCH: std::sync::Once = std::sync::Once::new();
static PREPARE_TPCDS: std::sync::Once = std::sync::Once::new();
static PREPARE_STAGE: std::sync::Once = std::sync::Once::new();
static PREPARE_SPILL: std::sync::Once = std::sync::Once::new();

pub fn lazy_prepare_data(file_path: &Path) -> Result<()> {
    let file_path = file_path.to_str().unwrap_or_default();
    if file_path.contains("tpch/") {
        PREPARE_TPCH.call_once(|| {
            run_script("prepare_tpch_data.sh").unwrap();
        });
    } else if file_path.contains("tpcds/") {
        PREPARE_TPCDS.call_once(|| {
            run_script("prepare_tpcds_data.sh").unwrap();
        });
    } else if file_path.contains("stage/") || file_path.contains("stage_parquet/") {
        PREPARE_STAGE.call_once(|| {
            run_script("prepare_stage.sh").unwrap();
        });
    } else if file_path.contains("spill/") {
        PREPARE_SPILL.call_once(|| run_script("prepare_spill_data.sh").unwrap())
    }
    Ok(())
}

fn run_script(name: &str) -> Result<()> {
    let path = format!("tests/sqllogictests/scripts/{}", name);
    let output = std::process::Command::new("bash")
        .arg(path)
        .output()
        .expect("failed to execute process");
    if !output.status.success() {
        return Err(DSqlLogicTestError::SelfError(format!(
            "Failed to run {}: {}",
            name,
            String::from_utf8(output.stderr).unwrap()
        )));
    }
    Ok(())
}
