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

use databend_common_catalog::table_args::TableArgs;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_storage::StageFilesInfo;
use databend_common_storages_fuse::table_functions::string_value;

#[derive(Clone)]
pub(crate) struct ListStageArgsParsed {
    pub(crate) location: String,
    pub(crate) files_info: StageFilesInfo,
}

impl ListStageArgsParsed {
    pub fn parse(table_args: &TableArgs) -> Result<Self> {
        let args = table_args.expect_all_named("list_stage")?;

        let mut location = None;
        let mut files_info = StageFilesInfo {
            path: "".to_string(),
            files: None,
            pattern: None,
            start_after: None,
        };

        for (k, v) in &args {
            match k.to_lowercase().as_str() {
                "location" => {
                    let v = string_value(v)?;
                    if let Some(name) = v.strip_prefix('@') {
                        location = Some(name.to_string());
                    } else {
                        return Err(ErrorCode::BadArguments(format!(
                            "location must start with @, but got {}",
                            v
                        )));
                    }
                }
                "pattern" => {
                    files_info.pattern = Some(string_value(v)?);
                }
                "start_after" => {
                    files_info.start_after = Some(string_value(v)?);
                }
                _ => {
                    return Err(ErrorCode::BadArguments(format!(
                        "unknown param {} for list_stage",
                        k
                    )));
                }
            }
        }

        let location =
            location.ok_or_else(|| ErrorCode::BadArguments("list_stage must specify location"))?;

        Ok(Self {
            location,
            files_info,
        })
    }
}
