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

use databend_common_meta_app::share::TableInfoMap;
use models::Credentials;
use models::RequestFile;
use poem::error::Result as PoemResult;
use poem::web::Json;
use poem::web::Path;

use crate::models;
use crate::models::PresignFileResponse;
use crate::models::ShareSpec;

#[poem::handler]
#[async_backtrace::framed]
pub async fn share_table_presign_files(
    _credentials: &Credentials,
    Path((_tenant_id, _share_name, _table_name)): Path<(String, String, String)>,
    Json(_request_files): Json<Vec<RequestFile>>,
) -> PoemResult<Json<Vec<PresignFileResponse>>> {
    unimplemented!()
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn share_table_meta(
    _credentials: &Credentials,
    Path((_tenant_id, _share_name)): Path<(String, String)>,
    Json(_request_tables): Json<Vec<String>>,
) -> PoemResult<Json<TableInfoMap>> {
    unimplemented!()
}

#[poem::handler]
#[async_backtrace::framed]
pub async fn share_spec(
    _credentials: &Credentials,
    Path(_tenant_id): Path<String>,
    Json(_request_tables): Json<Vec<String>>,
) -> PoemResult<Json<Vec<ShareSpec>>> {
    unimplemented!()
}
