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

use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::DatabaseInfo;

use crate::databases::default::DefaultDatabase;
use crate::databases::Database;
use crate::databases::DatabaseContext;

pub struct DatabaseFactory {}

impl DatabaseFactory {
    pub fn create(_: InnerConfig) -> Self {
        DatabaseFactory {}
    }

    pub fn build_database_by_engine(
        &self,
        ctx: DatabaseContext,
        db_info: &DatabaseInfo,
    ) -> Result<Arc<dyn Database>> {
        let db_engine = db_info.engine();

        let engine = if db_engine.is_empty() {
            "DEFAULT".to_string()
        } else {
            db_engine.to_uppercase()
        };

        let db = match engine.as_str() {
            DefaultDatabase::NAME => DefaultDatabase::try_create(ctx, db_info.clone())?,

            _ => {
                let err =
                    ErrorCode::UnknownDatabaseEngine(format!("Unknown database engine {}", engine));
                return Err(err);
            }
        };
        Ok(db.into())
    }
}
