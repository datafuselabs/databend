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

mod http_query_handlers;
mod query;
mod session;
mod stage;
pub mod string_block;
mod suggestions;

pub use http_query_handlers::make_final_uri;
pub use http_query_handlers::make_page_uri;
pub use http_query_handlers::make_state_uri;
pub use http_query_handlers::query_route;
pub use http_query_handlers::QueryError;
pub use http_query_handlers::QueryResponse;
pub use http_query_handlers::QueryStats;
pub use query::ExecuteStateKind;
pub use query::ExpiringMap;
pub use query::ExpiringState;
pub use query::HttpQueryContext;
pub use query::HttpQueryManager;
pub use query::HttpSessionConf;
pub use session::login_handler::login_handler;
pub use session::login_handler::LoginResponse;
pub use session::renew_handler::renew_handler;
pub use session::renew_handler::RenewResponse;
pub(crate) use session::SessionClaim;
pub use session::TokenManager;
pub use stage::upload_to_stage;
pub use stage::UploadToStageResponse;
pub(crate) use string_block::StringBlock;
pub use suggestions::list_suggestions;
pub use suggestions::SuggestionsResponse;

pub use crate::servers::http::clickhouse_handler::clickhouse_router;
