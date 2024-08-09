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
use std::sync::Arc;
use std::time::Instant;

use http::StatusCode;
use log::warn;
use poem::FromRequest;
use poem::Request;
use poem::RequestBody;

use crate::servers::http::v1::HttpQueryManager;
use crate::sessions::Session;
use crate::sessions::SessionManager;
use crate::sessions::SessionType;

#[derive(Clone)]
pub struct HttpQueryContext {
    pub session: Arc<Session>,
    pub query_id: String,
    pub node_id: String,
    pub expected_node_id: Option<String>,
    pub deduplicate_label: Option<String>,
    pub user_agent: Option<String>,
    pub trace_parent: Option<String>,
    pub opentelemetry_baggage: Option<Vec<(String, String)>>,
    pub http_method: String,
    pub uri: String,
    pub client_host: Option<String>,
    pub databend_token: Option<String>,
}

impl HttpQueryContext {
    pub fn upgrade_session(&self, session_type: SessionType) -> Result<Arc<Session>, poem::Error> {
        SessionManager::instance()
            .try_upgrade_session(self.session.clone(), session_type.clone())
            .map_err(|err| {
                poem::Error::from_string(err.message(), poem::http::StatusCode::TOO_MANY_REQUESTS)
            })?;
        Ok(self.session.clone())
    }

    pub fn to_fastrace_properties(&self) -> BTreeMap<String, String> {
        let mut result = BTreeMap::new();
        let properties = self.session.to_fastrace_properties();
        result.extend(properties);
        result.extend([
            ("query_id".to_string(), self.query_id.clone()),
            ("node_id".to_string(), self.node_id.clone()),
            (
                "deduplicate_label".to_string(),
                self.deduplicate_label.clone().unwrap_or_default(),
            ),
            (
                "user_agent".to_string(),
                self.user_agent.clone().unwrap_or_default(),
            ),
            ("http_method".to_string(), self.http_method.clone()),
            ("uri".to_string(), self.uri.clone()),
        ]);
        if let Some(baggage) = self.opentelemetry_baggage.clone() {
            result.extend(baggage);
        }
        result
    }

    pub fn set_fail(&self) {
        self.session.txn_mgr().lock().set_fail();
    }

    pub fn check_node_id(&self, query_id: &str) -> poem::Result<()> {
        if let Some(expected_node_id) = self.expected_node_id.as_ref() {
            if expected_node_id != &self.node_id {
                let manager = HttpQueryManager::instance();
                let start_time = manager.server_info.start_time.clone();
                let uptime = (Instant::now() - manager.start_instant).as_secs_f32();
                let msg = format!(
                    "route error: query {query_id} SHOULD be on server {expected_node_id}, but current server is {}, which started at {start_time}({uptime} secs ago)",
                    self.node_id
                );
                warn!("{msg}");
                return Err(poem::Error::from_string(msg, StatusCode::NOT_FOUND));
            }
        }

        Ok(())
    }
}

impl<'a> FromRequest<'a> for &'a HttpQueryContext {
    #[async_backtrace::framed]
    async fn from_request(req: &'a Request, _body: &mut RequestBody) -> poem::Result<Self> {
        Ok(req.extensions().get::<HttpQueryContext>().expect(
            "To use the `HttpQueryContext` extractor, the `HTTPSessionMiddleware` is required",
        ))
    }
}
