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

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use databend_common_base::headers::HEADER_DEDUPLICATE_LABEL;
use databend_common_base::headers::HEADER_NODE_ID;
use databend_common_base::headers::HEADER_QUERY_ID;
use databend_common_base::headers::HEADER_TENANT;
use databend_common_base::runtime::ThreadTracker;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::user_token::TokenType;
use databend_common_meta_app::tenant::Tenant;
use databend_common_metrics::http::metrics_incr_http_request_count;
use databend_common_metrics::http::metrics_incr_http_response_panics_count;
use databend_common_metrics::http::metrics_incr_http_slow_request_count;
use databend_common_metrics::http::metrics_observe_http_response_duration;
use databend_common_storages_fuse::TableContext;
use fastrace::func_name;
use headers::authorization::Basic;
use headers::authorization::Bearer;
use headers::authorization::Credentials;
use http::header::AUTHORIZATION;
use http::HeaderMap;
use http::HeaderValue;
use http::StatusCode;
use log::error;
use log::warn;
use opentelemetry::baggage::BaggageExt;
use opentelemetry::propagation::Extractor;
use opentelemetry::propagation::TextMapPropagator;
use opentelemetry_sdk::propagation::BaggagePropagator;
use poem::error::Error as PoemError;
use poem::error::Result as PoemResult;
use poem::Addr;
use poem::Body;
use poem::Endpoint;
use poem::IntoResponse;
use poem::Middleware;
use poem::Request;
use poem::Response;
use uuid::Uuid;

use super::v1::HttpQueryContext;
use super::v1::SessionClaim;
use crate::auth::AuthMgr;
use crate::auth::Credential;
use crate::servers::HttpHandlerKind;
use crate::sessions::SessionManager;
use crate::sessions::SessionType;

const USER_AGENT: &str = "User-Agent";
const TRACE_PARENT: &str = "traceparent";

pub struct HTTPSessionMiddleware {
    pub kind: HttpHandlerKind,
    pub auth_manager: Arc<AuthMgr>,
}

impl HTTPSessionMiddleware {
    pub fn create(kind: HttpHandlerKind, auth_manager: Arc<AuthMgr>) -> HTTPSessionMiddleware {
        HTTPSessionMiddleware { kind, auth_manager }
    }
}

pub struct HeaderExtractor<'a>(pub &'a http::HeaderMap);
impl<'a> Extractor for HeaderExtractor<'a> {
    /// Get a value for a key from the HeaderMap.  If the value is not valid ASCII, returns None.
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|value| value.to_str().ok())
    }

    /// Collect all the keys from the HeaderMap.
    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .map(|value| value.as_str())
            .collect::<Vec<_>>()
    }
}

fn extract_baggage_from_headers(headers: &HeaderMap) -> Option<Vec<(String, String)>> {
    headers.get("baggage")?;
    let propagator = BaggagePropagator::new();
    let extractor = HeaderExtractor(headers);
    let result: Vec<(String, String)> = {
        let context = propagator.extract(&extractor);
        let baggage = context.baggage();
        baggage
            .iter()
            .map(|(key, (value, _metadata))| (key.to_string(), value.to_string()))
            .collect()
    };
    Some(result)
}

fn get_credential(req: &Request, kind: HttpHandlerKind) -> Result<Credential> {
    let std_auth_headers: Vec<_> = req.headers().get_all(AUTHORIZATION).iter().collect();
    if std_auth_headers.len() > 1 {
        let msg = &format!("Multiple {} headers detected", AUTHORIZATION);
        return Err(ErrorCode::AuthenticateFailure(msg));
    }
    let client_ip = get_client_ip(req);
    if std_auth_headers.is_empty() {
        if matches!(kind, HttpHandlerKind::Clickhouse) {
            auth_clickhouse_name_password(req, client_ip)
        } else {
            Err(ErrorCode::AuthenticateFailure(
                "No authorization header detected",
            ))
        }
    } else {
        auth_by_header(&std_auth_headers, client_ip, req.uri().path())
    }
}

/// this function tries to get the client IP address from the headers. if the ip in header
/// not found, fallback to the remote address, which might be local proxy's ip address.
/// please note that when it comes with network policy, we need make sure the incoming
/// traffic comes from a trustworthy proxy instance.
pub fn get_client_ip(req: &Request) -> Option<String> {
    let headers = ["X-Real-IP", "X-Forwarded-For", "CF-Connecting-IP"];
    for &header in headers.iter() {
        if let Some(value) = req.headers().get(header) {
            if let Ok(mut ip_str) = value.to_str() {
                if header == "X-Forwarded-For" {
                    ip_str = ip_str.split(',').next().unwrap_or("");
                }
                return Some(ip_str.to_string());
            }
        }
    }

    // fallback to the connection's remote address, take care
    let client_ip = match req.remote_addr().0 {
        Addr::SocketAddr(addr) => Some(addr.ip().to_string()),
        Addr::Custom(..) => Some("127.0.0.1".to_string()),
        _ => None,
    };

    client_ip
}

fn auth_by_header(
    std_auth_headers: &[&HeaderValue],
    client_ip: Option<String>,
    path: &str,
) -> Result<Credential> {
    let value = &std_auth_headers[0];
    if value.as_bytes().starts_with(b"Basic ") {
        match Basic::decode(value) {
            Some(basic) => {
                let name = basic.username().to_string();
                let password = basic.password().to_owned().as_bytes().to_vec();
                let password = (!password.is_empty()).then_some(password);
                let c = Credential::Password {
                    name,
                    password,
                    client_ip,
                };
                Ok(c)
            }
            None => Err(ErrorCode::AuthenticateFailure("bad Basic auth header")),
        }
    } else if value.as_bytes().starts_with(b"Bearer ") {
        match Bearer::decode(value) {
            Some(bearer) => {
                let token = bearer.token().to_string();
                if SessionClaim::is_databend_token(&token) {
                    let (token_type, set_user) = if path == "/query" {
                        (TokenType::Session, true)
                    } else if path == "/session/renew" {
                        (TokenType::Refresh, true)
                    } else if path != "/session/login" {
                        (TokenType::Session, false)
                    } else {
                        return Err(ErrorCode::AuthenticateFailure(format!(
                            "should not use databend auth when accessing {path}"
                        )));
                    };
                    Ok(Credential::Databend {
                        token,
                        token_type,
                        set_user,
                    })
                } else {
                    Ok(Credential::Jwt { token, client_ip })
                }
            }
            None => Err(ErrorCode::AuthenticateFailure("bad Bearer auth header")),
        }
    } else {
        Err(ErrorCode::AuthenticateFailure("bad auth header"))
    }
}

fn auth_clickhouse_name_password(req: &Request, client_ip: Option<String>) -> Result<Credential> {
    let (user, key) = (
        req.headers().get("X-CLICKHOUSE-USER"),
        req.headers().get("X-CLICKHOUSE-KEY"),
    );
    if let (Some(name), Some(password)) = (user, key) {
        let c = Credential::Password {
            name: String::from_utf8(name.as_bytes().to_vec()).unwrap(),
            password: Some(password.as_bytes().to_vec()),
            client_ip,
        };
        Ok(c)
    } else {
        let query_str = req.uri().query().unwrap_or_default();
        let query_params = serde_urlencoded::from_str::<HashMap<String, String>>(query_str)
            .map_err(|e| ErrorCode::BadArguments(format!("{}", e)))?;
        let (user, key) = (query_params.get("user"), query_params.get("password"));
        if let (Some(name), Some(password)) = (user, key) {
            Ok(Credential::Password {
                name: name.clone(),
                password: Some(password.as_bytes().to_vec()),
                client_ip,
            })
        } else {
            Err(ErrorCode::AuthenticateFailure(
                "No header or query parameters for authorization detected",
            ))
        }
    }
}

impl<E: Endpoint> Middleware<E> for HTTPSessionMiddleware {
    type Output = HTTPSessionEndpoint<E>;
    fn transform(&self, ep: E) -> Self::Output {
        HTTPSessionEndpoint {
            ep,
            kind: self.kind,
            auth_manager: self.auth_manager.clone(),
        }
    }
}

pub struct HTTPSessionEndpoint<E> {
    ep: E,
    pub kind: HttpHandlerKind,
    pub auth_manager: Arc<AuthMgr>,
}

impl<E> HTTPSessionEndpoint<E> {
    #[async_backtrace::framed]
    async fn auth(&self, req: &Request, query_id: String) -> Result<HttpQueryContext> {
        let credential = get_credential(req, self.kind)?;

        let session_manager = SessionManager::instance();

        let mut session = session_manager.create_session(SessionType::Dummy).await?;

        if let Some(tenant_id) = req.headers().get(HEADER_TENANT) {
            let tenant_id = tenant_id.to_str().unwrap().to_string();
            let tenant = Tenant::new_or_err(tenant_id.clone(), func_name!())?;
            session.set_current_tenant(tenant);
        }

        self.auth_manager.auth(&mut session, &credential).await?;
        let databend_token = match credential {
            Credential::Databend { token, .. } => Some(token),
            _ => None,
        };

        let session = session_manager.register_session(session)?;

        let deduplicate_label = req
            .headers()
            .get(HEADER_DEDUPLICATE_LABEL)
            .map(|id| id.to_str().unwrap().to_string());

        let user_agent = req
            .headers()
            .get(USER_AGENT)
            .map(|id| id.to_str().unwrap().to_string());

        let expected_node_id = req
            .headers()
            .get(HEADER_NODE_ID)
            .map(|id| id.to_str().unwrap().to_string());

        let trace_parent = req
            .headers()
            .get(TRACE_PARENT)
            .map(|id| id.to_str().unwrap().to_string());
        let opentelemetry_baggage = extract_baggage_from_headers(req.headers());
        let client_host = get_client_ip(req);

        let ctx = session.create_query_context().await?;
        let node_id = ctx.get_cluster().local_id.clone();

        Ok(HttpQueryContext {
            session,
            query_id,
            node_id,
            expected_node_id,
            deduplicate_label,
            user_agent,
            trace_parent,
            opentelemetry_baggage,
            http_method: req.method().to_string(),
            uri: req.uri().to_string(),
            client_host,
            databend_token,
        })
    }
}

impl<E: Endpoint> Endpoint for HTTPSessionEndpoint<E> {
    type Output = Response;

    #[async_backtrace::framed]
    async fn call(&self, mut req: Request) -> PoemResult<Self::Output> {
        let method = req.method().clone();
        let uri = req.uri().clone();
        let headers = req.headers().clone();

        let query_id = req
            .headers()
            .get(HEADER_QUERY_ID)
            .map(|id| id.to_str().unwrap().to_string())
            .unwrap_or_else(|| Uuid::new_v4().to_string());

        let mut tracking_payload = ThreadTracker::new_tracking_payload();
        tracking_payload.query_id = Some(query_id.clone());
        let _guard = ThreadTracker::tracking(tracking_payload);

        ThreadTracker::tracking_future(async move {
            let res = match self.auth(&req, query_id).await {
                Ok(ctx) => {
                    req.extensions_mut().insert(ctx);
                    self.ep.call(req).await
                }
                Err(err) => match err.code() {
                    ErrorCode::AUTHENTICATE_FAILURE
                    | ErrorCode::SESSION_TOKEN_EXPIRED
                    | ErrorCode::SESSION_TOKEN_NOT_FOUND
                    | ErrorCode::REFRESH_TOKEN_EXPIRED
                    | ErrorCode::REFRESH_TOKEN_NOT_FOUND
                    | ErrorCode::UNKNOWN_USER => {
                        warn!(
                            "http auth failure: {method} {uri}, headers={:?}, error={}",
                            sanitize_request_headers(&headers),
                            err
                        );
                        Err(PoemError::from_string(
                            err.message(),
                            StatusCode::UNAUTHORIZED,
                        ))
                    }
                    _ => {
                        error!(
                            "http request err: {method} {uri}, headers={:?}, error={}",
                            sanitize_request_headers(&headers),
                            err
                        );
                        Err(PoemError::from_string(
                            err.message(),
                            StatusCode::INTERNAL_SERVER_ERROR,
                        ))
                    }
                },
            };
            match res {
                Err(err) => {
                    let body = Body::from_json(serde_json::json!({
                        "error": {
                            "code": err.status().as_str(),
                            "message": err.to_string(),
                        }
                    }))
                    .unwrap();
                    Ok(Response::builder().status(err.status()).body(body))
                }
                Ok(res) => Ok(res.into_response()),
            }
        })
        .await
    }
}

pub fn sanitize_request_headers(headers: &poem::http::HeaderMap) -> HashMap<String, String> {
    let sensitive_headers = ["authorization", "x-clickhouse-key", "cookie"];
    headers
        .iter()
        .map(|(k, v)| {
            let k = k.as_str().to_lowercase();
            if sensitive_headers.contains(&k.as_str()) {
                (k, "******".to_string())
            } else {
                (k, v.to_str().unwrap_or_default().to_string())
            }
        })
        .collect()
}

pub struct MetricsMiddleware {
    api: String,
}

impl MetricsMiddleware {
    pub fn new(api: impl Into<String>) -> Self {
        Self { api: api.into() }
    }
}

impl<E: Endpoint> Middleware<E> for MetricsMiddleware {
    type Output = MetricsMiddlewareEndpoint<E>;

    fn transform(&self, ep: E) -> Self::Output {
        MetricsMiddlewareEndpoint {
            ep,
            api: self.api.clone(),
        }
    }
}

pub struct MetricsMiddlewareEndpoint<E> {
    api: String,
    ep: E,
}

impl<E: Endpoint> Endpoint for MetricsMiddlewareEndpoint<E> {
    type Output = Response;

    async fn call(&self, req: Request) -> poem::error::Result<Self::Output> {
        let start_time = Instant::now();
        let method = req.method().to_string();
        let output = self.ep.call(req).await?;
        let resp = output.into_response();
        let status_code = resp.status().to_string();
        let duration = start_time.elapsed();
        metrics_incr_http_request_count(method.clone(), self.api.clone(), status_code.clone());
        metrics_observe_http_response_duration(method.clone(), self.api.clone(), duration);
        if duration.as_secs_f64() > 60.0 {
            // TODO: replace this into histogram
            metrics_incr_http_slow_request_count(method, self.api.clone(), status_code);
        }
        Ok(resp)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PanicHandler {}

impl PanicHandler {
    pub fn new() -> Self {
        Self {}
    }
}

impl poem::middleware::PanicHandler for PanicHandler {
    type Response = (StatusCode, &'static str);

    fn get_response(&self, _err: Box<dyn Any + Send + 'static>) -> Self::Response {
        metrics_incr_http_response_panics_count();
        (StatusCode::INTERNAL_SERVER_ERROR, "internal server error")
    }
}
