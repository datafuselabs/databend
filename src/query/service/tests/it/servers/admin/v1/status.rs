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

use databend_common_base::base::tokio;
use databend_common_exception::Result;
use databend_common_meta_app::principal::UserIdentity;
use databend_common_meta_app::tenant::Tenant;
use databend_common_users::UserApiProvider;
use databend_query::interpreters::interpreter_plan_sql;
use databend_query::interpreters::Interpreter;
use databend_query::interpreters::InterpreterFactory;
use databend_query::servers::admin::v1::instance_status::instance_status_handler;
use databend_query::servers::admin::v1::instance_status::InstanceStatus;
use databend_query::sessions::QueryContext;
use databend_query::sessions::SessionManager;
use databend_query::sessions::SessionType;
use databend_query::test_kits::*;
use http::header;
use http::Method;
use http::StatusCode;
use http::Uri;
use poem::get;
use poem::Endpoint;
use poem::Request;
use poem::Route;
use pretty_assertions::assert_eq;
use tokio_stream::StreamExt;

async fn get_status(ep: &Route) -> InstanceStatus {
    let response = ep
        .call(
            Request::builder()
                .uri(Uri::from_static("/v1/status"))
                .header(header::CONTENT_TYPE, "application/json")
                .method(Method::GET)
                .finish(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().into_vec().await.unwrap();
    serde_json::from_str::<InstanceStatus>(&String::from_utf8_lossy(&body)).unwrap()
}

async fn run_query(query_ctx: &Arc<QueryContext>) -> Result<Arc<dyn Interpreter>> {
    let sql = "select sleep(3) from numbers(1)";
    let user = UserApiProvider::instance()
        .get_user(&Tenant::new_literal("test"), UserIdentity::new("root"))
        .await?;
    query_ctx
        .get_current_session()
        .set_authed_user(user, None)
        .await?;
    let (plan, _) = interpreter_plan_sql(query_ctx.clone(), sql).await?;

    InterpreterFactory::get(query_ctx.clone(), &plan).await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_status() -> Result<()> {
    let fixture = TestFixture::setup().await?;

    let ep = Route::new().at("/v1/status", get(instance_status_handler));
    let status = get_status(&ep).await;
    assert_eq!(
        (
            status.running_queries_count,
            status.last_query_started_at.is_some(),
            status.last_query_finished_at.is_some(),
        ),
        (0, false, false),
        "before running"
    );

    let http_session = fixture
        .new_session_with_type(SessionType::HTTPQuery)
        .await?;
    let query_ctx = http_session.create_query_context().await?;

    {
        let interpreter = run_query(&query_ctx).await?;
        let mut stream = interpreter.execute(query_ctx.clone()).await?;
        let status = get_status(&ep).await;
        assert_eq!(
            (
                status.running_queries_count,
                status.last_query_started_at.is_some(),
                status.last_query_finished_at.is_some(),
            ),
            (1, true, false),
            "running"
        );

        while (stream.next().await).is_some() {}
    }
    let session = query_ctx.get_current_session();
    SessionManager::instance().destroy_session(&session.get_id());

    let status = get_status(&ep).await;
    assert_eq!(
        (
            status.running_queries_count,
            status.last_query_started_at.is_some(),
            status.last_query_finished_at.is_some(),
        ),
        (0, true, true),
        "finished"
    );

    Ok(())
}
