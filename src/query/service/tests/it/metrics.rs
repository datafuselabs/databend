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

use std::net::SocketAddr;

use databend_common_base::base::tokio;
use databend_common_base::runtime::metrics::register_counter;
use databend_query::metrics::MetricService;
use databend_query::servers::Server;

#[tokio::test(flavor = "multi_thread")]
async fn test_metric_server() -> databend_common_exception::Result<()> {
    let mut service = MetricService::create();
    let listening = "127.0.0.1:0".parse::<SocketAddr>()?;
    let listening = service.start(listening).await?;
    let client = reqwest::Client::builder().build().unwrap();
    let url = format!("http://{}/metrics", listening);
    let resp = client.get(url.clone()).send().await;
    assert!(resp.is_ok());
    let resp = resp.unwrap();
    assert!(resp.status().is_success());
    assert_eq!(
        resp.text().await.unwrap().find("unit_test_counter_total 1"),
        None
    );

    let test_counter = register_counter("unit_test_counter");
    test_counter.inc();

    let resp = client.get(url).send().await;
    assert!(resp.is_ok());
    let resp = resp.unwrap();
    assert!(resp.status().is_success());
    let output = resp.text().await.unwrap();
    assert!(output.contains("unit_test_counter_total 1"));

    Ok(())
}
