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

//! Test metasrv SchemaApi on a single node.

use std::sync::Arc;
use std::sync::Mutex;

use databend_common_meta_api::BackgroundApiTestSuite;
use databend_common_meta_api::SchemaApiTestSuite;
use test_harness::test;

use crate::testing::meta_service_test_harness;
use crate::tests::service::MetaSrvBuilder;

#[test(harness = meta_service_test_harness)]
#[minitrace::trace]
async fn test_meta_grpc_client_single() -> anyhow::Result<()> {
    let builder = MetaSrvBuilder {
        test_contexts: Arc::new(Mutex::new(vec![])),
    };

    SchemaApiTestSuite::test_single_node(builder.clone()).await?;

    BackgroundApiTestSuite::test_single_node(builder).await?;

    Ok(())
}

#[test(harness = meta_service_test_harness)]
#[minitrace::trace]
async fn test_meta_grpc_client_cluster() -> anyhow::Result<()> {
    let builder = MetaSrvBuilder {
        test_contexts: Arc::new(Mutex::new(vec![])),
    };

    SchemaApiTestSuite::test_cluster(builder).await?;

    Ok(())
}
