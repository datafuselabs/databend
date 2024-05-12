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
use std::sync::Mutex;

use async_trait::async_trait;
use databend_common_meta_api::BackgroundApiTestSuite;
use databend_common_meta_api::SchemaApiTestSuite;
use databend_common_meta_kvapi::kvapi;
use databend_common_meta_raft_store::state_machine::StateMachine;
use test_harness::test;

use crate::testing::new_raft_test_context;
use crate::testing::raft_store_test_harness;
use crate::testing::RaftTestContext;

#[derive(Clone)]
struct StateMachineBuilder {
    test_context: Arc<Mutex<Option<RaftTestContext>>>,
}

#[async_trait]
impl kvapi::ApiBuilder<StateMachine> for StateMachineBuilder {
    async fn build(&self) -> StateMachine {
        let tc = new_raft_test_context();
        let sm = StateMachine::open(&tc.raft_config, 1).await.unwrap();
        {
            let mut x = self.test_context.lock().unwrap();
            *x = Some(tc);
        }

        sm
    }

    async fn build_cluster(&self) -> Vec<StateMachine> {
        unimplemented!("StateMachine does not support cluster mode")
    }
}

#[test(harness = raft_store_test_harness)]
#[minitrace::trace]
async fn test_meta_embedded_single() -> anyhow::Result<()> {
    let builder = StateMachineBuilder {
        test_context: Default::default(),
    };

    SchemaApiTestSuite::test_single_node(builder.clone()).await?;

    BackgroundApiTestSuite::test_single_node(builder).await?;

    Ok(())
}
