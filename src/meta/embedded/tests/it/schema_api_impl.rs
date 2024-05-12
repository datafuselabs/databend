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

use async_trait::async_trait;
use databend_common_base::base::tokio;
use databend_common_meta_api::BackgroundApiTestSuite;
use databend_common_meta_api::SchemaApiTestSuite;
use databend_common_meta_embedded::MetaEmbedded;
use databend_common_meta_kvapi::kvapi;

#[derive(Clone)]
pub struct MetaEmbeddedBuilder {}

#[async_trait]
impl kvapi::ApiBuilder<MetaEmbedded> for MetaEmbeddedBuilder {
    async fn build(&self) -> MetaEmbedded {
        MetaEmbedded::new_temp().await.unwrap()
    }

    async fn build_cluster(&self) -> Vec<MetaEmbedded> {
        unimplemented!("embedded meta does not support cluster mode")
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_meta_embedded() -> anyhow::Result<()> {
    SchemaApiTestSuite::test_single_node(MetaEmbeddedBuilder {}).await?;

    BackgroundApiTestSuite::test_single_node(MetaEmbeddedBuilder {}).await?;

    Ok(())
}
