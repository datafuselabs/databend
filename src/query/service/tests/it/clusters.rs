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

use databend_common_base::base::tokio;
use databend_common_exception::Result;
use databend_query::clusters::ClusterDiscovery;
use databend_query::clusters::ClusterHelper;
use databend_query::test_kits::*;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_single_cluster_discovery() -> Result<()> {
    let config = ConfigBuilder::create().build();
    let _fixture = TestFixture::setup_with_config(&config).await?;

    let discover_cluster = ClusterDiscovery::instance().discover(&config).await?;

    let discover_cluster_nodes = discover_cluster.get_nodes();
    assert_eq!(discover_cluster_nodes.len(), 1);
    assert!(discover_cluster.is_empty());
    assert!(discover_cluster.is_local(&discover_cluster_nodes[0]));

    Ok(())
}
// TODO: license info with cluster

// #[tokio::test(flavor = "current_thread")]
// async fn test_remove_invalid_nodes() -> Result<()> {
//     let config = ConfigBuilder::create().build();
//     let _fixture = TestFixture::setup_with_config(&config).await?;
//
//     let config_1 = ConfigBuilder::create()
//         .query_flight_address("invalid_address_1")
//         .build();
//     let config_2 = ConfigBuilder::create()
//         .query_flight_address("invalid_address_2")
//         .build();
//
//     let metastore = ClusterDiscovery::create_meta_client(&config_1).await?;
//     let cluster_discovery_1 = ClusterDiscovery::try_create(&config_1, metastore.clone()).await?;
//     let cluster_discovery_2 = ClusterDiscovery::try_create(&config_2, metastore.clone()).await?;
//
//     cluster_discovery_1.register_to_metastore(&config_1).await?;
//     cluster_discovery_2.register_to_metastore(&config_2).await?;
//
//     let discover_cluster_1 = cluster_discovery_1.discover(&config_1).await?;
//     let discover_cluster_nodes_1 = discover_cluster_1.get_nodes();
//     assert_eq!(discover_cluster_nodes_1.len(), 1);
//     assert!(discover_cluster_1.is_empty());
//     assert!(discover_cluster_1.is_local(&discover_cluster_nodes_1[0]));
//
//     let discover_cluster_2 = cluster_discovery_2.discover(&config_1).await?;
//     let discover_cluster_nodes_2 = discover_cluster_2.get_nodes();
//     assert_eq!(discover_cluster_nodes_2.len(), 1);
//     assert!(discover_cluster_2.is_empty());
//     assert!(discover_cluster_2.is_local(&discover_cluster_nodes_2[0]));
//
//     Ok(())
// }

// TODO:(Winter) need kvapi::KVApi for cluster multiple nodes test
// #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
// async fn test_multiple_cluster_discovery() -> Result<()> {
//     let mut config = Config::default();
//     config.query.tenant = String::from("tenant_id");
//     config.query.namespace = String::from("namespace_id");
//
//     let cluster_discovery_1 = ClusterDiscovery::create_global(config.clone()).await?;
//     let cluster_discovery_2 = ClusterDiscovery::create_global(config.clone()).await?;
//
//     cluster_discovery_1.register_to_metastore(&config).await?;
//     cluster_discovery_2.register_to_metastore(&config).await?;
//
//     let discover_cluster_1 = cluster_discovery_1.discover().await?;
//     let discover_cluster_2 = cluster_discovery_2.discover().await?;
//
//     let discover_cluster_nodes_1 = discover_cluster_1.get_nodes();
//     let discover_cluster_nodes_2 = discover_cluster_2.get_nodes();
//
//     assert_eq!(discover_cluster_nodes_1.len(), 2);
//     assert_eq!(discover_cluster_nodes_2.len(), 2);
//     assert_eq!(discover_cluster_1.is_empty(), false);
//     assert_eq!(discover_cluster_2.is_empty(), false);
//     assert_eq!(discover_cluster_1.is_local(&discover_cluster_nodes_1[0]) || discover_cluster_1.is_local(&discover_cluster_nodes_1[1]), true);
//     assert_eq!(discover_cluster_1.is_local(&discover_cluster_nodes_1[0]) && discover_cluster_1.is_local(&discover_cluster_nodes_1[1]), false);
//     assert_eq!(discover_cluster_2.is_local(&discover_cluster_nodes_2[0]) || discover_cluster_1.is_local(&discover_cluster_nodes_2[1]), true);
//     assert_eq!(discover_cluster_2.is_local(&discover_cluster_nodes_2[0]) && discover_cluster_1.is_local(&discover_cluster_nodes_2[1]), false);
//
//     assert_eq!(discover_cluster_nodes_1, discover_cluster_nodes_2);
//     Ok(())
// }
