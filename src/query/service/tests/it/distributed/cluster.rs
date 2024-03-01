//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::thread;

use databend_common_base::base::tokio;
use databend_common_config::InnerConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_query::api::RpcService;
use databend_query::test_kits::*;
use futures_util::TryStreamExt;
use tokio::runtime::Builder as TokioRuntimeBuilder;

#[test]
fn test_simple_cluster() -> Result<()> {
    let configs = setup_node_configs(vec![
        "0.0.0.0:6061", // Node 1 flight address
        "0.0.0.0:6062", // Node 2 flight address
        "0.0.0.0:6063", // Node 3 flight address
        "0.0.0.0:6064", // Node 4 flight address
        "0.0.0.0:6065", // Node 5 flight address
    ]);

    let task_count = configs.len();
    let mut handles = Vec::with_capacity(task_count);

    let cluster_desc = setup_cluster(&configs);

    for (i, conf) in configs.into_iter().enumerate() {
        let thread_name = format!("custom-thread-node-{}", i + 1);
        let is_check_node = i == task_count - 1; // Make the last node the special one

        let conf_clone = conf.clone(); // Clone the configuration as well
        let cluster_desc_clone = cluster_desc.clone();

        let handle = thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                let rt = TokioRuntimeBuilder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Failed to create runtime");

                let inner_async = async move {
                    let fixture = TestFixture::setup_with_config(&conf_clone).await?;

                    let mut srv = RpcService::create(&conf_clone)?;
                    srv.start(conf_clone.query.flight_api_address.parse()?)
                        .await?;

                    if is_check_node {
                        // Create the ctx with cluster nodes.
                        let ctx = fixture
                            .new_query_ctx_with_cluster(cluster_desc_clone)
                            .await?;

                        // Check the cluster table.
                        {
                            let res = execute_query(
                                ctx.clone(),
                                "select name, host, port from system.clusters",
                            )
                            .await?;
                            let blocks = res.try_collect::<Vec<DataBlock>>().await?;
                            let expected = vec![
                                "+----------+-----------+----------+",
                                "| Column 0 | Column 1  | Column 2 |",
                                "+----------+-----------+----------+",
                                "| 'node1'  | '0.0.0.0' | 6061     |",
                                "| 'node2'  | '0.0.0.0' | 6062     |",
                                "| 'node3'  | '0.0.0.0' | 6063     |",
                                "| 'node4'  | '0.0.0.0' | 6064     |",
                                "| 'node5'  | '0.0.0.0' | 6065     |",
                                "+----------+-----------+----------+",
                            ];
                            databend_common_expression::block_debug::assert_blocks_sorted_eq(
                                expected,
                                blocks.as_slice(),
                            );
                        }
                    }

                    Ok::<(), ErrorCode>(())
                };

                if let Err(e) = rt.block_on(inner_async) {
                    eprintln!("Error in async block: {}", e);
                }
            })
            .map_err(|e| ErrorCode::UnknownException(format!("Failed to spawn thread: {}", e)))?;

        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Thread failed to complete");
    }

    Ok(())
}

/// Setup the configurations for the nodes in the cluster.
fn setup_node_configs(addresses: Vec<&str>) -> Vec<InnerConfig> {
    addresses
        .into_iter()
        .enumerate()
        .map(|(i, address)| {
            let mut conf = ConfigBuilder::create().build();
            conf.query.flight_api_address = address.to_string();
            conf.query.cluster_id = format!("node{}", i + 1);
            conf
        })
        .collect()
}

/// Setup the cluster descriptor for the nodes in the cluster.
fn setup_cluster(configs: &[InnerConfig]) -> ClusterDescriptor {
    let mut cluster_desc = ClusterDescriptor::new();
    for conf in configs.iter() {
        cluster_desc =
            cluster_desc.with_node(&conf.query.cluster_id, &conf.query.flight_api_address);
    }
    cluster_desc
}
