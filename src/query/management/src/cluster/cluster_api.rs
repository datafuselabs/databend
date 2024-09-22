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

use std::collections::HashMap;

use databend_common_exception::Result;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::NodeInfo;

#[async_trait::async_trait]
pub trait ClusterApi: Sync + Send {
    // Add a new node info to /tenant/cluster_id/node-name.
    async fn add_node(&self, node: NodeInfo) -> Result<u64>;

    // Get the tenant's cluster all nodes.
    async fn get_nodes(&self) -> Result<Vec<NodeInfo>>;

    async fn get_tenant_nodes(&self) -> Result<HashMap<String, Vec<NodeInfo>>>;

    // Drop the tenant's cluster one node by node.id.
    async fn drop_node(&self, node_id: String, seq: MatchSeq) -> Result<()>;

    // Keep the tenant's cluster node alive.
    async fn heartbeat(&self, node: &NodeInfo, seq: MatchSeq) -> Result<u64>;

    async fn get_local_addr(&self) -> Result<Option<String>>;
}
