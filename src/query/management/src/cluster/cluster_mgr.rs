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

use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::time::Duration;

use databend_common_base::base::escape_for_key;
use databend_common_base::base::unescape_for_key;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_kvapi::kvapi::UpsertKVReply;
use databend_common_meta_kvapi::kvapi::UpsertKVReq;
use databend_common_meta_store::MetaStore;
use databend_common_meta_types::seq_value::SeqV;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::MetaSpec;
use databend_common_meta_types::NodeInfo;
use databend_common_meta_types::Operation;

use crate::cluster::ClusterApi;

pub static CLUSTER_API_KEY_PREFIX: &str = "__fd_clusters_v3";

pub struct ClusterMgr {
    metastore: MetaStore,
    lift_time: Duration,
    tenant_prefix: String,
    cluster_prefix: String,
}

impl ClusterMgr {
    pub fn create(
        metastore: MetaStore,
        tenant: &str,
        cluster_id: &str,
        lift_time: Duration,
    ) -> Result<Self> {
        if tenant.is_empty() {
            return Err(ErrorCode::TenantIsEmpty(
                "Tenant can not empty(while cluster mgr create)",
            ));
        }

        let tenant_prefix = format!("{}/{}", CLUSTER_API_KEY_PREFIX, escape_for_key(tenant)?);
        let cluster_prefix = format!(
            "{}/{}/databend_query",
            tenant_prefix,
            escape_for_key(cluster_id)?
        );

        Ok(ClusterMgr {
            metastore,
            lift_time,
            tenant_prefix,
            cluster_prefix,
        })
    }

    fn new_lift_time(&self) -> MetaSpec {
        MetaSpec::new_ttl(self.lift_time)
    }
}

#[async_trait::async_trait]
impl ClusterApi for ClusterMgr {
    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn add_node(&self, node: NodeInfo) -> Result<u64> {
        // Only when there are no record, i.e. seq=0
        let seq = MatchSeq::Exact(0);
        let meta = Some(self.new_lift_time());
        let value = Operation::Update(serde_json::to_vec(&node)?);
        let node_key = format!("{}/{}", self.cluster_prefix, escape_for_key(&node.id)?);
        let upsert_node = self
            .metastore
            .upsert_kv(UpsertKVReq::new(&node_key, seq, value, meta));

        let res_seq = upsert_node.await?.added_seq_or_else(|_v| {
            ErrorCode::ClusterNodeAlreadyExists(format!(
                "Node with ID '{}' already exists in the cluster.",
                node.id
            ))
        })?;

        Ok(res_seq)
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn get_nodes(&self) -> Result<Vec<NodeInfo>> {
        let values = self.metastore.prefix_list_kv(&self.cluster_prefix).await?;

        let mut nodes_info = Vec::with_capacity(values.len());
        for (node_key, value) in values {
            let mut node_info = serde_json::from_slice::<NodeInfo>(&value.data)?;

            node_info.id = unescape_for_key(&node_key[self.cluster_prefix.len() + 1..])?;
            nodes_info.push(node_info);
        }

        Ok(nodes_info)
    }

    async fn get_tenant_nodes(&self) -> Result<HashMap<String, Vec<NodeInfo>>> {
        let values = self.metastore.prefix_list_kv(&self.tenant_prefix).await?;
        let mut nodes_info = HashMap::with_capacity(12);

        for (node_key, value) in values {
            let key_parts = node_key.split('/').collect::<Vec<_>>();

            assert_eq!(key_parts.len(), 5);
            assert_eq!(key_parts[0], "__fd_clusters_v2");
            assert_eq!(key_parts[3], "databend_query");

            let mut node_info = serde_json::from_slice::<NodeInfo>(&value.data)?;
            node_info.id = unescape_for_key(key_parts[4])?;

            match nodes_info.entry(unescape_for_key(key_parts[2])?) {
                Entry::Vacant(v) => {
                    v.insert(vec![node_info]);
                }
                Entry::Occupied(mut v) => {
                    v.get_mut().push(node_info);
                }
            }
        }

        Ok(nodes_info)
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn drop_node(&self, node_id: String, seq: MatchSeq) -> Result<()> {
        let node_key = format!("{}/{}", self.cluster_prefix, escape_for_key(&node_id)?);
        let upsert_node =
            self.metastore
                .upsert_kv(UpsertKVReq::new(&node_key, seq, Operation::Delete, None));

        match upsert_node.await? {
            UpsertKVReply {
                ident: None,
                prev: Some(_),
                result: None,
            } => Ok(()),
            UpsertKVReply { .. } => Err(ErrorCode::ClusterUnknownNode(format!(
                "Node with ID '{}' does not exist in the cluster.",
                node_id
            ))),
        }
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn heartbeat(&self, node: &NodeInfo, seq: MatchSeq) -> Result<u64> {
        let meta = Some(self.new_lift_time());
        let node_key = format!("{}/{}", self.cluster_prefix, escape_for_key(&node.id)?);

        let upsert_meta =
            self.metastore
                .upsert_kv(UpsertKVReq::new(&node_key, seq, Operation::AsIs, meta));

        match upsert_meta.await? {
            UpsertKVReply {
                ident: None,
                prev: Some(_),
                result: Some(SeqV { seq: s, .. }),
            } => Ok(s),
            UpsertKVReply { .. } => self.add_node(node.clone()).await,
        }
    }

    #[async_backtrace::framed]
    #[fastrace::trace]
    async fn get_local_addr(&self) -> Result<Option<String>> {
        Ok(self.metastore.get_local_addr().await?)
    }
}
