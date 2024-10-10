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
use std::fmt::Debug;
use std::fmt::Formatter;
use std::ops::Deref;
use std::sync::Arc;

use databend_common_catalog::cluster_info::Cluster;
use databend_common_catalog::query_kind::QueryKind;
use databend_common_catalog::table_context::TableContext;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_types::NodeInfo;
use databend_common_settings::Settings;
use log::debug;
use petgraph::dot::Dot;
use petgraph::graph::NodeIndex;
use petgraph::Graph;
use serde::Deserialize;
use serde::Serialize;

use crate::clusters::ClusterHelper;
use crate::clusters::FlightParams;
use crate::servers::flight::v1::actions::INIT_QUERY_ENV;
use crate::sessions::QueryContext;
use crate::sessions::SessionManager;
use crate::sessions::SessionType;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Edge {
    Statistics,
    Fragment(usize),
}

#[derive(Serialize, Deserialize)]
pub struct DataflowDiagram {
    graph: Graph<Arc<NodeInfo>, Edge>,
}

impl Deref for DataflowDiagram {
    type Target = Graph<Arc<NodeInfo>, Edge>;

    fn deref(&self) -> &Self::Target {
        &self.graph
    }
}

impl Debug for DataflowDiagram {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", &Dot::new(&self.graph))
    }
}

pub struct DataflowDiagramBuilder {
    nodes: HashMap<String, NodeIndex>,
    graph: Graph<Arc<NodeInfo>, Edge>,
}

impl DataflowDiagramBuilder {
    pub fn create(nodes: Vec<Arc<NodeInfo>>) -> DataflowDiagramBuilder {
        let mut nodes_index = HashMap::with_capacity(nodes.len());
        let mut graph = Graph::with_capacity(nodes.len(), nodes.len() * 2);

        for node in nodes {
            let node_id = node.id.clone();
            let node_index = graph.add_node(node);
            nodes_index.insert(node_id, node_index);
        }

        DataflowDiagramBuilder {
            graph,
            nodes: nodes_index,
        }
    }

    pub fn add_data_edge(&mut self, source: &str, destination: &str, v: usize) -> Result<()> {
        if source != destination {
            // avoid local to local
            let source = self
                .nodes
                .get(source)
                .ok_or_else(|| ErrorCode::NotFoundClusterNode(""))?;
            let destination = self
                .nodes
                .get(destination)
                .ok_or_else(|| ErrorCode::NotFoundClusterNode(""))?;

            self.graph
                .add_edge(*source, *destination, Edge::Fragment(v));
        }

        Ok(())
    }

    pub fn add_statistics_edge(&mut self, source: &str, destination: &str) -> Result<()> {
        if source != destination {
            // avoid local to local
            let source = self
                .nodes
                .get(source)
                .ok_or_else(|| ErrorCode::NotFoundClusterNode(""))?;
            let destination = self
                .nodes
                .get(destination)
                .ok_or_else(|| ErrorCode::NotFoundClusterNode(""))?;

            self.graph.add_edge(*source, *destination, Edge::Statistics);
        }

        Ok(())
    }

    pub fn build(self) -> DataflowDiagram {
        DataflowDiagram { graph: self.graph }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QueryEnv {
    pub query_id: String,
    pub cluster: Arc<Cluster>,
    pub settings: Arc<Settings>,
    pub query_kind: QueryKind,
    pub dataflow_diagram: Arc<DataflowDiagram>,
    pub request_server_id: String,
    pub create_rpc_clint_with_current_rt: bool,
}

impl QueryEnv {
    pub async fn init(&self, ctx: &Arc<QueryContext>, flight_params: FlightParams) -> Result<()> {
        debug!("Dataflow diagram {:?}", self.dataflow_diagram);

        let cluster = ctx.get_cluster();
        let mut message = HashMap::with_capacity(self.dataflow_diagram.node_count());

        for node in self.dataflow_diagram.node_weights() {
            message.insert(node.id.clone(), self.clone());
        }

        let _ = cluster
            .do_action::<_, ()>(INIT_QUERY_ENV, message, flight_params)
            .await?;

        Ok(())
    }

    pub async fn create_query_ctx(&self) -> Result<Arc<QueryContext>> {
        let session_manager = SessionManager::instance();

        let session = session_manager.register_session(
            session_manager.create_with_settings(SessionType::FlightRPC, self.settings.clone())?,
        )?;

        let query_ctx = session.create_query_context_with_cluster(Arc::new(Cluster {
            nodes: self.cluster.nodes.clone(),
            local_id: GlobalConfig::instance().query.node_id.clone(),
        }))?;

        query_ctx.set_id(self.query_id.clone());
        query_ctx.attach_query_str(self.query_kind, "".to_string());

        Ok(query_ctx)
    }
}
