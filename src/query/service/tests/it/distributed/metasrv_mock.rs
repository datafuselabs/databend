// Copyright 2021 Datafuse Labs.
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

use std::fmt;
use std::fs;

use common_base::base::tokio;
use common_base::base::tokio::time::sleep;
use common_base::base::GlobalSequence;
use common_base::base::Stoppable;
use common_meta_sled_store::init_sled_db;
use databend_meta::api::GrpcServer;
use databend_meta::configs;
use databend_meta::meta_service::MetaNode;
use log::info;
use log::warn;

/// MetaSrv mock for testing purpose.
/// Not embedded metasrv, but a real metasrv service.
pub struct MetaSrvMock {
    // /// To hold a per-case logging guard
    // logging_guard: (WorkerGuard, DefaultGuard),
    pub config: configs::Config,

    pub grpc_srv: Option<Box<GrpcServer>>,
}

impl Drop for MetaSrvMock {
    fn drop(&mut self) {
        self.rm_raft_dir("Drop MetaSrvTestContext");
    }
}

pub fn next_port() -> u32 {
    39000u32 + (GlobalSequence::next() as u32)
}

impl MetaSrvMock {
    pub async fn start() -> Self {
        let mut tc = MetaSrvMock::new(0);
        let config = tc.config.clone();

        let mn = MetaNode::start(&config).await.unwrap();
        let _ = mn
            .join_cluster(&config.raft_config, config.grpc_api_advertise_address())
            .await
            .unwrap();

        let mut srv = GrpcServer::create(config.clone(), mn);
        srv.start().await.unwrap();
        sleep(tokio::time::Duration::from_secs(5)).await;

        tc.grpc_srv = Some(Box::new(srv));

        tc
    }

    /// Create a new Config for test, with unique port assigned
    pub fn new(id: u64) -> MetaSrvMock {
        let config_id = next_port();

        let mut config = configs::Config::default();

        // On mac File::sync_all() takes 10 ms ~ 30 ms, 500 ms at worst, which very likely to fail a test.
        if cfg!(target_os = "macos") {
            warn!("Disabled fsync for meta data tests. fsync on mac is quite slow");
            config.raft_config.no_sync = true;
        }

        config.raft_config.id = id;

        config.raft_config.config_id = config_id.to_string();

        // Use a unique dir for each test case.
        config.raft_config.raft_dir = format!("{}-{}", config.raft_config.raft_dir, config_id);

        // By default, create a meta node instead of open an existent one.
        config.raft_config.single = true;

        config.raft_config.raft_api_port = config_id;

        // when running unit tests, set raft_listen_host to "127.0.0.1" and raft_advertise_host to localhost,
        // so if something wrong in raft meta nodes communication we will catch bug in unit tests.
        config.raft_config.raft_listen_host = "127.0.0.1".to_string();
        config.raft_config.raft_advertise_host = "localhost".to_string();

        let host = "127.0.0.1";

        // We use a single sled db for all unit test. Every unit test need a unique prefix so that it opens different tree.
        config.raft_config.sled_tree_prefix = format!("test-{}-", config_id);

        {
            let grpc_port = next_port();
            config.grpc_api_address = format!("{}:{}", host, grpc_port);
            config.grpc_api_advertise_host = Some(host.to_string());
        }

        {
            let http_port = next_port();
            config.admin_api_address = format!("{}:{}", host, http_port);
        }

        info!("new test context config: {:?}", config);

        // Init the sled db.
        init_sled_db(config.raft_config.raft_dir.clone());

        let c = MetaSrvMock {
            config,
            grpc_srv: None,
        };

        c
    }

    pub fn rm_raft_dir(&self, msg: impl fmt::Display + Copy) {
        let raft_dir = &self.config.raft_config.raft_dir;

        info!("{}: about to remove raft_dir: {:?}", msg, raft_dir);

        let res = fs::remove_dir_all(raft_dir);
        if let Err(e) = res {
            warn!("{}: can not remove raft_dir {:?}, {:?}", msg, raft_dir, e);
        } else {
            info!("{}: OK removed raft_dir {:?}", msg, raft_dir)
        }
    }
}
