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
use std::net::TcpListener;
use std::str::FromStr;
use std::sync::Arc;

use databend_common_arrow::arrow_format::flight::data::Empty;
use databend_common_arrow::arrow_format::flight::service::flight_service_client::FlightServiceClient;
use databend_common_base::base::tokio;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_grpc::ConnectionFactory;
use databend_common_grpc::GrpcConnectionError;
use databend_common_grpc::RpcClientTlsConfig;
use databend_query::api::RpcService;
use databend_query::test_kits::*;

use crate::tests::tls_constants::TEST_CA_CERT;
use crate::tests::tls_constants::TEST_CN_NAME;
use crate::tests::tls_constants::TEST_SERVER_CERT;
use crate::tests::tls_constants::TEST_SERVER_KEY;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_tls_rpc_server() -> Result<()> {
    let mut rpc_service = RpcService::create(
        &ConfigBuilder::create()
            .rpc_tls_server_key(TEST_SERVER_KEY)
            .rpc_tls_server_cert(TEST_SERVER_CERT)
            .build(),
    )?;

    let mut listener_address = SocketAddr::from_str("127.0.0.1:9991")?;
    listener_address = rpc_service.start(listener_address).await?;

    let tls_conf = Some(RpcClientTlsConfig {
        rpc_tls_server_root_ca_cert: TEST_CA_CERT.to_string(),
        domain_name: TEST_CN_NAME.to_string(),
    });

    // normal case
    let conn = ConnectionFactory::create_rpc_channel(listener_address, None, tls_conf).await?;
    let mut f_client = FlightServiceClient::new(conn);
    let r = f_client.list_actions(Empty {}).await;
    assert!(r.is_ok());

    // client access without tls enabled will be failed
    // - channel can still be created, but communication will be failed
    let channel = ConnectionFactory::create_rpc_channel(listener_address, None, None).await?;
    let mut f_client = FlightServiceClient::new(channel);
    let r = f_client.list_actions(Empty {}).await;
    assert!(r.is_err());

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_tls_rpc_server_invalid_server_config() -> Result<()> {
    // setup, invalid cert locations
    let mut srv = RpcService {
        config: ConfigBuilder::create()
            .rpc_tls_server_key("../tests/data/certs/none.key")
            .rpc_tls_server_cert("../tests/data/certs/none.pem")
            .build(),
        abort_notify: Arc::new(Default::default()),
    };
    let r = srv
        .start_with_incoming("127.0.0.1:0".parse().unwrap())
        .await;
    assert!(r.is_err());
    let e = r.unwrap_err();
    assert_eq!(e.code(), ErrorCode::TLSConfigurationFailure("").code());
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_tls_rpc_server_invalid_client_config() -> Result<()> {
    // setup, invalid cert locations
    let client_conf = RpcClientTlsConfig {
        rpc_tls_server_root_ca_cert: "../tests/data/certs/nowhere.pem".to_string(),
        domain_name: TEST_CN_NAME.to_string(),
    };

    let r = ConnectionFactory::create_rpc_channel("fake:1234", None, Some(client_conf)).await;
    assert!(r.is_err());
    let e = r.unwrap_err();
    match e {
        GrpcConnectionError::TLSConfigError { action, .. } => {
            assert_eq!("loading", action);
        }
        _ => unimplemented!("expect TLSConfigError"),
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_rpc_server_port_used() -> Result<()> {
    let listener = TcpListener::bind("0.0.0.0:0").unwrap();
    let local_socket = listener.local_addr().unwrap();

    let mut srv = RpcService {
        config: ConfigBuilder::create().build(),
        abort_notify: Arc::new(Default::default()),
    };

    let r = srv.start_with_incoming(local_socket).await;

    assert!(r.is_err());
    Ok(())
}
