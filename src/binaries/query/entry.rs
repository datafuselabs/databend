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

use std::env;
use std::time::Duration;

use databend_common_base::mem_allocator::GlobalAllocator;
use databend_common_base::runtime::set_alloc_error_hook;
use databend_common_base::runtime::GLOBAL_MEM_STAT;
use databend_common_config::Commands;
use databend_common_config::InnerConfig;
use databend_common_config::DATABEND_COMMIT_VERSION;
use databend_common_config::QUERY_GIT_SEMVER;
use databend_common_config::QUERY_GIT_SHA;
use databend_common_config::QUERY_SEMVER;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::ResultExt;
use databend_common_meta_client::MIN_METASRV_SEMVER;
use databend_common_metrics::system::set_system_version;
use databend_common_storage::DataOperator;
use databend_common_tracing::set_panic_hook;
use databend_enterprise_background_service::get_background_service_handler;
use databend_query::clusters::ClusterDiscovery;
use databend_query::local;
use databend_query::servers::admin::AdminService;
use databend_query::servers::flight::FlightService;
use databend_query::servers::metrics::MetricService;
use databend_query::servers::FlightSQLServer;
use databend_query::servers::HttpHandler;
use databend_query::servers::HttpHandlerKind;
use databend_query::servers::MySQLHandler;
use databend_query::servers::MySQLTlsConfig;
use databend_query::servers::Server;
use databend_query::servers::ShutdownHandle;
use databend_query::GlobalServices;
use log::info;

pub struct MainError;

pub async fn run_cmd(conf: &InnerConfig) -> Result<bool, MainError> {
    let make_error = || "failed to run cmd";

    match &conf.subcommand {
        None => return Ok(false),
        Some(Commands::Ver) => {
            println!("version: {}", *QUERY_SEMVER);
            println!("min-compatible-metasrv-version: {}", MIN_METASRV_SEMVER);
        }
        Some(Commands::Local {
            query,
            output_format,
        }) => local::query_local(query, output_format)
            .await
            .with_context(make_error)?,
    }

    Ok(true)
}

pub async fn init_services(conf: &InnerConfig) -> Result<(), MainError> {
    let make_error = || "failed to init services";

    set_panic_hook();
    set_alloc_error_hook();

    #[cfg(target_arch = "x86_64")]
    {
        if !std::is_x86_feature_detected!("sse4.2") {
            println!(
                "Current pre-built binary is typically compiled for x86_64 and leverage SSE 4.2 instruction set, you can build your own binary from source"
            );
            return Ok(());
        }
    }

    if conf.meta.is_embedded_meta().with_context(make_error)? {
        return Err(ErrorCode::Unimplemented(
            "Embedded meta is an deployment method and will not be supported since March 2023.",
        ))
        .with_context(make_error);
    }
    // Make sure global services have been inited.
    GlobalServices::init(conf).await.with_context(make_error)
}

async fn precheck_services(conf: &InnerConfig) -> Result<(), MainError> {
    let make_error = || "failed to precheck";

    if conf.query.max_memory_limit_enabled {
        let size = conf.query.max_server_memory_usage as i64;
        info!("Set memory limit: {}", size);
        GLOBAL_MEM_STAT.set_limit(size);
    }

    #[cfg(not(target_os = "macos"))]
    check_max_open_files();

    // Check storage enterprise features.
    DataOperator::instance()
        .check_license()
        .await
        .with_context(make_error)?;
    Ok(())
}

pub async fn start_services(conf: &InnerConfig) -> Result<(), MainError> {
    let make_error = || "failed to start service";

    precheck_services(conf).await.with_context(make_error)?;

    let mut shutdown_handle = ShutdownHandle::create().with_context(make_error)?;
    let start_time = std::time::Instant::now();

    info!("Databend Query start with config: {:?}", conf);

    // Cluster register.
    {
        ClusterDiscovery::instance()
            .register_to_metastore()
            .await
            .with_context(make_error)?;
        info!(
            "Databend query has been registered:{:?} to metasrv:{:?}.",
            conf.query.cluster_id, conf.meta.endpoints
        );
    }

    // RPC API service.
    {
        let address = conf.query.flight_api_address.clone();
        let mut srv = FlightService::create(conf.clone()).with_context(make_error)?;
        let listening = srv
            .start(address.parse().with_context(make_error)?)
            .await
            .with_context(make_error)?;
        shutdown_handle.add_service("RPCService", srv);
        info!("Listening for RPC API (interserver): {}", listening);
    }

    // MySQL handler.
    {
        let hostname = conf.query.mysql_handler_host.clone();
        let listening = format!("{}:{}", hostname, conf.query.mysql_handler_port);
        let tcp_keepalive_timeout_secs = conf.query.mysql_handler_tcp_keepalive_timeout_secs;
        let tls_config = MySQLTlsConfig::new(
            conf.query.mysql_tls_server_cert.clone(),
            conf.query.mysql_tls_server_key.clone(),
        );

        let mut handler = MySQLHandler::create(tcp_keepalive_timeout_secs, tls_config)
            .with_context(make_error)?;
        let listening = handler
            .start(listening.parse().with_context(make_error)?)
            .await
            .with_context(make_error)?;
        shutdown_handle.add_service("MySQLHandler", handler);

        info!(
            "Listening for MySQL compatibility protocol: {}, Usage: mysql -uroot -h{} -P{}",
            listening,
            listening.ip(),
            listening.port(),
        );
    }

    // ClickHouse HTTP handler.
    {
        let hostname = conf.query.clickhouse_http_handler_host.clone();
        let listening = format!("{}:{}", hostname, conf.query.clickhouse_http_handler_port);

        let mut srv = HttpHandler::create(HttpHandlerKind::Clickhouse);
        let listening = srv
            .start(listening.parse().with_context(make_error)?)
            .await
            .with_context(make_error)?;
        shutdown_handle.add_service("ClickHouseHandler", srv);

        let http_handler_usage = HttpHandlerKind::Clickhouse.usage(listening);
        info!(
            "Listening for ClickHouse compatibility http protocol: {}, Usage: {}",
            listening, http_handler_usage
        );
    }

    // Databend HTTP handler.
    {
        let hostname = conf.query.http_handler_host.clone();
        let listening = format!("{}:{}", hostname, conf.query.http_handler_port);

        let mut srv = HttpHandler::create(HttpHandlerKind::Query);
        let listening = srv
            .start(listening.parse().with_context(make_error)?)
            .await
            .with_context(make_error)?;
        shutdown_handle.add_service("DatabendHTTPHandler", srv);

        let http_handler_usage = HttpHandlerKind::Query.usage(listening);
        info!(
            "Listening for Databend HTTP API:  {}, Usage: {}",
            listening, http_handler_usage
        );
    }

    // Metric API service.
    {
        set_system_version("query", QUERY_GIT_SEMVER.as_str(), QUERY_GIT_SHA.as_str());
        let address = conf.query.metric_api_address.clone();
        let mut srv = MetricService::create();
        let listening = srv
            .start(address.parse().with_context(make_error)?)
            .await
            .with_context(make_error)?;
        shutdown_handle.add_service("MetricService", srv);
        info!("Listening for Metric API: {}/metrics", listening);
    }

    // Admin HTTP API service.
    {
        let address = conf.query.admin_api_address.clone();
        let mut srv = AdminService::create(conf);
        let listening = srv
            .start(address.parse().with_context(make_error)?)
            .await
            .with_context(make_error)?;
        shutdown_handle.add_service("AdminHTTP", srv);
        info!("Listening for Admin HTTP API: {}", listening);
    }

    // FlightSQL API service.
    {
        let address = format!(
            "{}:{}",
            conf.query.flight_sql_handler_host, conf.query.flight_sql_handler_port
        );
        let mut srv = FlightSQLServer::create(conf.clone()).with_context(make_error)?;
        let listening = srv
            .start(address.parse().with_context(make_error)?)
            .await
            .with_context(make_error)?;
        shutdown_handle.add_service("FlightSQLService", srv);
        info!("Listening for FlightSQL API: {}", listening);
    }

    // Print information to users.
    println!("Databend Query");

    println!();
    println!("Version: {}", *DATABEND_COMMIT_VERSION);

    println!();
    println!("Logging:");
    println!("    file: {}", conf.log.file);
    println!("    stderr: {}", conf.log.stderr);
    if conf.log.otlp.on {
        println!("    otlp: {}", conf.log.otlp);
    }
    if conf.log.query.on {
        println!("    query: {}", conf.log.query);
    }
    if conf.log.profile.on {
        println!("    profile: {}", conf.log.profile);
    }
    if conf.log.structlog.on {
        println!("    structlog: {}", conf.log.structlog);
    }

    println!();
    println!(
        "Meta: {}",
        if conf.meta.is_embedded_meta().with_context(make_error)? {
            format!("embedded at {}", conf.meta.embedded_dir)
        } else {
            format!("connected to endpoints {:#?}", conf.meta.endpoints)
        }
    );

    println!();
    println!("Memory:");
    println!("    limit: {}", {
        if conf.query.max_memory_limit_enabled {
            format!(
                "Memory: server memory limit to {} (bytes)",
                conf.query.max_server_memory_usage
            )
        } else {
            "unlimited".to_string()
        }
    });
    println!("    allocator: {}", GlobalAllocator::name());
    println!("    config: {}", GlobalAllocator::conf());

    println!();
    println!("Cluster: {}", {
        let cluster = ClusterDiscovery::instance()
            .discover(conf)
            .await
            .with_context(make_error)?;
        let nodes = cluster.nodes.len();
        if nodes > 1 {
            format!("[{}] nodes", nodes)
        } else {
            "standalone".to_string()
        }
    });

    println!();
    println!("Storage: {}", conf.storage.params);
    println!("Disk cache:");
    println!("    storage: {}", conf.cache.data_cache_storage);
    println!("    path: {:?}", conf.cache.disk_cache_config);
    println!(
        "    reload policy: {}",
        conf.cache.data_cache_key_reload_policy
    );

    println!();
    println!(
        "Builtin users: {}",
        conf.query
            .builtin
            .users
            .iter()
            .map(|config| config.name.clone())
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!();
    println!(
        "Builtin UDFs: {}",
        conf.query
            .builtin
            .udfs
            .iter()
            .map(|config| config.name.clone())
            .collect::<Vec<_>>()
            .join(", ")
    );

    println!();
    println!("Admin");
    println!("    listened at {}", conf.query.admin_api_address);
    println!("MySQL");
    println!(
        "    listened at {}:{}",
        conf.query.mysql_handler_host, conf.query.mysql_handler_port
    );
    println!(
        "    connect via: mysql -u${{USER}} -p${{PASSWORD}} -h{} -P{}",
        conf.query.mysql_handler_host, conf.query.mysql_handler_port
    );
    println!("Clickhouse(http)");
    println!(
        "    listened at {}:{}",
        conf.query.clickhouse_http_handler_host, conf.query.clickhouse_http_handler_port
    );
    println!(
        "    usage: {}",
        HttpHandlerKind::Clickhouse.usage(
            format!(
                "{}:{}",
                conf.query.clickhouse_http_handler_host, conf.query.clickhouse_http_handler_port
            )
            .parse()
            .with_context(make_error)?
        )
    );
    println!("Databend HTTP");
    println!(
        "    listened at {}:{}",
        conf.query.http_handler_host, conf.query.http_handler_port
    );
    println!(
        "    usage: {}",
        HttpHandlerKind::Query.usage(
            format!(
                "{}:{}",
                conf.query.http_handler_host, conf.query.http_handler_port
            )
            .parse()
            .with_context(make_error)?
        )
    );
    for (idx, (k, v)) in env::vars()
        .filter(|(k, _)| k.starts_with("_DATABEND"))
        .enumerate()
    {
        if idx == 0 {
            println!("Databend Internal:");
        }
        println!("    {}={}", k, v);
    }

    info!(
        "Ready for connections after {}s.",
        start_time.elapsed().as_secs_f32()
    );

    if conf.background.enable {
        println!("Start background service");
        get_background_service_handler()
            .start()
            .await
            .with_context(make_error)?;
        // for one shot background service, we need to drop it manually.
        drop(shutdown_handle);
    } else {
        let graceful_shutdown_timeout =
            Some(Duration::from_millis(conf.query.shutdown_wait_timeout_ms));
        shutdown_handle
            .wait_for_termination_request(graceful_shutdown_timeout)
            .await;
    }
    info!("Shutdown server.");
    log::logger().flush();
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn check_max_open_files() {
    use log::warn;

    let limits = match limits_rs::get_own_limits() {
        Ok(limits) => limits,
        Err(err) => {
            warn!("get system limit of databend-query failed: {:?}", err);
            return;
        }
    };
    let max_open_files_limit = limits.max_open_files.soft;
    if let Some(max_open_files) = max_open_files_limit {
        if max_open_files < 65535 {
            warn!(
                "The open file limit is too low for the databend-query. Please consider increase it by running `ulimit -n 65535`"
            );
        }
    }
}
