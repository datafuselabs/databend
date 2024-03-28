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

#![allow(clippy::uninlined_format_args)]

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fmt::Display;
use std::sync::Arc;
use std::time::Instant;

use chrono::Utc;
use clap::Parser;
use databend_common_base::base::tokio;
use databend_common_base::runtime;
use databend_common_meta_api::serialize_struct;
use databend_common_meta_api::txn_op_put;
use databend_common_meta_api::SchemaApi;
use databend_common_meta_app::schema::CreateDatabaseReq;
use databend_common_meta_app::schema::CreateOption;
use databend_common_meta_app::schema::CreateTableReq;
use databend_common_meta_app::schema::DatabaseNameIdent;
use databend_common_meta_app::schema::DropTableByIdReq;
use databend_common_meta_app::schema::GetTableReq;
use databend_common_meta_app::schema::TableCopiedFileInfo;
use databend_common_meta_app::schema::TableCopiedFileNameIdent;
use databend_common_meta_app::schema::TableNameIdent;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_meta_app::tenant::Tenant;
use databend_common_meta_client::ClientHandle;
use databend_common_meta_client::MetaGrpcClient;
use databend_common_meta_kvapi::kvapi::KVApi;
use databend_common_meta_kvapi::kvapi::UpsertKVReq;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::Operation;
use databend_common_meta_types::TxnRequest;
use databend_common_tracing::init_logging;
use databend_common_tracing::FileConfig;
use databend_common_tracing::StderrConfig;
use databend_meta::version::METASRV_COMMIT_VERSION;
use serde::Deserialize;
use serde::Serialize;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Parser)]
#[clap(about, version = &**METASRV_COMMIT_VERSION, author)]
struct Config {
    /// The prefix of keys to write.
    #[clap(long, default_value = "0")]
    pub prefix: u64,

    #[clap(long, default_value = "10")]
    pub client: u64,

    #[clap(long, default_value = "10000")]
    pub number: u64,

    #[clap(long, default_value = "INFO")]
    pub log_level: String,

    #[clap(long, env = "METASRV_GRPC_API_ADDRESS", default_value = "")]
    pub grpc_api_address: String,

    /// The RPC to benchmark:
    /// "upsert_kv": send kv-api upsert_kv,
    /// "table": create db, table and upsert_table_option;
    /// "get_table": single get_table() rpc;
    /// "table_copy_file": upsert table with copy file.
    /// "table_copy_file:{"file_cnt":100}": upsert table with 100 copy files. After ":" is a json config string
    #[clap(long, default_value = "upsert_kv")]
    pub rpc: String,
}

#[tokio::main]
async fn main() {
    let config = Config::parse();

    let log_config = databend_common_tracing::Config {
        file: FileConfig {
            on: true,
            level: config.log_level.clone(),
            dir: "./.databend/logs".to_string(),
            format: "text".to_string(),
            limit: 48,
            prefix_filter: "databend_".to_string(),
        },
        stderr: StderrConfig {
            on: true,
            level: "WARN".to_string(),
            format: "text".to_string(),
        },
        ..Default::default()
    };

    let _guards = init_logging("databend-metabench", &log_config, BTreeMap::new());

    println!("config: {:?}", config);
    if config.grpc_api_address.is_empty() {
        println!("grpc_api_address MUST not be empty!");
        return;
    }

    let start = Instant::now();
    let mut client_num = 0;
    let mut handles = Vec::new();
    while client_num < config.client {
        client_num += 1;
        let addr = config.grpc_api_address.clone();
        let rpc = config.rpc.clone();
        let prefix = config.prefix;

        let cmd_and_param = rpc.splitn(2, ':').collect::<Vec<_>>();
        let cmd = cmd_and_param[0].to_string();
        let param = cmd_and_param.get(1).unwrap_or(&"").to_string();

        let handle = runtime::spawn(async move {
            let client =
                MetaGrpcClient::try_create(vec![addr.to_string()], "root", "xxx", None, None, None);

            let client = match client {
                Ok(client) => client,
                Err(e) => {
                    eprintln!("Failed to create client: {}", e);
                    return;
                }
            };

            for i in 0..config.number {
                if cmd == "upsert_kv" {
                    benchmark_upsert(&client, prefix, client_num, i).await;
                } else if cmd == "table" {
                    benchmark_table(&client, prefix, client_num, i).await;
                } else if cmd == "get_table" {
                    benchmark_get_table(&client, prefix, client_num, i).await;
                } else if cmd == "table_copy_file" {
                    benchmark_table_copy_file(&client, prefix, client_num, i, &param).await;
                } else {
                    unreachable!("Invalid config.rpc: {}", rpc);
                }
            }
        });
        handles.push(handle)
    }

    for handle in handles {
        handle.await.unwrap();
    }
    let end = Instant::now();
    println!(
        "benchmark client({}) * number({}) in {} milliseconds",
        config.client,
        config.number,
        end.duration_since(start).as_millis()
    );
}

async fn benchmark_upsert(client: &Arc<ClientHandle>, prefix: u64, client_num: u64, i: u64) {
    let node_key = || format!("{}-{}-{}", prefix, client_num, i);

    let seq = MatchSeq::Any;
    let value = Operation::Update(node_key().as_bytes().to_vec());

    let res = client
        .upsert_kv(UpsertKVReq::new(&node_key(), seq, value, None))
        .await;

    print_res(i, "upsert_kv", &res);
}

async fn benchmark_table(client: &Arc<ClientHandle>, prefix: u64, client_num: u64, i: u64) {
    let tenant = || Tenant::new_literal(&format!("tenant-{}-{}", prefix, client_num));
    let db_name = || format!("db-{}-{}", prefix, client_num);
    let table_name = || format!("table-{}-{}", prefix, client_num);

    let tb_name_ident = || TableNameIdent {
        tenant: tenant(),
        db_name: db_name(),
        table_name: table_name(),
    };

    let res = client
        .create_database(CreateDatabaseReq {
            create_option: CreateOption::Create,
            name_ident: DatabaseNameIdent {
                tenant: tenant(),
                db_name: db_name(),
            },
            meta: Default::default(),
        })
        .await;

    print_res(i, "create_db", &res);
    let db_id = match res {
        Ok(res) => res.db_id,
        Err(_) => 0,
    };

    let res = client
        .create_table(CreateTableReq {
            create_option: CreateOption::CreateIfNotExists,
            name_ident: tb_name_ident(),
            table_meta: Default::default(),
        })
        .await;

    print_res(i, "create_table", &res);

    let res = client
        .get_table(GetTableReq::new(&tenant(), db_name(), table_name()))
        .await;

    print_res(i, "get_table", &res);

    let t = res.unwrap();

    let res = client
        .upsert_table_option(UpsertTableOptionReq {
            table_id: t.ident.table_id,
            seq: MatchSeq::GE(t.ident.seq),
            options: Default::default(),
        })
        .await;

    print_res(i, "upsert_table_option", &res);

    let res = client
        .drop_table_by_id(DropTableByIdReq {
            if_exists: false,
            tenant: tenant(),
            db_id,
            table_name: table_name(),
            tb_id: t.ident.table_id,
        })
        .await;

    print_res(i, "drop_table", &res);

    let res = client
        .create_table(CreateTableReq {
            create_option: CreateOption::CreateIfNotExists,
            name_ident: tb_name_ident(),
            table_meta: Default::default(),
        })
        .await;

    print_res(i, "create_table again", &res);
}

async fn benchmark_get_table(client: &Arc<ClientHandle>, prefix: u64, client_num: u64, i: u64) {
    let tenant = || Tenant::new_literal(&format!("tenant-{}-{}", prefix, client_num));
    let db_name = || format!("db-{}-{}", prefix, client_num);
    let table_name = || format!("table-{}-{}", prefix, client_num);

    let res = client
        .get_table(GetTableReq::new(&tenant(), db_name(), table_name()))
        .await;

    print_res(i, "get_table", &res);
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct TableCopyFileConfig {
    file_cnt: u64,
}

impl Default for TableCopyFileConfig {
    fn default() -> Self {
        Self { file_cnt: 100 }
    }
}

/// Benchmark upsert table with copy file.
async fn benchmark_table_copy_file(
    client: &Arc<ClientHandle>,
    prefix: u64,
    client_num: u64,
    i: u64,
    param: &str,
) {
    let param = if param.is_empty() {
        TableCopyFileConfig::default()
    } else {
        serde_json::from_str(param).unwrap()
    };

    let mut txn = TxnRequest {
        condition: vec![],
        if_then: vec![],
        else_then: vec![],
    };

    for file_index in 0..param.file_cnt {
        let copied_file_ident = TableCopiedFileNameIdent {
            table_id: prefix * 1_000_000 + client_num * 1_000 + i,
            file: format!("{}-{}-{}-{}", prefix, client_num, i, file_index),
        };
        let copied_file_value = TableCopiedFileInfo {
            etag: Some(format!("{}-{}-{}-{}", prefix, client_num, i, file_index)),
            content_length: 5,
            last_modified: Some(Utc::now()),
        };

        let put_op = txn_op_put(
            &copied_file_ident,
            serialize_struct(&copied_file_value).unwrap(),
        );
        txn.if_then.push(put_op);
    }

    let res = client.transaction(txn).await;

    print_res(i, "table_copy_file", &res);
    res.unwrap();
}

fn print_res<D: Debug>(i: u64, typ: impl Display, res: &D) {
    if i % 100 == 0 {
        println!("{:>10}-th {} result: {:?}", i, typ, res);
    }
}
