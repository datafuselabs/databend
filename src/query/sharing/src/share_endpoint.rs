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

use std::sync::Arc;

use bytes::Bytes;
use databend_common_auth::RefreshableToken;
use databend_common_base::base::GlobalInstance;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_api::ShareApi;
use databend_common_meta_app::schema::DatabaseInfo;
use databend_common_meta_app::share::GetShareEndpointReq;
use databend_common_meta_app::share::ShareNameIdent;
use databend_common_meta_app::share::ShareSpec;
use databend_common_meta_app::share::TableInfoMap;
use databend_common_storage::ShareTableConfig;
use databend_common_users::UserApiProvider;
use http::header::AUTHORIZATION;
use http::header::CONTENT_LENGTH;
use http::Method;
use http::Request;
use log::error;
use opendal::raw::AsyncBody;
use opendal::raw::HttpClient;

use crate::signer::TENANT_HEADER;

#[derive(Debug)]
struct EndpointConfig {
    pub url: String,
    pub token: RefreshableToken,
    pub tenant: String,
}

pub struct ShareEndpointManager {
    client: HttpClient,
}

impl ShareEndpointManager {
    pub fn init() -> Result<()> {
        GlobalInstance::set(Arc::new(ShareEndpointManager {
            client: HttpClient::new()?,
        }));
        Ok(())
    }

    pub fn instance() -> Arc<ShareEndpointManager> {
        GlobalInstance::get()
    }

    #[async_backtrace::framed]
    async fn get_share_endpoint_config(
        &self,
        from_tenant: &str,
        to_tenant: Option<String>,
    ) -> Result<Vec<EndpointConfig>> {
        if let Some(ref to_tenant) = to_tenant {
            if to_tenant == from_tenant {
                match ShareTableConfig::share_endpoint_address() {
                    Some(url) => {
                        return Ok(vec![EndpointConfig {
                            url: format!("http://{}/", url),
                            token: ShareTableConfig::share_endpoint_token(),
                            tenant: from_tenant.to_string(),
                        }]);
                    }
                    None => return Ok(vec![]),
                }
            }
        }

        let req = GetShareEndpointReq {
            tenant: from_tenant.to_owned(),
            endpoint: None,
            to_tenant,
        };
        let meta_api = UserApiProvider::instance().get_meta_store_client();
        let resp = meta_api.get_share_endpoint(req).await?;
        let mut share_endpoint_config_vec = Vec::with_capacity(resp.share_endpoint_meta_vec.len());
        for (_, endpoint_meta) in resp.share_endpoint_meta_vec.iter() {
            share_endpoint_config_vec.push(EndpointConfig {
                url: endpoint_meta.url.clone(),
                token: RefreshableToken::Direct(from_tenant.to_owned()),
                tenant: endpoint_meta.tenant.clone(),
            });
        }
        Ok(share_endpoint_config_vec)
    }

    #[async_backtrace::framed]
    pub async fn get_table_info_map(
        &self,
        from_tenant: &str,
        db_info: &DatabaseInfo,
        tables: Vec<String>,
    ) -> Result<TableInfoMap> {
        let to_tenant = &db_info.meta.from_share.as_ref().unwrap().tenant;
        let share_name = &db_info.meta.from_share.as_ref().unwrap().share_name;

        let endpoint_meta_config_vec = self
            .get_share_endpoint_config(from_tenant, Some(to_tenant.name().to_string()))
            .await?;
        let endpoint_config = match endpoint_meta_config_vec.first() {
            Some(endpoint_meta_config) => endpoint_meta_config,
            None => {
                return Err(ErrorCode::UnknownShareEndpoint(format!(
                    "Unknown share endpoint on accessing shared database from tenant {:?} to target tenant {:?}",
                    from_tenant, to_tenant
                )));
            }
        };

        let url = format!(
            "{}tenant/{}/{}/meta",
            endpoint_config.url,
            to_tenant.name(),
            share_name
        );
        let bs = Bytes::from(serde_json::to_vec(&tables)?);
        let auth = endpoint_config.token.to_header().await?;
        let requester = GlobalConfig::instance()
            .as_ref()
            .query
            .tenant_id
            .name()
            .to_string();
        let req = Request::builder()
            .method(Method::POST)
            .uri(&url)
            .header(AUTHORIZATION, auth)
            .header(CONTENT_LENGTH, bs.len())
            .header(TENANT_HEADER, requester)
            .body(AsyncBody::Bytes(bs))?;
        let resp = self.client.send(req).await;
        match resp {
            Ok(resp) => {
                if !resp.status().is_success() {
                    return Err(ErrorCode::ShareStorageError(format!(
                        "share {:?} storage error: HTTP status {:?}",
                        share_name,
                        match resp.status().canonical_reason() {
                            Some(reason) => reason.to_string(),
                            None => resp.status().to_string(),
                        }
                    )));
                }
                let bs = resp.into_body().bytes().await?;
                match serde_json::from_slice(&bs) {
                    Ok(table_info_map) => Ok(table_info_map),
                    Err(e) => Err(ErrorCode::ShareStorageError(format!(
                        "share {:?} storage error: deser json file error: {:?}",
                        share_name, e
                    ))),
                }
            }
            Err(err) => Err(err.into()),
        }
    }

    #[async_backtrace::framed]
    pub async fn get_inbound_shares(
        &self,
        from_tenant: &str,
        to_tenant: Option<String>,
        share_name: Option<ShareNameIdent>,
    ) -> Result<Vec<(String, ShareSpec)>> {
        let mut endpoint_meta_config_vec = vec![];
        // If `to_tenant` is None, query from same tenant for inbound shares
        if to_tenant.is_none() {
            if let Ok(config_vec) = self
                .get_share_endpoint_config(from_tenant, Some(from_tenant.to_string()))
                .await
            {
                endpoint_meta_config_vec.extend(config_vec);
            }
        }
        if let Ok(config_vec) = self.get_share_endpoint_config(from_tenant, to_tenant).await {
            endpoint_meta_config_vec.extend(config_vec);
        }

        let mut share_spec_vec = vec![];
        let share_names: Vec<String> = vec![];
        for endpoint_config in endpoint_meta_config_vec {
            let url = format!("{}tenant/{}/share_spec", endpoint_config.url, from_tenant);
            let bs = Bytes::from(serde_json::to_vec(&share_names)?);
            let auth = endpoint_config.token.to_header().await?;
            let requester = GlobalConfig::instance()
                .as_ref()
                .query
                .tenant_id
                .name()
                .to_string();
            let req = Request::builder()
                .method(Method::POST)
                .uri(&url)
                .header(AUTHORIZATION, auth)
                .header(CONTENT_LENGTH, bs.len())
                .header(TENANT_HEADER, requester)
                .body(AsyncBody::Bytes(bs))?;
            let resp = self.client.send(req).await;
            match resp {
                Ok(resp) => {
                    let bs = resp.into_body().bytes().await?;
                    let ret: Vec<ShareSpec> = serde_json::from_slice(&bs)?;
                    for share_spec in ret {
                        if let Some(ref share_name) = share_name {
                            if share_spec.name == share_name.share_name
                                && endpoint_config.tenant == share_name.tenant.name()
                            {
                                share_spec_vec.push((endpoint_config.tenant.clone(), share_spec));
                                return Ok(share_spec_vec);
                            }
                        }
                        share_spec_vec.push((endpoint_config.tenant.clone(), share_spec));
                    }
                }
                Err(err) => {
                    error!("get_inbound_shares error: {:?}", err);
                    continue;
                }
            }
        }

        Ok(share_spec_vec)
    }
}
