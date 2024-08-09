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

use std::collections::BTreeMap;

use base64::engine::general_purpose::STANDARD_NO_PAD;
use base64::Engine;
use databend_common_base::headers::HEADER_AUTH_METHOD;
use databend_common_base::headers::HEADER_SIGNATURE;
use databend_common_base::headers::HEADER_TENANT;
use databend_common_exception::Result;
use databend_common_meta_app::schema::ShareDBParams;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::share::ShareCredential;
use databend_common_meta_app::share::ShareEndpointMeta;
use databend_common_meta_app::share::ShareSpec;
use log::error;
use reqwest::header::HeaderMap;
use ring::hmac;

use crate::signer::HMAC_AUTH_METHOD;

#[derive(Clone)]
pub struct ShareEndpointClient {
    client: reqwest::Client,
}

impl ShareEndpointClient {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    pub fn generate_auth_headers(
        path: &str,
        credential: &ShareCredential,
        from_tenant: &str,
    ) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(HEADER_TENANT, from_tenant.parse().unwrap());
        match credential {
            ShareCredential::HMAC(hmac_credential) => {
                let key = hmac::Key::new(
                    hmac::HMAC_SHA256,
                    hmac_credential.key.as_bytes().to_vec().as_ref(),
                );
                headers.insert(HEADER_AUTH_METHOD, HMAC_AUTH_METHOD.parse().unwrap());
                // signature = HMAC(from_tenant@path, key)
                let auth = format!("{}@{}", from_tenant, path);
                let signature = hmac::sign(&key, auth.as_bytes());
                let signature = STANDARD_NO_PAD.encode(signature.as_ref());
                headers.insert(HEADER_SIGNATURE, signature.parse().unwrap());
            }
        }
        headers
    }

    #[async_backtrace::framed]
    pub async fn get_share_spec_by_name(
        &self,
        share_endpoint_meta: &ShareEndpointMeta,
        from_tenant: &str,
        to_tenant: &str,
        share_name: &str,
    ) -> Result<ShareSpec> {
        let path = format!("/{}/{}/v2/share_spec", to_tenant, share_name);
        // skip path first `/` char
        let uri = format!("{}{}", share_endpoint_meta.url, &path[1..]);
        let headers = if let Some(credential) = &share_endpoint_meta.credential {
            Self::generate_auth_headers(&path, credential, from_tenant)
        } else {
            HeaderMap::new()
        };

        let resp = self.client.get(&uri).headers(headers).send().await;

        match resp {
            Ok(resp) => {
                let body = resp.text().await?;
                let ret: ShareSpec = serde_json::from_str(&body)?;
                Ok(ret)
            }
            Err(err) => {
                error!("get_share_spec_by_name fail: {:?}", err);
                Err(err.into())
            }
        }
    }

    #[async_backtrace::framed]
    pub async fn get_share_table_by_name(
        &self,
        share_endpoint_meta: &ShareEndpointMeta,
        from_tenant: &str,
        to_tenant: &str,
        share_name: &str,
        db_id: u64,
        table_name: &str,
    ) -> Result<TableInfo> {
        let path = format!(
            "/{}/{}/{}/{}/v2/share_table_by_db_id",
            to_tenant, share_name, db_id, table_name
        );
        // skip path first `/` char
        let uri = format!("{}{}", share_endpoint_meta.url, &path[1..]);
        let headers = if let Some(credential) = &share_endpoint_meta.credential {
            Self::generate_auth_headers(&path, credential, from_tenant)
        } else {
            HeaderMap::new()
        };
        let resp = self.client.get(&uri).headers(headers).send().await;

        match resp {
            Ok(resp) => {
                let body = resp.text().await?;
                let ret: TableInfo = serde_json::from_str(&body)?;
                Ok(ret)
            }
            Err(err) => {
                error!("get_share_spec_by_name fail: {:?}", err);
                Err(err.into())
            }
        }
    }

    #[async_backtrace::framed]
    pub async fn get_share_table(
        &self,
        share_endpoint_meta: &ShareEndpointMeta,
        from_tenant: &str,
        to_tenant: &str,
        share_name: &str,
        db_name: &str,
        table_name: &str,
    ) -> Result<TableInfo> {
        let path = format!(
            "/{}/{}/{}/{}/v2/share_table",
            to_tenant, share_name, db_name, table_name
        );
        // skip path first `/` char
        let uri = format!("{}{}", share_endpoint_meta.url, &path[1..]);
        let headers = if let Some(credential) = &share_endpoint_meta.credential {
            Self::generate_auth_headers(&path, credential, from_tenant)
        } else {
            HeaderMap::new()
        };

        let resp = self.client.get(&uri).headers(headers).send().await;

        match resp {
            Ok(resp) => {
                let body = resp.text().await?;
                let ret: TableInfo = serde_json::from_str(&body)?;
                Ok(ret)
            }
            Err(err) => {
                error!("get_share_spec_by_name fail: {:?}", err);
                Err(err.into())
            }
        }
    }

    #[async_backtrace::framed]
    pub async fn get_share_tables(
        &self,
        share_endpoint_meta: &ShareEndpointMeta,
        from_tenant: &str,
        to_tenant: &str,
        share_name: &str,
        db_name: &str,
    ) -> Result<BTreeMap<String, TableInfo>> {
        let path = format!("/{}/{}/{}/v2/share_tables", to_tenant, share_name, db_name);
        // skip path first `/` char
        let uri = format!("{}{}", share_endpoint_meta.url, &path[1..]);
        let headers = if let Some(credential) = &share_endpoint_meta.credential {
            Self::generate_auth_headers(&path, credential, from_tenant)
        } else {
            HeaderMap::new()
        };

        let resp = self.client.get(&uri).headers(headers).send().await;

        match resp {
            Ok(resp) => {
                let body = resp.text().await?;
                let ret: BTreeMap<String, TableInfo> = serde_json::from_str(&body)?;
                Ok(ret)
            }
            Err(err) => {
                error!("get_share_tables fail: {:?}", err);
                Err(err.into())
            }
        }
    }

    #[async_backtrace::framed]
    pub async fn get_share_tables_by_db_id(
        &self,
        share_endpoint_meta: &ShareEndpointMeta,
        from_tenant: &str,
        to_tenant: &str,
        db_id: u64,
        share_name: &str,
    ) -> Result<BTreeMap<String, TableInfo>> {
        let path = format!(
            "/{}/{}/{}/v2/share_tables_by_db_id",
            to_tenant, share_name, db_id
        );
        // skip path first `/` char
        let uri = format!("{}{}", share_endpoint_meta.url, &path[1..]);
        let headers = if let Some(credential) = &share_endpoint_meta.credential {
            Self::generate_auth_headers(&path, credential, from_tenant)
        } else {
            HeaderMap::new()
        };
        let resp = self.client.get(&uri).headers(headers).send().await;

        match resp {
            Ok(resp) => {
                let body = resp.text().await?;
                let ret: BTreeMap<String, TableInfo> = serde_json::from_str(&body)?;
                Ok(ret)
            }
            Err(err) => {
                error!("get_share_spec_by_name fail: {:?}", err);
                Err(err.into())
            }
        }
    }

    #[async_backtrace::framed]
    pub async fn get_reference_table_by_name(
        &self,
        share_params: &ShareDBParams,
        from_tenant: &str,
        db: &str,
        table_name: &str,
    ) -> Result<TableInfo> {
        let to_tenant = share_params.share_ident.tenant_name();
        let share_name = share_params.share_ident.share_name();
        let path = format!(
            "/{}/{}/{}/{}/v2/reference_table",
            to_tenant, share_name, db, table_name
        );
        // skip path first `/` char
        let uri = format!("{}{}", share_params.share_endpoint_url, &path[1..]);
        let headers = Self::generate_auth_headers(
            &path,
            &share_params.share_endpoint_credential,
            from_tenant,
        );
        let resp = self.client.get(&uri).headers(headers).send().await;

        match resp {
            Ok(resp) => {
                let body = resp.text().await?;
                let ret: TableInfo = serde_json::from_str(&body)?;
                Ok(ret)
            }
            Err(err) => {
                error!("get_share_spec_by_name fail: {:?}", err);
                Err(err.into())
            }
        }
    }
}
