// Copyright 2023 Databend Cloud
//
// Licensed under the Elastic License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.elastic.co/licensing/elastic-license
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use dashmap::DashMap;
use databend_common_base::base::GlobalInstance;
use databend_common_exception::exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_exception::ToErrorCode;
use databend_common_license::license::Feature;
use databend_common_license::license::LicenseInfo;
use databend_common_license::license::StorageQuota;
use databend_common_license::license_manager::LicenseManager;
use databend_common_license::license_manager::LicenseManagerSwitch;
use jwt_simple::algorithms::ES256PublicKey;
use jwt_simple::claims::JWTClaims;
use jwt_simple::prelude::Clock;
use jwt_simple::prelude::ECDSAP256PublicKeyLike;
use jwt_simple::JWTError;

const LICENSE_PUBLIC_KEY: &str = r#"-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEGsKCbhXU7j56VKZ7piDlLXGhud0a
pWjW3wxSdeARerxs/BeoWK7FspDtfLaAT8iJe4YEmR0JpkRQ8foWs0ve3w==
-----END PUBLIC KEY-----"#;

pub struct RealLicenseManager {
    tenant: String,
    public_key: String,

    // cache available settings to get avoid of unneeded license parsing time.
    pub(crate) cache: DashMap<String, JWTClaims<LicenseInfo>>,
}

impl LicenseManager for RealLicenseManager {
    fn init(tenant: String) -> Result<()> {
        let rm = RealLicenseManager {
            tenant,
            cache: DashMap::new(),
            public_key: LICENSE_PUBLIC_KEY.to_string(),
        };

        GlobalInstance::set(Arc::new(LicenseManagerSwitch::create(Box::new(rm))));
        Ok(())
    }

    fn instance() -> Arc<Box<dyn LicenseManager>> {
        GlobalInstance::get()
    }

    fn check_enterprise_enabled(&self, license_key: String, feature: Feature) -> Result<()> {
        if license_key.is_empty() {
            return feature.verify_default(format!(
                "The use of this feature requires a Databend Enterprise Edition license. No license key found for tenant: {}. To unlock enterprise features, please contact Databend to obtain a license. Learn more at https://docs.databend.com/guides/overview/editions/dee/",
                self.tenant
            ));
        }

        if let Some(v) = self.cache.get(&license_key) {
            return self.verify_feature(v.value(), feature);
        }

        match self.parse_license(&license_key) {
            Ok(license) => {
                self.verify_feature(&license, feature)?;
                self.cache.insert(license_key, license);
                Ok(())
            }
            Err(e) => match e.code() == ErrorCode::LICENSE_KEY_EXPIRED {
                true => self.verify_if_expired(feature),
                false => Err(e),
            },
        }
    }

    fn parse_license(&self, raw: &str) -> Result<JWTClaims<LicenseInfo>> {
        let public_key = ES256PublicKey::from_pem(self.public_key.as_str())
            .map_err_to_code(ErrorCode::LicenseKeyParseError, || "public key load failed")?;

        match public_key.verify_token::<LicenseInfo>(raw, None) {
            Ok(v) => Ok(v),
            Err(cause) => match cause.downcast_ref::<JWTError>() {
                Some(JWTError::TokenHasExpired) => {
                    Err(ErrorCode::LicenseKeyExpired("license key is expired."))
                }
                _ => Err(ErrorCode::LicenseKeyParseError("jwt claim decode failed")),
            },
        }
    }

    fn get_storage_quota(&self, license_key: String) -> Result<StorageQuota> {
        if license_key.is_empty() {
            return Ok(StorageQuota::default());
        }

        if let Some(v) = self.cache.get(&license_key) {
            if Self::verify_license_expired(v.value())? {
                return Err(ErrorCode::LicenseKeyExpired(format!(
                    "license key expired in {:?}",
                    v.value().expires_at,
                )));
            }

            return Ok(v.custom.get_storage_quota());
        }

        let license = self.parse_license(&license_key).map_err_to_code(
            ErrorCode::LicenseKeyInvalid,
            || format!("use of storage requires an enterprise license. current license is invalid for {}", self.tenant),
        )?;

        if Self::verify_license_expired(&license)? {
            return Err(ErrorCode::LicenseKeyExpired(format!(
                "license key expired in {:?}",
                license.expires_at,
            )));
        }

        let quota = license.custom.get_storage_quota();
        self.cache.insert(license_key, license);
        Ok(quota)
    }
}

impl RealLicenseManager {
    // this method mainly used for unit tests
    pub fn new(tenant: String, public_key: String) -> Self {
        RealLicenseManager {
            tenant,
            public_key,

            cache: DashMap::new(),
        }
    }

    fn verify_license_expired(l: &JWTClaims<LicenseInfo>) -> Result<bool> {
        let now = Clock::now_since_epoch();
        match l.expires_at {
            Some(expire_at) => Ok(now > expire_at),
            None => Err(ErrorCode::LicenseKeyInvalid(
                "cannot find valid expire time",
            )),
        }
    }

    fn verify_feature(&self, l: &JWTClaims<LicenseInfo>, feature: Feature) -> Result<()> {
        if Self::verify_license_expired(l)? {
            return self.verify_if_expired(feature);
        }

        if l.custom.features.is_none() {
            return Ok(());
        }

        let verify_features = l.custom.features.as_ref().unwrap();
        for verify_feature in verify_features {
            if verify_feature.verify(&feature)? {
                return Ok(());
            }
        }

        Err(ErrorCode::LicenseKeyInvalid(format!(
            "license key does not support feature {}, supported features: {}",
            feature,
            l.custom.display_features()
        )))
    }

    fn verify_if_expired(&self, feature: Feature) -> Result<()> {
        feature.verify_default("").map_err(|_|
            ErrorCode::LicenseKeyExpired(format!(
                "The use of this feature requires a Databend Enterprise Edition license. License key has expired for tenant: {}. To unlock enterprise features, please contact Databend to obtain a license. Learn more at https://docs.databend.com/guides/overview/editions/dee/",
                self.tenant
            ))
        )
    }
}
