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

use std::ops::Deref;
use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use jwt_simple::claims::JWTClaims;

use crate::license::Feature;
use crate::license::LicenseInfo;
use crate::license::StorageQuota;

pub trait LicenseManager: Sync + Send {
    fn init(tenant: String) -> Result<()>
    where Self: Sized;

    fn instance() -> Arc<Box<dyn LicenseManager>>
    where Self: Sized;

    /// Check whether enterprise feature is available given context
    /// This function returns `LicenseKeyInvalid` error if enterprise license key is not valid or expired.
    fn check_enterprise_enabled(&self, license_key: String, feature: Feature) -> Result<()>;

    /// Encodes a raw license string as a JWT using the constant public key.
    ///
    /// This function takes a raw license string and a secret key,
    /// The function returns a `jwt_simple::Claim` object that represents the
    /// decoded contents of the JWT  with custom fields `LicenseInfo`
    ///
    /// # Arguments
    ///
    /// * `raw` - The raw license string to be encoded.
    ///
    /// # Returns
    ///
    /// A `jwt_simple::Claim` object representing the decoded contents of the JWT.
    ///
    /// # Errors
    ///
    /// This function may return `LicenseKeyParseError` error if the encoding or decoding of the JWT fails.
    /// ```
    fn parse_license(&self, raw: &str) -> Result<JWTClaims<LicenseInfo>>;

    /// Get the storage quota from license key.
    fn get_storage_quota(&self, license_key: String) -> Result<StorageQuota>;
}

pub struct LicenseManagerSwitch {
    manager: Box<dyn LicenseManager>,
}

impl LicenseManagerSwitch {
    pub fn create(manager: Box<dyn LicenseManager>) -> LicenseManagerSwitch {
        LicenseManagerSwitch { manager }
    }

    pub fn instance() -> Arc<LicenseManagerSwitch> {
        GlobalInstance::get()
    }
}

impl Deref for LicenseManagerSwitch {
    type Target = dyn LicenseManager;

    fn deref(&self) -> &Self::Target {
        self.manager.as_ref()
    }
}

pub struct OssLicenseManager {}

impl LicenseManager for OssLicenseManager {
    fn init(_tenant: String) -> Result<()> {
        let rm = OssLicenseManager {};
        GlobalInstance::set(Arc::new(LicenseManagerSwitch::create(Box::new(rm))));
        Ok(())
    }

    fn instance() -> Arc<Box<dyn LicenseManager>> {
        GlobalInstance::get()
    }

    fn check_enterprise_enabled(&self, _license_key: String, feature: Feature) -> Result<()> {
        // oss ignore license key.
        feature.verify_default("Need Commercial License".to_string())
    }

    fn parse_license(&self, _raw: &str) -> Result<JWTClaims<LicenseInfo>> {
        Err(ErrorCode::LicenceDenied(
            "Need Commercial License".to_string(),
        ))
    }

    /// Always return default storage quota.
    fn get_storage_quota(&self, _: String) -> Result<StorageQuota> {
        Ok(StorageQuota::default())
    }
}
