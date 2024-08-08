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
use std::time::Duration;

use databend_common_base::base::GlobalInstance;
use databend_common_exception::Result;
use moka::sync::Cache;
use opendal::raw::Operation;
use opendal::raw::PresignedRequest;
use parking_lot::RwLock;

use crate::signer::PresignRequest;

pub struct SharePresignedCacheManager {
    cache: Arc<RwLock<Cache<PresignRequest, PresignedRequest>>>,
}

impl SharePresignedCacheManager {
    /// Fetch manager from global instance.
    pub fn instance() -> Arc<SharePresignedCacheManager> {
        let global_instance: Arc<SharePresignedCacheManager> = GlobalInstance::get();
        global_instance
    }

    /// Init the manager in global instance.
    #[async_backtrace::framed]
    pub fn init() -> Result<()> {
        let cache = Cache::builder()
            // Databend Cloud Presign will expire after 3600s (1 hour).
            // We will expire them 10 minutes before to avoid edge cases.
            .time_to_live(Duration::from_secs(3000))
            .build();
        GlobalInstance::set(SharePresignedCacheManager {
            cache: Arc::new(RwLock::new(cache)),
        });

        Ok(())
    }

    /// Get a presign request.
    pub fn get(&self, path: &str, op: Operation) -> Option<PresignedRequest> {
        let cache = self.cache.read();
        cache.get(&PresignRequest::new(path, op))
    }

    /// Set a presigned request.
    ///
    /// This operation will update the expiry time about this request.
    pub fn set(&self, path: &str, op: Operation, signed: PresignedRequest) {
        let cache = self.cache.write();
        cache.insert(PresignRequest::new(path, op), signed)
    }
}
