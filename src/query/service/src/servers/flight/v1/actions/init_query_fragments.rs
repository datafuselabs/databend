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

use databend_common_base::runtime::ThreadTracker;
use databend_common_base::runtime::TrySpawn;
use databend_common_exception::Result;
use log::debug;

use crate::servers::flight::v1::exchange::DataExchangeManager;
use crate::servers::flight::v1::packets::QueryFragments;

pub static INIT_QUERY_FRAGMENTS: &str = "/actions/init_query_fragments";

pub async fn init_query_fragments(fragments: QueryFragments) -> Result<()> {
    let mut tracking_payload = ThreadTracker::new_tracking_payload();
    tracking_payload.query_id = Some(fragments.query_id.clone());
    let _guard = ThreadTracker::tracking(tracking_payload);

    debug!("init query fragments with {:?}", fragments);

    // Avoid blocking runtime.
    let query_id = fragments.query_id.clone();
    let ctx = DataExchangeManager::instance().get_query_ctx(&fragments.query_id)?;
    let join_handler = ctx.spawn(ThreadTracker::tracking_future(async move {
        DataExchangeManager::instance().init_query_fragments_plan(&fragments)
    }));

    if let Err(cause) = join_handler.await.flatten() {
        DataExchangeManager::instance().on_finished_query(&query_id);
        return Err(cause);
    }

    Ok(())
}
