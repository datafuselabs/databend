//  Copyright 2023 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::future::Future;
use std::sync::Arc;

use common_base::base::tokio::sync::Semaphore;
use common_base::runtime::Runtime;
use common_catalog::table_context::TableContext;
use common_exception::ErrorCode;
use common_exception::Result;
use futures_util::future;
use tracing::Instrument;

/// Run multiple futures parallel
/// using a semaphore to limit the parallelism number, and a specified thread pool to run the futures.
/// It waits for all futures to complete and returns their results.
pub async fn execute_futures_in_parallel<Fut>(
    ctx: Arc<dyn TableContext>,
    futures: impl IntoIterator<Item = Fut>,
    thread_name: String,
) -> Result<Vec<Fut::Output>>
where
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    let max_runtime_threads = ctx.get_settings().get_max_threads()? as usize;
    let max_io_requests = ctx.get_settings().get_max_storage_io_requests()? as usize;

    // 1. build the runtime.
    let semaphore = Semaphore::new(max_io_requests);
    let runtime = Arc::new(Runtime::with_worker_threads(
        max_runtime_threads,
        Some(thread_name),
        false,
    )?);

    // 2. spawn all the tasks to the runtime with semaphore.
    let join_handlers = runtime.try_spawn_batch(semaphore, futures).await?;

    // 3. get all the result.
    future::try_join_all(join_handlers)
        .instrument(tracing::debug_span!("execute_futures_in_parallel"))
        .await
        .map_err(|e| ErrorCode::StorageOther(format!("try join all futures failure, {}", e)))
}
