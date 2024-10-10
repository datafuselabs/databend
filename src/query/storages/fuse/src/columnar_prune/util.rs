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

use databend_common_base::runtime::Runtime;
use databend_common_base::runtime::TrySpawn;
use databend_common_exception::Result;
use futures::future::try_join_all;

/// Distribute tasks evenly across the given concurrency.
pub async fn parallelize_workload<Task, WorkerFn, WorkerFuture, Output, Context>(
    executor: &Arc<Runtime>,
    concurrency: usize,
    tasks: &[Task],
    ctx: Arc<Context>,
    worker: WorkerFn,
) -> Result<Vec<Result<Output>>>
where
    Task: Clone + Send + 'static,
    WorkerFn: Fn(Vec<Task>, Arc<Context>) -> WorkerFuture + Clone + Send + 'static,
    WorkerFuture: std::future::Future<Output = Result<Output>> + Send,
    Output: Send + 'static,
    Context: Send + Sync + 'static,
{
    let total_tasks = tasks.len();
    let tasks_per_worker = (total_tasks + concurrency - 1) / concurrency;

    let mut futures = Vec::with_capacity(concurrency);

    for worker_tasks in tasks.chunks(tasks_per_worker) {
        let worker_tasks = worker_tasks.to_vec();
        let worker_clone = worker.clone();
        let ctx_clone = ctx.clone();
        let future = executor.spawn(async move { worker_clone(worker_tasks, ctx_clone).await });

        futures.push(future);
    }

    try_join_all(futures).await
}
