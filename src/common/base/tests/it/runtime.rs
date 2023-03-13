// Copyright 2021 Datafuse Labs.
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
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

use common_base::runtime::Runtime;
use common_base::runtime::TrySpawn;
use common_exception::Result;
use once_cell::sync::Lazy;
use rand::distributions::Distribution;
use rand::distributions::Uniform;
use tokio::sync::Semaphore;
use tokio::time::sleep;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_runtime() -> Result<()> {
    let counter = Arc::new(Mutex::new(0));

    let runtime = Runtime::with_default_worker_threads(false)?;
    let runtime_counter = Arc::clone(&counter);
    let runtime_header = runtime.spawn(async move {
        let rt1 = Runtime::with_default_worker_threads(false).unwrap();
        let rt1_counter = Arc::clone(&runtime_counter);
        let rt1_header = rt1.spawn(async move {
            let rt2 = Runtime::with_worker_threads(1, None, false).unwrap();
            let rt2_counter = Arc::clone(&rt1_counter);
            let rt2_header = rt2.spawn(async move {
                let rt3 = Runtime::with_default_worker_threads(false).unwrap();
                let rt3_counter = Arc::clone(&rt2_counter);
                let rt3_header = rt3.spawn(async move {
                    let mut num = rt3_counter.lock().unwrap();
                    *num += 1;
                });
                rt3_header.await.unwrap();

                let mut num = rt2_counter.lock().unwrap();
                *num += 1;
            });
            rt2_header.await.unwrap();

            let mut num = rt1_counter.lock().unwrap();
            *num += 1;
        });
        rt1_header.await.unwrap();

        let mut num = runtime_counter.lock().unwrap();
        *num += 1;
    });
    runtime_header.await.unwrap();

    let result = *counter.lock().unwrap();
    assert_eq!(result, 4);

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_shutdown_long_run_runtime() -> Result<()> {
    let runtime = Runtime::with_default_worker_threads(false)?;

    runtime.spawn(async move {
        std::thread::sleep(Duration::from_secs(6));
    });

    let instant = Instant::now();
    drop(runtime);
    assert!(instant.elapsed() >= Duration::from_secs(3));
    assert!(instant.elapsed() < Duration::from_secs(4));

    Ok(())
}

static START_TIME: Lazy<Instant> = Lazy::new(Instant::now);
// println can more clearly know if they are parallel
async fn mock_get_page(i: usize) -> Vec<usize> {
    let millis = Uniform::from(0..10).sample(&mut rand::thread_rng());
    println!(
        "[{}] > get_page({}) will complete in {} ms, {:?}",
        START_TIME.elapsed().as_millis(),
        i,
        millis,
        std::thread::current().id(),
    );

    sleep(Duration::from_millis(millis)).await;
    println!(
        "[{}] < get_page({}) completed",
        START_TIME.elapsed().as_millis(),
        i
    );

    (i..(i + 1)).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_runtime_try_spawn_batch() -> Result<()> {
    let runtime = Runtime::with_default_worker_threads(false)?;

    let mut futs = vec![];
    for i in 0..20 {
        futs.push(mock_get_page(i));
    }

    let max_concurrency = Semaphore::new(3);
    let handlers = runtime.try_spawn_batch(max_concurrency, futs).await?;
    let result = futures::future::try_join_all(handlers).await.unwrap();
    assert_eq!(result.len(), 20);
    Ok(())
}
