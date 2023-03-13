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

use std::backtrace::Backtrace;
use std::cell::RefCell;
use std::future::Future;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use common_exception::ErrorCode;
use common_exception::Result;
use tokio::runtime::Builder;
use tokio::runtime::Handle;
use tokio::sync::oneshot;
use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;

use crate::runtime::catch_unwind::CatchUnwindFuture;
use crate::runtime::MemStat;

thread_local! {
    static STATE: RefCell<RuntimeState> = RefCell::new(RuntimeState::empty());
}

pub struct RuntimeState {
    global: bool,
}

impl RuntimeState {
    pub const fn empty() -> RuntimeState {
        RuntimeState { global: false }
    }

    pub fn set_global() {
        STATE.with(|v: &RefCell<RuntimeState>| {
            let mut borrow_mut = v.borrow_mut();
            borrow_mut.global = true;
        })
    }

    pub fn is_global() -> bool {
        STATE.with(|v: &RefCell<RuntimeState>| v.borrow().global)
    }
}

/// Methods to spawn tasks.
pub trait TrySpawn {
    /// Tries to spawn a new asynchronous task, returning a tokio::JoinHandle for it.
    ///
    /// It allows to return an error before spawning the task.
    #[track_caller]
    fn try_spawn<T>(&self, task: T) -> Result<JoinHandle<T::Output>>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static;

    /// Spawns a new asynchronous task, returning a tokio::JoinHandle for it.
    ///
    /// A default impl of this method just calls `try_spawn` and just panics if there is an error.
    #[track_caller]
    fn spawn<T>(&self, task: T) -> JoinHandle<T::Output>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        self.try_spawn(task).unwrap()
    }
}

impl<S: TrySpawn> TrySpawn for Arc<S> {
    #[track_caller]
    fn try_spawn<T>(&self, task: T) -> Result<JoinHandle<T::Output>>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        self.as_ref().try_spawn(task)
    }

    #[track_caller]
    fn spawn<T>(&self, task: T) -> JoinHandle<T::Output>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        self.as_ref().spawn(task)
    }
}

/// Tokio Runtime wrapper.
/// If a runtime is in an asynchronous context, shutdown it first.
pub struct Runtime {
    /// Runtime handle.
    handle: Handle,

    /// Memory tracker for this runtime
    tracker: Arc<MemStat>,

    /// Use to receive a drop signal when dropper is dropped.
    _dropper: Dropper,
}

impl Runtime {
    fn create(name: Option<String>, tracker: Arc<MemStat>, builder: &mut Builder) -> Result<Self> {
        let runtime = builder
            .build()
            .map_err(|tokio_error| ErrorCode::TokioError(tokio_error.to_string()))?;

        let (send_stop, recv_stop) = oneshot::channel();

        let handle = runtime.handle().clone();

        // Block the runtime to shutdown.
        let join_handler = thread::spawn(move || {
            // We ignore channel is closed.
            let _ = runtime.block_on(recv_stop);

            match !cfg!(debug_assertions) {
                true => false,
                false => {
                    let instant = Instant::now();
                    // We wait up to 3 seconds to complete the runtime shutdown.
                    runtime.shutdown_timeout(Duration::from_secs(3));

                    instant.elapsed() >= Duration::from_secs(3)
                }
            }
        });

        Ok(Runtime {
            handle,
            tracker,
            _dropper: Dropper {
                name,
                close: Some(send_stop),
                join_handler: Some(join_handler),
            },
        })
    }

    fn tracker_builder(mem_stat: Arc<MemStat>, global: bool) -> Builder {
        let mut builder = Builder::new_multi_thread();
        let fun = mem_stat.on_start_thread();
        builder.enable_all().on_thread_start(move || {
            if global {
                RuntimeState::set_global();
            }

            fun();
        });

        builder
    }

    pub fn get_tracker(&self) -> Arc<MemStat> {
        self.tracker.clone()
    }

    /// Spawns a new tokio runtime with a default thread count on a background
    /// thread and returns a `Handle` which can be used to spawn tasks via
    /// its executor.
    pub fn with_default_worker_threads(global: bool) -> Result<Self> {
        let mem_stat = MemStat::create(String::from("UnnamedRuntime"));
        let mut runtime_builder = Self::tracker_builder(mem_stat.clone(), global);

        #[cfg(debug_assertions)]
        {
            // We need to pass the thread name in the unit test, because the thread name is the test name
            if matches!(std::env::var("UNIT_TEST"), Ok(var_value) if var_value == "TRUE") {
                if let Some(thread_name) = std::thread::current().name() {
                    runtime_builder.thread_name(thread_name);
                }
            }
        }

        Self::create(None, mem_stat, &mut runtime_builder)
    }

    #[allow(unused_mut)]
    pub fn with_worker_threads(
        workers: usize,
        mut thread_name: Option<String>,
        global: bool,
    ) -> Result<Self> {
        let mut mem_stat_name = String::from("UnnamedRuntime");

        if let Some(thread_name) = thread_name.as_ref() {
            mem_stat_name = format!("{}Runtime", thread_name);
        }

        let mem_stat = MemStat::create(mem_stat_name);
        let mut runtime_builder = Self::tracker_builder(mem_stat.clone(), global);

        #[cfg(debug_assertions)]
        {
            // We need to pass the thread name in the unit test, because the thread name is the test name
            if matches!(std::env::var("UNIT_TEST"), Ok(var_value) if var_value == "TRUE") {
                if let Some(cur_thread_name) = std::thread::current().name() {
                    thread_name = Some(cur_thread_name.to_string());
                }
            }
        }

        if let Some(thread_name) = &thread_name {
            runtime_builder.thread_name(thread_name);
        }

        Self::create(
            thread_name,
            mem_stat,
            runtime_builder.worker_threads(workers),
        )
    }

    pub fn inner(&self) -> tokio::runtime::Handle {
        self.handle.clone()
    }

    pub fn block_on_with_current<F: Future>(future: F) -> F::Output {
        assert!(
            !RuntimeState::is_global(),
            "Cannot call block_on_current in global runtime"
        );
        Handle::current().block_on(future)
    }

    pub fn block_on<T, F>(&self, future: F) -> F::Output
    where F: Future<Output = Result<T>> + Send + 'static {
        assert!(
            !RuntimeState::is_global(),
            "Cannot call runtime block_on in global runtime"
        );
        let future = CatchUnwindFuture::create(future);
        self.handle.block_on(future).flatten()
    }

    // For each future of `futures`, before being executed
    // a permit will be acquired from the semaphore, and released when it is done
    pub async fn try_spawn_batch<Fut>(
        &self,
        semaphore: Semaphore,
        futures: impl IntoIterator<Item = Fut>,
    ) -> Result<Vec<JoinHandle<Fut::Output>>>
    where
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        let semaphore = Arc::new(semaphore);
        let iter = futures.into_iter().map(|v| {
            |permit| async {
                let r = v.await;
                drop(permit);
                r
            }
        });
        self.try_spawn_batch_with_owned_semaphore(semaphore, iter)
            .await
    }

    // For each future of `futures`, before being executed
    // a permit will be acquired from the semaphore, and released when it is done

    // Please take care using the `semaphore`.
    // If sub task may be spawned in the `futures`, and uses the
    // clone of semaphore to acquire permits, please release the permits on time,
    // or give sufficient(but not abundant, of course) permits, to tolerant the
    // maximum degree of parallelism, otherwise, it may lead to deadlock.
    pub async fn try_spawn_batch_with_owned_semaphore<F, Fut>(
        &self,
        semaphore: Arc<Semaphore>,
        futures: impl IntoIterator<Item = F>,
    ) -> Result<Vec<JoinHandle<Fut::Output>>>
    where
        F: FnOnce(OwnedSemaphorePermit) -> Fut + Send + 'static,
        Fut: Future + Send + 'static,
        Fut::Output: Send + 'static,
    {
        let iter = futures.into_iter();
        let mut handlers =
            Vec::with_capacity(iter.size_hint().1.unwrap_or_else(|| iter.size_hint().0));
        for fut in iter {
            let semaphore = semaphore.clone();
            // Although async task is rather lightweight, it do consumes resources,
            // so we acquire a permit BEFORE spawn.
            // Thus, the `futures` passed into this method is NOT suggested to be "materialized"
            // iterator, e.g. Vec<..>
            let permit = semaphore.acquire_owned().await.map_err(|e| {
                ErrorCode::Internal(format!("semaphore closed, acquire permit failure. {}", e))
            })?;
            let handler = self.handle.spawn(async move {
                // take the ownership of the permit, (implicitly) drop it when task is done
                fut(permit).await
            });
            handlers.push(handler)
        }

        Ok(handlers)
    }
}

impl TrySpawn for Runtime {
    #[track_caller]
    fn try_spawn<T>(&self, task: T) -> Result<JoinHandle<T::Output>>
    where
        T: Future + Send + 'static,
        T::Output: Send + 'static,
    {
        Ok(self.handle.spawn(task))
    }
}

/// Dropping the dropper will cause runtime to shutdown.
pub struct Dropper {
    name: Option<String>,
    close: Option<oneshot::Sender<()>>,
    join_handler: Option<thread::JoinHandle<bool>>,
}

impl Drop for Dropper {
    fn drop(&mut self) {
        // Send a signal to say i am dropping.
        if let Some(close_sender) = self.close.take() {
            if close_sender.send(()).is_ok() {
                match self.join_handler.take().unwrap().join() {
                    Err(e) => tracing::warn!("Runtime dropper panic, {:?}", e),
                    Ok(true) => {
                        // When the runtime shutdown is blocked for more than 3 seconds,
                        // we will print the backtrace in the warn log, which will help us debug.
                        tracing::warn!(
                            "Runtime dropper is blocked 3 seconds, runtime name: {:?}, drop backtrace: {:?}",
                            self.name,
                            Backtrace::capture()
                        );
                    }
                    _ => {}
                };
            }
        }
    }
}

pub async fn match_join_handle<T>(handle: JoinHandle<Result<T>>) -> Result<T> {
    match handle.await {
        Ok(Ok(res)) => Ok(res),
        Ok(Err(cause)) => Err(cause),
        Err(join_error) => match join_error.is_cancelled() {
            true => Err(ErrorCode::TokioError("Tokio error is cancelled.")),
            false => {
                let panic_error = join_error.into_panic();
                match panic_error.downcast_ref::<&'static str>() {
                    None => match panic_error.downcast_ref::<String>() {
                        None => Err(ErrorCode::PanicError("Sorry, unknown panic message")),
                        Some(message) => Err(ErrorCode::PanicError(message.to_string())),
                    },
                    Some(message) => Err(ErrorCode::PanicError(message.to_string())),
                }
            }
        },
    }
}
