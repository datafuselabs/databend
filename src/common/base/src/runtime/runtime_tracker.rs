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

//! Memory allocation stat tracker.
//!
//! Every thread has a thread local  `ThreadTracker` instance, inside which, there is a reference
//! to a `MemStat`.  A `MemStat` can be shared by multiple `ThreadTracker`.  A
//! `ThreadTracker` buffers allocation stat and flushes it to its `MemStat` when necessary.
//!
//! `MemStat` is organized in a hierarchical structure: when allocation stat is flushed to a
//! `MemStat`, it will then report the stat to its parent, and so on.  Finally if its parent
//! is `None`, it flushes stat to `GLOBAL_TRACKER`, which is the root of the `MemStat` tree.
//!
//! A reporting path could be `T3 -> M4 -> M2 -> G`, or `T1 -> G`:
//!
//! ```text
//! GLOBAL_TRACKER(G) <--- ThreadTracker(T1)
//! ^     ^      ^
//! |     |      '-------- ThreadTracker(T2)
//! |     `--------MemStat(M2)
//! |                 ^     ^
//! MemStat(M1)       |     '-----MemStat(M4) <--- ThreadTracker(T3)
//!                   |                    ^
//!                   MemStat(M3)          '------ ThreadTracker(T4)
//! ```
//!
//! A ThreadTracker that points to `GLOBAL_TRACKER` is installed automatically for every thread,
//! unless an application replaced it via `ThreadTracker::swap_with()`.
//!
//! An `TrackedFuture` has a embedded `ThreadTracker` installed for its inner `Future`.
//! When `TrackedFuture` is `poll()`ed, its `ThreadTracker` is installed to the running thread
//! and will be restored when `poll()` returns.

use std::alloc::AllocError;
use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;

use pin_project_lite::pin_project;

use crate::runtime::memory::MemStat;
use crate::runtime::memory::OutOfLimit;
use crate::runtime::memory::StatBuffer;
use crate::runtime::metrics::ScopedRegistry;
use crate::runtime::profile::Profile;

// For implemented and needs to call drop, we cannot use the attribute tag thread local.
// https://play.rust-lang.org/?version=nightly&mode=debug&edition=2021&gist=ea33533387d401e86423df1a764b5609
thread_local! {
    static TRACKER: RefCell<ThreadTracker> = const { RefCell::new(ThreadTracker::empty()) };
}

pub struct LimitMemGuard {
    saved: bool,
}

impl LimitMemGuard {
    pub fn enter_unlimited() -> Self {
        Self {
            saved: StatBuffer::current().set_unlimited_flag(true),
        }
    }

    pub fn enter_limited() -> Self {
        Self {
            saved: StatBuffer::current().set_unlimited_flag(false),
        }
    }

    pub(crate) fn is_unlimited() -> bool {
        StatBuffer::current().is_unlimited()
    }
}

impl Drop for LimitMemGuard {
    fn drop(&mut self) {
        StatBuffer::current().set_unlimited_flag(self.saved);
    }
}

/// A per-thread tracker that tracks memory usage stat.
pub struct ThreadTracker {
    out_of_limit_desc: Option<String>,
    pub(crate) payload: TrackingPayload,
}

#[derive(Clone)]
pub struct TrackingPayload {
    pub profile: Option<Arc<Profile>>,
    pub mem_stat: Option<Arc<MemStat>>,
    pub metrics: Option<Arc<ScopedRegistry>>,
}

pub struct TrackingGuard {
    saved: TrackingPayload,
}

impl Drop for TrackingGuard {
    fn drop(&mut self) {
        let _ = StatBuffer::current().flush::<false>(0);

        TRACKER.with(|x| {
            let mut thread_tracker = x.borrow_mut();
            std::mem::swap(&mut thread_tracker.payload, &mut self.saved);
        });
    }
}

pub struct TrackingFuture<T: Future> {
    inner: Pin<Box<T>>,
    tracking_payload: TrackingPayload,
}

impl<T: Future> TrackingFuture<T> {
    pub fn create(inner: T, tracking_payload: TrackingPayload) -> TrackingFuture<T> {
        TrackingFuture {
            inner: Box::pin(inner),
            tracking_payload,
        }
    }
}

impl<T: Future> Future for TrackingFuture<T> {
    type Output = T::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let _guard = ThreadTracker::tracking(self.tracking_payload.clone());
        self.inner.as_mut().poll(cx)
    }
}

impl Drop for ThreadTracker {
    fn drop(&mut self) {
        StatBuffer::current().mark_destroyed();
    }
}

/// A memory stat tracker with buffer.
///
/// Every ThreadTracker belongs to one MemStat.
/// A MemStat might receive memory stat from more than one ThreadTracker.
impl ThreadTracker {
    pub(crate) const fn empty() -> Self {
        Self {
            // mem_stat: None,
            out_of_limit_desc: None,
            payload: TrackingPayload {
                profile: None,
                metrics: None,
                mem_stat: None,
            },
        }
    }

    // rust style thread local is always lazy init.
    // need to be called immediately after the threads start
    pub fn init() {
        TRACKER.with(|x| {
            let _ = x.borrow_mut();
        })
    }

    pub(crate) fn with<F, R>(f: F) -> R
    where F: FnOnce(&RefCell<ThreadTracker>) -> R {
        TRACKER.with(f)
    }

    pub fn tracking(tracking_payload: TrackingPayload) -> TrackingGuard {
        let mut guard = TrackingGuard {
            saved: tracking_payload,
        };
        let _ = StatBuffer::current().flush::<false>(0);

        TRACKER.with(move |x| {
            let mut thread_tracker = x.borrow_mut();
            std::mem::swap(&mut thread_tracker.payload, &mut guard.saved);

            guard
        })
    }

    pub fn tracking_future<T: Future>(future: T) -> TrackingFuture<T> {
        TRACKER.with(move |x| TrackingFuture::create(future, x.borrow().payload.clone()))
    }

    pub fn tracking_function<F, T>(f: F) -> impl FnOnce() -> T
    where F: FnOnce() -> T {
        TRACKER.with(move |x| {
            let payload = x.borrow().payload.clone();
            move || {
                let _guard = ThreadTracker::tracking(payload);
                f()
            }
        })
    }

    pub fn new_tracking_payload() -> TrackingPayload {
        TRACKER.with(|x| x.borrow().payload.clone())
    }

    /// Replace the `out_of_limit_desc` with the current thread's.
    pub fn replace_error_message(desc: Option<String>) -> Option<String> {
        TRACKER.with(|v: &RefCell<ThreadTracker>| {
            let mut borrow_mut = v.borrow_mut();
            let old = borrow_mut.out_of_limit_desc.take();
            borrow_mut.out_of_limit_desc = desc;
            old
        })
    }

    /// Accumulate stat about allocated memory.
    ///
    /// `size` is the positive number of allocated bytes.
    #[inline]
    pub fn alloc(size: i64) -> Result<(), AllocError> {
        if let Err(out_of_limit) = StatBuffer::current().alloc(size) {
            // https://play.rust-lang.org/?version=stable&mode=debug&edition=2021&gist=03d21a15e52c7c0356fca04ece283cf9
            if !std::thread::panicking() && !LimitMemGuard::is_unlimited() {
                let _guard = LimitMemGuard::enter_unlimited();
                ThreadTracker::replace_error_message(Some(format!("{:?}", out_of_limit)));
                return Err(AllocError);
            }
        }

        Ok(())
    }

    /// Accumulate deallocated memory.
    ///
    /// `size` is positive number of bytes of the memory to deallocate.
    #[inline]
    pub fn dealloc(size: i64) {
        StatBuffer::current().dealloc(size)
    }

    pub fn movein_memory(size: i64) {
        TRACKER.with(|tracker| {
            let thread_tracker = tracker.borrow();
            if let Some(mem_stat) = &thread_tracker.payload.mem_stat {
                mem_stat.movein_memory(size);
            }
        })
    }

    pub fn moveout_memory(size: i64) {
        TRACKER.with(|tracker| {
            let thread_tracker = tracker.borrow();
            if let Some(mem_stat) = &thread_tracker.payload.mem_stat {
                mem_stat.moveout_memory(size);
            }
        })
    }

    pub fn record_memory<const ROLLBACK: bool>(batch: i64, cur: i64) -> Result<(), OutOfLimit> {
        let has_thread_local = TRACKER.try_with(|tracker: &RefCell<ThreadTracker>| {
            // We need to ensure no heap memory alloc or dealloc. it will cause panic of borrow recursive call.
            let tracker = tracker.borrow();
            match tracker.payload.mem_stat.as_deref() {
                None => Ok(()),
                Some(mem_stat) => mem_stat.record_memory::<ROLLBACK>(batch, cur),
            }
        });

        match has_thread_local {
            Ok(Ok(_)) | Err(_) => Ok(()),
            Ok(Err(oom)) => Err(oom),
        }
    }
}

pin_project! {
    /// A [`Future`] that enters its thread tracker when being polled.
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct UnlimitedFuture<T> {
        #[pin]
        inner: T,
    }
}

impl<T> UnlimitedFuture<T> {
    pub fn create(inner: T) -> UnlimitedFuture<T> {
        UnlimitedFuture::<T> { inner }
    }
}

impl<T: Future> Future for UnlimitedFuture<T> {
    type Output = T::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let _guard = LimitMemGuard::enter_unlimited();

        let this = self.project();
        this.inner.poll(cx)
    }
}
