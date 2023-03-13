use std::future::Future;

use crate::runtime::runtime::RuntimeState;

pub fn block_on<F: Future>(f: F) -> F::Output {
    assert!(
        !RuntimeState::is_global(),
        "Cannot call block_on in global runtime"
    );
    futures::executor::block_on(f)
}

pub unsafe fn uncheck_block_on<F: Future>(f: F) -> F::Output {
    futures::executor::block_on(f)
}
