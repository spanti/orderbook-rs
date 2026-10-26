//! Small synchronization helpers shared by the runtime modules.

use std::sync::{Mutex, MutexGuard};

/// Lock `mutex`, recovering the guard from a poisoned mutex.
///
/// Runtime counters and slot state are not invariants that a panic can corrupt:
/// the worst case is a stale observation. Recovering the guard keeps a panic in
/// one worker from turning every later lock into a second panic.
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
