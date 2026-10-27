//! The journal-to-publisher wake-up protocol.
//!
//! The journal advances the durability watermark and then bumps a generation
//! counter; the publisher remembers the generation it last observed, drains the
//! log, and waits only while the generation is unchanged. A notification that
//! races with a drain therefore makes the next wait return immediately instead
//! of sleeping until another event arrives.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use super::sync::lock;

/// A lost-wakeup-free signal from the journal thread to the Event Publisher.
#[derive(Clone, Default)]
pub(crate) struct PublisherSignal {
    inner: Arc<PublisherSignalInner>,
}

#[derive(Default)]
struct PublisherSignalInner {
    generation: Mutex<u64>,
    ready: Condvar,
    /// Set once no further durability advances can occur (the journal has
    /// exited and shutdown called [`PublisherSignal::finish`]). The publisher
    /// drains to the final watermark and then exits.
    finished: AtomicBool,
}

impl PublisherSignal {
    pub(crate) fn generation(&self) -> u64 {
        *lock(&self.inner.generation)
    }

    /// Wake the Event Publisher after a durability watermark advance.
    ///
    /// The signal is advisory: the publisher always re-reads the watermark and
    /// the log, so a lost wake-up cannot lose an event.
    pub(crate) fn notify(&self) {
        let mut generation = lock(&self.inner.generation);
        *generation = generation.wrapping_add(1);
        drop(generation);
        self.inner.ready.notify_all();
    }

    /// Block until the generation moves past `seen`. Returns the new
    /// generation.
    pub(crate) fn wait_for_change(&self, seen: u64) -> u64 {
        let mut generation = lock(&self.inner.generation);
        while *generation == seen {
            generation = self
                .inner
                .ready
                .wait(generation)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        *generation
    }

    /// Tell the publisher no further durability advances can occur.
    ///
    /// Called only after the journal thread has been joined. The publisher
    /// drains to the final watermark before exiting.
    pub(crate) fn finish(&self) {
        self.inner.finished.store(true, Ordering::Release);
        self.notify();
    }

    /// Whether shutdown has declared the durability watermark final.
    pub(crate) fn finished(&self) -> bool {
        self.inner.finished.load(Ordering::Acquire)
    }
}
