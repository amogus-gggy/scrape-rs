use crate::State;
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use thiserror::Error;
use ureq::http::Method;

/// Recover the data from a poisoned mutex instead of panicking.
/// A panic in one worker must never take down or hang unrelated threads.
pub(crate) fn recover_lock<T>(lock: &Mutex<T>) -> MutexGuard<'_, T> {
    lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub struct ScrapeJob {
    url: String,
    status: JobStatus,
    index: Option<usize>,
}

impl ScrapeJob {
    pub fn new(url: String) -> Self {
        Self {
            url,
            status: JobStatus::Pending,
            index: None,
        }
    }

    pub fn new_indexed(url: String, index: usize) -> Self {
        Self {
            url,
            status: JobStatus::Pending,
            index: Some(index),
        }
    }

    pub fn url(&self) -> &str {
        &self.url
    }
    pub fn status(&self) -> &JobStatus {
        &self.status
    }
    pub fn index(&self) -> Option<usize> {
        self.index
    }
    pub fn set_status(&mut self, status: JobStatus) {
        self.status = status;
    }
}

#[derive(Default)]
pub struct Queue {
    jobs: Mutex<VecDeque<Option<ScrapeJob>>>,
    avaliable: Condvar,
}

impl Queue {
    pub fn new() -> Self {
        Self {
            jobs: Mutex::new(VecDeque::new()),
            avaliable: Condvar::new(),
        }
    }

    pub fn push(&self, job: ScrapeJob) {
        let mut jobs = recover_lock(&self.jobs);
        jobs.push_back(Some(job));
        self.avaliable.notify_one();
    }

    pub fn next(&self) -> Option<ScrapeJob> {
        let mut jobs = recover_lock(&self.jobs);
        loop {
            match jobs.pop_front() {
                Some(Some(job)) => return Some(job),
                Some(None) => return None,
                None => {}
            }
            // Poison recovery on the condvar wait as well.
            jobs = match self
                .avaliable
                .wait_timeout(jobs, std::time::Duration::from_secs(1))
            {
                Ok((guard, _)) => guard,
                Err(poisoned) => {
                    let (guard, _) = poisoned.into_inner();
                    guard
                }
            };
        }
    }

    pub fn shutdown(&self, workers: usize) {
        let mut jobs = recover_lock(&self.jobs);
        for _ in 0..workers {
            jobs.push_back(None);
        }
        self.avaliable.notify_all();
    }
}

/// Runs jobs from `queue` until shutdown.
/// `handler` must return `true` when the job succeeded (`Finished`) and
/// `false` when it failed (`Failed`). A panic inside the handler is caught:
/// the job is marked `Failed` and the worker keeps serving the queue instead
/// of silently dying.
pub fn worker(queue: Arc<Queue>, mut handler: impl FnMut(&mut ScrapeJob) -> bool + Send + 'static) {
    while let Some(mut job) = queue.next() {
        job.set_status(JobStatus::Running);
        let ok = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler(&mut job)))
            .unwrap_or(false);
        let status = if ok {
            JobStatus::Finished
        } else {
            JobStatus::Failed
        };
        job.set_status(status);
    }
}

pub enum JobStatus {
    Running,
    Pending,
    Finished,
    Failed,
    Waiting,
}

#[derive(Error, Debug)]
pub enum FetchError {
    #[error("Requested {requested} threads, but avaliable only {available}")]
    TooManyThreads {
        requested: NonZeroUsize,
        available: NonZeroUsize,
    },
    #[error("Could not determine the number of available threads: {0}")]
    ParallelismUnavailable(#[from] std::io::Error),
}

/// Error returned by `fetch_link` / `fetch_link_with_agent` and stored in
/// `fetch_many` results.
#[derive(Error, Debug)]
pub enum FetchLinkError {
    /// The library simply does not implement this method — say so explicitly
    /// instead of masquerading as an HTTP 405 from the server.
    #[error(
        "method {0} is not implemented in scrape-rs (supported: GET, POST, PUT, PATCH, DELETE, HEAD, OPTIONS)"
    )]
    UnsupportedMethod(Method),
    #[error("worker thread panicked while fetching the URL")]
    WorkerPanic,
    #[error(transparent)]
    Http(#[from] ureq::Error),
}

/// Takes every completed-but-unread result out of `state`.
/// O(1) swap of the ready queue; no scan over `results`.
fn drain_ready(state: &mut State) -> Vec<(usize, Result<String, FetchLinkError>)> {
    let ready_indices = std::mem::take(&mut state.ready);
    let mut out = Vec::with_capacity(ready_indices.len());
    for index in ready_indices {
        // each ready index is guaranteed to have Some result
        if let Some(res) = state.results[index].take() {
            out.push((index, res));
        }
    }
    out
}

/// Handle to a background fetch: results are collected via `wait()` (blocks
/// until done) or `try_results()`. The total number of jobs is dynamic — it
/// grows as URLs are pushed to the owning [`WorkerPool`](crate::WorkerPool).
pub struct FetchHandle {
    pub(crate) state: Arc<(Mutex<State>, Condvar)>,
}

impl FetchHandle {
    /// true if all tasks have finished
    pub fn is_finished(&self) -> bool {
        let (lock, _) = &*self.state;
        let state = recover_lock(lock);
        state.completed >= state.total
    }

    /// Returns results if everything is ready, otherwise None (does not block).
    ///
    /// Results already taken by [`ready_results`](Self::ready_results) are not
    /// returned again.
    pub fn try_results(&self) -> Option<Vec<Result<String, FetchLinkError>>> {
        if !self.is_finished() {
            return None;
        }
        Some(self.collect())
    }

    /// How many tasks have finished already (does not block)
    pub fn completed(&self) -> usize {
        let (lock, _) = &*self.state;
        recover_lock(lock).completed
    }

    /// How many tasks have been enqueued so far (does not block)
    pub fn total(&self) -> usize {
        let (lock, _) = &*self.state;
        recover_lock(lock).total
    }

    /// Takes and returns the already-completed results (as they arrive).
    /// Each completed result is returned exactly once — O(1) queue drain, not O(n) scan.
    pub fn ready_results(&self) -> Vec<(usize, Result<String, FetchLinkError>)> {
        let (lock, _) = &*self.state;
        drain_ready(&mut recover_lock(lock))
    }

    /// Blocks until at least one result is ready, then takes everything that
    /// arrived, exactly like [`ready_results`](Self::ready_results).
    ///
    /// Returns an empty vector only when every enqueued job has completed, so
    /// a consumer loop is a plain `while` with no sleeping:
    ///
    /// ```ignore
    /// loop {
    ///     for (index, res) in handle.wait_ready() {
    ///         // ...
    ///     }
    ///     if handle.is_finished() {
    ///         break;
    ///     }
    /// }
    /// ```
    pub fn wait_ready(&self) -> Vec<(usize, Result<String, FetchLinkError>)> {
        let (lock, cvar) = &*self.state;
        let mut state = recover_lock(lock);
        // `total` grows as URLs are pushed and `push` notifies the condvar, so
        // waiting here cannot miss work that is enqueued after the check.
        while state.ready.is_empty() && state.completed < state.total {
            state = match cvar.wait(state) {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
        }
        drain_ready(&mut state)
    }

    /// Blocks until all tasks finish and returns the results in original order.
    ///
    /// Results already taken by [`ready_results`](Self::ready_results) are not
    /// returned again.
    pub fn wait(&self) -> Vec<Result<String, FetchLinkError>> {
        let (lock, cvar) = &*self.state;
        let mut state = recover_lock(lock);
        while state.completed < state.total {
            state = match cvar.wait(state) {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
        }
        drop(state);
        self.collect()
    }

    /// Drains every result still held by the state, in enqueue order.
    ///
    /// Results already handed out by [`ready_results`](Self::ready_results) are
    /// gone from the state and are simply skipped: taking a result twice is not
    /// possible, and mixing the two APIs must not panic.
    fn collect(&self) -> Vec<Result<String, FetchLinkError>> {
        let (lock, _) = &*self.state;
        let mut state = recover_lock(lock);
        // Clear ready since we drain everything in order
        state.ready.clear();
        let results = std::mem::take(&mut state.results);
        results.into_iter().flatten().collect()
    }
}
