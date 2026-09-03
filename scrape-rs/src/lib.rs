mod errors;
mod handle;
pub mod parsers;
mod queue;
mod state;
pub mod structs;

pub use crate::errors::{FetchError, FetchLinkError};
pub use crate::handle::FetchHandle;
use crate::queue::{Queue, ScrapeJob, recover_lock, worker};
use crate::state::State;
use std::num::NonZeroUsize;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use ureq::Agent;
use ureq::http::Method;

/// Default timeout for all fetches via `init_worker_pool` / `fetch_link` (global).
/// Applied even when a caller-provided `Agent` has no timeout configured,
/// so a silent server can never hang a worker forever.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);

/// Default agent: single `Agent` shared by all workers with `timeout_global = 5s`.
pub fn default_agent() -> Agent {
    Agent::config_builder()
        .timeout_global(Some(DEFAULT_TIMEOUT))
        .build()
        .new_agent()
}

/// Initializes a worker pool of `num_threads` background fetchers.
///
/// Returns a [`WorkerPool`] that owns the queue and the worker threads. Push
/// URLs into `pool.queue` (or via [`WorkerPool::push`]) at any time, then read
/// results through the [`FetchHandle`] obtained from [`WorkerPool::handle`].
/// Call [`WorkerPool::close`] once no more URLs will be pushed so the workers
/// can exit. All workers share a single `ureq::Agent` (connection pool + cookies).
pub fn init_worker_pool(num_threads: NonZeroUsize) -> Result<WorkerPool, FetchError> {
    init_worker_pool_with_agent(num_threads, default_agent())
}

/// Same as `init_worker_pool`, but uses a caller-provided `Agent`.
/// Allows custom TLS/proxy/config while still sharing one pool across all workers.
pub fn init_worker_pool_with_agent(
    num_threads: NonZeroUsize,
    agent: Agent,
) -> Result<WorkerPool, FetchError> {
    let available =
        std::thread::available_parallelism().map_err(FetchError::ParallelismUnavailable)?;

    if num_threads.get() > available.get() {
        return Err(FetchError::TooManyThreads {
            requested: num_threads,
            available,
        });
    }

    let queue = Arc::new(Queue::new());
    let state: Arc<(Mutex<State>, Condvar)> = Arc::new((Mutex::new(State::new()), Condvar::new()));
    let mut workers = Vec::with_capacity(num_threads.get());

    for _ in 0..num_threads.get() {
        let queue = Arc::clone(&queue);
        let state = Arc::clone(&state);
        let agent = agent.clone();
        workers.push(thread::spawn(move || {
            worker(queue, move |job| {
                let Some(index) = job.index() else {
                    return false;
                };
                let result = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    fetch_link_with_agent(&agent, job.url(), Method::GET, None, None)
                })) {
                    Ok(result) => result,
                    Err(_) => Err(FetchLinkError::WorkerPanic),
                };
                let succeeded = result.is_ok();
                let (lock, cvar) = &*state;
                let mut state = recover_lock(lock);
                state.results[index] = Some(result);
                state.completed += 1;
                state.ready.push_back(index);
                drop(state);
                cvar.notify_all();
                succeeded
            });
        }));
    }

    Ok(WorkerPool {
        queue: FetchQueue {
            queue,
            state: Arc::clone(&state),
        },
        state,
        workers,
        closed: false,
    })
}

/// Fetch a batch of URLs concurrently using the default agent.
pub fn fetch_many(urls: Vec<String>, num_threads: NonZeroUsize) -> Result<FetchHandle, FetchError> {
    fetch_many_with_agent(urls, num_threads, default_agent())
}

/// Fetch a batch of URLs concurrently using a shared caller-provided agent.
/// Results retain the order of the input URLs when collected with `wait()`.
pub fn fetch_many_with_agent(
    urls: Vec<String>,
    num_threads: NonZeroUsize,
    agent: Agent,
) -> Result<FetchHandle, FetchError> {
    let pool = init_worker_pool_with_agent(num_threads, agent)?;
    for url in urls {
        pool.push(url);
    }
    let handle = pool.handle();
    pool.close();
    Ok(handle)
}

/// A queue of URLs to be fetched by a [`WorkerPool`].
///
/// Each `push` is assigned a stable index so results come back in the order
/// they were enqueued. The pool keeps running until [`WorkerPool::close`],
/// so URLs may be added incrementally.
pub struct FetchQueue {
    queue: Arc<Queue>,
    state: Arc<(Mutex<State>, Condvar)>,
}

impl FetchQueue {
    /// Enqueue `url` for fetching. Returns the index used to order the result.
    pub fn push(&self, url: String) -> usize {
        let (lock, cvar) = &*self.state;
        let mut state = recover_lock(lock);
        let index = state.results.len();
        state.results.push(None);
        state.total += 1;
        drop(state);
        cvar.notify_all();
        self.queue.push(ScrapeJob::new_indexed(url, index));
        index
    }
}

/// A background worker pool: owns the queue, the shared agent, and the threads.
/// Push URLs via [`WorkerPool::queue`] (or [`WorkerPool::push`]) and collect
/// them with the [`FetchHandle`] from [`WorkerPool::handle`]. Call
/// [`WorkerPool::close`] when done to shut the workers down.
pub struct WorkerPool {
    pub queue: FetchQueue,
    state: Arc<(Mutex<State>, Condvar)>,
    workers: Vec<JoinHandle<()>>,
    closed: bool,
}

impl WorkerPool {
    /// Enqueue `url` for fetching. See [`FetchQueue::push`].
    pub fn push(&self, url: String) -> usize {
        self.queue.push(url)
    }

    /// Obtain a handle to read results for URLs pushed so far.
    pub fn handle(&self) -> FetchHandle {
        FetchHandle {
            state: Arc::clone(&self.state),
        }
    }

    /// Signal that no more URLs will be pushed. This is non-blocking: the
    /// workers keep draining any already-queued URLs and then exit on their
    /// own — they are *not* killed mid-fetch. Results remain readable through
    /// the [`FetchHandle`] regardless. Calling `close` (or dropping the pool)
    /// is only needed so the worker threads can terminate instead of idling
    /// forever; it does not affect already-submitted work.
    ///
    /// Use [`close_and_join`](Self::close_and_join) instead when you want to
    /// block until the workers are actually gone.
    pub fn close(mut self) {
        self.shutdown();
    }

    /// Like [`close`](Self::close), but waits for every worker thread to exit.
    ///
    /// Useful when the caller wants the end of the work as a plain function
    /// return — collecting everything in one go, or shutting down cleanly —
    /// rather than leaving detached threads running behind its back. Do not
    /// call it before consuming incremental results: it only returns once all
    /// queued URLs have been fetched.
    pub fn close_and_join(mut self) {
        self.shutdown();
        for worker in std::mem::take(&mut self.workers) {
            let _ = worker.join();
        }
    }

    fn shutdown(&mut self) {
        if !self.closed {
            self.closed = true;
            self.queue.queue.shutdown(self.workers.len());
        }
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Fetch a single URL using the shared-agent pattern.
/// This is a convenience wrapper that creates a one-off agent. Prefer
/// `fetch_link_with_agent` when you already have an `Agent`.
pub fn fetch_link(
    url: &str,
    method: Method,
    body: Option<String>,
    content_type: Option<&str>,
) -> Result<String, FetchLinkError> {
    fetch_link_with_agent(&default_agent(), url, method, body, content_type)
}

/// Fetch a single URL using an explicit `Agent` (shared pool).
///
/// A per-request `DEFAULT_TIMEOUT` is always applied on top of the agent
/// config, so an `Agent` without any timeout configured still cannot hang.
pub fn fetch_link_with_agent(
    agent: &Agent,
    url: &str,
    method: Method,
    body: Option<String>,
    content_type: Option<&str>,
) -> Result<String, FetchLinkError> {
    let response = match method {
        Method::GET => with_timeout(agent, agent.get(url)).call()?,
        Method::DELETE => with_timeout(agent, agent.delete(url)).call()?,
        Method::HEAD => with_timeout(agent, agent.head(url)).call()?,
        Method::OPTIONS => with_timeout(agent, agent.options(url)).call()?,
        Method::POST => send_body(
            with_timeout(agent, agent.post(url)),
            body.as_deref(),
            content_type,
        )?,
        Method::PUT => send_body(
            with_timeout(agent, agent.put(url)),
            body.as_deref(),
            content_type,
        )?,
        Method::PATCH => send_body(
            with_timeout(agent, agent.patch(url)),
            body.as_deref(),
            content_type,
        )?,
        // The library does not implement this method — report it clearly,
        // do not fake an HTTP 405 as if it came from the server.
        _ => return Err(FetchLinkError::UnsupportedMethod(method)),
    };
    Ok(response.into_body().read_to_string()?)
}

/// Bound the request end-to-end by `DEFAULT_TIMEOUT`.
/// If the agent already has a shorter global timeout configured, the stricter
/// one wins; an agent without any timeout still gets `DEFAULT_TIMEOUT`, so a
/// silently-hanging server can never block a call forever.
fn with_timeout<S>(agent: &Agent, builder: ureq::RequestBuilder<S>) -> ureq::RequestBuilder<S> {
    let effective = match agent.config().timeouts().global {
        Some(configured) if configured < DEFAULT_TIMEOUT => Some(configured),
        _ => Some(DEFAULT_TIMEOUT),
    };
    builder.config().timeout_global(effective).build()
}

fn send_body(
    builder: ureq::RequestBuilder<ureq::typestate::WithBody>,
    body: Option<&str>,
    content_type: Option<&str>,
) -> Result<ureq::http::Response<ureq::Body>, ureq::Error> {
    let builder = match content_type {
        Some(content_type) => builder.header("Content-Type", content_type),
        None => builder,
    };
    match body {
        Some(body) => builder.send(body),
        None => builder.send_empty(),
    }
}
