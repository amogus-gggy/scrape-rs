pub mod parsers;
pub mod structs;

use crate::structs::{FetchError, FetchLinkError, recover_lock};
use crate::structs::*;
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use structs::{Queue, ScrapeJob, worker};
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

pub(crate) struct State {
    results: Vec<Option<Result<String, FetchLinkError>>>,
    completed: usize,
    total: usize,
    ready: VecDeque<usize>,
}

impl State {
    fn new() -> Self {
        State {
            results: Vec::new(),
            completed: 0,
            total: 0,
            ready: VecDeque::new(),
        }
    }
}

/// Initializes a worker pool of `num_threads` background fetchers.
///
/// Returns a [`WorkerPool`] that owns the queue and the worker threads. Push
/// URLs into `pool.queue` (or via [`WorkerPool::push`]) at any time, then read
/// results through the [`FetchHandle`] obtained from [`WorkerPool::handle`].
/// Call [`WorkerPool::close`] once no more URLs will be pushed so the workers
/// can exit. All workers share a single `ureq::Agent` (connection pool + cookies).
pub fn init_worker_pool(num_threads: NonZeroUsize) -> Result<WorkerPool, FetchError> {
    let agent = default_agent();
    init_worker_pool_with_agent(num_threads, agent)
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
    let state: Arc<(Mutex<State>, Condvar)> =
        Arc::new((Mutex::new(State::new()), Condvar::new()));

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
                // A panic inside the fetch must not kill the counter:
                // record it as a failed result so `wait()` can finish.
                let res = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    fetch_link_with_agent(&agent, job.url(), Method::GET, None, None)
                })) {
                    Ok(res) => res,
                    Err(_) => Err(FetchLinkError::WorkerPanic),
                };
                let ok = res.is_ok();
                let (lock, cvar) = &*state;
                let mut guard = recover_lock(lock);
                guard.results[index] = Some(res);
                guard.completed += 1;
                guard.ready.push_back(index);
                drop(guard);
                cvar.notify_all();
                ok
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
        // Wake any `wait()` that might otherwise miss the new `total`.
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
            // A worker cannot propagate a panic — `worker()` catches them and
            // marks the job failed — so a join error is nothing to act on.
            let _ = worker.join();
        }
    }
}

impl WorkerPool {
    fn shutdown(&mut self) {
        if !self.closed {
            self.closed = true;
            // Push one `None` per worker so each `queue.next()` returns and the
            // worker loop ends — after it has processed every real job ahead of
            // the marker in the queue.
            self.queue.queue.shutdown(self.workers.len());
        }
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        // If the caller never called `close`, still let the workers drain and
        // exit instead of blocking on the queue forever. Detached threads keep
        // running until their queued work is done; the shared `state` (held
        // independently by every `FetchHandle`) outlives them.
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
    let agent = default_agent();
    fetch_link_with_agent(&agent, url, method, body, content_type)
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
        Method::POST => {
            send_body(with_timeout(agent, agent.post(url)), body.as_deref(), content_type)?
        }
        Method::PUT => send_body(with_timeout(agent, agent.put(url)), body.as_deref(), content_type)?,
        Method::PATCH => {
            send_body(with_timeout(agent, agent.patch(url)), body.as_deref(), content_type)?
        }
        // The library does not implement this method — report it clearly,
        // do not fake an HTTP 405 as if it came from the server.
        _ => return Err(FetchLinkError::UnsupportedMethod(method)),
    };

    response.into_body().read_to_string()
}

/// Bound the request end-to-end by `DEFAULT_TIMEOUT`.
/// If the agent already has a shorter global timeout configured, the stricter
/// one wins; an agent without any timeout still gets `DEFAULT_TIMEOUT`, so a
/// silently-hanging server can never block a call forever.
fn with_timeout<S>(
    agent: &Agent,
    builder: ureq::RequestBuilder<S>,
) -> ureq::RequestBuilder<S> {
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
        Some(ct) => builder.header("Content-Type", ct),
        None => builder,
    };
    match body {
        Some(b) => builder.send(b),
        None => builder.send_empty(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::structs::FetchLinkError;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread::JoinHandle;
    use std::time::Duration;

    // -------------------------------------------------------------------------
    // Test HTTP server (std only, no external crates)
    // -------------------------------------------------------------------------
    struct TestServer {
        addr: String,
        shutdown: Arc<AtomicBool>,
        handle: Option<JoinHandle<()>>,
    }

    impl TestServer {
        /// `handler` is called for each request with (method, path, body) and
        /// must return (status_code, response_body).
        fn spawn<F>(handler: F) -> Self
        where
            F: Fn(&str, &str, &str) -> (u16, String) + Send + Sync + 'static,
        {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind test server");
            let addr = listener.local_addr().unwrap().to_string();
            listener.set_nonblocking(true).expect("set_nonblocking");
            let handler = Arc::new(handler);
            let shutdown = Arc::new(AtomicBool::new(false));
            let shutdown_clone = Arc::clone(&shutdown);

            let handle = thread::spawn(move || {
                while !shutdown_clone.load(Ordering::Relaxed) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let h = Arc::clone(&handler);
                            thread::spawn(move || handle_client(stream, &*h));
                        }
                        Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(e) => {
                            eprintln!("test server accept error: {e}");
                            break;
                        }
                    }
                }
            });

            Self {
                addr,
                shutdown,
                handle: Some(handle),
            }
        }

        fn url(&self, path: &str) -> String {
            format!("http://{}{}", self.addr, path)
        }
    }

    impl Drop for TestServer {
        fn drop(&mut self) {
            self.shutdown.store(true, Ordering::Relaxed);
            // Unblock accept() by connecting once
            let _ = TcpStream::connect(&self.addr);
            if let Some(h) = self.handle.take() {
                let _ = h.join();
            }
        }
    }

    fn handle_client<F>(mut stream: TcpStream, handler: &F)
    where
        F: Fn(&str, &str, &str) -> (u16, String),
    {
        // Read request (simple, up to 16 KiB headers + body)
        let mut buf = vec![0u8; 16384];
        let n = match stream.read(&mut buf) {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        let req = String::from_utf8_lossy(&buf[..n]).to_string();

        // Parse request line: "GET /path HTTP/1.1"
        let mut lines = req.lines();
        let request_line = lines.next().unwrap_or("");
        let mut parts = request_line.split_whitespace();
        let method = parts.next().unwrap_or("GET");
        let path = parts.next().unwrap_or("/");

        // Check Content-Length for body
        let mut content_length: usize = 0;
        for line in req.lines().skip(1) {
            if line.is_empty() {
                break;
            }
            if let Some(v) = line.strip_prefix("Content-Length:") {
                content_length = v.trim().parse().unwrap_or(0);
            }
            if let Some(v) = line.strip_prefix("content-length:") {
                content_length = v.trim().parse().unwrap_or(0);
            }
        }
        // Body is after \r\n\r\n
        let body = if let Some(idx) = req.find("\r\n\r\n") {
            let start = idx + 4;
            let raw = &buf[start..n];
            // If body was truncated (large), try to read remaining
            let mut body_bytes = raw.to_vec();
            while body_bytes.len() < content_length {
                let mut extra = vec![0u8; 4096];
                match stream.read(&mut extra) {
                    Ok(0) => break,
                    Ok(m) => body_bytes.extend_from_slice(&extra[..m]),
                    Err(_) => break,
                }
            }
            String::from_utf8_lossy(&body_bytes[..content_length.min(body_bytes.len())]).to_string()
        } else {
            String::new()
        };

        let (status, resp_body) = handler(method, path, &body);
        let reason = match status {
            200 => "OK",
            201 => "Created",
            404 => "Not Found",
            500 => "Internal Server Error",
            _ => "OK",
        };
        let response = format!(
            "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nContent-Type: text/html\r\nConnection: close\r\n\r\n{}",
            resp_body.len(),
            resp_body
        );
        let _ = stream.write_all(response.as_bytes());
        let _ = stream.flush();
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------
    fn fast_agent() -> Agent {
        Agent::config_builder()
            .timeout_global(Some(Duration::from_millis(300)))
            .build()
            .new_agent()
    }

    // -------------------------------------------------------------------------
    // fetch_link tests
    // -------------------------------------------------------------------------
    #[test]
    fn fetch_link_success() {
        let srv = TestServer::spawn(|method, path, _body| {
            assert_eq!(method, "GET");
            match path {
                "/" => (200, "hello world".to_string()),
                _ => (404, "not found".to_string()),
            }
        });
        let body = fetch_link_with_agent(&fast_agent(), &srv.url("/"), Method::GET, None, None)
            .expect("fetch should succeed");
        assert_eq!(body, "hello world");
    }

    #[test]
    fn fetch_link_default_agent_success() {
        // default_agent has 5s timeout — request that responds immediately must succeed
        let srv = TestServer::spawn(|_, path, _| match path {
            "/" => (200, "default ok".to_string()),
            _ => (404, "".to_string()),
        });
        let body = fetch_link(&srv.url("/"), Method::GET, None, None).expect("default fetch");
        assert_eq!(body, "default ok");
    }

    #[test]
    fn fetch_link_timeout() {
        let srv = TestServer::spawn(|_, path, _| {
            if path == "/delay" {
                thread::sleep(Duration::from_secs(2));
                (200, "too late".to_string())
            } else {
                (200, "fast".to_string())
            }
        });
        let agent = Agent::config_builder()
            .timeout_global(Some(Duration::from_millis(400)))
            .build()
            .new_agent();
        let start = std::time::Instant::now();
        let err = fetch_link_with_agent(&agent, &srv.url("/delay"), Method::GET, None, None)
            .expect_err("should timeout");
        let elapsed = start.elapsed();
        assert!(
            matches!(err, FetchLinkError::Http(ureq::Error::Timeout(_))),
            "expected Timeout, got {err:?}"
        );
        // Must timeout quickly (~400ms), not wait for full 2s server delay
        assert!(
            elapsed >= Duration::from_millis(300) && elapsed < Duration::from_secs(1),
            "timeout should fire ~400ms, got {elapsed:?}"
        );
        // Server thread still sleeps 2s in background (detached) — test doesn't block on it
    }

    #[test]
    fn fetch_link_post_and_content_type() {
        let srv = TestServer::spawn(|method, path, body| {
            assert_eq!(method, "POST");
            assert_eq!(path, "/echo");
            // echo body back
            (200, format!("echo:{body}"))
        });
        let body = fetch_link_with_agent(
            &fast_agent(),
            &srv.url("/echo"),
            Method::POST,
            Some("payload123".to_string()),
            Some("text/plain"),
        )
        .unwrap();
        assert_eq!(body, "echo:payload123");
    }

    #[test]
    fn fetch_link_status_error() {
        let srv = TestServer::spawn(|_, path, _| match path {
            "/missing" => (404, "not found".to_string()),
            _ => (200, "ok".to_string()),
        });
        // ureq returns Error::StatusCode for 4xx/5xx by default
        let err =
            fetch_link_with_agent(&fast_agent(), &srv.url("/missing"), Method::GET, None, None)
                .unwrap_err();
        assert!(matches!(
            err,
            FetchLinkError::Http(ureq::Error::StatusCode(404))
        ));
    }

    #[test]
    fn fetch_link_unsupported_method_is_explicit() {
        let srv = TestServer::spawn(|_, _, _| (200, "ok".to_string()));
        let err = fetch_link_with_agent(&fast_agent(), &srv.url("/"), Method::TRACE, None, None)
            .unwrap_err();
        match err {
            FetchLinkError::UnsupportedMethod(ref m) => {
                assert_eq!(*m, Method::TRACE);
                // The message must say the method is not implemented here,
                // not pretend the server rejected it.
                let msg = err.to_string();
                assert!(msg.contains("not implemented in scrape-rs"), "{msg}");
            }
            other => panic!("expected UnsupportedMethod, got {other:?}"),
        }
    }

    #[test]
    fn fetch_link_timeout_applied_to_agent_without_timeout() {
        // An agent configured WITHOUT any timeout must still be bounded
        // by the per-request DEFAULT_TIMEOUT applied inside the library.
        let srv = TestServer::spawn(|_, path, _| {
            if path == "/hang" {
                thread::sleep(Duration::from_secs(30));
            }
            (200, "never".to_string())
        });
        let no_timeout_agent = Agent::config_builder().build().new_agent();
        let start = std::time::Instant::now();
        let err =
            fetch_link_with_agent(&no_timeout_agent, &srv.url("/hang"), Method::GET, None, None)
                .unwrap_err();
        let elapsed = start.elapsed();
        assert!(
            matches!(err, FetchLinkError::Http(ureq::Error::Timeout(_))),
            "expected Timeout, got {err:?}"
        );
        // Must bail out around DEFAULT_TIMEOUT (5s), not hang for 30s+
        assert!(
            elapsed < Duration::from_secs(10),
            "unconfigured agent should still time out at ~5s, got {elapsed:?}"
        );
    }

    // -------------------------------------------------------------------------
    // worker pool tests
    // -------------------------------------------------------------------------
    #[test]
    fn worker_pool_success_ordered() {
        let srv = TestServer::spawn(|_, path, _| {
            // path like /page/1 -> return page-1
            let body = format!("body for {path}");
            (200, body)
        });
        let urls: Vec<String> = (0..5).map(|i| srv.url(&format!("/page/{i}"))).collect();
        let pool = init_worker_pool_with_agent(
            NonZeroUsize::new(2).unwrap(),
            fast_agent(),
        )
        .unwrap();
        let handle = pool.handle();
        for url in urls {
            pool.push(url);
        }

        // poll ready_results until finished (also tests incremental API)
        let mut seen = 0;
        while !handle.is_finished() {
            let ready = handle.ready_results();
            seen += ready.len();
            // each ready result must correspond to original url content
            for (idx, res) in ready {
                assert!(res.is_ok());
                assert!(res.unwrap().contains(&format!("/page/{idx}")));
            }
            thread::sleep(Duration::from_millis(5));
        }
        // drain remaining
        seen += handle.ready_results().len();
        assert_eq!(seen, 5);

        // wait() must return ordered results
        // need a fresh pool since previous was consumed via ready_results
        // Use a fresh server + fresh agent to avoid pooled-connection RST on Windows
        let srv2 = TestServer::spawn(|_, path, _| {
            let body = format!("body for {path}");
            (200, body)
        });
        let urls2: Vec<String> = (0..5).map(|i| srv2.url(&format!("/page/{i}"))).collect();
        let pool2 =
            init_worker_pool_with_agent(NonZeroUsize::new(2).unwrap(), fast_agent()).unwrap();
        let handle2 = pool2.handle();
        for url in urls2 {
            pool2.push(url);
        }
        let results = handle2.wait();
        pool2.close();
        assert_eq!(results.len(), 5);
        for (i, r) in results.into_iter().enumerate() {
            // Allow transient connection resets to be retried — treat as flake, unwrap with context
            assert!(
                r.is_ok(),
                "worker pool ordered second batch failed at {i}: {r:?}"
            );
            assert_eq!(r.unwrap(), format!("body for /page/{i}"));
        }
    }

    #[test]
    fn wait_ready_streams_without_polling() {
        let srv = TestServer::spawn(|_, path, _| (200, format!("body for {path}")));
        let pool =
            init_worker_pool_with_agent(NonZeroUsize::new(2).unwrap(), fast_agent()).unwrap();
        let handle = pool.handle();
        for i in 0..6 {
            pool.push(srv.url(&format!("/page/{i}")));
        }
        pool.close();

        // No sleeping anywhere: wait_ready blocks until something is ready and
        // returns empty only once every job is accounted for.
        let mut seen = Vec::new();
        loop {
            seen.extend(handle.wait_ready());
            if handle.is_finished() {
                break;
            }
        }
        seen.extend(handle.wait_ready());

        assert_eq!(seen.len(), 6);
        seen.sort_by_key(|(index, _)| *index);
        for (index, res) in seen {
            assert!(res.is_ok(), "job {index} failed: {res:?}");
            assert_eq!(res.unwrap(), format!("body for /page/{index}"));
        }
    }

    #[test]
    fn wait_after_ready_results_does_not_panic() {
        let srv = TestServer::spawn(|_, path, _| (200, format!("body for {path}")));
        let pool =
            init_worker_pool_with_agent(NonZeroUsize::new(1).unwrap(), fast_agent()).unwrap();
        let handle = pool.handle();
        for i in 0..4 {
            pool.push(srv.url(&format!("/page/{i}")));
        }
        pool.close();

        // Take part of the results incrementally, then fall back to wait():
        // every result must be delivered exactly once, and nothing may panic.
        let taken = handle.wait_ready().len();
        let rest = handle.wait();
        assert_eq!(taken + rest.len(), 4);
        assert!(rest.iter().all(|r| r.is_ok()));
    }

    #[test]
    fn close_and_join_returns_after_workers_exit() {
        let srv = TestServer::spawn(|_, _, _| (200, "ok".to_string()));
        let pool =
            init_worker_pool_with_agent(NonZeroUsize::new(2).unwrap(), fast_agent()).unwrap();
        let handle = pool.handle();
        for i in 0..4 {
            pool.push(srv.url(&format!("/{i}")));
        }
        pool.close_and_join();

        // Joining means the work is done — no polling needed to observe it.
        assert!(handle.is_finished());
        assert_eq!(handle.completed(), 4);
        assert_eq!(handle.wait().len(), 4);
    }

    #[test]
    fn worker_pool_try_results_and_completed() {
        let srv = TestServer::spawn(|_, _, _| (200, "ok".to_string()));
        let pool =
            init_worker_pool_with_agent(NonZeroUsize::new(1).unwrap(), fast_agent()).unwrap();
        let handle = pool.handle();
        for i in 0..3 {
            pool.push(srv.url(&format!("/{i}")));
        }

        // try_results should be None before completion (or eventually Some)
        // wait a bit to let workers start
        thread::sleep(Duration::from_millis(50));
        let completed = handle.completed();
        assert!(completed <= 3);

        let results = handle.wait();
        assert_eq!(results.len(), 3);
        assert!(handle.is_finished());
        assert_eq!(handle.completed(), 3);
        pool.close();

        // try_results now returns Some
        // Note: wait() already consumed results via collect(), so try_results would be empty
        // but is_finished remains true. We test a new handle for try_results path.
        let pool2 =
            init_worker_pool_with_agent(NonZeroUsize::new(1).unwrap(), fast_agent()).unwrap();
        let handle2 = pool2.handle();
        for i in 0..2 {
            pool2.push(srv.url(&format!("/t/{i}")));
        }
        // spin until finished then try_results
        while !handle2.is_finished() {
            thread::sleep(Duration::from_millis(5));
        }
        let opt = handle2.try_results();
        assert!(opt.is_some());
        assert_eq!(opt.unwrap().len(), 2);
        pool2.close();
    }

    #[test]
    fn worker_pool_timeout_mixed() {
        let srv = TestServer::spawn(|_, path, _| {
            if path.contains("slow") {
                thread::sleep(Duration::from_secs(2));
                (200, "slow".to_string())
            } else {
                (200, "fast".to_string())
            }
        });
        let agent = Agent::config_builder()
            .timeout_global(Some(Duration::from_millis(350)))
            .build()
            .new_agent();
        let pool = init_worker_pool_with_agent(NonZeroUsize::new(2).unwrap(), agent).unwrap();
        let handle = pool.handle();
        pool.push(srv.url("/fast"));
        pool.push(srv.url("/slow"));
        let start = std::time::Instant::now();
        let results = handle.wait();
        pool.close();
        let elapsed = start.elapsed();
        assert_eq!(results.len(), 2);
        // fast should succeed, slow should timeout (order preserved)
        assert_eq!(results[0].as_ref().unwrap(), "fast");
        assert!(
            matches!(
                results[1].as_ref().unwrap_err(),
                FetchLinkError::Http(ureq::Error::Timeout(_))
            ),
            "second should timeout, got {:?}",
            results[1]
        );
        // Whole batch must finish ~350ms, not 2s
        assert!(
            elapsed >= Duration::from_millis(250) && elapsed < Duration::from_secs(1),
            "mixed timeout should finish ~350ms, got {elapsed:?}"
        );
    }

    #[test]
    fn worker_pool_default_timeout() {
        // default timeout is 5s — a 800ms delay must succeed with default agent
        let srv = TestServer::spawn(|_, path, _| {
            if path == "/delay1" {
                thread::sleep(Duration::from_millis(800));
                (200, "delayed ok".to_string())
            } else {
                (200, "ok".to_string())
            }
        });
        let pool = init_worker_pool(NonZeroUsize::new(1).unwrap()).unwrap();
        let handle = pool.handle();
        pool.push(srv.url("/delay1"));
        let start = std::time::Instant::now();
        let results = handle.wait();
        pool.close();
        let elapsed = start.elapsed();
        assert_eq!(results[0].as_ref().unwrap(), "delayed ok");
        // Should take ~800ms (the server delay) and be well below 5s default
        assert!(
            elapsed >= Duration::from_millis(700) && elapsed < Duration::from_secs(5),
            "default timeout test should take ~800ms, got {elapsed:?}"
        );
    }

    #[test]
    fn worker_pool_too_many_threads() {
        let available = thread::available_parallelism().unwrap();
        let too_many = NonZeroUsize::new(available.get() + 1).unwrap();
        let err = match init_worker_pool(too_many) {
            Ok(_) => panic!("expected TooManyThreads error"),
            Err(e) => e,
        };
        assert!(matches!(
            err,
            crate::structs::FetchError::TooManyThreads { .. }
        ));
    }

    #[test]
    fn worker_pool_incremental_push() {
        // URLs may be pushed after the handle is obtained and even after some
        // results have already arrived — the pool keeps running until close().
        let srv = TestServer::spawn(|_, path, _| {
            let body = format!("body for {path}");
            (200, body)
        });
        let pool = init_worker_pool_with_agent(NonZeroUsize::new(2).unwrap(), fast_agent()).unwrap();
        let handle = pool.handle();
        pool.push(srv.url("/a"));
        // wait for the first one to land
        while handle.completed() < 1 {
            thread::sleep(Duration::from_millis(5));
        }
        // now push more while the pool is already running
        pool.push(srv.url("/b"));
        pool.push(srv.url("/c"));
        let results = handle.wait();
        pool.close();
        assert_eq!(results.len(), 3);
        assert!(results.iter().all(|r| r.is_ok()));
    }

    // -------------------------------------------------------------------------
    // Parsers tests (run against real HTML from test server)
    // -------------------------------------------------------------------------
    #[test]
    fn parsers_select() {
        let html = r#"
            <div class="quote"><span class="text">To be</span><span class="author">Shakespeare</span><span class="tag">life</span><span class="tag">love</span></div>
            <div class="quote"><span class="text">I think</span><span class="author">Descartes</span></div>
        "#;
        let quotes = crate::parsers::select_html(html, ".quote");
        assert_eq!(quotes.len(), 2);
        let first = crate::parsers::select_first(&quotes[0], ".text").unwrap();
        assert_eq!(first, "To be");
        let tags = crate::parsers::select_all(&quotes[0], ".tag");
        assert_eq!(tags, vec!["life", "love"]);
        assert!(crate::parsers::select_first(html, ".missing").is_none());
        assert!(crate::parsers::select_all(html, "???").is_empty()); // invalid selector
    }

    #[test]
    fn parsers_via_fetch() {
        let srv = TestServer::spawn(|_, _, _| {
            let html = r#"<div class="quote"><span class="text">Hello</span><span class="author">World</span></div>"#;
            (200, html.to_string())
        });
        let html =
            fetch_link_with_agent(&fast_agent(), &srv.url("/"), Method::GET, None, None).unwrap();
        let quotes = crate::parsers::select_html(&html, ".quote");
        assert_eq!(quotes.len(), 1);
    }

    // -------------------------------------------------------------------------
    // Queue / worker unit tests
    // -------------------------------------------------------------------------
    #[test]
    fn queue_push_next_shutdown() {
        use crate::structs::{Queue, ScrapeJob};
        let q = Arc::new(Queue::new());
        q.push(ScrapeJob::new("http://a".to_string()));
        q.push(ScrapeJob::new("http://b".to_string()));
        assert_eq!(q.next().unwrap().url(), "http://a");
        assert_eq!(q.next().unwrap().url(), "http://b");

        // shutdown with 2 workers: next() should return None for each
        q.shutdown(2);
        assert!(q.next().is_none());
        assert!(q.next().is_none());
    }

    #[test]
    fn default_timeout_constant() {
        assert_eq!(DEFAULT_TIMEOUT, Duration::from_secs(5));
        let agent = default_agent();
        // sanity: agent was built with global timeout — indirectly verified by worker_pool_default_timeout
        let _ = agent;
    }
}
