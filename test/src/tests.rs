use scrape_rs::{
    DEFAULT_TIMEOUT, default_agent, fetch_link, fetch_link_with_agent, fetch_many,
    fetch_many_with_agent,
};

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use std::thread::{self, JoinHandle};
use ureq::{Agent, http::Method};

// -------------------------------------------------------------------------
// Test HTTP server (std only, no external crates)
// -------------------------------------------------------------------------
#[allow(unused)]
struct TestServer {
    addr: String,
    shutdown: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl TestServer {
    #![allow(unused)]

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
#[allow(unused)]
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
        matches!(err, scrape_rs::structs::FetchLinkError::Http(ureq::Error::Timeout(_))),
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
        scrape_rs::structs::FetchLinkError::Http(ureq::Error::StatusCode(404))
    ));
}

// -------------------------------------------------------------------------
// fetch_many tests
// -------------------------------------------------------------------------
#[test]
fn fetch_many_success_ordered() {
    let srv = TestServer::spawn(|_, path, _| {
        // path like /page/1 -> return page-1
        let body = format!("body for {path}");
        (200, body)
    });
    let urls: Vec<String> = (0..5).map(|i| srv.url(&format!("/page/{i}"))).collect();
    let handle =
        fetch_many_with_agent(urls.clone(), NonZeroUsize::new(2).unwrap(), fast_agent())
            .unwrap();

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
    // need a fresh handle since previous was consumed via ready_results
    // Use a fresh server + fresh agent to avoid pooled-connection RST on Windows
    let srv2 = TestServer::spawn(|_, path, _| {
        let body = format!("body for {path}");
        (200, body)
    });
    let urls2: Vec<String> = (0..5).map(|i| srv2.url(&format!("/page/{i}"))).collect();
    let handle2 =
        fetch_many_with_agent(urls2, NonZeroUsize::new(2).unwrap(), fast_agent()).unwrap();
    let results = handle2.wait();
    assert_eq!(results.len(), 5);
    for (i, r) in results.into_iter().enumerate() {
        // Allow transient connection resets to be retried — treat as flake, unwrap with context
        assert!(
            r.is_ok(),
            "fetch_many ordered second batch failed at {i}: {r:?}"
        );
        assert_eq!(r.unwrap(), format!("body for /page/{i}"));
    }
}

#[test]
fn fetch_many_try_results_and_completed() {
    let srv = TestServer::spawn(|_, _, _| (200, "ok".to_string()));
    let urls: Vec<String> = (0..3).map(|i| srv.url(&format!("/{i}"))).collect();
    let handle =
        fetch_many_with_agent(urls, NonZeroUsize::new(1).unwrap(), fast_agent()).unwrap();

    // try_results should be None before completion (or eventually Some)
    // wait a bit to let workers start
    thread::sleep(Duration::from_millis(50));
    let completed = handle.completed();
    assert!(completed <= 3);

    let results = handle.wait();
    assert_eq!(results.len(), 3);
    assert!(handle.is_finished());
    assert_eq!(handle.completed(), 3);
    // try_results now returns Some
    // Note: wait() already consumed results via collect(), so try_results would be empty
    // but is_finished remains true. We test a new handle for try_results path.
    let urls2: Vec<String> = (0..2).map(|i| srv.url(&format!("/t/{i}"))).collect();
    let h2 = fetch_many_with_agent(urls2, NonZeroUsize::new(1).unwrap(), fast_agent()).unwrap();
    // spin until finished then try_results
    while !h2.is_finished() {
        thread::sleep(Duration::from_millis(5));
    }
    let opt = h2.try_results();
    assert!(opt.is_some());
    assert_eq!(opt.unwrap().len(), 2);
}

#[test]
fn fetch_many_timeout_mixed() {
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
    let urls = vec![srv.url("/fast"), srv.url("/slow")];
    let start = std::time::Instant::now();
    let handle = fetch_many_with_agent(urls, NonZeroUsize::new(2).unwrap(), agent).unwrap();
    let results = handle.wait();
    let elapsed = start.elapsed();
    assert_eq!(results.len(), 2);
    // fast should succeed, slow should timeout (order preserved)
    assert_eq!(results[0].as_ref().unwrap(), "fast");
    assert!(
        matches!(
            results[1].as_ref().unwrap_err(),
            scrape_rs::structs::FetchLinkError::Http(ureq::Error::Timeout(_))
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
fn fetch_many_default_timeout() {
    // default timeout is 5s — a 800ms delay must succeed with default agent
    let srv = TestServer::spawn(|_, path, _| {
        if path == "/delay1" {
            thread::sleep(Duration::from_millis(800));
            (200, "delayed ok".to_string())
        } else {
            (200, "ok".to_string())
        }
    });
    let urls = vec![srv.url("/delay1")];
    let start = std::time::Instant::now();
    let handle = fetch_many(urls, NonZeroUsize::new(1).unwrap()).unwrap();
    let results = handle.wait();
    let elapsed = start.elapsed();
    assert_eq!(results[0].as_ref().unwrap(), "delayed ok");
    // Should take ~800ms (the server delay) and be well below 5s default
    assert!(
        elapsed >= Duration::from_millis(700) && elapsed < Duration::from_secs(5),
        "default timeout test should take ~800ms, got {elapsed:?}"
    );
}

#[test]
fn fetch_many_too_many_threads() {
    let available = thread::available_parallelism().unwrap();
    let too_many = NonZeroUsize::new(available.get() + 1).unwrap();
    let err = match fetch_many(vec!["http://example.com".to_string()], too_many) {
        Ok(_) => panic!("expected TooManyThreads error"),
        Err(e) => e,
    };
    assert!(matches!(
        err,
        scrape_rs::structs::FetchError::TooManyThreads { .. }
    ));
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
    let quotes = scrape_rs::parsers::select_html(html, ".quote");
    assert_eq!(quotes.len(), 2);
    let first = scrape_rs::parsers::select_first(&quotes[0], ".text").unwrap();
    assert_eq!(first, "To be");
    let tags = scrape_rs::parsers::select_all(&quotes[0], ".tag");
    assert_eq!(tags, vec!["life", "love"]);
    assert!(scrape_rs::parsers::select_first(html, ".missing").is_none());
    assert!(scrape_rs::parsers::select_all(html, "???").is_empty()); // invalid selector
}

#[test]
fn parsers_via_fetch() {
    let srv = TestServer::spawn(|_, _, _| {
        let html = r#"<div class="quote"><span class="text">Hello</span><span class="author">World</span></div>"#;
        (200, html.to_string())
    });
    let html =
        fetch_link_with_agent(&fast_agent(), &srv.url("/"), Method::GET, None, None).unwrap();
    let quotes = scrape_rs::parsers::select_html(&html, ".quote");
    assert_eq!(quotes.len(), 1);
}

// -------------------------------------------------------------------------
// Queue / worker unit tests
// -------------------------------------------------------------------------
#[test]
fn queue_push_next_shutdown() {
    use scrape_rs::structs::{Queue, ScrapeJob};
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
    // sanity: agent was built with global timeout — indirectly verified by fetch_many_default_timeout
    let _ = agent;
}
