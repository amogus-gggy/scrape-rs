<div align="center">

![BANNER](./banner.svg)

![License: AGPLv3](https://img.shields.io/badge/License-AGPLv3-blue?logo=opensourceinitiative&logoColor=white) ![Rust](https://img.shields.io/badge/Rust-1.95-orange?logo=rust) ![Scraping](https://img.shields.io/badge/Scraping-Multithreaded-blueviolet) ![Parsing](https://img.shields.io/badge/Parsing-CSS%20Selectors-E34F26?logo=html5&logoColor=white)

<img src="https://readme-typing-svg.demolab.com/?font=Fira+Code&pause=1000&color=EA580C&center=true&vCenter=true&width=700&lines=Multithreaded+Web+Scraping+in+Rust;Concurrent+Page+Fetching;CSS+Selector+HTML+Parsing;Incremental+Result+Processing" alt="Typing SVG" />

### A small, multithreaded web scraping library in Rust. It fetches web pages concurrently using a shared connection pool and provides simple CSS-selector-based HTML parsing

</div>

---

<div align="center">

# Features

</div>

- **Worker pool** — `init_worker_pool` spins up a configurable number of worker threads and returns a queue you can push URLs into at any time, plus a `FetchHandle` to read results
- **Incremental results** — process responses as soon as they arrive via `FetchHandle::ready_results`, or wait for everything with `wait()`
- **Single requests** — `fetch_link` for one-off GET/POST/PUT/PATCH/DELETE/HEAD/OPTIONS requests with optional body and content type
- **HTML parsing helpers** — `select_html`, `select_first`, and `select_all` for querying HTML with CSS selectors
- **Built-in timeouts** — 5 second default timeout, or bring your own `ureq::Agent`

---

<div align="center">

# Usage

</div>

Add to your `Cargo.toml`:

```toml
[dependencies]
scrape-rs = { path = "scrape-rs" }
```

<div align="center">

## Fetch many URLs concurrently

</div>

```rust
use std::num::NonZeroUsize;
use scrape_rs::{init_worker_pool, parsers::{select_first, select_html}};

let urls: Vec<String> = (1..=10)
    .map(|i| format!("https://quotes.toscrape.com/page/{i}"))
    .collect();

let pool = init_worker_pool(NonZeroUsize::new(4).unwrap()).unwrap();
let handle = pool.handle();

for url in urls {
    pool.push(url);
}
pool.close();

loop {
    // Handle results as they complete
    for (_, res) in handle.ready_results() {
        if let Ok(html) = res {
            if let Some(title) = select_first(&html, "h1") {
                println!("{title}");
            }
        }
    }
    if handle.is_finished() {
        break;
    }
    std::thread::sleep(std::time::Duration::from_millis(10));
}
```

<div align="center">

## Parse HTML with CSS selectors

</div>

```rust
use scrape_rs::parsers::{select_html, select_all, select_first};

for quote in select_html(html, ".quote") {
    let text = select_first(&quote, ".text").unwrap_or_default();
    let tags = select_all(&quote, ".tag");
    println!("{text} — tags: {tags:?}");
}
```

See the `test/` directory for a full working example.

---

<div align="center">

# Building & Testing

</div>

```sh
cargo test --workspace
```

Tests spin up a local HTTP server on `127.0.0.1` — no network access required.

---

<div align="center">

# License

</div>

This project is licensed under the [GNU Affero General Public License v3.0](LICENSE) (AGPL-3.0-or-later).
