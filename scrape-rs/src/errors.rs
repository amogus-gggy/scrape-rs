use std::num::NonZeroUsize;
use thiserror::Error;
use ureq::http::Method;

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
