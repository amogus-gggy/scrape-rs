use crate::FetchLinkError;
use std::collections::VecDeque;

pub(crate) struct State {
    pub(crate) results: Vec<Option<Result<String, FetchLinkError>>>,
    pub(crate) completed: usize,
    pub(crate) total: usize,
    pub(crate) ready: VecDeque<usize>,
}

impl State {
    pub(crate) fn new() -> Self {
        Self {
            results: Vec::new(),
            completed: 0,
            total: 0,
            ready: VecDeque::new(),
        }
    }
}
