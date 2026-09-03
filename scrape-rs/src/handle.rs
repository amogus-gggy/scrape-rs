use crate::errors::FetchLinkError;
use crate::queue::recover_lock;
use crate::state::State;
use std::sync::{Arc, Condvar, Mutex};

fn drain_ready(state: &mut State) -> Vec<(usize, Result<String, FetchLinkError>)> {
    let ready_indices = std::mem::take(&mut state.ready);
    let mut out = Vec::with_capacity(ready_indices.len());
    for index in ready_indices {
        if let Some(res) = state.results[index].take() {
            out.push((index, res));
        }
    }
    out
}

/// Handle to results from a background fetch pool.
pub struct FetchHandle {
    pub(crate) state: Arc<(Mutex<State>, Condvar)>,
}

impl FetchHandle {
    pub fn is_finished(&self) -> bool {
        let (lock, _) = &*self.state;
        let state = recover_lock(lock);
        state.completed >= state.total
    }

    pub fn try_results(&self) -> Option<Vec<Result<String, FetchLinkError>>> {
        if !self.is_finished() {
            return None;
        }
        Some(self.collect())
    }

    pub fn completed(&self) -> usize {
        let (lock, _) = &*self.state;
        recover_lock(lock).completed
    }

    pub fn total(&self) -> usize {
        let (lock, _) = &*self.state;
        recover_lock(lock).total
    }

    pub fn ready_results(&self) -> Vec<(usize, Result<String, FetchLinkError>)> {
        let (lock, _) = &*self.state;
        drain_ready(&mut recover_lock(lock))
    }

    pub fn wait_ready(&self) -> Vec<(usize, Result<String, FetchLinkError>)> {
        let (lock, cvar) = &*self.state;
        let mut state = recover_lock(lock);
        while state.ready.is_empty() && state.completed < state.total {
            state = match cvar.wait(state) {
                Ok(guard) => guard,
                Err(poisoned) => poisoned.into_inner(),
            };
        }
        drain_ready(&mut state)
    }

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

    fn collect(&self) -> Vec<Result<String, FetchLinkError>> {
        let (lock, _) = &*self.state;
        let mut state = recover_lock(lock);
        state.ready.clear();
        std::mem::take(&mut state.results)
            .into_iter()
            .flatten()
            .collect()
    }
}
