use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

/// Recover the data from a poisoned mutex instead of panicking.
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
            jobs = match self
                .avaliable
                .wait_timeout(jobs, std::time::Duration::from_secs(1))
            {
                Ok((guard, _)) => guard,
                Err(poisoned) => poisoned.into_inner().0,
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

pub fn worker(queue: Arc<Queue>, mut handler: impl FnMut(&mut ScrapeJob) -> bool + Send + 'static) {
    while let Some(mut job) = queue.next() {
        job.set_status(JobStatus::Running);
        let ok = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| handler(&mut job)))
            .unwrap_or(false);
        job.set_status(if ok {
            JobStatus::Finished
        } else {
            JobStatus::Failed
        });
    }
}

pub enum JobStatus {
    Running,
    Pending,
    Finished,
    Failed,
    Waiting,
}
