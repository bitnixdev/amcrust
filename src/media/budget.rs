//! Logical codec/mux job reservations. No lock is held across allocation or I/O.
use super::{Error, PoolSnapshot};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct JobBudget(Arc<Mutex<PoolSnapshot>>);
impl JobBudget {
    pub fn new(jobs: usize, bytes: usize) -> Result<Self, Error> {
        if jobs == 0 || jobs > 64 || bytes == 0 || bytes > u32::MAX as usize {
            return Err(Error::Configuration);
        }
        Ok(Self(Arc::new(Mutex::new(PoolSnapshot {
            reserved_jobs: 0,
            job_limit: jobs,
            reserved_bytes: 0,
            byte_limit: bytes,
            refusals: 0,
        }))))
    }
    pub fn snapshot(&self) -> PoolSnapshot {
        *self.0.lock().unwrap()
    }
    fn reserve(&self, bytes: usize) -> Result<Charge, Error> {
        let mut s = self.0.lock().unwrap();
        if s.reserved_jobs == s.job_limit || bytes > s.byte_limit - s.reserved_bytes {
            s.refusals = s.refusals.saturating_add(1);
            return Err(Error::Limit);
        }
        s.reserved_jobs += 1;
        s.reserved_bytes += bytes;
        Ok(Charge {
            budget: self.clone(),
            bytes,
        })
    }
}
pub(super) struct Charge {
    budget: JobBudget,
    bytes: usize,
}
impl Drop for Charge {
    fn drop(&mut self) {
        let mut s = self.budget.0.lock().unwrap();
        s.reserved_jobs -= 1;
        s.reserved_bytes -= self.bytes;
    }
}
#[derive(Clone)]
pub(super) struct RolePool {
    local: JobBudget,
    aggregate: Option<JobBudget>,
}
impl RolePool {
    pub(super) fn new(jobs: usize, bytes: usize) -> Result<Self, Error> {
        Ok(Self {
            local: JobBudget::new(jobs, bytes)?,
            aggregate: None,
        })
    }
    pub(super) fn attach(mut self, parent: &JobBudget) -> Self {
        self.aggregate = Some(parent.clone());
        self
    }
    pub(super) fn snapshot(&self) -> PoolSnapshot {
        self.local.snapshot()
    }
    pub(super) fn reserve(&self, bytes: usize) -> Result<Reservation, Error> {
        let local = self.local.reserve(bytes)?;
        let aggregate = self
            .aggregate
            .as_ref()
            .map(|p| p.reserve(bytes))
            .transpose()?;
        Ok(Reservation {
            _local: local,
            _aggregate: aggregate,
        })
    }
}
pub(super) struct Reservation {
    _local: Charge,
    _aggregate: Option<Charge>,
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn racing_roles_share_one_ceiling_until_the_last_output_drops() {
        use std::{sync::Barrier, thread};
        let parent = JobBudget::new(4, 10).unwrap();
        let gate = Barrier::new(16);
        let held = thread::scope(|scope| {
            let workers: Vec<_> = (0..16)
                .map(|_| {
                    let role = RolePool::new(1, 3).unwrap().attach(&parent);
                    let gate = &gate;
                    scope.spawn(move || {
                        gate.wait();
                        role.reserve(3)
                    })
                })
                .collect();
            workers
                .into_iter()
                .filter_map(|w| w.join().unwrap().ok())
                .collect::<Vec<_>>()
        });
        assert_eq!(held.len(), 3);
        let s = parent.snapshot();
        assert_eq!((s.reserved_jobs, s.reserved_bytes, s.refusals), (3, 9, 13));
        drop(held);
        assert_eq!(parent.snapshot().reserved_jobs, 0);
        assert_eq!(parent.snapshot().reserved_bytes, 0);
    }

    #[test]
    fn sibling_roles_refuse_on_bytes_or_jobs_without_losing_retained_charges() {
        let parent = JobBudget::new(2, 100).unwrap();
        let audio = RolePool::new(1, 100).unwrap().attach(&parent);
        let mux = RolePool::new(2, 100).unwrap().attach(&parent);
        let retained = Arc::new(audio.reserve(60).unwrap());
        let output = retained.clone();
        drop(retained);
        assert!(matches!(audio.reserve(1), Err(Error::Limit))); // local only
        assert!(matches!(mux.reserve(41), Err(Error::Limit))); // aggregate bytes
        assert_eq!(mux.snapshot().reserved_jobs, 0);
        assert_eq!(mux.snapshot().reserved_bytes, 0);
        let clip = mux.reserve(39).unwrap();
        assert!(matches!(mux.reserve(1), Err(Error::Limit))); // aggregate jobs
        assert_eq!(parent.snapshot().reserved_bytes, 99);
        assert_eq!(parent.snapshot().refusals, 2);
        assert_eq!(audio.snapshot().refusals, 1);
        assert_eq!(mux.snapshot().refusals, 0);
        drop(audio);
        drop(output);
        let next = mux.reserve(61).unwrap();
        assert_eq!(parent.snapshot().reserved_bytes, 100);
        drop((next, clip, mux));
        assert_eq!(parent.snapshot().reserved_jobs, 0);
        assert_eq!(parent.snapshot().reserved_bytes, 0);
    }
}
