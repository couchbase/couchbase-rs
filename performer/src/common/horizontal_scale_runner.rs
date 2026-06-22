use crate::common::batcher::Batcher;
use crate::common::bounds::{Bounds, BoundsExecutor};
use crate::common::executor::Executor;
use crate::counters::counter::Counter;
use crate::errors::error::{Error, Result};
use crate::proto::protocol::run::workload::Workload;
use crate::proto::protocol::shared::bounds;
use crate::proto::protocol::{run, sdk, shared};
use log::{error, info};
use std::ops::Add;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

pub struct PerHorizontalRunner {
    batcher: Arc<Batcher>,
    counters: Arc<crate::counters::counter::Counters>,
    workloads: Vec<run::Workload>,
}

impl PerHorizontalRunner {
    pub fn new(
        batcher: Arc<Batcher>,
        counters: Arc<crate::counters::counter::Counters>,
        workloads: Vec<run::Workload>,
    ) -> Self {
        Self {
            batcher,
            counters,
            workloads,
        }
    }
}

pub struct HorizontalScaleRunner {
    executor: Arc<Executor>,
    per: PerHorizontalRunner,
    success_count: usize,
    fail_count: usize,
}

impl HorizontalScaleRunner {
    pub fn new(executor: Arc<Executor>, per: PerHorizontalRunner) -> Self {
        HorizontalScaleRunner {
            executor,
            per,
            success_count: 0,
            fail_count: 0,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        info!(
            "Runner thread has started, will run {} workloads",
            self.per.workloads.len()
        );

        for workload in std::mem::take(&mut self.per.workloads) {
            match &workload.workload {
                Some(Workload::Sdk(w)) => self
                    .run_sdk_workload(w)
                    .await
                    .map_err(|e| Error::internal(format!("Failed to run SDK workload: {e}")))?,
                Some(Workload::Transaction(_)) => {
                    return Err(Error::unimplemented(
                        "Transactional workloads are not supported",
                    ))
                }
                Some(Workload::Grpc(_)) => {
                    return Err(Error::unimplemented("gRPC workloads are not supported"))
                }
                None => {
                    error!("Runner thread has no workload specified");
                    return Err(Error::invalid_argument("No workload specified"));
                }
            }
        }
        info!(
            "Runner thread has finished, success count: {}, fail count: {}",
            self.success_count, self.fail_count
        );

        Ok(())
    }

    async fn run_sdk_workload(&mut self, workload: &sdk::Workload) -> Result<()> {
        let num_commands = workload.command.len();
        let bounds = self.bounds(&workload.bounds, num_commands)?;

        let mut executed = 0;
        while bounds.can_execute() {
            let next_command = workload
                .command
                .get(executed % num_commands)
                .unwrap()
                .clone();
            executed += 1;

            match self
                .executor
                .perform_operation(next_command, &self.per.batcher)
                .await
            {
                Ok(true) => self.success_count += 1,
                Ok(false) => self.fail_count += 1,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn bounds(
        &mut self,
        bounds: &Option<shared::Bounds>,
        default_counter: usize,
    ) -> Result<Bounds> {
        match bounds {
            None => Ok(Bounds::new_counter(Arc::new(Counter::new(
                default_counter as i32,
            )))),
            Some(shared::Bounds {
                bounds: Some(bounds::Bounds::Counter(c)),
            }) => {
                let counter = self.per.counters.get(c)?;
                info!(
                    "Runner thread will run commands until counters {} is 0, currently {}",
                    c.counter_id,
                    counter.get()
                );
                Ok(Bounds::new_counter(counter.clone()))
            }
            Some(shared::Bounds {
                bounds: Some(bounds::Bounds::ForTime(f)),
            }) => {
                let deadline = SystemTime::now().add(Duration::from_secs(f.seconds as u64));
                Ok(Bounds::new_time(deadline))
            }
            _ => Err(Error::invalid_argument("Unknown bounds type specified")),
        }
    }
}
