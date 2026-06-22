use crate::proto::protocol::{metrics, run};
use serde::Serialize;
use std::time::Duration;
use sysinfo::{get_current_pid, Pid, System};
use tokio::runtime::Handle;
use tokio::select;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tonic::Status;

use crate::errors::error::Error;

#[cfg(target_os = "linux")]
use procfs::net::tcp;

const METRICS_REPORTER_DELAY: Duration = Duration::from_secs(1);

#[derive(Debug, Serialize)]
struct Metrics {
    #[serde(rename = "memHeapUsedMB")]
    pub mem_heap_used: u64,
    #[serde(rename = "processCpu")]
    pub process_cpu: f32,
    #[serde(rename = "systemCpu")]
    pub system_cpu: f32,
    #[serde(rename = "threadCount")]
    pub task_count: usize,

    #[cfg(target_os = "linux")]
    #[serde(rename = "openSockets")]
    pub system_open_sockets: usize,
}

pub struct MetricsReporter {
    run_id: String,
    tx: tokio::sync::mpsc::UnboundedSender<Result<run::Result, Status>>,
    system: System,
    pid: Pid,
    handle: Handle,

    success_count: usize,
    failure_count: usize,
}

impl MetricsReporter {
    pub fn new(
        run_id: String,
        tx: tokio::sync::mpsc::UnboundedSender<Result<run::Result, Status>>,
    ) -> Self {
        Self {
            run_id,
            tx,
            system: System::new_all(),
            pid: get_current_pid().expect("failed to get current PID"),
            handle: Handle::current(),

            success_count: 0,
            failure_count: 0,
        }
    }

    pub async fn run_reporter(mut self, shutdown_token: CancellationToken) {
        loop {
            select! {
                _ = shutdown_token.cancelled() => {
                    eprintln!("MetricsReporter: shutting down after {} successful and {} failed reports", self.success_count, self.failure_count);
                    return;
                },
                _ = sleep(METRICS_REPORTER_DELAY) => {}
            };

            let collected = match self.collect_metrics() {
                Ok(metrics) => metrics,
                Err(e) => {
                    self.failure_count += 1;
                    eprintln!(
                        "MetricsReporter: failed to collect metrics on run {}: {}",
                        self.run_id, e
                    );
                    continue;
                }
            };

            let encoded = match serde_json::to_string(&collected) {
                Ok(encoded) => encoded,
                Err(e) => {
                    self.failure_count += 1;
                    eprintln!(
                        "MetricsReporter: failed to encode metrics run {}: {}",
                        self.run_id, e
                    );
                    continue;
                }
            };

            let result = run::Result {
                elapsed_nanos: 0,
                initiated: None,
                result: Some(run::result::Result::Metrics(metrics::Result {
                    initiated: None,
                    metrics: encoded,
                })),
            };

            let res = self.tx.send(Ok(result));
            match res {
                Ok(_) => {
                    self.success_count += 1;
                }
                Err(e) => {
                    self.failure_count += 1;
                    eprintln!(
                        "MetricsReporter: failed to send metrics on run {}: {}",
                        self.run_id, e
                    );
                }
            }
        }
    }

    fn collect_metrics(&mut self) -> Result<Metrics, Box<Error>> {
        self.system.refresh_cpu_usage();
        let process = self
            .system
            .process(self.pid)
            .ok_or_else(|| Error::internal("failed to get process info"))?;

        Ok(Metrics {
            mem_heap_used: process.memory(),
            process_cpu: process.cpu_usage(),
            system_cpu: self.system.global_cpu_usage(),
            task_count: self.handle.metrics().num_alive_tasks(),

            #[cfg(target_os = "linux")]
            system_open_sockets: tcp()
                .map_err(|e| Error::internal(format!("failed to run tcp(): {e}")))?
                .len(),
        })
    }
}
