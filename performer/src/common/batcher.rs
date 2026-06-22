use crate::errors::error::{Error, Result};
use crate::proto::protocol::run;
use log::{error, info};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, Notify};
use tokio::time::{sleep, Duration};
use tokio_util::sync::CancellationToken;
use tonic::Status;

#[derive(Clone)]
pub struct Batcher {
    batch_size: i32,
    queue: Arc<Mutex<VecDeque<run::Result>>>,
    cancellation_token: CancellationToken,
    completion_notify: Arc<Notify>,
}

impl Batcher {
    pub fn new(batch_size: i32) -> Self {
        Batcher {
            batch_size,
            queue: Arc::new(Mutex::new(VecDeque::new())),
            cancellation_token: CancellationToken::new(),
            completion_notify: Arc::new(Notify::new()),
        }
    }

    pub async fn send(&self, result: run::Result) {
        let mut queue = self.queue.lock().unwrap();
        queue.push_back(result);
    }

    pub fn run(
        &self,
        sender: mpsc::UnboundedSender<std::result::Result<run::Result, Status>>,
    ) -> Arc<Notify> {
        let on_complete = self.completion_notify.clone();
        let this = self.clone();
        tokio::spawn(async move {
            this.batch_loop(sender, on_complete).await;
        });

        self.completion_notify.clone()
    }

    async fn batch_loop(
        &self,
        sender: mpsc::UnboundedSender<std::result::Result<run::Result, Status>>,
        on_complete: Arc<Notify>,
    ) {
        loop {
            if let Err(e) = self.flush(&sender).await {
                error!("Error flushing batcher: {e}");
                break;
            }

            tokio::select! {
                _ = sleep(Duration::from_millis(10)) => {},
                _ = self.cancellation_token.cancelled() => break,
            }
        }

        info!("Flushing final batch before exiting");

        if let Err(e) = self.flush(&sender).await {
            error!("Error flushing final batch: {e}");
        }

        on_complete.notify_waiters();
    }

    async fn flush(
        &self,
        sender: &mpsc::UnboundedSender<std::result::Result<run::Result, Status>>,
    ) -> Result<()> {
        if self.batch_size == 0 {
            self.flush_individual(sender).await
        } else {
            self.flush_batch(sender).await
        }
    }

    async fn flush_individual(
        &self,
        sender: &mpsc::UnboundedSender<std::result::Result<run::Result, Status>>,
    ) -> Result<()> {
        let items = self.items_to_flush().await;
        if items.is_empty() {
            return Ok(());
        }
        for item in items {
            sender
                .send(Ok(item))
                .map_err(|_| Error::internal("Error sending batch item"))?;
        }
        Ok(())
    }

    async fn flush_batch(
        &self,
        sender: &mpsc::UnboundedSender<std::result::Result<run::Result, Status>>,
    ) -> Result<()> {
        let items = self.items_to_flush().await;
        if items.is_empty() {
            return Ok(());
        }

        let batches = self.batch_items(items);

        for batch in batches {
            let result = run::Result {
                elapsed_nanos: 0,
                initiated: None,
                result: Some(run::result::Result::Batched(run::BatchedResult {
                    result: batch,
                })),
            };
            sender
                .send(Ok(result))
                .map_err(|e| Error::internal(format!("Failed to send batch: {}", e)))?;
        }
        Ok(())
    }

    async fn items_to_flush(&self) -> Vec<run::Result> {
        let mut queue = self.queue.lock().unwrap();
        queue.drain(..).collect()
    }

    fn batch_items(&self, mut items: Vec<run::Result>) -> Vec<Vec<run::Result>> {
        let mut batches = Vec::new();
        let batch_size = self.batch_size as usize;

        while !items.is_empty() {
            let take = items.len().min(batch_size);
            batches.push(items.drain(..take).collect());
        }

        batches
    }

    pub async fn stop(&self) {
        self.cancellation_token.cancel();
        self.completion_notify.notified().await;
    }
}
