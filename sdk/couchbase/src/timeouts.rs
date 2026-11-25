/*
 *
 *  * Copyright (c) 2025 Couchbase, Inc.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *    http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

use crate::error::{Error, ErrorKind, Result};
use crate::options::cluster_options::TimeoutOptions;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::time::Sleep;

const DEFAULT_KV_TIMEOUT: Duration = Duration::from_millis(2500);
const DEFAULT_KV_DURABLE_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(75);
const DEFAULT_SEARCH_TIMEOUT: Duration = Duration::from_secs(75);
const DEFAULT_MANAGEMENT_TIMEOUT: Duration = Duration::from_secs(75);

/// The cluster-wide default timeouts, with the SDK defaults applied.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Timeouts {
    pub kv: Duration,
    pub kv_durable: Duration,
    pub query: Duration,
    pub search: Duration,
    pub management: Duration,
}

impl Default for Timeouts {
    fn default() -> Self {
        Self::from(&TimeoutOptions::default())
    }
}

impl From<&TimeoutOptions> for Timeouts {
    fn from(opts: &TimeoutOptions) -> Self {
        Self {
            kv: opts.kv_timeout.unwrap_or(DEFAULT_KV_TIMEOUT),
            kv_durable: opts
                .kv_durable_timeout
                .unwrap_or(DEFAULT_KV_DURABLE_TIMEOUT),
            query: opts.query_timeout.unwrap_or(DEFAULT_QUERY_TIMEOUT),
            search: opts.search_timeout.unwrap_or(DEFAULT_SEARCH_TIMEOUT),
            management: opts
                .management_timeout
                .unwrap_or(DEFAULT_MANAGEMENT_TIMEOUT),
        }
    }
}

pub(crate) fn timeout_error(ambiguous: bool) -> Error {
    Error::new(if ambiguous {
        ErrorKind::AmbiguousTimeout
    } else {
        ErrorKind::UnambiguousTimeout
    })
}

/// Runs `fut`, failing with a timeout error if it does not complete within `timeout`. Dropping
/// `fut` on timeout cancels the operation on the client side.
pub(crate) async fn with_timeout<T>(
    timeout: Duration,
    ambiguous: bool,
    fut: impl Future<Output = Result<T>>,
) -> Result<T> {
    // Boxed so that wrapping operations doesn't deepen the nesting of async types, which would
    // otherwise overflow the compiler's layout depth limit in generic callers.
    match tokio::time::timeout(timeout, Box::pin(fut)).await {
        Ok(res) => res,
        Err(_) => Err(timeout_error(ambiguous)),
    }
}

/// Like [`with_timeout`], but runs `fut` without a timeout when `timeout` is `None`.
pub(crate) async fn with_optional_timeout<T>(
    timeout: Option<Duration>,
    ambiguous: bool,
    fut: impl Future<Output = Result<T>>,
) -> Result<T> {
    match timeout {
        Some(timeout) => with_timeout(timeout, ambiguous, fut).await,
        None => fut.await,
    }
}

/// The deadline of an operation whose result rows are streamed, so that it also bounds the
/// consumption of the rows.
pub(crate) struct OperationDeadline {
    sleep: Pin<Box<Sleep>>,
    ambiguous: bool,
    done: bool,
}

impl OperationDeadline {
    pub fn new(timeout: Duration, ambiguous: bool) -> Self {
        Self {
            sleep: Box::pin(tokio::time::sleep(timeout)),
            ambiguous,
            done: false,
        }
    }

    /// Runs `fut`, failing with a timeout error if the deadline is reached first.
    pub async fn run<T>(&self, fut: impl Future<Output = Result<T>>) -> Result<T> {
        match tokio::time::timeout_at(self.sleep.deadline(), fut).await {
            Ok(res) => res,
            Err(_) => Err(timeout_error(self.ambiguous)),
        }
    }

    /// Returns `Ready(Err(_))` the first time the deadline is reached, then `Ready(Ok(()))`, so that
    /// a stream reports the timeout once and then ends.
    pub fn poll(&mut self, cx: &mut Context<'_>) -> Poll<Result<()>> {
        if self.done {
            return Poll::Ready(Ok(()));
        }

        match self.sleep.as_mut().poll(cx) {
            Poll::Ready(()) => {
                self.done = true;
                Poll::Ready(Err(timeout_error(self.ambiguous)))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    /// Stops enforcing the deadline, once all the rows have been consumed.
    pub fn disarm(&mut self) {
        self.done = true;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn with_timeout_reports_ambiguity() {
        let never = || std::future::pending::<Result<()>>();

        let err = with_timeout(Duration::from_millis(10), false, never())
            .await
            .unwrap_err();
        assert_eq!(err.kind(), &ErrorKind::UnambiguousTimeout);

        let err = with_timeout(Duration::from_millis(10), true, never())
            .await
            .unwrap_err();
        assert_eq!(err.kind(), &ErrorKind::AmbiguousTimeout);
    }

    #[tokio::test]
    async fn deadline_reports_timeout_once() {
        let mut deadline = OperationDeadline::new(Duration::from_millis(10), false);

        let err = std::future::poll_fn(|cx| deadline.poll(cx))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), &ErrorKind::UnambiguousTimeout);
        assert!(std::future::poll_fn(|cx| deadline.poll(cx)).await.is_ok());
    }
}
