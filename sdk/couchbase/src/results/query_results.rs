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

use crate::error;
use crate::timeouts::OperationDeadline;
use couchbase_core::queryx;
use couchbase_core::results::query::QueryResultStream;
use futures::{Stream, StreamExt, TryStreamExt};
use serde::de::DeserializeOwned;
use serde_json::value::RawValue;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

/// The result of a SQL++ (N1QL) query.
///
/// Use [`rows`](QueryResult::rows) to stream result rows, and
/// [`metadata`](QueryResult::metadata) to access query metadata (status, metrics, warnings).
///
/// # Example
///
/// ```rust,no_run
/// # use couchbase::cluster::Cluster;
/// # use couchbase::authenticator::PasswordAuthenticator;
/// # use couchbase::options::cluster_options::ClusterOptions;
/// use futures::TryStreamExt;
/// use serde_json::Value;
///
/// # async fn example() -> couchbase::error::Result<()> {
/// # let cluster = Cluster::connect("couchbase://localhost",
/// #     ClusterOptions::new(PasswordAuthenticator::new("u", "p").into())).await?;
/// let mut result = cluster.query("SELECT 1 AS num", None).await?;
///
/// // Stream rows
/// let rows: Vec<Value> = result.rows().try_collect().await?;
///
/// // Access metadata
/// let meta = result.metadata()?;
/// println!("Status: {:?}", meta.status);
/// # Ok(())
/// # }
/// ```
pub struct QueryResult {
    wrapped: QueryResultStream,
    deadline: Option<OperationDeadline>,
}

/// A warning returned by the query engine (non-fatal).
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub struct QueryWarning {
    /// The warning code.
    pub code: u32,
    /// A human-readable warning message.
    pub message: String,
}

impl From<queryx::query_result::Warning> for QueryWarning {
    fn from(warning: queryx::query_result::Warning) -> Self {
        Self {
            code: warning.code,
            message: warning.message,
        }
    }
}

/// The execution status of a SQL++ query.
#[derive(Debug, Clone, Copy, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum QueryStatus {
    Running,
    Success,
    Errors,
    Completed,
    Stopped,
    Timeout,
    Closed,
    Fatal,
    Aborted,
    Unknown,
}

impl From<queryx::query_result::Status> for QueryStatus {
    fn from(status: queryx::query_result::Status) -> Self {
        match status {
            queryx::query_result::Status::Running => Self::Running,
            queryx::query_result::Status::Success => Self::Success,
            queryx::query_result::Status::Errors => Self::Errors,
            queryx::query_result::Status::Completed => Self::Completed,
            queryx::query_result::Status::Stopped => Self::Stopped,
            queryx::query_result::Status::Timeout => Self::Timeout,
            queryx::query_result::Status::Closed => Self::Closed,
            queryx::query_result::Status::Fatal => Self::Fatal,
            queryx::query_result::Status::Aborted => Self::Aborted,
            _ => Self::Unknown,
        }
    }
}

/// Performance metrics for a completed SQL++ query.
///
/// Available when [`QueryOptions::metrics`](crate::options::query_options::QueryOptions::metrics)
/// is set to `true`.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub struct QueryMetrics {
    /// Total time from query submission to completion.
    pub elapsed_time: Duration,
    /// Time spent actually executing the query.
    pub execution_time: Duration,
    /// Number of result rows returned.
    pub result_count: u64,
    /// Total size of all result rows in bytes.
    pub result_size: u64,
    /// Number of mutations performed by the query.
    pub mutation_count: u64,
    /// Number of rows sorted.
    pub sort_count: u64,
    /// Number of errors encountered.
    pub error_count: u64,
    /// Number of warnings generated.
    pub warning_count: u64,
}

impl From<&queryx::query_result::Metrics> for QueryMetrics {
    fn from(metrics: &queryx::query_result::Metrics) -> Self {
        Self {
            elapsed_time: metrics.elapsed_time,
            execution_time: metrics.execution_time,
            result_count: metrics.result_count,
            result_size: metrics.result_size,
            mutation_count: metrics.mutation_count,
            sort_count: metrics.sort_count,
            error_count: metrics.error_count,
            warning_count: metrics.warning_count,
        }
    }
}

/// Metadata associated with a SQL++ query result.
///
/// Includes the request ID, status, optional metrics, warnings, and profile information.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct QueryMetaData<'a> {
    /// The server-assigned request ID for this query.
    pub request_id: &'a str,
    /// The client-provided context ID, if any.
    pub client_context_id: &'a str,
    /// The execution status of the query.
    pub status: QueryStatus,
    /// Query performance metrics (present when metrics were requested).
    pub metrics: Option<QueryMetrics>,
    /// The query result signature (schema of result rows), if available.
    pub signature: Option<&'a RawValue>,
    /// Warnings generated during query execution.
    pub warnings: Vec<QueryWarning>,
    /// Query profiling information (present when profiling was requested).
    pub profile: Option<&'a RawValue>,
}

// TODO: fix ownership.
impl<'a> From<&'a queryx::query_result::MetaData> for QueryMetaData<'a> {
    fn from(meta: &'a queryx::query_result::MetaData) -> Self {
        Self {
            request_id: meta.request_id.as_ref(),
            client_context_id: meta.client_context_id.as_ref(),
            status: meta.status.into(),
            metrics: meta.metrics.as_ref().map(QueryMetrics::from),
            signature: meta.signature.as_deref(),
            warnings: meta
                .warnings
                .clone()
                .into_iter()
                .map(|w| w.into())
                .collect(),
            profile: meta.profile.as_deref(),
        }
    }
}

impl From<QueryResultStream> for QueryResult {
    fn from(wrapped: QueryResultStream) -> Self {
        Self {
            wrapped,
            deadline: None,
        }
    }
}

impl QueryResult {
    pub(crate) fn with_deadline(wrapped: QueryResultStream, deadline: OperationDeadline) -> Self {
        Self {
            wrapped,
            deadline: Some(deadline),
        }
    }
}

struct QueryRows<'a, V: DeserializeOwned> {
    wrapped: &'a mut QueryResultStream,
    deadline: Option<&'a mut OperationDeadline>,
    phantom_data: PhantomData<&'a V>,
}

impl<V: DeserializeOwned> Stream for QueryRows<'_, V> {
    type Item = error::Result<V>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Some(deadline) = self.deadline.as_mut() {
            if let Poll::Ready(res) = deadline.poll(cx) {
                return Poll::Ready(res.err().map(Err));
            }
        }

        let row = match self.wrapped.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(row))) => row,
            Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e.into()))),
            Poll::Ready(None) => {
                if let Some(deadline) = self.deadline.as_mut() {
                    deadline.disarm();
                }
                return Poll::Ready(None);
            }
            Poll::Pending => return Poll::Pending,
        };

        let row = serde_json::from_slice(&row).map_err(error::Error::decoding_failure_from_serde);
        Poll::Ready(Some(row))
    }
}

impl QueryResult {
    /// Returns the metadata for this query result.
    ///
    /// Metadata includes the query status, metrics, warnings, and profile information.
    /// This method may only be called after all rows have been consumed, or on a result
    /// where you don't need the rows.
    pub fn metadata(&self) -> error::Result<QueryMetaData<'_>> {
        Ok(self.wrapped.metadata()?.into())
    }

    /// Returns a [`Stream`] of deserialized result rows.
    ///
    /// Each row is deserialized from JSON into the requested type `V`.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use couchbase::results::query_results::QueryResult;
    /// use futures::TryStreamExt;
    /// use serde::Deserialize;
    ///
    /// #[derive(Deserialize)]
    /// struct Row { name: String }
    ///
    /// # async fn example(mut result: QueryResult) -> couchbase::error::Result<()> {
    /// let rows: Vec<Row> = result.rows().try_collect().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn rows<'a, V: DeserializeOwned + 'a>(
        &'a mut self,
    ) -> impl Stream<Item = error::Result<V>> + 'a {
        QueryRows {
            wrapped: &mut self.wrapped,
            deadline: self.deadline.as_mut(),
            phantom_data: PhantomData,
        }
    }
}
