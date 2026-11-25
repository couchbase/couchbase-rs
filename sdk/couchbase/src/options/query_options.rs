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

//! Options for SQL++ (N1QL) query operations.
//!
//! Use [`QueryOptions`] with [`Cluster::query`](crate::cluster::Cluster::query) or
//! [`Scope::query`](crate::scope::Scope::query) to configure query behavior such as
//! scan consistency, positional/named parameters, timeouts, and more.

use crate::error;
use crate::error::Error;
use crate::mutation_state::MutationState;
use crate::retry::RetryStrategy;
use couchbase_core::queryx;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// Scan consistency level for SQL++ queries.
///
/// Controls whether the query engine waits for indexes to be updated before returning results.
#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum ScanConsistency {
    /// No consistency requirement — the query may return stale results (fastest).
    NotBounded,
    /// Wait for all mutations up to the query request to be indexed.
    RequestPlus,
    /// Wait for specific mutations (identified by a [`MutationState`]) to be indexed.
    AtPlus(MutationState),
}

/// Controls whether a SQL++ query can be serviced by index replicas.
///
/// When set to [`On`](ReplicaLevel::On), the query engine may read from index replica
/// nodes in addition to the primary index node, which can improve availability
/// at the cost of potentially returning slightly stale results.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum ReplicaLevel {
    /// Allow the query engine to use index replicas.
    On,
    /// Only use the primary index node (default).
    Off,
}

impl From<ReplicaLevel> for queryx::query_options::ReplicaLevel {
    fn from(rl: ReplicaLevel) -> Self {
        match rl {
            ReplicaLevel::On => queryx::query_options::ReplicaLevel::On,
            ReplicaLevel::Off => queryx::query_options::ReplicaLevel::Off,
        }
    }
}

/// Controls the level of profiling information returned with query results.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
#[non_exhaustive]
pub enum ProfileMode {
    /// No profiling (default).
    Off,
    /// Include phase-level timing information.
    Phases,
    /// Include detailed per-operator timing information.
    Timings,
}

impl From<ProfileMode> for queryx::query_options::ProfileMode {
    fn from(pm: ProfileMode) -> Self {
        match pm {
            ProfileMode::Off => queryx::query_options::ProfileMode::Off,
            ProfileMode::Phases => queryx::query_options::ProfileMode::Phases,
            ProfileMode::Timings => queryx::query_options::ProfileMode::Timings,
        }
    }
}

/// Options for SQL++ (N1QL) queries executed via [`Cluster::query`](crate::cluster::Cluster::query)
/// or [`Scope::query`](crate::scope::Scope::query).
///
/// # Example
///
/// ```rust
/// use couchbase::options::query_options::{QueryOptions, ScanConsistency};
///
/// let opts = QueryOptions::new()
///     .scan_consistency(ScanConsistency::RequestPlus)
///     .metrics(true)
///     .add_positional_parameter("Alice").unwrap();
/// ```
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct QueryOptions {
    /// If `false`, the query will be prepared and cached for faster subsequent executions.
    pub ad_hoc: Option<bool>,
    /// A client-provided identifier for this query, useful for tracing and debugging.
    pub client_context_id: Option<String>,
    /// If `true`, enables use of Flex Indexes (full-text search indexes in queries).
    pub flex_index: Option<bool>,
    /// Maximum parallelism level for the query engine.
    pub max_parallelism: Option<u32>,
    /// If `true`, includes query execution metrics in the result.
    pub metrics: Option<bool>,
    /// Named parameters for the query (e.g. `$name`).
    pub named_parameters: Option<HashMap<String, Value>>,
    /// The pipeline batch size (number of items in each batch for the query pipeline).
    pub pipeline_batch: Option<u32>,
    /// The pipeline cap (maximum number of items buffered in the query pipeline).
    pub pipeline_cap: Option<u32>,
    /// Positional parameters for the query (referenced as `$1`, `$2`, etc.).
    pub positional_parameters: Option<Vec<Value>>,
    /// If `true`, preserves document expiry on mutations performed by this query.
    pub preserve_expiry: Option<bool>,
    /// The profiling mode for the query.
    pub profile: Option<ProfileMode>,
    /// Raw key/value parameters passed directly to the query request body.
    pub raw: Option<HashMap<String, Value>>,
    /// If `true`, marks the query as read-only (no mutations).
    pub read_only: Option<bool>,
    /// Maximum buffered channel size for index scans.
    pub scan_cap: Option<u32>,
    /// The scan consistency level for the query.
    pub scan_consistency: Option<ScanConsistency>,
    /// Maximum time the query engine will wait for index consistency.
    pub scan_wait: Option<Duration>,
    /// Server-side timeout for the query.
    pub server_timeout: Option<Duration>,
    /// Whether to allow index replicas to service the query.
    pub use_replica: Option<ReplicaLevel>,
    /// Custom retry strategy for this query.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Covers the consumption of the result rows. Defaults to
    /// [`TimeoutOptions::query_timeout`](crate::options::cluster_options::TimeoutOptions::query_timeout).
    pub timeout: Option<Duration>,
}

impl QueryOptions {
    /// Creates a new `QueryOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// If `false`, the query will be prepared and cached for subsequent executions.
    pub fn ad_hoc(mut self, ad_hoc: bool) -> Self {
        self.ad_hoc = Some(ad_hoc);
        self
    }

    /// Sets a client-provided context ID for this query (useful for debugging).
    pub fn client_context_id(mut self, client_context_id: impl Into<String>) -> Self {
        self.client_context_id = Some(client_context_id.into());
        self
    }

    /// If `true`, enables use of Flex Indexes (full-text search indexes in queries).
    pub fn flex_index(mut self, flex_index: bool) -> Self {
        self.flex_index = Some(flex_index);
        self
    }

    /// Sets the maximum parallelism for the query engine.
    pub fn max_parallelism(mut self, max_parallelism: u32) -> Self {
        self.max_parallelism = Some(max_parallelism);
        self
    }

    /// If `true`, includes query execution metrics in the result.
    pub fn metrics(mut self, metrics: bool) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Adds a named parameter to the query.
    pub fn add_named_parameter<T: Serialize>(
        mut self,
        key: impl Into<String>,
        value: T,
    ) -> error::Result<Self> {
        let value = serde_json::to_value(&value).map_err(Error::encoding_failure_from_serde)?;

        match self.named_parameters {
            Some(mut params) => {
                params.insert(key.into(), value);
                self.named_parameters = Some(params);
            }
            None => {
                let mut params = HashMap::new();
                params.insert(key.into(), value);
                self.named_parameters = Some(params);
            }
        }
        Ok(self)
    }

    /// Sets the pipeline batch size (number of items in each batch for the query pipeline).
    pub fn pipeline_batch(mut self, pipeline_batch: u32) -> Self {
        self.pipeline_batch = Some(pipeline_batch);
        self
    }

    /// Sets the pipeline cap (maximum number of items buffered in the query pipeline).
    pub fn pipeline_cap(mut self, pipeline_cap: u32) -> Self {
        self.pipeline_cap = Some(pipeline_cap);
        self
    }

    /// Adds a positional parameter to the query.
    pub fn add_positional_parameter<T: Serialize>(mut self, parameters: T) -> error::Result<Self> {
        let parameters =
            serde_json::to_value(&parameters).map_err(Error::encoding_failure_from_serde)?;

        match self.positional_parameters {
            Some(mut params) => {
                params.push(parameters);
                self.positional_parameters = Some(params);
            }
            None => {
                self.positional_parameters = Some(vec![parameters]);
            }
        }
        Ok(self)
    }

    /// If `true`, preserves document expiry on mutations performed by this query.
    pub fn preserve_expiry(mut self, preserve_expiry: bool) -> Self {
        self.preserve_expiry = Some(preserve_expiry);
        self
    }

    /// Sets the profiling mode for the query.
    pub fn profile(mut self, profile: ProfileMode) -> Self {
        self.profile = Some(profile);
        self
    }

    /// Adds a raw key/value parameter to the query request body.
    pub fn add_raw<T: Serialize>(
        mut self,
        key: impl Into<String>,
        value: T,
    ) -> error::Result<Self> {
        let value = serde_json::to_value(&value).map_err(Error::encoding_failure_from_serde)?;

        match self.raw {
            Some(mut params) => {
                params.insert(key.into(), value);
                self.raw = Some(params);
            }
            None => {
                let mut params = HashMap::new();
                params.insert(key.into(), value);
                self.raw = Some(params);
            }
        }
        Ok(self)
    }

    /// If `true`, marks the query as read-only (no mutations).
    pub fn read_only(mut self, read_only: bool) -> Self {
        self.read_only = Some(read_only);
        self
    }

    /// Sets the maximum buffered channel size for index scans.
    pub fn scan_cap(mut self, scan_cap: u32) -> Self {
        self.scan_cap = Some(scan_cap);
        self
    }

    /// Sets the scan consistency level for the query.
    pub fn scan_consistency(mut self, scan_consistency: ScanConsistency) -> Self {
        self.scan_consistency = Some(scan_consistency);
        self
    }

    /// Sets the maximum time the query engine will wait for index consistency.
    pub fn scan_wait(mut self, scan_wait: Duration) -> Self {
        self.scan_wait = Some(scan_wait);
        self
    }

    /// Sets the server-side timeout for the query.
    pub fn server_timeout(mut self, server_timeout: Duration) -> Self {
        self.server_timeout = Some(server_timeout);
        self
    }

    /// Sets the replica read level for the query.
    pub fn use_replica(mut self, use_replica: ReplicaLevel) -> Self {
        self.use_replica = Some(use_replica);
        self
    }

    /// Sets a custom retry strategy for this operation.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }

    /// Sets the timeout for this operation, including retries.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }
}
