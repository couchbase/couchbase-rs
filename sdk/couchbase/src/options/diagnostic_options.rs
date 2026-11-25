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

//! Options for diagnostic and health-check operations.
//!
//! Use [`DiagnosticsOptions`] with [`Cluster::diagnostics`](crate::cluster::Cluster::diagnostics)
//! and [`PingOptions`] with [`Cluster::ping`](crate::cluster::Cluster::ping) or
//! [`Bucket::ping`](crate::bucket::Bucket::ping).

use crate::retry::RetryStrategy;
use crate::service_type::ServiceType;
use std::fmt::Display;
use std::sync::Arc;

/// Options for [`Cluster::diagnostics`](crate::cluster::Cluster::diagnostics).
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct DiagnosticsOptions {}

impl DiagnosticsOptions {
    /// Creates a new `DiagnosticsOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }
}

/// Options for [`Cluster::ping`](crate::cluster::Cluster::ping) and
/// [`Bucket::ping`](crate::bucket::Bucket::ping).
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct PingOptions {
    /// Limit the ping to specific service types. If `None`, all services are pinged.
    pub service_types: Option<Vec<ServiceType>>,
    /// Timeout for KV service ping operations.
    pub kv_timeout: Option<std::time::Duration>,
    /// Timeout for query service ping operations.
    pub query_timeout: Option<std::time::Duration>,
    /// Timeout for search service ping operations.
    pub search_timeout: Option<std::time::Duration>,
}

impl PingOptions {
    /// Creates a new `PingOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Restricts the ping to the given service types.
    pub fn service_types(mut self, service_types: Vec<ServiceType>) -> Self {
        self.service_types = Some(service_types);
        self
    }

    /// Sets the timeout for KV service pings.
    pub fn kv_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.kv_timeout = Some(timeout);
        self
    }

    /// Sets the timeout for query service pings.
    pub fn query_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.query_timeout = Some(timeout);
        self
    }

    /// Sets the timeout for search service pings.
    pub fn search_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.search_timeout = Some(timeout);
        self
    }
}

/// The desired cluster state for [`Cluster::wait_until_ready`](crate::cluster::Cluster::wait_until_ready).
#[derive(Copy, Debug, Default, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum ClusterState {
    /// All configured services have at least one connected endpoint.
    #[default]
    Online,
    /// At least one service has a connected endpoint but not all do.
    Degraded,
    /// No services have any connected endpoints.
    Offline,
}

/// Options for [`Cluster::wait_until_ready`](crate::cluster::Cluster::wait_until_ready) and
/// [`Bucket::wait_until_ready`](crate::bucket::Bucket::wait_until_ready).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct WaitUntilReadyOptions {
    /// The desired state to wait for. Defaults to [`ClusterState::Online`].
    pub desired_state: Option<ClusterState>,
    /// Limit the check to specific service types.
    pub service_types: Option<Vec<ServiceType>>,
    /// Override the default retry strategy.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for waiting until ready. Without one, waits until the desired state is reached.
    pub timeout: Option<std::time::Duration>,
}

impl Default for WaitUntilReadyOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl WaitUntilReadyOptions {
    /// Creates a new `WaitUntilReadyOptions` with default values.
    pub fn new() -> Self {
        Self {
            desired_state: None,
            service_types: None,
            retry_strategy: None,
            timeout: None,
        }
    }

    /// Sets the desired cluster state to wait for.
    pub fn desired_state(mut self, state: ClusterState) -> Self {
        self.desired_state = Some(state);
        self
    }

    /// Sets the timeout for waiting until ready. When it elapses, fails with
    /// [`ErrorKind::UnambiguousTimeout`](crate::error::ErrorKind::UnambiguousTimeout).
    pub fn timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Restricts the readiness check to the given service types.
    pub fn service_types(mut self, service_types: Vec<ServiceType>) -> Self {
        self.service_types = Some(service_types);
        self
    }

    /// Sets a custom retry strategy for this operation.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

impl From<ClusterState> for couchbase_core::options::waituntilready::ClusterState {
    fn from(state: ClusterState) -> Self {
        match state {
            ClusterState::Online => couchbase_core::options::waituntilready::ClusterState::Online,
            ClusterState::Degraded => {
                couchbase_core::options::waituntilready::ClusterState::Degraded
            }
            ClusterState::Offline => couchbase_core::options::waituntilready::ClusterState::Offline,
        }
    }
}
