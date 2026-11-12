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

//! Options for query index management operations.

use crate::retry::RetryStrategy;
use std::sync::Arc;
use std::time::Duration;

/// Options for listing all query indexes.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetAllQueryIndexesOptions {
    /// Override the default retry strategy for this operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl GetAllQueryIndexesOptions {
    /// Creates a new instance of `GetAllQueryIndexesOptions`.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the retry strategy for the operation.
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

/// Options for creating a secondary query index.
///
/// # Fields
///
/// * `ignore_if_exists` — If `true`, the operation will not fail if the index already exists.
/// * `num_replicas` — The number of index replicas to create.
/// * `deferred` — If `true`, the index is created in a deferred state and must be explicitly built.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct CreateQueryIndexOptions {
    /// If `true`, the operation will not fail if the index already exists.
    pub ignore_if_exists: Option<bool>,
    /// The number of index replicas to create.
    pub num_replicas: Option<u32>,
    /// If `true`, the index is created in a deferred state and must be explicitly built.
    pub deferred: Option<bool>,
    /// Override the default retry strategy for this operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl CreateQueryIndexOptions {
    /// Creates a new instance of `CreateQueryIndexOptions`.
    pub fn new() -> Self {
        Self {
            ignore_if_exists: None,
            num_replicas: None,
            deferred: None,
            retry_strategy: None,
            timeout: None,
        }
    }

    /// Ignores the index creation if the index already exists.
    pub fn ignore_if_exists(mut self, ignore_if_exists: bool) -> Self {
        self.ignore_if_exists = Some(ignore_if_exists);
        self
    }

    /// Sets the number of replicas for the index.
    pub fn num_replicas(mut self, num_replicas: u32) -> Self {
        self.num_replicas = Some(num_replicas);
        self
    }

    /// Creates the index in a deferred state.
    pub fn deferred(mut self, deferred: bool) -> Self {
        self.deferred = Some(deferred);
        self
    }

    /// Sets the retry strategy for the operation.
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

/// Options for creating a primary query index.
///
/// # Fields
///
/// * `index_name` — Custom name for the primary index (default is `#primary`).
/// * `ignore_if_exists` — If `true`, the operation will not fail if the index already exists.
/// * `num_replicas` — The number of index replicas to create.
/// * `deferred` — If `true`, the index is created in a deferred state and must be explicitly built.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct CreatePrimaryQueryIndexOptions {
    /// Custom name for the primary index (default is `#primary`).
    pub index_name: Option<String>,
    /// If `true`, the operation will not fail if the index already exists.
    pub ignore_if_exists: Option<bool>,
    /// The number of index replicas to create.
    pub num_replicas: Option<u32>,
    /// If `true`, the index is created in a deferred state and must be explicitly built.
    pub deferred: Option<bool>,
    /// Override the default retry strategy for this operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl CreatePrimaryQueryIndexOptions {
    /// Creates a new instance of `CreatePrimaryQueryIndexOptions`.
    pub fn new() -> Self {
        Self {
            index_name: None,
            ignore_if_exists: None,
            num_replicas: None,
            deferred: None,
            retry_strategy: None,
            timeout: None,
        }
    }

    /// Sets a custom name for the primary index.
    pub fn index_name(mut self, index_name: impl Into<String>) -> Self {
        self.index_name = Some(index_name.into());
        self
    }

    /// Ignores the index creation if the index already exists.
    pub fn ignore_if_exists(mut self, ignore_if_exists: bool) -> Self {
        self.ignore_if_exists = Some(ignore_if_exists);
        self
    }

    /// Sets the number of replicas for the index.
    pub fn num_replicas(mut self, num_replicas: u32) -> Self {
        self.num_replicas = Some(num_replicas);
        self
    }

    /// Creates the index in a deferred state.
    pub fn deferred(mut self, deferred: bool) -> Self {
        self.deferred = Some(deferred);
        self
    }

    /// Sets the retry strategy for the operation.
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

/// Options for dropping a secondary query index.
///
/// * `ignore_if_not_exists` — If `true`, the operation will not fail if the index does not exist.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct DropQueryIndexOptions {
    /// If `true`, the operation will not fail if the index does not exist.
    pub ignore_if_not_exists: Option<bool>,
    /// Override the default retry strategy for this operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl DropQueryIndexOptions {
    /// Creates a new instance of `DropQueryIndexOptions`.
    pub fn new() -> Self {
        Self {
            ignore_if_not_exists: None,
            retry_strategy: None,
            timeout: None,
        }
    }

    /// Ignores the index drop operation if the index does not exist.
    pub fn ignore_if_not_exists(mut self, ignore_if_not_exists: bool) -> Self {
        self.ignore_if_not_exists = Some(ignore_if_not_exists);
        self
    }

    /// Sets the retry strategy for the operation.
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

/// Options for dropping a primary query index.
///
/// * `index_name` — The name of the primary index to drop (defaults to `#primary`).
/// * `ignore_if_not_exists` — If `true`, the operation will not fail if the index does not exist.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct DropPrimaryQueryIndexOptions {
    /// The name of the primary index to drop (defaults to `#primary`).
    pub index_name: Option<String>,
    /// If `true`, the operation will not fail if the index does not exist.
    pub ignore_if_not_exists: Option<bool>,
    /// Override the default retry strategy for this operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl DropPrimaryQueryIndexOptions {
    /// Creates a new instance of `DropPrimaryQueryIndexOptions`.
    pub fn new() -> Self {
        Self {
            index_name: None,
            ignore_if_not_exists: None,
            retry_strategy: None,
            timeout: None,
        }
    }

    /// Sets the name of the primary index to drop.
    pub fn index_name(mut self, index_name: impl Into<String>) -> Self {
        self.index_name = Some(index_name.into());
        self
    }

    /// Ignores the index drop operation if the index does not exist.
    pub fn ignore_if_not_exists(mut self, ignore_if_not_exists: bool) -> Self {
        self.ignore_if_not_exists = Some(ignore_if_not_exists);
        self
    }

    /// Sets the retry strategy for the operation.
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

/// Options for watching query indexes until they come online.
///
/// * `watch_primary` — If `true`, the primary index is also watched.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct WatchQueryIndexOptions {
    /// If `true`, the primary index is also watched.
    pub watch_primary: Option<bool>,
    /// Override the default retry strategy for this operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for waiting until the indexes are online, including retries. Without one,
    /// waits until they are online.
    pub timeout: Option<Duration>,
}

impl WatchQueryIndexOptions {
    /// Creates a new instance of `WatchQueryIndexOptions`.
    pub fn new() -> Self {
        Self {
            watch_primary: None,
            retry_strategy: None,
            timeout: None,
        }
    }

    /// Watches the primary index as well.
    pub fn watch_primary(mut self, watch_primary: bool) -> Self {
        self.watch_primary = Some(watch_primary);
        self
    }

    /// Sets the retry strategy for the operation.
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

/// Options for building all deferred query indexes.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct BuildQueryIndexOptions {
    /// Override the default retry strategy for this operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl BuildQueryIndexOptions {
    /// Creates a new instance of `BuildQueryIndexOptions`.
    pub fn new() -> Self {
        Self {
            retry_strategy: None,
            timeout: None,
        }
    }

    /// Sets the retry strategy for the operation.
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
