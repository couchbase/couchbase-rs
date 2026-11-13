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

//! Options for scope and collection management operations.
//!
//! Each management operation has a corresponding options struct for customizing behavior
//! such as retry strategies.

use crate::retry::RetryStrategy;
use std::sync::Arc;
use std::time::Duration;

/// Options for listing all scopes in a bucket.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetAllScopesOptions {
    /// Retry strategy for the operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl GetAllScopesOptions {
    /// Creates a new instance of `GetAllScopesOptions`.
    pub fn new() -> Self {
        Default::default()
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

/// Options for creating a scope.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct CreateScopeOptions {
    /// Retry strategy for the operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl CreateScopeOptions {
    /// Creates a new instance of `CreateScopeOptions`.
    pub fn new() -> Self {
        Default::default()
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

/// Options for dropping a scope.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct DropScopeOptions {
    /// Retry strategy for the operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl DropScopeOptions {
    /// Creates a new instance of `DropScopeOptions`.
    pub fn new() -> Self {
        Default::default()
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

/// Options for creating a collection.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct CreateCollectionOptions {
    /// Retry strategy for the operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl CreateCollectionOptions {
    /// Creates a new instance of `CreateCollectionOptions`.
    pub fn new() -> Self {
        Default::default()
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

/// Options for updating a collection's settings.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct UpdateCollectionOptions {
    /// Retry strategy for the operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl UpdateCollectionOptions {
    /// Creates a new instance of `UpdateCollectionOptions`.
    pub fn new() -> Self {
        Default::default()
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

/// Options for dropping a collection.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct DropCollectionOptions {
    /// Retry strategy for the operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl DropCollectionOptions {
    /// Creates a new instance of `DropCollectionOptions`.
    pub fn new() -> Self {
        Default::default()
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
