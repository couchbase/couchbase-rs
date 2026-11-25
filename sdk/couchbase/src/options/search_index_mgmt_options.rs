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

//! Options for search index management operations.

use crate::retry::RetryStrategy;
use std::sync::Arc;
use std::time::Duration;

/// Options for retrieving a single search index by name.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetSearchIndexOptions {
    /// Retry strategy for the operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl GetSearchIndexOptions {
    /// Creates a new instance of `GetSearchIndexOptions`.
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

/// Options for listing all search indexes.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetAllSearchIndexesOptions {
    /// Retry strategy for the operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl GetAllSearchIndexesOptions {
    /// Creates a new instance of `GetAllSearchIndexesOptions`.
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

/// Options for creating or updating a search index.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct UpsertSearchIndexOptions {
    /// Retry strategy for the operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl UpsertSearchIndexOptions {
    /// Creates a new instance of `UpsertSearchIndexOptions`.
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

/// Options for dropping a search index.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct DropSearchIndexOptions {
    /// Retry strategy for the operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl DropSearchIndexOptions {
    /// Creates a new instance of `DropSearchIndexOptions`.
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

/// Options for analyzing a document against a search index.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct AnalyzeDocumentOptions {
    /// Retry strategy for the operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl AnalyzeDocumentOptions {
    /// Creates a new instance of `AnalyzeDocumentOptions`.
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

/// Options for retrieving the count of indexed documents in a search index.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetIndexedDocumentsCountOptions {
    /// Retry strategy for the operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl GetIndexedDocumentsCountOptions {
    /// Creates a new instance of `GetIndexedDocumentsCountOptions`.
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

/// Options for pausing document ingestion on a search index.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct PauseIngestSearchIndexOptions {
    /// Retry strategy for the operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl PauseIngestSearchIndexOptions {
    /// Creates a new instance of `PauseIngestSearchIndexOptions`.
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

/// Options for resuming document ingestion on a search index.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct ResumeIngestSearchIndexOptions {
    /// Retry strategy for the operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl ResumeIngestSearchIndexOptions {
    /// Creates a new instance of `ResumeIngestSearchIndexOptions`.
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

/// Options for allowing querying on a search index.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct AllowQueryingSearchIndexOptions {
    /// Retry strategy for the operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl AllowQueryingSearchIndexOptions {
    /// Creates a new instance of `AllowQueryingSearchIndexOptions`.
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

/// Options for disallowing querying on a search index.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct DisallowQueryingSearchIndexOptions {
    /// Retry strategy for the operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl DisallowQueryingSearchIndexOptions {
    /// Creates a new instance of `DisallowQueryingSearchIndexOptions`.
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

/// Options for freezing the plan of a search index (preventing it from being reassigned).
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct FreezePlanSearchIndexOptions {
    /// Retry strategy for the operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl FreezePlanSearchIndexOptions {
    /// Creates a new instance of `FreezePlanSearchIndexOptions`.
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

/// Options for unfreezing the plan of a search index (allowing reassignment).
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct UnfreezePlanSearchIndexOptions {
    /// Retry strategy for the operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl UnfreezePlanSearchIndexOptions {
    /// Creates a new instance of `UnfreezePlanSearchIndexOptions`.
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
