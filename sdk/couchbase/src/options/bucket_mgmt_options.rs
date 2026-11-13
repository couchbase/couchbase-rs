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

//! Options for bucket management operations.
//!
//! This module provides structures and implementations for options used in
//! various bucket management operations, such as creating, updating, and
//! deleting buckets. Each operation has an associated options struct that
//! can be used to customize the behavior of the operation, including
//! specifying a retry strategy in case of transient failures.
use crate::retry::RetryStrategy;
use std::sync::Arc;
use std::time::Duration;

/// Options for [`BucketManager::get_all_buckets`](crate::management::buckets::bucket_manager::BucketManager::get_all_buckets).
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetAllBucketsOptions {
    /// The retry strategy to use in case of transient failures.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl GetAllBucketsOptions {
    /// Creates a new instance of [`GetAllBucketsOptions`].
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets the retry strategy.
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

/// Options for [`BucketManager::get_bucket`](crate::management::buckets::bucket_manager::BucketManager::get_bucket).
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetBucketOptions {
    /// The retry strategy to use in case of transient failures.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl GetBucketOptions {
    /// Creates a new instance of [`GetBucketOptions`].
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets the retry strategy.
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

/// Options for [`BucketManager::create_bucket`](crate::management::buckets::bucket_manager::BucketManager::create_bucket).
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct CreateBucketOptions {
    /// The retry strategy to use in case of transient failures.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl CreateBucketOptions {
    /// Creates a new instance of [`CreateBucketOptions`].
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets the retry strategy.
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

/// Options for [`BucketManager::update_bucket`](crate::management::buckets::bucket_manager::BucketManager::update_bucket).
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct UpdateBucketOptions {
    /// The retry strategy to use in case of transient failures.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl UpdateBucketOptions {
    /// Creates a new instance of [`UpdateBucketOptions`].
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets the retry strategy.
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

/// Options for [`BucketManager::drop_bucket`](crate::management::buckets::bucket_manager::BucketManager::drop_bucket).
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct DropBucketOptions {
    /// The retry strategy to use in case of transient failures.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl DropBucketOptions {
    /// Creates a new instance of [`DropBucketOptions`].
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets the retry strategy.
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

/// Options for [`BucketManager::flush_bucket`](crate::management::buckets::bucket_manager::BucketManager::flush_bucket).
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct FlushBucketOptions {
    /// The retry strategy to use in case of transient failures.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::management_timeout`](crate::options::cluster_options::TimeoutOptions::management_timeout).
    pub timeout: Option<Duration>,
}

impl FlushBucketOptions {
    /// Creates a new instance of [`FlushBucketOptions`].
    pub fn new() -> Self {
        Default::default()
    }

    /// Sets the retry strategy.
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
