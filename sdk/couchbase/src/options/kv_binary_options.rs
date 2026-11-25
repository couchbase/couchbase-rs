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

//! Options for binary (non-JSON) operations on a
//! [`BinaryCollection`](crate::collection::BinaryCollection).

use crate::durability_level::DurabilityLevel;
use crate::retry::RetryStrategy;
use std::sync::Arc;
use std::time::Duration;

/// Options for [`BinaryCollection::append`](crate::collection::BinaryCollection).
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct AppendOptions {
    /// Durability level for this mutation.
    pub durability_level: Option<DurabilityLevel>,
    /// CAS value for optimistic concurrency control.
    pub cas: Option<u64>,
    /// Override the default retry strategy for this operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::kv_timeout`](crate::options::cluster_options::TimeoutOptions::kv_timeout), or
    /// [`TimeoutOptions::kv_durable_timeout`](crate::options::cluster_options::TimeoutOptions::kv_durable_timeout) when a durability level is
    /// set.
    pub timeout: Option<Duration>,
}

impl AppendOptions {
    /// Creates a new `AppendOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }
    /// Sets the durability level for this mutation.
    pub fn durability_level(mut self, durability_level: impl Into<DurabilityLevel>) -> Self {
        self.durability_level = Some(durability_level.into());
        self
    }
    /// Sets the CAS value for optimistic concurrency control.
    pub fn cas(mut self, cas: u64) -> Self {
        self.cas = Some(cas);
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

/// Options for [`BinaryCollection::prepend`](crate::collection::BinaryCollection).
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct PrependOptions {
    /// Durability level for this mutation.
    pub durability_level: Option<DurabilityLevel>,
    /// CAS value for optimistic concurrency control.
    pub cas: Option<u64>,
    /// Override the default retry strategy for this operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::kv_timeout`](crate::options::cluster_options::TimeoutOptions::kv_timeout), or
    /// [`TimeoutOptions::kv_durable_timeout`](crate::options::cluster_options::TimeoutOptions::kv_durable_timeout) when a durability level is
    /// set.
    pub timeout: Option<Duration>,
}

impl PrependOptions {
    /// Creates a new `PrependOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }
    /// Sets the durability level for this mutation.
    pub fn durability_level(mut self, durability_level: impl Into<DurabilityLevel>) -> Self {
        self.durability_level = Some(durability_level.into());
        self
    }
    /// Sets the CAS value for optimistic concurrency control.
    pub fn cas(mut self, cas: u64) -> Self {
        self.cas = Some(cas);
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

/// Options for [`BinaryCollection::increment`](crate::collection::BinaryCollection).
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct IncrementOptions {
    /// Document expiry time.
    pub expiry: Option<Duration>,
    /// Initial value if the counter document does not exist.
    pub initial: Option<u64>,
    /// Amount to increment by. Defaults to 1.
    pub delta: Option<u64>,
    /// Durability level for this mutation.
    pub durability_level: Option<DurabilityLevel>,
    /// Override the default retry strategy for this operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::kv_timeout`](crate::options::cluster_options::TimeoutOptions::kv_timeout), or
    /// [`TimeoutOptions::kv_durable_timeout`](crate::options::cluster_options::TimeoutOptions::kv_durable_timeout) when a durability level is
    /// set.
    pub timeout: Option<Duration>,
}

impl IncrementOptions {
    /// Creates a new `IncrementOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }
    /// Sets the document expiry time.
    pub fn expiry(mut self, expiry: Duration) -> Self {
        self.expiry = Some(expiry);
        self
    }
    /// Sets the initial value if the counter document does not exist.
    pub fn initial(mut self, initial: u64) -> Self {
        self.initial = Some(initial);
        self
    }
    /// Sets the amount to increment by.
    pub fn delta(mut self, delta: u64) -> Self {
        self.delta = Some(delta);
        self
    }
    /// Sets the durability level for this mutation.
    pub fn durability_level(mut self, durability_level: impl Into<DurabilityLevel>) -> Self {
        self.durability_level = Some(durability_level.into());
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

/// Options for [`BinaryCollection::decrement`](crate::collection::BinaryCollection).
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct DecrementOptions {
    /// Document expiry time.
    pub expiry: Option<Duration>,
    /// Initial value if the counter document does not exist.
    pub initial: Option<u64>,
    /// Amount to decrement by. Defaults to 1.
    pub delta: Option<u64>,
    /// Durability level for this mutation.
    pub durability_level: Option<DurabilityLevel>,
    /// Override the default retry strategy for this operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
    /// Timeout for this operation, including retries. Defaults to [`TimeoutOptions::kv_timeout`](crate::options::cluster_options::TimeoutOptions::kv_timeout), or
    /// [`TimeoutOptions::kv_durable_timeout`](crate::options::cluster_options::TimeoutOptions::kv_durable_timeout) when a durability level is
    /// set.
    pub timeout: Option<Duration>,
}

impl DecrementOptions {
    /// Creates a new `DecrementOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }
    /// Sets the document expiry time.
    pub fn expiry(mut self, expiry: Duration) -> Self {
        self.expiry = Some(expiry);
        self
    }
    /// Sets the initial value if the counter document does not exist.
    pub fn initial(mut self, initial: u64) -> Self {
        self.initial = Some(initial);
        self
    }
    /// Sets the amount to decrement by.
    pub fn delta(mut self, delta: u64) -> Self {
        self.delta = Some(delta);
        self
    }
    /// Sets the durability level for this mutation.
    pub fn durability_level(mut self, durability_level: impl Into<DurabilityLevel>) -> Self {
        self.durability_level = Some(durability_level.into());
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
