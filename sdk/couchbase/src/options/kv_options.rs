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
//! Options for key-value (KV) operations on a [`Collection`](crate::collection::Collection).
//!
//! Each KV operation has a corresponding options struct (e.g. [`UpsertOptions`], [`GetOptions`]).
//! All options structs implement `Default` and provide a builder-style API. Pass `None` to
//! any operation to use the defaults.

use crate::durability_level::DurabilityLevel;
use crate::retry::RetryStrategy;
use std::sync::Arc;
use std::time::Duration;

/// Options for [`Collection::upsert`](crate::collection::Collection).
///
/// # Example
///
/// ```rust
/// use couchbase::options::kv_options::UpsertOptions;
/// use couchbase::durability_level::DurabilityLevel;
/// use std::time::Duration;
///
/// let opts = UpsertOptions::new()
///     .expiry(Duration::from_secs(300))
///     .durability_level(DurabilityLevel::MAJORITY);
/// ```
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct UpsertOptions {
    /// Document expiry time. `None` means the document does not expire.
    pub expiry: Option<Duration>,
    /// Durability level for this mutation.
    pub durability_level: Option<DurabilityLevel>,
    /// If `true`, the existing document expiry is preserved even when the content is replaced.
    pub preserve_expiry: Option<bool>,
    /// Override the default retry strategy for this operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl UpsertOptions {
    /// Creates a new `UpsertOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the document expiry time.
    pub fn expiry(mut self, expiry: Duration) -> Self {
        self.expiry = Some(expiry);
        self
    }

    /// Sets the durability level for this mutation.
    pub fn durability_level(mut self, durability_level: impl Into<DurabilityLevel>) -> Self {
        self.durability_level = Some(durability_level.into());
        self
    }

    /// If `true`, preserves the existing document expiry when replacing the content.
    pub fn preserve_expiry(mut self, preserve_expiry: bool) -> Self {
        self.preserve_expiry = Some(preserve_expiry);
        self
    }

    /// Sets a custom retry strategy for this operation.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

/// Options for [`Collection::insert`](crate::collection::Collection).
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct InsertOptions {
    /// Document expiry time. `None` means the document does not expire.
    pub expiry: Option<Duration>,
    /// Durability level for this mutation.
    pub durability_level: Option<DurabilityLevel>,
    /// Override the default retry strategy for this operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl InsertOptions {
    /// Creates a new `InsertOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the document expiry time.
    pub fn expiry(mut self, expiry: Duration) -> Self {
        self.expiry = Some(expiry);
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
}

/// Options for [`Collection::replace`](crate::collection::Collection).
///
/// Use [`cas`](ReplaceOptions::cas) for optimistic locking.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct ReplaceOptions {
    /// Document expiry time.
    pub expiry: Option<Duration>,
    /// Durability level for this mutation.
    pub durability_level: Option<DurabilityLevel>,
    /// If `true`, the existing document expiry is preserved.
    pub preserve_expiry: Option<bool>,
    /// CAS value for optimistic concurrency control. If set, the replace only
    /// succeeds if the current CAS matches.
    pub cas: Option<u64>,
    /// Override the default retry strategy.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl ReplaceOptions {
    /// Creates a new `ReplaceOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the document expiry time.
    pub fn expiry(mut self, expiry: Duration) -> Self {
        self.expiry = Some(expiry);
        self
    }

    /// Sets the durability level for this mutation.
    pub fn durability_level(mut self, durability_level: impl Into<DurabilityLevel>) -> Self {
        self.durability_level = Some(durability_level.into());
        self
    }

    /// If `true`, preserves the existing document expiry when replacing the content.
    pub fn preserve_expiry(mut self, preserve_expiry: bool) -> Self {
        self.preserve_expiry = Some(preserve_expiry);
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
}

/// Options for [`Collection::get`](crate::collection::Collection).
///
/// Supports projections (fetching only specific JSON paths) and optionally
/// fetching the document expiry time.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetOptions {
    /// If `true`, the document's expiry time will be included in the result.
    pub expiry: Option<bool>,
    /// Fetch only these JSON paths from the document (sub-document projection).
    pub projections: Option<Vec<String>>,
    /// Override the default retry strategy.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl GetOptions {
    /// Creates a new `GetOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// If `true`, includes the document's expiry time in the result.
    pub fn expiry(mut self, expiry: bool) -> Self {
        self.expiry = Some(expiry);
        self
    }
    /// Sets the JSON paths to project (fetch only specific fields).
    pub fn projections(mut self, projections: Vec<String>) -> Self {
        self.projections = Some(projections);
        self
    }

    /// Sets a custom retry strategy for this operation.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

/// Options for [`Collection::get_replica`](crate::collection::Collection).
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetReplicaOptions {
    /// Override the default retry strategy.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl GetReplicaOptions {
    /// Creates a new `GetReplicaOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets a custom retry strategy for this operation.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

/// Options for [`Collection::exists`](crate::collection::Collection).
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct ExistsOptions {
    /// Override the default retry strategy for this operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl ExistsOptions {
    /// Creates a new `ExistsOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets a custom retry strategy for this operation.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

/// Options for [`Collection::remove`](crate::collection::Collection).
///
/// Use [`cas`](RemoveOptions::cas) for optimistic concurrency control.
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct RemoveOptions {
    /// Durability level for this mutation.
    pub durability_level: Option<DurabilityLevel>,
    /// CAS value for optimistic concurrency control. If set, the remove only
    /// succeeds if the current CAS matches.
    pub cas: Option<u64>,
    /// Override the default retry strategy for this operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl RemoveOptions {
    /// Creates a new `RemoveOptions` with default values.
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
}

/// Options for [`Collection::get_and_touch`](crate::collection::Collection).
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetAndTouchOptions {
    /// Override the default retry strategy for this operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl GetAndTouchOptions {
    /// Creates a new `GetAndTouchOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets a custom retry strategy for this operation.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

/// Options for [`Collection::get_and_lock`](crate::collection::Collection).
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct GetAndLockOptions {
    /// Override the default retry strategy for this operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl GetAndLockOptions {
    /// Creates a new `GetAndLockOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets a custom retry strategy for this operation.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

/// Options for [`Collection::unlock`](crate::collection::Collection).
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct UnlockOptions {
    /// Override the default retry strategy for this operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl UnlockOptions {
    /// Creates a new `UnlockOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets a custom retry strategy for this operation.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

/// Options for [`Collection::touch`](crate::collection::Collection).
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct TouchOptions {
    /// Override the default retry strategy for this operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl TouchOptions {
    /// Creates a new `TouchOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets a custom retry strategy for this operation.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

/// Options for [`Collection::lookup_in`](crate::collection::Collection).
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct LookupInOptions {
    /// If `true`, the operation can access soft-deleted (tombstoned) documents.
    pub access_deleted: Option<bool>,
    /// Custom retry strategy for this operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl LookupInOptions {
    /// Creates a new `LookupInOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// If `true`, allows accessing soft-deleted (tombstoned) documents.
    pub fn access_deleted(mut self, access_deleted: bool) -> Self {
        self.access_deleted = Some(access_deleted);
        self
    }

    /// Sets a custom retry strategy for this operation.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}

/// Determines how the document is treated when performing a [`mutate_in`](crate::collection::Collection)
/// operation.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum StoreSemantics {
    /// The document must already exist (default).
    Replace,
    /// The document is created if it doesn't exist, or updated if it does.
    Upsert,
    /// The document must not exist; fails if it already does.
    Insert,
}

/// Options for [`Collection::mutate_in`](crate::collection::Collection).
#[derive(Default, Debug, Clone)]
#[non_exhaustive]
pub struct MutateInOptions {
    /// Document expiry time. After this duration the document will be automatically deleted.
    pub expiry: Option<Duration>,
    /// If `true`, preserves the existing document expiry instead of resetting it.
    pub preserve_expiry: Option<bool>,
    /// CAS value for optimistic concurrency control.
    pub cas: Option<u64>,
    /// The durability level for this mutation.
    pub durability_level: Option<DurabilityLevel>,
    /// Controls whether the document must exist, must not exist, or is upserted.
    pub store_semantics: Option<StoreSemantics>,
    /// If `true`, allows accessing soft-deleted (tombstoned) documents.
    pub access_deleted: Option<bool>,
    /// Custom retry strategy for this operation.
    pub retry_strategy: Option<Arc<dyn RetryStrategy>>,
}

impl MutateInOptions {
    /// Creates a new `MutateInOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the document expiry time.
    pub fn expiry(mut self, expiry: Duration) -> Self {
        self.expiry = Some(expiry);
        self
    }

    /// If `true`, preserves the existing document expiry.
    pub fn preserve_expiry(mut self, preserve_expiry: bool) -> Self {
        self.preserve_expiry = Some(preserve_expiry);
        self
    }

    /// Sets the CAS value for optimistic concurrency control.
    pub fn cas(mut self, cas: u64) -> Self {
        self.cas = Some(cas);
        self
    }

    /// Sets the durability level for this mutation.
    pub fn durability_level(mut self, durability_level: impl Into<DurabilityLevel>) -> Self {
        self.durability_level = Some(durability_level.into());
        self
    }

    /// Sets the store semantics (replace, upsert, or insert).
    pub fn store_semantics(mut self, store_semantics: StoreSemantics) -> Self {
        self.store_semantics = Some(store_semantics);
        self
    }

    /// If `true`, allows accessing soft-deleted (tombstoned) documents.
    // Internal
    pub fn access_deleted(mut self, access_deleted: bool) -> Self {
        self.access_deleted = Some(access_deleted);
        self
    }

    /// Sets a custom retry strategy for this operation.
    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = Some(retry_strategy);
        self
    }
}
