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

//! Result types for key-value operations.

use crate::mutation_state::MutationToken;
use crate::subdoc::lookup_in_specs::LookupInOpType;
use crate::{error, transcoding};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;

/// The result of a `get`, `get_and_touch`, or `get_and_lock` operation.
///
/// Contains the document content, CAS value, and optionally the expiry time.
///
/// # Example
///
/// ```rust,no_run
/// # use couchbase::collection::Collection;
/// # async fn example(collection: Collection) -> couchbase::error::Result<()> {
/// let result = collection.get("doc::1", None).await?;
///
/// // Deserialize as a typed struct
/// let value: serde_json::Value = result.content_as()?;
///
/// // Access raw bytes and flags
/// let (bytes, flags) = result.content_as_raw();
///
/// // Get the CAS value for optimistic locking
/// let cas = result.cas();
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct GetResult {
    pub(crate) content: Bytes,
    pub(crate) flags: u32,
    pub(crate) cas: u64,
    pub(crate) expiry_time: Option<DateTime<Utc>>,
}

impl GetResult {
    /// Deserializes the document content into the requested type using JSON transcoding.
    ///
    /// # Errors
    ///
    /// Returns a decoding error if the content cannot be deserialized into `V`.
    pub fn content_as<V: DeserializeOwned>(&self) -> error::Result<V> {
        let (content, flags) = self.content_as_raw();
        transcoding::json::decode(content, flags)
    }

    /// Returns the raw document content bytes and the associated common flags.
    ///
    /// Use this for custom transcoding or when working with non-JSON data.
    pub fn content_as_raw(&self) -> (&[u8], u32) {
        (&self.content, self.flags)
    }

    /// Returns the CAS (Compare-And-Swap) value of the document.
    ///
    /// The CAS value changes on every mutation and can be used for optimistic concurrency
    /// control with operations like [`replace`](crate::collection::Collection) and
    /// [`remove`](crate::collection::Collection).
    pub fn cas(&self) -> u64 {
        self.cas
    }

    /// Returns the document's expiry time, if it was requested via [`GetOptions::expiry`](crate::options::kv_options::GetOptions).
    pub fn expiry_time(&self) -> Option<&DateTime<Utc>> {
        self.expiry_time.as_ref()
    }
}

impl From<couchbase_core::results::kv::GetResult> for GetResult {
    fn from(result: couchbase_core::results::kv::GetResult) -> Self {
        Self {
            content: result.value,
            flags: result.flags,
            cas: result.cas,
            expiry_time: None,
        }
    }
}

impl From<couchbase_core::results::kv::GetAndTouchResult> for GetResult {
    fn from(result: couchbase_core::results::kv::GetAndTouchResult) -> Self {
        Self {
            content: result.value,
            flags: result.flags,
            cas: result.cas,
            expiry_time: None,
        }
    }
}

impl From<couchbase_core::results::kv::GetAndLockResult> for GetResult {
    fn from(result: couchbase_core::results::kv::GetAndLockResult) -> Self {
        Self {
            content: result.value,
            flags: result.flags,
            cas: result.cas,
            expiry_time: None,
        }
    }
}

/// The result of an [`exists`](crate::collection::Collection) operation.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct ExistsResult {
    exists: bool,
    cas: u64,
}

impl ExistsResult {
    /// Returns `true` if the document exists in the collection.
    pub fn exists(&self) -> bool {
        self.exists
    }

    /// Returns the CAS value of the document, if it exists.
    pub fn cas(&self) -> u64 {
        self.cas
    }
}

impl From<couchbase_core::results::kv::GetMetaResult> for ExistsResult {
    fn from(result: couchbase_core::results::kv::GetMetaResult) -> Self {
        Self {
            exists: !result.deleted,
            cas: result.cas,
        }
    }
}

/// The result of a [`touch`](crate::collection::Collection) operation.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct TouchResult {
    cas: u64,
}

impl TouchResult {
    /// Returns the CAS value of the document after the touch.
    pub fn cas(&self) -> u64 {
        self.cas
    }
}

impl From<couchbase_core::results::kv::TouchResult> for TouchResult {
    fn from(result: couchbase_core::results::kv::TouchResult) -> Self {
        Self { cas: result.cas }
    }
}

/// The result of a mutation operation (upsert, insert, replace, or remove).
///
/// Contains the CAS value and optionally a [`MutationToken`] for use with
/// [`MutationState`](crate::mutation_state::MutationState) for scan consistency.
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct MutationResult {
    pub(crate) cas: u64,
    pub(crate) mutation_token: Option<MutationToken>,
}

impl MutationResult {
    /// Returns the CAS value of the document after the mutation.
    pub fn cas(&self) -> u64 {
        self.cas
    }

    /// Returns the mutation token, which can be used with
    /// [`MutationState`](crate::mutation_state::MutationState) for `AtPlus` scan consistency.
    pub fn mutation_token(&self) -> Option<&MutationToken> {
        self.mutation_token.as_ref()
    }
}
#[derive(Debug, Clone, Eq, PartialEq)]
pub(crate) struct LookupInResultEntry {
    pub(crate) value: Option<Bytes>,
    pub(crate) error: Option<error::Error>,
    pub(crate) op: LookupInOpType,
}

/// The result of a [`lookup_in`](crate::collection::Collection) sub-document operation.
///
/// Access individual results by their spec index using [`content_as`](LookupInResult::content_as)
/// or [`exists`](LookupInResult::exists).
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct LookupInResult {
    pub(crate) cas: u64,
    pub(crate) entries: Vec<LookupInResultEntry>,
    pub(crate) is_deleted: bool,
}

impl LookupInResult {
    /// Deserializes the content at the given spec index into the requested type.
    ///
    /// # Arguments
    ///
    /// * `lookup_index` — The zero-based index of the spec in the lookup_in call.
    pub fn content_as<V: DeserializeOwned>(&self, lookup_index: usize) -> error::Result<V> {
        let content = self.content_as_raw(lookup_index)?;
        serde_json::from_slice(&content).map_err(error::Error::decoding_failure_from_serde)
    }

    /// Returns the raw bytes for the given spec index.
    pub fn content_as_raw(&self, lookup_index: usize) -> error::Result<Bytes> {
        if lookup_index >= self.entries.len() {
            return Err(error::Error::invalid_argument(
                "index",
                "index cannot be >= number of lookups",
            ));
        }

        let entry = self
            .entries
            .get(lookup_index)
            .ok_or_else(|| error::Error::invalid_argument("index", "index out of bounds"))?;

        if entry.op == LookupInOpType::Exists {
            let res = self.exists(lookup_index)?;
            let val = Bytes::from(
                serde_json::to_vec(&res).map_err(error::Error::decoding_failure_from_serde)?,
            );
            return Ok(val);
        }

        if let Some(err) = &entry.error {
            return Err(err.clone());
        }

        Ok(entry.value.clone().unwrap_or_default())
    }

    /// Returns whether the path at the given spec index exists in the document.
    ///
    /// For `LookupInSpec::exists` specs, this checks the path existence.
    /// For other spec types, it returns `true` if the path was found without error.
    pub fn exists(&self, lookup_index: usize) -> error::Result<bool> {
        if lookup_index >= self.entries.len() {
            return Err(error::Error::invalid_argument(
                "index",
                "index cannot be >= number of lookups",
            ));
        }

        let entry = self
            .entries
            .get(lookup_index)
            .ok_or_else(|| error::Error::invalid_argument("index", "index out of bounds"))?;

        if let Some(err) = &entry.error {
            return match err.kind() {
                error::ErrorKind::PathNotFound => Ok(false),
                _ => Err(err.clone()),
            };
        };

        Ok(true)
    }

    /// Returns the CAS value of the document.
    pub fn cas(&self) -> u64 {
        self.cas
    }
}

/// An individual entry within a [`MutateInResult`].
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct MutateInResultEntry {
    pub(crate) value: Option<Bytes>,
}

impl MutateInResultEntry {
    /// Returns a reference to the raw value returned by the server, if any (e.g. the new counter value).
    pub fn value(&self) -> Option<&Bytes> {
        self.value.as_ref()
    }

    /// Consumes this entry and returns the raw value, if any.
    pub fn into_value(self) -> Option<Bytes> {
        self.value
    }
}

/// The result of a [`mutate_in`](crate::collection::Collection) sub-document operation.
///
/// Access individual operation results by their spec index using
/// [`content_as`](MutateInResult::content_as) (useful for counter operations that return a value).
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct MutateInResult {
    pub(crate) cas: u64,
    pub(crate) mutation_token: Option<MutationToken>,
    pub(crate) entries: Vec<MutateInResultEntry>,
}

impl MutateInResult {
    /// Returns the CAS value of the document after the mutation.
    pub fn cas(&self) -> u64 {
        self.cas
    }

    /// Returns the mutation token for use with scan consistency.
    pub fn mutation_token(&self) -> Option<&MutationToken> {
        self.mutation_token.as_ref()
    }

    /// Returns the individual operation result entries, one per spec.
    pub fn entries(&self) -> &[MutateInResultEntry] {
        &self.entries
    }

    /// Deserializes the content at the given spec index (e.g. the new counter value).
    pub fn content_as<V: DeserializeOwned>(&self, mutate_index: usize) -> error::Result<V> {
        let content = self.content_as_raw(mutate_index)?;

        serde_json::from_slice(&content).map_err(error::Error::decoding_failure_from_serde)
    }

    /// Returns the raw bytes at the given spec index.
    pub fn content_as_raw(&self, mutate_index: usize) -> error::Result<Bytes> {
        if mutate_index >= self.entries.len() {
            return Err(error::Error::invalid_argument(
                "index",
                "index cannot be >= number of operations",
            ));
        }

        let entry = self
            .entries
            .get(mutate_index)
            .ok_or_else(|| error::Error::invalid_argument("index", "index out of bounds"))?;

        Ok(entry.value.clone().unwrap_or_default())
    }
}
