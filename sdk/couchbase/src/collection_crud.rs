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

//! Key-value CRUD operations on a [`Collection`].
//!
//! This module implements the core document operations: get, upsert, insert, replace, remove,
//! exists, touch, get-and-touch, get-and-lock, unlock, and sub-document lookup_in/mutate_in.
//!
//! All methods accept an optional options struct (pass `None` for defaults) and return
//! a `Result` with the appropriate result type.

use crate::collection::Collection;
use crate::get_replica_strategy::GetReplicaStrategy;
use crate::options::kv_options::*;
use crate::results::kv_results::*;
use crate::subdoc::lookup_in_specs::LookupInSpec;
use crate::subdoc::mutate_in_specs::MutateInSpec;
use crate::tracing::SERVICE_VALUE_KV;
use crate::transcoding;
use couchbase_core::create_span;
use serde::Serialize;
use std::time::Duration;
use tracing::Instrument;

impl Collection {
    /// Upserts (inserts or replaces) a JSON document in the collection.
    ///
    /// The value is automatically serialized to JSON using `serde`. To store raw bytes
    /// with custom flags, use [`upsert_raw`](Collection::upsert_raw).
    ///
    /// # Arguments
    ///
    /// * `id` — The document key.
    /// * `value` — A value that implements `Serialize`.
    /// * `options` — Optional [`UpsertOptions`] (expiry, durability, etc.). Pass `None` for defaults.
    ///
    /// # Errors
    ///
    /// Returns an error on encoding failure, timeout, or server-side errors.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use couchbase::collection::Collection;
    /// # async fn example(collection: Collection) -> couchbase::error::Result<()> {
    /// use serde_json::json;
    /// let result = collection.upsert("doc::1", &json!({"key": "value"}), None).await?;
    /// println!("CAS: {}", result.cas());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn upsert<V: Serialize>(
        &self,
        id: impl AsRef<str>,
        value: V,
        options: impl Into<Option<UpsertOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        let span = create_span!("upsert").with_durability(options.durability_level.as_ref());
        let ctx = self
            .tracing_client
            .begin_operation(Some(SERVICE_VALUE_KV), self.keyspace(), span)
            .await;
        let (value, flags) = match self
            .tracing_client
            .with_request_encoding_span(|| transcoding::json::encode(value))
            .instrument(ctx.span().clone())
            .await
        {
            Ok(v) => v,
            Err(e) => {
                ctx.end_operation(Some(&e));
                return Err(e);
            }
        };
        let result = self
            .core_kv_client
            .upsert(id.as_ref(), &value, flags, options)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    /// Upserts a document with raw bytes and explicit flags.
    ///
    /// Unlike [`upsert`](Collection::upsert), no automatic JSON encoding is performed.
    /// The caller is responsible for encoding the value and providing the correct
    /// [common flags](crate::transcoding).
    ///
    /// # Arguments
    ///
    /// * `id` — The document key.
    /// * `value` — The raw byte content of the document.
    /// * `flags` — Common flags indicating the content type (see the [`transcoding`] module).
    /// * `options` — Optional [`UpsertOptions`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use couchbase::collection::Collection;
    /// # async fn example(collection: &Collection) -> couchbase::error::Result<()> {
    /// use couchbase::transcoding;
    ///
    /// // Store pre-encoded JSON bytes with JSON flags.
    /// let json = br#"{"greeting": "hello"}"#;
    /// let (content, flags) = transcoding::raw_json::encode(json)?;
    /// collection.upsert_raw("doc-key", &content, flags, None).await?;
    ///
    /// // Store arbitrary binary data with binary flags.
    /// let binary_data: &[u8] = &[0xDE, 0xAD, 0xBE, 0xEF];
    /// let (content, flags) = transcoding::raw_binary::encode(binary_data)?;
    /// collection.upsert_raw("bin-key", &content, flags, None).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn upsert_raw(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        flags: u32,
        options: impl Into<Option<UpsertOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        let span = create_span!("upsert").with_durability(options.durability_level.as_ref());
        let ctx = self
            .tracing_client
            .begin_operation(Some(SERVICE_VALUE_KV), self.keyspace(), span)
            .await;
        let result = self
            .core_kv_client
            .upsert(id.as_ref(), value, flags, options)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    /// Inserts a new JSON document into the collection.
    ///
    /// Fails with [`ErrorKind::DocumentExists`](crate::error::ErrorKind::DocumentExists)
    /// if a document with the same key already exists.
    ///
    /// # Arguments
    ///
    /// * `id` — The document key.
    /// * `value` — A value that implements `Serialize`.
    /// * `options` — Optional [`InsertOptions`].
    pub async fn insert<V: Serialize>(
        &self,
        id: impl AsRef<str>,
        value: V,
        options: impl Into<Option<InsertOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        let span = create_span!("insert").with_durability(options.durability_level.as_ref());
        let ctx = self
            .tracing_client
            .begin_operation(Some(SERVICE_VALUE_KV), self.keyspace(), span)
            .await;
        let (value, flags) = match self
            .tracing_client
            .with_request_encoding_span(|| transcoding::json::encode(value))
            .instrument(ctx.span().clone())
            .await
        {
            Ok(v) => v,
            Err(e) => {
                ctx.end_operation(Some(&e));
                return Err(e);
            }
        };
        let result = self
            .core_kv_client
            .insert(id.as_ref(), &value, flags, options)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    /// Inserts a document with raw bytes and explicit flags.
    ///
    /// See [`insert`](Collection::insert) for the JSON variant.
    pub async fn insert_raw(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        flags: u32,
        options: impl Into<Option<InsertOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        let span = create_span!("insert").with_durability(options.durability_level.as_ref());
        let ctx = self
            .tracing_client
            .begin_operation(Some(SERVICE_VALUE_KV), self.keyspace(), span)
            .await;
        let result = self
            .core_kv_client
            .insert(id.as_ref(), value, flags, options)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    /// Replaces an existing JSON document in the collection.
    ///
    /// Fails with [`ErrorKind::DocumentNotFound`](crate::error::ErrorKind::DocumentNotFound)
    /// if the document does not exist. Use [`ReplaceOptions::cas`] for optimistic concurrency.
    ///
    /// # Arguments
    ///
    /// * `id` — The document key.
    /// * `value` — A value that implements `Serialize`.
    /// * `options` — Optional [`ReplaceOptions`].
    pub async fn replace<V: Serialize>(
        &self,
        id: impl AsRef<str>,
        value: V,
        options: impl Into<Option<ReplaceOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        let span = create_span!("replace").with_durability(options.durability_level.as_ref());
        let ctx = self
            .tracing_client
            .begin_operation(Some(SERVICE_VALUE_KV), self.keyspace(), span)
            .await;
        let (value, flags) = match self
            .tracing_client
            .with_request_encoding_span(|| transcoding::json::encode(value))
            .instrument(ctx.span().clone())
            .await
        {
            Ok(v) => v,
            Err(e) => {
                ctx.end_operation(Some(&e));
                return Err(e);
            }
        };
        let result = self
            .core_kv_client
            .replace(id.as_ref(), &value, flags, options)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    /// Replaces a document with raw bytes and explicit flags.
    ///
    /// See [`replace`](Collection::replace) for the JSON variant.
    pub async fn replace_raw(
        &self,
        id: impl AsRef<str>,
        value: &[u8],
        flags: u32,
        options: impl Into<Option<ReplaceOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let options = options.into().unwrap_or_default();
        let span = create_span!("replace").with_durability(options.durability_level.as_ref());
        let ctx = self
            .tracing_client
            .begin_operation(Some(SERVICE_VALUE_KV), self.keyspace(), span)
            .await;
        let result = self
            .core_kv_client
            .replace(id.as_ref(), value, flags, options)
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    /// Removes (deletes) a document from the collection.
    ///
    /// Fails with [`ErrorKind::DocumentNotFound`](crate::error::ErrorKind::DocumentNotFound)
    /// if the document does not exist. Use [`RemoveOptions::cas`] for optimistic concurrency.
    ///
    /// # Arguments
    ///
    /// * `id` — The document key.
    /// * `options` — Optional [`RemoveOptions`].
    pub async fn remove(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<RemoveOptions>>,
    ) -> crate::error::Result<MutationResult> {
        let ctx = self
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.keyspace(),
                create_span!("remove"),
            )
            .await;
        let result = self
            .core_kv_client
            .remove(id.as_ref(), options.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    /// Retrieves a document from the collection.
    ///
    /// Returns a [`GetResult`] which can be deserialized into a concrete type via
    /// [`GetResult::content_as`].
    ///
    /// Fails with [`ErrorKind::DocumentNotFound`](crate::error::ErrorKind::DocumentNotFound)
    /// if the document does not exist.
    ///
    /// # Arguments
    ///
    /// * `id` — The document key.
    /// * `options` — Optional [`GetOptions`] (projections, expiry, etc.).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use couchbase::collection::Collection;
    /// # async fn example(collection: Collection) -> couchbase::error::Result<()> {
    /// let result = collection.get("user::1", None).await?;
    /// let user: serde_json::Value = result.content_as()?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<GetOptions>>,
    ) -> crate::error::Result<GetResult> {
        let ctx = self
            .tracing_client
            .begin_operation(Some(SERVICE_VALUE_KV), self.keyspace(), create_span!("get"))
            .await;
        let result = self
            .core_kv_client
            .get(id.as_ref(), options.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    /// Retrieves a document from one of its replicas, per the given [`GetReplicaStrategy`].
    ///
    /// Returns a [`GetReplicaResult`] which can be deserialized into a concrete type via
    /// [`GetReplicaResult::content_as`].
    ///
    /// Fails with
    /// [`ErrorKind::DocumentNotFoundOnReplica`](crate::error::ErrorKind::DocumentNotFoundOnReplica)
    /// if the replica read has no document for this key,
    /// [`ErrorKind::ReplicaIndexOutOfBounds`](crate::error::ErrorKind::ReplicaIndexOutOfBounds)
    /// if the requested replica index exceeds the bucket's replica count (and `wrap` was not
    /// set), or
    /// [`ErrorKind::ReplicaIndexCurrentlyUnavailable`](crate::error::ErrorKind::ReplicaIndexCurrentlyUnavailable)
    /// if the requested replica currently has no node assigned (e.g. mid-rebalance/failover).
    ///
    /// # Arguments
    ///
    /// * `id` — The document key.
    /// * `strategy` — A [`GetReplicaStrategy`] describing which replica to read from.
    /// * `options` — Optional [`GetReplicaOptions`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use couchbase::collection::Collection;
    /// # use couchbase::get_replica_strategy::{GetReplicaStrategy, ReplicaIndex};
    /// # async fn example(collection: Collection) -> couchbase::error::Result<()> {
    /// let strategy = GetReplicaStrategy::from_index(ReplicaIndex::First, None);
    /// let result = collection.get_replica("user::1", strategy, None).await?;
    /// let user: serde_json::Value = result.content_as()?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_replica(
        &self,
        id: impl AsRef<str>,
        strategy: impl Into<GetReplicaStrategy>,
        options: impl Into<Option<GetReplicaOptions>>,
    ) -> crate::error::Result<GetReplicaResult> {
        let ctx = self
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.keyspace(),
                create_span!("get_replica"),
            )
            .await;
        let result = self
            .core_kv_client
            .get_replica(
                id.as_ref(),
                strategy.into(),
                options.into().unwrap_or_default(),
            )
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    /// Checks whether a document exists in the collection without retrieving its content.
    ///
    /// Returns an [`ExistsResult`] with an `exists()` method that returns `true` if the
    /// document is present.
    pub async fn exists(
        &self,
        id: impl AsRef<str>,
        options: impl Into<Option<ExistsOptions>>,
    ) -> crate::error::Result<ExistsResult> {
        let ctx = self
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.keyspace(),
                create_span!("exists"),
            )
            .await;
        let result = self
            .core_kv_client
            .exists(id.as_ref(), options.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    /// Retrieves a document and simultaneously updates its expiry time.
    ///
    /// # Arguments
    ///
    /// * `id` — The document key.
    /// * `expiry` — The new expiry duration from now. Use `Duration::ZERO` to remove expiry.
    /// * `options` — Optional [`GetAndTouchOptions`].
    pub async fn get_and_touch(
        &self,
        id: impl AsRef<str>,
        expiry: Duration,
        options: impl Into<Option<GetAndTouchOptions>>,
    ) -> crate::error::Result<GetResult> {
        let ctx = self
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.keyspace(),
                create_span!("get_and_touch"),
            )
            .await;
        let result = self
            .core_kv_client
            .get_and_touch(id.as_ref(), expiry, options.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    /// Retrieves a document and locks it for the specified duration.
    ///
    /// While locked, only the holder of the CAS value can modify the document.
    /// Use [`unlock`](Collection::unlock) to release the lock before expiry,
    /// or let it expire automatically.
    ///
    /// # Arguments
    ///
    /// * `id` — The document key.
    /// * `lock_time` — How long to lock the document (max 30 seconds).
    /// * `options` — Optional [`GetAndLockOptions`].
    pub async fn get_and_lock(
        &self,
        id: impl AsRef<str>,
        lock_time: Duration,
        options: impl Into<Option<GetAndLockOptions>>,
    ) -> crate::error::Result<GetResult> {
        let ctx = self
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.keyspace(),
                create_span!("get_and_lock"),
            )
            .await;
        let result = self
            .core_kv_client
            .get_and_lock(id.as_ref(), lock_time, options.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    /// Unlocks a document that was previously locked with [`get_and_lock`](Collection::get_and_lock).
    ///
    /// # Arguments
    ///
    /// * `id` — The document key.
    /// * `cas` — The CAS value from the [`GetResult`] returned by `get_and_lock`.
    /// * `options` — Optional [`UnlockOptions`].
    pub async fn unlock(
        &self,
        id: impl AsRef<str>,
        cas: u64,
        options: impl Into<Option<UnlockOptions>>,
    ) -> crate::error::Result<()> {
        let ctx = self
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.keyspace(),
                create_span!("unlock"),
            )
            .await;
        let result = self
            .core_kv_client
            .unlock(id.as_ref(), cas, options.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    /// Updates the expiry time of a document without retrieving its content.
    ///
    /// # Arguments
    ///
    /// * `id` — The document key.
    /// * `expiry` — The new expiry duration from now.
    /// * `options` — Optional [`TouchOptions`].
    pub async fn touch(
        &self,
        id: impl AsRef<str>,
        expiry: Duration,
        options: impl Into<Option<TouchOptions>>,
    ) -> crate::error::Result<TouchResult> {
        let ctx = self
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.keyspace(),
                create_span!("touch"),
            )
            .await;
        let result = self
            .core_kv_client
            .touch(id.as_ref(), expiry, options.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    /// Performs one or more sub-document lookup operations on a document.
    ///
    /// Sub-document lookups allow you to retrieve or check specific paths within a JSON
    /// document without fetching the entire document.
    ///
    /// # Arguments
    ///
    /// * `id` — The document key.
    /// * `specs` — A slice of [`LookupInSpec`]s describing the paths to look up.
    /// * `options` — Optional [`LookupInOptions`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use couchbase::collection::Collection;
    /// use couchbase::subdoc::lookup_in_specs::LookupInSpec;
    ///
    /// # async fn example(collection: Collection) -> couchbase::error::Result<()> {
    /// let result = collection.lookup_in("user::1", &[
    ///     LookupInSpec::get("name", None),
    ///     LookupInSpec::exists("email", None),
    ///     LookupInSpec::count("addresses", None),
    /// ], None).await?;
    ///
    /// let name: String = result.content_as(0)?;
    /// let has_email: bool = result.exists(1)?;
    /// let addr_count: u64 = result.content_as(2)?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn lookup_in(
        &self,
        id: impl AsRef<str>,
        specs: &[LookupInSpec],
        options: impl Into<Option<LookupInOptions>>,
    ) -> crate::error::Result<LookupInResult> {
        let ctx = self
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.keyspace(),
                create_span!("lookup_in"),
            )
            .await;
        let result = self
            .core_kv_client
            .lookup_in(id.as_ref(), specs, options.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    /// Performs one or more sub-document mutation operations on a document.
    ///
    /// Sub-document mutations allow you to modify specific paths within a JSON document
    /// atomically without replacing the entire document.
    ///
    /// # Arguments
    ///
    /// * `id` — The document key.
    /// * `specs` — A slice of [`MutateInSpec`]s describing the mutations.
    /// * `options` — Optional [`MutateInOptions`] (CAS, expiry, store semantics, etc.).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use couchbase::collection::Collection;
    /// use couchbase::subdoc::mutate_in_specs::MutateInSpec;
    ///
    /// # async fn example(collection: Collection) -> couchbase::error::Result<()> {
    /// let result = collection.mutate_in("user::1", &[
    ///     MutateInSpec::upsert("name", "Bob", None)?,
    ///     MutateInSpec::remove("temporary_field", None),
    /// ], None).await?;
    /// println!("CAS after mutation: {}", result.cas());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn mutate_in(
        &self,
        id: impl AsRef<str>,
        specs: &[MutateInSpec],
        options: impl Into<Option<MutateInOptions>>,
    ) -> crate::error::Result<MutateInResult> {
        let ctx = self
            .tracing_client
            .begin_operation(
                Some(SERVICE_VALUE_KV),
                self.keyspace(),
                create_span!("mutate_in"),
            )
            .await;
        let result = self
            .core_kv_client
            .mutate_in(id.as_ref(), specs, options.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }
}
