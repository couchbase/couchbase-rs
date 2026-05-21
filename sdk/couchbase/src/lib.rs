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

//! # Couchbase Rust SDK
//!
//! The official Couchbase SDK for Rust, providing asynchronous access to
//! [Couchbase Server](https://www.couchbase.com/) from Rust applications.
//!
//! This crate enables you to interact with a Couchbase cluster for key-value (KV) operations,
//! SQL++ (N1QL) queries, Full-Text Search (FTS), sub-document operations, and cluster management.
//!
//! # Getting Started
//!
//! Add the dependency to your `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! couchbase = "1.0.0-beta.1"
//! tokio = { version = "1", features = ["full"] }
//! serde = { version = "1", features = ["derive"] }
//! serde_json = "1"
//! ```
//!
//! ## Connecting to a Cluster
//!
//! The entry point is the [`Cluster`](cluster::Cluster), which is created by calling
//! [`Cluster::connect`](cluster::Cluster::connect) with a connection string and
//! [`ClusterOptions`](options::cluster_options::ClusterOptions).
//!
//! ```rust,no_run
//! use couchbase::cluster::Cluster;
//! use couchbase::authenticator::PasswordAuthenticator;
//! use couchbase::options::cluster_options::ClusterOptions;
//!
//! # async fn example() -> couchbase::error::Result<()> {
//! let authenticator = PasswordAuthenticator::new("username", "password");
//! let options = ClusterOptions::new(authenticator.into());
//!
//! let cluster = Cluster::connect("couchbase://localhost", options).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Basic Key-Value Operations
//!
//! Once connected, open a [`Bucket`](bucket::Bucket), navigate to a [`Scope`](scope::Scope)
//! and [`Collection`](collection::Collection), and perform CRUD operations:
//!
//! ```rust,no_run
//! use couchbase::cluster::Cluster;
//! use couchbase::authenticator::PasswordAuthenticator;
//! use couchbase::options::cluster_options::ClusterOptions;
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Debug, Serialize, Deserialize)]
//! struct User {
//!     name: String,
//!     email: String,
//!     age: u32,
//! }
//!
//! # async fn example() -> couchbase::error::Result<()> {
//! let authenticator = PasswordAuthenticator::new("username", "password");
//! let options = ClusterOptions::new(authenticator.into());
//! let cluster = Cluster::connect("couchbase://localhost", options).await?;
//!
//! let bucket = cluster.bucket("my-bucket");
//! let collection = bucket.default_collection();
//!
//! // Upsert a document
//! let user = User { name: "Alice".into(), email: "alice@example.com".into(), age: 30 };
//! let result = collection.upsert("user::alice", &user, None).await?;
//! println!("CAS: {}", result.cas());
//!
//! // Get a document
//! let result = collection.get("user::alice", None).await?;
//! let user: User = result.content_as()?;
//! println!("Got user: {:?}", user);
//!
//! // Remove a document
//! collection.remove("user::alice", None).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## SQL++ (N1QL) Queries
//!
//! Execute SQL++ queries against the cluster or a specific scope using
//! [`Cluster::query`](cluster::Cluster::query) or [`Scope::query`](scope::Scope::query):
//!
//! ```rust,no_run
//! use couchbase::cluster::Cluster;
//! use couchbase::authenticator::PasswordAuthenticator;
//! use couchbase::options::cluster_options::ClusterOptions;
//! use couchbase::options::query_options::QueryOptions;
//! use futures::TryStreamExt;
//! use serde_json::Value;
//!
//! # async fn example() -> couchbase::error::Result<()> {
//! # let authenticator = PasswordAuthenticator::new("username", "password");
//! # let options = ClusterOptions::new(authenticator.into());
//! # let cluster = Cluster::connect("couchbase://localhost", options).await?;
//! let opts = QueryOptions::new()
//!     .add_positional_parameter("Alice")?;
//!
//! let mut result = cluster.query(
//!     "SELECT * FROM `my-bucket` WHERE name = $1",
//!     opts,
//! ).await?;
//!
//! let rows: Vec<Value> = result.rows().try_collect().await?;
//! for row in rows {
//!     println!("{:?}", row);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Sub-Document Operations
//!
//! Operate on parts of a JSON document using [`lookup_in`](collection::Collection) and
//! [`mutate_in`](collection::Collection) with sub-document specs:
//!
//! ```rust,no_run
//! use couchbase::subdoc::lookup_in_specs::LookupInSpec;
//! use couchbase::subdoc::mutate_in_specs::MutateInSpec;
//! # use couchbase::cluster::Cluster;
//! # use couchbase::authenticator::PasswordAuthenticator;
//! # use couchbase::options::cluster_options::ClusterOptions;
//!
//! # async fn example() -> couchbase::error::Result<()> {
//! # let authenticator = PasswordAuthenticator::new("username", "password");
//! # let options = ClusterOptions::new(authenticator.into());
//! # let cluster = Cluster::connect("couchbase://localhost", options).await?;
//! # let collection = cluster.bucket("b").default_collection();
//! // Lookup sub-document paths
//! let result = collection.lookup_in("user::alice", &[
//!     LookupInSpec::get("name", None),
//!     LookupInSpec::exists("email", None),
//! ], None).await?;
//!
//! let name: String = result.content_as(0)?;
//! let email_exists: bool = result.exists(1)?;
//!
//! // Mutate sub-document paths
//! collection.mutate_in("user::alice", &[
//!     MutateInSpec::upsert("age", 31, None)?,
//!     MutateInSpec::insert("verified", true, None)?,
//! ], None).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Error Handling
//!
//! All fallible operations return [`error::Result<T>`], which wraps
//! [`error::Error`]. Inspect the error kind via [`Error::kind()`](error::Error::kind):
//!
//! ```rust,no_run
//! use couchbase::error::{Error, ErrorKind};
//! # use couchbase::cluster::Cluster;
//! # use couchbase::authenticator::PasswordAuthenticator;
//! # use couchbase::options::cluster_options::ClusterOptions;
//!
//! # async fn example() -> couchbase::error::Result<()> {
//! # let authenticator = PasswordAuthenticator::new("username", "password");
//! # let options = ClusterOptions::new(authenticator.into());
//! # let cluster = Cluster::connect("couchbase://localhost", options).await?;
//! # let collection = cluster.bucket("b").default_collection();
//! match collection.get("nonexistent-key", None).await {
//!     Ok(result) => println!("Found document"),
//!     Err(e) if *e.kind() == ErrorKind::DocumentNotFound => {
//!         println!("Document does not exist");
//!     }
//!     Err(e) => return Err(e),
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Architecture
//!
//! The SDK follows the Couchbase data model hierarchy:
//!
//! - **[`Cluster`](cluster::Cluster)** — Top-level entry point. Connect here, run cluster-level
//!   queries, and access management APIs.
//! - **[`Bucket`](bucket::Bucket)** — Represents a Couchbase bucket. Obtained from `Cluster::bucket()`.
//! - **[`Scope`](scope::Scope)** — A namespace within a bucket. Obtained from `Bucket::scope()`.
//!   Supports scoped queries and Full-Text Search.
//! - **[`Collection`](collection::Collection)** — Holds documents. Obtained from `Scope::collection()`.
//!   Supports all KV, sub-document, and data structure operations.
//! - **[`BinaryCollection`](collection::BinaryCollection)** — Accessed via `Collection::binary()`.
//!   Provides binary append/prepend and counter operations.
//!
//! # Modules Overview
//!
//! | Module | Description |
//! |--------|-------------|
//! | [`authenticator`] | Authentication types ([`PasswordAuthenticator`](authenticator::PasswordAuthenticator), [`CertificateAuthenticator`](authenticator::CertificateAuthenticator)) |
//! | [`cluster`] | [`Cluster`](cluster::Cluster) — the main entry point |
//! | [`bucket`] | [`Bucket`](bucket::Bucket) — bucket-level operations |
//! | [`scope`] | [`Scope`](scope::Scope) — scoped queries and search |
//! | [`collection`] | [`Collection`](collection::Collection) and [`BinaryCollection`](collection::BinaryCollection) |
//! | [`collection_ds`] | Data structure helpers (list, map, set, queue) on `Collection` |
//! | [`options`] | Option structs for configuring every operation |
//! | [`results`] | Result types returned by operations |
//! | [`error`] | [`Error`](error::Error) and [`ErrorKind`](error::ErrorKind) |
//! | [`subdoc`] | Sub-document operation specs ([`LookupInSpec`](subdoc::lookup_in_specs::LookupInSpec), [`MutateInSpec`](subdoc::mutate_in_specs::MutateInSpec)) |
//! | [`search`] | Full-Text Search queries, facets, sorting, and vector search |
//! | [`transcoding`] | Encoding/decoding helpers (JSON, raw binary, raw JSON, raw string) |
//! | [`management`] | Cluster management (buckets, collections, users, query indexes, search indexes) |
//! | [`durability_level`] | [`DurabilityLevel`](durability_level::DurabilityLevel) constants |
//! | [`mod@mutation_state`] | [`MutationState`](mutation_state::MutationState) for scan consistency |
//! | [`retry`] | Retry strategies ([`BestEffortRetryStrategy`](retry::BestEffortRetryStrategy)) |
//! | [`diagnostics`] | Connection state types |
//! | [`service_type`] | [`ServiceType`](service_type::ServiceType) constants |
//! | [`logging_meter`] | [`LoggingMeter`](logging_meter::LoggingMeter) for operation metrics |
//! | [`threshold_logging_tracer`] | [`ThresholdLoggingTracer`](threshold_logging_tracer::ThresholdLoggingTracer) for slow-operation logging |
//!
//! # Feature Flags
//!
//! | Feature | Default | Description |
//! |---------|---------|-------------|
//! | `default-tls` | ✅ | Alias for `rustls-tls` |
//! | `rustls-tls` | ✅ | Use `rustls` for TLS (recommended) |
//! | `native-tls` | ❌ | Use the platform's native TLS stack instead of `rustls` |
//! | `logging-meter` | ✅ | Enable [`LoggingMeter`](logging_meter::LoggingMeter) (pulls in `hdrhistogram` and `tracing-subscriber`) |
//! | `threshold-logging-tracer` | ✅ | Enable [`ThresholdLoggingTracer`](threshold_logging_tracer::ThresholdLoggingTracer) (pulls in `tracing-subscriber`) |
//! | `dns-srv` | ✅ | Enable DNS-SRV bootstrap via `hickory-resolver` |
//!
//! Note that the SDK does not typically use feature flags for API stability levels.
//! Instead, unstable features are commented with **uncommitted** or **volatile**.

extern crate core;
pub mod authenticator;
pub mod bucket;
mod capella_ca;
mod clients;
pub mod cluster;
pub mod collection;
mod collection_binary_crud;
mod collection_crud;
pub mod collection_ds;
pub mod diagnostics;
pub mod durability_level;
pub mod error;
mod error_context;
#[cfg(feature = "logging-meter")]
pub mod logging_meter;
pub mod management;
pub mod mutation_state;
pub mod options;
pub mod results;
pub mod retry;
pub mod scope;
pub mod search;
pub mod service_type;
pub mod subdoc;
#[cfg(feature = "threshold-logging-tracer")]
pub mod threshold_logging_tracer;
mod tracing;
pub mod transcoding;
