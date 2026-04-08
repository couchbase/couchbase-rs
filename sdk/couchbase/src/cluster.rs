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

//! The main entry point for the Couchbase SDK.
//!
//! Use [`Cluster::connect`](Cluster::connect) to establish a connection to a Couchbase cluster.
//! From a `Cluster` you can open [`Bucket`]s, execute cluster-level
//! SQL++ queries, and access management APIs.

use crate::authenticator::Authenticator;
use crate::bucket::Bucket;
use crate::clients::bucket_mgmt_client::BucketMgmtClient;
use crate::clients::cluster_client::ClusterClient;
use crate::clients::diagnostics_client::DiagnosticsClient;
use crate::clients::query_client::QueryClient;
use crate::clients::tracing_client::TracingClient;
use crate::clients::user_mgmt_client::UserMgmtClient;
use crate::error;
use crate::management::buckets::bucket_manager::BucketManager;
use crate::management::users::user_manager::UserManager;
use crate::options::cluster_options::ClusterOptions;
use crate::options::diagnostic_options::{DiagnosticsOptions, PingOptions, WaitUntilReadyOptions};
use crate::options::query_options::QueryOptions;
use crate::results::diagnostics::{DiagnosticsResult, PingReport};
use crate::results::query_results::QueryResult;
use crate::tracing::{Keyspace, SERVICE_VALUE_QUERY};
use couchbase_core::create_span;
use std::sync::Arc;
use tracing::info;
use tracing::Instrument;

/// The main entry point for interacting with a Couchbase cluster.
///
/// A `Cluster` is created by calling [`Cluster::connect`] with a connection string and
/// [`ClusterOptions`]. From a `Cluster`, you
/// can open [`Bucket`]s, execute cluster-level SQL++ queries, access
/// management APIs, and perform diagnostics.
///
/// `Cluster` is cheaply cloneable — all clones share the same underlying connection pool.
///
/// To close connections to the Couchbase cluster this `Cluster` must be dropped.
/// After dropping, all operations against child resources will error with [`error::ErrorKind::ClusterDropped`].
///
/// # Example
///
/// ```rust,no_run
/// use couchbase::cluster::Cluster;
/// use couchbase::authenticator::PasswordAuthenticator;
/// use couchbase::options::cluster_options::ClusterOptions;
///
/// # async fn example() -> couchbase::error::Result<()> {
/// let opts = ClusterOptions::new(
///     PasswordAuthenticator::new("user", "pass").into(),
/// );
/// let cluster = Cluster::connect("couchbase://127.0.0.1", opts).await?;
///
/// // Open a bucket
/// let bucket = cluster.bucket("travel-sample");
///
/// // Run a query
/// let mut result = cluster.query("SELECT 1=1", None).await?;
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct Cluster {
    client: Arc<ClusterClient>,
    query_client: Arc<QueryClient>,
    bucket_mgmt_client: Arc<BucketMgmtClient>,
    user_mgmt_client: Arc<UserMgmtClient>,
    diagnostics_client: Arc<DiagnosticsClient>,
    tracing_client: Arc<TracingClient>,
}

impl Cluster {
    /// Connects to a Couchbase cluster and returns a new [`Cluster`] instance.
    ///
    /// The connection string should follow the Couchbase URI scheme, e.g.
    /// `couchbase://host1,host2` or `couchbases://host` for TLS connections.
    ///
    /// # Arguments
    ///
    /// * `conn_str` — A Couchbase connection string (e.g. `"couchbase://localhost"`).
    /// * `opts` — [`ClusterOptions`] containing
    ///   the authenticator and optional configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if the connection string is invalid or the initial connection fails.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use couchbase::cluster::Cluster;
    /// use couchbase::authenticator::PasswordAuthenticator;
    /// use couchbase::options::cluster_options::ClusterOptions;
    ///
    /// # async fn example() -> couchbase::error::Result<()> {
    /// let cluster = Cluster::connect(
    ///     "couchbase://localhost",
    ///     ClusterOptions::new(PasswordAuthenticator::new("user", "pass").into()),
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(
        conn_str: impl AsRef<str>,
        opts: ClusterOptions,
    ) -> error::Result<Cluster> {
        info!("SDK Version: {}", env!("CARGO_PKG_VERSION"));
        info!("Cluster Options {opts}");
        let client = Arc::new(ClusterClient::connect(conn_str, opts).await?);

        let query_client = Arc::new(client.query_client());
        let bucket_mgmt_client = Arc::new(client.buckets_client());
        let user_mgmt_client = Arc::new(client.users_client());
        let diagnostics_client = Arc::new(client.diagnostics_client());
        let tracing_client = Arc::new(client.tracing_client());

        Ok(Cluster {
            client,
            query_client,
            bucket_mgmt_client,
            user_mgmt_client,
            diagnostics_client,
            tracing_client,
        })
    }

    /// Returns a [`Bucket`] instance for the given bucket name.
    ///
    /// If not already available then this opens a new set of connections
    /// specific to this bucket in a new task.
    /// This function does not block, connection management is performed in the background tasks.
    ///
    /// # Arguments
    ///
    /// * `name` — The name of the bucket to open.
    pub fn bucket(&self, name: impl Into<String>) -> Bucket {
        let bucket_client = self.client.bucket_client(name.into());

        Bucket::new(bucket_client)
    }

    /// Executes a cluster-level SQL++ (N1QL) query.
    ///
    /// For scope-level queries, use [`Scope::query`](crate::scope::Scope::query) instead.
    ///
    /// # Arguments
    ///
    /// * `statement` — The SQL++ query string.
    /// * `opts` — Optional [`QueryOptions`] to
    ///   configure parameters, consistency, timeouts, etc. Pass `None` for defaults.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails (e.g. syntax error, timeout, authentication failure).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use couchbase::cluster::Cluster;
    /// # use couchbase::authenticator::PasswordAuthenticator;
    /// # use couchbase::options::cluster_options::ClusterOptions;
    /// use couchbase::options::query_options::QueryOptions;
    /// use futures::TryStreamExt;
    /// use serde_json::Value;
    ///
    /// # async fn example() -> couchbase::error::Result<()> {
    /// # let cluster = Cluster::connect("couchbase://localhost",
    /// #     ClusterOptions::new(PasswordAuthenticator::new("u", "p").into())).await?;
    /// let mut result = cluster.query(
    ///     "SELECT * FROM `travel-sample` LIMIT 10",
    ///     None,
    /// ).await?;
    ///
    /// let rows: Vec<Value> = result.rows().try_collect().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn query(
        &self,
        statement: impl Into<String>,
        opts: impl Into<Option<QueryOptions>>,
    ) -> error::Result<QueryResult> {
        let statement: String = statement.into();
        let span = create_span!("query").with_statement(&statement);
        let ctx = self
            .tracing_client
            .begin_operation(Some(SERVICE_VALUE_QUERY), Keyspace::Cluster, span)
            .await;
        let result = self
            .query_client
            .query(statement, opts.into())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    /// Returns a [`BucketManager`]
    /// for managing buckets on this cluster (create, update, drop, list, flush).
    pub fn buckets(&self) -> BucketManager {
        BucketManager::new(self.bucket_mgmt_client.clone())
    }

    /// Returns a [`UserManager`]
    /// for managing users and groups on this cluster.
    pub fn users(&self) -> UserManager {
        UserManager::new(self.user_mgmt_client.clone())
    }

    /// Returns a [`DiagnosticsResult`] containing
    /// the current state of all connections in the SDK.
    ///
    /// Unlike [`ping`](Cluster::ping), this does **not** send any traffic to the cluster;
    /// it reports the last-known state of each connection.
    pub async fn diagnostics(
        &self,
        opts: impl Into<Option<DiagnosticsOptions>>,
    ) -> error::Result<DiagnosticsResult> {
        let ctx = self
            .tracing_client
            .begin_operation(None, Keyspace::Cluster, create_span!("diagnostics"))
            .await;
        let result = self
            .diagnostics_client
            .diagnostics(opts.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    /// Actively pings cluster nodes and returns a [`PingReport`]
    /// with latency information for each endpoint.
    ///
    /// This sends actual traffic to the cluster to measure reachability and latency.
    /// Use [`diagnostics`](Cluster::diagnostics) if you only need the cached connection state.
    pub async fn ping(&self, opts: impl Into<Option<PingOptions>>) -> error::Result<PingReport> {
        let ctx = self
            .tracing_client
            .begin_operation(None, Keyspace::Cluster, create_span!("ping"))
            .await;
        let result = self
            .diagnostics_client
            .ping(opts.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    /// Waits until the cluster is in the desired state (default: online).
    ///
    /// This is useful during application startup to ensure the cluster is ready before
    /// serving requests.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use couchbase::cluster::Cluster;
    /// # use couchbase::authenticator::PasswordAuthenticator;
    /// # use couchbase::options::cluster_options::ClusterOptions;
    /// use couchbase::options::diagnostic_options::WaitUntilReadyOptions;
    ///
    /// # async fn example() -> couchbase::error::Result<()> {
    /// # let cluster = Cluster::connect("couchbase://localhost",
    /// #     ClusterOptions::new(PasswordAuthenticator::new("u", "p").into())).await?;
    /// cluster.wait_until_ready(WaitUntilReadyOptions::new()).await?;
    /// println!("Cluster is ready!");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn wait_until_ready(
        &self,
        opts: impl Into<Option<WaitUntilReadyOptions>>,
    ) -> error::Result<()> {
        let ctx = self
            .tracing_client
            .begin_operation(None, Keyspace::Cluster, create_span!("wait_until_ready"))
            .await;
        let result = self
            .diagnostics_client
            .wait_until_ready(opts.into().unwrap_or_default())
            .instrument(ctx.span().clone())
            .await;
        ctx.end_operation(result.as_ref().err());
        result
    }

    /// Sets a new authenticator for the cluster.
    ///
    /// The behavior depends on both service and authenticator type.
    ///
    /// For KV:
    ///    [`JwtAuthenticator`](crate::authenticator::JwtAuthenticator) will send an authentication
    ///    request to the server.
    ///    Other authenticator types do not take effect until connections are re-established.
    /// For HTTP:
    /// - Authenticators which apply authentication per-request (such as
    ///   [`PasswordAuthenticator`](crate::authenticator::PasswordAuthenticator)) take effect
    ///   immediately.
    /// - Transport-level authenticators (such as
    ///   [`CertificateAuthenticator`](crate::authenticator::CertificateAuthenticator)) do not
    ///   take effect until new connections are created.
    pub async fn set_authenticator(&self, authenticator: Authenticator) -> error::Result<()> {
        self.client.set_authenticator(authenticator).await
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        if Arc::strong_count(&self.client) == 1 {
            info!("Dropping Cluster");
        }
    }
}
