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
use crate::clients::agent_provider::CouchbaseAgentProvider;
use crate::clients::tracing_client::{CouchbaseTracingClient, TracingClient, TracingClientBackend};
use crate::error;
use crate::options::query_options::{QueryOptions, ScanConsistency};
use crate::results::query_results::QueryResult;
use crate::retry::RetryStrategy;
use crate::timeouts::{OperationDeadline, Timeouts};
use couchbase_core::options::query;
use couchbase_core::queryx;
use std::sync::Arc;
use uuid::Uuid;

pub(crate) struct QueryClient {
    backend: QueryClientBackend,
}

impl QueryClient {
    pub fn new(backend: QueryClientBackend) -> Self {
        Self { backend }
    }

    pub async fn query(
        &self,
        statement: String,
        opts: Option<QueryOptions>,
    ) -> error::Result<QueryResult> {
        match &self.backend {
            QueryClientBackend::CouchbaseQueryClientBackend(backend) => {
                backend.query(statement, opts).await
            }
            QueryClientBackend::Couchbase2QueryClientBackend(backend) => {
                backend.query(statement, opts).await
            }
        }
    }

    pub fn tracing_client(&self) -> TracingClient {
        match &self.backend {
            QueryClientBackend::CouchbaseQueryClientBackend(backend) => TracingClient::new(
                TracingClientBackend::CouchbaseTracingClientBackend(backend.tracing_client()),
            ),
            QueryClientBackend::Couchbase2QueryClientBackend(backend) => {
                unimplemented!()
            }
        }
    }
}

pub(crate) enum QueryClientBackend {
    CouchbaseQueryClientBackend(CouchbaseQueryClient),
    Couchbase2QueryClientBackend(Couchbase2QueryClient),
}

pub(crate) struct QueryKeyspace {
    pub bucket_name: String,
    pub scope_name: String,
}

pub(crate) struct CouchbaseQueryClient {
    agent_provider: CouchbaseAgentProvider,
    keyspace: Option<QueryKeyspace>,
    default_retry_strategy: Arc<dyn RetryStrategy>,
    default_timeouts: Timeouts,
}

impl CouchbaseQueryClient {
    pub fn new(
        agent_provider: CouchbaseAgentProvider,
        default_retry_strategy: Arc<dyn RetryStrategy>,
        default_timeouts: Timeouts,
    ) -> Self {
        Self {
            agent_provider,
            keyspace: None,
            default_retry_strategy,
            default_timeouts,
        }
    }

    pub fn with_keyspace(
        agent_provider: CouchbaseAgentProvider,
        keyspace: QueryKeyspace,
        default_retry_strategy: Arc<dyn RetryStrategy>,
        default_timeouts: Timeouts,
    ) -> Self {
        Self {
            agent_provider,
            keyspace: Some(keyspace),
            default_retry_strategy,
            default_timeouts,
        }
    }

    async fn query(
        &self,
        statement: String,
        opts: Option<QueryOptions>,
    ) -> error::Result<QueryResult> {
        let mut opts = opts.unwrap_or_default();
        if opts.client_context_id.is_none() {
            opts.client_context_id = Some(Uuid::new_v4().to_string());
        }

        let ad_hoc = opts.ad_hoc.unwrap_or(true);
        let timeout = opts.timeout.unwrap_or(self.default_timeouts.query);
        let deadline = OperationDeadline::new(timeout, !opts.read_only.unwrap_or(false));

        let (mutation_state, scan_consistency) = match opts.scan_consistency {
            Some(ScanConsistency::AtPlus(state)) => (
                Some(state.into()),
                Some(queryx::query_options::ScanConsistency::AtPlus),
            ),
            Some(ScanConsistency::NotBounded) => (
                None,
                Some(queryx::query_options::ScanConsistency::NotBounded),
            ),
            Some(ScanConsistency::RequestPlus) => (
                None,
                Some(queryx::query_options::ScanConsistency::RequestPlus),
            ),
            None => (None, None),
        };

        let mut query_opts = query::QueryOptions::new()
            .args(opts.positional_parameters)
            .client_context_id(opts.client_context_id)
            .max_parallelism(opts.max_parallelism)
            .metrics(opts.metrics.unwrap_or_default())
            .pipeline_batch(opts.pipeline_batch)
            .pipeline_cap(opts.pipeline_cap)
            .preserve_expiry(opts.preserve_expiry)
            .profile(opts.profile.map(|p| p.into()))
            .read_only(opts.read_only)
            .scan_cap(opts.scan_cap)
            .scan_consistency(scan_consistency)
            .scan_wait(opts.scan_wait)
            .sparse_scan_vectors(mutation_state)
            .timeout(opts.server_timeout.or(Some(timeout)))
            .use_replica(opts.use_replica.map(|r| r.into()))
            .named_args(opts.named_parameters)
            .raw(opts.raw)
            .statement(statement)
            .retry_strategy(
                opts.retry_strategy
                    .unwrap_or_else(|| self.default_retry_strategy.clone()),
            );

        if let Some(keyspace) = &self.keyspace {
            query_opts = query_opts.query_context(format!(
                "`{}`.`{}`",
                keyspace.bucket_name.clone(),
                keyspace.scope_name.clone()
            ));
        }

        let stream = deadline
            .run(async {
                let agent = self.agent_provider.get_agent().await;
                let agent = CouchbaseAgentProvider::upgrade_agent(agent)?;
                let stream = if ad_hoc {
                    agent.query(query_opts).await?
                } else {
                    agent.prepared_query(query_opts).await?
                };
                Ok(stream)
            })
            .await?;

        Ok(QueryResult::with_deadline(stream, deadline))
    }

    pub fn tracing_client(&self) -> CouchbaseTracingClient {
        CouchbaseTracingClient::new(self.agent_provider.clone())
    }
}

pub(crate) struct Couchbase2QueryClient {}

impl Couchbase2QueryClient {
    pub fn new() -> Self {
        unimplemented!()
    }

    async fn query(
        &self,
        _statement: String,
        _opts: Option<QueryOptions>,
    ) -> error::Result<QueryResult> {
        unimplemented!()
    }
}
