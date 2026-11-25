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
use crate::options::diagnostic_options::{DiagnosticsOptions, PingOptions, WaitUntilReadyOptions};
use crate::results::diagnostics::{DiagnosticsResult, PingReport};
use crate::retry::RetryStrategy;
use crate::timeouts::{with_optional_timeout, Timeouts};
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct DiagnosticsClient {
    backend: DiagnosticsClientBackend,
}

impl DiagnosticsClient {
    pub fn new(backend: DiagnosticsClientBackend) -> Self {
        Self { backend }
    }

    pub async fn diagnostics(&self, opts: DiagnosticsOptions) -> error::Result<DiagnosticsResult> {
        match &self.backend {
            DiagnosticsClientBackend::CouchbaseDiagnosticsClientBackend(backend) => {
                backend.diagnostics(opts).await
            }
            DiagnosticsClientBackend::Couchbase2DiagnosticsClientBackend(backend) => {
                backend.diagnostics(opts).await
            }
        }
    }

    pub async fn ping(&self, opts: PingOptions) -> error::Result<PingReport> {
        match &self.backend {
            DiagnosticsClientBackend::CouchbaseDiagnosticsClientBackend(backend) => {
                backend.ping(opts).await
            }
            DiagnosticsClientBackend::Couchbase2DiagnosticsClientBackend(backend) => {
                backend.ping(opts).await
            }
        }
    }

    pub async fn wait_until_ready(&self, opts: WaitUntilReadyOptions) -> error::Result<()> {
        match &self.backend {
            DiagnosticsClientBackend::CouchbaseDiagnosticsClientBackend(backend) => {
                backend.wait_until_ready(opts).await
            }
            DiagnosticsClientBackend::Couchbase2DiagnosticsClientBackend(backend) => {
                backend.wait_until_ready(opts).await
            }
        }
    }

    pub fn tracing_client(&self) -> TracingClient {
        match &self.backend {
            DiagnosticsClientBackend::CouchbaseDiagnosticsClientBackend(backend) => {
                TracingClient::new(TracingClientBackend::CouchbaseTracingClientBackend(
                    backend.tracing_client(),
                ))
            }
            DiagnosticsClientBackend::Couchbase2DiagnosticsClientBackend(backend) => {
                unimplemented!()
            }
        }
    }
}

#[derive(Clone)]
pub(crate) enum DiagnosticsClientBackend {
    CouchbaseDiagnosticsClientBackend(CouchbaseDiagnosticsClient),
    Couchbase2DiagnosticsClientBackend(Couchbase2DiagnosticsClient),
}

#[derive(Clone)]
pub(crate) struct CouchbaseDiagnosticsClient {
    agent_provider: CouchbaseAgentProvider,
    default_retry_strategy: Arc<dyn RetryStrategy>,
    default_timeouts: Timeouts,
}

impl CouchbaseDiagnosticsClient {
    pub fn new(
        agent_provider: CouchbaseAgentProvider,
        default_retry_strategy: Arc<dyn RetryStrategy>,
        default_timeouts: Timeouts,
    ) -> Self {
        Self {
            agent_provider,
            default_retry_strategy,
            default_timeouts,
        }
    }

    async fn diagnostics(&self, _opts: DiagnosticsOptions) -> error::Result<DiagnosticsResult> {
        let agent = self.agent_provider.get_agent().await;

        let core_opts = couchbase_core::options::diagnostics::DiagnosticsOptions::new();

        let report = CouchbaseAgentProvider::upgrade_agent(agent)?
            .diagnostics(&core_opts)
            .await?;

        Ok(DiagnosticsResult::from(report))
    }

    async fn ping(&self, opts: PingOptions) -> error::Result<PingReport> {
        let agent = self.agent_provider.get_agent().await;

        let mut core_opts = couchbase_core::options::ping::PingOptions::new();

        if let Some(service_types) = opts.service_types {
            core_opts =
                core_opts.service_types(service_types.into_iter().map(|s| s.into()).collect());
        }

        if let Some(timeout) = opts.kv_timeout {
            core_opts = core_opts.kv_timeout(timeout);
        }
        if let Some(timeout) = opts.query_timeout {
            core_opts = core_opts.query_timeout(timeout);
        }
        if let Some(timeout) = opts.search_timeout {
            core_opts = core_opts.search_timeout(timeout);
        }

        let report = CouchbaseAgentProvider::upgrade_agent(agent)?
            .ping(&core_opts)
            .await?;

        Ok(PingReport::from(report))
    }

    async fn wait_until_ready(&self, opts: WaitUntilReadyOptions) -> error::Result<()> {
        with_optional_timeout(opts.timeout, false, self.wait_until_ready_inner(opts)).await
    }

    async fn wait_until_ready_inner(&self, opts: WaitUntilReadyOptions) -> error::Result<()> {
        let agent = self.agent_provider.get_agent().await;

        let mut core_opts = couchbase_core::options::waituntilready::WaitUntilReadyOptions::new()
            .retry_strategy(
                opts.retry_strategy
                    .unwrap_or_else(|| self.default_retry_strategy.clone()),
            );

        if let Some(state) = opts.desired_state {
            core_opts = core_opts.desired_state(state.into());
        }

        if let Some(service_types) = opts.service_types {
            core_opts =
                core_opts.service_types(service_types.into_iter().map(|s| s.into()).collect());
        }

        Ok(CouchbaseAgentProvider::upgrade_agent(agent)?
            .wait_until_ready(&core_opts)
            .await?)
    }

    pub fn tracing_client(&self) -> CouchbaseTracingClient {
        CouchbaseTracingClient::new(self.agent_provider.clone())
    }
}

#[derive(Clone)]
pub(crate) struct Couchbase2DiagnosticsClient {}

impl Couchbase2DiagnosticsClient {
    pub fn new() -> Self {
        unimplemented!()
    }

    async fn diagnostics(&self, _opts: DiagnosticsOptions) -> error::Result<DiagnosticsResult> {
        unimplemented!()
    }

    async fn ping(&self, _opts: PingOptions) -> error::Result<PingReport> {
        unimplemented!()
    }

    async fn wait_until_ready(&self, _opts: WaitUntilReadyOptions) -> error::Result<()> {
        unimplemented!()
    }
}
