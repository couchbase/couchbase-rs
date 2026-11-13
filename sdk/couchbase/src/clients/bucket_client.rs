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
use crate::clients::collections_mgmt_client::{
    CollectionsMgmtClient, CollectionsMgmtClientBackend, CouchbaseCollectionsMgmtClient,
};
use crate::clients::diagnostics_client::{
    CouchbaseDiagnosticsClient, DiagnosticsClient, DiagnosticsClientBackend,
};
use crate::clients::scope_client::{
    Couchbase2ScopeClient, CouchbaseScopeClient, ScopeClient, ScopeClientBackend,
};
use crate::clients::tracing_client::{CouchbaseTracingClient, TracingClient, TracingClientBackend};
use crate::retry::RetryStrategy;
use crate::timeouts::Timeouts;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct BucketClient {
    backend: BucketClientBackend,
}

impl BucketClient {
    pub fn new(bucket_client_backend: BucketClientBackend) -> Self {
        Self {
            backend: bucket_client_backend,
        }
    }

    pub fn name(&self) -> &str {
        match &self.backend {
            BucketClientBackend::CouchbaseBucketBackend(client) => client.name(),
            BucketClientBackend::Couchbase2BucketBackend(client) => client.name(),
        }
    }

    pub fn scope_client(&self, name: String) -> ScopeClient {
        match &self.backend {
            BucketClientBackend::CouchbaseBucketBackend(client) => client.scope(name),
            BucketClientBackend::Couchbase2BucketBackend(client) => client.scope(name),
        }
    }

    pub fn collections_management_client(&self) -> CollectionsMgmtClient {
        match &self.backend {
            BucketClientBackend::CouchbaseBucketBackend(backend) => {
                let client = backend.collections_management_client();

                CollectionsMgmtClient::new(
                    CollectionsMgmtClientBackend::CouchbaseCollectionsMgmtClientBackend(client),
                )
            }
            BucketClientBackend::Couchbase2BucketBackend(_) => {
                unimplemented!()
            }
        }
    }

    pub fn diagnostics_client(&self) -> DiagnosticsClient {
        match &self.backend {
            BucketClientBackend::CouchbaseBucketBackend(backend) => {
                let diagnostics_client = backend.diagnostics_client();

                DiagnosticsClient::new(DiagnosticsClientBackend::CouchbaseDiagnosticsClientBackend(
                    diagnostics_client,
                ))
            }
            BucketClientBackend::Couchbase2BucketBackend(_) => {
                unimplemented!()
            }
        }
    }

    pub fn tracing_client(&self) -> TracingClient {
        match &self.backend {
            BucketClientBackend::CouchbaseBucketBackend(backend) => TracingClient::new(
                TracingClientBackend::CouchbaseTracingClientBackend(backend.tracing_client()),
            ),
            BucketClientBackend::Couchbase2BucketBackend(_) => {
                unimplemented!()
            }
        }
    }
}

#[derive(Clone)]
pub(crate) enum BucketClientBackend {
    CouchbaseBucketBackend(CouchbaseBucketClient),
    Couchbase2BucketBackend(Couchbase2BucketClient),
}

#[derive(Clone)]
pub(crate) struct CouchbaseBucketClient {
    agent_provider: CouchbaseAgentProvider,
    name: String,
    default_retry_strategy: Arc<dyn RetryStrategy>,
    default_timeouts: Timeouts,
}

impl CouchbaseBucketClient {
    pub fn new(
        agent_provider: CouchbaseAgentProvider,
        name: String,
        default_retry_strategy: Arc<dyn RetryStrategy>,
        default_timeouts: Timeouts,
    ) -> Self {
        Self {
            agent_provider,
            name,
            default_retry_strategy,
            default_timeouts,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn scope(&self, name: String) -> ScopeClient {
        ScopeClient::new(ScopeClientBackend::CouchbaseScopeBackend(
            CouchbaseScopeClient::new(
                self.agent_provider.clone(),
                self.name().to_string(),
                name,
                self.default_retry_strategy.clone(),
                self.default_timeouts,
            ),
        ))
    }

    pub fn collections_management_client(&self) -> CouchbaseCollectionsMgmtClient {
        CouchbaseCollectionsMgmtClient::new(
            self.agent_provider.clone(),
            self.name.clone(),
            self.default_retry_strategy.clone(),
            self.default_timeouts,
        )
    }

    pub fn diagnostics_client(&self) -> CouchbaseDiagnosticsClient {
        CouchbaseDiagnosticsClient::new(
            self.agent_provider.clone(),
            self.default_retry_strategy.clone(),
            self.default_timeouts,
        )
    }

    pub fn tracing_client(&self) -> CouchbaseTracingClient {
        CouchbaseTracingClient::new(self.agent_provider.clone())
    }
}

#[derive(Clone)]
pub(crate) struct Couchbase2BucketClient {
    name: String,
}

impl Couchbase2BucketClient {
    pub fn new(name: String) -> Self {
        Self { name }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn scope(&self, _name: String) -> ScopeClient {
        ScopeClient::new(ScopeClientBackend::Couchbase2ScopeBackend(
            Couchbase2ScopeClient::new(),
        ))
    }

    pub fn collections_management_client(&self) -> CouchbaseCollectionsMgmtClient {
        unimplemented!()
    }
}
