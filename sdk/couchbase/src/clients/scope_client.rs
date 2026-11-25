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
use crate::clients::collection_client::{
    CollectionClient, CollectionClientBackend, Couchbase2CollectionClient,
    CouchbaseCollectionClient,
};
use crate::clients::query_client::{
    CouchbaseQueryClient, QueryClient, QueryClientBackend, QueryKeyspace,
};
use crate::clients::search_client::{
    CouchbaseSearchClient, SearchClient, SearchClientBackend, SearchKeyspace,
};
use crate::clients::search_index_mgmt_client::{
    CouchbaseSearchIndexMgmtClient, SearchIndexKeyspace, SearchIndexMgmtClient,
    SearchIndexMgmtClientBackend,
};
use crate::clients::tracing_client::{CouchbaseTracingClient, TracingClient, TracingClientBackend};
use crate::retry::RetryStrategy;
use crate::timeouts::Timeouts;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct ScopeClient {
    backend: ScopeClientBackend,
}

impl ScopeClient {
    pub fn new(scope_client_backend: ScopeClientBackend) -> Self {
        Self {
            backend: scope_client_backend,
        }
    }

    pub fn name(&self) -> &str {
        match &self.backend {
            ScopeClientBackend::CouchbaseScopeBackend(client) => client.name(),
            ScopeClientBackend::Couchbase2ScopeBackend(client) => client.name(),
        }
    }

    pub fn bucket_name(&self) -> &str {
        match &self.backend {
            ScopeClientBackend::CouchbaseScopeBackend(client) => client.bucket_name(),
            ScopeClientBackend::Couchbase2ScopeBackend(client) => client.bucket_name(),
        }
    }

    pub fn query_client(&self) -> QueryClient {
        match &self.backend {
            ScopeClientBackend::CouchbaseScopeBackend(backend) => {
                let query_client = backend.query_client();

                QueryClient::new(QueryClientBackend::CouchbaseQueryClientBackend(
                    query_client,
                ))
            }
            ScopeClientBackend::Couchbase2ScopeBackend(_) => {
                unimplemented!()
            }
        }
    }

    pub fn tracing_client(&self) -> TracingClient {
        match &self.backend {
            ScopeClientBackend::CouchbaseScopeBackend(client) => TracingClient::new(
                TracingClientBackend::CouchbaseTracingClientBackend(client.tracing_client()),
            ),
            ScopeClientBackend::Couchbase2ScopeBackend(_) => {
                unimplemented!()
            }
        }
    }

    pub fn search_client(&self) -> SearchClient {
        match &self.backend {
            ScopeClientBackend::CouchbaseScopeBackend(backend) => {
                let search_client = backend.search_client();

                SearchClient::new(SearchClientBackend::CouchbaseSearchClientBackend(
                    search_client,
                ))
            }
            ScopeClientBackend::Couchbase2ScopeBackend(_) => {
                unimplemented!()
            }
        }
    }

    pub fn search_index_management_client(&self) -> SearchIndexMgmtClient {
        match &self.backend {
            ScopeClientBackend::CouchbaseScopeBackend(backend) => {
                let client = backend.search_index_management_client();

                SearchIndexMgmtClient::new(
                    SearchIndexMgmtClientBackend::CouchbaseSearchIndexMgmtClientBackend(client),
                )
            }
            ScopeClientBackend::Couchbase2ScopeBackend(_) => {
                unimplemented!()
            }
        }
    }

    pub fn collection_client(&self, name: String) -> CollectionClient {
        match &self.backend {
            ScopeClientBackend::CouchbaseScopeBackend(client) => client.collection(name),
            ScopeClientBackend::Couchbase2ScopeBackend(client) => client.collection(name),
        }
    }
}

#[derive(Clone)]
pub(crate) enum ScopeClientBackend {
    CouchbaseScopeBackend(CouchbaseScopeClient),
    Couchbase2ScopeBackend(Couchbase2ScopeClient),
}

#[derive(Clone)]
pub(crate) struct CouchbaseScopeClient {
    agent_provider: CouchbaseAgentProvider,
    bucket_name: String,
    name: String,
    default_retry_strategy: Arc<dyn RetryStrategy>,
    default_timeouts: Timeouts,
}

impl CouchbaseScopeClient {
    pub fn new(
        agent_provider: CouchbaseAgentProvider,
        bucket_name: String,
        name: String,
        default_retry_strategy: Arc<dyn RetryStrategy>,
        default_timeouts: Timeouts,
    ) -> Self {
        Self {
            agent_provider,
            bucket_name,
            name,
            default_retry_strategy,
            default_timeouts,
        }
    }

    pub fn bucket_name(&self) -> &str {
        &self.bucket_name
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn collection(&self, name: String) -> CollectionClient {
        CollectionClient::new(CollectionClientBackend::CouchbaseCollectionBackend(
            CouchbaseCollectionClient::new(
                self.agent_provider.clone(),
                self.bucket_name().to_string(),
                self.name().to_string(),
                name,
                self.default_retry_strategy.clone(),
                self.default_timeouts,
            ),
        ))
    }

    pub fn query_client(&self) -> CouchbaseQueryClient {
        CouchbaseQueryClient::with_keyspace(
            self.agent_provider.clone(),
            QueryKeyspace {
                bucket_name: self.bucket_name().to_string(),
                scope_name: self.name().to_string(),
            },
            self.default_retry_strategy.clone(),
            self.default_timeouts,
        )
    }

    pub fn search_client(&self) -> CouchbaseSearchClient {
        CouchbaseSearchClient::with_keyspace(
            self.agent_provider.clone(),
            SearchKeyspace {
                bucket_name: self.bucket_name().to_string(),
                scope_name: self.name().to_string(),
            },
            self.default_retry_strategy.clone(),
            self.default_timeouts,
        )
    }

    pub fn search_index_management_client(&self) -> CouchbaseSearchIndexMgmtClient {
        CouchbaseSearchIndexMgmtClient::new(
            self.agent_provider.clone(),
            SearchIndexKeyspace {
                bucket_name: self.bucket_name().to_string(),
                scope_name: self.name().to_string(),
            },
            self.default_retry_strategy.clone(),
            self.default_timeouts,
        )
    }

    pub fn tracing_client(&self) -> CouchbaseTracingClient {
        CouchbaseTracingClient::new(self.agent_provider.clone())
    }
}

#[derive(Clone)]
pub(crate) struct Couchbase2ScopeClient {
    bucket_name: String,
    name: String,
}

impl Couchbase2ScopeClient {
    pub fn new() -> Self {
        unimplemented!()
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn bucket_name(&self) -> &str {
        &self.bucket_name
    }

    pub fn collection(&self, _name: String) -> CollectionClient {
        CollectionClient::new(CollectionClientBackend::Couchbase2CollectionBackend(
            Couchbase2CollectionClient::new(),
        ))
    }
}
