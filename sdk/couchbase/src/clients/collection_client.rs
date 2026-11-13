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
use crate::clients::core_kv_client::{CoreKvClient, CoreKvClientBackend, Couchbase2CoreKvClient};
use crate::clients::couchbase_core_kv_client::CouchbaseCoreKvClient;
use crate::clients::query_index_mgmt_client::{
    CouchbaseQueryIndexMgmtClient, QueryIndexKeyspace, QueryIndexMgmtClient,
    QueryIndexMgmtClientBackend,
};
use crate::clients::tracing_client::{CouchbaseTracingClient, TracingClient, TracingClientBackend};
use crate::retry::RetryStrategy;
use crate::timeouts::Timeouts;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct CollectionClient {
    backend: CollectionClientBackend,
}

impl CollectionClient {
    pub fn new(backend: CollectionClientBackend) -> Self {
        Self { backend }
    }

    pub fn name(&self) -> &str {
        match &self.backend {
            CollectionClientBackend::CouchbaseCollectionBackend(client) => client.name.as_str(),
            CollectionClientBackend::Couchbase2CollectionBackend(client) => client.name.as_str(),
        }
    }

    pub fn bucket_name(&self) -> &str {
        match &self.backend {
            CollectionClientBackend::CouchbaseCollectionBackend(client) => {
                client.bucket_name.as_str()
            }
            CollectionClientBackend::Couchbase2CollectionBackend(client) => {
                client.bucket_name.as_str()
            }
        }
    }

    pub fn scope_name(&self) -> &str {
        match &self.backend {
            CollectionClientBackend::CouchbaseCollectionBackend(client) => {
                client.scope_name.as_str()
            }
            CollectionClientBackend::Couchbase2CollectionBackend(client) => {
                client.scope_name.as_str()
            }
        }
    }

    pub fn core_kv_client(&self) -> CoreKvClient {
        match &self.backend {
            CollectionClientBackend::CouchbaseCollectionBackend(client) => client.core_kv_client(),
            CollectionClientBackend::Couchbase2CollectionBackend(client) => client.core_kv_client(),
        }
    }

    pub fn tracing_client(&self) -> TracingClient {
        match &self.backend {
            CollectionClientBackend::CouchbaseCollectionBackend(client) => TracingClient::new(
                TracingClientBackend::CouchbaseTracingClientBackend(client.tracing_client()),
            ),
            CollectionClientBackend::Couchbase2CollectionBackend(_) => {
                unimplemented!()
            }
        }
    }

    pub fn query_index_management_client(&self) -> QueryIndexMgmtClient {
        match &self.backend {
            CollectionClientBackend::CouchbaseCollectionBackend(client) => {
                let query_index_mgmt_client = client.query_index_management_client();

                QueryIndexMgmtClient::new(
                    QueryIndexMgmtClientBackend::CouchbaseQueryIndexMgmtClientBackend(
                        query_index_mgmt_client,
                    ),
                )
            }
            CollectionClientBackend::Couchbase2CollectionBackend(_) => {
                unimplemented!()
            }
        }
    }
}

#[derive(Clone)]
pub(crate) enum CollectionClientBackend {
    CouchbaseCollectionBackend(CouchbaseCollectionClient),
    Couchbase2CollectionBackend(Couchbase2CollectionClient),
}

#[derive(Clone)]
pub(crate) struct CouchbaseCollectionClient {
    agent_provider: CouchbaseAgentProvider,
    bucket_name: String,
    scope_name: String,
    name: String,
    default_retry_strategy: Arc<dyn RetryStrategy>,
    default_timeouts: Timeouts,
}

impl CouchbaseCollectionClient {
    pub fn new(
        agent_provider: CouchbaseAgentProvider,
        bucket_name: String,
        scope_name: String,
        name: String,
        default_retry_strategy: Arc<dyn RetryStrategy>,
        default_timeouts: Timeouts,
    ) -> Self {
        Self {
            agent_provider,
            bucket_name,
            scope_name,
            name,
            default_retry_strategy,
            default_timeouts,
        }
    }

    pub fn bucket_name(&self) -> &str {
        &self.bucket_name
    }

    pub fn scope_name(&self) -> &str {
        &self.scope_name
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn core_kv_client(&self) -> CoreKvClient {
        CoreKvClient::new(CoreKvClientBackend::CouchbaseCoreKvClientBackend(
            CouchbaseCoreKvClient::new(
                self.agent_provider.clone(),
                self.bucket_name().to_string(),
                self.scope_name().to_string(),
                self.name().to_string(),
                self.default_retry_strategy.clone(),
                self.default_timeouts,
            ),
        ))
    }

    pub fn query_index_management_client(&self) -> CouchbaseQueryIndexMgmtClient {
        CouchbaseQueryIndexMgmtClient::new(
            self.agent_provider.clone(),
            QueryIndexKeyspace {
                bucket_name: self.bucket_name().to_string(),
                scope_name: self.scope_name().to_string(),
                collection_name: self.name().to_string(),
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
pub(crate) struct Couchbase2CollectionClient {
    bucket_name: String,
    scope_name: String,
    name: String,
}

impl Couchbase2CollectionClient {
    pub fn new() -> Self {
        unimplemented!()
    }

    pub fn core_kv_client(&self) -> CoreKvClient {
        CoreKvClient::new(CoreKvClientBackend::Couchbase2CoreKvClientBackend(
            Couchbase2CoreKvClient::new(),
        ))
    }
}
