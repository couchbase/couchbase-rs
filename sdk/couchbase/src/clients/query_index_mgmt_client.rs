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
use crate::options::query_index_mgmt_options::{
    BuildQueryIndexOptions, CreatePrimaryQueryIndexOptions, CreateQueryIndexOptions,
    DropPrimaryQueryIndexOptions, DropQueryIndexOptions, GetAllQueryIndexesOptions,
    WatchQueryIndexOptions,
};
use crate::results::query_index_mgmt_results::QueryIndex;
use crate::retry::RetryStrategy;
use crate::timeouts::{with_optional_timeout, with_timeout, Timeouts};
use crate::tracing::Keyspace;
use std::sync::Arc;

pub(crate) struct QueryIndexMgmtClient {
    backend: QueryIndexMgmtClientBackend,
}

impl QueryIndexMgmtClient {
    pub fn new(backend: QueryIndexMgmtClientBackend) -> Self {
        Self { backend }
    }

    pub async fn get_all_indexes(
        &self,
        opts: Option<GetAllQueryIndexesOptions>,
    ) -> error::Result<Vec<QueryIndex>> {
        match &self.backend {
            QueryIndexMgmtClientBackend::CouchbaseQueryIndexMgmtClientBackend(backend) => {
                backend.get_all_indexes(opts).await
            }
            QueryIndexMgmtClientBackend::Couchbase2QueryIndexMgmtClientBackend(backend) => {
                backend.get_all_indexes(opts).await
            }
        }
    }

    pub async fn create_index(
        &self,
        index_name: String,
        fields: Vec<String>,
        opts: Option<CreateQueryIndexOptions>,
    ) -> error::Result<()> {
        match &self.backend {
            QueryIndexMgmtClientBackend::CouchbaseQueryIndexMgmtClientBackend(backend) => {
                backend.create_index(index_name, fields, opts).await
            }
            QueryIndexMgmtClientBackend::Couchbase2QueryIndexMgmtClientBackend(backend) => {
                backend.create_index(index_name, fields, opts).await
            }
        }
    }

    pub async fn create_primary_index(
        &self,
        opts: Option<CreatePrimaryQueryIndexOptions>,
    ) -> error::Result<()> {
        match &self.backend {
            QueryIndexMgmtClientBackend::CouchbaseQueryIndexMgmtClientBackend(backend) => {
                backend.create_primary_index(opts).await
            }
            QueryIndexMgmtClientBackend::Couchbase2QueryIndexMgmtClientBackend(backend) => {
                backend.create_primary_index(opts).await
            }
        }
    }

    pub async fn drop_index(
        &self,
        index_name: String,
        opts: Option<DropQueryIndexOptions>,
    ) -> error::Result<()> {
        match &self.backend {
            QueryIndexMgmtClientBackend::CouchbaseQueryIndexMgmtClientBackend(backend) => {
                backend.drop_index(index_name, opts).await
            }
            QueryIndexMgmtClientBackend::Couchbase2QueryIndexMgmtClientBackend(backend) => {
                backend.drop_index(index_name, opts).await
            }
        }
    }

    pub async fn drop_primary_index(
        &self,
        opts: Option<DropPrimaryQueryIndexOptions>,
    ) -> error::Result<()> {
        match &self.backend {
            QueryIndexMgmtClientBackend::CouchbaseQueryIndexMgmtClientBackend(backend) => {
                backend.drop_primary_index(opts).await
            }
            QueryIndexMgmtClientBackend::Couchbase2QueryIndexMgmtClientBackend(backend) => {
                backend.drop_primary_index(opts).await
            }
        }
    }

    pub async fn watch_indexes(
        &self,
        index_names: Vec<String>,
        opts: Option<WatchQueryIndexOptions>,
    ) -> error::Result<()> {
        match &self.backend {
            QueryIndexMgmtClientBackend::CouchbaseQueryIndexMgmtClientBackend(backend) => {
                backend.watch_indexes(index_names, opts).await
            }
            QueryIndexMgmtClientBackend::Couchbase2QueryIndexMgmtClientBackend(backend) => {
                backend.watch_indexes(index_names, opts).await
            }
        }
    }

    pub async fn build_deferred_indexes(
        &self,
        opts: Option<BuildQueryIndexOptions>,
    ) -> error::Result<()> {
        match &self.backend {
            QueryIndexMgmtClientBackend::CouchbaseQueryIndexMgmtClientBackend(backend) => {
                backend.build_deferred_indexes(opts).await
            }
            QueryIndexMgmtClientBackend::Couchbase2QueryIndexMgmtClientBackend(backend) => {
                backend.build_deferred_indexes(opts).await
            }
        }
    }

    pub fn tracing_client(&self) -> TracingClient {
        match &self.backend {
            QueryIndexMgmtClientBackend::CouchbaseQueryIndexMgmtClientBackend(backend) => {
                let tracing_client = backend.tracing_client();

                TracingClient::new(TracingClientBackend::CouchbaseTracingClientBackend(
                    tracing_client,
                ))
            }
            QueryIndexMgmtClientBackend::Couchbase2QueryIndexMgmtClientBackend(_) => {
                unimplemented!()
            }
        }
    }

    pub fn bucket_name(&self) -> &str {
        match &self.backend {
            QueryIndexMgmtClientBackend::CouchbaseQueryIndexMgmtClientBackend(backend) => {
                backend.bucket_name()
            }
            QueryIndexMgmtClientBackend::Couchbase2QueryIndexMgmtClientBackend(_) => {
                unimplemented!()
            }
        }
    }

    pub fn scope_name(&self) -> &str {
        match &self.backend {
            QueryIndexMgmtClientBackend::CouchbaseQueryIndexMgmtClientBackend(backend) => {
                backend.scope_name()
            }
            QueryIndexMgmtClientBackend::Couchbase2QueryIndexMgmtClientBackend(_) => {
                unimplemented!()
            }
        }
    }

    pub fn collection_name(&self) -> &str {
        match &self.backend {
            QueryIndexMgmtClientBackend::CouchbaseQueryIndexMgmtClientBackend(backend) => {
                backend.collection_name()
            }
            QueryIndexMgmtClientBackend::Couchbase2QueryIndexMgmtClientBackend(_) => {
                unimplemented!()
            }
        }
    }

    pub fn keyspace(&self) -> Keyspace<'_> {
        match &self.backend {
            QueryIndexMgmtClientBackend::CouchbaseQueryIndexMgmtClientBackend(backend) => {
                (&backend.keyspace).into()
            }
            QueryIndexMgmtClientBackend::Couchbase2QueryIndexMgmtClientBackend(_) => {
                unimplemented!()
            }
        }
    }
}

pub(crate) enum QueryIndexMgmtClientBackend {
    CouchbaseQueryIndexMgmtClientBackend(CouchbaseQueryIndexMgmtClient),
    Couchbase2QueryIndexMgmtClientBackend(Couchbase2QueryIndexMgmtClient),
}

#[derive(Clone, Debug)]
pub(crate) struct QueryIndexKeyspace {
    pub bucket_name: String,
    pub scope_name: String,
    pub collection_name: String,
}

impl QueryIndexKeyspace {
    pub(crate) fn bucket_name(&self) -> &str {
        self.bucket_name.as_str()
    }

    pub(crate) fn scope_name(&self) -> &str {
        self.scope_name.as_str()
    }

    pub(crate) fn collection_name(&self) -> &str {
        self.collection_name.as_str()
    }
}

impl<'a> From<&'a QueryIndexKeyspace> for Keyspace<'a> {
    fn from(keyspace: &'a QueryIndexKeyspace) -> Self {
        Keyspace::Collection {
            bucket: &keyspace.bucket_name,
            scope: &keyspace.scope_name,
            collection: &keyspace.collection_name,
        }
    }
}

pub(crate) struct CouchbaseQueryIndexMgmtClient {
    agent_provider: CouchbaseAgentProvider,
    keyspace: QueryIndexKeyspace,
    default_retry_strategy: Arc<dyn RetryStrategy>,
    default_timeouts: Timeouts,
}

impl CouchbaseQueryIndexMgmtClient {
    pub fn new(
        agent_provider: CouchbaseAgentProvider,
        keyspace: QueryIndexKeyspace,
        default_retry_strategy: Arc<dyn RetryStrategy>,
        default_timeouts: Timeouts,
    ) -> Self {
        Self {
            agent_provider,
            keyspace,
            default_retry_strategy,
            default_timeouts,
        }
    }

    async fn get_all_indexes(
        &self,
        opts: Option<GetAllQueryIndexesOptions>,
    ) -> error::Result<Vec<QueryIndex>> {
        let timeout = opts
            .as_ref()
            .and_then(|o| o.timeout)
            .unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, false, async move {
            let opts = opts.unwrap_or_default();
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());

            let mut get_indexes_opts = couchbase_core::options::query::GetAllIndexesOptions::new(
                &self.keyspace.bucket_name,
            )
            .scope_name(&self.keyspace.scope_name)
            .collection_name(&self.keyspace.collection_name);
            get_indexes_opts = get_indexes_opts.retry_strategy(retry);

            let agent = self.agent_provider.get_agent().await;

            let indexes = CouchbaseAgentProvider::upgrade_agent(agent)?
                .get_all_indexes(&get_indexes_opts)
                .await?;

            Ok(indexes.into_iter().map(QueryIndex::from).collect())
        })
        .await
    }

    async fn create_index(
        &self,
        index_name: impl Into<String>,
        fields: Vec<String>,
        opts: Option<CreateQueryIndexOptions>,
    ) -> error::Result<()> {
        let timeout = opts
            .as_ref()
            .and_then(|o| o.timeout)
            .unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, true, async move {
            let opts = opts.unwrap_or_default();
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());

            let index_name = index_name.into();
            let fields: Vec<&str> = fields.iter().map(String::as_str).collect();

            let mut create_index_opts = couchbase_core::options::query::CreateIndexOptions::new(
                &self.keyspace.bucket_name,
                &index_name,
                &fields,
            )
            .scope_name(&self.keyspace.scope_name)
            .collection_name(&self.keyspace.collection_name);

            if let Some(deferred) = opts.deferred {
                create_index_opts = create_index_opts.deferred(deferred);
            }
            if let Some(ignore_if_exists) = opts.ignore_if_exists {
                create_index_opts = create_index_opts.ignore_if_exists(ignore_if_exists);
            }
            if let Some(num_replicas) = opts.num_replicas {
                create_index_opts = create_index_opts.num_replicas(num_replicas);
            }
            create_index_opts = create_index_opts.retry_strategy(retry);

            let agent = self.agent_provider.get_agent().await;

            CouchbaseAgentProvider::upgrade_agent(agent)?
                .create_index(&create_index_opts)
                .await?;
            Ok(())
        })
        .await
    }

    async fn create_primary_index(
        &self,
        opts: Option<CreatePrimaryQueryIndexOptions>,
    ) -> error::Result<()> {
        let timeout = opts
            .as_ref()
            .and_then(|o| o.timeout)
            .unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, true, async move {
            let opts = opts.unwrap_or_default();
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());

            let mut create_index_opts =
                couchbase_core::options::query::CreatePrimaryIndexOptions::new(
                    &self.keyspace.bucket_name,
                )
                .scope_name(&self.keyspace.scope_name)
                .collection_name(&self.keyspace.collection_name);

            if let Some(deferred) = opts.deferred {
                create_index_opts = create_index_opts.deferred(deferred);
            }
            if let Some(ignore_if_exists) = opts.ignore_if_exists {
                create_index_opts = create_index_opts.ignore_if_exists(ignore_if_exists);
            }
            if let Some(num_replicas) = opts.num_replicas {
                create_index_opts = create_index_opts.num_replicas(num_replicas);
            }
            if let Some(index_name) = opts.index_name.as_deref() {
                create_index_opts = create_index_opts.index_name(index_name);
            }
            create_index_opts = create_index_opts.retry_strategy(retry);

            let agent = self.agent_provider.get_agent().await;

            CouchbaseAgentProvider::upgrade_agent(agent)?
                .create_primary_index(&create_index_opts)
                .await?;

            Ok(())
        })
        .await
    }

    async fn drop_index(
        &self,
        index_name: String,
        opts: Option<DropQueryIndexOptions>,
    ) -> error::Result<()> {
        let timeout = opts
            .as_ref()
            .and_then(|o| o.timeout)
            .unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, true, async move {
            let opts = opts.unwrap_or_default();
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());

            let mut drop_index_opts = couchbase_core::options::query::DropIndexOptions::new(
                &self.keyspace.bucket_name,
                index_name.as_str(),
            )
            .scope_name(&self.keyspace.scope_name)
            .collection_name(&self.keyspace.collection_name);

            if let Some(ignore) = opts.ignore_if_not_exists {
                drop_index_opts = drop_index_opts.ignore_if_not_exists(ignore);
            }
            drop_index_opts = drop_index_opts.retry_strategy(retry);

            let agent = self.agent_provider.get_agent().await;

            CouchbaseAgentProvider::upgrade_agent(agent)?
                .drop_index(&drop_index_opts)
                .await?;

            Ok(())
        })
        .await
    }

    async fn drop_primary_index(
        &self,
        opts: Option<DropPrimaryQueryIndexOptions>,
    ) -> error::Result<()> {
        let timeout = opts
            .as_ref()
            .and_then(|o| o.timeout)
            .unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, true, async move {
            let opts = opts.unwrap_or_default();
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());

            let mut drop_index_opts = couchbase_core::options::query::DropPrimaryIndexOptions::new(
                &self.keyspace.bucket_name,
            )
            .scope_name(&self.keyspace.scope_name)
            .collection_name(&self.keyspace.collection_name);

            if let Some(ignore) = opts.ignore_if_not_exists {
                drop_index_opts = drop_index_opts.ignore_if_not_exists(ignore);
            }
            if let Some(index_name) = opts.index_name.as_deref() {
                drop_index_opts = drop_index_opts.index_name(index_name);
            }
            drop_index_opts = drop_index_opts.retry_strategy(retry);

            let agent = self.agent_provider.get_agent().await;

            CouchbaseAgentProvider::upgrade_agent(agent)?
                .drop_primary_index(&drop_index_opts)
                .await?;

            Ok(())
        })
        .await
    }

    async fn watch_indexes(
        &self,
        index_names: Vec<String>,
        opts: Option<WatchQueryIndexOptions>,
    ) -> error::Result<()> {
        let opts = opts.unwrap_or_default();
        with_optional_timeout(
            opts.timeout,
            false,
            self.watch_indexes_inner(index_names, opts),
        )
        .await
    }

    async fn watch_indexes_inner(
        &self,
        index_names: Vec<String>,
        opts: WatchQueryIndexOptions,
    ) -> error::Result<()> {
        let retry = opts
            .retry_strategy
            .unwrap_or_else(|| self.default_retry_strategy.clone());

        let index_names_refs: Vec<&str> = index_names.iter().map(String::as_str).collect();

        let mut watch_indexes_opts = couchbase_core::options::query::WatchIndexesOptions::new(
            &self.keyspace.bucket_name,
            &index_names_refs,
        )
        .scope_name(&self.keyspace.scope_name)
        .collection_name(&self.keyspace.collection_name);

        if let Some(watch_primary) = opts.watch_primary {
            watch_indexes_opts = watch_indexes_opts.watch_primary(watch_primary);
        }
        watch_indexes_opts = watch_indexes_opts.retry_strategy(retry);

        let agent = self.agent_provider.get_agent().await;

        CouchbaseAgentProvider::upgrade_agent(agent)?
            .watch_indexes(&watch_indexes_opts)
            .await?;

        Ok(())
    }

    async fn build_deferred_indexes(
        &self,
        opts: Option<BuildQueryIndexOptions>,
    ) -> error::Result<()> {
        let timeout = opts
            .as_ref()
            .and_then(|o| o.timeout)
            .unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, true, async move {
            let opts = opts.unwrap_or_default();
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());

            let mut build_indexes_opts =
                couchbase_core::options::query::BuildDeferredIndexesOptions::new(
                    &self.keyspace.bucket_name,
                )
                .scope_name(&self.keyspace.scope_name)
                .collection_name(&self.keyspace.collection_name);

            build_indexes_opts = build_indexes_opts.retry_strategy(retry);

            let agent = self.agent_provider.get_agent().await;

            CouchbaseAgentProvider::upgrade_agent(agent)?
                .build_deferred_indexes(&build_indexes_opts)
                .await?;

            Ok(())
        })
        .await
    }

    fn tracing_client(&self) -> CouchbaseTracingClient {
        CouchbaseTracingClient::new(self.agent_provider.clone())
    }

    fn bucket_name(&self) -> &str {
        self.keyspace.bucket_name()
    }

    fn scope_name(&self) -> &str {
        self.keyspace.scope_name()
    }

    fn collection_name(&self) -> &str {
        self.keyspace.collection_name()
    }
}

pub(crate) struct Couchbase2QueryIndexMgmtClient {}

impl Couchbase2QueryIndexMgmtClient {
    pub fn new() -> Self {
        unimplemented!()
    }

    async fn get_all_indexes(
        &self,
        _opts: Option<GetAllQueryIndexesOptions>,
    ) -> error::Result<Vec<QueryIndex>> {
        unimplemented!()
    }

    async fn create_index(
        &self,
        _index_name: String,
        _fields: Vec<String>,
        _opts: Option<CreateQueryIndexOptions>,
    ) -> error::Result<()> {
        unimplemented!()
    }

    async fn create_primary_index(
        &self,
        _opts: Option<CreatePrimaryQueryIndexOptions>,
    ) -> error::Result<()> {
        unimplemented!()
    }

    async fn drop_index(
        &self,
        _index_name: String,
        _opts: Option<DropQueryIndexOptions>,
    ) -> error::Result<()> {
        unimplemented!()
    }

    async fn drop_primary_index(
        &self,
        _opts: Option<DropPrimaryQueryIndexOptions>,
    ) -> error::Result<()> {
        unimplemented!()
    }

    async fn watch_indexes(
        &self,
        _index_names: Vec<String>,
        _opts: Option<WatchQueryIndexOptions>,
    ) -> error::Result<()> {
        unimplemented!()
    }

    async fn build_deferred_indexes(
        &self,
        _opts: Option<BuildQueryIndexOptions>,
    ) -> error::Result<()> {
        unimplemented!()
    }
}
