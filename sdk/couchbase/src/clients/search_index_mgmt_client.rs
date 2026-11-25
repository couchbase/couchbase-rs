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
use crate::management::search::index::SearchIndex;
use crate::options::search_index_mgmt_options::{
    AllowQueryingSearchIndexOptions, AnalyzeDocumentOptions, DisallowQueryingSearchIndexOptions,
    DropSearchIndexOptions, FreezePlanSearchIndexOptions, GetAllSearchIndexesOptions,
    GetIndexedDocumentsCountOptions, GetSearchIndexOptions, PauseIngestSearchIndexOptions,
    ResumeIngestSearchIndexOptions, UnfreezePlanSearchIndexOptions, UpsertSearchIndexOptions,
};
use crate::retry::RetryStrategy;
use crate::timeouts::{with_timeout, Timeouts};
use crate::tracing::Keyspace;
use couchbase_core::options::search_management;
use serde_json::Value;
use std::sync::Arc;

pub(crate) struct SearchIndexMgmtClient {
    backend: SearchIndexMgmtClientBackend,
}

impl SearchIndexMgmtClient {
    pub fn new(backend: SearchIndexMgmtClientBackend) -> Self {
        Self { backend }
    }

    pub async fn get_index(
        &self,
        index_name: String,
        opts: Option<GetSearchIndexOptions>,
    ) -> error::Result<SearchIndex> {
        match &self.backend {
            SearchIndexMgmtClientBackend::CouchbaseSearchIndexMgmtClientBackend(backend) => {
                backend.get_index(index_name, opts).await
            }
            SearchIndexMgmtClientBackend::Couchbase2SearchIndexMgmtClientBackend(backend) => {
                backend.get_index(index_name, opts).await
            }
        }
    }

    pub async fn get_all_indexes(
        &self,
        opts: Option<GetAllSearchIndexesOptions>,
    ) -> error::Result<Vec<SearchIndex>> {
        match &self.backend {
            SearchIndexMgmtClientBackend::CouchbaseSearchIndexMgmtClientBackend(backend) => {
                backend.get_all_indexes(opts).await
            }
            SearchIndexMgmtClientBackend::Couchbase2SearchIndexMgmtClientBackend(backend) => {
                backend.get_all_indexes(opts).await
            }
        }
    }

    pub async fn upsert_index(
        &self,
        index: SearchIndex,
        opts: Option<UpsertSearchIndexOptions>,
    ) -> error::Result<()> {
        match &self.backend {
            SearchIndexMgmtClientBackend::CouchbaseSearchIndexMgmtClientBackend(backend) => {
                backend.upsert_index(index, opts).await
            }
            SearchIndexMgmtClientBackend::Couchbase2SearchIndexMgmtClientBackend(backend) => {
                backend.upsert_index(index, opts).await
            }
        }
    }

    pub async fn drop_index(
        &self,
        index_name: String,
        opts: Option<DropSearchIndexOptions>,
    ) -> error::Result<()> {
        match &self.backend {
            SearchIndexMgmtClientBackend::CouchbaseSearchIndexMgmtClientBackend(backend) => {
                backend.drop_index(index_name, opts).await
            }
            SearchIndexMgmtClientBackend::Couchbase2SearchIndexMgmtClientBackend(backend) => {
                backend.drop_index(index_name, opts).await
            }
        }
    }

    pub async fn analyze_document(
        &self,
        index_name: String,
        document: Value,
        opts: Option<AnalyzeDocumentOptions>,
    ) -> error::Result<Value> {
        match &self.backend {
            SearchIndexMgmtClientBackend::CouchbaseSearchIndexMgmtClientBackend(backend) => {
                backend.analyze_document(index_name, document, opts).await
            }
            SearchIndexMgmtClientBackend::Couchbase2SearchIndexMgmtClientBackend(backend) => {
                backend.analyze_document(index_name, document, opts).await
            }
        }
    }

    pub async fn get_indexed_documents_count(
        &self,
        index_name: String,
        opts: Option<GetIndexedDocumentsCountOptions>,
    ) -> error::Result<u64> {
        match &self.backend {
            SearchIndexMgmtClientBackend::CouchbaseSearchIndexMgmtClientBackend(backend) => {
                backend.get_indexed_documents_count(index_name, opts).await
            }
            SearchIndexMgmtClientBackend::Couchbase2SearchIndexMgmtClientBackend(backend) => {
                backend.get_indexed_documents_count(index_name, opts).await
            }
        }
    }

    pub async fn pause_ingest(
        &self,
        index_name: String,
        opts: Option<PauseIngestSearchIndexOptions>,
    ) -> error::Result<()> {
        match &self.backend {
            SearchIndexMgmtClientBackend::CouchbaseSearchIndexMgmtClientBackend(backend) => {
                backend.pause_ingest(index_name, opts).await
            }
            SearchIndexMgmtClientBackend::Couchbase2SearchIndexMgmtClientBackend(backend) => {
                backend.pause_ingest(index_name, opts).await
            }
        }
    }

    pub async fn resume_ingest(
        &self,
        index_name: String,
        opts: Option<ResumeIngestSearchIndexOptions>,
    ) -> error::Result<()> {
        match &self.backend {
            SearchIndexMgmtClientBackend::CouchbaseSearchIndexMgmtClientBackend(backend) => {
                backend.resume_ingest(index_name, opts).await
            }
            SearchIndexMgmtClientBackend::Couchbase2SearchIndexMgmtClientBackend(backend) => {
                backend.resume_ingest(index_name, opts).await
            }
        }
    }

    pub async fn allow_querying(
        &self,
        index_name: String,
        opts: Option<AllowQueryingSearchIndexOptions>,
    ) -> error::Result<()> {
        match &self.backend {
            SearchIndexMgmtClientBackend::CouchbaseSearchIndexMgmtClientBackend(backend) => {
                backend.allow_querying(index_name, opts).await
            }
            SearchIndexMgmtClientBackend::Couchbase2SearchIndexMgmtClientBackend(backend) => {
                backend.allow_querying(index_name, opts).await
            }
        }
    }

    pub async fn disallow_querying(
        &self,
        index_name: String,
        opts: Option<DisallowQueryingSearchIndexOptions>,
    ) -> error::Result<()> {
        match &self.backend {
            SearchIndexMgmtClientBackend::CouchbaseSearchIndexMgmtClientBackend(backend) => {
                backend.disallow_querying(index_name, opts).await
            }
            SearchIndexMgmtClientBackend::Couchbase2SearchIndexMgmtClientBackend(backend) => {
                backend.disallow_querying(index_name, opts).await
            }
        }
    }

    pub async fn freeze_plan(
        &self,
        index_name: String,
        opts: Option<FreezePlanSearchIndexOptions>,
    ) -> error::Result<()> {
        match &self.backend {
            SearchIndexMgmtClientBackend::CouchbaseSearchIndexMgmtClientBackend(backend) => {
                backend.freeze_plan(index_name, opts).await
            }
            SearchIndexMgmtClientBackend::Couchbase2SearchIndexMgmtClientBackend(backend) => {
                backend.freeze_plan(index_name, opts).await
            }
        }
    }

    pub async fn unfreeze_plan(
        &self,
        index_name: String,
        opts: Option<UnfreezePlanSearchIndexOptions>,
    ) -> error::Result<()> {
        match &self.backend {
            SearchIndexMgmtClientBackend::CouchbaseSearchIndexMgmtClientBackend(backend) => {
                backend.unfreeze_plan(index_name, opts).await
            }
            SearchIndexMgmtClientBackend::Couchbase2SearchIndexMgmtClientBackend(backend) => {
                backend.unfreeze_plan(index_name, opts).await
            }
        }
    }

    pub fn tracing_client(&self) -> TracingClient {
        match &self.backend {
            SearchIndexMgmtClientBackend::CouchbaseSearchIndexMgmtClientBackend(backend) => {
                TracingClient::new(TracingClientBackend::CouchbaseTracingClientBackend(
                    backend.tracing_client(),
                ))
            }
            SearchIndexMgmtClientBackend::Couchbase2SearchIndexMgmtClientBackend(_) => {
                unimplemented!()
            }
        }
    }

    pub fn bucket_name(&self) -> &str {
        match &self.backend {
            SearchIndexMgmtClientBackend::CouchbaseSearchIndexMgmtClientBackend(backend) => {
                backend.bucket_name()
            }
            SearchIndexMgmtClientBackend::Couchbase2SearchIndexMgmtClientBackend(_) => {
                unimplemented!()
            }
        }
    }

    pub fn scope_name(&self) -> &str {
        match &self.backend {
            SearchIndexMgmtClientBackend::CouchbaseSearchIndexMgmtClientBackend(backend) => {
                backend.scope_name()
            }
            SearchIndexMgmtClientBackend::Couchbase2SearchIndexMgmtClientBackend(_) => {
                unimplemented!()
            }
        }
    }

    pub fn keyspace(&self) -> Keyspace<'_> {
        match &self.backend {
            SearchIndexMgmtClientBackend::CouchbaseSearchIndexMgmtClientBackend(backend) => {
                (&backend.keyspace).into()
            }
            SearchIndexMgmtClientBackend::Couchbase2SearchIndexMgmtClientBackend(_) => {
                unimplemented!()
            }
        }
    }
}

pub(crate) enum SearchIndexMgmtClientBackend {
    CouchbaseSearchIndexMgmtClientBackend(CouchbaseSearchIndexMgmtClient),
    Couchbase2SearchIndexMgmtClientBackend(Couchbase2SearchIndexMgmtClient),
}

#[derive(Clone, Debug)]
pub(crate) struct SearchIndexKeyspace {
    pub bucket_name: String,
    pub scope_name: String,
}

impl SearchIndexKeyspace {
    pub(crate) fn bucket_name(&self) -> &str {
        self.bucket_name.as_str()
    }

    pub(crate) fn scope_name(&self) -> &str {
        self.scope_name.as_str()
    }
}

impl<'a> From<&'a SearchIndexKeyspace> for Keyspace<'a> {
    fn from(keyspace: &'a SearchIndexKeyspace) -> Self {
        Keyspace::Scope {
            bucket: &keyspace.bucket_name,
            scope: &keyspace.scope_name,
        }
    }
}

pub(crate) struct CouchbaseSearchIndexMgmtClient {
    agent_provider: CouchbaseAgentProvider,
    keyspace: SearchIndexKeyspace,
    default_retry_strategy: Arc<dyn RetryStrategy>,
    default_timeouts: Timeouts,
}

impl CouchbaseSearchIndexMgmtClient {
    pub fn new(
        agent_provider: CouchbaseAgentProvider,
        keyspace: SearchIndexKeyspace,
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

    async fn get_index(
        &self,
        index_name: String,
        opts: Option<GetSearchIndexOptions>,
    ) -> error::Result<SearchIndex> {
        let timeout = opts
            .as_ref()
            .and_then(|o| o.timeout)
            .unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, false, async move {
            let opts = opts.unwrap_or_default();
            let agent = self.agent_provider.get_agent().await;
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());

            let get_opts = search_management::GetIndexOptions::new(&index_name)
                .bucket_name(&self.keyspace.bucket_name)
                .scope_name(&self.keyspace.scope_name)
                .retry_strategy(retry);

            let index = CouchbaseAgentProvider::upgrade_agent(agent)?
                .get_search_index(&get_opts)
                .await?;
            Ok(index.into())
        })
        .await
    }

    async fn get_all_indexes(
        &self,
        opts: Option<GetAllSearchIndexesOptions>,
    ) -> error::Result<Vec<SearchIndex>> {
        let timeout = opts
            .as_ref()
            .and_then(|o| o.timeout)
            .unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, false, async move {
            let opts = opts.unwrap_or_default();
            let agent = self.agent_provider.get_agent().await;
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());

            let get_all_opts = search_management::GetAllIndexesOptions::new()
                .bucket_name(&self.keyspace.bucket_name)
                .scope_name(&self.keyspace.scope_name)
                .retry_strategy(retry);

            let indexes = CouchbaseAgentProvider::upgrade_agent(agent)?
                .get_all_search_indexes(&get_all_opts)
                .await?;

            Ok(indexes.into_iter().map(SearchIndex::from).collect())
        })
        .await
    }

    async fn upsert_index(
        &self,
        index: SearchIndex,
        opts: Option<UpsertSearchIndexOptions>,
    ) -> error::Result<()> {
        let timeout = opts
            .as_ref()
            .and_then(|o| o.timeout)
            .unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, true, async move {
            let opts = opts.unwrap_or_default();
            let agent = self.agent_provider.get_agent().await;
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());

            let index = &index.into();
            let upsert_opts = search_management::UpsertIndexOptions::new(index)
                .bucket_name(&self.keyspace.bucket_name)
                .scope_name(&self.keyspace.scope_name)
                .retry_strategy(retry);

            CouchbaseAgentProvider::upgrade_agent(agent)?
                .upsert_search_index(&upsert_opts)
                .await?;
            Ok(())
        })
        .await
    }

    async fn drop_index(
        &self,
        index_name: String,
        opts: Option<DropSearchIndexOptions>,
    ) -> error::Result<()> {
        let timeout = opts
            .as_ref()
            .and_then(|o| o.timeout)
            .unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, true, async move {
            let opts = opts.unwrap_or_default();
            let agent = self.agent_provider.get_agent().await;
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());

            let delete_opts = search_management::DeleteIndexOptions::new(&index_name)
                .bucket_name(&self.keyspace.bucket_name)
                .scope_name(&self.keyspace.scope_name)
                .retry_strategy(retry);

            CouchbaseAgentProvider::upgrade_agent(agent)?
                .delete_search_index(&delete_opts)
                .await?;
            Ok(())
        })
        .await
    }

    async fn analyze_document(
        &self,
        index_name: String,
        document: Value,
        opts: Option<AnalyzeDocumentOptions>,
    ) -> error::Result<Value> {
        let timeout = opts
            .as_ref()
            .and_then(|o| o.timeout)
            .unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, false, async move {
            let opts = opts.unwrap_or_default();
            let agent = self.agent_provider.get_agent().await;
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());

            let value =
                serde_json::to_vec(&document).map_err(error::Error::encoding_failure_from_serde)?;

            let analyze_opts = search_management::AnalyzeDocumentOptions::new(&index_name, &value)
                .bucket_name(&self.keyspace.bucket_name)
                .scope_name(&self.keyspace.scope_name)
                .retry_strategy(retry);

            let analysis = CouchbaseAgentProvider::upgrade_agent(agent)?
                .analyze_search_document(&analyze_opts)
                .await?;

            let analysed = serde_json::from_slice(&analysis.analyzed)
                .map_err(error::Error::decoding_failure_from_serde)?;

            Ok(analysed)
        })
        .await
    }

    async fn get_indexed_documents_count(
        &self,
        index_name: String,
        opts: Option<GetIndexedDocumentsCountOptions>,
    ) -> error::Result<u64> {
        let timeout = opts
            .as_ref()
            .and_then(|o| o.timeout)
            .unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, false, async move {
            let opts = opts.unwrap_or_default();
            let agent = self.agent_provider.get_agent().await;
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());

            let count_opts = search_management::GetIndexedDocumentsCountOptions::new(&index_name)
                .bucket_name(&self.keyspace.bucket_name)
                .scope_name(&self.keyspace.scope_name)
                .retry_strategy(retry);

            let count = CouchbaseAgentProvider::upgrade_agent(agent)?
                .get_search_indexed_documents_count(&count_opts)
                .await?;
            Ok(count)
        })
        .await
    }

    async fn pause_ingest(
        &self,
        index_name: String,
        opts: Option<PauseIngestSearchIndexOptions>,
    ) -> error::Result<()> {
        let timeout = opts
            .as_ref()
            .and_then(|o| o.timeout)
            .unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, true, async move {
            let opts = opts.unwrap_or_default();
            let agent = self.agent_provider.get_agent().await;
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());

            let pause_opts = search_management::PauseIngestOptions::new(&index_name)
                .bucket_name(&self.keyspace.bucket_name)
                .scope_name(&self.keyspace.scope_name)
                .retry_strategy(retry);

            CouchbaseAgentProvider::upgrade_agent(agent)?
                .pause_search_index_ingest(&pause_opts)
                .await?;
            Ok(())
        })
        .await
    }

    async fn resume_ingest(
        &self,
        index_name: String,
        opts: Option<ResumeIngestSearchIndexOptions>,
    ) -> error::Result<()> {
        let timeout = opts
            .as_ref()
            .and_then(|o| o.timeout)
            .unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, true, async move {
            let opts = opts.unwrap_or_default();
            let agent = self.agent_provider.get_agent().await;
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());

            let resume_opts = search_management::ResumeIngestOptions::new(&index_name)
                .bucket_name(&self.keyspace.bucket_name)
                .scope_name(&self.keyspace.scope_name)
                .retry_strategy(retry);

            CouchbaseAgentProvider::upgrade_agent(agent)?
                .resume_search_index_ingest(&resume_opts)
                .await?;
            Ok(())
        })
        .await
    }

    async fn allow_querying(
        &self,
        index_name: String,
        opts: Option<AllowQueryingSearchIndexOptions>,
    ) -> error::Result<()> {
        let timeout = opts
            .as_ref()
            .and_then(|o| o.timeout)
            .unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, true, async move {
            let opts = opts.unwrap_or_default();
            let agent = self.agent_provider.get_agent().await;
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());

            let allow_opts = search_management::AllowQueryingOptions::new(&index_name)
                .bucket_name(&self.keyspace.bucket_name)
                .scope_name(&self.keyspace.scope_name)
                .retry_strategy(retry);

            CouchbaseAgentProvider::upgrade_agent(agent)?
                .allow_search_index_querying(&allow_opts)
                .await?;
            Ok(())
        })
        .await
    }

    async fn disallow_querying(
        &self,
        index_name: String,
        opts: Option<DisallowQueryingSearchIndexOptions>,
    ) -> error::Result<()> {
        let timeout = opts
            .as_ref()
            .and_then(|o| o.timeout)
            .unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, true, async move {
            let opts = opts.unwrap_or_default();
            let agent = self.agent_provider.get_agent().await;
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());

            let disallow_opts = search_management::DisallowQueryingOptions::new(&index_name)
                .bucket_name(&self.keyspace.bucket_name)
                .scope_name(&self.keyspace.scope_name)
                .retry_strategy(retry);

            CouchbaseAgentProvider::upgrade_agent(agent)?
                .disallow_search_index_querying(&disallow_opts)
                .await?;
            Ok(())
        })
        .await
    }

    async fn freeze_plan(
        &self,
        index_name: String,
        opts: Option<FreezePlanSearchIndexOptions>,
    ) -> error::Result<()> {
        let timeout = opts
            .as_ref()
            .and_then(|o| o.timeout)
            .unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, true, async move {
            let opts = opts.unwrap_or_default();
            let agent = self.agent_provider.get_agent().await;
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());

            let freeze_opts = search_management::FreezePlanOptions::new(&index_name)
                .bucket_name(&self.keyspace.bucket_name)
                .scope_name(&self.keyspace.scope_name)
                .retry_strategy(retry);

            CouchbaseAgentProvider::upgrade_agent(agent)?
                .freeze_search_index_plan(&freeze_opts)
                .await?;
            Ok(())
        })
        .await
    }

    async fn unfreeze_plan(
        &self,
        index_name: String,
        opts: Option<UnfreezePlanSearchIndexOptions>,
    ) -> error::Result<()> {
        let timeout = opts
            .as_ref()
            .and_then(|o| o.timeout)
            .unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, true, async move {
            let opts = opts.unwrap_or_default();
            let agent = self.agent_provider.get_agent().await;
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());

            let unfreeze_opts = search_management::UnfreezePlanOptions::new(&index_name)
                .bucket_name(&self.keyspace.bucket_name)
                .scope_name(&self.keyspace.scope_name)
                .retry_strategy(retry);

            CouchbaseAgentProvider::upgrade_agent(agent)?
                .unfreeze_search_index_plan(&unfreeze_opts)
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
}

pub(crate) struct Couchbase2SearchIndexMgmtClient {}

impl Couchbase2SearchIndexMgmtClient {
    pub fn new() -> Self {
        unimplemented!()
    }

    async fn get_index(
        &self,
        _index_name: String,
        _opts: Option<GetSearchIndexOptions>,
    ) -> error::Result<SearchIndex> {
        unimplemented!()
    }

    async fn get_all_indexes(
        &self,
        _opts: Option<GetAllSearchIndexesOptions>,
    ) -> error::Result<Vec<SearchIndex>> {
        unimplemented!()
    }

    async fn upsert_index(
        &self,
        _index: SearchIndex,
        _opts: Option<UpsertSearchIndexOptions>,
    ) -> error::Result<()> {
        unimplemented!()
    }

    async fn drop_index(
        &self,
        _index_name: String,
        _opts: Option<DropSearchIndexOptions>,
    ) -> error::Result<()> {
        unimplemented!()
    }

    async fn analyze_document(
        &self,
        _index_name: String,
        _document: Value,
        _opts: Option<AnalyzeDocumentOptions>,
    ) -> error::Result<Value> {
        unimplemented!()
    }

    async fn get_indexed_documents_count(
        &self,
        _index_name: String,
        _opts: Option<GetIndexedDocumentsCountOptions>,
    ) -> error::Result<u64> {
        unimplemented!()
    }

    async fn pause_ingest(
        &self,
        _index_name: String,
        _opts: Option<PauseIngestSearchIndexOptions>,
    ) -> error::Result<()> {
        unimplemented!()
    }

    async fn resume_ingest(
        &self,
        _index_name: String,
        _opts: Option<ResumeIngestSearchIndexOptions>,
    ) -> error::Result<()> {
        unimplemented!()
    }

    async fn allow_querying(
        &self,
        _index_name: String,
        _opts: Option<AllowQueryingSearchIndexOptions>,
    ) -> error::Result<()> {
        unimplemented!()
    }

    async fn disallow_querying(
        &self,
        _index_name: String,
        _opts: Option<DisallowQueryingSearchIndexOptions>,
    ) -> error::Result<()> {
        unimplemented!()
    }

    async fn freeze_plan(
        &self,
        _index_name: String,
        _opts: Option<FreezePlanSearchIndexOptions>,
    ) -> error::Result<()> {
        unimplemented!()
    }

    async fn unfreeze_plan(
        &self,
        _index_name: String,
        _opts: Option<UnfreezePlanSearchIndexOptions>,
    ) -> error::Result<()> {
        unimplemented!()
    }
}
