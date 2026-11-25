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
use crate::error::FeatureNotAvailableErrorKind;
use crate::management::collections::collection_settings::{
    CreateCollectionSettings, MaxExpiryValue, UpdateCollectionSettings,
};
use crate::options::collection_mgmt_options::*;
use crate::results::collections_mgmt_results::{CollectionSpec, ScopeSpec};
use crate::retry::RetryStrategy;
use crate::timeouts::{with_timeout, Timeouts};
use couchbase_core::features::BucketFeature;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct CollectionsMgmtClient {
    backend: CollectionsMgmtClientBackend,
}

impl CollectionsMgmtClient {
    pub fn new(backend: CollectionsMgmtClientBackend) -> Self {
        Self { backend }
    }

    pub async fn create_scope(
        &self,
        scope_name: impl Into<String>,
        opts: CreateScopeOptions,
    ) -> error::Result<()> {
        match &self.backend {
            CollectionsMgmtClientBackend::CouchbaseCollectionsMgmtClientBackend(client) => {
                client.create_scope(scope_name, opts).await
            }
            CollectionsMgmtClientBackend::Couchbase2CollectionsMgmtClientBackend(client) => {
                client.create_scope(scope_name, opts).await
            }
        }
    }

    pub async fn drop_scope(
        &self,
        scope_name: impl Into<String>,
        opts: DropScopeOptions,
    ) -> error::Result<()> {
        match &self.backend {
            CollectionsMgmtClientBackend::CouchbaseCollectionsMgmtClientBackend(client) => {
                client.drop_scope(scope_name, opts).await
            }
            CollectionsMgmtClientBackend::Couchbase2CollectionsMgmtClientBackend(client) => {
                client.drop_scope(scope_name, opts).await
            }
        }
    }

    pub async fn create_collection(
        &self,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
        settings: CreateCollectionSettings,
        opts: CreateCollectionOptions,
    ) -> error::Result<()> {
        match &self.backend {
            CollectionsMgmtClientBackend::CouchbaseCollectionsMgmtClientBackend(client) => {
                client
                    .create_collection(scope_name, collection_name, settings, opts)
                    .await
            }
            CollectionsMgmtClientBackend::Couchbase2CollectionsMgmtClientBackend(client) => {
                client
                    .create_collection(scope_name, collection_name, settings, opts)
                    .await
            }
        }
    }

    pub async fn update_collection(
        &self,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
        settings: UpdateCollectionSettings,
        opts: UpdateCollectionOptions,
    ) -> error::Result<()> {
        match &self.backend {
            CollectionsMgmtClientBackend::CouchbaseCollectionsMgmtClientBackend(client) => {
                client
                    .update_collection(scope_name, collection_name, settings, opts)
                    .await
            }
            CollectionsMgmtClientBackend::Couchbase2CollectionsMgmtClientBackend(client) => {
                client
                    .update_collection(scope_name, collection_name, settings, opts)
                    .await
            }
        }
    }

    pub async fn drop_collection(
        &self,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
        opts: DropCollectionOptions,
    ) -> error::Result<()> {
        match &self.backend {
            CollectionsMgmtClientBackend::CouchbaseCollectionsMgmtClientBackend(client) => {
                client
                    .drop_collection(scope_name, collection_name, opts)
                    .await
            }
            CollectionsMgmtClientBackend::Couchbase2CollectionsMgmtClientBackend(client) => {
                client
                    .drop_collection(scope_name, collection_name, opts)
                    .await
            }
        }
    }

    pub async fn get_all_scopes(&self, opts: GetAllScopesOptions) -> error::Result<Vec<ScopeSpec>> {
        match &self.backend {
            CollectionsMgmtClientBackend::CouchbaseCollectionsMgmtClientBackend(client) => {
                client.get_all_scopes(opts).await
            }
            CollectionsMgmtClientBackend::Couchbase2CollectionsMgmtClientBackend(client) => {
                client.get_all_scopes(opts).await
            }
        }
    }

    pub fn bucket_name(&self) -> &str {
        match &self.backend {
            CollectionsMgmtClientBackend::CouchbaseCollectionsMgmtClientBackend(client) => {
                client.bucket_name()
            }
            CollectionsMgmtClientBackend::Couchbase2CollectionsMgmtClientBackend(_) => {
                unimplemented!()
            }
        }
    }

    pub fn tracing_client(&self) -> TracingClient {
        match &self.backend {
            CollectionsMgmtClientBackend::CouchbaseCollectionsMgmtClientBackend(client) => {
                let tracing_client = client.tracing_client();

                TracingClient::new(TracingClientBackend::CouchbaseTracingClientBackend(
                    tracing_client,
                ))
            }
            CollectionsMgmtClientBackend::Couchbase2CollectionsMgmtClientBackend(_) => {
                unimplemented!()
            }
        }
    }
}

#[derive(Clone)]
pub(crate) enum CollectionsMgmtClientBackend {
    CouchbaseCollectionsMgmtClientBackend(CouchbaseCollectionsMgmtClient),
    Couchbase2CollectionsMgmtClientBackend(Couchbase2CollectionsMgmtClient),
}

#[derive(Clone)]
pub(crate) struct CouchbaseCollectionsMgmtClient {
    agent_provider: CouchbaseAgentProvider,
    bucket_name: String,

    default_retry_strategy: Arc<dyn RetryStrategy>,

    default_timeouts: Timeouts,
}

impl CouchbaseCollectionsMgmtClient {
    pub fn new(
        agent_provider: CouchbaseAgentProvider,
        bucket_name: String,
        default_retry_strategy: Arc<dyn RetryStrategy>,
        default_timeouts: Timeouts,
    ) -> Self {
        Self {
            agent_provider,
            bucket_name,
            default_retry_strategy,
            default_timeouts,
        }
    }

    pub async fn create_scope(
        &self,
        scope_name: impl Into<String>,
        opts: CreateScopeOptions,
    ) -> error::Result<()> {
        let timeout = opts.timeout.unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, true, async move {
            let agent = self.agent_provider.get_agent().await;
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());
            CouchbaseAgentProvider::upgrade_agent(agent)?
                .create_scope(
                    &couchbase_core::options::management::CreateScopeOptions::new(
                        &self.bucket_name,
                        scope_name.into().as_str(),
                    )
                    .retry_strategy(retry),
                )
                .await?;

            Ok(())
        })
        .await
    }

    pub async fn drop_scope(
        &self,
        scope_name: impl Into<String>,
        opts: DropScopeOptions,
    ) -> error::Result<()> {
        let timeout = opts.timeout.unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, true, async move {
            let agent = self.agent_provider.get_agent().await;
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());
            CouchbaseAgentProvider::upgrade_agent(agent)?
                .delete_scope(
                    &couchbase_core::options::management::DeleteScopeOptions::new(
                        &self.bucket_name,
                        scope_name.into().as_str(),
                    )
                    .retry_strategy(retry),
                )
                .await?;

            Ok(())
        })
        .await
    }

    pub async fn create_collection(
        &self,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
        settings: CreateCollectionSettings,
        opts: CreateCollectionOptions,
    ) -> error::Result<()> {
        let timeout = opts.timeout.unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, true, async move {
        let agent = self.agent_provider.get_agent().await;
        let agent = CouchbaseAgentProvider::upgrade_agent(agent)?;

        let scope_name = scope_name.into();
        let collection_name = collection_name.into();
        let retry = opts
            .retry_strategy
            .unwrap_or_else(|| self.default_retry_strategy.clone());

        let mut core_opts = couchbase_core::options::management::CreateCollectionOptions::new(
            &self.bucket_name,
            scope_name.as_str(),
            collection_name.as_str(),
        )
        .retry_strategy(retry);

        if let Some(max_ttl) = settings.max_expiry {
            core_opts = core_opts.max_ttl(max_ttl.into());
        }

        if let Some(history_enabled) = settings.history {
            if !agent
                .bucket_features()
                .await?
                .contains(&BucketFeature::NonDedupedHistory)
            {
                return Err(
                    error::Error::new(error::ErrorKind::FeatureNotAvailable(FeatureNotAvailableErrorKind {
                        feature: "history retention".to_string(),
                        msg: Some("history retention is not supported - note that both server 7.2+ and Magma storage engine must be used".to_string()),
                    })),
                );
            }

            core_opts = core_opts.history_enabled(history_enabled);
        }

        agent.create_collection(&core_opts).await?;

        Ok(())
        })
        .await
    }

    pub async fn update_collection(
        &self,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
        settings: UpdateCollectionSettings,
        opts: UpdateCollectionOptions,
    ) -> error::Result<()> {
        let timeout = opts.timeout.unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, true, async move {
        let agent = self.agent_provider.get_agent().await;
        let agent = CouchbaseAgentProvider::upgrade_agent(agent)?;

        let scope_name = scope_name.into();
        let collection_name = collection_name.into();
        let retry = opts
            .retry_strategy
            .unwrap_or_else(|| self.default_retry_strategy.clone());
        let mut core_opts = couchbase_core::options::management::UpdateCollectionOptions::new(
            &self.bucket_name,
            scope_name.as_str(),
            collection_name.as_str(),
        )
        .retry_strategy(retry);

        if let Some(max_ttl) = settings.max_expiry {
            core_opts = core_opts.max_ttl(max_ttl.into());
        }

        if let Some(history_enabled) = settings.history {
            if !agent
                .bucket_features()
                .await?
                .contains(&BucketFeature::NonDedupedHistory)
            {
                return Err(
                    error::Error::new(error::ErrorKind::FeatureNotAvailable(FeatureNotAvailableErrorKind {
                        feature: "history retention".to_string(),
                        msg: Some("history retention is not supported - note that both server 7.2+ and Magma storage engine must be used".to_string()),
                    })),
                );
            }

            core_opts = core_opts.history_enabled(history_enabled);
        }

        agent.update_collection(&core_opts).await?;

        Ok(())
        })
        .await
    }

    pub async fn drop_collection(
        &self,
        scope_name: impl Into<String>,
        collection_name: impl Into<String>,
        opts: DropCollectionOptions,
    ) -> error::Result<()> {
        let timeout = opts.timeout.unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, true, async move {
            let agent = self.agent_provider.get_agent().await;
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());
            CouchbaseAgentProvider::upgrade_agent(agent)?
                .delete_collection(
                    &couchbase_core::options::management::DeleteCollectionOptions::new(
                        &self.bucket_name,
                        scope_name.into().as_str(),
                        collection_name.into().as_str(),
                    )
                    .retry_strategy(retry),
                )
                .await?;

            Ok(())
        })
        .await
    }

    pub async fn get_all_scopes(&self, opts: GetAllScopesOptions) -> error::Result<Vec<ScopeSpec>> {
        let timeout = opts.timeout.unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, false, async move {
            let agent = self.agent_provider.get_agent().await;
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());
            let manifest = CouchbaseAgentProvider::upgrade_agent(agent)?
                .get_collection_manifest(
                    &couchbase_core::options::management::GetCollectionManifestOptions::new(
                        &self.bucket_name,
                    )
                    .retry_strategy(retry),
                )
                .await?;

            let mut scopes = vec![];
            for scope in manifest.scopes {
                let mut collections = vec![];
                for collection in scope.collections {
                    collections.push(CollectionSpec {
                        name: collection.name,
                        scope_name: scope.name.clone(),
                        max_expiry: collection
                            .max_ttl
                            .map(MaxExpiryValue::from)
                            .unwrap_or(MaxExpiryValue::InheritFromBucket),
                        history: collection.history.unwrap_or(false),
                    })
                }

                scopes.push(ScopeSpec {
                    name: scope.name,
                    collections,
                });
            }

            Ok(scopes)
        })
        .await
    }

    pub fn bucket_name(&self) -> &str {
        &self.bucket_name
    }

    pub fn tracing_client(&self) -> CouchbaseTracingClient {
        CouchbaseTracingClient::new(self.agent_provider.clone())
    }
}

#[derive(Clone)]
pub(crate) struct Couchbase2CollectionsMgmtClient {}

impl Couchbase2CollectionsMgmtClient {
    pub fn new() -> Self {
        unimplemented!()
    }

    pub async fn create_scope(
        &self,
        _scope_name: impl Into<String>,
        _opts: CreateScopeOptions,
    ) -> error::Result<()> {
        unimplemented!()
    }

    pub async fn drop_scope(
        &self,
        _scope_name: impl Into<String>,
        _opts: DropScopeOptions,
    ) -> error::Result<()> {
        unimplemented!()
    }

    pub async fn create_collection(
        &self,
        _scope_name: impl Into<String>,
        _collection_name: impl Into<String>,
        _settings: CreateCollectionSettings,
        _opts: CreateCollectionOptions,
    ) -> error::Result<()> {
        unimplemented!()
    }

    pub async fn update_collection(
        &self,
        _scope_name: impl Into<String>,
        _collection_name: impl Into<String>,
        _settings: UpdateCollectionSettings,
        _opts: UpdateCollectionOptions,
    ) -> error::Result<()> {
        unimplemented!()
    }

    pub async fn drop_collection(
        &self,
        _scope_name: impl Into<String>,
        _collection_name: impl Into<String>,
        _opts: DropCollectionOptions,
    ) -> error::Result<()> {
        unimplemented!()
    }

    pub async fn get_all_scopes(
        &self,
        _opts: GetAllScopesOptions,
    ) -> error::Result<Vec<ScopeSpec>> {
        unimplemented!()
    }
}
