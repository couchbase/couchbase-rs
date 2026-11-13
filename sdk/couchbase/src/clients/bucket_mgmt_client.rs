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
use crate::clients::collections_mgmt_client::CollectionsMgmtClientBackend;
use crate::clients::tracing_client::{CouchbaseTracingClient, TracingClient, TracingClientBackend};
use crate::error;
use crate::management::buckets::bucket_settings::BucketSettings;
use crate::options::bucket_mgmt_options::{
    CreateBucketOptions, DropBucketOptions, FlushBucketOptions, GetAllBucketsOptions,
    GetBucketOptions, UpdateBucketOptions,
};
use crate::retry::RetryStrategy;
use crate::timeouts::{with_timeout, Timeouts};
use couchbase_core::mgmtx;
use std::sync::Arc;

pub(crate) struct BucketMgmtClient {
    backend: BucketMgmtClientBackend,
}

impl BucketMgmtClient {
    pub fn new(backend: BucketMgmtClientBackend) -> Self {
        Self { backend }
    }

    pub async fn get_all_buckets(
        &self,
        opts: GetAllBucketsOptions,
    ) -> error::Result<Vec<BucketSettings>> {
        match &self.backend {
            BucketMgmtClientBackend::CouchbaseBucketMgmtClientBackend(client) => {
                client.get_all_buckets(opts).await
            }
            BucketMgmtClientBackend::Couchbase2BucketMgmtClientBackend(client) => {
                client.get_all_buckets(opts).await
            }
        }
    }

    pub async fn get_bucket(
        &self,
        bucket_name: String,
        opts: GetBucketOptions,
    ) -> error::Result<BucketSettings> {
        match &self.backend {
            BucketMgmtClientBackend::CouchbaseBucketMgmtClientBackend(client) => {
                client.get_bucket(bucket_name, opts).await
            }
            BucketMgmtClientBackend::Couchbase2BucketMgmtClientBackend(client) => {
                client.get_bucket(bucket_name, opts).await
            }
        }
    }

    pub async fn create_bucket(
        &self,
        settings: BucketSettings,
        opts: CreateBucketOptions,
    ) -> error::Result<()> {
        match &self.backend {
            BucketMgmtClientBackend::CouchbaseBucketMgmtClientBackend(client) => {
                client.create_bucket(settings, opts).await
            }
            BucketMgmtClientBackend::Couchbase2BucketMgmtClientBackend(client) => {
                client.create_bucket(settings, opts).await
            }
        }
    }

    pub async fn update_bucket(
        &self,
        settings: BucketSettings,
        opts: UpdateBucketOptions,
    ) -> error::Result<()> {
        match &self.backend {
            BucketMgmtClientBackend::CouchbaseBucketMgmtClientBackend(client) => {
                client.update_bucket(settings, opts).await
            }
            BucketMgmtClientBackend::Couchbase2BucketMgmtClientBackend(client) => {
                client.update_bucket(settings, opts).await
            }
        }
    }

    pub async fn drop_bucket(
        &self,
        bucket_name: String,
        opts: DropBucketOptions,
    ) -> error::Result<()> {
        match &self.backend {
            BucketMgmtClientBackend::CouchbaseBucketMgmtClientBackend(client) => {
                client.drop_bucket(bucket_name, opts).await
            }
            BucketMgmtClientBackend::Couchbase2BucketMgmtClientBackend(client) => {
                client.drop_bucket(bucket_name, opts).await
            }
        }
    }

    pub async fn flush_bucket(
        &self,
        bucket_name: String,
        opts: FlushBucketOptions,
    ) -> error::Result<()> {
        match &self.backend {
            BucketMgmtClientBackend::CouchbaseBucketMgmtClientBackend(client) => {
                client.flush_bucket(bucket_name, opts).await
            }
            BucketMgmtClientBackend::Couchbase2BucketMgmtClientBackend(client) => {
                client.flush_bucket(bucket_name, opts).await
            }
        }
    }

    pub fn tracing_client(&self) -> TracingClient {
        match &self.backend {
            BucketMgmtClientBackend::CouchbaseBucketMgmtClientBackend(client) => {
                let tracing_client = client.tracing_client();

                TracingClient::new(TracingClientBackend::CouchbaseTracingClientBackend(
                    tracing_client,
                ))
            }
            BucketMgmtClientBackend::Couchbase2BucketMgmtClientBackend(client) => {
                unimplemented!()
            }
        }
    }
}

pub(crate) enum BucketMgmtClientBackend {
    CouchbaseBucketMgmtClientBackend(CouchbaseBucketMgmtClient),
    Couchbase2BucketMgmtClientBackend(Couchbase2BucketMgmtClient),
}

pub(crate) struct CouchbaseBucketMgmtClient {
    agent_provider: CouchbaseAgentProvider,

    default_retry_strategy: Arc<dyn RetryStrategy>,

    default_timeouts: Timeouts,
}

impl CouchbaseBucketMgmtClient {
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

    pub async fn get_all_buckets(
        &self,
        opts: GetAllBucketsOptions,
    ) -> error::Result<Vec<BucketSettings>> {
        let timeout = opts.timeout.unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, false, async move {
            let agent = self.agent_provider.get_agent().await;
            let opts = couchbase_core::options::management::GetAllBucketsOptions::new()
                .retry_strategy(
                    opts.retry_strategy
                        .unwrap_or_else(|| self.default_retry_strategy.clone()),
                );

            let buckets = CouchbaseAgentProvider::upgrade_agent(agent)?
                .get_all_buckets(&opts)
                .await?;

            Ok(buckets.into_iter().map(|b| b.into()).collect())
        })
        .await
    }

    pub async fn get_bucket(
        &self,
        bucket_name: String,
        opts: GetBucketOptions,
    ) -> error::Result<BucketSettings> {
        let timeout = opts.timeout.unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, false, async move {
            let agent = self.agent_provider.get_agent().await;
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());
            let opts = couchbase_core::options::management::GetBucketOptions::new(&bucket_name)
                .retry_strategy(retry);

            let bucket = CouchbaseAgentProvider::upgrade_agent(agent)?
                .get_bucket(&opts)
                .await?;

            Ok(bucket.into())
        })
        .await
    }

    pub async fn create_bucket(
        &self,
        settings: BucketSettings,
        opts: CreateBucketOptions,
    ) -> error::Result<()> {
        let timeout = opts.timeout.unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, true, async move {
            let agent = self.agent_provider.get_agent().await;

            let mut core_settings = mgmtx::bucket_settings::BucketSettings::default();

            if let Some(ram_quota_mb) = settings.ram_quota_mb {
                core_settings = core_settings.ram_quota_mb(ram_quota_mb);
            }

            if let Some(flush_enabled) = settings.flush_enabled {
                core_settings = core_settings.flush_enabled(flush_enabled);
            }

            if let Some(replica_number) = settings.num_replicas {
                core_settings = core_settings.replica_number(replica_number);
            }

            if let Some(eviction_policy) = settings.eviction_policy {
                core_settings = core_settings.eviction_policy(eviction_policy);
            }

            if let Some(max_ttl) = settings.max_expiry {
                core_settings = core_settings.max_ttl(max_ttl);
            }

            if let Some(compression_mode) = settings.compression_mode {
                core_settings = core_settings.compression_mode(compression_mode);
            }

            if let Some(durability_min_level) = settings.minimum_durability_level {
                core_settings = core_settings.durability_min_level(durability_min_level);
            }

            if let Some(history_retention_collection_default) =
                settings.history_retention_collection_default
            {
                core_settings = core_settings
                    .history_retention_collection_default(history_retention_collection_default);
            }

            if let Some(history_retention_bytes) = settings.history_retention_bytes {
                core_settings = core_settings.history_retention_bytes(history_retention_bytes);
            }

            if let Some(history_retention_duration) = settings.history_retention_duration {
                core_settings = core_settings
                    .history_retention_seconds(history_retention_duration.as_secs() as u32);
            }

            if let Some(conflict_resolution_type) = settings.conflict_resolution_type {
                core_settings = core_settings.conflict_resolution_type(conflict_resolution_type);
            }

            if let Some(replica_index) = settings.replica_indexes {
                core_settings = core_settings.replica_index(replica_index);
            }

            if let Some(bucket_type) = settings.bucket_type {
                core_settings = core_settings.bucket_type(bucket_type);
            }

            if let Some(storage_backend) = settings.storage_backend {
                core_settings = core_settings.storage_backend(storage_backend);
            }

            if let Some(num_vbuckets) = settings.num_vbuckets {
                core_settings = core_settings.num_vbuckets(num_vbuckets);
            }

            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());
            let opts = couchbase_core::options::management::CreateBucketOptions::new(
                &settings.name,
                &core_settings,
            )
            .retry_strategy(retry);

            CouchbaseAgentProvider::upgrade_agent(agent)?
                .create_bucket(&opts)
                .await?;

            Ok(())
        })
        .await
    }

    pub async fn update_bucket(
        &self,
        settings: BucketSettings,
        opts: UpdateBucketOptions,
    ) -> error::Result<()> {
        let timeout = opts.timeout.unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, true, async move {
            let agent = self.agent_provider.get_agent().await;

            let mut core_settings = mgmtx::bucket_settings::BucketSettings::default();

            if let Some(ram_quota_mb) = settings.ram_quota_mb {
                core_settings = core_settings.ram_quota_mb(ram_quota_mb);
            }

            if let Some(flush_enabled) = settings.flush_enabled {
                core_settings = core_settings.flush_enabled(flush_enabled);
            }

            if let Some(replica_number) = settings.num_replicas {
                core_settings = core_settings.replica_number(replica_number);
            }

            if let Some(eviction_policy) = settings.eviction_policy {
                core_settings = core_settings.eviction_policy(eviction_policy);
            }

            if let Some(max_ttl) = settings.max_expiry {
                core_settings = core_settings.max_ttl(max_ttl);
            }

            if let Some(compression_mode) = settings.compression_mode {
                core_settings = core_settings.compression_mode(compression_mode);
            }

            if let Some(durability_min_level) = settings.minimum_durability_level {
                core_settings = core_settings.durability_min_level(durability_min_level);
            }

            if let Some(history_retention_collection_default) =
                settings.history_retention_collection_default
            {
                core_settings = core_settings
                    .history_retention_collection_default(history_retention_collection_default);
            }

            if let Some(history_retention_bytes) = settings.history_retention_bytes {
                core_settings = core_settings.history_retention_bytes(history_retention_bytes);
            }

            if let Some(history_retention_duration) = settings.history_retention_duration {
                core_settings = core_settings
                    .history_retention_seconds(history_retention_duration.as_secs() as u32);
            }

            if let Some(conflict_resolution_type) = settings.conflict_resolution_type {
                core_settings = core_settings.conflict_resolution_type(conflict_resolution_type);
            }

            if let Some(replica_indexes) = settings.replica_indexes {
                core_settings = core_settings.replica_index(replica_indexes);
            }

            if let Some(bucket_type) = settings.bucket_type {
                core_settings = core_settings.bucket_type(bucket_type);
            }

            if let Some(num_vbuckets) = settings.num_vbuckets {
                core_settings = core_settings.num_vbuckets(num_vbuckets);
            }

            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());
            let opts = couchbase_core::options::management::UpdateBucketOptions::new(
                &settings.name,
                &core_settings,
            )
            .retry_strategy(retry);

            CouchbaseAgentProvider::upgrade_agent(agent)?
                .update_bucket(&opts)
                .await?;

            Ok(())
        })
        .await
    }

    pub async fn drop_bucket(
        &self,
        bucket_name: String,
        opts: DropBucketOptions,
    ) -> error::Result<()> {
        let timeout = opts.timeout.unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, true, async move {
            let agent = self.agent_provider.get_agent().await;
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());
            let opts = couchbase_core::options::management::DeleteBucketOptions::new(&bucket_name)
                .retry_strategy(retry);

            CouchbaseAgentProvider::upgrade_agent(agent)?
                .delete_bucket(&opts)
                .await?;

            Ok(())
        })
        .await
    }

    pub async fn flush_bucket(
        &self,
        bucket_name: String,
        opts: FlushBucketOptions,
    ) -> error::Result<()> {
        let timeout = opts.timeout.unwrap_or(self.default_timeouts.management);
        with_timeout(timeout, true, async move {
            let agent = self.agent_provider.get_agent().await;
            let retry = opts
                .retry_strategy
                .unwrap_or_else(|| self.default_retry_strategy.clone());
            let opts = couchbase_core::options::management::FlushBucketOptions::new(&bucket_name)
                .retry_strategy(retry);

            CouchbaseAgentProvider::upgrade_agent(agent)?
                .flush_bucket(&opts)
                .await?;

            Ok(())
        })
        .await
    }

    pub fn tracing_client(&self) -> CouchbaseTracingClient {
        CouchbaseTracingClient::new(self.agent_provider.clone())
    }
}

pub(crate) struct Couchbase2BucketMgmtClient {}

impl Couchbase2BucketMgmtClient {
    pub fn new() -> Self {
        unimplemented!()
    }

    pub async fn get_all_buckets(
        &self,
        _opts: GetAllBucketsOptions,
    ) -> error::Result<Vec<BucketSettings>> {
        unimplemented!()
    }

    pub async fn get_bucket(
        &self,
        _bucket_name: String,
        _opts: GetBucketOptions,
    ) -> error::Result<BucketSettings> {
        unimplemented!()
    }

    pub async fn create_bucket(
        &self,
        _settings: BucketSettings,
        _opts: CreateBucketOptions,
    ) -> error::Result<()> {
        unimplemented!()
    }

    pub async fn update_bucket(
        &self,
        _settings: BucketSettings,
        _opts: UpdateBucketOptions,
    ) -> error::Result<()> {
        unimplemented!()
    }

    pub async fn drop_bucket(
        &self,
        _bucket_name: String,
        _opts: DropBucketOptions,
    ) -> error::Result<()> {
        unimplemented!()
    }

    pub async fn flush_bucket(
        &self,
        _bucket_name: String,
        _opts: FlushBucketOptions,
    ) -> error::Result<()> {
        unimplemented!()
    }
}
