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

use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::httpx::request::OnBehalfOfInfo;
use crate::queryx;
pub use crate::queryx::ensure_index_helper::DesiredState;
use crate::queryx::query_options::{FullScanVectors, SparseScanVectors};
use crate::retry::{RetryStrategy, DEFAULT_RETRY_STRATEGY};

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct QueryOptions {
    pub args: Option<Vec<Value>>,
    pub atr_collection: Option<String>,
    pub auto_execute: Option<bool>,
    pub client_context_id: Option<String>,
    pub compression: Option<queryx::query_options::Compression>,
    pub controls: Option<bool>,
    pub creds: Option<Vec<queryx::query_options::CredsJson>>,
    pub durability_level: Option<queryx::query_options::DurabilityLevel>,
    pub encoded_plan: Option<String>,
    pub encoding: Option<queryx::query_options::Encoding>,
    pub format: Option<queryx::query_options::Format>,
    pub kv_timeout: Option<Duration>,
    pub max_parallelism: Option<u32>,
    pub memory_quota: Option<u32>,
    pub metrics: Option<bool>,
    pub namespace: Option<String>,
    pub num_atrs: Option<u32>,
    pub pipeline_batch: Option<u32>,
    pub pipeline_cap: Option<u32>,
    pub prepared: Option<String>,
    pub preserve_expiry: Option<bool>,
    pub pretty: Option<bool>,
    pub profile: Option<queryx::query_options::ProfileMode>,
    pub query_context: Option<String>,
    pub read_only: Option<bool>,
    pub scan_cap: Option<u32>,
    pub scan_consistency: Option<queryx::query_options::ScanConsistency>,
    pub sparse_scan_vector: Option<SparseScanVectors>,
    pub full_scan_vector: Option<FullScanVectors>,
    pub sparse_scan_vectors: Option<HashMap<String, SparseScanVectors>>,
    pub full_scan_vectors: Option<HashMap<String, FullScanVectors>>,
    pub scan_wait: Option<Duration>,
    pub signature: Option<bool>,
    pub statement: Option<String>,
    pub timeout: Option<Duration>,
    pub tx_data: Option<Vec<u8>>,
    pub tx_id: Option<String>,
    pub tx_implicit: Option<bool>,
    pub tx_stmt_num: Option<u32>,
    pub tx_timeout: Option<Duration>,
    pub use_cbo: Option<bool>,
    pub use_fts: Option<bool>,
    pub use_replica: Option<queryx::query_options::ReplicaLevel>,

    pub named_args: Option<HashMap<String, Value>>,
    pub raw: Option<HashMap<String, Value>>,
    pub natural_language: Option<queryx::query_options::NaturalLanguageOptions>,

    pub on_behalf_of: Option<OnBehalfOfInfo>,
    pub idempotent: Option<bool>, // Defaults to the value of read_only.
    // The query node to send the request to. `None` means an endpoint is selected at random.
    pub node_uuid: Option<String>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl Default for QueryOptions {
    fn default() -> Self {
        Self {
            args: None,
            atr_collection: None,
            auto_execute: None,
            client_context_id: None,
            compression: None,
            controls: None,
            creds: None,
            durability_level: None,
            encoded_plan: None,
            encoding: None,
            format: None,
            kv_timeout: None,
            max_parallelism: None,
            memory_quota: None,
            metrics: None,
            namespace: None,
            num_atrs: None,
            pipeline_batch: None,
            pipeline_cap: None,
            prepared: None,
            preserve_expiry: None,
            pretty: None,
            profile: None,
            query_context: None,
            read_only: None,
            scan_cap: None,
            scan_consistency: None,
            sparse_scan_vector: None,
            full_scan_vector: None,
            sparse_scan_vectors: None,
            full_scan_vectors: None,
            scan_wait: None,
            signature: None,
            statement: None,
            timeout: None,
            tx_data: None,
            tx_id: None,
            tx_implicit: None,
            tx_stmt_num: None,
            tx_timeout: None,
            use_cbo: None,
            use_fts: None,
            use_replica: None,
            named_args: None,
            raw: None,
            natural_language: None,
            on_behalf_of: None,
            idempotent: None,
            node_uuid: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }
}

impl QueryOptions {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn args(mut self, args: impl Into<Option<Vec<Value>>>) -> Self {
        self.args = args.into();
        self
    }

    pub fn atr_collection(mut self, atr_collection: impl Into<Option<String>>) -> Self {
        self.atr_collection = atr_collection.into();
        self
    }

    pub fn auto_execute(mut self, auto_execute: impl Into<Option<bool>>) -> Self {
        self.auto_execute = auto_execute.into();
        self
    }

    pub fn client_context_id(mut self, client_context_id: impl Into<Option<String>>) -> Self {
        self.client_context_id = client_context_id.into();
        self
    }

    pub fn compression(
        mut self,
        compression: impl Into<Option<queryx::query_options::Compression>>,
    ) -> Self {
        self.compression = compression.into();
        self
    }

    pub fn controls(mut self, controls: impl Into<Option<bool>>) -> Self {
        self.controls = controls.into();
        self
    }

    pub fn creds(
        mut self,
        creds: impl Into<Option<Vec<queryx::query_options::CredsJson>>>,
    ) -> Self {
        self.creds = creds.into();
        self
    }

    pub fn durability_level(
        mut self,
        durability_level: impl Into<Option<queryx::query_options::DurabilityLevel>>,
    ) -> Self {
        self.durability_level = durability_level.into();
        self
    }

    pub fn encoded_plan(mut self, encoded_plan: impl Into<Option<String>>) -> Self {
        self.encoded_plan = encoded_plan.into();
        self
    }

    pub fn encoding(
        mut self,
        encoding: impl Into<Option<queryx::query_options::Encoding>>,
    ) -> Self {
        self.encoding = encoding.into();
        self
    }

    pub fn format(mut self, format: impl Into<Option<queryx::query_options::Format>>) -> Self {
        self.format = format.into();
        self
    }

    pub fn kv_timeout(mut self, kv_timeout: impl Into<Option<Duration>>) -> Self {
        self.kv_timeout = kv_timeout.into();
        self
    }

    pub fn max_parallelism(mut self, max_parallelism: impl Into<Option<u32>>) -> Self {
        self.max_parallelism = max_parallelism.into();
        self
    }

    pub fn memory_quota(mut self, memory_quota: impl Into<Option<u32>>) -> Self {
        self.memory_quota = memory_quota.into();
        self
    }

    pub fn metrics(mut self, metrics: impl Into<Option<bool>>) -> Self {
        self.metrics = metrics.into();
        self
    }

    pub fn namespace(mut self, namespace: impl Into<Option<String>>) -> Self {
        self.namespace = namespace.into();
        self
    }

    pub fn num_atrs(mut self, num_atrs: impl Into<Option<u32>>) -> Self {
        self.num_atrs = num_atrs.into();
        self
    }

    pub fn pipeline_batch(mut self, pipeline_batch: impl Into<Option<u32>>) -> Self {
        self.pipeline_batch = pipeline_batch.into();
        self
    }

    pub fn pipeline_cap(mut self, pipeline_cap: impl Into<Option<u32>>) -> Self {
        self.pipeline_cap = pipeline_cap.into();
        self
    }

    pub fn prepared(mut self, prepared: impl Into<Option<String>>) -> Self {
        self.prepared = prepared.into();
        self
    }

    pub fn preserve_expiry(mut self, preserve_expiry: impl Into<Option<bool>>) -> Self {
        self.preserve_expiry = preserve_expiry.into();
        self
    }

    pub fn pretty(mut self, pretty: impl Into<Option<bool>>) -> Self {
        self.pretty = pretty.into();
        self
    }

    pub fn profile(
        mut self,
        profile: impl Into<Option<queryx::query_options::ProfileMode>>,
    ) -> Self {
        self.profile = profile.into();
        self
    }

    pub fn query_context(mut self, query_context: impl Into<Option<String>>) -> Self {
        self.query_context = query_context.into();
        self
    }

    pub fn read_only(mut self, read_only: impl Into<Option<bool>>) -> Self {
        self.read_only = read_only.into();
        self
    }

    pub fn scan_cap(mut self, scan_cap: impl Into<Option<u32>>) -> Self {
        self.scan_cap = scan_cap.into();
        self
    }

    pub fn scan_consistency(
        mut self,
        scan_consistency: impl Into<Option<queryx::query_options::ScanConsistency>>,
    ) -> Self {
        self.scan_consistency = scan_consistency.into();
        self
    }

    pub fn sparse_scan_vector(
        mut self,
        sparse_scan_vector: impl Into<Option<SparseScanVectors>>,
    ) -> Self {
        self.sparse_scan_vector = sparse_scan_vector.into();
        self
    }

    pub fn full_scan_vector(
        mut self,
        full_scan_vector: impl Into<Option<FullScanVectors>>,
    ) -> Self {
        self.full_scan_vector = full_scan_vector.into();
        self
    }

    pub fn sparse_scan_vectors(
        mut self,
        sparse_scan_vectors: impl Into<Option<HashMap<String, SparseScanVectors>>>,
    ) -> Self {
        self.sparse_scan_vectors = sparse_scan_vectors.into();
        self
    }

    pub fn full_scan_vectors(
        mut self,
        full_scan_vectors: impl Into<Option<HashMap<String, FullScanVectors>>>,
    ) -> Self {
        self.full_scan_vectors = full_scan_vectors.into();
        self
    }

    pub fn scan_wait(mut self, scan_wait: impl Into<Option<Duration>>) -> Self {
        self.scan_wait = scan_wait.into();
        self
    }

    pub fn signature(mut self, signature: impl Into<Option<bool>>) -> Self {
        self.signature = signature.into();
        self
    }

    pub fn statement(mut self, statement: impl Into<Option<String>>) -> Self {
        self.statement = statement.into();
        self
    }

    pub fn timeout(mut self, timeout: impl Into<Option<Duration>>) -> Self {
        self.timeout = timeout.into();
        self
    }

    pub fn tx_data(mut self, tx_data: impl Into<Option<Vec<u8>>>) -> Self {
        self.tx_data = tx_data.into();
        self
    }

    pub fn tx_id(mut self, tx_id: impl Into<Option<String>>) -> Self {
        self.tx_id = tx_id.into();
        self
    }

    pub fn tx_implicit(mut self, tx_implicit: impl Into<Option<bool>>) -> Self {
        self.tx_implicit = tx_implicit.into();
        self
    }

    pub fn tx_stmt_num(mut self, tx_stmt_num: impl Into<Option<u32>>) -> Self {
        self.tx_stmt_num = tx_stmt_num.into();
        self
    }

    pub fn tx_timeout(mut self, tx_timeout: impl Into<Option<Duration>>) -> Self {
        self.tx_timeout = tx_timeout.into();
        self
    }

    pub fn use_cbo(mut self, use_cbo: impl Into<Option<bool>>) -> Self {
        self.use_cbo = use_cbo.into();
        self
    }

    pub fn use_fts(mut self, use_fts: impl Into<Option<bool>>) -> Self {
        self.use_fts = use_fts.into();
        self
    }

    pub fn use_replica(
        mut self,
        use_replica: impl Into<Option<queryx::query_options::ReplicaLevel>>,
    ) -> Self {
        self.use_replica = use_replica.into();
        self
    }

    pub fn named_args(mut self, named_args: impl Into<Option<HashMap<String, Value>>>) -> Self {
        self.named_args = named_args.into();
        self
    }

    pub fn raw(mut self, raw: impl Into<Option<HashMap<String, Value>>>) -> Self {
        self.raw = raw.into();
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: impl Into<Option<OnBehalfOfInfo>>) -> Self {
        self.on_behalf_of = on_behalf_of.into();
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }

    pub fn node_uuid(mut self, node_uuid: impl Into<Option<String>>) -> Self {
        self.node_uuid = node_uuid.into();
        self
    }

    pub fn natural_language(
        mut self,
        natural_language: impl Into<Option<queryx::query_options::NaturalLanguageOptions>>,
    ) -> Self {
        self.natural_language = natural_language.into();
        self
    }

    pub fn idempotent(mut self, idempotent: impl Into<Option<bool>>) -> Self {
        self.idempotent = idempotent.into();
        self
    }

    pub(crate) fn is_idempotent(&self) -> bool {
        self.idempotent
            .unwrap_or_else(|| self.read_only.unwrap_or_default())
    }
}

impl From<QueryOptions> for queryx::query_options::QueryOptions {
    fn from(opts: QueryOptions) -> Self {
        queryx::query_options::QueryOptions {
            args: opts.args,
            atr_collection: opts.atr_collection,
            auto_execute: opts.auto_execute,
            client_context_id: opts.client_context_id,
            compression: opts.compression,
            controls: opts.controls,
            creds: opts.creds,
            durability_level: opts.durability_level,
            encoded_plan: opts.encoded_plan,
            encoding: opts.encoding,
            format: opts.format,
            kv_timeout: opts.kv_timeout,
            max_parallelism: opts.max_parallelism,
            memory_quota: opts.memory_quota,
            metrics: opts.metrics,
            namespace: opts.namespace,
            num_atrs: opts.num_atrs,
            pipeline_batch: opts.pipeline_batch,
            pipeline_cap: opts.pipeline_cap,
            prepared: opts.prepared,
            preserve_expiry: opts.preserve_expiry,
            pretty: opts.pretty,
            profile: opts.profile,
            query_context: opts.query_context,
            read_only: opts.read_only,
            scan_cap: opts.scan_cap,
            scan_consistency: opts.scan_consistency,
            sparse_scan_vector: opts.sparse_scan_vector,
            full_scan_vector: opts.full_scan_vector,
            sparse_scan_vectors: opts.sparse_scan_vectors,
            full_scan_vectors: opts.full_scan_vectors,
            scan_wait: opts.scan_wait,
            signature: opts.signature,
            statement: opts.statement,
            timeout: opts.timeout,
            tx_data: opts.tx_data,
            tx_id: opts.tx_id,
            tx_implicit: opts.tx_implicit,
            tx_stmt_num: opts.tx_stmt_num,
            tx_timeout: opts.tx_timeout,
            use_cbo: opts.use_cbo,
            use_fts: opts.use_fts,
            use_replica: opts.use_replica,

            named_args: opts.named_args,
            raw: opts.raw,
            natural_language: opts.natural_language,

            on_behalf_of: opts.on_behalf_of,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct GetAllIndexesOptions<'a> {
    pub bucket_name: &'a str,
    pub scope_name: Option<&'a str>,
    pub collection_name: Option<&'a str>,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
    pub endpoint: Option<String>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> GetAllIndexesOptions<'a> {
    pub fn new(bucket_name: &'a str) -> Self {
        Self {
            bucket_name,
            scope_name: None,
            collection_name: None,
            on_behalf_of: None,
            endpoint: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn scope_name(mut self, scope_name: &'a str) -> Self {
        self.scope_name = Some(scope_name);
        self
    }

    pub fn collection_name(mut self, collection_name: &'a str) -> Self {
        self.collection_name = Some(collection_name);
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of = Some(on_behalf_of);
        self
    }

    pub fn endpoint(mut self, endpoint: String) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

impl<'a> From<&GetAllIndexesOptions<'a>> for queryx::query_options::GetAllIndexesOptions<'a> {
    fn from(opts: &GetAllIndexesOptions<'a>) -> Self {
        queryx::query_options::GetAllIndexesOptions {
            bucket_name: opts.bucket_name,
            scope_name: opts.scope_name,
            collection_name: opts.collection_name,
            on_behalf_of: opts.on_behalf_of,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct CreatePrimaryIndexOptions<'a> {
    pub bucket_name: &'a str,
    pub scope_name: Option<&'a str>,
    pub collection_name: Option<&'a str>,
    pub index_name: Option<&'a str>,
    pub num_replicas: Option<u32>,
    pub deferred: Option<bool>,
    pub ignore_if_exists: Option<bool>,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
    pub endpoint: Option<String>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> CreatePrimaryIndexOptions<'a> {
    pub fn new(bucket_name: &'a str) -> Self {
        Self {
            bucket_name,
            scope_name: None,
            collection_name: None,
            index_name: None,
            num_replicas: None,
            deferred: None,
            ignore_if_exists: None,
            on_behalf_of: None,
            endpoint: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn scope_name(mut self, scope_name: &'a str) -> Self {
        self.scope_name = Some(scope_name);
        self
    }

    pub fn collection_name(mut self, collection_name: &'a str) -> Self {
        self.collection_name = Some(collection_name);
        self
    }

    pub fn index_name(mut self, index_name: &'a str) -> Self {
        self.index_name = Some(index_name);
        self
    }

    pub fn num_replicas(mut self, num_replicas: u32) -> Self {
        self.num_replicas = Some(num_replicas);
        self
    }

    pub fn deferred(mut self, deferred: bool) -> Self {
        self.deferred = Some(deferred);
        self
    }

    pub fn ignore_if_exists(mut self, ignore_if_exists: bool) -> Self {
        self.ignore_if_exists = Some(ignore_if_exists);
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of = Some(on_behalf_of);
        self
    }

    pub fn endpoint(mut self, endpoint: String) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

impl<'a> From<&CreatePrimaryIndexOptions<'a>>
    for queryx::query_options::CreatePrimaryIndexOptions<'a>
{
    fn from(opts: &CreatePrimaryIndexOptions<'a>) -> Self {
        queryx::query_options::CreatePrimaryIndexOptions {
            bucket_name: opts.bucket_name,
            scope_name: opts.scope_name,
            collection_name: opts.collection_name,
            index_name: opts.index_name,
            num_replicas: opts.num_replicas,
            deferred: opts.deferred,
            ignore_if_exists: opts.ignore_if_exists,
            on_behalf_of: opts.on_behalf_of,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct CreateIndexOptions<'a> {
    pub bucket_name: &'a str,
    pub scope_name: Option<&'a str>,
    pub collection_name: Option<&'a str>,
    pub index_name: &'a str,
    pub num_replicas: Option<u32>,
    pub fields: &'a [&'a str],
    pub deferred: Option<bool>,
    pub ignore_if_exists: Option<bool>,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
    pub endpoint: Option<String>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> CreateIndexOptions<'a> {
    pub fn new(bucket_name: &'a str, index_name: &'a str, fields: &'a [&'a str]) -> Self {
        Self {
            bucket_name,
            scope_name: None,
            collection_name: None,
            index_name,
            num_replicas: None,
            fields,
            deferred: None,
            ignore_if_exists: None,
            on_behalf_of: None,
            endpoint: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn scope_name(mut self, scope_name: &'a str) -> Self {
        self.scope_name = Some(scope_name);
        self
    }

    pub fn collection_name(mut self, collection_name: &'a str) -> Self {
        self.collection_name = Some(collection_name);
        self
    }

    pub fn num_replicas(mut self, num_replicas: u32) -> Self {
        self.num_replicas = Some(num_replicas);
        self
    }

    pub fn deferred(mut self, deferred: bool) -> Self {
        self.deferred = Some(deferred);
        self
    }

    pub fn ignore_if_exists(mut self, ignore_if_exists: bool) -> Self {
        self.ignore_if_exists = Some(ignore_if_exists);
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of = Some(on_behalf_of);
        self
    }

    pub fn endpoint(mut self, endpoint: String) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

impl<'a> From<&CreateIndexOptions<'a>> for queryx::query_options::CreateIndexOptions<'a> {
    fn from(opts: &CreateIndexOptions<'a>) -> Self {
        queryx::query_options::CreateIndexOptions {
            bucket_name: opts.bucket_name,
            scope_name: opts.scope_name,
            collection_name: opts.collection_name,
            index_name: opts.index_name,
            num_replicas: opts.num_replicas,
            fields: opts.fields,
            deferred: opts.deferred,
            ignore_if_exists: opts.ignore_if_exists,
            on_behalf_of: opts.on_behalf_of,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DropPrimaryIndexOptions<'a> {
    pub bucket_name: &'a str,
    pub scope_name: Option<&'a str>,
    pub collection_name: Option<&'a str>,
    pub index_name: Option<&'a str>,
    pub ignore_if_not_exists: Option<bool>,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
    pub endpoint: Option<String>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> DropPrimaryIndexOptions<'a> {
    pub fn new(bucket_name: &'a str) -> Self {
        Self {
            bucket_name,
            scope_name: None,
            collection_name: None,
            index_name: None,
            ignore_if_not_exists: None,
            on_behalf_of: None,
            endpoint: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn scope_name(mut self, scope_name: &'a str) -> Self {
        self.scope_name = Some(scope_name);
        self
    }

    pub fn collection_name(mut self, collection_name: &'a str) -> Self {
        self.collection_name = Some(collection_name);
        self
    }

    pub fn index_name(mut self, index_name: &'a str) -> Self {
        self.index_name = Some(index_name);
        self
    }

    pub fn ignore_if_not_exists(mut self, ignore_if_not_exists: bool) -> Self {
        self.ignore_if_not_exists = Some(ignore_if_not_exists);
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of = Some(on_behalf_of);
        self
    }

    pub fn endpoint(mut self, endpoint: String) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

impl<'a> From<&DropPrimaryIndexOptions<'a>> for queryx::query_options::DropPrimaryIndexOptions<'a> {
    fn from(opts: &DropPrimaryIndexOptions<'a>) -> Self {
        queryx::query_options::DropPrimaryIndexOptions {
            bucket_name: opts.bucket_name,
            scope_name: opts.scope_name,
            collection_name: opts.collection_name,
            index_name: opts.index_name,
            ignore_if_not_exists: opts.ignore_if_not_exists,
            on_behalf_of: opts.on_behalf_of,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct DropIndexOptions<'a> {
    pub bucket_name: &'a str,
    pub scope_name: Option<&'a str>,
    pub collection_name: Option<&'a str>,
    pub index_name: &'a str,
    pub ignore_if_not_exists: Option<bool>,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
    pub endpoint: Option<String>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> DropIndexOptions<'a> {
    pub fn new(bucket_name: &'a str, index_name: &'a str) -> Self {
        Self {
            bucket_name,
            scope_name: None,
            collection_name: None,
            index_name,
            ignore_if_not_exists: None,
            on_behalf_of: None,
            endpoint: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn scope_name(mut self, scope_name: &'a str) -> Self {
        self.scope_name = Some(scope_name);
        self
    }

    pub fn collection_name(mut self, collection_name: &'a str) -> Self {
        self.collection_name = Some(collection_name);
        self
    }

    pub fn ignore_if_not_exists(mut self, ignore_if_not_exists: bool) -> Self {
        self.ignore_if_not_exists = Some(ignore_if_not_exists);
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of = Some(on_behalf_of);
        self
    }

    pub fn endpoint(mut self, endpoint: String) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

impl<'a> From<&DropIndexOptions<'a>> for queryx::query_options::DropIndexOptions<'a> {
    fn from(opts: &DropIndexOptions<'a>) -> Self {
        queryx::query_options::DropIndexOptions {
            bucket_name: opts.bucket_name,
            scope_name: opts.scope_name,
            collection_name: opts.collection_name,
            index_name: opts.index_name,
            ignore_if_not_exists: opts.ignore_if_not_exists,
            on_behalf_of: opts.on_behalf_of,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct BuildDeferredIndexesOptions<'a> {
    pub bucket_name: &'a str,
    pub scope_name: Option<&'a str>,
    pub collection_name: Option<&'a str>,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
    pub endpoint: Option<String>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> BuildDeferredIndexesOptions<'a> {
    pub fn new(bucket_name: &'a str) -> Self {
        Self {
            bucket_name,
            scope_name: None,
            collection_name: None,
            on_behalf_of: None,
            endpoint: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn scope_name(mut self, scope_name: &'a str) -> Self {
        self.scope_name = Some(scope_name);
        self
    }

    pub fn collection_name(mut self, collection_name: &'a str) -> Self {
        self.collection_name = Some(collection_name);
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of = Some(on_behalf_of);
        self
    }

    pub fn endpoint(mut self, endpoint: String) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

impl<'a> From<&BuildDeferredIndexesOptions<'a>>
    for queryx::query_options::BuildDeferredIndexesOptions<'a>
{
    fn from(opts: &BuildDeferredIndexesOptions<'a>) -> Self {
        queryx::query_options::BuildDeferredIndexesOptions {
            bucket_name: opts.bucket_name,
            scope_name: opts.scope_name,
            collection_name: opts.collection_name,
            on_behalf_of: opts.on_behalf_of,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct WatchIndexesOptions<'a> {
    pub bucket_name: &'a str,
    pub scope_name: Option<&'a str>,
    pub collection_name: Option<&'a str>,
    pub indexes: &'a [&'a str],
    pub watch_primary: Option<bool>,
    pub on_behalf_of: Option<&'a OnBehalfOfInfo>,
    pub endpoint: Option<String>,
    pub retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> WatchIndexesOptions<'a> {
    pub fn new(bucket_name: &'a str, indexes: &'a [&'a str]) -> Self {
        Self {
            bucket_name,
            scope_name: None,
            collection_name: None,
            indexes,
            watch_primary: None,
            on_behalf_of: None,
            endpoint: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn scope_name(mut self, scope_name: &'a str) -> Self {
        self.scope_name = Some(scope_name);
        self
    }

    pub fn collection_name(mut self, collection_name: &'a str) -> Self {
        self.collection_name = Some(collection_name);
        self
    }

    pub fn watch_primary(mut self, watch_primary: bool) -> Self {
        self.watch_primary = Some(watch_primary);
        self
    }

    pub fn on_behalf_of(mut self, on_behalf_of: &'a OnBehalfOfInfo) -> Self {
        self.on_behalf_of = Some(on_behalf_of);
        self
    }

    pub fn endpoint(mut self, endpoint: String) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

impl<'a> From<&WatchIndexesOptions<'a>> for queryx::query_options::WatchIndexesOptions<'a> {
    fn from(opts: &WatchIndexesOptions<'a>) -> Self {
        queryx::query_options::WatchIndexesOptions {
            bucket_name: opts.bucket_name,
            scope_name: opts.scope_name,
            collection_name: opts.collection_name,
            indexes: opts.indexes,
            watch_primary: opts.watch_primary,
            on_behalf_of: opts.on_behalf_of,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct EnsureIndexOptions<'a> {
    pub index_name: &'a str,
    pub bucket_name: &'a str,
    pub scope_name: Option<&'a str>,
    pub collection_name: Option<&'a str>,
    pub on_behalf_of_info: Option<&'a OnBehalfOfInfo>,
    pub desired_state: DesiredState,
}

impl<'a> EnsureIndexOptions<'a> {
    pub fn new(
        index_name: &'a str,
        bucket_name: &'a str,
        scope_name: Option<&'a str>,
        collection_name: Option<&'a str>,
        desired_state: DesiredState,
    ) -> Self {
        Self {
            index_name,
            bucket_name,
            scope_name,
            collection_name,
            on_behalf_of_info: None,
            desired_state,
        }
    }
}
