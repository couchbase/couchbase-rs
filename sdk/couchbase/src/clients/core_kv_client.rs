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

use crate::clients::couchbase_core_kv_client::CouchbaseCoreKvClient;
use crate::error;
use crate::get_replica_strategy::GetReplicaStrategy;
use crate::options::kv_binary_options::*;
use crate::options::kv_options::*;
use crate::results::kv_binary_results::CounterResult;
use crate::results::kv_results::*;
use crate::subdoc::lookup_in_specs::LookupInSpec;
use crate::subdoc::mutate_in_specs::MutateInSpec;
use std::time::Duration;

#[derive(Clone)]
pub(crate) struct CoreKvClient {
    backend: CoreKvClientBackend,
}

impl CoreKvClient {
    pub fn new(backend: CoreKvClientBackend) -> Self {
        Self { backend }
    }

    pub fn collection_name(&self) -> &str {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => client.collection_name(),
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                unimplemented!()
            }
        }
    }

    pub fn scope_name(&self) -> &str {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => client.scope_name(),
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                unimplemented!()
            }
        }
    }

    pub fn bucket_name(&self) -> &str {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => client.bucket_name(),
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                unimplemented!()
            }
        }
    }

    pub async fn upsert(
        &self,
        id: &str,
        value: &[u8],
        flags: u32,
        options: UpsertOptions,
    ) -> error::Result<MutationResult> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.upsert(id, value, flags, options).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.upsert(id, value, flags, options).await
            }
        }
    }

    pub async fn insert(
        &self,
        id: &str,
        value: &[u8],
        flags: u32,
        options: InsertOptions,
    ) -> error::Result<MutationResult> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.insert(id, value, flags, options).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.insert(id, value, flags, options).await
            }
        }
    }

    pub async fn replace(
        &self,
        id: &str,
        value: &[u8],
        flags: u32,
        options: ReplaceOptions,
    ) -> error::Result<MutationResult> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.replace(id, value, flags, options).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.replace(id, value, flags, options).await
            }
        }
    }

    pub async fn remove(&self, id: &str, options: RemoveOptions) -> error::Result<MutationResult> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.remove(id, options).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.remove(id, options).await
            }
        }
    }

    pub async fn get(&self, id: &str, options: GetOptions) -> error::Result<GetResult> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.get(id, options).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.get(id, options).await
            }
        }
    }

    pub async fn get_replica(
        &self,
        id: &str,
        strategy: GetReplicaStrategy,
        options: GetReplicaOptions,
    ) -> error::Result<GetReplicaResult> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.get_replica(id, strategy, options).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.get_replica(id, strategy, options).await
            }
        }
    }

    pub async fn exists(&self, id: &str, options: ExistsOptions) -> error::Result<ExistsResult> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.exists(id, options).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.exists(id, options).await
            }
        }
    }

    pub async fn get_and_touch(
        &self,
        id: &str,
        expiry: Duration,
        options: GetAndTouchOptions,
    ) -> error::Result<GetResult> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.get_and_touch(id, expiry, options).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.get_and_touch(id, expiry, options).await
            }
        }
    }

    pub async fn get_and_lock(
        &self,
        id: &str,
        lock_time: Duration,
        options: GetAndLockOptions,
    ) -> error::Result<GetResult> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.get_and_lock(id, lock_time, options).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.get_and_lock(id, lock_time, options).await
            }
        }
    }

    pub async fn unlock(&self, id: &str, cas: u64, options: UnlockOptions) -> error::Result<()> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.unlock(id, cas, options).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.unlock(id, cas, options).await
            }
        }
    }

    pub async fn touch(
        &self,
        id: &str,
        expiry: Duration,
        options: TouchOptions,
    ) -> error::Result<TouchResult> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.touch(id, expiry, options).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.touch(id, expiry, options).await
            }
        }
    }

    pub async fn append(
        &self,
        id: &str,
        value: &[u8],
        options: AppendOptions,
    ) -> error::Result<MutationResult> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.append(id, value, options).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.append(id, value, options).await
            }
        }
    }

    pub async fn prepend(
        &self,
        id: &str,
        value: &[u8],
        options: PrependOptions,
    ) -> error::Result<MutationResult> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.prepend(id, value, options).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.prepend(id, value, options).await
            }
        }
    }

    pub async fn increment(
        &self,
        id: &str,
        options: IncrementOptions,
    ) -> error::Result<CounterResult> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.increment(id, options).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.increment(id, options).await
            }
        }
    }

    pub async fn decrement(
        &self,
        id: &str,
        options: DecrementOptions,
    ) -> error::Result<CounterResult> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.decrement(id, options).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.decrement(id, options).await
            }
        }
    }

    pub async fn lookup_in(
        &self,
        id: &str,
        specs: &[LookupInSpec],
        options: LookupInOptions,
    ) -> error::Result<LookupInResult> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.lookup_in(id, specs, options).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.lookup_in(id, specs, options).await
            }
        }
    }

    pub async fn mutate_in(
        &self,
        id: &str,
        specs: &[MutateInSpec],
        options: MutateInOptions,
    ) -> error::Result<MutateInResult> {
        match &self.backend {
            CoreKvClientBackend::CouchbaseCoreKvClientBackend(client) => {
                client.mutate_in(id, specs, options).await
            }
            CoreKvClientBackend::Couchbase2CoreKvClientBackend(client) => {
                client.mutate_in(id, specs, options).await
            }
        }
    }
}

#[derive(Clone)]
pub(crate) enum CoreKvClientBackend {
    CouchbaseCoreKvClientBackend(CouchbaseCoreKvClient),
    Couchbase2CoreKvClientBackend(Couchbase2CoreKvClient),
}

#[derive(Clone)]
pub(crate) struct Couchbase2CoreKvClient {}

impl Couchbase2CoreKvClient {
    pub fn new() -> Self {
        unimplemented!()
    }

    pub async fn upsert(
        &self,
        _id: &str,
        _value: &[u8],
        _flags: u32,
        _options: impl Into<Option<UpsertOptions>>,
    ) -> error::Result<MutationResult> {
        unimplemented!()
    }

    pub async fn insert(
        &self,
        _id: &str,
        _value: &[u8],
        _flags: u32,
        _options: InsertOptions,
    ) -> error::Result<MutationResult> {
        unimplemented!()
    }

    pub async fn replace(
        &self,
        _id: &str,
        _value: &[u8],
        _flags: u32,
        _options: ReplaceOptions,
    ) -> error::Result<MutationResult> {
        unimplemented!()
    }

    pub async fn remove(
        &self,
        _id: &str,
        _options: RemoveOptions,
    ) -> error::Result<MutationResult> {
        unimplemented!()
    }

    pub async fn get(&self, _id: &str, _options: GetOptions) -> error::Result<GetResult> {
        unimplemented!()
    }

    pub async fn get_replica(
        &self,
        _id: &str,
        _strategy: GetReplicaStrategy,
        _options: GetReplicaOptions,
    ) -> error::Result<GetReplicaResult> {
        unimplemented!()
    }

    pub async fn exists(&self, _id: &str, _options: ExistsOptions) -> error::Result<ExistsResult> {
        unimplemented!()
    }

    pub async fn get_and_touch(
        &self,
        _id: &str,
        _expiry: Duration,
        _options: GetAndTouchOptions,
    ) -> error::Result<GetResult> {
        unimplemented!()
    }

    pub async fn get_and_lock(
        &self,
        _id: &str,
        _lock_time: Duration,
        _options: GetAndLockOptions,
    ) -> error::Result<GetResult> {
        unimplemented!()
    }

    pub async fn unlock(&self, _id: &str, _cas: u64, _options: UnlockOptions) -> error::Result<()> {
        unimplemented!()
    }

    pub async fn touch(
        &self,
        _id: &str,
        _expiry: Duration,
        _options: TouchOptions,
    ) -> error::Result<TouchResult> {
        unimplemented!()
    }

    pub async fn append(
        &self,
        _id: &str,
        _value: &[u8],
        _options: AppendOptions,
    ) -> error::Result<MutationResult> {
        unimplemented!()
    }

    pub async fn prepend(
        &self,
        _id: &str,
        _value: &[u8],
        _options: PrependOptions,
    ) -> error::Result<MutationResult> {
        unimplemented!()
    }

    pub async fn increment(
        &self,
        _id: &str,
        _options: IncrementOptions,
    ) -> error::Result<CounterResult> {
        unimplemented!()
    }

    pub async fn decrement(
        &self,
        _id: &str,
        _options: DecrementOptions,
    ) -> error::Result<CounterResult> {
        unimplemented!()
    }

    pub async fn lookup_in(
        &self,
        _id: &str,
        _specs: &[LookupInSpec],
        _options: LookupInOptions,
    ) -> error::Result<LookupInResult> {
        unimplemented!()
    }

    pub async fn mutate_in(
        &self,
        _id: &str,
        _specs: &[MutateInSpec],
        _options: MutateInOptions,
    ) -> error::Result<MutateInResult> {
        unimplemented!()
    }
}
